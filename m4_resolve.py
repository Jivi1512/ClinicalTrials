"""
m4_resolve.py — Batched async ChEMBL REST API resolver.

Optimizations vs. per-drug sequential approach:
  • pref_name__in batch:  50 drug names → 1 API call  (was 50 calls)
  • mechanism batch:     100 ChEMBL IDs → 1 API call  (was 100 calls)
  • Synonym fallback runs concurrently with mechanism batch
  • In-process cache shared across all chunks — each unique drug queried once
  • Vectorized pandas assignment (no iterrows)
"""

import asyncio
import logging
import re
import time
import pandas as pd
import aiohttp
from datetime import date
from config import (
    CHEMBL_API_BASE, CHEMBL_SEMAPHORE, CHEMBL_TIMEOUT,
    CHEMBL_RETRY_MAX, CHEMBL_RETRY_BACKOFF,
    CHEMBL_BATCH_SIZE, CHEMBL_MECH_BATCH_SIZE, CHEMBL_SYNONYM_FALLBACK
)

# ── Module-level cache ────────────────────────────────────────────────────────
# {drug_name_norm → {target_primary, targets_raw, ...} | None}
_chembl_cache: dict = {}
_DB_VERSION = f"ChEMBL-{date.today().strftime('%Y%m%d')}"
_MIN_NAME_LEN = 3

_CLEAN_RE = re.compile(
    r"\s+(?:hydrochloride|hcl|sodium|potassium|sulfate|phosphate|acetate"
    r"|tartrate|citrate|mesylate|maleate|fumarate|bromide|chloride"
    r"|monohydrate|dihydrate|hemihydrate|nitrate|succinate)\s*$",
    re.IGNORECASE
)

# Track synonym fallback budget across the whole run
_synonym_budget_used: int = 0


def _clean_for_chembl(name: str) -> str:
    """Strip residual salt suffixes before ChEMBL lookup."""
    c = _CLEAN_RE.sub("", name).strip()
    return c if len(c) >= _MIN_NAME_LEN else name


# ── HTTP helper ───────────────────────────────────────────────────────────────

async def _chembl_get(
    session: aiohttp.ClientSession,
    endpoint: str,
    params: dict,
    sem: asyncio.Semaphore
) -> dict | None:
    url = f"{CHEMBL_API_BASE}/{endpoint}"
    for attempt in range(CHEMBL_RETRY_MAX + 1):
        try:
            async with sem:
                async with session.get(
                    url, params=params,
                    timeout=aiohttp.ClientTimeout(total=CHEMBL_TIMEOUT)
                ) as resp:
                    if resp.status == 200:
                        return await resp.json(content_type=None)
                    if resp.status == 429:
                        wait = CHEMBL_RETRY_BACKOFF ** (attempt + 1)
                        logging.warning(f"ChEMBL 429 rate-limit; backing off {wait}s")
                        await asyncio.sleep(wait)
                        continue
                    if resp.status >= 500:
                        await asyncio.sleep(CHEMBL_RETRY_BACKOFF ** (attempt + 1))
                        continue
                    return None  # 404 / 400 — don't retry
        except asyncio.TimeoutError:
            logging.debug(f"ChEMBL timeout ({endpoint}), attempt {attempt+1}")
            await asyncio.sleep(CHEMBL_RETRY_BACKOFF ** (attempt + 1))
        except aiohttp.ClientError as exc:
            logging.debug(f"ChEMBL client error: {exc}")
            await asyncio.sleep(CHEMBL_RETRY_BACKOFF ** (attempt + 1))
    return None


# ── Batch molecule lookup ─────────────────────────────────────────────────────

async def _batch_molecules(
    names: list[str],
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore
) -> dict[str, str]:
    """
    Batch pref_name__in lookup for a list of drug names.
    ChEMBL stores pref_names in UPPERCASE; we send uppercase and map back.
    Returns tuple({original_lowercase_name: parent_chembl_id}, {parent_chembl_id: smiles}).
    """
    if not names:
        return {}

    # Build upper→lower mapping (multiple originals may map to same upper)
    upper_to_lower: dict[str, str] = {}
    for n in names:
        u = _clean_for_chembl(n).upper()
        if u not in upper_to_lower:
            upper_to_lower[u] = n
    upper_names = list(upper_to_lower)

    # Split into batches and fire all concurrently
    batches = [
        upper_names[i: i + CHEMBL_BATCH_SIZE]
        for i in range(0, len(upper_names), CHEMBL_BATCH_SIZE)
    ]

    async def _one_batch(batch: list[str]) -> list[dict]:
        data = await _chembl_get(session, "molecule", {
            "pref_name__in": ",".join(batch),
            "format": "json",
            "limit": len(batch) + 5,
        }, sem)
        return (data or {}).get("molecules", [])

    all_mols: list[dict] = []
    for mols in await asyncio.gather(*[_one_batch(b) for b in batches]):
        all_mols.extend(mols)

    name_to_id: dict[str, str] = {}
    id_to_smiles: dict[str, str] = {}
    for mol in all_mols:
        pref = mol.get("pref_name") or ""
        parent_id = (
            (mol.get("molecule_hierarchy") or {}).get("parent_chembl_id")
            or mol.get("molecule_chembl_id")
        )
        orig = upper_to_lower.get(pref)
        smiles = (mol.get("molecule_structures") or {}).get("canonical_smiles")
        if orig and parent_id:
            name_to_id[orig] = parent_id
            if smiles:
                id_to_smiles[parent_id] = smiles

    return name_to_id, id_to_smiles


# ── Batch mechanism lookup ────────────────────────────────────────────────────

async def _batch_mechanisms(
    parent_ids: list[str],
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore
) -> dict[str, list[dict]]:
    """
    Batch parent_molecule_chembl_id__in lookup.
    Returns {parent_chembl_id: [mechanism_dict, ...]}.
    """
    if not parent_ids:
        return {}

    batches = [
        parent_ids[i: i + CHEMBL_MECH_BATCH_SIZE]
        for i in range(0, len(parent_ids), CHEMBL_MECH_BATCH_SIZE)
    ]

    async def _one_batch(batch: list[str]) -> list[dict]:
        data = await _chembl_get(session, "mechanism", {
            "parent_molecule_chembl_id__in": ",".join(batch),
            "format": "json",
            "limit": len(batch) * 6,  # avg ~2-4 mechanisms/drug; 6× is safe
        }, sem)
        return (data or {}).get("mechanisms", [])

    id_to_mechs: dict[str, list] = {}
    for mechs in await asyncio.gather(*[_one_batch(b) for b in batches]):
        for m in mechs:
            pid = m.get("parent_molecule_chembl_id")
            if pid:
                id_to_mechs.setdefault(pid, []).append(m)

    return id_to_mechs


# ── Synonym fallback ──────────────────────────────────────────────────────────

async def _synonym_fallback(
    missed: list[str],
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore
) -> tuple[dict[str, str], dict[str, str]]:
    """
    Individual synonym search for names that didn't match pref_name.
    Capped by CHEMBL_SYNONYM_FALLBACK budget to prevent runaway API usage.
    Returns tuple({name: parent_chembl_id}, {parent_chembl_id: smiles}).
    """
    global _synonym_budget_used
    available = max(0, CHEMBL_SYNONYM_FALLBACK - _synonym_budget_used)
    to_check = missed[:available]
    if not to_check:
        return {}, {}

    _synonym_budget_used += len(to_check)

    async def _one(name: str) -> tuple[str, str | None, str | None]:
        cleaned = _clean_for_chembl(name)
        data = await _chembl_get(session, "molecule", {
            "molecule_synonyms__synonym__iexact": cleaned,
            "format": "json",
            "limit": 1,
        }, sem)
        mols = (data or {}).get("molecules", [])
        if mols:
            m = mols[0]
            pid = (
                (m.get("molecule_hierarchy") or {}).get("parent_chembl_id")
                or m.get("molecule_chembl_id")
            )
            smiles = (m.get("molecule_structures") or {}).get("canonical_smiles")
            return name, pid, smiles
        return name, None, None

    results = await asyncio.gather(*[_one(n) for n in to_check])
    syn_name_to_id = {}
    syn_id_to_smiles = {}
    for name, pid, smiles in results:
        if pid:
            syn_name_to_id[name] = pid
            if smiles:
                syn_id_to_smiles[pid] = smiles
    return syn_name_to_id, syn_id_to_smiles


# ── Entry dict builder ────────────────────────────────────────────────────────

def _build_entry(mechanisms: list[dict], smiles: str | None) -> dict | None:
    if not mechanisms:
        return None
    # Prefer mechanisms with disease_efficacy flag (approved indications)
    preferred = [m for m in mechanisms if m.get("disease_efficacy") == 1] or mechanisms
    seen: set = set()
    unique = []
    for m in preferred:
        moa = m.get("mechanism_of_action") or ""
        if moa and moa not in seen:
            seen.add(moa)
            unique.append(m)
    if not unique:
        return None
    moas = [m["mechanism_of_action"] for m in unique if m.get("mechanism_of_action")]
    tids = [m["target_chembl_id"] for m in unique if m.get("target_chembl_id")]
    return {
        "target_primary":       unique[0].get("mechanism_of_action"),
        "targets_raw":          "|".join(dict.fromkeys(moas)),
        "target_chembl_ids":    "|".join(dict.fromkeys(filter(None, tids))),
        "target_source":        "chembl",
        "target_confidence":    "high",
        "reference_db_version": _DB_VERSION,
        "drug_smiles":          smiles,
    }


# ── Public chunk resolver ─────────────────────────────────────────────────────

async def resolve_chunk_async(
    df_chunk: pd.DataFrame,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    chunk_index: int,
    pair_id_start: int
) -> tuple[int, list[dict]]:
    """
    Resolve all drug names in the chunk via batched ChEMBL API calls.
    Uses the shared _chembl_cache so each unique drug is queried once per run.
    """
    t0 = time.monotonic()

    drug_col = (
        df_chunk["drug_name_norm"]
        if "drug_name_norm" in df_chunk.columns
        else pd.Series(dtype=str, index=df_chunk.index)
    )

    # Unique names not yet in cache (also skip very short names)
    uncached = [
        d for d in drug_col.dropna().unique()
        if isinstance(d, str) and len(d) >= _MIN_NAME_LEN and d not in _chembl_cache
    ]

    if uncached:
        # ── Step 1: Batch molecule pref_name lookup ──────────────────────────
        name_to_id, id_to_smiles = await _batch_molecules(uncached, session, sem)

        # ── Step 2 + 3 in parallel ───────────────────────────────────────────
        missed = [d for d in uncached if d not in name_to_id]
        found_ids = list(dict.fromkeys(name_to_id.values()))

        # Mechanism batch and synonym fallback run concurrently
        mechs_task = asyncio.create_task(_batch_mechanisms(found_ids, session, sem))
        syn_task   = asyncio.create_task(_synonym_fallback(missed, session, sem))
        id_to_mechs, (syn_name_to_id, syn_id_to_smiles) = await asyncio.gather(mechs_task, syn_task)

        # ── Step 4: Mechanism batch for synonym-resolved IDs (new ones only) ─
        new_ids = [pid for pid in syn_name_to_id.values() if pid not in id_to_mechs]
        if new_ids:
            extra = await _batch_mechanisms(new_ids, session, sem)
            id_to_mechs.update(extra)

        # ── Step 5: Populate cache ────────────────────────────────────────────
        all_name_to_id = {**name_to_id, **syn_name_to_id}
        all_id_to_smiles = {**id_to_smiles, **syn_id_to_smiles}
        for name in uncached:
            cid = all_name_to_id.get(name)
            smiles = all_id_to_smiles.get(cid) if cid else None
            _chembl_cache[name] = _build_entry(id_to_mechs.get(cid, []), smiles) if cid else None

        found_pref   = len(name_to_id)
        found_syn    = len(syn_name_to_id)
        total_resolved = sum(1 for n in uncached if _chembl_cache.get(n))
        logging.info(
            f"  ChEMBL | chunk {chunk_index:>4} | "
            f"queried {len(uncached):>5} new drugs | "
            f"pref_name {found_pref} + synonym {found_syn} = {total_resolved} resolved"
        )

    # ── Vectorized assignment from cache ──────────────────────────────────────
    df = df_chunk.copy()

    def _f(d: object, field: str) -> object:
        e = _chembl_cache.get(d) if isinstance(d, str) else None
        return e.get(field) if e else None

    resolved_mask = drug_col.map(lambda d: bool(_chembl_cache.get(d)) if isinstance(d, str) else False)

    df["target_primary"]         = drug_col.map(lambda d: _f(d, "target_primary"))
    df["targets_raw"]            = drug_col.map(lambda d: _f(d, "targets_raw"))
    df["target_source"]          = resolved_mask.map({True: "chembl", False: "none"})
    df["target_confidence"]      = resolved_mask.map({True: "high",   False: "none"})
    df["target_evidence_text"]   = None
    df["target_evidence_source"] = resolved_mask.map({True: "chembl", False: None})
    df["reference_db_version"]   = drug_col.map(lambda d: _f(d, "reference_db_version"))
    df["drug_smiles"]            = drug_col.map(lambda d: _f(d, "drug_smiles"))
    df["pair_id"]                = range(pair_id_start, pair_id_start + len(df))

    n_resolved = int(df["target_primary"].notna().sum())
    elapsed    = time.monotonic() - t0
    logging.info(
        f"  Chunk {chunk_index:>4} done | "
        f"{n_resolved}/{len(df)} rows resolved "
        f"({n_resolved/max(len(df),1)*100:.1f}%) | "
        f"{elapsed:.1f}s | cache total: {len(_chembl_cache)}"
    )

    return chunk_index, df.to_dict("records")


def get_cache_stats() -> dict:
    total    = len(_chembl_cache)
    resolved = sum(1 for v in _chembl_cache.values() if v is not None)
    return {"cache_total": total, "cache_resolved": resolved, "cache_miss": total - resolved}
