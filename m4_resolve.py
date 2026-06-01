"""
m4_resolve.py — ChEMBL API-based drug→target resolver.

Strategy:
  1. Collect unique normalized drug names from the chunk.
  2. Concurrently query ChEMBL molecule endpoint (pref_name__iexact) for each unique name.
     On miss, fall back to molecule synonym search.
  3. For each resolved ChEMBL ID, query the mechanism endpoint (parent_molecule_chembl_id).
  4. Cache all results in-process across chunks so each unique drug is queried at most once.
  5. Apply the cache to the full chunk via vectorized pandas map.

Output columns added per row:
  pair_id, targets_raw, target_primary, target_source, target_confidence,
  target_evidence_text, target_evidence_source, reference_db_version
"""

import asyncio
import logging
import re
import pandas as pd
import aiohttp
from datetime import date
from config import (
    CHEMBL_API_BASE, CHEMBL_SEMAPHORE, CHEMBL_TIMEOUT,
    CHEMBL_RETRY_MAX, CHEMBL_RETRY_BACKOFF
)

# ──────────────────────────────────────────────────────────────────────────────
# Module-level cache: {drug_name_norm → resolved_entry | None}
# Persists across all chunks within a pipeline run.
# ──────────────────────────────────────────────────────────────────────────────
_chembl_cache: dict = {}
_DB_VERSION = f"ChEMBL-{date.today().strftime('%Y%m%d')}"

# Minimum candidate word length to accept as a name (avoids junk)
_MIN_NAME_LEN = 3

# Regex to strip trailing salt/form suffixes before ChEMBL lookup
_CLEAN_RE = re.compile(
    r"\s+(?:hydrochloride|hcl|sodium|potassium|sulfate|phosphate|acetate"
    r"|tartrate|citrate|mesylate|maleate|fumarate|bromide|chloride"
    r"|monohydrate|dihydrate|hemihydrate|nitrate|succinate)\s*$",
    re.IGNORECASE
)


def _clean_for_chembl(name: str) -> str:
    """Light cleaning for ChEMBL API lookup (already normalized by M3)."""
    if not name:
        return name
    cleaned = _CLEAN_RE.sub("", name).strip()
    return cleaned if len(cleaned) >= _MIN_NAME_LEN else name


# ──────────────────────────────────────────────────────────────────────────────
# ChEMBL API helpers (all async)
# ──────────────────────────────────────────────────────────────────────────────

async def _chembl_get(session: aiohttp.ClientSession, url: str, params: dict) -> dict | None:
    """GET a ChEMBL API endpoint with retry on 5xx/timeout. Returns JSON or None."""
    for attempt in range(CHEMBL_RETRY_MAX + 1):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=CHEMBL_TIMEOUT)) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None)
                if resp.status == 429:
                    wait = CHEMBL_RETRY_BACKOFF ** (attempt + 1)
                    logging.debug(f"ChEMBL 429, backing off {wait}s")
                    await asyncio.sleep(wait)
                    continue
                if resp.status >= 500:
                    wait = CHEMBL_RETRY_BACKOFF ** (attempt + 1)
                    await asyncio.sleep(wait)
                    continue
                return None  # 404, 400, etc.
        except asyncio.TimeoutError:
            await asyncio.sleep(CHEMBL_RETRY_BACKOFF ** (attempt + 1))
        except aiohttp.ClientError as e:
            logging.debug(f"ChEMBL client error: {e}")
            await asyncio.sleep(CHEMBL_RETRY_BACKOFF ** (attempt + 1))
    return None


async def _get_chembl_id(drug_name: str, session: aiohttp.ClientSession, sem: asyncio.Semaphore) -> str | None:
    """
    Look up a ChEMBL molecule by preferred name (exact, case-insensitive).
    Falls back to synonym search if exact match fails.
    Returns parent_chembl_id or None.
    """
    async with sem:
        # Primary: pref_name exact match
        data = await _chembl_get(
            session,
            f"{CHEMBL_API_BASE}/molecule",
            {"pref_name__iexact": drug_name, "format": "json", "limit": 1}
        )
        if data and data.get("molecules"):
            mol = data["molecules"][0]
            return mol.get("molecule_hierarchy", {}).get("parent_chembl_id") or mol.get("molecule_chembl_id")

        # Fallback: search in molecule synonyms
        data = await _chembl_get(
            session,
            f"{CHEMBL_API_BASE}/molecule",
            {"molecule_synonyms__synonym__iexact": drug_name, "format": "json", "limit": 1}
        )
        if data and data.get("molecules"):
            mol = data["molecules"][0]
            return mol.get("molecule_hierarchy", {}).get("parent_chembl_id") or mol.get("molecule_chembl_id")

    return None


async def _get_mechanisms(parent_chembl_id: str, session: aiohttp.ClientSession, sem: asyncio.Semaphore) -> list[dict]:
    """
    Retrieve mechanisms of action for a parent molecule ChEMBL ID.
    Returns list of mechanism dicts (may be empty).
    """
    async with sem:
        data = await _chembl_get(
            session,
            f"{CHEMBL_API_BASE}/mechanism",
            {"parent_molecule_chembl_id": parent_chembl_id, "format": "json", "limit": 50}
        )
    if data and data.get("mechanisms"):
        return data["mechanisms"]
    return []


def _build_entry(mechanisms: list[dict]) -> dict | None:
    """
    Convert a list of ChEMBL mechanism records into a resolved entry dict.
    Picks the first mechanism with disease_efficacy=1 (approved indication),
    falling back to the first mechanism overall.
    """
    if not mechanisms:
        return None

    # Prefer mechanisms that have disease_efficacy flag set
    preferred = [m for m in mechanisms if m.get("disease_efficacy") == 1] or mechanisms

    # Deduplicate by mechanism_of_action string
    seen_moa = set()
    unique_mechs = []
    for m in preferred:
        moa = m.get("mechanism_of_action") or ""
        if moa and moa not in seen_moa:
            seen_moa.add(moa)
            unique_mechs.append(m)

    if not unique_mechs:
        return None

    primary_moa = unique_mechs[0].get("mechanism_of_action", "")
    target_ids = [m.get("target_chembl_id") for m in unique_mechs if m.get("target_chembl_id")]
    all_moas = [m.get("mechanism_of_action", "") for m in unique_mechs if m.get("mechanism_of_action")]

    return {
        "target_primary": primary_moa,
        "targets_raw": "|".join(dict.fromkeys(all_moas)),  # deduplicated, insertion-order
        "target_chembl_ids": "|".join(dict.fromkeys(filter(None, target_ids))),
        "target_source": "chembl",
        "target_confidence": "high",
        "reference_db_version": _DB_VERSION,
    }


async def _resolve_single(
    drug_norm: str,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore
) -> dict | None:
    """
    Resolve one drug name → ChEMBL target entry.
    Checks module-level cache first; populates it on miss.
    """
    global _chembl_cache

    if drug_norm in _chembl_cache:
        return _chembl_cache[drug_norm]

    cleaned = _clean_for_chembl(drug_norm)
    if not cleaned or len(cleaned) < _MIN_NAME_LEN:
        _chembl_cache[drug_norm] = None
        return None

    chembl_id = await _get_chembl_id(cleaned, session, sem)

    # If cleaned name failed and it differs from original, try original
    if not chembl_id and cleaned != drug_norm:
        chembl_id = await _get_chembl_id(drug_norm, session, sem)

    if not chembl_id:
        _chembl_cache[drug_norm] = None
        return None

    mechanisms = await _get_mechanisms(chembl_id, session, sem)
    entry = _build_entry(mechanisms)
    _chembl_cache[drug_norm] = entry
    return entry


# ──────────────────────────────────────────────────────────────────────────────
# Public API called from main.py
# ──────────────────────────────────────────────────────────────────────────────

async def resolve_chunk_async(
    df_chunk: pd.DataFrame,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    chunk_index: int,
    pair_id_start: int
) -> tuple[int, list[dict]]:
    """
    Resolve all drug names in a chunk against ChEMBL API, then apply
    results back to the DataFrame via vectorized map. Returns
    (chunk_index, list_of_row_dicts) suitable for DatasetBuilder.process_chunk().
    """
    # 1. Collect unique unresolved drug names
    drug_col = df_chunk["drug_name_norm"] if "drug_name_norm" in df_chunk.columns else pd.Series(dtype=str)
    unique_drugs = [d for d in drug_col.dropna().unique() if d not in _chembl_cache and isinstance(d, str)]

    # 2. Fan out concurrent ChEMBL lookups for cache misses
    if unique_drugs:
        tasks = [_resolve_single(d, session, sem) for d in unique_drugs]
        await asyncio.gather(*tasks)
        logging.debug(f"M4 chunk {chunk_index}: resolved {len(unique_drugs)} unique drugs via ChEMBL")

    # 3. Vectorized assignment from cache
    df = df_chunk.copy()

    def _get_field(drug_norm, field):
        entry = _chembl_cache.get(drug_norm) if isinstance(drug_norm, str) else None
        return entry.get(field) if entry else None

    df["target_primary"]         = drug_col.map(lambda d: _get_field(d, "target_primary"))
    df["targets_raw"]            = drug_col.map(lambda d: _get_field(d, "targets_raw"))
    df["target_source"]          = drug_col.map(lambda d: _get_field(d, "target_source") if _chembl_cache.get(d) else "none")
    df["target_confidence"]      = drug_col.map(lambda d: _get_field(d, "target_confidence") if _chembl_cache.get(d) else "none")
    df["target_evidence_text"]   = None  # No free-text evidence; MoA is in target_primary
    df["target_evidence_source"] = drug_col.map(lambda d: "chembl" if _chembl_cache.get(d) else None)
    df["reference_db_version"]   = drug_col.map(lambda d: _get_field(d, "reference_db_version"))

    # 4. Assign pair IDs (sequential, vectorized)
    df["pair_id"] = range(pair_id_start, pair_id_start + len(df))

    resolved = int(df["target_primary"].notna().sum())
    logging.info(
        f"M4 chunk {chunk_index}: {resolved}/{len(df)} rows resolved "
        f"({resolved/max(len(df),1)*100:.1f}%)"
    )

    return chunk_index, df.to_dict("records")


def get_cache_stats() -> dict:
    """Return summary stats on the in-process ChEMBL cache."""
    total = len(_chembl_cache)
    resolved = sum(1 for v in _chembl_cache.values() if v is not None)
    return {"cache_total": total, "cache_resolved": resolved, "cache_miss": total - resolved}
