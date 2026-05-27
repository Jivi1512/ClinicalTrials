import re
import logging
import pandas as pd
from config import ANCHOR_KEYWORDS, MESH_TARGET_MAP

_lookup_dict=None
_NLP_PATTERNS=None

def worker_init(lookup_dict):
    global _lookup_dict, _NLP_PATTERNS
    _lookup_dict=lookup_dict
    _NLP_PATTERNS={
        "inhibitor": re.compile(r"(\w+(?:\s+\w+){0,3})\s+inhibitor", re.IGNORECASE),
        "agonist": re.compile(r"(\w+(?:\s+\w+){0,3})\s+agonist", re.IGNORECASE),
        "antagonist": re.compile(r"(\w+(?:\s+\w+){0,3})\s+antagonist", re.IGNORECASE),
        "receptor": re.compile(r"(\w+(?:\s+\w+){0,3})\s+receptor", re.IGNORECASE),
        "blocker": re.compile(r"(\w+(?:\s+\w+){0,3})\s+(?:channel\s+)?blocker", re.IGNORECASE),
        "kinase": re.compile(r"(\w+(?:\s+\w+){0,2})\s+kinase", re.IGNORECASE),
        "enzyme": re.compile(r"(\w+(?:\s+\w+){0,2})\s+enzyme", re.IGNORECASE),
        "transporter": re.compile(r"(\w+(?:\s+\w+){0,2})\s+transporter", re.IGNORECASE)
    }

def _lookup_resolve(drug_norm):
    if not drug_norm or _lookup_dict is None:
        return None
    entry=_lookup_dict.get(str(drug_norm).lower().strip())
    return entry

def _mesh_resolve(mesh_terms_intervention):
    if not mesh_terms_intervention:
        return None, None
    terms_lower=str(mesh_terms_intervention).lower()
    for mesh_term, target_class in MESH_TARGET_MAP.items():
        if mesh_term in terms_lower:
            return target_class, mesh_term
    return None, None

def _nlp_resolve(nlp_candidate_text):
    if not nlp_candidate_text or _NLP_PATTERNS is None:
        return None, None
    text=str(nlp_candidate_text)
    for pattern_name, pattern in _NLP_PATTERNS.items():
        match=pattern.search(text)
        if match:
            candidate=match.group(1).strip()
            if len(candidate)>2:
                return candidate, pattern_name
    return None, None

def _determine_evidence_source(row, target_text):
    if not target_text:
        return None
    brief=str(row.get("brief_summary") or "").lower()
    detail=str(row.get("detailed_description") or "").lower()
    if target_text.lower() in brief:
        return "brief_summary"
    if target_text.lower() in detail:
        return "detailed_description"
    return "nlp_candidate_text"

def resolve_chunk_worker(serialized_rows, chunk_index, pair_id_start):
    df=pd.DataFrame(serialized_rows)

    targets_raw_list=[]
    target_primary_list=[]
    target_source_list=[]
    target_confidence_list=[]
    target_evidence_text_list=[]
    target_evidence_source_list=[]
    reference_db_version_list=[]
    pair_id_list=[]

    current_pair_id=pair_id_start

    for _, row in df.iterrows():
        drug_norm=row.get("drug_name_norm")

        entry=_lookup_resolve(drug_norm)
        if entry and entry.get("targets"):
            targets=entry["targets"]
            targets_raw_list.append("|".join(targets))
            target_primary_list.append(targets[0])
            target_source_list.append(entry.get("source", "drugbank"))
            target_confidence_list.append("high")
            target_evidence_text_list.append(None)
            target_evidence_source_list.append(entry.get("source", "drugbank"))
            reference_db_version_list.append(entry.get("db_version"))
            pair_id_list.append(current_pair_id)
            current_pair_id+=1
            continue

        mesh_target, mesh_evidence=_mesh_resolve(row.get("mesh_terms_intervention"))
        if mesh_target:
            targets_raw_list.append(mesh_target)
            target_primary_list.append(mesh_target)
            target_source_list.append("mesh")
            target_confidence_list.append("medium")
            target_evidence_text_list.append(mesh_evidence)
            target_evidence_source_list.append("mesh")
            reference_db_version_list.append(None)
            pair_id_list.append(current_pair_id)
            current_pair_id+=1
            continue

        nlp_target, nlp_pattern=_nlp_resolve(row.get("nlp_candidate_text"))
        if nlp_target:
            ev_src=_determine_evidence_source(row, nlp_target)
            targets_raw_list.append(nlp_target)
            target_primary_list.append(nlp_target)
            target_source_list.append("nlp")
            target_confidence_list.append("low")
            target_evidence_text_list.append(nlp_target)
            target_evidence_source_list.append(ev_src)
            reference_db_version_list.append(None)
            pair_id_list.append(current_pair_id)
            current_pair_id+=1
            continue

        targets_raw_list.append(None)
        target_primary_list.append(None)
        target_source_list.append("none")
        target_confidence_list.append("none")
        target_evidence_text_list.append(None)
        target_evidence_source_list.append(None)
        reference_db_version_list.append(None)
        pair_id_list.append(current_pair_id)
        current_pair_id+=1

    df=df.copy()
    df["pair_id"]=pair_id_list
    df["targets_raw"]=targets_raw_list
    df["target_primary"]=target_primary_list
    df["target_source"]=target_source_list
    df["target_confidence"]=target_confidence_list
    df["target_evidence_text"]=target_evidence_text_list
    df["target_evidence_source"]=target_evidence_source_list
    df["reference_db_version"]=reference_db_version_list

    return chunk_index, df.to_dict("records")
