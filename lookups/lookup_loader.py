import os
import sqlite3
import logging
from config import DRUGBANK_DB_PATH, CHEMBL_DB_PATH

def _normalize_key(name):
    if not name:
        return None
    return name.lower().strip()

def _load_drugbank(path):
    result={}
    if not os.path.exists(path):
        logging.warning(f"DrugBank DB not found at {path}. PLACEHOLDER: provide real DB.")
        return result
    conn=sqlite3.connect(path)
    conn.row_factory=sqlite3.Row
    c=conn.cursor()
    c.execute("SELECT common_name, targets, synonyms, brand_names, db_version FROM drugs")
    rows=c.fetchall()
    conn.close()
    for row in rows:
        common_key=_normalize_key(row["common_name"])
        if not common_key:
            continue
        entry={
            "targets": [t.strip() for t in (row["targets"] or "").split("|") if t.strip()],
            "source": "drugbank",
            "db_version": row["db_version"] or "unknown",
            "confidence": "high"
        }
        result[common_key]=entry
        for syn in (row["synonyms"] or "").split("|"):
            k=_normalize_key(syn)
            if k and k not in result:
                result[k]=entry
        for brand in (row["brand_names"] or "").split("|"):
            k=_normalize_key(brand)
            if k and k not in result:
                result[k]=entry
    return result

def _load_chembl(path):
    result={}
    if not os.path.exists(path):
        logging.warning(f"ChEMBL DB not found at {path}. PLACEHOLDER: provide real DB.")
        return result
    conn=sqlite3.connect(path)
    conn.row_factory=sqlite3.Row
    c=conn.cursor()
    c.execute("SELECT common_name, targets, synonyms, db_version FROM drugs")
    rows=c.fetchall()
    conn.close()
    for row in rows:
        common_key=_normalize_key(row["common_name"])
        if not common_key:
            continue
        entry={
            "targets": [t.strip() for t in (row["targets"] or "").split("|") if t.strip()],
            "source": "chembl",
            "db_version": row["db_version"] or "unknown",
            "confidence": "high"
        }
        result[common_key]=entry
        for syn in (row["synonyms"] or "").split("|"):
            k=_normalize_key(syn)
            if k and k not in result:
                result[k]=entry
    return result

def load_lookup_dict():
    logging.info("Loading DrugBank lookup...")
    drugbank=_load_drugbank(DRUGBANK_DB_PATH)
    logging.info(f"DrugBank loaded: {len(drugbank)} keys")
    logging.info("Loading ChEMBL lookup...")
    chembl=_load_chembl(CHEMBL_DB_PATH)
    logging.info(f"ChEMBL loaded: {len(chembl)} keys")
    merged={}
    all_keys=set(drugbank.keys())|set(chembl.keys())
    for key in all_keys:
        if key in drugbank and key in chembl:
            db_entry=drugbank[key]
            ch_entry=chembl[key]
            combined_targets=list(dict.fromkeys(db_entry["targets"]+ch_entry["targets"]))
            merged[key]={
                "targets": combined_targets,
                "source": "drugbank+chembl",
                "db_version": f"{db_entry['db_version']}|{ch_entry['db_version']}",
                "confidence": "high"
            }
        elif key in drugbank:
            merged[key]=drugbank[key]
        else:
            merged[key]=chembl[key]
    logging.info(f"Merged lookup dict: {len(merged)} total keys")
    return merged

def build_brand_to_generic():
    brand_map={}
    if not os.path.exists(DRUGBANK_DB_PATH):
        return brand_map
    conn=sqlite3.connect(DRUGBANK_DB_PATH)
    conn.row_factory=sqlite3.Row
    c=conn.cursor()
    c.execute("SELECT common_name, brand_names FROM drugs")
    rows=c.fetchall()
    conn.close()
    for row in rows:
        common=_normalize_key(row["common_name"])
        if not common:
            continue
        for brand in (row["brand_names"] or "").split("|"):
            k=_normalize_key(brand)
            if k:
                brand_map[k]=common
    return brand_map
