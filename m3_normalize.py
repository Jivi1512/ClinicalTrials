import re
import logging
import pandas as pd
from config import SALT_SUFFIXES, DOSAGE_PATTERN, ROUTE_PATTERN

_SALT_RE=re.compile("|".join(SALT_SUFFIXES), re.IGNORECASE)
_DOSAGE_RE=re.compile(DOSAGE_PATTERN, re.IGNORECASE)
_ROUTE_RE=re.compile(ROUTE_PATTERN, re.IGNORECASE)
_WHITESPACE_RE=re.compile(r"\s+")

def _build_brand_map():
    try:
        from lookups.lookup_loader import build_brand_to_generic
        return build_brand_to_generic()
    except Exception as e:
        logging.warning(f"Could not load brand map: {e}")
        return {}

_BRAND_MAP=_build_brand_map()

def normalize_chunk(df):
    if "drug_name_raw" not in df.columns:
        df["drug_name_norm"]=None
        df["is_duplicate"]=False
        return df

    series=df["drug_name_raw"].astype(str).str.strip()
    series=series.str.lower()
    series=series.str.replace(_DOSAGE_RE, "", regex=True)
    series=series.str.replace(_ROUTE_RE, "", regex=True)
    series=series.str.replace(_SALT_RE, "", regex=True)
    series=series.str.replace(_WHITESPACE_RE, " ", regex=True)
    series=series.str.strip()
    series=series.where(series!="nan", other=None)
    series=series.where(series!="", other=None)

    if _BRAND_MAP:
        series=series.map(lambda x: _BRAND_MAP.get(x, x) if x else x)

    df=df.copy()
    df["drug_name_norm"]=series

    df["_dedup_key"]=df["nct_id"].astype(str)+"|||"+df["drug_name_norm"].astype(str)
    df["is_duplicate"]=df.duplicated(subset=["_dedup_key"], keep="first")
    df=df.drop(columns=["_dedup_key"])

    logging.debug(f"M3: normalized {len(df)} rows, {df['is_duplicate'].sum()} intra-chunk duplicates")
    return df
