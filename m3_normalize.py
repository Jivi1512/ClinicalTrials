import re
import logging
import pandas as pd
from config import SALT_SUFFIXES, DOSAGE_PATTERN, ROUTE_PATTERN

_SALT_RE=re.compile("|".join(SALT_SUFFIXES), re.IGNORECASE)
_DOSAGE_RE=re.compile(DOSAGE_PATTERN, re.IGNORECASE)
_ROUTE_RE=re.compile(ROUTE_PATTERN, re.IGNORECASE)
_WHITESPACE_RE=re.compile(r"\s+")

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

    df=df.copy()
    df["drug_name_norm"]=series

    df["_dedup_key"]=df["nct_id"].astype(str)+"|||"+df["drug_name_norm"].astype(str)
    df["is_duplicate"]=df.duplicated(subset=["_dedup_key"], keep="first")
    df=df.drop(columns=["_dedup_key"])

    logging.debug(f"M3: normalized {len(df)} rows, {df['is_duplicate'].sum()} intra-chunk duplicates")
    return df
