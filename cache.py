import os
import time
import hashlib
import logging
from datetime import datetime, timedelta
from config import CACHE_DIR, CACHE_TTL_DAYS
from utils import atomic_write_gzip, atomic_read_gzip

os.makedirs(CACHE_DIR, exist_ok=True)

def _token_to_hash(page_token):
    key=(page_token or "page_0").encode("utf-8")
    return hashlib.sha256(key).hexdigest()[:24]

def _cache_path(token_hash):
    return os.path.join(CACHE_DIR, f"{token_hash}.json.gz")

def write_page_cache(page_token, raw_response):
    token_hash=_token_to_hash(page_token)
    path=_cache_path(token_hash)
    records=raw_response.get("studies", [])
    payload={
        "response": raw_response,
        "record_count": len(records),
        "cached_at": datetime.utcnow().isoformat()
    }
    atomic_write_gzip(path, payload)
    return token_hash

def read_page_cache(page_token):
    token_hash=_token_to_hash(page_token)
    path=_cache_path(token_hash)
    if not os.path.exists(path):
        return None
    try:
        payload=atomic_read_gzip(path)
        return payload
    except Exception as e:
        logging.warning(f"Cache read failed for {token_hash}: {e}")
        return None

def validate_page_cache(page_token):
    payload=read_page_cache(page_token)
    if payload is None:
        return False
    records=payload.get("response", {}).get("studies", [])
    expected_count=payload.get("record_count", -1)
    if len(records)!=expected_count:
        logging.warning(f"Cache integrity fail for token {page_token[:12] if page_token else 'page_0'}: expected {expected_count} got {len(records)}")
        _delete_cache(page_token)
        return False
    return True

def _delete_cache(page_token):
    token_hash=_token_to_hash(page_token)
    path=_cache_path(token_hash)
    if os.path.exists(path):
        os.remove(path)

def should_refetch(page_token, records):
    active_statuses={"RECRUITING", "ACTIVE_NOT_RECRUITING"}
    for study in records:
        status=study.get("protocolSection", {}).get("statusModule", {}).get("overallStatus", "")
        if status in active_statuses:
            return True
    token_hash=_token_to_hash(page_token)
    path=_cache_path(token_hash)
    if not os.path.exists(path):
        return True
    mtime=os.path.getmtime(path)
    age_days=(time.time()-mtime)/86400
    return age_days>CACHE_TTL_DAYS
