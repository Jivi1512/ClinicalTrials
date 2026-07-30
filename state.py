import os
import uuid
import logging
from datetime import datetime
from config import (
    API_CHECKPOINT_PATH, PAIR_CHECKPOINT_PATH,
    PAIR_REGISTRY_PATH, STATE_DIR)
from utils import atomic_write_json, atomic_read_json

os.makedirs(STATE_DIR, exist_ok=True)

def read_api_checkpoint():
    default={
        "page_token": None,
        "pages_fetched": 0,
        "total_count_expected": 0,
        "last_updated": None}
    data=atomic_read_json(API_CHECKPOINT_PATH, default)
    if not isinstance(data, dict):
        logging.warning("api_checkpoint.json corrupt, resetting")
        return default
    return data

def write_api_checkpoint(data):
    atomic_write_json(API_CHECKPOINT_PATH, data)

def read_pair_checkpoint():
    default={
        "last_pair_id_exported": -1,
        "pipeline_run_id": str(uuid.uuid4()),
        "last_updated": None}
    data=atomic_read_json(PAIR_CHECKPOINT_PATH, default)
    if not isinstance(data, dict):
        logging.warning("pair_checkpoint.json corrupt, resetting")
        return default
    return data

def write_pair_checkpoint(last_pair_id, pipeline_run_id):
    atomic_write_json(PAIR_CHECKPOINT_PATH, {
        "last_pair_id_exported": last_pair_id,
        "pipeline_run_id": pipeline_run_id,
        "last_updated": datetime.utcnow().isoformat()})

def read_pair_registry():
    default={"next_available_pair_id": 0, "allocated_ranges": []}
    data=atomic_read_json(PAIR_REGISTRY_PATH, default)
    if not isinstance(data, dict):
        logging.warning("pair_registry.json corrupt, resetting")
        return default
    return data

def allocate_pair_range(n_pairs):
    registry=read_pair_registry()
    start=registry["next_available_pair_id"]
    end=start+n_pairs
    registry["next_available_pair_id"]=end
    atomic_write_json(PAIR_REGISTRY_PATH, registry)
    return start

def get_pipeline_run_id():
    checkpoint=read_pair_checkpoint()
    return checkpoint.get("pipeline_run_id", str(uuid.uuid4()))