"""
lookup_loader.py — Stub module kept for backwards compatibility.

The sample SQLite lookup databases (drugbank.db, chembl.db) are no longer used.
Target resolution is now performed live via the ChEMBL REST API in m4_resolve.py.
"""
import logging

def load_lookup_dict():
    logging.info("lookup_loader: sample lookup disabled; using ChEMBL API instead")
    return {}

def build_brand_to_generic():
    return {}
