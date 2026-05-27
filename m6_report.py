import json
import time
import logging
import requests
from datetime import datetime
from config import (
    RUN_REPORT_PATH, NULL_THRESHOLDS, API_BASE_URL,
    SPOT_CHECK_CONDITIONS, REQUEST_TIMEOUT, OUTPUT_CSV_PATH,
    OUTPUT_DIR
)
import os
import pandas as pd

os.makedirs(OUTPUT_DIR, exist_ok=True)

def _spot_check(pipeline_run_id):
    if not os.path.exists(OUTPUT_CSV_PATH):
        logging.warning("M6: output CSV not found for spot check")
        return 0.0, []

    try:
        output_nct_ids=set(pd.read_csv(OUTPUT_CSV_PATH, usecols=["nct_id"])["nct_id"].dropna().astype(str))
    except Exception as e:
        logging.warning(f"M6: could not read output CSV for spot check: {e}")
        return 0.0, []

    passes=0
    failures=[]

    for condition in SPOT_CHECK_CONDITIONS:
        try:
            params={
                "format": "json",
                "pageSize": 10,
                "query.cond": condition,
                "query.term": "AREA[InterventionType]DRUG OR AREA[InterventionType]BIOLOGICAL"
            }
            resp=requests.get(API_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data=resp.json()
            studies=data.get("studies", [])
            condition_ncts=[
                s.get("protocolSection", {}).get("identificationModule", {}).get("nctId")
                for s in studies
            ]
            condition_ncts=[n for n in condition_ncts if n]

            if not condition_ncts:
                passes+=1
                continue

            found=sum(1 for n in condition_ncts if n in output_nct_ids)
            rate=found/len(condition_ncts)
            if rate>=0.90:
                passes+=1
            else:
                missing=[n for n in condition_ncts if n not in output_nct_ids]
                failures.append({"condition": condition, "pass_rate": rate, "missing_ncts": missing})

        except Exception as e:
            logging.warning(f"M6: spot check failed for condition '{condition}': {e}")

    total=len(SPOT_CHECK_CONDITIONS)
    pass_rate=passes/total if total>0 else 0.0
    return pass_rate, failures

def generate_report(stats, pages_fetched, total_count_expected, pipeline_run_id, runtime_seconds):
    total=stats.get("total_rows", 0)
    null_counts=stats.get("null_counts", {})
    threshold_breaches=[]
    null_rates={}

    for col, threshold in NULL_THRESHOLDS.items():
        null_count=null_counts.get(col, 0)
        rate=(null_count/total) if total>0 else 0.0
        null_rates[col]=round(rate, 4)
        if rate>threshold:
            threshold_breaches.append({
                "column": col,
                "null_rate": round(rate, 4),
                "threshold": threshold
            })

    distinct_pairs=total
    try:
        if os.path.exists(OUTPUT_CSV_PATH):
            df_check=pd.read_csv(OUTPUT_CSV_PATH, usecols=["nct_id", "drug_name_norm"])
            distinct_pairs=df_check.dropna().drop_duplicates().shape[0]
    except Exception:
        distinct_pairs=total

    duplicate_rate=1.0-(distinct_pairs/total) if total>0 else 0.0

    coverage_rate=0.0
    if total_count_expected>0 and pages_fetched>0:
        avg_pairs_per_record=3.0
        coverage_rate=min(1.0, total/(total_count_expected*avg_pairs_per_record))

    logging.info("M6: running spot checks...")
    spot_pass_rate, spot_failures=_spot_check(pipeline_run_id)

    report={
        "pipeline_run_id": pipeline_run_id,
        "extraction_date": str(datetime.utcnow().date()),
        "total_rows": total,
        "distinct_pairs": distinct_pairs,
        "duplicate_rate": round(duplicate_rate, 4),
        "null_rates": null_rates,
        "threshold_breaches": threshold_breaches,
        "target_source_distribution": stats.get("target_source_dist", {}),
        "phase_distribution": stats.get("phase_dist", {}),
        "status_distribution": stats.get("status_dist", {}),
        "api_pages_fetched": pages_fetched,
        "total_count_expected": total_count_expected,
        "coverage_rate": round(coverage_rate, 4),
        "spot_check_pass_rate": round(spot_pass_rate, 4),
        "spot_check_failures": spot_failures,
        "validation_warnings_count": stats.get("validation_warnings_count", 0),
        "runtime_seconds": round(runtime_seconds, 2)
    }

    with open(RUN_REPORT_PATH, "w") as f:
        json.dump(report, f, indent=2)

    logging.info(f"M6: report written to {RUN_REPORT_PATH}")

    if threshold_breaches:
        logging.error(f"M6: NULL threshold breaches: {threshold_breaches}")
        return 1
    if spot_pass_rate<0.90:
        logging.error(f"M6: spot check pass rate {spot_pass_rate:.2%} below 90%")
        return 2
    if coverage_rate<0.95 and total_count_expected>0:
        logging.warning(f"M6: coverage rate {coverage_rate:.2%} below 95%")
        return 3
    logging.info("M6: all checks passed")
    return 0
