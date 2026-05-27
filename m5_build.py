import os
import logging
import pandas as pd
from datetime import date
from config import (
    OUTPUT_CSV_PATH, REVIEW_QUEUE_PATH, DUPLICATES_LOG_PATH,
    TRIAL_STATUS_ENUM, ALL_OUTPUT_COLUMNS, OUTPUT_DIR
)
from state import write_pair_checkpoint

os.makedirs(OUTPUT_DIR, exist_ok=True)

REVIEW_COLUMNS=[
    "pair_id", "nct_id", "drug_name_raw", "drug_name_norm",
    "nlp_candidate_text", "mesh_terms_intervention"
]

class DatasetBuilder:
    def __init__(self, pipeline_run_id):
        self._run_id=pipeline_run_id
        self._seen_pairs=set()
        self._stats={
            "total_rows": 0,
            "null_counts": {col: 0 for col in ALL_OUTPUT_COLUMNS},
            "target_source_dist": {"drugbank": 0, "chembl": 0, "drugbank+chembl": 0, "mesh": 0, "nlp": 0, "none": 0},
            "phase_dist": {},
            "status_dist": {},
            "validation_warnings_count": 0
        }
        self._main_header_written=os.path.exists(OUTPUT_CSV_PATH)
        self._review_header_written=os.path.exists(REVIEW_QUEUE_PATH)
        self._dupes_header_written=os.path.exists(DUPLICATES_LOG_PATH)
        self._last_pair_id=-1

    def _validate_row(self, row):
        warnings=[]
        status=row.get("overall_status")
        if status and status not in TRIAL_STATUS_ENUM:
            warnings.append(f"unexpected_status:{status}")
        return "|".join(warnings) if warnings else None

    def _ensure_columns(self, df):
        today=str(date.today())
        df["pipeline_run_id"]=self._run_id
        df["extraction_date"]=today
        for col in ALL_OUTPUT_COLUMNS:
            if col not in df.columns:
                df[col]=None
        return df[ALL_OUTPUT_COLUMNS]

    def process_chunk(self, chunk_index, records):
        if not records:
            logging.warning(f"M5: chunk {chunk_index} empty, skipping")
            return

        df=pd.DataFrame(records)
        df=self._ensure_columns(df)

        main_rows=[]
        review_rows=[]
        dupe_rows=[]

        for _, row in df.iterrows():
            nct=str(row.get("nct_id") or "")
            norm=str(row.get("drug_name_norm") or "")
            pair_key=nct+"|||"+norm

            warning=self._validate_row(row)
            row=row.copy()
            row["validation_warnings"]=warning
            if warning:
                self._stats["validation_warnings_count"]+=1

            if pair_key in self._seen_pairs or row.get("is_duplicate"):
                row["is_duplicate"]=True
                dupe_rows.append(row)
                continue

            self._seen_pairs.add(pair_key)
            row["is_duplicate"]=False

            if not row.get("target_primary"):
                review_rows.append(row)

            main_rows.append(row)

            src=str(row.get("target_source") or "none")
            if src in self._stats["target_source_dist"]:
                self._stats["target_source_dist"][src]+=1

            ph=str(row.get("phase") or "null")
            self._stats["phase_dist"][ph]=self._stats["phase_dist"].get(ph, 0)+1

            st=str(row.get("overall_status") or "null")
            self._stats["status_dist"][st]=self._stats["status_dist"].get(st, 0)+1

            for col in ALL_OUTPUT_COLUMNS:
                val=row.get(col)
                if val is None or (isinstance(val, float) and pd.isna(val)) or val=="":
                    self._stats["null_counts"][col]+=1

            pid=row.get("pair_id")
            if pid is not None and int(pid)>self._last_pair_id:
                self._last_pair_id=int(pid)

        self._stats["total_rows"]+=len(main_rows)

        if main_rows:
            main_df=pd.DataFrame(main_rows)[ALL_OUTPUT_COLUMNS]
            main_df.to_csv(
                OUTPUT_CSV_PATH,
                mode="a",
                header=not self._main_header_written,
                index=False
            )
            self._main_header_written=True
            write_pair_checkpoint(self._last_pair_id, self._run_id)
            logging.info(f"M5: wrote {len(main_rows)} rows from chunk {chunk_index}")

        if review_rows:
            review_df=pd.DataFrame(review_rows)
            for col in REVIEW_COLUMNS:
                if col not in review_df.columns:
                    review_df[col]=None
            review_df[REVIEW_COLUMNS].to_csv(
                REVIEW_QUEUE_PATH,
                mode="a",
                header=not self._review_header_written,
                index=False
            )
            self._review_header_written=True

        if dupe_rows:
            dupe_df=pd.DataFrame(dupe_rows)[ALL_OUTPUT_COLUMNS]
            dupe_df.to_csv(
                DUPLICATES_LOG_PATH,
                mode="a",
                header=not self._dupes_header_written,
                index=False
            )
            self._dupes_header_written=True

    def get_stats(self):
        return self._stats
