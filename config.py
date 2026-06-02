import os

BASE_DIR=os.path.dirname(os.path.abspath(__file__))
STATE_DIR=os.path.join(BASE_DIR, "state")
CACHE_DIR=os.path.join(BASE_DIR, "cache", "pages")
OUTPUT_DIR=os.path.join(BASE_DIR, "output")
LOOKUPS_DIR=os.path.join(BASE_DIR, "lookups")

API_CHECKPOINT_PATH=os.path.join(STATE_DIR, "api_checkpoint.json")
PAIR_CHECKPOINT_PATH=os.path.join(STATE_DIR, "pair_checkpoint.json")
PAIR_REGISTRY_PATH=os.path.join(STATE_DIR, "pair_registry.json")

OUTPUT_CSV_PATH=os.path.join(OUTPUT_DIR, "drug_target_dataset.csv")
REVIEW_QUEUE_PATH=os.path.join(OUTPUT_DIR, "review_queue.csv")
RUN_REPORT_PATH=os.path.join(OUTPUT_DIR, "run_report.json")
DUPLICATES_LOG_PATH=os.path.join(OUTPUT_DIR, "duplicates_log.csv")

# ── ClinicalTrials.gov API ────────────────────────────────────────────────────
API_BASE_URL="https://clinicaltrials.gov/api/v2/studies"
PAGE_SIZE=1000          # API maximum; ~249 pages for the target dataset
SEMAPHORE_LIMIT=10      # concurrent fetch slots (pages are sequential, kept for retries)
QUEUE_MAX_PAGES=30      # larger queue so parse never stalls waiting for fetch
CHUNK_SIZE=500          # rows per parse chunk → resolve task
TOKEN_BUCKET_RATE=8     # req/s — slightly above default; CT.gov allows ~10
TOKEN_BUCKET_INIT=1.0   # start full so first request is immediate
RETRY_MAX=4
RETRY_BACKOFF_BASE=2
REQUEST_TIMEOUT=20      # seconds; CT.gov pages usually <5 s; 20 s leaves room for spikes
CACHE_TTL_DAYS=7
PAIR_RANGE_SIZE=10000

# ── ChEMBL REST API ───────────────────────────────────────────────────────────
CHEMBL_API_BASE="https://www.ebi.ac.uk/chembl/api/data"
CHEMBL_SEMAPHORE=25         # concurrent ChEMBL HTTP slots
CHEMBL_TIMEOUT=20           # seconds per request
CHEMBL_RETRY_MAX=3
CHEMBL_RETRY_BACKOFF=2
CHEMBL_BATCH_SIZE=50        # drug names per pref_name__in batch call
CHEMBL_MECH_BATCH_SIZE=100  # ChEMBL IDs per mechanism batch call
CHEMBL_SYNONYM_FALLBACK=300 # max individual synonym lookups per run

# max concurrent chunk-resolve tasks in flight (memory guard: 20×500 rows)
RESOLVE_CONCURRENCY=20

# ── Intervention types ────────────────────────────────────────────────────────
INTERVENTION_TYPES=[
    "DRUG",
    "BIOLOGICAL",
    "COMBINATION_PRODUCT",
    "DIETARY_SUPPLEMENT"
]
FILTER_ADVANCED=(
    "AREA[InterventionType]DRUG OR AREA[InterventionType]BIOLOGICAL "
    "OR AREA[InterventionType]COMBINATION_PRODUCT "
    "OR AREA[InterventionType]DIETARY_SUPPLEMENT"
)
API_FIELDS_PARAM=",".join([
    "NCTId","BriefTitle","OverallStatus","Phase","Condition",
    "InterventionName","InterventionType","BriefSummary","DetailedDescription",
    "ConditionMesh","InterventionMesh",
    "LeadSponsorName","CollaboratorName","StartDate",
    "PrimaryCompletionDate","EnrollmentCount","EnrollmentType","EligibilityCriteria"
])

TRIAL_STATUS_ENUM=[
    "COMPLETED","RECRUITING","ACTIVE_NOT_RECRUITING",
    "ENROLLING_BY_INVITATION","NOT_YET_RECRUITING",
    "SUSPENDED","TERMINATED","WITHDRAWN","UNKNOWN"
]

NULL_THRESHOLDS={
    "nct_id": 0.00,
    "drug_name_norm": 0.02,
    "conditions": 0.01,
    "phase": 0.15,
    "target_primary": 0.30
}

PHASE_ORDER={
    "PHASE4": 5, "PHASE3": 4, "PHASE2": 3,
    "PHASE1": 2, "EARLY_PHASE1": 1, "NA": 0
}

SALT_SUFFIXES=[
    r"\s+hydrochloride", r"\s+sodium", r"\s+potassium",
    r"\s+sulfate", r"\s+phosphate", r"\s+acetate",
    r"\s+tartrate", r"\s+citrate", r"\s+mesylate",
    r"\s+maleate", r"\s+fumarate", r"\s+bromide",
    r"\s+monohydrate", r"\s+dihydrate", r"\s+hemihydrate",
    r"\s+chloride", r"\s+nitrate", r"\s+succinate"
]

DOSAGE_PATTERN=r"\s+\d+[\.,]?\d*\s*(mg|mcg|ml|iu|%)(\s|$).*"
ROUTE_PATTERN=r"\s+(oral|iv|topical|intravenous|subcutaneous|intramuscular|inhaled|nasal)(\s|$).*"

SPOT_CHECK_CONDITIONS=[
    "cancer","diabetes","hypertension","cardiovascular",
    "HIV","asthma","Alzheimer","depression","arthritis","COVID"
]

ALL_OUTPUT_COLUMNS=[
    "pair_id","pipeline_run_id","extraction_date","nct_id",
    "brief_title","overall_status","phase","conditions",
    "mesh_terms_condition","mesh_terms_intervention",
    "drug_name_raw","drug_name_norm","intervention_type",
    "intervention_description","target_primary","targets_raw",
    "target_source","target_confidence","target_evidence_text",
    "target_evidence_source","reference_db_version",
    "brief_summary","detailed_description",
    "sponsor_lead","sponsor_collaborators","start_date",
    "completion_date","enrollment_count","enrollment_type",
    "eligibility_criteria","eligibility_parsed",
    "validation_warnings","is_duplicate","drug_smiles"
]
