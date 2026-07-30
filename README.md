# Clinical Trials Drug–Target Pipeline

## Overview

Pipeline for extracting, processing, and analyzing drug–target relationships from the ClinicalTrials.gov v2 API.
---

## Getting Started

### Setup

Install dependencies:

```bash
pip install -r requirements.txt
```

### Run Pipeline

```bash
python main.py
```
Other files are linked to main.py

**RUN ONLY 'main.py'**

---

## Important Notes

* Use `ReviewClinicalTrialsDataset.ipynb` to explore and inspect the dataset.
* Final datasets are written to the **output folders**.
* The pipeline uses **checkpointing** stored in `state/` and `cache/` directories.
* If execution is interrupted (error/network/manual stop), progress is preserved and resumes from the last checkpoint.
* Data is written **only after the final stage completes**.
* Expected runtime: **~1 to 1.5 hours**

---

## Pipeline Architecture

### `main.py`

* Entry point of the pipeline
* Orchestrates the full workflow
* Initializes worker processes
* Connects asynchronous queues
* Executes modules sequentially:

  ```
  fetch:- parse:- normalize:- resolve :- build :- report
  ```

---

### `config.py`

* Central configuration module
* Contains:

  * API endpoints
  * File paths
  * Regex patterns
  * Field definitions
  * Enumerations
  * Threshold values
* No executable logic

---

### `m1_fetch.py`: Data Fetching

* Retrieves data from ClinicalTrials.gov API (paginated)
* Implements:

  * Token bucket rate limiting
  * Retry logic
  * Response caching
  * Checkpointing for resumption

---

### `m2_parse.py`: Parsing

* Parses raw JSON responses
* Extracts relevant fields from nested structures
* Filters:

  * DRUG
  * BIOLOGICAL interventions
  * COMBINATION_PRODUCT
  * DIETARY SUPPLEMENT
* Outputs chunked, flat DataFrames

---

### `m3_normalize.py`: Drug Normalization

* Cleans drug names:

  * Removes dosage
  * Removes route of administration
  * Removes salt forms
  * Trims whitespace
* Maps brand names TO generic names
* Identifies duplicates within chunks

---

### `m4_resolve.py`: Target Resolution

* Maps drugs to biological targets using hierarchy:

  1. DrugBank (exact match)
  2. ChEMBL (exact match)
  3. MeSH term matching
  4. NLP-based extraction from descriptions
* Assigns confidence score:

  * high / medium / low / none

---

### `m5_build.py`: Output Construction

* Aggregates all processed records
* Removes cross-chunk duplicates
* Validates fields
* Outputs:

  * Main CSV dataset
  * Unresolved entries (review queue)
  * Duplicate logs

---

### `m6_report.py`: Reporting

* Generates pipeline quality metrics:

  * Null rates
  * Duplication rates
  * Coverage vs API counts
* Performs real-time validation
* Outputs JSON report
* Returns exit status (success/failure)

---

### `state.py`: State Management

* Manages checkpoint persistence
* Tracks:

  * API pagination progress
  * Unique pair IDs
* Ensures safe pipeline resumption and global ID consistency

---

### `utils.py`: Utilities

* Safe file I/O operations
* Uses temporary file + rename strategy
* Prevents corruption during writes

---

### `requirements.txt`

* Lists all dependencies with pinned versions:

  * aiohttp
  * aiofiles
  * pandas
  * numpy
  * requests

---

### `pipeline.log`

* Execution log file
* Contains:

  * Timestamps
  * Info logs
  * Warnings
  * Errors
* Used for debugging and monitoring

---

## Dataset columns
---
| **S.no** | **Column**                   | **What it contains**                                                                          |
| -------- | ---------------------------- | --------------------------------------------------------------------------------------------- |
| 1        | **pair_id**                  | Unique integer ID for each drug–trial pair in this pipeline run                               |
| 2        | **pipeline_run_id**          | UUID identifying which specific pipeline execution produced this row                          |
| 3        | **extraction_date**          | Date this row was written to the CSV                                                          |
| 4        | **nct_id**                   | The official ClinicalTrials.gov trial identifier (e.g. NCT01234567)                           |
| 5        | **brief_title**              | Short human-readable title of the clinical trial                                              |
| 6        | **overall_status**           | Current trial status: RECRUITING,COMPLETED,TERMINATED,etc.                                 |
| 7        | **phase**                    | Highest clinical trial phase: PHASE1 through PHASE4,or NA                                    |
| 8        | **conditions**               | Disease(s) or condition(s) the trial is studying,pipe-separated                              |
| 9        | **mesh_terms_condition**     | Standardized MeSH vocabulary terms for the condition(s),pipe-separated                       |
| 10       | **mesh_terms_intervention**  | Standardized MeSH vocabulary terms for the intervention(s),pipe-separated                    |
| 11       | **drug_name_raw**            | Intervention name exactly as the sponsor entered it on ClinicalTrials.gov                     |
| 12       | **drug_name_norm**           | Cleaned drug name after stripping salts,dosages,and routes                                  |
| 13       | **intervention_type**        | Category of the intervention: DRUG,BIOLOGICAL,COMBINATION_PRODUCT,or DIETARY_SUPPLEMENT    |
| 14       | **intervention_description** | Free-text description of the intervention as entered by the sponsor (mostly empty)            |
| 15       | **target_primary**           | Single best biological target assigned to the drug                                            |
| 16       | **targets_raw**              | All targets found before selecting the primary,pipe-separated                                |
| 17       | **target_source**            | Which tier resolved the target: drugbank,chembl,mesh,nlp,or none                          |
| 18       | **target_confidence**        | Confidence level of the target assignment: high,medium,low,or none                         |
| 19       | **target_evidence_text**     | The specific text fragment that triggered the target assignment                               |
| 20       | **target_evidence_source**   | Where the evidence text came from: brief_summary,detailed_description,or nlp_candidate_text |
| 21       | **reference_db_version**     | Version tag of the DrugBank or ChEMBL database entry used,if applicable                      |
| 22       | **brief_summary**            | Short paragraph describing the trial's purpose,as written by the sponsor                     |
| 23       | **detailed_description**     | Full-length trial description text,as written by the sponsor                                 |
| 24       | **nlp_candidate_text**       | Sentences extracted from the description that contained target-relevant keywords              |
| 25       | **sponsor_lead**             | Name of the primary organization running the trial                                            |
| 26       | **sponsor_collaborators**    | Names of collaborating organizations,pipe-separated                                          |
| 27       | **start_date**               | Date the trial began or is expected to begin                                                  |
| 28       | **completion_date**          | Expected or actual primary completion date                                                    |
| 29       | **enrollment_count**         | Number of participants enrolled or targeted for enrollment                                    |
| 30       | **enrollment_type**          | Whether enrollment count is ACTUAL (real) or ANTICIPATED (planned)                            |
| 31       | **eligibility_criteria**     | Full inclusion and exclusion criteria text for trial participants                             |
| 32       | **eligibility_parsed**       | Boolean flag; currently always False as eligibility parsing is not yet implemented            |
| 33       | **validation_warnings**      | Pipe-separated flags for any data quality issues detected in this row                         |
| 34       | **is_duplicate**             | True if this drug–trial pair appeared more than once; duplicates are logged separately        |
---
