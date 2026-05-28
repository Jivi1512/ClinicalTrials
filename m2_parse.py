import re
import asyncio
import logging
import pandas as pd
from config import (
    CHUNK_SIZE, INTERVENTION_TYPES, PHASE_ORDER,
    ANCHOR_KEYWORDS, NLP_WINDOW_SIZE
)

_NLP_PATTERN=re.compile(
    r"[^.!?]*(?:" + "|".join(re.escape(k) for k in ANCHOR_KEYWORDS) + r")[^.!?]*[.!?]",
    re.IGNORECASE
)

def _safe_get(d, *keys, default=None):
    for k in keys:
        if not isinstance(d, dict):
            return default
        d=d.get(k, default)
        if d is None:
            return default
    return d

def _highest_phase(phases_list):
    if not phases_list:
        return None
    best=None
    best_rank=-1
    for p in phases_list:
        rank=PHASE_ORDER.get(p, -1)
        if rank>best_rank:
            best_rank=rank
            best=p
    return best

def _pipe(lst):
    if not lst:
        return None
    cleaned=[str(x).strip() for x in lst if x]
    return "|".join(cleaned) if cleaned else None

def _extract_nlp_candidates(text_brief, text_detailed):
    candidates=[]
    sentence_count=0
    for text in [text_brief, text_detailed]:
        if not text:
            continue
        sentences=re.split(r"(?<=[.!?])\s+", text)
        for i, sent in enumerate(sentences):
            if _NLP_PATTERN.search(sent):
                window_start=max(0, i-NLP_WINDOW_SIZE)
                window_end=min(len(sentences), i+NLP_WINDOW_SIZE+1)
                window=" ".join(sentences[window_start:window_end])
                candidates.append(window)
                sentence_count+=1
    if candidates:
        return " ... ".join(candidates[:5])
    return None

def _parse_study(study):
    ps=study.get("protocolSection", {})
    ds=study.get("derivedSection", {})

    id_mod=ps.get("identificationModule", {})
    status_mod=ps.get("statusModule", {})
    desc_mod=ps.get("descriptionModule", {})
    cond_mod=ps.get("conditionsModule", {})
    design_mod=ps.get("designModule", {})
    arms_mod=ps.get("armsInterventionsModule", {})
    sponsor_mod=ps.get("sponsorCollaboratorsModule", {})
    elig_mod=ps.get("eligibilityModule", {})

    cond_mesh_raw=ds.get("conditionBrowseModule") or ds.get("conditionMeshList") or ds.get("conditionMeshes") or {}
    intr_mesh_raw=ds.get("interventionBrowseModule") or ds.get("interventionMeshList") or ds.get("interventionMeshes") or {}
    cond_mesh=cond_mesh_raw.get("meshes") or cond_mesh_raw.get("mesh") or cond_mesh_raw.get("conditionMesh") or []
    intr_mesh=intr_mesh_raw.get("meshes") or intr_mesh_raw.get("mesh") or intr_mesh_raw.get("interventionMesh") or []

    nct_id=id_mod.get("nctId")
    brief_title=id_mod.get("briefTitle")
    overall_status=status_mod.get("overallStatus")
    phases_raw=design_mod.get("phases", [])
    phase=_highest_phase(phases_raw)
    conditions=_pipe(cond_mod.get("conditions", []))
    mesh_condition=_pipe([m.get("term") for m in cond_mesh])
    mesh_intervention=_pipe([m.get("term") for m in intr_mesh])
    brief_summary=desc_mod.get("briefSummary") or desc_mod.get("brief_summary")
    detailed_description=desc_mod.get("detailedDescription") or desc_mod.get("detailed_description")

    collabs=sponsor_mod.get("collaborators", [])
    sponsor_lead=_safe_get(sponsor_mod, "leadSponsor", "name")
    sponsor_collabs=_pipe([c.get("name") for c in collabs])

    start_date_struct=status_mod.get("startDateStruct") or {}
    completion_date_struct=status_mod.get("primaryCompletionDateStruct") or {}
    start_date=start_date_struct.get("date")
    completion_date=completion_date_struct.get("date")

    enroll_info=design_mod.get("enrollmentInfo", {})
    enrollment_count=enroll_info.get("count")
    enrollment_type=enroll_info.get("type")

    eligibility=elig_mod.get("eligibilityCriteria")

    nlp_candidate=_extract_nlp_candidates(brief_summary, detailed_description)

    base={
        "nct_id": nct_id,
        "brief_title": brief_title,
        "overall_status": overall_status,
        "phase": phase,
        "conditions": conditions,
        "mesh_terms_condition": mesh_condition,
        "mesh_terms_intervention": mesh_intervention,
        "brief_summary": brief_summary,
        "detailed_description": detailed_description,
        "nlp_candidate_text": nlp_candidate,
        "sponsor_lead": sponsor_lead,
        "sponsor_collaborators": sponsor_collabs,
        "start_date": start_date,
        "completion_date": completion_date,
        "enrollment_count": enrollment_count,
        "enrollment_type": enrollment_type,
        "eligibility_criteria": eligibility,
        "eligibility_parsed": False
    }

    interventions=arms_mod.get("interventions", [])
    rows=[]
    for intr in interventions:
        intr_type=intr.get("type", "")
        if intr_type not in INTERVENTION_TYPES:
            continue
        row=dict(base)
        row["drug_name_raw"]=intr.get("name")
        row["intervention_type"]=intr_type
        row["intervention_description"]=intr.get("description")
        rows.append(row)

    return rows

async def parse_pages(page_queue):
    buffer=[]
    chunk_index=0
    total_rows=0

    while True:
        response_data=await page_queue.get()
        page_queue.task_done()

        if response_data is None:
            if buffer:
                df=pd.DataFrame(buffer)
                logging.info(f"M2: yielding final chunk {chunk_index} with {len(df)} rows")
                yield df, chunk_index
                chunk_index+=1
                buffer=[]
            break

        studies=response_data.get("studies", [])
        for study in studies:
            rows=_parse_study(study)
            buffer.extend(rows)
            total_rows+=len(rows)

            while len(buffer)>=CHUNK_SIZE:
                chunk_rows=buffer[:CHUNK_SIZE]
                buffer=buffer[CHUNK_SIZE:]
                df=pd.DataFrame(chunk_rows)
                logging.info(f"M2: yielding chunk {chunk_index} with {len(df)} rows")
                yield df, chunk_index
                chunk_index+=1

    logging.info(f"M2 complete: {total_rows} total rows parsed across {chunk_index} chunks")