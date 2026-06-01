import sys
import asyncio
import logging
import time
import uuid
import os
import aiohttp
from datetime import datetime
from config import CHUNK_SIZE, QUEUE_MAX_PAGES, OUTPUT_CSV_PATH, REVIEW_QUEUE_PATH, DUPLICATES_LOG_PATH, CHEMBL_SEMAPHORE
from state import read_api_checkpoint, allocate_pair_range
from m1_fetch import fetch_all_pages
from m2_parse import parse_pages
from m3_normalize import normalize_chunk
from m4_resolve import resolve_chunk_async, get_cache_stats
from m5_build import DatasetBuilder
from m6_report import generate_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log")
    ]
)
logging.getLogger("aiohttp.client").setLevel(logging.ERROR)

async def run_pipeline():
    start_time=time.monotonic()
    pipeline_run_id=str(uuid.uuid4())
    logging.info(f"Pipeline started: run_id={pipeline_run_id}")

    checkpoint=read_api_checkpoint()
    if checkpoint.get("pages_fetched", 0)==0:
        for path in [OUTPUT_CSV_PATH, REVIEW_QUEUE_PATH, DUPLICATES_LOG_PATH]:
            if os.path.exists(path):
                os.remove(path)
                logging.info(f"Cleared stale output file: {path}")

    # Shared ChEMBL HTTP session for all resolve calls
    connector=aiohttp.TCPConnector(limit=CHEMBL_SEMAPHORE * 2)
    chembl_sem=asyncio.Semaphore(CHEMBL_SEMAPHORE)

    page_queue=asyncio.Queue(maxsize=QUEUE_MAX_PAGES)
    builder=DatasetBuilder(pipeline_run_id)

    fetch_task=asyncio.create_task(fetch_all_pages(page_queue))

    async with aiohttp.ClientSession(connector=connector) as chembl_session:
        # Stream: parse → normalize → resolve → write, one chunk at a time
        async for df_chunk, chunk_index in parse_pages(page_queue):
            df_chunk=normalize_chunk(df_chunk)
            pair_id_start=allocate_pair_range(len(df_chunk))

            returned_index, records=await resolve_chunk_async(
                df_chunk, chembl_session, chembl_sem, chunk_index, pair_id_start
            )
            builder.process_chunk(returned_index, records)

    await fetch_task

    stats=builder.get_stats()
    cache_stats=get_cache_stats()
    logging.info(
        f"M5 complete: {stats['total_rows']} rows written. "
        f"ChEMBL cache: {cache_stats['cache_resolved']}/{cache_stats['cache_total']} resolved "
        f"({cache_stats['cache_miss']} misses)"
    )

    checkpoint=read_api_checkpoint()
    pages_fetched=checkpoint.get("pages_fetched", 0)
    total_count=checkpoint.get("total_count_expected", 0)

    runtime=time.monotonic()-start_time
    exit_code=generate_report(stats, pages_fetched, total_count, pipeline_run_id, runtime)

    logging.info(f"Pipeline complete in {runtime:.1f}s ({runtime/60:.1f} min). Exit code: {exit_code}")
    return exit_code

def main():
    exit_code=asyncio.run(run_pipeline())
    sys.exit(exit_code)

if __name__=="__main__":
    main()