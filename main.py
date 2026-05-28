import sys
import asyncio
import logging
import time
import uuid
import os
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count
from datetime import datetime
from config import CHUNK_SIZE, QUEUE_MAX_PAGES, OUTPUT_CSV_PATH, REVIEW_QUEUE_PATH, DUPLICATES_LOG_PATH
from state import read_api_checkpoint, allocate_pair_range
from lookups.lookup_loader import load_lookup_dict
from m1_fetch import fetch_all_pages
from m2_parse import parse_pages
from m3_normalize import normalize_chunk
from m4_resolve import worker_init, resolve_chunk_worker
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

    logging.info("Loading lookup dictionaries...")
    lookup_dict=load_lookup_dict()

    n_workers=max(1, cpu_count()-1)
    logging.info(f"Spawning {n_workers} worker processes")

    executor=ProcessPoolExecutor(
        max_workers=n_workers,
        initializer=worker_init,
        initargs=(lookup_dict,)
    )

    page_queue=asyncio.Queue(maxsize=QUEUE_MAX_PAGES)
    builder=DatasetBuilder(pipeline_run_id)
    pending_futures=[]
    chunk_index_counter=0
    loop=asyncio.get_running_loop()

    fetch_task=asyncio.create_task(fetch_all_pages(page_queue))

    async for df_chunk, chunk_index in parse_pages(page_queue):
        df_chunk=normalize_chunk(df_chunk)
        n_rows=len(df_chunk)
        pair_id_start=allocate_pair_range(n_rows)
        serialized=df_chunk.to_dict("records")
        future=loop.run_in_executor(
            executor,
            resolve_chunk_worker,
            serialized,
            chunk_index,
            pair_id_start
        )
        pending_futures.append(future)
        chunk_index_counter+=1

    await fetch_task
    logging.info(f"M1 fetch complete. Waiting for {len(pending_futures)} M4 worker chunks...")

    results=await asyncio.gather(*pending_futures)
    results_sorted=sorted(results, key=lambda x: x[0])

    for returned_index, records in results_sorted:
        builder.process_chunk(returned_index, records)

    stats=builder.get_stats()
    logging.info(f"M5 complete: {stats['total_rows']} rows written")

    checkpoint=read_api_checkpoint()
    pages_fetched=checkpoint.get("pages_fetched", 0)
    total_count=checkpoint.get("total_count_expected", 0)

    runtime=time.monotonic()-start_time
    exit_code=generate_report(stats, pages_fetched, total_count, pipeline_run_id, runtime)

    executor.shutdown(wait=False)
    logging.info(f"Pipeline complete in {runtime:.1f}s. Exit code: {exit_code}")
    return exit_code

def main():
    exit_code=asyncio.run(run_pipeline())
    sys.exit(exit_code)

if __name__=="__main__":
    main()