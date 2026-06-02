"""
main.py — Pipeline orchestrator.

Execution model:
  • M1 fetch runs as a background asyncio task, filling page_queue.
  • M2 parse consumes page_queue; each parsed chunk immediately gets a
    resolve task (asyncio.create_task) — fetch and resolve run concurrently.
  • A bounded semaphore (RESOLVE_CONCURRENCY) caps in-flight resolve tasks
    so memory stays bounded even with 500+ chunks.
  • Results are buffered by chunk_index; DatasetBuilder writes in order as
    each consecutive chunk completes (streaming CSV, no end-of-run bottleneck).
  • Live progress is logged throughout: fetch ETA, resolve rate, cache stats.
"""

import sys
import asyncio
import logging
import time
import uuid
import os
import aiohttp
from config import (
    CHUNK_SIZE, QUEUE_MAX_PAGES, OUTPUT_CSV_PATH, REVIEW_QUEUE_PATH,
    DUPLICATES_LOG_PATH, CHEMBL_SEMAPHORE, RESOLVE_CONCURRENCY
)
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
        logging.FileHandler("pipeline.log", mode="a")
    ]
)
logging.getLogger("aiohttp.client").setLevel(logging.ERROR)


# ─────────────────────────────────────────────────────────────────────────────
# Progress helpers
# ─────────────────────────────────────────────────────────────────────────────

def _eta(elapsed_s: float, done: int, total: int) -> str:
    if done <= 0 or total <= 0:
        return "?"
    rate = done / elapsed_s
    remaining = (total - done) / rate
    if remaining < 60:
        return f"{remaining:.0f}s"
    return f"{remaining/60:.1f}min"


async def _progress_logger(
    start: float,
    chunks_done_ref: list,   # mutable list used as a pointer
    chunks_total_ref: list,
    stop_event: asyncio.Event
):
    """Logs a heartbeat every 30 s while the pipeline is running."""
    while not stop_event.is_set():
        await asyncio.sleep(30)
        if stop_event.is_set():
            break
        elapsed = time.monotonic() - start
        done  = chunks_done_ref[0]
        total = chunks_total_ref[0]
        cs    = get_cache_stats()
        logging.info(
            f"-- Progress -- {elapsed/60:.1f} min elapsed | "
            f"chunks written: {done}/{total if total else '?'} | "
            f"ChEMBL cache: {cs['cache_resolved']}/{cs['cache_total']} resolved "
            f"({cs['cache_miss']} misses)"
        )

async def run_pipeline():
    start = time.monotonic()
    pipeline_run_id = str(uuid.uuid4())
    logging.info(f"Pipeline started | run_id={pipeline_run_id}")

    # Clear stale outputs on a fresh run
    checkpoint = read_api_checkpoint()
    if checkpoint.get("pages_fetched", 0) == 0:
        for path in [OUTPUT_CSV_PATH, REVIEW_QUEUE_PATH, DUPLICATES_LOG_PATH]:
            if os.path.exists(path):
                os.remove(path)
                logging.info(f"Cleared stale output: {path}")

    # Shared state for progress logging
    chunks_done_ref  = [0]
    chunks_total_ref = [0]
    stop_progress    = asyncio.Event()

    builder       = DatasetBuilder(pipeline_run_id)
    page_queue    = asyncio.Queue(maxsize=QUEUE_MAX_PAGES)
    chembl_sem    = asyncio.Semaphore(CHEMBL_SEMAPHORE)
    resolve_gate  = asyncio.Semaphore(RESOLVE_CONCURRENCY)

    # Results buffer for ordered writing: {chunk_index → records}
    results_buf: dict[int, list] = {}
    next_write   = 0               # next chunk_index to write to CSV
    tasks_launched = 0

    connector = aiohttp.TCPConnector(limit=CHEMBL_SEMAPHORE + 10, ttl_dns_cache=300)

    fetch_task       = asyncio.create_task(fetch_all_pages(page_queue))
    progress_task    = asyncio.create_task(
        _progress_logger(start, chunks_done_ref, chunks_total_ref, stop_progress)
    )

    async def _resolve_with_gate(df_c, chunk_idx, pair_start):
        """Gated resolve: respects RESOLVE_CONCURRENCY bound."""
        try:
            async with resolve_gate:
                return await resolve_chunk_async(df_c, chembl_session, chembl_sem, chunk_idx, pair_start)
        except Exception as e:
            logging.error(f"Error resolving chunk {chunk_idx}: {e}")
            return chunk_idx, df_c.to_dict("records")

    def _flush_buffer():
        """Write all consecutively available chunks to CSV."""
        nonlocal next_write
        while next_write in results_buf:
            builder.process_chunk(next_write, results_buf.pop(next_write))
            chunks_done_ref[0] += 1
            logging.info(
                f"  Written chunk {next_write} | "
                f"total rows so far: {builder.get_stats()['total_rows']} | "
                f"elapsed: {(time.monotonic()-start)/60:.1f} min"
            )
            next_write += 1

    async with aiohttp.ClientSession(connector=connector) as chembl_session:
        resolve_tasks: list[asyncio.Task] = []

        # ── Parse loop: launch a resolve task for every chunk ────────────────
        async for df_chunk, chunk_index in parse_pages(page_queue):
            df_chunk = normalize_chunk(df_chunk)
            pair_id_start = allocate_pair_range(len(df_chunk))

            task = asyncio.create_task(
                _resolve_with_gate(df_chunk, chunk_index, pair_id_start)
            )
            resolve_tasks.append(task)
            tasks_launched += 1
            chunks_total_ref[0] = tasks_launched

            # Opportunistically flush any completed chunks (non-blocking)
            done_tasks = [t for t in resolve_tasks if t.done()]
            for t in done_tasks:
                ci, records = t.result()
                results_buf[ci] = records
                resolve_tasks.remove(t)
            _flush_buffer()

        logging.info(
            f"Parse complete | {tasks_launched} chunks launched | "
            f"{len(resolve_tasks)} resolve tasks still in flight"
        )

        # ── Drain remaining resolve tasks ─────────────────────────────────────
        if resolve_tasks:
            for coro in asyncio.as_completed(resolve_tasks):
                ci, records = await coro
                results_buf[ci] = records
                _flush_buffer()
                elapsed = time.monotonic() - start
                remaining = len([k for k in results_buf if k >= next_write])
                logging.info(
                    f"  Resolve drain | chunk {ci} done | "
                    f"{chunks_done_ref[0]}/{tasks_launched} written | "
                    f"{remaining} buffered | "
                    f"ETA: {_eta(elapsed, chunks_done_ref[0], tasks_launched)}"
                )

        # Final flush in case anything remains
        _flush_buffer()

    await fetch_task

    # Stop progress logger
    stop_progress.set()
    progress_task.cancel()
    try:
        await progress_task
    except asyncio.CancelledError:
        pass

    stats      = builder.get_stats()
    cache_stats = get_cache_stats()
    runtime    = time.monotonic() - start

    logging.info(
        f"Pipeline complete | {runtime/60:.1f} min | "
        f"{stats['total_rows']} rows | "
        f"ChEMBL cache: {cache_stats['cache_resolved']}/{cache_stats['cache_total']} resolved"
    )

    checkpoint   = read_api_checkpoint()
    pages_fetched = checkpoint.get("pages_fetched", 0)
    total_count   = checkpoint.get("total_count_expected", 0)

    exit_code = generate_report(stats, pages_fetched, total_count, pipeline_run_id, runtime)
    logging.info(f"Exit code: {exit_code}")
    return exit_code


def main():
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    sys.exit(asyncio.run(run_pipeline()))


if __name__ == "__main__":
    main()