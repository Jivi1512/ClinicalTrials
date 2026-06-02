import asyncio
import time
import logging
from datetime import datetime
import aiohttp
from cache import write_page_cache, validate_page_cache, read_page_cache, should_refetch
from state import read_api_checkpoint, write_api_checkpoint
from urllib.parse import urlencode
from config import (
    API_BASE_URL, PAGE_SIZE,
    SEMAPHORE_LIMIT, TOKEN_BUCKET_RATE, TOKEN_BUCKET_INIT,
    RETRY_MAX, RETRY_BACKOFF_BASE, REQUEST_TIMEOUT,
    FILTER_ADVANCED, API_FIELDS_PARAM
)

class TokenBucket:
    def __init__(self, rate, init_fill):
        self._rate=rate
        self._tokens=rate*init_fill
        self._max=float(rate)
        self._last=time.monotonic()
        self._lock=asyncio.Lock()

    async def consume(self):
        async with self._lock:
            now=time.monotonic()
            elapsed=now-self._last
            self._tokens=min(self._max, self._tokens+elapsed*self._rate)
            self._last=now
            if self._tokens<1.0:
                wait=(1.0-self._tokens)/self._rate
                await asyncio.sleep(wait)
                self._tokens=0.0
            else:
                self._tokens-=1.0

def _build_url(page_token):
    params={"format": "json", "pageSize": PAGE_SIZE, "countTotal": "true"}
    if page_token:
        params["pageToken"]=page_token
    params["filter.advanced"]=FILTER_ADVANCED
    params["fields"]=API_FIELDS_PARAM
    return f"{API_BASE_URL}?{urlencode(params)}"

async def _fetch_one_page(session, semaphore, bucket, page_token):
    url=_build_url(page_token)
    for attempt in range(RETRY_MAX):
        await bucket.consume()
        try:
            async with semaphore:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                ) as resp:
                    if resp.status==400:
                        logging.warning(f"400 Bad Request for page token={page_token}, token may be stale")
                        return None, True
                    if resp.status==429:
                        wait=RETRY_BACKOFF_BASE**(attempt+1)
                        logging.warning(f"Rate limited (429), backing off {wait}s (attempt {attempt+1})")
                        await asyncio.sleep(wait)
                        continue
                    resp.raise_for_status()
                    data=await resp.json()
                    return data, False
        except asyncio.TimeoutError:
            logging.warning(f"Timeout on page (token={page_token}), attempt {attempt+1}")
            if attempt<RETRY_MAX-1:
                await asyncio.sleep(RETRY_BACKOFF_BASE**(attempt+1))
        except Exception as e:
            logging.warning(f"Fetch error on page (token={page_token}), attempt {attempt+1}: {e}")
            if attempt<RETRY_MAX-1:
                await asyncio.sleep(RETRY_BACKOFF_BASE**(attempt+1))
    logging.error(f"All {RETRY_MAX} retries failed for page token={page_token}")
    return None, False

async def fetch_all_pages(page_queue):
    checkpoint=read_api_checkpoint()
    page_token=checkpoint.get("page_token")
    pages_fetched=checkpoint.get("pages_fetched", 0)
    total_count=checkpoint.get("total_count_expected", 0)

    bucket=TokenBucket(TOKEN_BUCKET_RATE, TOKEN_BUCKET_INIT)
    semaphore=asyncio.Semaphore(SEMAPHORE_LIMIT)

    logging.info(f"M1 start: resuming from page_token={page_token}, pages_fetched={pages_fetched}")
    fetch_start = time.monotonic()
    total_pages_estimate = 0  # updated from first response

    async with aiohttp.ClientSession() as session:
        try:
            while True:
                cached_valid=validate_page_cache(page_token)
                if cached_valid:
                    payload=read_page_cache(page_token)
                    records=payload["response"].get("studies", [])
                    if not should_refetch(page_token, records):
                        logging.info(f"Using cached page token={page_token}")
                        await page_queue.put(payload["response"])
                        next_token=payload["response"].get("nextPageToken")
                        if not next_token:
                            break
                        page_token=next_token
                        pages_fetched+=1
                        continue

                response_data, is_stale_token=await _fetch_one_page(session, semaphore, bucket, page_token)

                if is_stale_token:
                    logging.warning(f"Stale pageToken detected, resetting checkpoint and restarting from page 1")
                    page_token=None
                    pages_fetched=0
                    total_count=0
                    write_api_checkpoint({
                        "page_token": None,
                        "pages_fetched": 0,
                        "total_count_expected": 0,
                        "last_updated": datetime.utcnow().isoformat()
                    })
                    response_data, is_stale_token=await _fetch_one_page(session, semaphore, bucket, None)
                    if response_data is None:
                        logging.error("Failed to fetch first page after checkpoint reset")
                        break

                if response_data is None:
                    logging.error(f"Skipping page token={page_token} after all retries")
                    break

                if pages_fetched==0:
                    total_count=response_data.get("totalCount", 0)
                    total_pages_estimate = max(1, -(-total_count // PAGE_SIZE))  # ceiling div
                    logging.info(
                        f"M1: total records={total_count:,} -> ~{total_pages_estimate} pages to fetch"
                    )

                write_page_cache(page_token, response_data)
                pages_fetched+=1

                next_token=response_data.get("nextPageToken")

                write_api_checkpoint({
                    "page_token": next_token,
                    "pages_fetched": pages_fetched,
                    "total_count_expected": total_count,
                    "last_updated": datetime.utcnow().isoformat()
                })

                await page_queue.put(response_data)
                elapsed = time.monotonic() - fetch_start
                rate = pages_fetched / elapsed if elapsed > 0 else 0
                eta_s = (total_pages_estimate - pages_fetched) / rate if rate > 0 and total_pages_estimate > 0 else 0
                eta_str = f"{eta_s/60:.1f}min" if eta_s >= 60 else f"{eta_s:.0f}s"
                pct = f"{pages_fetched/max(total_pages_estimate,1)*100:.1f}%" if total_pages_estimate else "?"
                logging.info(
                    f"M1: page {pages_fetched}/{total_pages_estimate} ({pct}) | "
                    f"{rate:.2f} pages/s | ETA: {eta_str} | "
                    f"next_token={'...' + next_token[-12:] if next_token else 'None'}"
                )

                if not next_token:
                    break

                page_token=next_token

            expected=total_count
            fetched_records=pages_fetched*PAGE_SIZE
            if expected>0 and fetched_records<expected:
                gap=expected-fetched_records
                logging.warning(f"COMPLETENESS_WARNING: gap of ~{gap} records (fetched ~{fetched_records}, expected {expected})")
        finally:
            await page_queue.put(None)
            logging.info(f"M1 complete: {pages_fetched} pages fetched")