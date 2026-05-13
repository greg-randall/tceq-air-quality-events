import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Lock

import pandas as pd
import warnings
from tqdm import tqdm

from utils import BASE_URL, create_session, fetch_with_retry, get_incident_id_column, months_old

warnings.filterwarnings('ignore', category=UserWarning, message='Could not infer format')

NUM_THREADS = 8


def build_existing_cache(output_dir):
    """Walk the output directory once and return a set of (source_month, base_name)
    keys for incidents that have both .html and .xls files cached."""
    cache = set()
    if not output_dir.exists():
        return cache

    for html_file in output_dir.rglob("*.html"):
        xls_file = html_file.with_suffix(".xls")
        if xls_file.exists():
            source_month = html_file.parent.name
            cache.add((source_month, html_file.stem))

    return cache


def load_404_set(output_dir):
    """Read the 404 log and return a set of incident IDs to skip."""
    log_path = output_dir / "_404.log"
    if not log_path.exists():
        return set()
    with open(log_path, 'r') as f:
        return {line.strip() for line in f if line.strip()}


def log_404(output_dir, inc_id, lock):
    """Thread-safe append of a 404 incident ID to the log."""
    log_path = output_dir / "_404.log"
    with lock:
        with open(log_path, 'a') as f:
            f.write(f"{inc_id}\n")


def compute_start_date_col(df):
    """Vectorized start-date extraction — returns a Series of YYYY-MM-DD strings."""
    for col in ['START DATE/TIME', 'START DATE', 'Event Start', 'Date']:
        if col in df.columns:
            dates = pd.to_datetime(df[col], errors='coerce')
            return dates.dt.strftime('%Y-%m-%d').fillna('unknown')
    return pd.Series('unknown', index=df.index)


def build_tasks(df, id_column, output_dir, today, max_months_to_refresh, skip_404):
    """Build task list from dataframe, skipping cached rows and known-404 IDs."""
    tasks = []
    cached = 0
    invalid = 0
    not_found = 0

    # Build in-memory cache of existing files (one directory walk, not per-row)
    print("Scanning existing files...")
    existing = build_existing_cache(output_dir)
    print(f"Found {len(existing):,} cached incidents")
    if skip_404:
        print(f"Found {len(skip_404):,} previously-404 incidents\n")
    else:
        print()

    # Pre-compute columns for fast iteration (avoid iterrows)
    start_date_series = compute_start_date_col(df)
    source_month_series = df['source_month'].astype(str)
    id_series = df[id_column].astype(str)

    print("Building task list...")
    for i in tqdm(range(len(df)), total=len(df), desc="Pre-scan", unit="row"):
        inc_id = id_series.iloc[i].strip()
        if not inc_id or inc_id.lower() in ['nan', 'none', '', '0']:
            invalid += 1
            continue

        # Skip known-404 incidents
        if inc_id in skip_404:
            not_found += 1
            continue

        start_date_str = start_date_series.iloc[i]
        base_name = f"{start_date_str}_{inc_id}"
        source_month = source_month_series.iloc[i]

        if source_month == 'nan':
            source_month = ''

        # Check in-memory cache instead of individual filesystem calls
        cached_key = (source_month if source_month else 'unknown', base_name)

        if cached_key in existing:
            if source_month:
                try:
                    mdate = pd.to_datetime(source_month + '-01').to_pydatetime()
                    if months_old(mdate, today) > max_months_to_refresh:
                        cached += 1
                        continue
                except Exception:
                    cached += 1
                    continue
            else:
                cached += 1
                continue

        month_dir = output_dir / (source_month if source_month else 'unknown')

        tasks.append({
            'inc_id': inc_id,
            'start_date_str': start_date_str,
            'html_path': month_dir / f"{base_name}.html",
            'xls_path': month_dir / f"{base_name}.xls",
            'month_dir': month_dir,
        })

    return tasks, cached, invalid, not_found


class RateLimiter:
    """Thread-safe adaptive rate limiter with backoff on 409 responses."""

    def __init__(self, initial_sleep=1.5):
        self.base_sleep = initial_sleep
        self.lock = Lock()

    def get_sleep(self):
        with self.lock:
            return self.base_sleep

    def backoff(self):
        """Double the base sleep on rate-limit response, capped at 60s."""
        with self.lock:
            old = self.base_sleep
            self.base_sleep = min(self.base_sleep * 2, 60.0)
            return self.base_sleep != old

    def sleep(self):
        """Sleep for a random interval based on current base_sleep."""
        s = self.get_sleep()
        time.sleep(random.uniform(s * 0.5, s * 1.5))


def fetch_one(task, rate_limiter, output_dir, not_found_lock):
    """Download HTML + XLS for a single incident. Returns (status, inc_id)."""
    session = create_session()
    inc_id = task['inc_id']
    html_path = task['html_path']
    xls_path = task['xls_path']

    task['month_dir'].mkdir(parents=True, exist_ok=True)

    got_409 = False
    got_404 = False
    success = True

    # Fetch HTML details
    try:
        detail_url = f"{BASE_URL}?fuseaction=main.getDetails&target={inc_id}"
        r = fetch_with_retry(session, "GET", detail_url, timeout=30)

        if r.status_code == 409:
            got_409 = True
            success = False
        elif r.status_code == 404:
            got_404 = True
            success = False
        elif r.status_code == 200 and len(r.text) > 4000:
            html_path.write_text(r.text, encoding='utf-8')
        else:
            success = False
    except Exception:
        success = False

    # Fetch XLS emissions (only if HTML succeeded and no 409)
    if success:
        try:
            emission_url = f"{BASE_URL}?fuseaction=main.emissiondwnld&target={inc_id}"
            r = fetch_with_retry(session, "GET", emission_url, timeout=30)

            if r.status_code == 409:
                got_409 = True
                success = False
            elif r.status_code == 404:
                got_404 = True
                success = False
            elif r.status_code == 200 and len(r.content) > 1500:
                xls_path.write_bytes(r.content)
            else:
                success = False
        except Exception:
            success = False

    # Log 404s so future runs skip them
    if got_404:
        log_404(output_dir, inc_id, not_found_lock)

    # Handle rate limiting
    if got_409:
        if rate_limiter.backoff():
            tqdm.write(f"RATE-LIMIT (409) on {inc_id} — base sleep now {rate_limiter.get_sleep():.1f}s, pausing 300s")
        else:
            tqdm.write(f"RATE-LIMIT (409) on {inc_id} — pausing 300s")
        time.sleep(300)
    else:
        rate_limiter.sleep()

    return ('ok' if success else 'warn', inc_id)


def download_incident_details(max_months_to_refresh=2):
    master_path = Path("output/eer_master_all.csv")
    if not master_path.exists():
        print("Master file not found. Run script 2 first.")
        return

    df = pd.read_csv(master_path)
    id_column = get_incident_id_column(df)

    print(f"Using ID column: '{id_column}'")
    print(f"Total incidents: {len(df):,}\n")

    output_dir = Path("incident_full_data")
    output_dir.mkdir(exist_ok=True)

    today = datetime.now()

    # Sort oldest to newest
    if 'START DATE/TIME' in df.columns:
        df['sort_date'] = pd.to_datetime(df['START DATE/TIME'], errors='coerce', format='mixed')
        df = df.sort_values(by='sort_date').reset_index(drop=True)
        print("Sorted by 'START DATE/TIME' (oldest -> newest)")
    else:
        print("START DATE/TIME column not found")

    # Load known-404 set
    skip_404 = load_404_set(output_dir)

    # Build task list (pre-scan filesystem to exclude cached and known-404)
    tasks, cached, invalid, not_found = build_tasks(
        df, id_column, output_dir, today, max_months_to_refresh, skip_404)

    print(f"{len(tasks):,} to fetch, {cached:,} cached, {not_found:,} known-404, {invalid:,} invalid")
    print(f"Using {NUM_THREADS} threads\n")

    rate_limiter = RateLimiter(initial_sleep=1.5)
    not_found_lock = Lock()
    processed = 0
    failed = 0
    lock = Lock()

    pbar = tqdm(total=len(tasks), desc="Fetching incidents", unit="inc")

    try:
        with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            futures = {executor.submit(fetch_one, t, rate_limiter, output_dir, not_found_lock): t for t in tasks}

            for future in as_completed(futures):
                try:
                    status, inc_id = future.result()
                except Exception:
                    status, inc_id = 'fail', futures[future]['inc_id']

                with lock:
                    if status == 'ok':
                        processed += 1
                    elif status == 'warn':
                        failed += 1
                        tqdm.write(f"WARN {inc_id}")
                    else:
                        failed += 1
                        tqdm.write(f"FAIL {inc_id}")

                    pbar.set_postfix(proc=processed, fail=failed)
                    pbar.update(1)

    except KeyboardInterrupt:
        tqdm.write("\nStopped by user. Shutting down threads...")
        executor.shutdown(wait=False, cancel_futures=True)

    pbar.close()

    print("\n" + "=" * 80)
    print("DOWNLOAD SESSION COMPLETED")
    print("=" * 80)
    print(f"Processed : {processed:,}")
    print(f"Failed    : {failed:,}")
    print(f"Cached    : {cached:,}")
    print(f"Folder    : {output_dir.resolve()}")
    print("=" * 80)


if __name__ == "__main__":
    download_incident_details(max_months_to_refresh=2)
