import time
from datetime import datetime, timedelta
from pathlib import Path

from tqdm import tqdm

from utils import BASE_URL, create_session, fetch_with_retry, months_old

SESSION = create_session()


def build_month_list():
    """Pre-compute all months from 2003-01 to present for the progress bar."""
    months = []
    current = datetime(2003, 1, 1)
    today = datetime.now()
    max_month = today.replace(day=1) + timedelta(days=32)

    while current <= max_month:
        month_start = current.replace(day=1)
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1, day=1)
        else:
            next_month = current.replace(month=current.month + 1, day=1)
        month_end = next_month - timedelta(days=1)
        month_str = current.strftime("%Y-%m")
        months.append((month_start, month_end, month_str))
        current = next_month

    return months


def download_monthly():
    output_dir = Path("eer_monthly_exports")
    output_dir.mkdir(exist_ok=True)

    today = datetime.now()
    months = build_month_list()

    # Filter out cached months so the progress bar shows only real work
    to_download = []
    cached_count = 0
    for month_start, month_end, month_str in months:
        filename = output_dir / f"eer_{month_str}.xls"
        file_exists = filename.exists() and filename.stat().st_size > 5000
        current = datetime(int(month_str[:4]), int(month_str[5:7]), 1)
        force_redownload = months_old(current, today) <= 2
        if file_exists and not force_redownload:
            cached_count += 1
        else:
            to_download.append((month_start, month_end, month_str))

    if cached_count:
        print(f"{cached_count} months cached, {len(to_download)} to download\n")

    pbar = tqdm(to_download, desc="Downloading", unit="mo")
    for month_start, month_end, month_str in pbar:
        filename = output_dir / f"eer_{month_str}.xls"
        pbar.set_description(f"Download {month_str}")

        payload = {
            'newsearch': 'yes',
            'incid_track_num': '',
            'event_start_beg_dt': month_start.strftime("%m/%d/%Y"),
            'event_start_end_dt': month_end.strftime("%m/%d/%Y"),
            'event_end_beg_dt': '',
            'event_end_end_dt': '',
            'cn_txt': '',
            'cust_name': '',
            'rn_txt': '',
            're_name': '',
            'ls_cnty_name': '',
            'ls_region_cd': '',
            'ls_event_typ_cd': '',
            '_fuseaction=main.searchresults.x': '32',
            '_fuseaction=main.searchresults.y': '13',
        }

        try:
            post_resp = fetch_with_retry(SESSION, "POST", BASE_URL, data=payload, timeout=30)
            if post_resp.status_code != 200:
                tqdm.write(f"   Fail: search POST returned {post_resp.status_code}")
                continue

            resp = fetch_with_retry(SESSION, "GET", f"{BASE_URL}?fuseaction=main.searchdwnld", timeout=30)

            if resp.status_code == 200 and len(resp.content) > 8000 and b"<!DOCTYPE html" not in resp.content[:1000]:
                filename.write_bytes(resp.content)
                tqdm.write(f"   OK {filename.name}  ({len(resp.content)//1024:,} KB)")
            else:
                tqdm.write(f"   Fail for {month_str} ({len(resp.content)//1024} KB)")

        except Exception as e:
            tqdm.write(f"   Fail for {month_str}: {e}")

        time.sleep(2.0)

    pbar.close()


if __name__ == "__main__":
    download_monthly()
