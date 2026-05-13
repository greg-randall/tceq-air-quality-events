import time

import requests
from datetime import datetime

BASE_URL = "https://www2.tceq.texas.gov/oce/eer/index.cfm"
MAX_RETRIES = 3


def create_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/147.0.0.0 Safari/537.36 Edg/147.0.0.0"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": BASE_URL,
    })
    return session


def months_old(dt, today=None):
    """Approximate months between dt and today (or a given reference date)."""
    if today is None:
        today = datetime.now()
    return (today - dt).days // 30


def fetch_with_retry(session, method, url, **kwargs):
    """GET or POST with exponential backoff on network errors."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if method == "POST":
                return session.post(url, **kwargs)
            else:
                return session.get(url, **kwargs)
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES:
                raise
            wait = (2 ** attempt) * 1.5
            print(f"   Retry {attempt}/{MAX_RETRIES} ({e}), waiting {wait:.1f}s...")
            time.sleep(wait)


def get_incident_id_column(df):
    possible = ['INCIDENT NO.', 'Incident Number', 'incid_track_num']
    for col in df.columns:
        if any(p.lower() in str(col).lower() for p in possible):
            return col
    return df.columns[0]
