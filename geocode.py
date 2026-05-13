"""Geocode incident addresses via Census (free) with optional Mapbox fallback.

Caches every lookup in geocode_cache.json so repeated addresses don't
re-hit the API across runs.
"""
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

CACHE_PATH = Path("geocode_cache.jsonl")
CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
MAPBOX_URL = "https://api.mapbox.com/search/geocode/v6/forward"
USER_AGENT = "TCEQAirQualityAnalysis/1.0"
CENSUS_SLEEP = 0.25
MAPBOX_SLEEP = 0.05

MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN")
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_SLEEP = 1.2
_nominatim_sem = threading.Semaphore(1)  # Nominatim allows 1 req/sec
_cache_lock = threading.RLock()
_cache = None
_cache_hits = 0
_cache_misses = 0
_PENDING = object()  # sentinel: another thread is fetching this key


def get_cache_stats():
    """Return (hits, misses) counters for geocode cache lookups."""
    return _cache_hits, _cache_misses


# Load Texas cities list (lazy, on first use)
_cities = None
CITIES_PATH = Path("texas-cities.txt")
_city_coords = None
CITY_COORDS_PATH = Path("texas_city_coords.json")


def _load_city_coords():
    """Return {CITY_NAME: {lat, lon}} dict from Census gazetteer."""
    global _city_coords
    if _city_coords is None:
        if CITY_COORDS_PATH.exists():
            with CITY_COORDS_PATH.open() as f:
                _city_coords = json.load(f)
        else:
            _city_coords = {}
    return _city_coords


def _load_cities():
    global _cities
    if _cities is None:
        if CITIES_PATH.exists():
            with CITIES_PATH.open() as f:
                raw = {line.strip() for line in f if line.strip()}
            # Filter out city names that are also common words in driving
            # directions — these generate false positives in text scanning
            _SKIP_CITIES = {
                "West",       # cardinal direction
                "Junction",   # road junction
                "Miles",      # distance measurement
                "Orange",     # color (ambiguous, less confident match)
                "Road Runner",  # road name, not a city
            }
            raw.difference_update(_SKIP_CITIES)
            # Pre-normalize: (normalized, original) tuples, longest first
            _cities = sorted(
                [
                    (re.sub(r"[^a-zA-Z\s]", " ", c).strip().lower(), c)
                    for c in raw
                ],
                key=lambda x: len(x[0]),
                reverse=True,
            )
        else:
            _cities = []
    return _cities


_city_patterns = None


def _scan_for_city(text):
    """Try to find a Texas city name anywhere in the text.

    Returns the longest matching city name, or None.
    Normalizes both sides (letters only, lowercased, word boundaries).
    """
    global _city_patterns
    cities = _load_cities()
    if not cities:
        return None

    # Lazy-compile patterns
    if _city_patterns is None:
        _city_patterns = [
            (re.compile(r"\b" + re.escape(norm) + r"\b"), orig)
            for norm, orig in cities
        ]

    clean = " ".join(re.sub(r"[^a-zA-Z\s]", " ", text).split()).lower()
    for pattern, original in _city_patterns:
        if pattern.search(clean):
            return original.upper()
    return None


# Extract city from "123 Main St, City, TX 12345"
_CITY_RE = re.compile(r'[,;]\s*([^,;]+?),\s*(?:TX|TEXAS)\b', re.IGNORECASE)
# Extract 5-digit ZIP codes
_ZIP_RE = re.compile(r'\b(\d{5})\b')
# Texas ZIP: 75000-79999, 88500-88599 plus special prefixes
_TX_ZIP_RE = re.compile(r'[78][356789]\d{3}$')


def extract_city_zip(address):
    """Try to pull city name and a valid Texas ZIP from an address string.

    Returns (city, zipcode) tuple; each is None if not found.
    Tries structured "City, TX" pattern first, then falls back to
    scanning the text for known Texas city names.
    """
    zip_match = _ZIP_RE.findall(address)
    tx_zip = None
    for z in zip_match:
        if _TX_ZIP_RE.match(z):
            tx_zip = z
            break

    city_match = _CITY_RE.search(address)
    city = city_match.group(1).strip() if city_match else None

    # If no structured city found, scan for known Texas city names
    if city is None:
        city = _scan_for_city(address)

    return city, tx_zip


def _clean_address(address):
    """Normalize an incident physical_location for geocoding.

    Replaces semicolons with commas (common in TCEQ data), collapses
    whitespace, and strips trailing junk.
    """
    addr = address.replace(";", ",")
    addr = " ".join(addr.split())
    return addr.strip(" ,")


def _http_get_json(url, params, timeout=30):
    full = url + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(full, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
    if not body.strip():
        raise json.JSONDecodeError("empty response", "", 0)
    return json.loads(body)


def _census_geocode(address):
    """Geocode a single address via Census Bureau geocoder (free, no key)."""
    try:
        res = _http_get_json(CENSUS_URL, {
            "address": address,
            "benchmark": "Public_AR_Current",
            "format": "json",
        })
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as e:
        return {"status": "error", "error": str(e)}
    matches = res.get("result", {}).get("addressMatches") or []
    if not matches:
        return {"status": "not_found"}
    m = matches[0]
    state = (m.get("addressComponents", {}) or {}).get("state", "").upper()
    return {
        "status": "ok",
        "lat": float(m["coordinates"]["y"]),
        "lon": float(m["coordinates"]["x"]),
        "matched_address": m.get("matchedAddress", ""),
        "state": state,
    }


def _mapbox_geocode(address):
    """Geocode a single address via Mapbox v6 geocoder."""
    if not MAPBOX_TOKEN:
        return {"status": "error", "error": "MAPBOX_TOKEN not set"}
    params = {
        "q": address,
        "country": "us",
        "autocomplete": "false",
        "types": "address,street",
        "limit": 3,
        "access_token": MAPBOX_TOKEN,
    }
    try:
        res = _http_get_json(MAPBOX_URL, params)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as e:
        return {"status": "error", "error": str(e)}

    for f in (res.get("features") or []):
        props = f.get("properties", {})
        mc = props.get("match_code", {})
        confidence = mc.get("confidence", "")
        if confidence == "low":
            continue
        if confidence == "medium" and mc.get("region") != "matched":
            continue
        coords = props.get("coordinates", {})
        lat = coords.get("latitude")
        lon = coords.get("longitude")
        if lat is None or lon is None:
            continue
        context = props.get("context", {})
        region_code = context.get("region", {}).get("region_code", "")
        if region_code and region_code != "TX":
            continue
        return {
            "status": "ok",
            "lat": float(lat),
            "lon": float(lon),
            "matched_address": props.get("full_address", ""),
            "state": "TX",
            "mapbox_confidence": confidence,
        }
    return {"status": "not_found"}


def _load_cache():
    global _cache
    if _cache is None:
        _cache = {}
        if CACHE_PATH.exists():
            duplicates = 0
            try:
                with CACHE_PATH.open() as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                            key = entry.pop("_key")
                            src = entry.get("source")
                            if src is None and "sources_tried" not in entry:
                                continue
                            if key in _cache:
                                duplicates += 1
                            _cache[key] = entry  # last-wins
                        except (json.JSONDecodeError, KeyError):
                            continue
            except OSError:
                pass
            # Rewrite cleanly if duplicates were found
            if duplicates > 0:
                _rewrite_cache()
    return _cache


def _rewrite_cache():
    """Rewrite cache file without duplicate keys."""
    tmp = CACHE_PATH.with_suffix(".tmp")
    with tmp.open("w") as f:
        for key, entry in _cache.items():
            entry["_key"] = key
            f.write(json.dumps(entry, separators=(",", ":")) + "\n")
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(CACHE_PATH)


def _save_entry(key, value):
    """Append a single cache entry as one JSONL line with fsync."""
    with _cache_lock:
        value["_key"] = key
        with CACHE_PATH.open("a") as f:
            f.write(json.dumps(value, separators=(",", ":")) + "\n")
            f.flush()
            os.fsync(f.fileno())


def _nominatim_search(query):
    """Geocode via Nominatim (OpenStreetMap). Free, 1 req/sec rate limit.

    Uses a semaphore to prevent concurrent requests across threads,
    and retries on HTTP 429 (rate limit) with exponential backoff.
    The 1.2s rate-limit sleep happens inside the semaphore so threads
    don't release it before the cooldown.
    """
    for attempt in range(3):
        with _nominatim_sem:
            try:
                res = _http_get_json(NOMINATIM_URL, {
                    "q": query,
                    "format": "json",
                    "limit": 1,
                    "addressdetails": 1,
                    "countrycodes": "us",
                })
            except urllib.error.HTTPError as e:
                if e.code == 429 and attempt < 2:
                    time.sleep(2 ** attempt * 2)
                    continue
                return {"status": "error", "error": str(e)}
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
                if attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                return {"status": "error", "error": str(e)}
            # Rate-limit sleep inside semaphore so next thread waits
            time.sleep(NOMINATIM_SLEEP)
            break
    if not res:
        return {"status": "not_found"}
    r = res[0]
    addr = r.get("address", {}) or {}
    state = (addr.get("state") or "").upper()
    return {
        "status": "ok",
        "lat": float(r["lat"]),
        "lon": float(r["lon"]),
        "matched_address": r.get("display_name", ""),
        "state": state,
    }


def _cached_lookup(key, fetch_fn, source_name, sleep_after=0):
    """Generic cache wrapper. ``key`` is the cache key string.

    Only caches definitive results (success or not_found). Errors are
    not cached so they can be retried on the next run.
    ``source_name`` is used to label negative cache entries.
    """
    global _cache_hits, _cache_misses
    while True:
        with _cache_lock:
            cache = _load_cache()
            if key in cache:
                entry = cache[key]
                if entry is _PENDING:
                    pass  # another thread fetching
                elif entry.get("lat") is not None:
                    _cache_hits += 1
                    return entry
                elif not _should_retry(entry):
                    _cache_hits += 1
                    return None
                else:
                    del cache[key]
                    cache[key] = _PENDING
                    break
            else:
                cache[key] = _PENDING
                break
        time.sleep(0.1)

    _cache_misses += 1
    r = fetch_fn()
    if sleep_after:
        time.sleep(sleep_after)

    result = None
    if r["status"] == "ok":
        result = {
            "lat": r["lat"],
            "lon": r["lon"],
            "source": source_name,
            "matched_address": r["matched_address"],
        }

    # Only cache definitive results — errors can be retried
    if r["status"] != "error":
        with _cache_lock:
            cache = _load_cache()
            if result is not None:
                cache[key] = result
            else:
                cache[key] = {
                    "lat": None,
                    "lon": None,
                    "sources_tried": [source_name],
                }
            _save_entry(key, cache[key])

    return result


def geocode_zip(zipcode):
    """Geocode a Texas ZIP code to its centroid via Nominatim.

    Returns same dict shape as geocode_address, or None.
    """
    key = f"zip:{zipcode}"

    def _fetch():
        r = _nominatim_search(zipcode)
        return r

    return _cached_lookup(key, _fetch, "zip_centroid", sleep_after=NOMINATIM_SLEEP)


def geocode_city_county(city, county, state="TX"):
    """Geocode a city to its centroid.

    Uses the local Census gazetteer database (instant, no API call),
    falling back to Nominatim only for unknown cities.
    """
    # Tier 1: local Census gazetteer (instant)
    coords = _load_city_coords()
    if city in coords:
        return {
            "lat": coords[city]["lat"],
            "lon": coords[city]["lon"],
            "source": "city_centroid",
            "matched_address": f"{city}, {state}",
        }

    # Tier 2: Nominatim city+county
    key = f"city:{city},{county},{state}"

    def _fetch_county():
        r = _nominatim_search(f"{city}, {county} County, {state}")
        return r

    result = _cached_lookup(key, _fetch_county, "city_centroid",
                            sleep_after=NOMINATIM_SLEEP)
    if result is not None:
        return result

    # Tier 3: Nominatim city+state (neighboring county reference)
    key2 = f"city:{city},,{state}"

    def _fetch_state():
        r = _nominatim_search(f"{city}, {state}")
        return r

    return _cached_lookup(key2, _fetch_state, "city_centroid",
                          sleep_after=NOMINATIM_SLEEP)


def geocode_county(county, state="TX"):
    """Geocode a county to its centroid via Nominatim.

    Returns same dict shape as geocode_address, or None.
    """
    key = f"county:{county},{state}"

    def _fetch():
        r = _nominatim_search(f"{county} County, {state}")
        return r

    return _cached_lookup(key, _fetch, "county_centroid", sleep_after=NOMINATIM_SLEEP)


def _available_sources():
    """Return set of environment-gated geocoding source names.

    Only sources whose availability depends on the environment (API key
    configured, service online) belong here. Address-content-dependent
    tiers like nominatim_city (only tried when there's no street number)
    are excluded since their absence doesn't indicate staleness.
    """
    sources = {"census"}
    if MAPBOX_TOKEN:
        sources.add("mapbox")
    return sources


def _should_retry(entry):
    """Check if a cached negative entry should be retried.

    Returns True if there are currently-available sources that weren't
    tried when this entry was cached (e.g. a new geocoder was added).
    Old-format entries without ``sources_tried`` always retry once.
    """
    tried = entry.get("sources_tried")
    if tried is None:
        return True  # old format, retry once to convert
    available = _available_sources()
    return not set(tried).issuperset(available)


def geocode_address(address):
    """Geocode an address string.

    Returns dict with lat, lon, source, matched_address on success,
    or None if the address could not be resolved.

    Negative results are cached with ``sources_tried`` so newly-added
    geocoding methods automatically retry on the next run.
    """
    cleaned = _clean_address(address)
    if not cleaned:
        return None

    # Check cache; reserve key if miss so other threads don't duplicate work.
    # Retry loop: if another thread is already fetching this key (_PENDING),
    # wait briefly and re-check instead of duplicating the API call.
    while True:
        with _cache_lock:
            cache = _load_cache()
            if cleaned in cache:
                global _cache_hits
                entry = cache[cleaned]
                if entry is _PENDING:
                    pass  # another thread fetching, retry after sleep
                elif entry.get("lat") is not None:
                    _cache_hits += 1
                    return entry
                elif not _should_retry(entry):
                    _cache_hits += 1
                    return None
                else:
                    del cache[cleaned]  # stale, retry
                    cache[cleaned] = _PENDING
                    break
            else:
                cache[cleaned] = _PENDING  # reserve key
                break
        time.sleep(0.1)  # wait for other thread to finish

    global _cache_misses
    _cache_misses += 1
    result = None
    tried = []

    # Tier 1: Census (free, no API key)
    tried.append("census")
    r = _census_geocode(cleaned)
    time.sleep(CENSUS_SLEEP)
    if r["status"] == "ok" and r.get("state") == "TX":
        result = {
            "lat": r["lat"],
            "lon": r["lon"],
            "source": "census",
            "matched_address": r["matched_address"],
        }

    # Tier 2: Mapbox (if token available)
    if result is None and MAPBOX_TOKEN:
        tried.append("mapbox")
        r = _mapbox_geocode(cleaned)
        time.sleep(MAPBOX_SLEEP)
        if r["status"] == "ok":
            result = {
                "lat": r["lat"],
                "lon": r["lon"],
                "source": "mapbox",
                "matched_address": r["matched_address"],
            }

    # Tier 3: Bare city name (no street number) -> Nominatim
    if result is None and not any(c.isdigit() for c in cleaned):
        tried.append("nominatim_city")
        r = _nominatim_search(cleaned + ", Texas")
        time.sleep(NOMINATIM_SLEEP)
        if r["status"] == "ok":
            result = {
                "lat": r["lat"],
                "lon": r["lon"],
                "source": "nominatim_city",
                "matched_address": r["matched_address"],
            }

    # Replace sentinel with real result
    with _cache_lock:
        cache = _load_cache()
        entry = result if result is not None else {
            "lat": None,
            "lon": None,
            "sources_tried": tried,
        }
        cache[cleaned] = entry
        _save_entry(cleaned, entry)

    return result
