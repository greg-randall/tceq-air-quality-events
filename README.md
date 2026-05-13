# TCEQ Air Emission Event Reports

TCEQ publishes air emission event reports on their website, but the
data is split across 102,551 individual pages and Excel files with no
bulk download. This pipeline scrapes, parses, and geocodes all of it
into clean CSVs you can actually work with.

Locations are geocoded at varying precision — some are street-level
hits, others are city or county centroids where the address was a
driving direction or a rural description. It's the best you can get
from the source data, but don't expect survey-grade coordinates.

## Scope

| | |
|---|---|
| **Incidents** | 102,551 (May 2026) |
| **Date range** | Jan 2004 to Dec 2025 |
| **Counties** | 211 |
| **Event types** | 5 (air startup, air shutdown, emissions event, excess opacity, maintenance) |
| **Operators** | 1,373 |
| **Contaminants** | 2,108 unique compounds |
| **Geocode coverage** | All incidents have lat/lon; precision varies by address quality |
| **Source** | [TCEQ Air Emission Event Reports](https://www2.tceq.texas.gov/oce/eer/index.cfm) |

## Pipeline

```
1.py
  Download monthly Excel exports (2003–present)
  POST monthly search, GET Excel download

  -> eer_monthly_exports/
       eer_2003-02.xls
       eer_2003-02.csv
       ...

2.py
  Combine all monthly exports
  Merge + deduplicate, write per-file CSV

  -> output/
       eer_master_all.csv      102,551 rows (May 2026)
       eer_master_all.parquet

3.py
  Download per-incident detail HTML + emission XLS
  8 threads, adaptive rate limiting, 404 log

  -> incident_full_data/
       2003-02/
         2003-02-01_13981.html
         2003-02-01_13981.xls
         ...

4.py
  Parse HTML into structured data, geocode addresses inline
  15 threads (cores-1). Caches parsed results as .html.json
  beside each HTML file; converts XLS to .xls.csv on first run.
  --debug (random 10), --limit N, --force-regen

  -> incident_full_data/
       *.html.json              per-file parse caches
       *.xls.csv                per-file emissions CSVs
  -> output/
       incidents.jsonl          nested, with lat/lon
       incidents.csv            flat metadata, with lat/lon
       incident_contaminants.csv denormalized, with lat/lon
  -> geocode_cache.jsonl        address cache (JSONL)
  -> texas_city_coords.json     1,841 Texas place coords

5.py
  Gzip output files for GitHub (they're 150-300 MB uncompressed)

  -> output/
       incidents.csv.gz         ~37 MB
       incidents.jsonl.gz       ~55 MB
       incident_contaminants.csv.gz  ~12 MB
```

- **1.py** — POSTs a monthly search to TCEQ, downloads Excel results. Skips already-downloaded months unless they're recent.
- **2.py** — Merges all monthly Excel files into a master dataset, writes a CSV next to each `.xls`.
- **3.py** — Downloads each incident's detail HTML and emission XLS. 8 threads, throttles itself against TCEQ's rate limits, logs 404s so the next run skips nonexistent incidents.
- **4.py** — Parses HTML with BeautifulSoup, geocodes addresses. `--debug` pulls 10 random files for testing. `--limit N` takes the first N (sorted). `--force-regen` ignores `.html.json` caches and re-parses everything.
- **5.py** — Gzips the output files so they fit under GitHub's 100 MB file size limit.

## Geocoding

Geocoding runs inline during parsing. Each address gets up to six
attempts, from street address to county centroid. The fallback chain
means everything resolves, but the less specific tiers give you city
or county centroids rather than exact coordinates.

| Tier | Source | Notes |
|---|---|---|
| 1 | Census Bureau | Free, no key. Catches ~28% of street addresses |
| 2 | Mapbox v6 | Set `MAPBOX_TOKEN` to enable. Another ~23% |
| 3 | Nominatim | Bare city names with ", Texas" tacked on |
| 4 | ZIP centroid | Nominatim lookup by Texas ZIP code |
| 5 | City centroid | Local DB of 1,841 Texas places — instant, no API call |
| 6 | County centroid | Last resort. Low precision but covers everything |

Results land in `geocode_cache.jsonl`. Negative results track which
sources were tried (`sources_tried`), so if someone adds a new
geocoder later, previously-unresolved addresses retry automatically.

The city database comes from the Census Bureau gazetteer. To rebuild
it, grab `gaz_place_48.txt` from census.gov and run the snippet in
`4.py`.

Some city names that happen to be common English words (West,
Junction, Miles) are excluded from text scanning since they match
road descriptions in driving directions more often than they match
actual cities.

## Output files

| File | Format | Rows | Description |
|---|---|---|---|
| `output/eer_master_all.csv` | CSV | 102,551 | Merged dataset from monthly exports |
| `output/eer_master_all.parquet` | Parquet | 102,551 | Same, compressed |
| `output/incidents.jsonl` | JSONL | 102,551 | Parsed incidents with nested emissions and lat/lon |
| `output/incidents.csv` | CSV | 102,551 | Flat metadata, narrative text, lat/lon |
| `output/incident_contaminants.csv` | CSV | 568,661 | One row per contaminant, denormalized |
| `geocode_cache.jsonl` | JSONL | ~4k | Address geocode cache |
| `texas_city_coords.json` | JSON | 1,841 | Local Texas city coordinate database |

### incidents.csv (24 columns)

One row per incident. The narrative columns (`cause`, `actions`, `basis`)
can have embedded newlines.

| Column | Description |
|---|---|
| `incident_id` | TCEQ tracking number |
| `incident_status` | CLOSED, etc. |
| `report_type` | FINAL, etc. |
| `report_date` | MM/DD/YYYY |
| `owner_name` | Operator name |
| `cn` | Customer Number |
| `regulated_entity_name` | Facility name |
| `rn` | Regulated Entity Number |
| `physical_location` | Raw address string |
| `county` | County name |
| `event_type` | AIR STARTUP, AIR SHUTDOWN, EMISSIONS EVENT, EXCESS OPACITY, MAINTENANCE |
| `event_start` | MM/DD/YYYY HH:MM AM/PM |
| `event_end` | MM/DD/YYYY HH:MM AM/PM |
| `event_duration` | "97 hours, 30 minutes", etc. |
| `notification_date` | MM/DD/YYYY HH:MM AM/PM |
| `notification_method` | STEERS, etc. |
| `notification_jurisdictions` | TCEQ region or null |
| `publication_status` | AUTOMATICALLY VERIFIED, VERIFIED BY A CUSTOMER, etc. |
| `cause` | Narrative text |
| `actions` | Narrative text |
| `basis` | Narrative text |
| `latitude` | Decimal degrees |
| `longitude` | Decimal degrees |
| `geocode_source` | census, mapbox, nominatim_city, zip_centroid, city_centroid, county_centroid |

### incident_contaminants.csv (21 columns)

Denormalized, one row per contaminant per emission point. An incident
with three contaminants at two emission points produces six rows. Join
to `incidents.csv` on `incident_id`.

| Column | Description |
|---|---|
| `incident_id` | TCEQ tracking number |
| `incident_status`, `report_type`, `report_date`, `owner_name`, `regulated_entity_name`, `county`, `event_type`, `event_start`, `event_end` | From the incident |
| `ep_name` | Emission point common name |
| `epn` | Emission Point Number |
| `contaminant` | Chemical name or "Opacity" |
| `est_quantity` | Estimated quantity released |
| `units` | POUNDS, % OPACITY, etc. |
| `emission_limit` | Regulatory limit |
| `limit_units` | Limit units |
| `authorization` | Permit citation |
| `latitude`, `longitude`, `geocode_source` | From the incident |

### incidents.jsonl

One JSON object per line. Same metadata fields as `incidents.csv`, plus
nested arrays:

- `facilities`: `[{"name": "...", "fin": "..."}, ...]`
- `process_units`: `["Unit 1", "Unit 2", ...]`
- `emission_points`: `[{"name": "...", "epn": "...", "contaminants": [{"description": "...", "est_quantity": ..., "units": "...", "emission_limit": ..., "limit_units": "...", "authorization": "..."}, ...]}, ...]`

## Setup

```
pip install requests pandas beautifulsoup4 lxml tqdm python-calamine openpyxl
```

Set `MAPBOX_TOKEN` if you want Mapbox geocoding. The free Census
geocoder handles about half of street addresses on its own.

## Usage

Run in order. Everything is resumable: each script skips files it
already processed.

```
python3 1.py    # download monthly exports
python3 2.py    # combine into master dataset
python3 3.py    # download per-incident details
python3 4.py    # parse HTML into structured output
python3 5.py    # gzip output files for GitHub
```
