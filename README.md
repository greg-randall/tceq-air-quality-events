# TCEQ Air Emission Event Reports

Texas requires companies to report unplanned air emissions to the TCEQ.
The reports are public record, but TCEQ buries them behind 102,551
individual pages and Excel files. This pipeline pulls all of it into
CSVs and JSONL so you can see who's dumping what into the air.

The data covers 2004 through 2025: refineries in Harris County,
compressor stations in Ector County, chemical plants in Gray County.
Some locations are street addresses, others are driving directions for
well pads and pipeline segments that don't have one. 1,373 operators.
2,108 distinct contaminants. Releases range from under an hour to
several weeks. It's a lot of sulfur dioxide, a lot of benzene, and a
lot of things the state would rather you didn't think about.

A note on data quality: some of the reported numbers are clearly
wrong. Sulfur dioxide is listed with an opacity of 127,807%. A few
tons of xylene somehow became a single opacity percentage. Whether
this is carelessness, an underfunded agency, or something less
charitable — you'll have to decide for yourself. We pass the data
through as TCEQ published it.

The only columns we add are `latitude`, `longitude`, and
`geocode_source`. Everything else comes straight from TCEQ. Coordinates
are geocoded at the best precision the source address allows — street
level when we can get it, city or county center when the address was
a highway intersection or a lease road.

Source: [TCEQ Air Emission Event Reports](https://www2.tceq.texas.gov/oce/eer/index.cfm)

## Output files

The compressed files in `output/` are zip archives — any modern OS can
open them without extra software.

| File | Format | Rows | Description |
|---|---|---|---|
| `output/eer_master_all.csv.zip` | CSV | 102,551 | Merged dataset from monthly exports |
| `output/incidents.csv.zip` | CSV | 102,551 | Flat metadata, narrative text, lat/lon |
| `output/incidents.jsonl.zip` | JSONL | 102,551 | Parsed incidents with nested emissions and lat/lon |
| `output/incident_contaminants.csv.zip` | CSV | 568,661 | One row per contaminant, incident columns repeated |

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
| `geocode_source` | census, mapbox, nominatim_city, zip_center, city_center, county_center |

### incident_contaminants.csv (21 columns)

One row per contaminant per emission point — an incident with three
contaminants at two emission points produces six rows. The incident
metadata columns are repeated on each row, so you can filter by county
or date without joining. If you need the narrative text, join to
`incidents.csv` on `incident_id`.

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

One JSON object per line. Same fields as `incidents.csv`, plus nested
`facilities`, `process_units`, and `emission_points`. A real entry
looks roughly like:

```json
{
  "incident_id": "159622",
  "county": "HARRISON",
  "event_type": "AIR SHUTDOWN",
  "event_start": "10/14/2011 11:00 PM",
  "owner_name": "SOUTHWESTERN ELECTRIC POWER COMPANY",
  "physical_location": "2400 FM 3251; HALLSVILLE, TX 75650",
  "latitude": 32.472652887918,
  "longitude": -94.47314906774,
  "geocode_source": "census",
  "facilities": [
    {"name": "Unit #1 Boiler", "fin": "P-16"}
  ],
  "emission_points": [
    {
      "name": "Boiler Stack",
      "epn": "16",
      "contaminants": [
        {
          "description": "Opacity",
          "est_quantity": 20.0,
          "units": "% OPACITY",
          "emission_limit": 20.0,
          "limit_units": "% OPACITY",
          "authorization": "R6269"
        }
      ]
    }
  ]
}
```

## Pipeline

1. **1.py** — POSTs a monthly search to TCEQ, downloads the Excel results. Skips months that are already downloaded unless they're recent. Output: `eer_monthly_exports/`.
2. **2.py** — Merges all 279 monthly files into one dataset (102,551 rows), deduplicates, writes `output/eer_master_all.csv` and a matching `.parquet`.
3. **3.py** — Downloads each incident's detail HTML and emission XLS from TCEQ. 8 threads, adaptive rate limiting, logs 404s so the next run skips nonexistent incidents. Output: `incident_full_data/{month}/`.
4. **4.py** — Parses every HTML file with BeautifulSoup, geocodes addresses inline (15 threads). Caches parsed results as `.html.json` alongside each HTML so subsequent runs skip the parse. Converts XLS to `.xls.csv` on first encounter. Output: `output/incidents.jsonl`, `output/incidents.csv`, `output/incident_contaminants.csv`.
5. **5.py** — Zips the four output files so they slide under GitHub's 100 MB file size limit. Output: `.zip` versions in `output/`.

## Geocoding

Geocoding runs inline during parsing. Each address gets up to six
attempts, from street address to county center. The fallback chain
means everything resolves, but the less specific tiers give you city
or county centers rather than exact coordinates.

| Tier | Source | Notes |
|---|---|---|
| 1 | Census Bureau | Free, no key. Catches ~28% of street addresses |
| 2 | Mapbox v6 | Set `MAPBOX_TOKEN` to enable. Another ~23% |
| 3 | Nominatim | Bare city names with ", Texas" tacked on |
| 4 | ZIP center | Nominatim lookup by Texas ZIP code |
| 5 | City center | Local DB of 1,841 Texas places, instant |
| 6 | County center | Last resort. Low precision but covers everything |

Results land in `geocode_cache.jsonl`. Negative results track which
sources were tried (`sources_tried`), so adding a new geocoder later
automatically retries previously-unresolved addresses.

The city database comes from the Census Bureau gazetteer. To rebuild
it, grab `gaz_place_48.txt` from census.gov and run the snippet in
`4.py`.

Some city names that happen to be common English words (West,
Junction, Miles) are excluded from text scanning since they match
road descriptions in driving directions more often than they match
actual cities.

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
python3 5.py    # zip output files for GitHub
```
