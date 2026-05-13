import json
import csv
import os
import random
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from bs4 import BeautifulSoup
from tqdm import tqdm

from geocode import extract_city_zip, geocode_address, geocode_city_county, geocode_county, geocode_zip, get_cache_stats


def _xls_to_csv(xls_path, csv_path):
    """Convert a TCEQ emissions XLS file to CSV."""
    import pandas as pd
    df = pd.read_excel(xls_path, engine="calamine")
    df.to_csv(csv_path, index=False)


def _write_contaminants_md(output_dir):
    """Generate CONTAMINANTS.md from the contaminant CSV."""
    from collections import defaultdict

    cont_csv = output_dir / "incident_contaminants.csv"
    if not cont_csv.exists():
        print("incident_contaminants.csv not found, run 4.py first")
        return

    data = defaultdict(lambda: {"count": 0, "by_unit": defaultdict(
        lambda: {"count": 0, "total": 0, "min": None, "max": None})})
    with cont_csv.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("contaminant", "")
            unit = row.get("units", "").strip()
            data[name]["count"] += 1
            try:
                qty = float(row.get("est_quantity", 0) or 0)
                ud = data[name]["by_unit"][unit]
                ud["total"] += qty
                ud["count"] += 1
                if ud["min"] is None or qty < ud["min"]:
                    ud["min"] = qty
                if ud["max"] is None or qty > ud["max"]:
                    ud["max"] = qty
            except (ValueError, TypeError):
                pass

    top = sorted(data.items(), key=lambda x: x[1]["count"], reverse=True)

    lines = [
        "# Contaminants",
        "",
        f"{len(data)} unique compounds tracked across {sum(v['count'] for v in data.values()):,} reported releases.",
        "",
    ]

    tables = [
        ("POUNDS",
         "Total mass released across all events. Each row shows the sum of all "
         "reported quantities for that contaminant in pounds, plus how many "
         "individual releases made up that total."),
        ("LBS/HR",
         "Instantaneous emission rate in pounds per hour. This is a rate at a "
         "moment in time, not a total — you can't compare these directly with "
         "the POUNDS table without knowing how long each release lasted."),
        ("TONS/YR",
         "Annualized estimate in tons per year. A small number here can mean "
         "either a small release or a short-duration event annualized. These "
         "are estimates, not measured totals."),
        ("% OPACITY",
         "How dense the visible plume was, as a percentage. This is about "
         "appearance, not mass — 100% means you can't see through it at all. "
         "Not comparable with the mass tables above."),
    ]

    for unit_key, desc in tables:
        unit_data = []
        for name, info in top:
            ud = info["by_unit"].get(unit_key)
            if ud is not None and ud["count"] > 0:
                unit_data.append((name, ud))
        if not unit_data:
            continue

        lines.append(f"### {unit_key}")
        lines.append("")
        lines.append(desc)
        lines.append("")

        if unit_key == "% OPACITY":
            lines.append("| Contaminant | Releases | Average | Min | Max |")
            lines.append("|---|---|---|---|---|")
            for name, ud in sorted(unit_data, key=lambda x: x[1]["count"], reverse=True)[:30]:
                avg = ud["total"] / ud["count"] if ud["count"] else 0
                lines.append(f"| {name} | {ud['count']} | {avg:.0f}% | {ud['min']:.0f}% | {ud['max']:.0f}% |")
        else:
            lines.append(f"| Contaminant | Releases | {unit_key} |")
            lines.append("|---|---|---|")
            for name, ud in sorted(unit_data, key=lambda x: x[1]["total"], reverse=True)[:50]:
                lines.append(f"| {name} | {ud['count']} | {ud['total']:,.0f} |")
        lines.append("")
    md_path = Path("CONTAMINANTS.md")
    md_path.write_text("\n".join(lines))
    print(f"Wrote {len(data)} contaminants to {md_path}")


def get_text(td):
    """Extract stripped text from a BeautifulSoup tag, return None if empty/nbsp."""
    if td is None:
        return None
    text = td.get_text(strip=True)
    if not text or text == '\xa0':
        return None
    return text


def table_rows(soup, summary):
    """Return all <tr> rows from a table matching the summary attribute."""
    table = soup.find('table', summary=summary)
    if not table:
        return []
    return table.find_all('tr')


def extract_key_values(rows):
    """Walk <th>/<td> pairs from table rows into a flat dict."""
    data = {}
    for row in rows:
        cells = row.find_all(['th', 'td'])
        key = None
        for cell in cells:
            text = get_text(cell)
            if text is None:
                continue
            if cell.name == 'th':
                key = text.rstrip(':').strip()
            elif cell.name == 'td' and key is not None:
                if key not in data:
                    data[key] = text
                key = None
    return data


def parse_incident_metadata(soup):
    """Extract top-level incident info from the incident+owner and duration tables."""
    data = {}

    # Table 1: Incident and owner section
    rows = table_rows(soup, "Incident and owner section")
    data.update(extract_key_values(rows))

    # Table 2: Event duration
    rows = table_rows(soup, "displays event duration section")
    data.update(extract_key_values(rows))

    # Table 7: Initial notification
    rows = table_rows(soup, "displays initial notification and jurisdiction section")
    data.update(extract_key_values(rows))

    # Normalize key names to snake_case
    key_map = {
        'Incident Tracking Number': 'incident_id',
        'Incident Status': 'incident_status',
        'Report Type': 'report_type',
        'Report Date': 'report_date',
        'Name of Owner or Operator': 'owner_name',
        'CN': 'cn',
        'Regulated Entity Name': 'regulated_entity_name',
        'RN': 'rn',
        'Physical Location': 'physical_location',
        'County': 'county',
        'Event/Activity Type': 'event_type',
        'Date and Time Event Discovered or Scheduled Activity Start': 'event_start',
        'Date and Time Event or Scheduled Activity Ended': 'event_end',
        'Event Duration': 'event_duration',
        'Initial Notification Date/Time': 'notification_date',
        'Method': 'notification_method',
        'Notification Jurisdictions': 'notification_jurisdictions',
        'Publication Status': 'publication_status',
    }

    normalized = {}
    for old_key, new_key in key_map.items():
        if old_key in data:
            normalized[new_key] = data[old_key]

    return normalized


def parse_process_units(soup):
    """Extract process unit names."""
    rows = table_rows(soup, "Process area or unit list")
    units = []
    for row in rows:
        tds = row.find_all('td')
        for td in tds:
            text = get_text(td)
            if text:
                units.append(text)
    return units


def parse_facilities(soup):
    """Extract facility names and FINs."""
    rows = table_rows(soup, "Facility list section")
    facilities = []
    for row in rows:
        tds = row.find_all('td')
        if len(tds) >= 2:
            name = get_text(tds[0])
            fin = get_text(tds[1])
            if name and fin:
                facilities.append({'name': name, 'fin': fin})
    return facilities


def parse_emission_points(soup):
    """Extract emission points with nested contaminants."""
    tables = soup.find_all('table', summary=re.compile(r'^Emission point'))
    points = []

    for table in tables:
        rows = table.find_all('tr')

        ep_name = None
        ep_number = None

        # Look for name/number row: first row with <th> containing "Emission Point"
        for i, row in enumerate(rows):
            ths = row.find_all('th')
            th_texts = [get_text(t) for t in ths]
            combined = ' '.join(t for t in th_texts if t)
            if 'Emission Point Common Name' in combined:
                # The next row has the values
                if i + 1 < len(rows):
                    vals = rows[i + 1].find_all('td')
                    if len(vals) >= 2:
                        ep_name = get_text(vals[0])
                        ep_number = get_text(vals[1])
                break

        # If both name and number are empty/nbsp, skip this emission point
        if not ep_name and not ep_number:
            continue

        # Parse contaminant rows
        contaminants = []
        header_found = False
        for row in rows:
            ths = row.find_all('th')
            th_texts = [get_text(t) for t in ths]

            # Detect the contaminant header row
            if any('Description' in (t or '') for t in th_texts):
                header_found = True
                continue

            if not header_found:
                continue

            tds = row.find_all('td')
            if len(tds) < 6:
                continue

            desc = get_text(tds[0])
            est_qty = get_text(tds[1])
            units = get_text(tds[2])
            limit = get_text(tds[3])
            limit_units = get_text(tds[4])
            auth = get_text(tds[5])

            # Skip entirely empty contaminant rows
            if not any([desc, est_qty, units, limit, limit_units, auth]):
                continue

            # Convert numeric strings
            try:
                est_qty = float(est_qty) if est_qty else None
            except (ValueError, TypeError):
                pass
            try:
                limit = float(limit) if limit else None
            except (ValueError, TypeError):
                pass

            contaminants.append({
                'description': desc,
                'est_quantity': est_qty,
                'units': units,
                'emission_limit': limit,
                'limit_units': limit_units,
                'authorization': auth,
            })

        points.append({
            'name': ep_name,
            'epn': ep_number,
            'contaminants': contaminants,
        })

    return points


def parse_comments(soup):
    """Extract the three comment text blocks."""
    rows = table_rows(soup, "displays comment section")
    texts = {}
    key_order = ['cause', 'actions', 'basis']
    idx = 0

    for row in rows:
        ths = row.find_all('th')
        tds = row.find_all('td')
        if ths:
            # This row contains a label, next row (if td) has the text
            continue
        if tds and idx < len(key_order):
            text = get_text(tds[0])
            if text:
                texts[key_order[idx]] = text
            idx += 1

    return texts


def parse_html(filepath):
    """Parse a single incident HTML file into a nested dict."""
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        soup = BeautifulSoup(f.read(), 'lxml')

    incident = parse_incident_metadata(soup)
    incident['process_units'] = parse_process_units(soup)
    incident['facilities'] = parse_facilities(soup)
    incident['emission_points'] = parse_emission_points(soup)
    incident.update(parse_comments(soup))

    # Use filename-based id as fallback
    if not incident.get('incident_id'):
        stem = Path(filepath).stem
        parts = stem.rsplit('_', 1)
        if len(parts) == 2:
            incident['incident_id'] = parts[1]

    return incident


def process_all():
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    # CLI: --debug (random 10), --limit N, --force-regen, --contaminants
    limit = None
    debug = False
    force_regen = "--force-regen" in sys.argv
    gen_contaminants = "--contaminants" in sys.argv
    args = sys.argv[1:]
    if "--debug" in args:
        limit = 10
        debug = True
        print("[debug mode] 10 random files\n")
    for i, arg in enumerate(args):
        if arg == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1])
            print(f"[--limit] limiting to {limit} files\n")
            break
    if force_regen:
        print("[--force-regen] ignoring cached .html.json and .xls.csv\n")
    if gen_contaminants:
        # Just regenerate CONTAMINANTS.md from existing output, then exit
        _write_contaminants_md(output_dir)
        return

    input_dir = Path("incident_full_data")
    html_files = sorted(input_dir.rglob("*.html"))

    if not html_files:
        print("No HTML files found in incident_full_data/. Run script 3 first.")
        return

    if limit and limit < len(html_files):
        if debug:
            html_files = random.sample(html_files, limit)
        else:
            html_files = html_files[:limit]

    print(f"Parsing {len(html_files)} HTML files...\n")

    workers = max(os.cpu_count() - 1, 1)
    print(f"Using {workers} threads")

    def parse_one(fp):
        json_cache = fp.with_suffix(fp.suffix + ".json")
        if not force_regen and json_cache.exists():
            try:
                with json_cache.open() as f:
                    inc = json.load(f)
            except (json.JSONDecodeError, OSError) as e:
                tqdm.write(f"BAD CACHE {json_cache.name} -> {e}")
                inc = None
            if inc is not None:
                # Re-geocode in case new geocoders are available
                _geocode_incident(inc)
                return inc
            # Fall through to re-parse

        try:
            inc = parse_html(fp)
        except Exception as e:
            tqdm.write(f"FAIL {fp.name} -> {e}")
            return None

        # Geocode inline so cache populates as we parse
        _geocode_incident(inc)

        # Cache the parsed result (pre-geocode, or post? post is more useful)
        try:
            with json_cache.open("w") as f:
                json.dump(inc, f, default=str)
        except OSError as e:
            tqdm.write(f"WRITE CACHE {json_cache.name} -> {e}")

        # Convert XLS to CSV alongside
        xls_path = fp.with_suffix(".xls")
        csv_path = xls_path.with_suffix(xls_path.suffix + ".csv")
        if xls_path.exists() and (force_regen or not csv_path.exists()):
            try:
                _xls_to_csv(xls_path, csv_path)
            except Exception as e:
                tqdm.write(f"XLS {xls_path.name} -> {e}")

        return inc

    def _geocode_incident(inc):
        loc = inc.get("physical_location", "")
        geo = None
        if loc:
            geo = geocode_address(loc)
            if geo is None:
                city, zipcode = extract_city_zip(loc)
                if zipcode:
                    geo = geocode_zip(zipcode)
                if geo is None and city:
                    county = inc.get("county", "")
                    if county:
                        geo = geocode_city_county(city, county)
            if geo is None:
                county = inc.get("county", "")
                if county:
                    geo = geocode_county(county)
        inc["latitude"] = geo["lat"] if geo else None
        inc["longitude"] = geo["lon"] if geo else None
        inc["geocode_source"] = geo["source"] if geo else None

    incidents = []
    errors = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = list(tqdm(
            executor.map(parse_one, html_files),
            total=len(html_files),
            desc="Parsing HTML",
            unit="file",
        ))

    incidents = [r for r in results if r is not None]
    errors = len(html_files) - len(incidents)

    print(f"\nParsed {len(incidents)} incidents ({errors} errors)")

    # ---- Geocode stats ----
    geo_hits = sum(1 for inc in incidents if inc.get("latitude") is not None)
    cache_hits, cache_misses = get_cache_stats()
    print(f"Geocoded {geo_hits}/{len(incidents)} incidents ({cache_hits} cache hits, {cache_misses} API calls)")

    # ---- JSONL ----
    jsonl_path = output_dir / "incidents.jsonl"
    with open(jsonl_path, 'w', encoding='utf-8') as f:
        for inc in incidents:
            f.write(json.dumps(inc, ensure_ascii=False, default=str) + '\n')
    print(f"JSONL -> {jsonl_path}")

    # ---- Incidents CSV (flat, no nested data) ----
    flat_fields = [
        'incident_id', 'incident_status', 'report_type', 'report_date',
        'owner_name', 'cn', 'regulated_entity_name', 'rn',
        'physical_location', 'county', 'event_type',
        'event_start', 'event_end', 'event_duration',
        'notification_date', 'notification_method', 'notification_jurisdictions',
        'publication_status', 'cause', 'actions', 'basis',
        'latitude', 'longitude', 'geocode_source',
    ]

    inc_csv_path = output_dir / "incidents.csv"
    with open(inc_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=flat_fields, extrasaction='ignore')
        writer.writeheader()
        for inc in incidents:
            writer.writerow(inc)
    print(f"CSV  -> {inc_csv_path} ({len(incidents)} rows)")

    # ---- Contaminants CSV (denormalized, one row per contaminant) ----
    cont_fields = [
        'incident_id', 'incident_status', 'report_type', 'report_date',
        'owner_name', 'regulated_entity_name', 'county', 'event_type',
        'event_start', 'event_end',
        'ep_name', 'epn',
        'contaminant', 'est_quantity', 'units',
        'emission_limit', 'limit_units', 'authorization',
        'latitude', 'longitude', 'geocode_source',
    ]

    cont_csv_path = output_dir / "incident_contaminants.csv"
    cont_count = 0
    with open(cont_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=cont_fields, extrasaction='ignore')
        writer.writeheader()
        for inc in incidents:
            for ep in inc.get('emission_points', []):
                for cont in ep.get('contaminants', []):
                    row = {
                        'incident_id': inc.get('incident_id'),
                        'incident_status': inc.get('incident_status'),
                        'report_type': inc.get('report_type'),
                        'report_date': inc.get('report_date'),
                        'owner_name': inc.get('owner_name'),
                        'regulated_entity_name': inc.get('regulated_entity_name'),
                        'county': inc.get('county'),
                        'event_type': inc.get('event_type'),
                        'event_start': inc.get('event_start'),
                        'event_end': inc.get('event_end'),
                        'ep_name': ep.get('name'),
                        'epn': ep.get('epn'),
                        'contaminant': cont.get('description'),
                        'est_quantity': cont.get('est_quantity'),
                        'units': cont.get('units'),
                        'emission_limit': cont.get('emission_limit'),
                        'limit_units': cont.get('limit_units'),
                        'authorization': cont.get('authorization'),
                        'latitude': inc.get('latitude'),
                        'longitude': inc.get('longitude'),
                        'geocode_source': inc.get('geocode_source'),
                    }
                    writer.writerow(row)
                    cont_count += 1
    print(f"CSV  -> {cont_csv_path} ({cont_count} contaminant rows)")

    print("\nDone.")


if __name__ == "__main__":
    process_all()
