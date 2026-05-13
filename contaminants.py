"""Generate CONTAMINANTS.md and CONTAMINANTS-full.md from output data.

Run standalone:  python3 5.py [--full]
Or imported:     from contaminants import generate
"""

import csv
import sys
from collections import defaultdict
from pathlib import Path


def generate(output_dir, full=False):
    """Generate CONTAMINANTS.md (top 50) or CONTAMINANTS-full.md (all)."""
    cont_csv = output_dir / "incident_contaminants.csv"
    if not cont_csv.exists():
        print("incident_contaminants.csv not found, run 4.py first")
        return

    # Load aliases: aggressive for top-50 summary, conservative for full
    aliases = {}
    alias_path = Path("contaminant_aliases.yaml" if not full
                      else "contaminant_aliases_conservative.yaml")
    if alias_path.exists():
        import yaml
        with alias_path.open() as f:
            raw = yaml.safe_load(f) or {}
        for canonical, variants in raw.items():
            for v in variants:
                aliases[v] = canonical

    # {name: {count, by_unit: {unit: {count, total, min, max}},
    #          by_year: {year: {count, by_unit: {unit: {...}}}}}}
    data = defaultdict(lambda: {
        "count": 0,
        "by_unit": defaultdict(lambda: {"count": 0, "total": 0, "min": None, "max": None}),
        "by_year": defaultdict(lambda: {
            "count": 0,
            "by_unit": defaultdict(lambda: {"count": 0, "total": 0, "min": None, "max": None})
        }),
    })

    with cont_csv.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get("contaminant", "")
            name = aliases.get(name, name)  # canonicalize
            unit = row.get("units", "").strip()
            data[name]["count"] += 1
            # Overall unit stats
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
            # Yearly breakdown
            # event_start is MM/DD/YYYY HH:MM AM/PM
            ds = (row.get("event_start", "") or "").split("/")
            year = ds[-1][:4] if len(ds) >= 3 else ""
            if year.isdigit():
                data[name]["by_year"][year]["count"] += 1
                try:
                    qty2 = float(row.get("est_quantity", 0) or 0)
                    yud = data[name]["by_year"][year]["by_unit"][unit]
                    yud["total"] += qty2
                    yud["count"] += 1
                    if yud["min"] is None or qty2 < yud["min"]:
                        yud["min"] = qty2
                    if yud["max"] is None or qty2 > yud["max"]:
                        yud["max"] = qty2
                except (ValueError, TypeError):
                    pass

    top = sorted(data.items(), key=lambda x: x[1]["count"], reverse=True)
    total_releases = sum(v["count"] for v in data.values())
    years = sorted({y for d in data.values() for y in d["by_year"]})

    lines = [
        "# Contaminants",
        "",
        f"{len(data)} unique compounds tracked across {total_releases:,} "
        f"reported releases, {years[0]} to {years[-1]}.",
        "",
    ]

    # ---- Yearly index ----
    lines.append("## By year")
    lines.append("")
    lines.append("| Year | Releases | Worst contaminant |")
    lines.append("|---|---|---|")
    for y in years:
        count = sum(d["by_year"].get(y, {}).get("count", 0) for d in data.values())
        worst = max(data.items(), key=lambda x: x[1]["by_year"].get(y, {}).get("count", 0))
        wname = worst[0]
        wcount = worst[1]["by_year"].get(y, {}).get("count", 0)
        wstr = f"{wname} ({wcount:,})" if wcount > 0 else ""
        note = " *(partial year)*" if y == max(years) else ""
        lines.append(f"| [{y}](#{y}) | {count:,}{note} | {wstr} |")
    lines.append("")

    # ---- Overall unit tables ----
    lines.append("## All years")
    lines.append("")

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
         "How dense the visible plume was, estimated by a trained observer "
         "comparing against the sky (EPA Method 9). 0% is invisible, 100% "
         "is completely opaque. TCEQ limits are typically 20-30%. Values "
         "above 100% are data entry errors — probably mass quantities "
         "entered in the wrong field. Not comparable with the mass tables."),
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
            for name, ud in sorted(unit_data, key=lambda x: x[1]["count"], reverse=True):
                avg = ud["total"] / ud["count"] if ud["count"] else 0
                lines.append(f"| {name} | {ud['count']} | {avg:.0f}% | {ud['min']:.0f}% | {ud['max']:.0f}% |")
        else:
            lines.append(f"| Contaminant | Releases | {unit_key} |")
            lines.append("|---|---|---|")
            ud_sorted = sorted(unit_data, key=lambda x: x[1]["total"], reverse=True)
            for name, ud in (ud_sorted if full else ud_sorted[:50]):
                lines.append(f"| {name} | {ud['count']} | {ud['total']:,.0f} |")
        lines.append("")

    # ---- Per-year POUNDS tables ----
    lines.append("## By year (POUNDS, top 20)")
    lines.append("")
    for y in years:
        yr_data = []
        for name, info in top:
            yu = info["by_year"].get(y, {}).get("by_unit", {}).get("POUNDS")
            if yu is not None and yu["count"] > 0:
                yr_data.append((name, yu))
        if not yr_data:
            continue
        lines.append(f"### {y}")
        lines.append("")
        if y == max(years):
            lines.append(f"*Data for {y} is incomplete.*")
            lines.append("")
        lines.append("| Contaminant | Releases | POUNDS |")
        lines.append("|---|---|---|")
        yr_sorted = sorted(yr_data, key=lambda x: x[1]["total"], reverse=True)
        for name, yu in (yr_sorted if full else yr_sorted[:20]):
            lines.append(f"| {name} | {yu['count']} | {yu['total']:,.0f} |")
        lines.append("")

    lines.append("")
    md_path = Path("CONTAMINANTS-full.md" if full else "CONTAMINANTS.md")
    md_path.write_text("\n".join(lines))
    print(f"Wrote {len(data)} contaminants to {md_path}")


if __name__ == "__main__":
    generate(Path("output"), full="--full" in sys.argv)
