"""Compress output files that exceed GitHub's 100 MB file size limit.

Uses zip so non-technical folks can open them without installing anything.
"""

import zipfile
from pathlib import Path


def compress_file(path, keep_original=False):
    """Zip a file in place, removing the original unless keep_original."""
    zip_path = path.with_suffix(path.suffix + ".zip")
    print(f"  {path.name} ({path.stat().st_size / 1024**2:.0f} MB) -> {zip_path.name}")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        zf.write(path, path.name)
    if not keep_original:
        path.unlink()


def main():
    output_dir = Path("output")
    targets = [
        "incidents.csv",
        "incidents.jsonl",
        "incident_contaminants.csv",
        "eer_master_all.csv",
    ]

    print("Compressing output files...\n")
    for name in targets:
        path = output_dir / name
        zip_path = path.with_suffix(path.suffix + ".zip")
        if not path.exists():
            if zip_path.exists():
                print(f"  {name} already compressed, skipping")
                continue
            print(f"  {name} not found, skipping")
            continue
        compress_file(path)


if __name__ == "__main__":
    main()
