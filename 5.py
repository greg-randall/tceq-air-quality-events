"""Compress output files that exceed GitHub's 100 MB file size limit."""

import gzip
import shutil
from pathlib import Path


def compress_file(path, keep_original=False):
    """Gzip a file in place, removing the original unless keep_original."""
    gz_path = path.with_suffix(path.suffix + ".gz")
    print(f"  {path.name} ({path.stat().st_size / 1024**2:.0f} MB) -> {gz_path.name}")
    with path.open("rb") as src, gzip.open(gz_path, "wb", compresslevel=6) as dst:
        shutil.copyfileobj(src, dst)
    if not keep_original:
        path.unlink()


def main():
    output_dir = Path("output")
    targets = [
        "incidents.csv",
        "incidents.jsonl",
        "incident_contaminants.csv",
        "eer_master_all.csv",
        "eer_master_all.parquet",
    ]

    print("Compressing output files...\n")
    for name in targets:
        path = output_dir / name
        gz_path = output_dir / (name + ".gz")
        if not path.exists():
            # Already compressed — skip or decompress then recompress
            if gz_path.exists():
                print(f"  {name} already compressed, skipping")
                continue
            print(f"  {name} not found, skipping")
            continue
        compress_file(path)


if __name__ == "__main__":
    main()
