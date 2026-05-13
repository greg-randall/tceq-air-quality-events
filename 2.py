import pandas as pd
from pathlib import Path

from tqdm import tqdm


def combine_monthly_exports():
    input_dir = Path("eer_monthly_exports")
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    xls_files = sorted(input_dir.glob("eer_*.xls"))

    print(f"Found {len(xls_files)} monthly Excel files...\n")

    all_dfs = []
    successful = 0
    failed = 0

    for file in tqdm(xls_files, desc="Reading monthly files", unit="file"):
        try:
            df = None
            for engine in ['calamine', 'pyxlsb', 'openpyxl', 'xlrd']:
                try:
                    df = pd.read_excel(file, engine=engine)
                    break
                except Exception:
                    continue

            if df is None:
                raise Exception("All engines failed")

            df = df.dropna(how='all').copy()
            df['source_month'] = file.stem.replace("eer_", "")
            df.columns = [str(col).strip() for col in df.columns]

            # Save per-file CSV alongside the .xls
            csv_path = file.with_suffix(".csv")
            df.to_csv(csv_path, index=False)

            all_dfs.append(df)
            successful += 1
            tqdm.write(f"OK {file.name} -> {len(df):,} rows")

        except Exception as e:
            tqdm.write(f"FAIL {file.name} -> {e}")
            failed += 1

    if not all_dfs:
        print("\nNo files could be read.")
        print("Try this command:")
        print("   python3 -m pip install python-calamine pyxlsb openpyxl --upgrade")
        return

    print(f"\nMerging {len(all_dfs)} files...")
    master_df = pd.concat(all_dfs, ignore_index=True)

    master_df = master_df.drop_duplicates(ignore_index=True)

    master_csv = output_dir / "eer_master_all.csv"
    master_parquet = output_dir / "eer_master_all.parquet"

    master_df.to_csv(master_csv, index=False)
    master_df.to_parquet(master_parquet, index=False)

    print("\n" + "=" * 80)
    print("COMBINATION FINISHED")
    print(f"Total rows      : {len(master_df):,}")
    print(f"Columns         : {len(master_df.columns)}")
    print(f"Files processed : {successful} successful / {len(xls_files)} total")
    print("\nFiles saved:")
    print(f"   -> {master_csv}")
    print(f"   -> {master_parquet}")
    print("=" * 80)

    print("\nSample columns:", master_df.columns.tolist()[:10])


if __name__ == "__main__":
    combine_monthly_exports()
