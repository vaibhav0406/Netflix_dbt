
print("Script started")

"""
load_raw_data.py
────────────────────────────────────────────────────────────────────────────────
Loads CSV files from a local folder into DuckDB as raw tables.

HOW TO USE
──────────
1. Set DATA_FOLDER to the folder where your CSV files live.
2. Set DB_PATH to the path of your DuckDB file (your dev.duckdb).
3. Run:  python scripts/load_raw_data.py

WHAT IT DOES
────────────
- Scans DATA_FOLDER for every .csv file
- Creates a 'raw' schema in DuckDB if it doesn't exist
- For each CSV: drops the existing table (if any), recreates it fresh
- Detects column types automatically
- Prints a summary showing row counts and any errors

SECOND RUN BEHAVIOUR
─────────────────────
Every run does a full drop + reload. If a file hasn't changed, it still
reloads. This keeps things simple and predictable — you always know the
table matches the CSV exactly.
"""

import duckdb
import os
import sys
import time
from pathlib import Path

# ── ❶  CONFIGURE THESE TWO PATHS ─────────────────────────────────────────────

# Folder where the CSV files are stored
DATA_FOLDER = r"C:\Users\vaibh\Downloads\Netflix_data" 

# Path to your DuckDB database file
DB_PATH = r"C:\Users\vaibh\OneDrive\Documents\DBT Project\netflix\dev.duckdb"

# ── ❷  OPTIONAL SETTINGS ─────────────────────────────────────────────────────

# Name of the schema that will be created in DuckDB
RAW_SCHEMA = "raw"

# How many rows to sample when auto-detecting column types
# Higher = more accurate detection, slightly slower
SAMPLE_SIZE = 20_000

# If True, prints the column names and types detected for each file
SHOW_COLUMN_TYPES = True

# ─────────────────────────────────────────────────────────────────────────────


def format_number(n: int) -> str:
    """Format large numbers with commas for readability."""
    return f"{n:,}"


def clean_table_name(filename: str) -> str:
    """
    Convert a filename to a clean SQL table name.
    Examples:
        raw_bets.csv          → raw_bets
        Customer Data.csv     → customer_data
        2024-transactions.csv → _2024_transactions
    """
    name = Path(filename).stem          # remove .csv extension
    name = name.lower()                 # lowercase
    name = name.replace(" ", "_")       # spaces to underscores
    name = name.replace("-", "_")       # hyphens to underscores
    name = name.replace(".", "_")       # dots to underscores
    # If name starts with a digit, prefix with underscore (invalid SQL identifier)
    if name[0].isdigit():
        name = "_" + name
    return name


def load_csv_to_duckdb(con: duckdb.DuckDBPyConnection,
                        csv_path: Path,
                        schema: str,
                        sample_size: int,
                        show_types: bool) -> dict:
    """
    Load a single CSV file into DuckDB.

    Returns a dict with:
        success   bool
        table     str   — fully qualified table name
        rows      int   — number of rows loaded
        columns   int   — number of columns
        error     str   — error message if success is False
        duration  float — seconds taken
    """
    table_name = clean_table_name(csv_path.name)
    full_table = f"{schema}.{table_name}"
    start      = time.time()

    try:
        # ── Drop existing table ───────────────────────────────────────────────
        con.execute(f"DROP TABLE IF EXISTS {full_table}")

        # ── Create fresh table from CSV ───────────────────────────────────────
        # read_csv_auto detects:
        #   - delimiter (, or ; or | or tab)
        #   - column types (integer, float, varchar, date, timestamp, boolean)
        #   - null values ("", "NULL", "null", "NA", "N/A", "nan")
        #   - date and timestamp formats
        con.execute(f"""
            CREATE TABLE {full_table} AS
            SELECT *
            FROM read_csv_auto(
                '{csv_path.as_posix()}',
                header        = true,
                sample_size   = {sample_size},
                null_padding  = true,
                ignore_errors = false
            )
        """)

        # ── Get stats ─────────────────────────────────────────────────────────
        row_count = con.execute(
            f"SELECT COUNT(*) FROM {full_table}"
        ).fetchone()[0]

        col_info = con.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
              AND table_name   = '{table_name}'
            ORDER BY ordinal_position
        """).fetchall()

        duration = time.time() - start

        # ── Optionally print column types ─────────────────────────────────────
        if show_types:
            print(f"\n    Columns detected:")
            for col_name, col_type in col_info:
                print(f"      {col_name:<35} {col_type}")

        return {
            "success":  True,
            "table":    full_table,
            "rows":     row_count,
            "columns":  len(col_info),
            "error":    None,
            "duration": duration,
        }

    except Exception as e:
        duration = time.time() - start
        return {
            "success":  False,
            "table":    full_table,
            "rows":     0,
            "columns":  0,
            "error":    str(e),
            "duration": duration,
        }


def main():
    print("=" * 70)
    print("  DuckDB Raw Data Loader")
    print("=" * 70)

    # ── Validate paths ────────────────────────────────────────────────────────
    data_folder = Path(DATA_FOLDER)
    db_path     = Path(DB_PATH)

    if not data_folder.exists():
        print(f"\n  ERROR: DATA_FOLDER does not exist:")
        print(f"         {data_folder}")
        print(f"\n  Fix:   Update the DATA_FOLDER variable at the top of this script.")
        sys.exit(1)

    if not db_path.parent.exists():
        print(f"\n  ERROR: The folder for DB_PATH does not exist:")
        print(f"         {db_path.parent}")
        print(f"\n  Fix:   Update the DB_PATH variable at the top of this script.")
        sys.exit(1)

    # ── Find CSV files ────────────────────────────────────────────────────────
    csv_files = sorted(data_folder.glob("*.csv"))

    if not csv_files:
        print(f"\n  No CSV files found in: {data_folder}")
        print(f"  Check that DATA_FOLDER is set to the correct path.")
        sys.exit(1)

    print(f"\n  Data folder : {data_folder}")
    print(f"  Database    : {db_path}")
    print(f"  Schema      : {RAW_SCHEMA}")
    print(f"  CSV files   : {len(csv_files)} found")
    print()

    for f in csv_files:
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"    {f.name:<45}  {size_mb:>8.1f} MB")

    print()

    # ── Connect to DuckDB ─────────────────────────────────────────────────────
    print(f"  Connecting to DuckDB...")
    con = duckdb.connect(str(db_path))

    # Create raw schema
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
    print(f"  Schema '{RAW_SCHEMA}' ready")
    print()
    print("-" * 70)

    # ── Load each file ────────────────────────────────────────────────────────
    results   = []
    total_start = time.time()

    for i, csv_path in enumerate(csv_files, 1):
        size_mb = csv_path.stat().st_size / (1024 * 1024)
        print(f"\n  [{i}/{len(csv_files)}]  {csv_path.name}  ({size_mb:.1f} MB)")

        result = load_csv_to_duckdb(
            con        = con,
            csv_path   = csv_path,
            schema     = RAW_SCHEMA,
            sample_size= SAMPLE_SIZE,
            show_types = SHOW_COLUMN_TYPES,
        )
        results.append(result)

        if result["success"]:
            print(
                f"\n    ✓  {result['table']}"
                f"  |  {format_number(result['rows'])} rows"
                f"  |  {result['columns']} columns"
                f"  |  {result['duration']:.1f}s"
            )
        else:
            print(f"\n    ✗  FAILED: {result['table']}")
            print(f"       Error: {result['error']}")

    # ── Summary ───────────────────────────────────────────────────────────────
    total_duration = time.time() - total_start
    successful     = [r for r in results if r["success"]]
    failed         = [r for r in results if not r["success"]]
    total_rows     = sum(r["rows"] for r in successful)

    print()
    print("=" * 70)
    print("  SUMMARY")
    print("=" * 70)
    print(f"  Files processed : {len(results)}")
    print(f"  Successful      : {len(successful)}")
    print(f"  Failed          : {len(failed)}")
    print(f"  Total rows      : {format_number(total_rows)}")
    print(f"  Total time      : {total_duration:.1f}s")

    if successful:
        print()
        print("  Tables loaded:")
        for r in successful:
            print(f"    ✓  {r['table']:<45}  {format_number(r['rows']):>10} rows")

    if failed:
        print()
        print("  Failed files:")
        for r in failed:
            print(f"    ✗  {r['table']}")
            print(f"       {r['error']}")

    print()

    # ── Verification query ────────────────────────────────────────────────────
    # Runs a quick count of all tables in the raw schema so you can
    # confirm everything loaded correctly at a glance
    print("  Verification — row counts from DuckDB:")
    print()

    try:
        tables_in_db = con.execute(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{RAW_SCHEMA}'
            ORDER BY table_name
        """).fetchall()

        for (tbl,) in tables_in_db:
            count = con.execute(
                f"SELECT COUNT(*) FROM {RAW_SCHEMA}.{tbl}"
            ).fetchone()[0]
            print(f"    {RAW_SCHEMA}.{tbl:<40}  {format_number(count):>10} rows")
    except Exception as e:
        print(f"    Could not run verification: {e}")

    print()
    con.close()

    if failed:
        print("  Some files failed to load. See errors above.")
        print("  Common causes:")
        print("    - File is open in Excel (close it and retry)")
        print("    - Mixed data types in a column (e.g. numbers and text mixed)")
        print("    - File encoding issue (try saving as UTF-8 CSV from Excel)")
        print()
        sys.exit(1)
    else:
        print("  All files loaded successfully.")
        print("  Open DBeaver, refresh your connection, and expand the")
        print(f"  '{RAW_SCHEMA}' schema to see your tables.")
        print()


if __name__ == "__main__":
    main()