"""
load_to_snowflake.py

Loads Instacart CSVs into Snowflake's RAW schema with zero transformation --
exact source fidelity. This is the EL half of an ELT pipeline: dbt (the T half)
takes over from here, reading from RAW and building staging/mart models.

Why no transformation happens here: keeping ingestion "dumb" (just land what's in
the source) means the raw layer is always a faithful, re-creatable mirror of the
source files. If a transformation rule later turns out to be wrong, you can fix it
in dbt and re-run -- you never have to re-ingest, because the raw data was never
mutated by a transformation decision in the first place.

Credentials are read from environment variables, never hardcoded and never committed:
    SNOWFLAKE_ACCOUNT    e.g. kt84139.us-east-2.aws
    SNOWFLAKE_USER
    SNOWFLAKE_PASSWORD
    SNOWFLAKE_WAREHOUSE  e.g. instacart_wh
    SNOWFLAKE_DATABASE   e.g. instacart_db
    SNOWFLAKE_SCHEMA     e.g. raw

Usage:
    python load_to_snowflake.py --source data/raw_sample
    python load_to_snowflake.py --source ~/Downloads/instacart_full   # full dataset, later
"""

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Maps each source CSV to the raw table it should land in, and the explicit dtype
# to read it as. Reading explicitly (rather than letting pandas infer types) means
# the eventual Snowflake column types are intentional, not accidental -- e.g.
# order_hour_of_day is read as a string here to preserve its zero-padded raw form
# (see BUILD_LOG.md, messiness finding #2); staging will cast it to an integer
# deliberately, not as a side effect of how pandas guessed the column.
TABLE_MAP = {
    "orders.csv": {
        "table": "RAW_ORDERS",
        "dtype": {
            "order_id": "int64", "user_id": "int64", "eval_set": "string",
            "order_number": "int64", "order_dow": "int64",
            "order_hour_of_day": "string",  # preserve zero-padding, e.g. "08"
            "days_since_prior_order": "float64",  # float to allow NaN (first orders)
        },
    },
    "products.csv": {
        "table": "RAW_PRODUCTS",
        "dtype": {"product_id": "int64", "product_name": "string",
                   "aisle_id": "int64", "department_id": "int64"},
    },
    "aisles.csv": {
        "table": "RAW_AISLES",
        "dtype": {"aisle_id": "int64", "aisle": "string"},
    },
    "departments.csv": {
        "table": "RAW_DEPARTMENTS",
        "dtype": {"department_id": "int64", "department": "string"},
    },
    "order_products__prior.csv": {
        "table": "RAW_ORDER_PRODUCTS_PRIOR",
        "dtype": {"order_id": "int64", "product_id": "int64",
                   "add_to_cart_order": "int64", "reordered": "int64"},
    },
    "order_products__train.csv": {
        "table": "RAW_ORDER_PRODUCTS_TRAIN",
        "dtype": {"order_id": "int64", "product_id": "int64",
                   "add_to_cart_order": "int64", "reordered": "int64"},
    },
}

REQUIRED_ENV_VARS = [
    "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA",
]


def get_connection():
    missing = [v for v in REQUIRED_ENV_VARS if not os.environ.get(v)]
    if missing:
        print(f"Missing required environment variables: {', '.join(missing)}")
        print("Set these before running, e.g.: export SNOWFLAKE_ACCOUNT=kt84139.us-east-2.aws")
        sys.exit(1)

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )


def load_file(conn, source_dir: Path, filename: str, table: str, dtype: dict) -> None:
    filepath = source_dir / filename
    if not filepath.exists():
        print(f"  SKIP: {filepath} not found")
        return

    print(f"  Reading {filename} ...")
    df = pd.read_csv(filepath, dtype=dtype)
    df.columns = [c.upper() for c in df.columns]
    print(f"  Loading {len(df):,} rows into RAW.{table} ...")
    # write_pandas creates the table if it doesn't exist, and replaces its contents
    # by default truncating first -- appropriate here since this is a full re-land
    # of the raw layer each run, not an incremental raw load. Incremental logic
    # belongs in dbt's mart layer (fact_orders), not in this ingestion step --
    # see BUILD_LOG.md for why that boundary was drawn there.
    success, nchunks, nrows, _ = write_pandas(
        conn, df, table_name=table, auto_create_table=True, overwrite=True,
    )
    print(f"  -> wrote {nrows:,} rows to RAW.{table} (success={success})")


def main(source: str) -> None:
    source_dir = Path(source).expanduser()
    if not source_dir.exists():
        print(f"Source directory does not exist: {source_dir}")
        sys.exit(1)

    print(f"Loading from: {source_dir}\n")
    conn = get_connection()
    try:
        for filename, cfg in TABLE_MAP.items():
            load_file(conn, source_dir, filename, cfg["table"], cfg["dtype"])
            print()
    finally:
        conn.close()

    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Instacart CSVs into Snowflake RAW schema.")
    parser.add_argument("--source", required=True, help="Directory containing the source CSVs (e.g. data/raw_sample)")
    args = parser.parse_args()
    main(args.source)