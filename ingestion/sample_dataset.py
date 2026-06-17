"""
sample_dataset.py

Pulls a referentially-consistent subset of the Instacart Market Basket dataset for
fast local iteration on the dbt project, before scaling up to the full dataset.

Sampling strategy: anchor on a random sample of user_ids, then pull every order those
users placed, every order_products line tied to those orders, and every product/aisle/
department referenced by those lines. This guarantees no orphaned foreign keys in the
sample -- a naive `head -N` per file would break referential integrity and cause dbt
`relationships` tests to fail for reasons unrelated to real data quality.

Usage:
    python sample_dataset.py --n-users 5000

Input:  CSVs at SOURCE_DIR (the full unzipped Kaggle download)
Output: CSVs at OUTPUT_DIR (data/raw_sample/ in the repo), same filenames/schemas
"""

import argparse
import pandas as pd
from pathlib import Path

SOURCE_DIR = Path.home() / "Downloads" / "instacart_full"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data" / "raw_sample"


def sample(n_users: int, seed: int) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Reading orders.csv from {SOURCE_DIR} ...")
    # order_hour_of_day must be read as a string, not inferred, to preserve its
    # zero-padded raw form (e.g. "08"). Without this, pandas infers it as an integer
    # and silently drops the leading zero -- and once that happens here, at the
    # sampling step, no amount of correct dtype handling later in the ingestion
    # script can recover it, since the information is already gone from the
    # sampled CSV written to disk. (Found via spot-check after ingestion -- see
    # BUILD_LOG.md.)
    orders = pd.read_csv(SOURCE_DIR / "orders.csv", dtype={"order_hour_of_day": "string"})

    all_user_ids = orders["user_id"].unique()
    rng = pd.Series(all_user_ids).sample(n=n_users, random_state=seed)
    sampled_user_ids = set(rng)
    print(f"Sampled {len(sampled_user_ids)} users out of {len(all_user_ids)} total.")

    orders_sample = orders[orders["user_id"].isin(sampled_user_ids)].copy()
    sampled_order_ids = set(orders_sample["order_id"])
    print(f"Sampled users placed {len(sampled_order_ids)} orders.")

    print("Reading and filtering order_products__prior.csv ...")
    prior = pd.read_csv(SOURCE_DIR / "order_products__prior.csv")
    prior_sample = prior[prior["order_id"].isin(sampled_order_ids)].copy()

    print("Reading and filtering order_products__train.csv ...")
    train = pd.read_csv(SOURCE_DIR / "order_products__train.csv")
    train_sample = train[train["order_id"].isin(sampled_order_ids)].copy()

    sampled_product_ids = set(prior_sample["product_id"]) | set(train_sample["product_id"])
    print(f"Sample references {len(sampled_product_ids)} distinct products.")

    print("Reading and filtering products.csv ...")
    products = pd.read_csv(SOURCE_DIR / "products.csv")
    products_sample = products[products["product_id"].isin(sampled_product_ids)].copy()

    sampled_aisle_ids = set(products_sample["aisle_id"])
    sampled_department_ids = set(products_sample["department_id"])

    print("Reading and filtering aisles.csv and departments.csv ...")
    aisles = pd.read_csv(SOURCE_DIR / "aisles.csv")
    aisles_sample = aisles[aisles["aisle_id"].isin(sampled_aisle_ids)].copy()

    departments = pd.read_csv(SOURCE_DIR / "departments.csv")
    departments_sample = departments[departments["department_id"].isin(sampled_department_ids)].copy()

    orders_sample.to_csv(OUTPUT_DIR / "orders.csv", index=False)
    prior_sample.to_csv(OUTPUT_DIR / "order_products__prior.csv", index=False)
    train_sample.to_csv(OUTPUT_DIR / "order_products__train.csv", index=False)
    products_sample.to_csv(OUTPUT_DIR / "products.csv", index=False)
    aisles_sample.to_csv(OUTPUT_DIR / "aisles.csv", index=False)
    departments_sample.to_csv(OUTPUT_DIR / "departments.csv", index=False)

    print("\nSample written to:", OUTPUT_DIR)
    print(f"  orders.csv:                 {len(orders_sample):,} rows")
    print(f"  order_products__prior.csv:  {len(prior_sample):,} rows")
    print(f"  order_products__train.csv:  {len(train_sample):,} rows")
    print(f"  products.csv:                {len(products_sample):,} rows")
    print(f"  aisles.csv:                  {len(aisles_sample):,} rows")
    print(f"  departments.csv:             {len(departments_sample):,} rows")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sample the Instacart dataset down to a referentially-consistent subset.")
    parser.add_argument("--n-users", type=int, default=5000, help="Number of users to sample (default: 5000)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility (default: 42)")
    args = parser.parse_args()
    sample(n_users=args.n_users, seed=args.seed)