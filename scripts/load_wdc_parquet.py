"""Load cleaned WDC Parquet into a staged ClickHouse table."""

import argparse
import subprocess
import uuid
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--expected-records", type=int, required=True)
args = parser.parse_args()

files = sorted(Path(args.input).glob("*.parquet"))
if not files:
    raise SystemExit("No Parquet files found")
if args.expected_records < 1:
    raise SystemExit("Expected record count must be positive")

stage = "wdc_cleaned_offers_stage_" + uuid.uuid4().hex
target = "commerceradar.wdc_cleaned_offers"

def query(sql, stream=None):
    command = [
        "docker", "compose", "exec", "-T", "clickhouse", "sh", "-c",
        'exec clickhouse-client --user "$CLICKHOUSE_USER" '
        '--password "$CLICKHOUSE_PASSWORD" --query "$1"',
        "wdc-loader", sql,
    ]
    result = subprocess.run(
        command,
        stdin=stream if stream is not None else subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.decode("utf-8", errors="replace"))
    return result.stdout.decode("utf-8").strip()

try:
    engine = query(
        "SELECT engine FROM system.databases WHERE name='commerceradar'"
    )
    if engine != "Atomic":
        raise RuntimeError("An Atomic database is required")

    query(f"CREATE TABLE commerceradar.{stage} AS {target}")
    print(f"Staging table: commerceradar.{stage}", flush=True)

    insert = f"""
INSERT INTO commerceradar.{stage}
(source_record_id, cluster_id, title, brand, category,
 price, currency, price_parse_status)
SELECT source_record_id, cluster_id, title, brand, category,
       price_decimal, currency, price_parse_status
FROM input(
    'source_record_id String, cluster_id Nullable(String),
     title Nullable(String), brand Nullable(String),
     category Nullable(String), price_decimal Nullable(Decimal(18,2)),
     currency Nullable(String), price_parse_status String'
)
FORMAT Parquet
"""
    for number, file in enumerate(files, 1):
        with file.open("rb") as stream:
            query(insert, stream)
        print(f"Loaded file {number}/{len(files)}: {file.name}", flush=True)

    stored = int(query(f"SELECT count() FROM commerceradar.{stage}"))
    print(f"Stored records: {stored:,}", flush=True)
    if stored != args.expected_records:
        raise RuntimeError(
            f"Expected {args.expected_records:,}; found {stored:,}. "
            "Active table was not replaced."
        )

    print("Publishing verified table...", flush=True)
    query(f"EXCHANGE TABLES {target} AND commerceradar.{stage}")
    print(
        f"OK: {stored:,} records published. "
        f"Previous table retained in commerceradar.{stage}",
        flush=True,
    )
except Exception:
    print(
        f"FAILED: staging table {stage} retained. "
        "If publishing had started, inspect before retrying.",
        flush=True,
    )
    raise
