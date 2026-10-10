"""Load experimental identifier matches into a staged ClickHouse table."""

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

stage = "wdc_identifier_matches_stage_" + uuid.uuid4().hex
target = "commerceradar.wdc_identifier_matches"

def query(sql, stream=None):
    command = [
        "docker", "compose", "exec", "-T", "clickhouse", "sh", "-c",
        'exec clickhouse-client --user "$CLICKHOUSE_USER" '
        '--password "$CLICKHOUSE_PASSWORD" --max_threads=1 --max_insert_threads=1 --input_format_parallel_parsing=0 --input_format_parquet_preserve_order=1 --input_format_parquet_max_block_size=8192 --max_insert_block_size=32768 --min_insert_block_size_rows=32768 --min_insert_block_size_bytes=8388608 --query "$1"',
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
 price, currency, price_parse_status,
 canonical_product_id, identifier_key, match_method)
SELECT source_record_id, cluster_id, title, brand, category,
       price_decimal, currency, price_parse_status,
       canonical_product_id, identifier_key, match_method
FROM input(
    'source_record_id String, cluster_id Nullable(String),
     title Nullable(String), brand Nullable(String),
     category Nullable(String), price_decimal Nullable(Decimal(18,2)),
     currency Nullable(String), price_parse_status String,
     canonical_product_id String, identifier_key Nullable(String),
     match_method String'
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

    invalid = int(query(f"""
SELECT count()
FROM commerceradar.{stage}
WHERE empty(trimBoth(source_record_id))
   OR NOT match(canonical_product_id, '^[0-9a-f]{{64}}$')
   OR match_method NOT IN
      ('experimental_identifier_key', 'unresolved_singleton')
   OR (match_method = 'experimental_identifier_key'
       AND identifier_key IS NULL)
   OR (match_method = 'unresolved_singleton'
       AND identifier_key IS NOT NULL)
"""))
    if invalid:
        raise RuntimeError(f"Invalid staged rows: {invalid}; not publishing")

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
