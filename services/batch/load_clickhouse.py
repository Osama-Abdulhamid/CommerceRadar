"""Load CommerceRadar batch intelligence Parquet outputs into ClickHouse."""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import uuid
import urllib.parse
import urllib.request
from datetime import date, datetime
from decimal import Decimal
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


TABLES = {
    "product_matches": {
        "table": "batch_product_matches",
        "columns": [
            "canonical_product_id",
            "source",
            "source_product_id",
            "source_record_id",
            "cluster_id",
            "original_product_title",
            "normalized_title",
            "brand",
            "normalized_brand",
            "category",
            "normalized_category",
            "price",
            "currency",
            "price_parse_status",
            "match_confidence",
            "match_method",
        ],
    },
    "product_price_summary": {
        "table": "batch_product_price_summary",
        "columns": [
            "canonical_product_id",
            "currency",
            "min_price",
            "max_price",
            "avg_price",
            "observation_count",
            "source_count",
            "offer_count",
            "price_range",
        ],
    },
    "product_source_summary": {
        "table": "batch_product_source_summary",
        "columns": [
            "canonical_product_id",
            "source",
            "currency",
            "min_price",
            "max_price",
            "avg_price",
            "observation_count",
        ],
    },
    "matching_evaluation": {
        "table": "batch_matching_evaluation",
        "columns": [
            "input_records",
            "candidate_pairs",
            "matched_pairs",
            "canonical_groups",
            "unmatched_records",
            "true_positive_pairs",
            "false_positive_pairs",
            "false_negative_pairs",
            "precision",
            "recall",
            "f1",
            "blocking_strategy",
        ],
    },
}


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("CommerceRadarLoadBatchClickHouse").getOrCreate()


def json_default(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Unsupported JSON value: {type(value)!r}")


def execute_sql(sql, http_url, database, user, password, body=b""):
    params = urllib.parse.urlencode({
        "query": sql,
        "database": database,
        "wait_end_of_query": "1",
        "async_insert": "0",
    })
    request = urllib.request.Request(
        http_url.rstrip("/") + "/?" + params,
        data=body,
        method="POST",
    )
    token = base64.b64encode(
        f"{user}:{password}".encode("utf-8")
    ).decode("ascii")
    request.add_header("Authorization", f"Basic {token}")
    with urllib.request.urlopen(request, timeout=120) as response:
        return response.read().decode("utf-8")


def load_frame(
    frame, table, columns, http_url, database,
    user, password, batch_size,
):
    # Only the driver sends inserts: worker retries cannot insert twice.
    batch = []
    batch_bytes = 0
    sent = 0

    def send(lines):
        body = (chr(10).join(lines) + chr(10)).encode("utf-8")
        execute_sql(
            f"INSERT INTO {database}.{table} "
            f"({', '.join(columns)}) FORMAT JSONEachRow",
            http_url, database, user, password, body,
        )

    for row in frame.select(*columns).toLocalIterator(
        prefetchPartitions=False
    ):
        line = json.dumps(
            {column: row[column] for column in columns},
            default=json_default,
            ensure_ascii=False,
            allow_nan=False,
        )
        size = len(line.encode("utf-8")) + 1
        if size > 4_000_000:
            raise ValueError("One row exceeds the 4 MB insert limit")
        if batch and (
            len(batch) >= batch_size or batch_bytes + size > 4_000_000
        ):
            send(batch)
            sent += len(batch)
            batch = []
            batch_bytes = 0
        batch.append(line)
        batch_bytes += size

    if batch:
        send(batch)
        sent += len(batch)
    return sent


def prepare_frame(spark: SparkSession, base_path: str, dataset: str) -> DataFrame:
    frame = spark.read.parquet(f"{base_path.rstrip('/')}/{dataset}")
    if dataset == "product_matches":
        frame = frame.withColumn("price", F.col("price_decimal"))
    return frame



def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-base", default=os.getenv("BATCH_OUTPUT_PATH"), required=False)
    parser.add_argument("--clickhouse-url", default=os.getenv("CLICKHOUSE_HTTP_URL", "http://clickhouse:8123"))
    parser.add_argument("--database", default=os.getenv("CLICKHOUSE_DB", "commerceradar"))
    parser.add_argument("--user", default=os.getenv("CLICKHOUSE_USER", "commerceradar"))
    parser.add_argument("--password", default=os.getenv("CLICKHOUSE_PASSWORD"), required=False)
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("CLICKHOUSE_BATCH_SIZE", "1000")))
    parser.add_argument("--truncate", action="store_true")
    args = parser.parse_args(argv)
    if not args.input_base:
        parser.error("--input-base or BATCH_OUTPUT_PATH is required")
    if not args.password:
        parser.error("--password or CLICKHOUSE_PASSWORD is required")
    if args.truncate:
        parser.error("--truncate is disabled; staging replacement is used")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.database):
        parser.error("Invalid database identifier")
    if args.batch_size < 1:
        parser.error("--batch-size must be positive")
    url = urllib.parse.urlsplit(args.clickhouse_url)
    if (
        url.scheme not in {"http", "https"} or not url.hostname
        or url.username or url.password or url.query or url.fragment
        or url.path not in {"", "/"}
    ):
        parser.error("Invalid ClickHouse HTTP URL")
    return args


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    run_id = uuid.uuid4().hex
    staged = []

    def sql(query):
        return execute_sql(
            query, args.clickhouse_url, args.database,
            args.user, args.password,
        )

    print(f"Batch load run: {run_id}", flush=True)

    try:
        engine = sql(
            "SELECT engine FROM system.databases "
            f"WHERE name = '{args.database}'"
        ).strip()
        if engine != "Atomic":
            raise ValueError("Staging replacement requires an Atomic database")

        for dataset, spec in TABLES.items():
            target = spec["table"]
            stage = f"{target}_stage_{run_id}"
            frame = prepare_frame(spark, args.input_base, dataset)
            expected = frame.count()

            if dataset == "product_matches":
                keys = ["source_record_id"]
                if expected == 0:
                    raise ValueError("Product matches must not be empty")
            elif dataset == "product_price_summary":
                keys = ["canonical_product_id", "currency"]
            elif dataset == "product_source_summary":
                keys = ["canonical_product_id", "source", "currency"]
            else:
                keys = []
                if expected != 1:
                    raise ValueError("Expected exactly one evaluation row")

            if keys and (
                frame.groupBy(*keys).count()
                .filter(F.col("count") > 1).limit(1).count()
            ):
                raise ValueError(f"Duplicate input keys in {dataset}")

            sql(f"CREATE TABLE {args.database}.{stage} AS {args.database}.{target}")
            print(f"Staging: {stage}; expected={expected}", flush=True)

            sent = load_frame(
                frame, stage, spec["columns"],
                args.clickhouse_url, args.database,
                args.user, args.password, args.batch_size,
            )
            stored = int(sql(
                f"SELECT count() FROM {args.database}.{stage}"
            ).strip())
            if sent != expected or stored != expected:
                raise ValueError(
                    f"{dataset}: expected={expected}, sent={sent}, stored={stored}"
                )
            staged.append((target, stage))
            print(f"Verified {dataset}: {stored} rows", flush=True)

        # All staging tables passed checks before publishing starts.
        # Each exchange is atomic separately, not across all four tables.
        for target, stage in staged:
            print(f"Publishing {target}; previous data retained in {stage}", flush=True)
            sql(
                f"EXCHANGE TABLES {args.database}.{target} "
                f"AND {args.database}.{stage}"
            )
            print(f"Published {target}", flush=True)

        print("OK: Batch tables replaced; previous tables retained.", flush=True)
        return 0
    except Exception:
        print(
            f"FAILED run={run_id}. Staging/backup tables retained. "
            "If publishing started, inspect the tables before retrying.",
            flush=True,
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
