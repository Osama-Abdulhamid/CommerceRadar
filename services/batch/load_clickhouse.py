"""Load CommerceRadar batch intelligence Parquet outputs into ClickHouse."""

from __future__ import annotations

import argparse
import base64
import json
import os
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


def post_json_each_row(
    rows: list[dict],
    table: str,
    columns: list[str],
    http_url: str,
    database: str,
    user: str,
    password: str,
) -> None:
    if not rows:
        return

    column_sql = ", ".join(columns)
    query = f"INSERT INTO {database}.{table} ({column_sql}) FORMAT JSONEachRow"
    url = f"{http_url.rstrip('/')}?query={urllib.parse.quote(query)}"
    body = "\n".join(json.dumps(row, default=json_default, ensure_ascii=False) for row in rows).encode("utf-8")

    request = urllib.request.Request(url, data=body, method="POST")
    token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    request.add_header("Authorization", f"Basic {token}")

    with urllib.request.urlopen(request, timeout=60) as response:
        response.read()


def load_frame(
    frame: DataFrame,
    table: str,
    columns: list[str],
    http_url: str,
    database: str,
    user: str,
    password: str,
    batch_size: int,
) -> None:
    def send_partition(iterator):
        batch = []
        for row in iterator:
            batch.append({column: row[column] for column in columns})
            if len(batch) >= batch_size:
                post_json_each_row(batch, table, columns, http_url, database, user, password)
                batch = []
        post_json_each_row(batch, table, columns, http_url, database, user, password)
        yield 1

    frame.select(*columns).rdd.mapPartitions(send_partition).count()


def prepare_frame(spark: SparkSession, base_path: str, dataset: str) -> DataFrame:
    frame = spark.read.parquet(f"{base_path.rstrip('/')}/{dataset}")
    if dataset == "product_matches":
        frame = frame.withColumn("price", F.col("price_decimal"))
    return frame


def truncate_tables(http_url: str, database: str, user: str, password: str) -> None:
    for spec in TABLES.values():
        query = f"TRUNCATE TABLE IF EXISTS {database}.{spec['table']}"
        url = f"{http_url.rstrip('/')}?query={urllib.parse.quote(query)}"
        request = urllib.request.Request(url, method="POST")
        token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
        request.add_header("Authorization", f"Basic {token}")
        with urllib.request.urlopen(request, timeout=60) as response:
            response.read()


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
    return args


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if args.truncate:
            truncate_tables(args.clickhouse_url, args.database, args.user, args.password)

        for dataset, spec in TABLES.items():
            frame = prepare_frame(spark, args.input_base, dataset)
            load_frame(
                frame=frame,
                table=spec["table"],
                columns=spec["columns"],
                http_url=args.clickhouse_url,
                database=args.database,
                user=args.user,
                password=args.password,
                batch_size=args.batch_size,
            )
            print(f"Loaded {dataset} into {spec['table']}", flush=True)

        print("OK: Batch ClickHouse load complete.", flush=True)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
