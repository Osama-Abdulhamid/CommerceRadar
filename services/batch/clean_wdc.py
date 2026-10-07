"""Clean WDC records with shared rules and write Parquet."""

import json
import os
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType

spark = (
    SparkSession.builder
    .appName("CommerceRadarWDCBatch")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Ship the existing cleaning module to Spark workers.
spark.sparkContext.addPyFile("/workspace/scripts/clean_wdc_sample.py")

fields = [
    "source_record_id", "cluster_id", "title", "brand", "category",
    "price", "currency", "price_parse_status", "cleaned_record_json",
]
schema = StructType([
    StructField(name, StringType(), True) for name in fields
])


def clean_partition(lines):
    from clean_wdc_sample import clean_text, parse_price

    for line in lines:
        raw = json.loads(line)
        price, currency, status = parse_price(raw.get("price"))
        title = clean_text(raw.get("title"))

        record = {
            "batch_schema_version": "1.0",
            "dataset": "wdc_english_v2_non_norm",
            "source_record_id": str(raw["id"]),
            "cluster_id": raw.get("cluster_id"),
            "title": title,
            "brand": clean_text(raw.get("brand")),
            "category": clean_text(raw.get("category")),
            "identifiers": raw.get("identifiers"),
            "price": price,
            "currency": currency,
            "price_parse_status": status,
            "quality_flags": ["missing_title"] if title is None else [],
            "raw_record": raw,
        }

        yield (
            record["source_record_id"],
            str(record["cluster_id"]) if record["cluster_id"] is not None else None,
            title,
            record["brand"],
            record["category"],
            price,
            currency,
            status,
            json.dumps(record, ensure_ascii=False, allow_nan=False),
        )


try:
    input_path = os.environ["BATCH_INPUT_PATH"]
    output_path = os.environ["BATCH_OUTPUT_PATH"]

    rows = spark.read.text(input_path).rdd.map(
        lambda row: row.value
    ).mapPartitions(clean_partition)

    cleaned = spark.createDataFrame(rows, schema)
    cleaned = cleaned.withColumn(
        "price_decimal", F.col("price").cast("decimal(18,2)")
    )

    # Prevent silent loss when the analytical decimal cannot hold a price.
    overflow = cleaned.filter(
        F.col("price").isNotNull() & F.col("price_decimal").isNull()
    ).limit(1).count()
    if overflow:
        raise ValueError("Price exceeds Decimal(18,2); review before writing")

    cleaned.write.mode("errorifexists").parquet(output_path)

    saved = spark.read.parquet(output_path)
    print("Saved records:", saved.count(), flush=True)
    saved.groupBy("price_parse_status").count().orderBy(
        "price_parse_status"
    ).show(truncate=False)

    duplicate_ids = (
        saved.groupBy("source_record_id").count()
        .filter(F.col("count") > 1)
    )
    print("Duplicate ID groups:", duplicate_ids.count(), flush=True)
    print("OK: Parquet written and read back.", flush=True)
finally:
    spark.stop()
