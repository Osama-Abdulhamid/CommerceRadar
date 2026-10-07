"""Read Kafka observations and display typed records."""

import os
import json
from pathlib import Path
from jsonschema import Draft202012Validator, FormatChecker

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


spark = (
    SparkSession.builder
    .appName("CommerceRadarStreaming")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

fields = [
    "schema_version", "event_id", "observed_at", "source",
    "source_product_id", "product_url", "title", "price",
    "currency", "availability", "brand", "category",
]
schema = StructType([
    StructField(name, StringType(), True) for name in fields
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP_SERVERS"])
    .option("subscribe", os.environ["KAFKA_TOPIC"])
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "true")
    .load()
)

parsed = raw.select(
    F.col("topic"),
    F.col("partition"),
    F.col("offset"),
    F.col("timestamp").alias("kafka_timestamp"),
    F.col("value").cast("string").alias("raw_json"),
    F.from_json(F.col("value").cast("string"), schema).alias("event"),
)

typed = (
    parsed.select(
        "topic", "partition", "offset", "kafka_timestamp",
        "raw_json", "event.*",
    )
    .withColumn("price_decimal", F.col("price").cast("decimal(18,2)"))
    .withColumn("observed_at_utc", F.to_timestamp("observed_at"))
)


contract = json.loads(
    Path("/opt/spark/work-dir/product_observation.schema.json")
    .read_text(encoding="utf-8")
)
Draft202012Validator.check_schema(contract)
contract_json = json.dumps(contract)
_worker_validator = None


@F.udf(returnType=StringType())
def validate_record(raw_json):
    global _worker_validator
    if _worker_validator is None:
        _worker_validator = Draft202012Validator(
            json.loads(contract_json),
            format_checker=FormatChecker(),
        )
    try:
        def reject_constant(value):
            raise ValueError(f"Invalid JSON constant: {value}")

        event = json.loads(raw_json, parse_constant=reject_constant)
    except (ValueError, TypeError) as error:
        return f"Invalid JSON: {error}"

    error = next(_worker_validator.iter_errors(event), None)
    if error is not None:
        location = ".".join(str(item) for item in error.absolute_path)
        return f"{location or '$'}: {error.message}"
    return None


typed = (
    typed.withColumn("validation_error", validate_record("raw_json"))
    .withColumn(
        "validation_error",
        F.when(
            F.col("validation_error").isNotNull(),
            F.col("validation_error"),
        )
        .when(
            F.col("price_decimal").isNull(),
            F.lit("Price cannot be represented as Decimal(18,2)"),
        )
        .when(
            F.col("observed_at_utc").isNull(),
            F.lit("Observation timestamp could not be parsed"),
        )
        .otherwise(F.lit(None).cast("string")),
    )
)

def display_batch(batch, batch_id):
    batch.persist()
    try:
        count = batch.count()
        print(f"CommerceRadar batch={batch_id} records={count}", flush=True)
        rejected = batch.filter(F.col("validation_error").isNotNull())
        rejected_count = rejected.count()
        print(
            f"Validation batch={batch_id} valid={count - rejected_count} "
            f"rejected={rejected_count}",
            flush=True,
        )
        if rejected_count:
            rejection_path = f"/state/rejected-console-v1/batch-{batch_id}"
            (
                rejected.select(
                    "topic", "partition", "offset", "kafka_timestamp",
                    "raw_json", "validation_error",
                )
                .write.mode("overwrite")
                .parquet(rejection_path)
            )
            print(
                f"Saved rejected records: {rejection_path}",
                flush=True,
            )
            rejected.select(
                "topic", "partition", "offset",
                "validation_error", "raw_json",
            ).show(100, truncate=False)

        batch.filter(F.col("validation_error").isNull()).select(
            "event_id", "source", "source_product_id",
            "price_decimal", "currency", "observed_at_utc",
            "availability", "partition", "offset",
        ).orderBy("partition", "offset").show(100, truncate=False)
    finally:
        batch.unpersist()

writer = (
    typed.writeStream
    .foreachBatch(display_batch)
    .option("checkpointLocation", "/state/console-v1")
)

if os.getenv("STREAM_TRIGGER", "available_now") == "available_now":
    writer = writer.trigger(availableNow=True)
else:
    writer = writer.trigger(processingTime="5 seconds")

try:
    query = writer.start()
    query.awaitTermination()
finally:
    spark.stop()
