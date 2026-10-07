"""Read Kafka observations and display typed records."""

import os

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

def display_batch(batch, batch_id):
    batch.persist()
    try:
        count = batch.count()
        print(f"CommerceRadar batch={batch_id} records={count}", flush=True)
        batch.select(
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
