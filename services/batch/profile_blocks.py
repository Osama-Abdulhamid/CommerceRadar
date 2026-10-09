"""Measure WDC matching blocks without generating candidate pairs."""

import os

from pyspark.sql import SparkSession, functions as F, types as T

spark = (
    SparkSession.builder
    .appName("CommerceRadarProfileBlocks")
    .config("spark.sql.shuffle.partitions", "64")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

spark.sparkContext.addPyFile(
    "/workspace/services/batch/product_intelligence.py"
)
from product_intelligence import normalize_text, key_tokens

normalize = F.udf(normalize_text, T.StringType())
tokens = F.udf(key_tokens, T.ArrayType(T.StringType()))

try:
    data = spark.read.parquet(os.environ["BATCH_INPUT_PATH"])
    blocks = (
        data.select("title", "category")
        .withColumn("normalized_title", normalize("title"))
        .withColumn(
            "block_category",
            F.coalesce(normalize("category"), F.lit("")),
        )
        .withColumn("blocking_tokens", tokens("normalized_title"))
        .select(
            "block_category",
            F.explode("blocking_tokens").alias("blocking_token"),
        )
        .groupBy("block_category", "blocking_token")
        .count()
    )

    print("Largest category/token blocks:", flush=True)
    blocks.orderBy(F.desc("count")).show(20, truncate=False)

    blocks.agg(
        F.count("*").alias("total_blocks"),
        F.max("count").alias("largest_block"),
        F.sum(
            F.when(F.col("count") > 500, 1).otherwise(0)
        ).alias("blocks_over_500"),
        F.sum(
            F.col("count").cast("decimal(38,0)")
            * (F.col("count") - 1) / F.lit(2)
        ).alias("pair_upper_bound"),
    ).show(truncate=False)

    print("OK: Full-data block profile completed; no pairs generated.", flush=True)
finally:
    spark.stop()
