from pyspark import StorageLevel
"""Profile exact normalized groups without generating matching pairs."""

import os

from pyspark.sql import SparkSession, functions as F, types as T

spark = (
    SparkSession.builder
    .appName("CommerceRadarExactGroupProfile")
    .config("spark.sql.shuffle.partitions", "64")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.addPyFile(
    "/workspace/services/batch/product_intelligence.py"
)
from product_intelligence import normalize_text

normalize = F.udf(normalize_text, T.StringType())

try:
    data = spark.read.parquet(os.environ["BATCH_INPUT_PATH"])
    prepared = (
        data.select("source_record_id", "title", "brand", "category", "cluster_id")
        .withColumn("normalized_title", normalize("title"))
        .withColumn("normalized_brand", normalize("brand"))
        .withColumn("normalized_category", normalize("category"))
    )

    eligible = prepared.filter(F.col("normalized_title").isNotNull())
    groups = eligible.groupBy(
        "normalized_title", "normalized_brand", "normalized_category"
    ).agg(
        F.count("*").alias("offers"),
        F.countDistinct("cluster_id").alias("reference_clusters"),
    ).persist(StorageLevel.DISK_ONLY)

    print("Largest exact-title/brand/category groups:", flush=True)
    groups.orderBy(F.desc("offers")).show(15, truncate=100)

    groups.agg(
        F.count("*").alias("exact_groups"),
        F.max("offers").alias("largest_group"),
        F.sum(F.when(F.col("offers") > 1, 1).otherwise(0))
            .alias("multi_offer_groups"),
        F.sum(F.when(F.col("offers") > 1, F.col("offers")).otherwise(0))
            .alias("offers_in_multi_offer_groups"),
        F.sum(F.when(F.col("reference_clusters") > 1, 1).otherwise(0))
            .alias("groups_with_conflicting_reference_clusters"),
    ).show(truncate=False)

    print(
        "Missing normalized titles:",
        prepared.filter(F.col("normalized_title").isNull()).count(),
        flush=True,
    )
    print(
        "OK: Exact-group profile completed; no predictions published.",
        flush=True,
    )
finally:
    spark.stop()
