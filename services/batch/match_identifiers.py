import os
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

from identifier_rules import matching_key

spark = SparkSession.builder.appName("WDCIdentifierMatching").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.addPyFile(
    str(Path(__file__).with_name("identifier_rules.py"))
)

def pair_count(column):
    n = F.col(column).cast("decimal(38,0)")
    return n * (n - 1) / F.lit(2)

def total_pairs(frame):
    value = (
        frame.groupBy("canonical_product_id").count()
        .agg(F.sum(pair_count("count")).alias("pairs"))
        .first()["pairs"]
    )
    return int(value or 0)

offers = None
try:
    input_path = os.environ["MATCH_INPUT_PATH"]
    output_path = os.environ["MATCH_OUTPUT_PATH"]
    limit = int(os.getenv("MATCH_INPUT_LIMIT", "0"))
    if limit < 0:
        raise ValueError("MATCH_INPUT_LIMIT must be nonnegative")

    # Refuse to overwrite any existing output.
    if not output_path.startswith("file:///"):
        raise ValueError("This runner requires an explicit local file:/// output")
    if Path(output_path.removeprefix("file://")).exists():
        raise FileExistsError(output_path)

    frame = spark.read.parquet(input_path)
    if limit:
        frame = frame.limit(limit)

    key_udf = F.udf(matching_key, StringType())
    offers = (
        frame.withColumn("identifier_key", key_udf("cleaned_record_json"))
        .withColumn(
            "canonical_product_id",
            F.sha2(
                F.when(
                    F.col("identifier_key").isNotNull(),
                    F.concat(F.lit("identifier:"), F.col("identifier_key")),
                ).otherwise(
                    F.concat(F.lit("singleton:"), F.col("source_record_id"))
                ),
                256,
            ),
        )
        .withColumn(
            "match_method",
            F.when(
                F.col("identifier_key").isNotNull(),
                F.lit("experimental_identifier_key"),
            ).otherwise(F.lit("unresolved_singleton")),
        )
        .drop("cleaned_record_json")
        .persist(StorageLevel.DISK_ONLY)
    )

    stats = offers.agg(
        F.count("*").alias("input_records"),
        F.countDistinct("source_record_id").alias("unique_ids"),
        F.count("identifier_key").alias("eligible_records"),
        F.countDistinct("canonical_product_id").alias("canonical_groups"),
        F.sum(
            F.when(
                F.col("source_record_id").isNull()
                | (F.trim("source_record_id") == ""), 1
            ).otherwise(0)
        ).alias("invalid_ids"),
    ).first().asDict()
    print("Input checks:", stats, flush=True)
    if stats["invalid_ids"] or stats["input_records"] != stats["unique_ids"]:
        raise ValueError("Missing or duplicate source IDs; no outputs published")

    labelled = offers.filter(
        F.col("cluster_id").isNotNull() & (F.trim("cluster_id") != "")
    )
    all_predicted = total_pairs(offers)
    labelled_predicted = total_pairs(labelled)

    true_positive = labelled.groupBy(
        "canonical_product_id", "cluster_id"
    ).count().agg(
        F.sum(pair_count("count")).alias("pairs")
    ).first()["pairs"]
    tp = int(true_positive or 0)

    truth = labelled.groupBy("cluster_id").count().agg(
        F.sum(pair_count("count")).alias("pairs")
    ).first()["pairs"]
    truth_pairs = int(truth or 0)
    fp = labelled_predicted - tp
    fn = truth_pairs - tp

    metrics = {
        **{key: int(value or 0) for key, value in stats.items()},
        "predicted_pairs": all_predicted,
        "evaluated_predicted_pairs": labelled_predicted,
        "pairs_without_complete_labels": all_predicted - labelled_predicted,
        "true_positive_pairs": tp,
        "false_positive_pairs": fp,
        "false_negative_pairs": fn,
        "reference_pairs": truth_pairs,
        "precision": tp / labelled_predicted if labelled_predicted else None,
        "recall": tp / truth_pairs if truth_pairs else None,
        "method": "one deterministic identifier key; pack-aware; experimental",
    }
    print("Evaluation:", metrics, flush=True)

    offers.write.mode("errorifexists").parquet(output_path + "/offers")
    spark.createDataFrame(
        [( __import__("json").dumps(metrics), )], ["value"]
    ).coalesce(1).write.mode("errorifexists").text(output_path + "/evaluation")

    stored = spark.read.parquet(output_path + "/offers").count()
    if stored != stats["input_records"]:
        raise ValueError("Written row count mismatch")
    print(f"OK: Verified {stored} output offers; ClickHouse unchanged.", flush=True)
finally:
    if offers is not None:
        offers.unpersist()
    spark.stop()
