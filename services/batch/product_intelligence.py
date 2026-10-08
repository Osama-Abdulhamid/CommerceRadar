"""Batch product matching and historical analytics for cleaned WDC data."""

from __future__ import annotations

import argparse
import os
import re
import unicodedata
from typing import Iterable, TYPE_CHECKING

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
except ModuleNotFoundError:  # Allows pure normalization tests without PySpark.
    SparkSession = None
    F = None
    T = None

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
else:
    DataFrame = object


STOPWORDS = {
    "a", "an", "and", "are", "at", "by", "for", "from", "in", "is",
    "of", "on", "or", "the", "to", "with", "shop", "sale", "new",
}


def normalize_text(value: str | None) -> str | None:
    """Deterministically normalize product text without inventing values."""
    if value is None:
        return None
    text = unicodedata.normalize("NFKC", value).lower()
    text = re.sub(r'"@[\w-]+', '"', text)
    text = re.sub(r"@[\w-]+\b", " ", text)
    text = re.sub(r"[^0-9a-z]+", " ", text)
    text = " ".join(text.split())
    return text or None


def key_tokens(value: str | None, limit: int = 4) -> list[str]:
    """Return stable blocking tokens ordered by usefulness."""
    if value is None:
        return []
    tokens = [
        token for token in value.split()
        if len(token) >= 3 and token not in STOPWORDS
    ]
    # Long model-like tokens are usually more selective than generic words.
    ordered = sorted(set(tokens), key=lambda token: (-len(token), token))
    return ordered[:limit]


def token_jaccard(left: str | None, right: str | None) -> float:
    if not left or not right:
        return 0.0
    left_tokens = {
        token for token in left.split()
        if len(token) >= 2 and token not in STOPWORDS
    }
    right_tokens = {
        token for token in right.split()
        if len(token) >= 2 and token not in STOPWORDS
    }
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


if F is not None and T is not None:
    NORMALIZE_TEXT_UDF = F.udf(normalize_text, T.StringType())
    KEY_TOKENS_UDF = F.udf(key_tokens, T.ArrayType(T.StringType()))
    TOKEN_JACCARD_UDF = F.udf(token_jaccard, T.DoubleType())
else:
    NORMALIZE_TEXT_UDF = None
    KEY_TOKENS_UDF = None
    TOKEN_JACCARD_UDF = None


def build_spark(app_name: str = "CommerceRadarBatchIntelligence") -> SparkSession:
    if SparkSession is None:
        raise RuntimeError("PySpark is required to run the batch intelligence pipeline")
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "64"))
        .getOrCreate()
    )


def normalize_products(cleaned: DataFrame) -> DataFrame:
    """Add normalized fields and deterministic blocking tokens."""
    return (
        cleaned
        .withColumn("normalized_title", NORMALIZE_TEXT_UDF("title"))
        .withColumn("normalized_brand", NORMALIZE_TEXT_UDF("brand"))
        .withColumn("normalized_category", NORMALIZE_TEXT_UDF("category"))
        .withColumn("blocking_tokens", KEY_TOKENS_UDF("normalized_title"))
        .withColumn(
            "source",
            F.lit("wdc_english_v2_non_norm"),
        )
        .withColumn("source_product_id", F.col("source_record_id"))
        .withColumn("price_decimal", F.col("price").cast("decimal(18,2)"))
    )


def generate_candidate_pairs(normalized: DataFrame) -> DataFrame:
    """Generate candidate pairs with blocking; never performs global cross joins."""
    base = normalized.filter(
        F.col("source_record_id").isNotNull()
        & F.col("normalized_title").isNotNull()
        & (F.size("blocking_tokens") > 0)
    ).select(
        "source_record_id", "cluster_id", "normalized_title",
        "normalized_brand", "normalized_category", "blocking_tokens",
    )

    exploded = base.select(
        "*",
        F.explode("blocking_tokens").alias("blocking_token"),
    )

    left = exploded.alias("left")
    right = exploded.alias("right")

    category_matches = (
        F.coalesce(F.col("left.normalized_category"), F.lit(""))
        == F.coalesce(F.col("right.normalized_category"), F.lit(""))
    )
    brand_matches = (
        F.coalesce(F.col("left.normalized_brand"), F.lit(""))
        == F.coalesce(F.col("right.normalized_brand"), F.lit(""))
    )

    return (
        left.join(
            right,
            (F.col("left.blocking_token") == F.col("right.blocking_token"))
            & (F.col("left.source_record_id") < F.col("right.source_record_id"))
            & category_matches
            & (
                brand_matches
                | F.col("left.normalized_brand").isNull()
                | F.col("right.normalized_brand").isNull()
            ),
            "inner",
        )
        .select(
            F.col("left.source_record_id").alias("left_source_record_id"),
            F.col("right.source_record_id").alias("right_source_record_id"),
            F.col("left.cluster_id").alias("left_cluster_id"),
            F.col("right.cluster_id").alias("right_cluster_id"),
            F.col("left.normalized_title").alias("left_normalized_title"),
            F.col("right.normalized_title").alias("right_normalized_title"),
            F.col("left.normalized_brand").alias("left_normalized_brand"),
            F.col("right.normalized_brand").alias("right_normalized_brand"),
            F.col("left.normalized_category").alias("left_normalized_category"),
            F.col("right.normalized_category").alias("right_normalized_category"),
            F.col("left.blocking_token").alias("blocking_token"),
        )
        .dropDuplicates(["left_source_record_id", "right_source_record_id"])
    )


def score_candidate_pairs(candidates: DataFrame, threshold: float) -> DataFrame:
    scored = (
        candidates
        .withColumn(
            "title_similarity",
            TOKEN_JACCARD_UDF("left_normalized_title", "right_normalized_title"),
        )
        .withColumn(
            "brand_agreement",
            F.when(
                F.col("left_normalized_brand").isNull()
                | F.col("right_normalized_brand").isNull(),
                F.lit(None).cast("double"),
            ).when(
                F.col("left_normalized_brand") == F.col("right_normalized_brand"),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "category_agreement",
            F.when(
                F.col("left_normalized_category").isNull()
                | F.col("right_normalized_category").isNull(),
                F.lit(None).cast("double"),
            ).when(
                F.col("left_normalized_category") == F.col("right_normalized_category"),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "match_confidence",
            F.least(
                F.lit(1.0),
                F.col("title_similarity") * F.lit(0.80)
                + F.coalesce(F.col("brand_agreement"), F.lit(0.50)) * F.lit(0.10)
                + F.coalesce(F.col("category_agreement"), F.lit(0.50)) * F.lit(0.10),
            ),
        )
        .withColumn("is_match", F.col("match_confidence") >= F.lit(threshold))
        .withColumn("matching_threshold", F.lit(threshold))
    )
    return scored


def build_product_matches(normalized: DataFrame, scored_pairs: DataFrame) -> DataFrame:
    """Assign each offer to a deterministic canonical product ID."""
    matched_pairs = scored_pairs.filter(F.col("is_match"))
    endpoints = (
        matched_pairs.select(
            F.col("left_source_record_id").alias("source_record_id"),
            F.col("right_source_record_id").alias("peer_source_record_id"),
            "match_confidence",
        )
        .unionByName(
            matched_pairs.select(
                F.col("right_source_record_id").alias("source_record_id"),
                F.col("left_source_record_id").alias("peer_source_record_id"),
                "match_confidence",
            )
        )
    )
    representatives = endpoints.groupBy("source_record_id").agg(
        F.min("peer_source_record_id").alias("min_peer_source_record_id"),
        F.max("match_confidence").alias("pair_match_confidence"),
    )

    return (
        normalized.join(representatives, "source_record_id", "left")
        .withColumn(
            "canonical_seed",
            F.least(
                F.col("source_record_id"),
                F.coalesce(F.col("min_peer_source_record_id"), F.col("source_record_id")),
            ),
        )
        .withColumn("canonical_product_id", F.sha2(F.concat_ws(":", F.lit("wdc"), "canonical_seed"), 256))
        .withColumn(
            "match_confidence",
            F.coalesce(F.col("pair_match_confidence"), F.lit(1.0)),
        )
        .withColumn(
            "match_method",
            F.when(F.col("pair_match_confidence").isNull(), F.lit("singleton"))
            .otherwise(F.lit("blocked_token_jaccard")),
        )
        .select(
            "canonical_product_id",
            "source",
            "source_product_id",
            "source_record_id",
            "cluster_id",
            F.col("title").alias("original_product_title"),
            "normalized_title",
            F.col("brand").alias("brand"),
            "normalized_brand",
            F.col("category").alias("category"),
            "normalized_category",
            "price_decimal",
            "currency",
            "price_parse_status",
            "match_confidence",
            "match_method",
        )
    )


def evaluate_matches(scored_pairs: DataFrame, product_matches: DataFrame, input_count: int) -> DataFrame:
    candidates = scored_pairs.count()
    predicted_matches = scored_pairs.filter(F.col("is_match")).count()
    true_positive = scored_pairs.filter(
        F.col("is_match")
        & F.col("left_cluster_id").isNotNull()
        & (F.col("left_cluster_id") == F.col("right_cluster_id"))
    ).count()
    false_positive = predicted_matches - true_positive
    total_positive_pairs = (
        product_matches.filter(F.col("cluster_id").isNotNull())
        .groupBy("cluster_id")
        .count()
        .select(F.sum((F.col("count") * (F.col("count") - 1)) / F.lit(2)).alias("pairs"))
        .collect()[0]["pairs"]
    ) or 0
    false_negative = int(total_positive_pairs) - true_positive
    precision = true_positive / predicted_matches if predicted_matches else None
    recall_denominator = true_positive + false_negative
    recall = true_positive / recall_denominator if recall_denominator else None
    f1 = (
        2 * precision * recall / (precision + recall)
        if precision is not None and recall is not None and precision + recall
        else None
    )

    spark = scored_pairs.sparkSession
    groups = product_matches.select("canonical_product_id").distinct().count()
    unmatched = product_matches.filter(F.col("match_method") == "singleton").count()

    return spark.createDataFrame(
        [(
            input_count,
            candidates,
            predicted_matches,
            groups,
            unmatched,
            true_positive,
            false_positive,
            false_negative,
            precision,
            recall,
            f1,
            "category + token, brand equality when both brands exist",
        )],
        schema=T.StructType([
            T.StructField("input_records", T.LongType(), False),
            T.StructField("candidate_pairs", T.LongType(), False),
            T.StructField("matched_pairs", T.LongType(), False),
            T.StructField("canonical_groups", T.LongType(), False),
            T.StructField("unmatched_records", T.LongType(), False),
            T.StructField("true_positive_pairs", T.LongType(), False),
            T.StructField("false_positive_pairs", T.LongType(), False),
            T.StructField("false_negative_pairs", T.LongType(), False),
            T.StructField("precision", T.DoubleType(), True),
            T.StructField("recall", T.DoubleType(), True),
            T.StructField("f1", T.DoubleType(), True),
            T.StructField("blocking_strategy", T.StringType(), False),
        ]),
    )


def build_product_history(product_matches: DataFrame) -> DataFrame:
    return product_matches.select(
        "canonical_product_id",
        "source",
        "source_product_id",
        "source_record_id",
        "price_decimal",
        "currency",
        "price_parse_status",
    )


def build_price_summary(product_matches: DataFrame) -> DataFrame:
    priced = product_matches.filter(
        F.col("price_decimal").isNotNull() & F.col("currency").isNotNull()
    )
    return (
        priced.groupBy("canonical_product_id", "currency")
        .agg(
            F.min("price_decimal").alias("min_price"),
            F.max("price_decimal").alias("max_price"),
            F.avg("price_decimal").cast("decimal(18,2)").alias("avg_price"),
            F.count("*").alias("observation_count"),
            F.countDistinct("source").alias("source_count"),
            F.countDistinct("source_product_id").alias("offer_count"),
        )
        .withColumn("price_range", F.col("max_price") - F.col("min_price"))
    )


def build_source_summary(product_matches: DataFrame) -> DataFrame:
    priced = product_matches.filter(
        F.col("price_decimal").isNotNull() & F.col("currency").isNotNull()
    )
    return (
        priced.groupBy("canonical_product_id", "source", "currency")
        .agg(
            F.min("price_decimal").alias("min_price"),
            F.max("price_decimal").alias("max_price"),
            F.avg("price_decimal").cast("decimal(18,2)").alias("avg_price"),
            F.count("*").alias("observation_count"),
        )
    )


def validate_outputs(
    product_matches: DataFrame,
    price_summary: DataFrame,
    source_summary: DataFrame,
) -> None:
    if product_matches.filter(F.col("canonical_product_id").isNull()).limit(1).count():
        raise ValueError("canonical_product_id contains nulls")
    if product_matches.filter(
        (F.col("match_confidence") < 0) | (F.col("match_confidence") > 1)
    ).limit(1).count():
        raise ValueError("match_confidence outside [0, 1]")
    if product_matches.filter(F.col("price_decimal") < 0).limit(1).count():
        raise ValueError("negative prices found")

    for name, aggregate in (
        ("price_summary", price_summary),
        ("source_summary", source_summary),
    ):
        if aggregate.filter(F.col("observation_count") <= 0).limit(1).count():
            raise ValueError(f"{name} has non-positive observation_count")
        if aggregate.filter(
            (F.col("min_price") > F.col("avg_price"))
            | (F.col("avg_price") > F.col("max_price"))
        ).limit(1).count():
            raise ValueError(f"{name} violates min <= avg <= max")
        missing = (
            aggregate.select("canonical_product_id").distinct()
            .join(
                product_matches.select("canonical_product_id").distinct(),
                "canonical_product_id",
                "left_anti",
            )
            .limit(1)
            .count()
        )
        if missing:
            raise ValueError(f"{name} contains unknown canonical_product_id")


def write_outputs(outputs: dict[str, DataFrame], output_path: str, mode: str) -> None:
    for name, frame in outputs.items():
        frame.write.mode(mode).parquet(f"{output_path.rstrip('/')}/{name}")


def read_cleaned_input(spark: SparkSession, input_path: str, input_format: str) -> DataFrame:
    """Read cleaned WDC input from production Parquet or JSONL samples."""
    normalized_format = input_format.lower()
    if normalized_format == "auto":
        lowered_path = input_path.lower()
        normalized_format = "json" if lowered_path.endswith((".json", ".jsonl")) else "parquet"

    if normalized_format == "parquet":
        return spark.read.parquet(input_path)
    if normalized_format == "json":
        lines = spark.read.text(input_path)
        return lines.select(
            F.get_json_object("value", "$.source_record_id").alias("source_record_id"),
            F.get_json_object("value", "$.cluster_id").alias("cluster_id"),
            F.get_json_object("value", "$.title").alias("title"),
            F.get_json_object("value", "$.brand").alias("brand"),
            F.get_json_object("value", "$.category").alias("category"),
            F.get_json_object("value", "$.price").alias("price"),
            F.get_json_object("value", "$.currency").alias("currency"),
            F.get_json_object("value", "$.price_parse_status").alias("price_parse_status"),
            F.col("value").alias("cleaned_record_json"),
        )

    raise ValueError("input_format must be one of: auto, parquet, json")


def run_pipeline(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    threshold: float,
    write_mode: str,
    input_format: str = "auto",
) -> dict[str, DataFrame]:
    cleaned = read_cleaned_input(spark, input_path, input_format)
    input_count = cleaned.count()
    normalized = normalize_products(cleaned)
    candidates = generate_candidate_pairs(normalized)
    scored = score_candidate_pairs(candidates, threshold)
    product_matches = build_product_matches(normalized, scored)
    evaluation = evaluate_matches(scored, product_matches, input_count)
    product_history = build_product_history(product_matches)
    price_summary = build_price_summary(product_matches)
    source_summary = build_source_summary(product_matches)

    validate_outputs(product_matches, price_summary, source_summary)

    outputs = {
        "product_matches": product_matches,
        "matching_evaluation": evaluation,
        "product_history": product_history,
        "product_price_summary": price_summary,
        "product_source_summary": source_summary,
    }
    write_outputs(outputs, output_path, write_mode)
    return outputs


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=os.getenv("BATCH_INPUT_PATH"), required=False)
    parser.add_argument("--output", default=os.getenv("BATCH_OUTPUT_PATH"), required=False)
    parser.add_argument(
        "--input-format",
        choices=("auto", "parquet", "json"),
        default=os.getenv("BATCH_INPUT_FORMAT", "auto"),
        help="Use parquet for production outputs or json for cleaned JSONL samples.",
    )
    parser.add_argument("--threshold", type=float, default=float(os.getenv("MATCH_THRESHOLD", "0.65")))
    parser.add_argument("--write-mode", default=os.getenv("BATCH_WRITE_MODE", "errorifexists"))
    args = parser.parse_args(argv)
    if not args.input:
        parser.error("--input or BATCH_INPUT_PATH is required")
    if not args.output:
        parser.error("--output or BATCH_OUTPUT_PATH is required")
    return args


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        outputs = run_pipeline(
            spark=spark,
            input_path=args.input,
            output_path=args.output,
            threshold=args.threshold,
            write_mode=args.write_mode,
            input_format=args.input_format,
        )
        print("OK: Batch product intelligence outputs written.", flush=True)
        outputs["matching_evaluation"].show(truncate=False)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
