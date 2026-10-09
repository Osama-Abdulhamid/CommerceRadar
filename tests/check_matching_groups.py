import sys
from decimal import Decimal

sys.path.insert(0, "/workspace")

from pyspark.sql import SparkSession
from services.batch.product_intelligence import build_product_matches

spark = (
    SparkSession.builder
    .appName("VerifyMatchingGroups")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

try:
    rows = [
        (
            str(i), "wdc", str(i), "cluster", "Product",
            "product", "Brand", "brand", "Category", "category",
            Decimal("10.00"), "USD", "parsed",
        )
        for i in range(1, 5)
    ]
    columns = [
        "source_record_id", "source", "source_product_id",
        "cluster_id", "title", "normalized_title", "brand",
        "normalized_brand", "category", "normalized_category",
        "price_decimal", "currency", "price_parse_status",
    ]
    normalized = spark.createDataFrame(rows, columns)

    pairs = spark.createDataFrame(
        [("1", "2", 0.9, True), ("2", "3", 0.9, True)],
        [
            "left_source_record_id", "right_source_record_id",
            "match_confidence", "is_match",
        ],
    )

    result = build_product_matches(normalized, pairs)
    records = {
        row.source_record_id: row
        for row in result.collect()
    }

    assert len(records) == 4
    assert (
        records["1"].canonical_product_id
        == records["2"].canonical_product_id
        == records["3"].canonical_product_id
    ), "Connected products were split"

    assert (
        records["4"].canonical_product_id
        != records["1"].canonical_product_id
    ), "Unrelated product was merged"

    assert records["4"].match_method == "singleton"
    print("OK: Products 1, 2, 3 share one group; product 4 stays separate.")
finally:
    spark.stop()
