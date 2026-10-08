"""Create a tiny cleaned-WDC Parquet fixture for integration checks."""

from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[1]").appName("batch-fixture").getOrCreate()

rows = [
    ("1", "100", "Samsung Galaxy S24 128GB Black", "Samsung", "Phones", "799.00", "USD", "parsed", "{}"),
    ("2", "100", "Galaxy S24 128 GB Samsung Black", "SAMSUNG", "Phones", "749.00", "USD", "parsed", "{}"),
    ("3", "200", "Krowne Royal Wall Mount Faucet", "Krowne", "Tools", "129.00", "USD", "parsed", "{}"),
    ("4", "300", "Sony Headphones WH1000XM5", "Sony", "Audio", "299.00", "USD", "parsed", "{}"),
]
columns = [
    "source_record_id",
    "cluster_id",
    "title",
    "brand",
    "category",
    "price",
    "currency",
    "price_parse_status",
    "cleaned_record_json",
]

spark.createDataFrame(rows, columns).write.mode("overwrite").parquet(
    "/workspace/data/processed/batch_intelligence_test/input"
)
spark.stop()
