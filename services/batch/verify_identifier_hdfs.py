import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("VerifyIdentifierHDFS").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
base = "hdfs://namenode:8020/commerceradar/curated/wdc_identifier_full_v1"

try:
    count = spark.read.parquet(base + "/offers").count()
    assert count == 16451499, f"Unexpected offer count: {count}"

    rows = spark.read.text(base + "/evaluation").collect()
    assert len(rows) == 1, "Expected one evaluation record"
    metrics = json.loads(rows[0]["value"])
    assert metrics["input_records"] == count

    print(f"OK: HDFS contains {count} readable offers.", flush=True)
    print("Precision:", metrics["precision"])
    print("Recall:", metrics["recall"])
finally:
    spark.stop()
