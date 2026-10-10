import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("InspectWDCIdentifiers").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
try:
    rows = (
        spark.read.parquet("file:///input/wdc_full_cleaned_v1")
        .select("source_record_id", "cleaned_record_json")
        .limit(3)
        .collect()
    )
    for row in rows:
        record = json.loads(row["cleaned_record_json"])
        print("\nRecord:", row["source_record_id"])
        print("JSON keys:", sorted(record))
        print("Identifiers:", json.dumps(
            record.get("identifiers"), ensure_ascii=False
        ))
        raw = record.get("raw_record")
        if isinstance(raw, dict):
            print("Raw identifiers:", json.dumps(
                raw.get("identifiers"), ensure_ascii=False
            ))
finally:
    spark.stop()
