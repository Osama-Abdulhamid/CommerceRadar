from pathlib import Path
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ExtractIdentifierSample").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
try:
    output = Path("/output/wdc_identifier_sample_50000.jsonl")
    rows = (
        spark.read.parquet("file:///input/wdc_full_cleaned_v1")
        .select("cleaned_record_json")
        .limit(50000)
        .toLocalIterator()
    )
    count = 0
    with output.open("x", encoding="utf-8") as stream:
        for row in rows:
            if row["cleaned_record_json"] is None:
                raise ValueError("Missing cleaned record JSON")
            stream.write(row["cleaned_record_json"] + "\n")
            count += 1
    print(f"OK: Extracted {count} records.")
finally:
    spark.stop()
