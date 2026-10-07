"""Validate every cleaned WDC JSON record stored in Parquet."""

import json
import os

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CommerceRadarValidateWDC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema_path = "/workspace/contracts/v1/wdc_cleaned_record.schema.json"
with open(schema_path, encoding="utf-8") as stream:
    schema = json.load(stream)

from jsonschema import Draft202012Validator

Draft202012Validator.check_schema(schema)


def validate_partition(rows):
    from jsonschema import Draft202012Validator

    validator = Draft202012Validator(schema)
    checked = 0
    invalid = 0
    examples = []

    for row in rows:
        checked += 1
        try:
            record = json.loads(row.cleaned_record_json)
            error = next(validator.iter_errors(record), None)
            if error is not None:
                raise ValueError(error.message)
        except (ValueError, TypeError) as error:
            invalid += 1
            if len(examples) < 3:
                examples.append({
                    "source_record_id": row.source_record_id,
                    "error": str(error),
                })

    yield checked, invalid, examples


try:
    path = os.environ["BATCH_OUTPUT_PATH"]
    data = spark.read.parquet(path).select(
        "source_record_id", "cleaned_record_json"
    )

    results = data.rdd.mapPartitions(validate_partition).collect()
    checked = sum(result[0] for result in results)
    invalid = sum(result[1] for result in results)

    print(f"Checked records: {checked:,}", flush=True)
    print(f"Invalid records: {invalid:,}", flush=True)

    examples = [
        example
        for result in results
        for example in result[2]
    ]
    for example in examples[:10]:
        print(json.dumps(example, ensure_ascii=False), flush=True)

    if checked != 16_451_499:
        raise ValueError("Unexpected output record count")
    if invalid:
        raise ValueError("Batch contract violations found")

    print("OK: All full-dataset JSON records match the batch contract.",
          flush=True)
finally:
    spark.stop()
