"""Run the 1000-record HDFS batch demonstration."""

import os
import subprocess
from pathlib import Path
from uuid import uuid4

ROOT = Path("/workspace")
run_id = uuid4().hex
directory = Path("/state/batch-runs") / run_id
directory.mkdir(parents=True)
cleaned = directory / "cleaned"
intelligence = directory / "intelligence"

environment = os.environ.copy()
environment.update({
    "PYSPARK_PYTHON": "python3",
    "PYSPARK_DRIVER_PYTHON": "python3",
    "BATCH_INPUT_PATH": "hdfs://namenode:8020/commerceradar/raw/wdc/sample_1000.jsonl",
    "BATCH_OUTPUT_PATH": cleaned.as_uri(),
})

spark = [
    "/opt/spark/bin/spark-submit",
    "--master", "local[2]",
    "--driver-memory", "1g",
    "--conf", "spark.sql.shuffle.partitions=4",
]


def run(script, *arguments):
    print(f"START: {script}", flush=True)
    subprocess.run(
        spark + [str(ROOT / script)] + list(arguments),
        env=environment,
        check=True,
        timeout=600,
    )
    print(f"OK: {script}", flush=True)


print(f"Batch sample run: {run_id}", flush=True)
run("services/batch/clean_wdc.py")
run(
    "services/batch/product_intelligence.py",
    "--input", cleaned.as_uri(),
    "--input-format", "parquet",
    "--threshold", "0.90",
    "--output", intelligence.as_uri(),
    "--write-mode", "errorifexists",
)
run(
    "services/batch/load_clickhouse.py",
    "--input-base", intelligence.as_uri(),
)
print(f"OK: Sample batch pipeline completed; outputs={directory}", flush=True)
