"""Run HDFS sample cleaning, matching and staged ClickHouse loading."""

import json
import os
import urllib.request
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_sample_pipeline():
    request = urllib.request.Request(
        "http://batch-runner:8090/run",
        data=b"",
        method="POST",
        headers={"X-Service-Key": os.environ["INTERNAL_SERVICE_KEY"]},
    )
    with urllib.request.urlopen(request, timeout=1850) as response:
        result = json.load(response)
    if result.get("success") is not True:
        raise RuntimeError("Sample batch pipeline failed")
    print("OK: HDFS sample batch pipeline completed.")
    return result


with DAG(
    dag_id="commerceradar_batch_sample",
    description="1000-record HDFS sample: clean, match and load batch tables",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["commerceradar", "batch", "sample"],
) as dag:
    PythonOperator(
        task_id="run_sample_pipeline",
        python_callable=run_sample_pipeline,
        execution_timeout=timedelta(minutes=35),
    )
