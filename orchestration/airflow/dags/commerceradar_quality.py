"""Hourly full-table quality checks."""

import json
import os
import urllib.request
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_quality():
    request = urllib.request.Request(
        "http://quality-check:8090/run",
        data=b"",
        method="POST",
        headers={"X-Service-Key": os.environ["INTERNAL_SERVICE_KEY"]},
    )
    with urllib.request.urlopen(request, timeout=270) as response:
        result = json.load(response)
    if result.get("success") is not True:
        raise RuntimeError("Quality validation failed")
    print("OK: Scheduled full-table GX validation passed.")
    return result


with DAG(
    dag_id="commerceradar_quality",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["commerceradar", "quality"],
) as dag:
    PythonOperator(
        task_id="validate_full_wdc",
        python_callable=run_quality,
        execution_timeout=timedelta(minutes=5),
    )
