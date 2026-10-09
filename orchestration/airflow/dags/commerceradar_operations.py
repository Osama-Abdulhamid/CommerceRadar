"""Scheduled CommerceRadar service checks."""

import json
import os
import urllib.request
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator


def check_services():
    for path in ("/health", "/health/postgres"):
        with urllib.request.urlopen(
            f"http://api:8000{path}", timeout=30,
        ) as response:
            result = json.load(response)
        if result.get("status") != "ok":
            raise RuntimeError(f"Service check failed: {path}")
        print(f"OK: {path}")



def evaluate_alerts():
    request = urllib.request.Request(
        "http://api:8000/internal/alerts/evaluate",
        data=b"{}",
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Service-Key": os.environ["INTERNAL_SERVICE_KEY"],
        },
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        result = json.load(response)
    print("Alert evaluation:", result)
    return result


with DAG(
    dag_id="commerceradar_operations",
    description="Check services and evaluate enabled user alert rules",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["commerceradar", "operations"],
) as dag:
    checks = PythonOperator(
        task_id="check_services",
        python_callable=check_services,
        execution_timeout=timedelta(minutes=2),
    )

    alerts = PythonOperator(
        task_id="evaluate_alerts",
        python_callable=evaluate_alerts,
        execution_timeout=timedelta(minutes=3),
    )
    checks >> alerts
