# CommerceRadar Local Airflow

## Deployment

Image: apache/airflow:2.11.0-python3.12.
Runs with the standalone command for a local development prototype.
Airflow metadata and logs persist in the airflow-data Docker volume.
Metadata uses SQLite, separately from the application's PostgreSQL.
This setup is not a production or high-availability deployment.

UI: http://localhost:18080

Start from the repository root:
    docker compose up -d airflow

The standalone command creates a local admin account.
Keep its generated password private.

## Workflow

DAG: commerceradar_operations
Schedule: every five minutes.
Catchup: disabled.
Maximum active runs: one.
Retries: two, separated by thirty seconds.

Task order:
1. check_services: verify ClickHouse and PostgreSQL through API health routes.
2. evaluate_alerts: evaluate enabled rules for users with alerts enabled.

The internal API endpoint requires INTERNAL_SERVICE_KEY.
The key is generated locally and stored in the ignored .env file.
Airflow does not log the key.

## Verification

Verified manually:
- DAG imports without errors.
- Both health checks pass.
- Protected alert evaluation executes successfully.
- Both tasks and the complete DAG finish successfully.

Check scheduled runs:
    docker compose exec -T airflow airflow dags list-runs -d commerceradar_operations

A successful manual test alone does not prove scheduled execution.
Look for a successful scheduled run.

## Scope

This DAG checks services and schedules alert evaluation.
The Adafruit collector runs its own polling loop.
Full WDC ingestion, cleaning and loading remain manually invoked.
This DAG does not yet orchestrate those batch jobs.

Alerts are stored in PostgreSQL and served through the authenticated API.
They are in-app notifications; no email or SMS delivery is configured.
Duplicate rule/event combinations are prevented by a database constraint.
Price-below rules can notify for each new qualifying observation.
