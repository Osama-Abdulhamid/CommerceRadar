# CommerceRadar Data Quality

Great Expectations version: 1.24.0.

ClickHouse scans the full wdc_cleaned_offers table and returns aggregate
metrics. GX validates those metrics in a small Pandas dataframe.
The full dataset is not downloaded into Python memory.

Checks:
- The table is nonempty.
- Parsed-price count is positive and does not exceed total rows.
- No empty source record IDs.
- No unrecognized price parsing statuses.
- Parsed prices have a nonnegative amount and a three-letter currency.
- Rows with other parsing statuses do not contain a parsed price.

Verified full-table results:
- Total offers: 16,451,499.
- Parsed-price offers: 1,522,526, approximately 9.25%.
- All four invalid-row counters: zero.
- All six expectations passed.

These checks do not establish product-matching accuracy, completeness
of prices, currency correctness, or uniqueness of source record IDs.

Reports persist under COMMERCE_DATA_ROOT/quality-reports.
Each run creates a timestamped JSON report and updates latest.json.

Start the internal runner:
    docker compose --profile quality up -d --build quality-check

Run a check directly:
    docker compose --profile quality run --rm quality-check python /app/check_data.py

Airflow DAG: commerceradar_quality.
Schedule: hourly, catchup disabled.
The runner requires INTERNAL_SERVICE_KEY and is not published to the host.
Failed expectations cause the runner and Airflow task to fail.
Concurrent requests are rejected while a check is running.

Manual DAG verification:
    docker compose exec -T airflow airflow dags test commerceradar_quality 2026-10-09
