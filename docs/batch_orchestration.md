# Batch Sample Orchestration

DAG: commerceradar_batch_sample.
Runs on demand; no automatic schedule is configured.

Input:
hdfs://namenode:8020/commerceradar/raw/wdc/sample_1000.jsonl

Stages:
1. Spark cleaning with shared WDC cleaning rules.
2. Spark product matching and price summaries at threshold 0.90.
3. Staged loading of four batch intelligence tables into ClickHouse.

Airflow calls an authenticated internal batch runner.
The runner accepts one pipeline request at a time.
No Docker socket is mounted into Airflow or the runner.

Outputs persist in batch-state under /state/batch-runs/<run-id>.
Spark output paths explicitly use file:// to select local storage.
The raw input is read from HDFS.

Verified:
- Complete DAG test succeeded.
- Product matches: 1000 rows and 1000 distinct source record IDs.
- Price summaries: 106 rows.
- Source summaries: 106 rows.
- Evaluation: 1000 inputs, 87 predicted pairs.
- Development-sample precision: 0.781609; recall: 1.0.
- Full WDC table remained at 16,451,499 rows.

Matching remains experimental; these are not independent evaluation results.
Full-dataset matching and ingestion are not orchestrated by this DAG.

Loading replaces the four active batch intelligence tables.
Previous contents remain in run-specific staging tables.
Each table exchange is atomic separately, not one transaction across all four.
Do not run another batch loader concurrently.

Start:
    docker compose --profile batch up -d --build batch-runner

Test:
    docker compose exec -T airflow airflow dags test commerceradar_batch_sample 2026-10-09

Pipeline details:
    docker compose logs --tail 100 batch-runner
