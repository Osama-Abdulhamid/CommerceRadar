# CommerceRadar submission status

## Verified implementation
- Single-machine Docker Compose deployment.
- Kafka ingestion and Spark Structured Streaming to ClickHouse.
- Real Adafruit polling plus explicitly simulated replay events.
- Full cleaned WDC: 16,451,499 offers in ClickHouse.
- Full experimental identifier matching: 16,451,499 output offers.
- Curated matching Parquet copied to HDFS; filesystem healthy.
- Spark verified all curated HDFS offers are readable.
- Identifier results published through a verified staging-table exchange.
- API provides WDC analytics, matching coverage, observation history,
  current offers, changes and simulated competitor comparisons.
- PostgreSQL accounts, sessions, settings and user-scoped in-app alerts.
- Airflow service checks and alert evaluation every five minutes.
- Hourly quality workflow using Great Expectations on full-table metrics.
- On-demand Airflow workflow for the 1000-record HDFS batch sample.
- Prometheus API metrics and Grafana operations dashboard.
- CSV/JSON snapshot export for Power BI.

## Matching results and limitations
- Identifier-eligible offers: 2,203,388.
- Unresolved singleton offers: 14,248,111.
- Precision: 0.9901701692347378.
- Recall: 0.000784693833511571.
- Eligible offers are not equivalent to matched offers.
- Matching is experimental and conservative.
- These results are not an independent holdout evaluation.
- Reference cluster labels are used for evaluation, not predictions.

## Data scope
- WDC supplies historical offers and category/price analytics.
- WDC does not establish time-based price trends or reliable store identity.
- Adafruit supplies real observations for one monitored product.
- The competitor demonstration uses three explicitly mapped products
  at two simulated stores, not automatically matched real competitors.
- Replay demonstrates recorded price and stock changes.
- Notifications are in-app; email/SMS delivery is not configured.
- Current state and change analytics are implemented through ClickHouse views.
- Full-data matching/loading is manually invoked; only the sample batch
  pipeline is currently orchestrated through Airflow.
- Local Airflow and single-node HDFS are prototype deployments.
- Monitoring covers the API, not dedicated exporters for every component.

## Remaining acceptance work
- Complete and review the Power BI report.
- Confirm the latest real collected event reaches ClickHouse.
- Record screenshots/video and prepare the presentation.
- Review repository setup instructions for a fresh machine.
- Review and integrate the fix/batch-integration branch into the
  submission branch; currently it is pushed but not confirmed merged.

## Demo locations
- API documentation: http://localhost:18000/docs
- Airflow: http://localhost:18080
- Grafana: http://localhost:13000
- Prometheus targets: http://localhost:19090/targets
- HDFS NameNode: http://localhost:9870

Credentials remain in local private configuration.
Do not include .env or passwords in the submission.

## Suggested recording sequence
1. Show the Docker services.
2. Show full WDC counts and category/quality analytics.
3. Show readable curated HDFS output and matching evaluation.
4. Show real collector delivery and its ClickHouse observation.
5. Show simulated price history, stock changes and competitor comparison.
6. Show authenticated alerts and the successful Airflow workflows.
7. Show Grafana metrics.
8. Show the completed Power BI report.

## Power BI handoff
Latest exported snapshot:
CommerceRadar_20261010T205511Z.zip

CSV files are imported snapshots, not automatic live refreshes.
Keep currencies separate and record identifiers as text.
Label demo sources as simulated.
Format matching precision and recall as percentages.
