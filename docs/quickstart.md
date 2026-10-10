# CommerceRadar local quickstart

## Scope
Run from the repository root in Linux or WSL2 with Docker Compose.
Power BI runs separately on Windows.

This guide initializes a new local prototype.
It does not restore the full WDC dataset or existing database contents.
The full dataset is not included in Git or the source ZIP.
Fresh-machine startup has not yet been independently verified.

## 1. Configure a fresh checkout
Do not replace an existing .env.
Copy .env.example to .env only if .env does not exist.

Set these private values locally:
- CLICKHOUSE_PASSWORD
- POSTGRES_PASSWORD
- AIRFLOW_SECRET_KEY
- INTERNAL_SERVICE_KEY
- GRAFANA_ADMIN_PASSWORD

Use independently generated random values.
Do not commit or include .env in the submission.

Adjust COMMERCE_DATA_ROOT and HDFS_STORAGE_ROOT for your machine.
The example uses /mnt/e/CommerceRadarData.
Keep CLICKHOUSE_DB=commerceradar because application SQL uses that name.
Keep POSTGRES_DB=commerceradar_app unless you adjust the application config.

Create directories matching your configured paths, for example:

```bash
mkdir -p /mnt/e/CommerceRadarData/hdfs/namenode
mkdir -p /mnt/e/CommerceRadarData/hdfs/datanode
mkdir -p /mnt/e/CommerceRadarData/quality-reports
mkdir -p /mnt/e/CommerceRadarData/curated
mkdir -p /mnt/e/CommerceRadarData/processed
mkdir -p /mnt/e/CommerceRadarData/logs
docker compose config -q
```

## 2. Start storage and Kafka

```bash
docker compose up -d --wait --wait-timeout 180 \
  clickhouse postgres kafka namenode datanode
```

Create the Kafka topic:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic product-observations.v1 \
  --partitions 3 --replication-factor 1
```

## 3. Initialize ClickHouse and PostgreSQL

Run ClickHouse SQL files in filename order:

```bash
for sql in infra/clickhouse/00*.sql; do
  docker compose exec -T clickhouse sh -c \
    'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
    < "$sql" || break
done
```

All seven files must complete without errors before continuing.

Initialize the application schema:

```bash
docker compose exec -T postgres sh -c \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
  < infra/postgres/001_application.sql
```

## 4. Start streaming and API

```bash
STREAM_TRIGGER=continuous docker compose up -d --build spark-streaming
docker compose up -d --build api
```

Wait for application startup, then verify:

```bash
curl --fail http://localhost:18000/health
curl --fail http://localhost:18000/health/postgres
```

Publish the original synthetic sample once:

```bash
docker compose run --rm --build replay-producer
```

Start real periodic collection:

```bash
ADAFRUIT_MAX_CYCLES=0 ADAFRUIT_POLL_SECONDS=60 \
docker compose --profile ingestion up -d --build adafruit-collector
```

## 5. Optional monitoring and orchestration

```bash
docker compose --profile quality --profile batch up -d --build \
  quality-check batch-runner
docker compose up -d prometheus grafana airflow
```

Quality checks require loaded WDC data and will fail on an empty table.
The sample batch DAG requires its HDFS input.
Do not present successful container startup as successful workflow execution.

## 6. Optional HDFS sample input

```bash
docker compose exec -T namenode \
  hdfs dfs -mkdir -p /commerceradar/raw/wdc

docker compose exec -T namenode \
  hdfs dfs -put - \
  /commerceradar/raw/wdc/sample_1000.jsonl \
  < data/samples/wdc_english_v2.raw.1000.jsonl
```

The upload refuses an existing destination; do not overwrite it blindly.
The on-demand DAG is commerceradar_batch_sample.
It processes the 1000-record sample, not the full dataset.

## 7. Full-data restoration
See:
- docs/full_identifier_matching.md
- docs/batch_integration_verification.md
- infra/hdfs/README.md

Obtain the external cleaned and curated Parquet datasets separately.
Loaders:
- scripts/load_wdc_parquet.py
- scripts/load_identifier_parquet.py

Expected full-dataset offer count: 16,451,499.
Both loaders use staging replacement and retain prior table contents.
Avoid concurrent loads and inspect failed staging runs before retrying.
A full WDC load is not required merely to demonstrate replay streaming.

## Interfaces
- API: http://localhost:18000/docs
- Airflow: http://localhost:18080
- Grafana: http://localhost:13000
- Prometheus: http://localhost:19090/targets
- HDFS: http://localhost:9870

Grafana username is admin; password is locally configured.
Airflow standalone generates its own local admin credentials.
Keep passwords out of screenshots and logs shared for submission.

## Existing verified machine
Do not recreate .env, clear checkpoints, format HDFS or reload full data.
Restart the required services using the commands above.
Keep STREAM_TRIGGER=continuous and ADAFRUIT_MAX_CYCLES=0 explicit.

## Power BI
CSV exports are snapshots:
    python scripts/export_powerbi.py

The export requires full WDC/matching data and local evaluation files.
Share the generated ZIP, not .env.
The PBIX report is a separate deliverable.

## Persistence
Git stores code and configuration templates, not populated databases.
Docker volumes and HDFS bind mounts hold local data.
Do not use docker compose down -v unless deliberate data deletion is intended.
