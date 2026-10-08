# ClickHouse Local Prototype

## Deployment
- Version verified: 25.8.33.6; image pinned by digest in .env.example.
- Configure CLICKHOUSE_DB, CLICKHOUSE_USER and CLICKHOUSE_PASSWORD in .env.
- Supplied table SQL expects CLICKHOUSE_DB=commerceradar.
- Windows HTTP endpoint: http://localhost:18123.
- Docker HTTP endpoint: http://clickhouse:8123.
- Memory limit: 2 GiB; CPU limit: 2 cores.
- Persistent storage: Docker volume clickhouse-data.
- Docker Desktop disk image was moved to E:/DockerDesktopData.
- Direct Windows-drive storage failed when renaming directories with open files.
- Linux-volume storage resolved the insertion failure.
- Old storage at E:/CommerceRadarData/clickhouse is retained as a migration backup.
- Never commit .env or passwords.

## First Setup
Run from the repository root.

Start ClickHouse:
    docker compose up -d --wait --wait-timeout 120 clickhouse

Create the table before starting Spark:
    docker compose exec -T clickhouse sh -c 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' < infra/clickhouse/001_product_observations.sql

## Streaming
- Spark reads Kafka and validates observations against contract v1.0.
- Valid observations are inserted synchronously over HTTP.
- Checkpoint: /state/clickhouse-v1.
- Rejected records: /state/rejected-clickhouse-v1/batch-<batch_id>.
- Insertion failures fail the Spark batch so it can be retried.
- ReplacingMergeTree replaces identical sorting keys during merges.
- FINAL queries apply logical deduplication.
- Partial writes and retries remain possible; this is not exactly-once delivery.

Build:
    docker compose build spark-streaming

Process the available backlog and exit:
    STREAM_TRIGGER=available_now docker compose up -d spark-streaming

Process repeated five-second micro-batches:
    STREAM_TRIGGER=continuous docker compose up -d spark-streaming

Here continuous is an application setting, not Spark Continuous Processing.

## Verified Results
- Initial backlog: 26 records; 24 valid and 2 rejected test records.
- FINAL returned 9 unique observations: Adafruit 3, shop A 4, shop B 2.
- ClickHouse restart preserved those observations.
- Spark checkpoint restart exited successfully; table remained at 9 rows.
- A fresh Adafruit event was subsequently verified in ClickHouse:
  a95abcb6-6a59-4aad-a473-04c6e47af2aa.
- Product 5813, price 200 USD, availability in_stock.
- Verified path: Adafruit -> Kafka -> Spark -> ClickHouse.
- The collector fetches once per run; periodic polling is not implemented yet.

## Safe Shutdown
    docker compose stop -t 60

Preserve the Docker volumes. Git contains code and configuration,
not the stored database or full WDC dataset.
