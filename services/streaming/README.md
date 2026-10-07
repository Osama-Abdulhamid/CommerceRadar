# Spark Structured Streaming

Reads product observations from Kafka and displays typed records in logs.
Runs Spark 3.5.7 with local[2] inside Docker.

## Build

    docker compose build spark-streaming

## Process currently available messages and exit

    docker compose run --rm spark-streaming

## Run continuously

    STREAM_TRIGGER=continuous docker compose up -d spark-streaming

## View logs

    docker compose logs --tail=80 spark-streaming

## Stop

    docker compose stop spark-streaming

## State and dependencies

The spark-state volume stores the checkpoint at /state/console-v1.
The spark-ivy volume caches the Kafka connector dependencies.
Do not run two streaming instances against the same checkpoint.

The checkpoint tracks processed Kafka offsets.
It does not deduplicate repeated event IDs.
Console output is for development and is not durable storage.

## Verified

- Read existing Kafka records.
- Converted prices to Decimal(18,2) and observation timestamps to UTC.
- Received five newly replayed messages during continuous execution.
- Restarted without displaying previously processed records again.

## Pending

- Duplicate-event handling.
- ClickHouse delivery and recovery tests.

## Schema Validation

Every Kafka payload is checked against the v1 JSON Schema with format checks.
Invalid records are excluded from the valid-record display.
Rejection logs include the reason, original JSON, topic, partition, and offset.
Decimal overflow and unparseable timestamps are also rejected.

Verified: five valid observations accepted and one incomplete record rejected.
Rejected records are logged and saved as Parquet in the spark-state volume.

## Rejected Record Storage

Path: /state/rejected-console-v1/batch-<batch_id>

Stored fields: topic, partition, offset, kafka_timestamp,
raw_json, and validation_error.

Retries overwrite the directory for the same batch ID.
Keep this storage paired with its original checkpoint.
Records processed before this feature are not backfilled.
Storage is local to this prototype; a distributed deployment needs shared storage.

Verified: one rejected record was saved and read back successfully.
