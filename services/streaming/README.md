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

- Full data-contract validation and invalid-record handling.
- Duplicate-event handling.
- ClickHouse delivery and recovery tests.
