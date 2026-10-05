# CommerceRadar

E-commerce Big Data Market Intelligence Platform.

## Overview

CommerceRadar is a four-member graduation project for collecting,
processing, and analyzing product offers from multiple e-commerce sources.

The prototype runs on a single machine. Service boundaries and data
contracts are designed to support future expansion; multi-node deployment
is outside the current prototype scope.

## Current Status

Implemented:
- Local Git repository on the main branch.
- Project directory structure.
- Product observation JSON Schema v1.0.
- Five synthetic sample events in JSONL format.
- Sample validation script with format and duplicate-ID checks.
- Pinned development dependencies.
- Environment variable template.
- Docker Compose skeleton, validated with docker compose config.

No application or infrastructure services are running yet.

## Planned Architecture

Streaming:
Data Sources -> Kafka -> Spark Structured Streaming -> ClickHouse
-> Power BI / FastAPI

Batch:
Raw Data -> HDFS -> Parquet datasets -> Spark Batch -> ClickHouse
-> Power BI

Parquet is a file format. Batch processing will read and write Parquet
datasets stored in HDFS.

Application:
- FastAPI: backend API.
- PostgreSQL: users, settings, and alert rules only.

Supporting components:
- Airflow: orchestration.
- Great Expectations: data quality.
- Prometheus and Grafana: monitoring.

Data sources:
- WDC Product Dataset for historical batch processing.
- APIs and permitted web scraping.
- Python Replay Producer for the streaming demonstration.

Excluded technologies:
- Flink.
- Hive.
- Elasticsearch.

## Development Environment

- Windows host.
- Ubuntu on WSL2.
- Docker Desktop using Linux containers and Ubuntu WSL integration.
- Project stored inside the Ubuntu filesystem.
- Power BI runs on Windows.

The foundation validation tooling was verified with Python 3.14.
Python versions for individual services will be selected separately.

## Local Setup

Run these commands from the project root, one at a time.

Create a virtual environment:

```bash
python3 -m venv .venv
```

On Ubuntu, if this fails because ensurepip is unavailable, install the
matching python3.X-venv package for your Python version, then retry.

Activate the environment:

```bash
source .venv/bin/activate
```

Install development dependencies:

```bash
python -m pip install -r requirements-dev.txt
```

Create your local environment file if it does not already exist:

```bash
cp --update=none .env.example .env
```

This copy command is intended for the Ubuntu development environment.

Validate sample events:

```bash
python scripts/validate_sample_data.py
```

Expected final output:

```text
OK: All 5 events match schema v1.0; event IDs are unique.
```

Validate the Compose configuration:

```bash
docker compose config
```

The current Compose file has no services. Do not run docker compose up yet.

## Data Contract

Schema:
contracts/v1/product_observation.schema.json

Sample:
data/samples/product_observations.v1.jsonl

Each JSONL line is one product offer observed at a source and time.

Rules:
- schema_version is 1.0.
- event_id is a UUID.
- observed_at is a UTC timestamp ending in Z.
- Product identity within a source uses source and source_product_id.
- price is a non-negative decimal string with two fractional digits.
- Supported currencies are EGP, USD, EUR, GBP, SAR, and AED.
- availability is in_stock, out_of_stock, or unknown.
- brand and category are optional.
- Undeclared fields are rejected.

The validator checks schema validity, event fields, formats, and duplicate
event IDs within the sample file. This is not pipeline-wide deduplication.

All sample events are synthetic. Prices are illustrative and product URLs
are placeholders.

## Directory Layout

| Path | Purpose |
|---|---|
| docs/ | Architecture, decisions, and team instructions |
| contracts/ | Versioned data schemas |
| data/samples/ | Small shareable development samples |
| data/raw/ | Local raw datasets; ignored by Git |
| data/processed/ | Local processing outputs; ignored by Git |
| services/ingestion/ | APIs and permitted scraping |
| services/replay_producer/ | Streaming demonstration producer |
| services/streaming/ | Spark Structured Streaming code |
| services/batch/ | Spark Batch, cleaning, and product matching |
| services/api/ | FastAPI backend |
| infra/ | Infrastructure configuration and initialization files |
| orchestration/airflow/dags/ | Airflow workflows |
| quality/ | Data quality rules |
| monitoring/ | Prometheus and Grafana configuration |
| tests/ | Automated tests |
| scripts/ | Development and validation utilities |

Git does not track empty directories. Some planned directories will not
appear in a fresh clone until files are added.

Database and HDFS runtime storage will be configured using Docker volumes.

## Team Responsibilities

| Member | Responsibilities |
|---|---|
| Lead | Integration, Docker, Kafka, Spark Structured Streaming |
| Member 2 | Dataset, APIs, scraping, Replay Producer |
| Member 3 | HDFS, Parquet, Spark Batch, cleaning, product matching |
| Member 4 | ClickHouse, FastAPI, PostgreSQL, Power BI |

Shared: Airflow, testing, monitoring, and final integration.

## Implementation Order

1. Foundation, data contract, samples, and validation.
2. Replay Producer and Kafka.
3. Spark Structured Streaming and ClickHouse.
4. HDFS, Parquet, and Spark Batch.
5. FastAPI, PostgreSQL, and Power BI.
6. Airflow, data quality, and monitoring.
7. Full dataset testing and team handoff.

Each component will be configured and tested before the next integration.

## Configuration and Git Rules

- .env.example is the shared configuration template.
- .env contains local settings and is ignored by Git.
- Compose reads .env for variable substitution.
- Application settings must be explicitly passed to containers or loaded
  by application code; .env is not automatically injected into containers.
- Never commit credentials or large datasets.
- Keep development samples small.
- Document and version data contract changes before changing producers
  or consumers.
