# CommerceRadar

E-commerce Big Data Market Intelligence Platform.

## Overview

CommerceRadar is a four-member graduation project for collecting,
processing, and analyzing product offers from multiple e-commerce sources.

The prototype runs on a single machine. Service boundaries and data
contracts are designed to support future expansion; multi-node deployment
is outside the current prototype scope.

## Current Status

Verified implementation:
- Kafka and Spark Structured Streaming with validation and checkpoints.
- Real periodic Adafruit collection and explicitly simulated replay data.
- Full cleaned WDC: 16,451,499 offers in ClickHouse.
- Full experimental identifier matching, stored in Parquet and HDFS.
- Verified staged publication of matching results to ClickHouse.
- WDC category, price and quality analytics.
- Current offers, observation history and price/stock change views.
- Explicitly mapped simulated competitor comparisons.
- FastAPI analytics and authenticated application routes.
- PostgreSQL accounts, sessions, settings and in-app alerts.
- Airflow operations and quality schedules; on-demand HDFS sample batch.
- Great Expectations full-table aggregate checks.
- Prometheus API metrics and provisioned Grafana dashboard.
- CSV/JSON snapshot exports for Power BI.

Remaining submission work:
- Complete and review the Power BI report.
- Prepare presentation, screenshots and demonstration video.
- Review fresh-machine setup.

Important limits:
- Identifier matching is experimental: high precision, very low recall.
- WDC does not establish temporal price trends or reliable store identity.
- Competitor comparisons use simulated stores and explicit demo IDs.
- Full-data batch jobs are manually invoked.
- CSV exports do not refresh automatically.
- This is a single-machine prototype.

Current implementation branch: `main`.

Detailed verified scope:
- [Submission status](docs/submission_status.md)
- [Full identifier matching](docs/full_identifier_matching.md)
- [Market demonstration](docs/market_demo.md)
- [Batch integration](docs/batch_integration_verification.md)
- [Batch orchestration](docs/batch_orchestration.md)
- [Airflow](orchestration/airflow/README.md)
- [Data quality](quality/README.md)
- [Monitoring](monitoring/README.md)

## Planned Architecture

Streaming:
Data Sources -> Kafka -> Spark Structured Streaming -> ClickHouse
-> Power BI / FastAPI

Batch:
Raw Data -> HDFS -> Parquet datasets -> Spark Batch -> ClickHouse
-> Power BI

Implemented batch intelligence:
Cleaned WDC Parquet -> Product normalization -> Candidate blocking
-> Similarity scoring -> Canonical product groups -> Historical price/source
analytics -> ClickHouse-ready batch tables.

Parquet is a file format. Batch processing will read and write Parquet
datasets stored in HDFS.

Application:
- FastAPI: backend API.
- PostgreSQL: users, sessions, settings, alert rules and notifications.

Implemented API:
FastAPI reads ClickHouse batch product-intelligence tables and the streaming
observation table, full WDC analytics and identifier coverage. Power BI report preparation is in progress.

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
docker compose config -q
```

Build the Replay Producer image:

```bash
docker compose build replay-producer
```

Run the Replay Producer in a temporary container:

```bash
docker compose run --rm replay-producer
```

The producer delivers five sample events to Kafka and exits. Create the topic first; see the service README.
The temporary container is removed after execution.

Kafka, Spark streaming, HDFS, and ClickHouse are implemented. Follow the service guides for startup and initialization.
See services/replay_producer/README.md for details.

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

The five replay sample events are synthetic; their prices are illustrative
and URLs are placeholders. The WDC samples contain historical source data.
The Adafruit collector obtains real observations. WDC records use a separate
batch contract; missing timestamps, stock, or URLs are not invented.

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

Kafka, Spark state, and ClickHouse use Docker volumes. HDFS uses persistent
bind mounts configured through HDFS_STORAGE_ROOT. In the verified setup,
large datasets and HDFS storage are on E, and Docker Desktop's disk image
is at E:/DockerDesktopData. Each teammate must configure local paths.

Before validating Compose, fill required .env settings, including a private
CLICKHOUSE_PASSWORD, and create the HDFS storage directories. Follow the
HDFS guide before starting its services. Create the Kafka topic before
sending events and the ClickHouse table before starting the streaming sink.
Use docker compose config -q to validate without displaying resolved secrets.

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

For local Replay Producer execution, also install services/replay_producer/requirements.txt.
