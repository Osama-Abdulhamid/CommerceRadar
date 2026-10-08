# Team Handoff

## Starting Point
Repository: https://github.com/Osama-Abdulhamid/CommerceRadar
Integration branch: feature/kafka-streaming
Main has not yet received the integration changes.

Each teammate clones the repository and checks out the integration branch.
Create your own feature branch from it. Submit changes through pull requests.
Do not overwrite shared contracts or Compose settings without coordination.

## Already Available
- Dockerized Kafka, Replay Producer and Spark streaming.
- Streaming contract validation and rejected-record storage.
- Real Adafruit collector, one request per execution.
- Verified Adafruit -> Kafka -> Spark -> ClickHouse path.
- Persistent ClickHouse and HDFS storage.
- WDC cleaning and batch validation scripts.
- Raw and cleaned WDC samples committed to Git.
- Full WDC dataset: 16,451,499 validated cleaned records.

## Local Setup
Read README.md and the relevant service guides.
Install Docker Desktop and enable WSL integration on Windows.
Create your own .env from .env.example and configure local paths.
Set a private ClickHouse password; never share or commit .env.
Create the storage directories required by the HDFS guide.
Initialize the Kafka topic and ClickHouse table before running the pipeline.

Code is shared through Git. Docker services run independently on each machine.
The full dataset and database volumes are not included in a clone.
Use the small committed samples first.
Coordinate full-dataset transfer or download separately.

## Osama — Lead
Own Docker integration, Kafka and Spark Structured Streaming.
Next: price/stock change detection, retry behavior and integration reviews.
Maintain shared contracts and reproducible setup instructions.

## Member 2 — Ingestion
Extend the existing ingestion and Replay Producer services.
Next: configurable product lists, permitted periodic collection,
timeouts, backoff and source documentation.
Keep the current observation contract and Kafka key convention.
Deliver: Docker execution and verified valid events in Kafka.

## Member 3 — Batch
Extend the existing cleaning, HDFS and Parquet foundation.
Next: product matching using WDC cluster IDs for evaluation,
historical aggregations and batch-to-ClickHouse integration.
Preserve source IDs and document uncertain/missing values.
Deliver: reproducible sample runs, quality metrics and documented outputs.

## Member 4 — Serving and Application
Use the existing ClickHouse service and observation table.
Next: analytical queries/views, FastAPI, PostgreSQL and Power BI.
PostgreSQL stores users, settings and alert rules only.
Use FINAL where the current observation table needs logical deduplication.
Deliver: documented API endpoints and a dashboard using stored observations.

## Shared Work
Airflow, Great Expectations, monitoring, testing and final integration.
Develop against samples before running full datasets.
Document commands, expected results and limitations with each change.
