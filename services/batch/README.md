> Current integration behavior and verified limitations: see docs/batch_integration_verification.md. The loader uses staging replacement; --truncate is disabled.

# WDC Batch Cleaning

## Purpose

Clean historical WDC product offers and write Parquet using Spark.
This job currently runs in local mode inside Docker.
HDFS integration is not implemented yet.

## Dataset

WDC Product Data Corpus V2, English non-normalized offers.

Download URL:
https://data.dws.informatik.uni-mannheim.de/largescaleproductcorpus/data/v2_nonnorm/offers_corpus_english_v2_non_norm.json.gz

Verified compressed size: 3,793,679,127 bytes.

Local data directory used for this run:
E:\CommerceRadarData

WSL equivalent:
/mnt/e/CommerceRadarData

The full dataset and generated Parquet are not stored in Git.
Small raw and cleaned samples are included in data/samples.

This is historical catalog data, not a live price feed.
Do not invent observation timestamps, product URLs or availability.

## Cleaning Rules

The job reuses scripts/clean_wdc_sample.py.

- Normalize whitespace, HTML entities and language-tag syntax.
- Preserve source record IDs and cluster IDs.
- Parse prices conservatively.
- Keep missing or ambiguous prices null and record the reason.
- Flag missing titles.
- Preserve each original record inside cleaned_record_json.
- Fail if a parsed price cannot fit Decimal(18,2).

The JSON record contract is:
contracts/v1/wdc_cleaned_record.schema.json

The Parquet output contains analytical columns plus
cleaned_record_json, which embeds the cleaned JSON record
and its original raw record.

The 1,000-record cleaned sample passed JSON Schema validation.
The cleaning job does not perform schema validation itself.
A separate validation job checked all 16,451,499 output JSON records
against the batch contract and found zero violations.

## Run the Full Dataset

Run from the repository root in Ubuntu WSL.
Docker Desktop must be running.

Place the downloaded archive in:
/mnt/e/CommerceRadarData/raw/wdc/

Create output, temporary and log directories:

```bash
mkdir -p /mnt/e/CommerceRadarData/processed /mnt/e/CommerceRadarData/spark-tmp /mnt/e/CommerceRadarData/logs
```

Run:

```bash
set -o pipefail

docker run --rm --user 0:0 \
  --mount type=bind,source="$PWD",target=/workspace,readonly \
  --mount type=bind,source=/mnt/e/CommerceRadarData/raw/wdc,target=/input,readonly \
  --mount type=bind,source=/mnt/e/CommerceRadarData/processed,target=/output \
  --mount type=bind,source=/mnt/e/CommerceRadarData/spark-tmp,target=/spark-tmp \
  -e BATCH_INPUT_PATH=/input/offers_corpus_english_v2_non_norm.json.gz \
  -e BATCH_OUTPUT_PATH=/output/wdc_full_cleaned_v1 \
  -e PYSPARK_PYTHON=python3 \
  -e PYSPARK_DRIVER_PYTHON=python3 \
  --entrypoint /opt/spark/bin/spark-submit \
  apache/spark:3.5.7-java17-python3 \
  --master 'local[2]' \
  --driver-memory 2g \
  --conf spark.local.dir=/spark-tmp \
  --conf spark.sql.files.maxRecordsPerFile=250000 \
  /workspace/services/batch/clean_wdc.py \
  2>&1 | tee /mnt/e/CommerceRadarData/logs/wdc_full_cleaned_v1.log
```

The output path must not already exist.
Use a new output path for a separate run.
Change the log filename too, to preserve previous logs.

The container runs as root for this Windows-drive bind mount
because a non-root run encountered permission problems.
This is a local prototype configuration.

A single gzip input is not splittable, so its reading stage
uses one task even with local[2].

## Verified Full Run

- Saved records: 16,451,499.
- Duplicate source ID groups: 0.
- Parquet written and read back successfully.
- Shell exit code: 0.

| Price parsing status | Records |
| --- | ---: |
| parsed | 1,522,526 |
| missing | 14,474,182 |
| missing_amount | 57,315 |
| unknown_currency | 254,282 |
| ambiguous_number_format | 88,680 |
| precision_or_separator_ambiguity | 14,678 |
| multiple_amounts | 38,079 |
| conflicting_currencies | 1,493 |
| negative_amount | 264 |

These counts describe the current parsing rules.
They do not prove that every parsed price is semantically correct.

## Remaining Work

- Expand cleaning checks using additional real examples.
- Run product matching and historical aggregations against the full local/HDFS dataset.
- Load generated analytical Parquet outputs into ClickHouse.
- Improve storage layout to reduce duplicated information.

## Product Intelligence

Member 3 batch intelligence is implemented in:

```text
services/batch/product_intelligence.py
```

The job reads cleaned WDC Parquet, normalizes titles, brands and categories,
generates blocked candidate pairs, scores candidate similarity, assigns
deterministic canonical product IDs and writes curated analytical Parquet
outputs.

Outputs:

```text
product_matches
matching_evaluation
product_history
product_price_summary
product_source_summary
```

Availability analytics are intentionally omitted because the cleaned WDC
contract does not contain a trustworthy availability field. Latest-price
analytics are also omitted because the cleaned WDC records do not include a
trustworthy observation timestamp.

Run against HDFS:

```bash
docker run --rm --user 0:0 \
  --network commerceradar_commerceradar \
  --mount type=bind,source="$PWD",target=/workspace,readonly \
  --mount type=bind,source=/mnt/e/CommerceRadarData/spark-tmp,target=/spark-tmp \
  -e BATCH_INPUT_PATH=hdfs://namenode:8020/commerceradar/cleaned/wdc/wdc_full_cleaned_v1 \
  -e BATCH_OUTPUT_PATH=hdfs://namenode:8020/commerceradar/curated/wdc/batch_product_intelligence_v1 \
  -e MATCH_THRESHOLD=0.65 \
  -e PYSPARK_PYTHON=python3 \
  -e PYSPARK_DRIVER_PYTHON=python3 \
  --entrypoint /opt/spark/bin/spark-submit \
  apache/spark:3.5.7-java17-python3 \
  --master local[2] \
  --driver-memory 2g \
  --conf spark.local.dir=/spark-tmp \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 \
  /workspace/services/batch/product_intelligence.py
```

See `docs/batch_intelligence.md` for the audit, methodology, validation
checks, ClickHouse DDL and reproduction notes.

Run against the committed cleaned JSONL development sample:

```bash
docker run --rm --user 0:0 \
  --mount type=bind,source="$PWD",target=/workspace \
  --entrypoint /opt/spark/bin/spark-submit \
  apache/spark:3.5.7-java17-python3 \
  --master local[1] \
  /workspace/services/batch/product_intelligence.py \
  --input /workspace/data/samples/wdc_english_v2.cleaned.1000.jsonl \
  --input-format json \
  --output /workspace/data/processed/batch_intelligence_wdc_sample \
  --write-mode overwrite
```

Load generated outputs into ClickHouse after creating
`infra/clickhouse/002_batch_product_intelligence.sql` tables:

```bash
docker run --rm --user 0:0 \
  --network commerceradar_commerceradar \
  --mount type=bind,source="$PWD",target=/workspace,readonly \
  -e CLICKHOUSE_HTTP_URL=http://clickhouse:8123 \
  -e CLICKHOUSE_DB=commerceradar \
  -e CLICKHOUSE_USER=commerceradar \
  -e CLICKHOUSE_PASSWORD="$CLICKHOUSE_PASSWORD" \
  --entrypoint /opt/spark/bin/spark-submit \
  apache/spark:3.5.7-java17-python3 \
  --master local[1] \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 \
  /workspace/services/batch/load_clickhouse.py \
  --input-base hdfs://namenode:8020/commerceradar/curated/wdc/batch_product_intelligence_sample_v1 \
  --truncate
```

## Full Dataset Validation

Verified after cleaning:

- Source records: 16,451,499.
- Saved records: 16,451,499.
- All embedded cleaned JSON records checked against the batch contract.
- Contract violations: 0.
- Duplicate source ID groups: 0.

Schema validation checks structure and declared constraints.
It does not prove semantic accuracy of parsed prices.

### Run Validation

Run from the repository root.
The validation uses the locally built Spark Streaming image,
which includes jsonschema. Build it first if unavailable:

```bash
docker compose build spark-streaming
```

Validate the existing Parquet output without modifying it:

```bash
docker run --rm --user 0:0 \
  --mount type=bind,source="$PWD",target=/workspace,readonly \
  --mount type=bind,source=/mnt/e/CommerceRadarData/processed,target=/output,readonly \
  --mount type=bind,source=/mnt/e/CommerceRadarData/spark-tmp,target=/spark-tmp \
  -e BATCH_OUTPUT_PATH=/output/wdc_full_cleaned_v1 \
  -e PYSPARK_PYTHON=python3 \
  -e PYSPARK_DRIVER_PYTHON=python3 \
  --entrypoint /opt/spark/bin/spark-submit \
  commerceradar-spark-streaming:latest \
  --master 'local[2]' \
  --driver-memory 2g \
  --conf spark.local.dir=/spark-tmp \
  /workspace/services/batch/validate_wdc.py
```

The script reports results after all partitions finish.
It fails if any JSON record violates the contract or
the record count differs from 16,451,499.
