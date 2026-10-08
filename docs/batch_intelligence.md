# Member 3 Batch Intelligence

## Repository Audit

Inspected:
- `README.md`
- `docs/team_handoff.md`
- `contracts/v1/`
- `data/samples/`
- `services/batch/`
- `services/streaming/`
- `infra/hdfs/`
- `infra/clickhouse/`
- `compose.yaml`
- `scripts/`

Existing batch components:
- WDC raw and cleaned development samples are committed under `data/samples/`.
- `services/batch/clean_wdc.py` cleans WDC JSON into Parquet with Spark.
- `services/batch/validate_wdc.py` validates embedded cleaned JSON records against the WDC batch contract.
- `scripts/clean_wdc_sample.py` contains the shared text and price cleaning rules.
- HDFS NameNode/DataNode services are defined in `compose.yaml`.
- HDFS documentation defines `/commerceradar/raw/wdc`, `/commerceradar/cleaned/wdc`, and `/commerceradar/curated/wdc`.

Existing WDC schema:
- Contract: `contracts/v1/wdc_cleaned_record.schema.json`.
- Core fields: `source_record_id`, `cluster_id`, `title`, `brand`, `category`, `identifiers`, `price`, `currency`, `price_parse_status`, `quality_flags`, and `raw_record`.
- The cleaned Parquet job also stores analytical columns plus `cleaned_record_json`.

Existing WDC cleaning pipeline:
- Cleaning preserves missing values.
- Prices are parsed conservatively into `Decimal(18,2)` compatible strings.
- Missing, ambiguous, or unsupported prices remain null with a parse status.
- No timestamps, product URLs, availability, brands, or categories are invented.

Existing Parquet locations:
- Local verified output: `/mnt/e/CommerceRadarData/processed/wdc_full_cleaned_v1`.
- HDFS verified output: `/commerceradar/cleaned/wdc/wdc_full_cleaned_v1`.
- New curated output target: `/commerceradar/curated/wdc/batch_product_intelligence_v1`.

Existing HDFS locations:
- Raw WDC archive: `/commerceradar/raw/wdc`.
- Cleaned WDC Parquet: `/commerceradar/cleaned/wdc`.
- Curated analytical outputs: `/commerceradar/curated/wdc`.

Existing Spark jobs:
- `services/batch/clean_wdc.py`
- `services/batch/validate_wdc.py`
- Spark Structured Streaming under `services/streaming/`

Existing ClickHouse tables:
- `commerceradar.product_observations` from `infra/clickhouse/001_product_observations.sql`.
- This is the streaming table and is left unchanged.

Existing contracts:
- `contracts/v1/product_observation.schema.json`
- `contracts/v1/wdc_cleaned_record.schema.json`

Existing tests:
- `scripts/validate_sample_data.py` validates streaming sample observations.
- No automated Member 3 matching or analytics tests existed before this work.

Existing documentation:
- Root `README.md`
- `docs/team_handoff.md`
- `services/batch/README.md`
- `infra/hdfs/README.md`
- `infra/clickhouse/README.md`

Missing Member 3 functionality:
- Product normalization for matching.
- Scalable candidate generation/blocking.
- Similarity scoring and matching decisions.
- Deterministic canonical product IDs.
- Matching evaluation using `cluster_id` where available.
- Historical price and source summaries.
- Data-quality validation for batch outputs.
- ClickHouse batch analytical table DDL.
- Reproduction commands and focused tests.

## Pipeline

`services/batch/product_intelligence.py` reads cleaned WDC Parquet and writes:

- `product_matches`
- `matching_evaluation`
- `product_history`
- `product_price_summary`
- `product_source_summary`

The job uses Spark DataFrames throughout production processing. It does not use pandas and does not perform a global Cartesian comparison.

## Matching Methodology

Normalization:
- Lowercase text.
- Apply Unicode NFKC normalization.
- Remove WDC language-tag artifacts such as `@en`.
- Replace punctuation and separators with spaces.
- Collapse whitespace.
- Preserve original source fields.

Blocking:
- Derive up to four selective title tokens per record.
- Explode those tokens into blocking rows.
- Self-join only records sharing a blocking token and normalized category.
- Require normalized brand equality when both sides have a brand.
- Keep records with a missing brand eligible for candidate generation.

Similarity features:
- Token Jaccard similarity over normalized titles.
- Brand agreement.
- Category agreement.

Default threshold:
- `MATCH_THRESHOLD=0.65`.

Canonical IDs:
- Deterministic SHA-256 IDs using the smallest record ID seen in a matched pair neighborhood.
- Singleton records receive stable IDs from their own `source_record_id`.
- IDs are reproducible for the generated batch output.

Ground truth:
- `cluster_id` is used only for evaluation, not for blocking or matching decisions.
- Precision is reported over predicted pairs with non-null `cluster_id`.
- Recall compares true positive predicted pairs to all same-`cluster_id` pairs, so blocking misses are counted as false negatives.

## Historical Analytics

Price summaries group by `canonical_product_id` and `currency`. Currencies are never mixed.

`product_price_summary` includes:
- `min_price`
- `max_price`
- `avg_price`
- `price_range`
- `observation_count`
- `source_count`
- `offer_count`

`product_source_summary` includes source-level price statistics grouped by product, source, and currency.

Availability analytics are not generated because the WDC cleaned contract has no trustworthy availability field.

The job does not calculate latest price because the WDC cleaned data does not provide a trustworthy observation timestamp.

## Validation

The batch job validates:
- `canonical_product_id` is non-null.
- `match_confidence` is between 0 and 1.
- Parsed prices are non-negative.
- Aggregate `observation_count` values are positive.
- `min_price <= avg_price <= max_price`.
- Every aggregate `canonical_product_id` exists in `product_matches`.
- Aggregations remain grouped by currency.

## ClickHouse

Batch DDL:
- `infra/clickhouse/002_batch_product_intelligence.sql`

Tables:
- `commerceradar.batch_product_matches`
- `commerceradar.batch_product_price_summary`
- `commerceradar.batch_product_source_summary`
- `commerceradar.batch_matching_evaluation`

The existing streaming table `commerceradar.product_observations` is preserved.

Create the batch tables:

```bash
docker compose exec -T clickhouse sh -c 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' < infra/clickhouse/002_batch_product_intelligence.sql
```

Sample verification queries:

```sql
SELECT count() FROM commerceradar.batch_product_matches;
SELECT * FROM commerceradar.batch_matching_evaluation ORDER BY loaded_at DESC LIMIT 1;
SELECT canonical_product_id, currency, min_price, avg_price, max_price
FROM commerceradar.batch_product_price_summary
WHERE min_price <= avg_price AND avg_price <= max_price
LIMIT 10;
```

## Run Commands

Run against the HDFS cleaned dataset and write HDFS curated outputs:

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

Run against a local cleaned Parquet sample by replacing `BATCH_INPUT_PATH` and `BATCH_OUTPUT_PATH` with local bind-mounted paths.

Run unit tests:

```bash
python -m unittest discover -s tests
```

## Limitations

- WDC does not expose a reliable source domain, product URL, availability field, or observation timestamp in the cleaned contract.
- Latest-price and availability summaries are intentionally omitted.
- The connected-component step is conservative and deterministic; very large transitive match chains may remain split.
- Full-dataset metrics depend on running the Spark job against the local/HDFS WDC data, which is not stored in Git.
