# Product Ingestion

## Adafruit Adapter — Local Sample Prototype

`adafruit.py` converts a saved Adafruit product response into a
CommerceRadar product observation and validates it against contract v1.0.

It does not fetch products, poll the store, or publish to Kafka directly.

## Requirements

Run from the project root with the virtual environment activated.
Install the existing Replay Producer dependencies:

```bash
python -m pip install -r services/replay_producer/requirements.txt
```

## Download a Sample

```bash
curl --fail --location --max-time 30 --output data/samples/adafruit_5813.raw.json 'https://www.adafruit.com/api/product/5813'
```

This overwrites the local sample if it already exists.
Downloaded responses are ignored by Git.

## Convert and Validate

USD was verified in the official product page's priceCurrency metadata
during the integration test on 2026-10-08.
The Products API response itself does not include a currency field.

```bash
ADAFRUIT_CURRENCY=USD python services/ingestion/adafruit.py
```

Expected: a product observation followed by:

```text
OK: Observation matches contract v1.0.
```

Mappings:
- product_id → source_product_id
- product_name → title
- product_url → product_url
- product_price → price
- product_manufacturer → optional brand
- "in stock" → in_stock
- "out of stock" → out_of_stock
- Unrecognized stock values → unknown

Prices use Decimal and must be finite, non-negative, and representable
with two decimal places.

observed_at approximates collection time using the saved file's
modification timestamp. It is not the time the store changed its price.
Copying or touching the sample can change this timestamp.

Each adapter invocation generates a new event_id.
Do not repeatedly convert the same sample as if it were a new observation.

## Save as JSONL

```bash
set -o pipefail
ADAFRUIT_CURRENCY=USD python services/ingestion/adafruit.py | python -c 'import json,sys; print(json.dumps(json.load(sys.stdin), ensure_ascii=False))' > data/raw/adafruit_observation.v1.jsonl
```

Check that the command succeeds before publishing.
The generated file is ignored by Git.

## Publish the Saved Observation

Kafka must be running and product-observations.v1 must exist.

```bash
REPLAY_OUTPUT=kafka KAFKA_BOOTSTRAP_SERVERS=localhost:19092 REPLAY_INPUT_PATH=data/raw/adafruit_observation.v1.jsonl python services/replay_producer/main.py
```

Replaying this file preserves its event_id and observed_at.
Repeated publication can still create duplicate Kafka records.

## Verified Integration

On 2026-10-08:
- Read product 5813: Raspberry Pi 5 - 8 GB RAM.
- API returned price 200.00 and stock "in stock".
- Converted and validated the saved response.
- Kafka acknowledged delivery.
- Spark processed one valid observation with zero rejected records.
- Spark displayed price 200.00, currency USD, and availability in_stock.

This verifies the saved-sample path through Kafka and Spark.
Valid streaming observations are currently displayed in Spark logs;
a ClickHouse sink is not implemented yet.

## Source Rules and Remaining Work

Official API documentation:
https://www.adafruit.com/products_api

The documented limit is five requests per minute or fewer.
Do not hotlink images.

The published content authorization refers to use for an online store.
Permission for this project's periodic monitoring, historical retention,
and redistribution has not been established.

Remaining:
- Clarify permitted project use before scheduled collection.
- Implement configurable product selection and polling.
- Review delivery failure recovery and duplicate handling.
- Connect valid streaming observations to ClickHouse.

## Direct Adafruit Collector

`adafruit_collector.py` fetches one product directly from the API,
normalizes it using the adapter, validates the observation, and publishes
it to Kafka with acknowledged delivery.

Each invocation makes one HTTP request and then exits.
Scheduled polling is not implemented.

observed_at is recorded in UTC immediately after receiving the response.
It represents collection time, not the time the store changed its data.

Configuration:
- ADAFRUIT_PRODUCT_ID: product ID; default 5813.
- ADAFRUIT_CURRENCY: required locally; Compose defaults to verified USD.
- KAFKA_BOOTSTRAP_SERVERS: required locally; Compose uses kafka:9092.
- KAFKA_TOPIC: defaults to product-observations.v1.

The collector requires curl and the packages in
services/ingestion/requirements.txt.

### Local Execution

```bash
python -m pip install -r services/ingestion/requirements.txt
ADAFRUIT_PRODUCT_ID=5813 ADAFRUIT_CURRENCY=USD KAFKA_BOOTSTRAP_SERVERS=localhost:19092 python services/ingestion/adafruit_collector.py
```

### Docker Execution

Kafka must be available and the configured topic must exist.

```bash
docker compose --profile ingestion config -q
docker compose --profile ingestion build adafruit-collector
docker compose --profile ingestion run --rm adafruit-collector
```

The container runs as UID/GID 10001.
Compose waits for Kafka to become healthy.
The ingestion profile keeps the collector optional.
The temporary container is removed after it exits.

To read another product:

```bash
docker compose --profile ingestion run --rm -e ADAFRUIT_PRODUCT_ID=5812 adafruit-collector
```

Respect the documented aggregate limit of five source requests per minute
or fewer, including manual tests and parallel collectors.

### Failure and Delivery Behavior

- HTTP requests have a 30-second timeout and no automatic HTTP retries.
- The returned product ID must match the requested ID.
- Invalid observations are not published.
- Kafka uses acks=all and up to three internal retries.
- A single event_id is retained throughout one delivery attempt and its
  internal Kafka retries.
- Restarting the program creates a new observation and event_id.
- Kafka delivery can still produce duplicates.
- Failed collection or delivery returns a nonzero exit code.

### Verified Docker Integration

On 2026-10-08, the Docker collector fetched product 5813 and delivered an
observation to Kafka partition 2, offset 6.

Spark processed the same event in batch 9:
- Valid records: 1.
- Rejected records: 0.
- Price: 200.00 USD.
- Availability: in_stock.
- Collection timestamp: 2026-10-08T18:01:39Z.

This verifies direct API → Docker collector → Kafka → Spark.
ClickHouse persistence, periodic polling, and source-use clarification
remain outstanding.
