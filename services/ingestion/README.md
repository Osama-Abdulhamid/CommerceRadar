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
- Capture collection timestamps directly.
- Add bounded retries and preserve event identity during delivery retries.
- Package the collector for Docker Compose.
- Connect valid streaming observations to ClickHouse.
