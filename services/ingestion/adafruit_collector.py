"""Fetch one Adafruit product, validate it, and publish to Kafka."""

import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone

from jsonschema import Draft202012Validator, FormatChecker
from kafka import KafkaProducer

from adafruit import ROOT, normalize

LOGGER = logging.getLogger("adafruit_collector")


def main():
    producer = None
    try:
        product_id = os.getenv("ADAFRUIT_PRODUCT_ID", "5813").strip()
        currency = os.getenv("ADAFRUIT_CURRENCY", "").strip().upper()
        brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
        topic = os.getenv(
            "KAFKA_TOPIC", "product-observations.v1"
        ).strip()

        if not product_id.isdigit():
            raise ValueError("ADAFRUIT_PRODUCT_ID must contain digits")
        if not currency:
            raise ValueError("ADAFRUIT_CURRENCY is required")
        if not brokers or not topic:
            raise ValueError("Kafka bootstrap servers and topic are required")

        schema_path = (
            ROOT / "contracts/v1/product_observation.schema.json"
        )
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(
            schema, format_checker=FormatChecker()
        )

        # Use curl, which successfully accessed the API in our tests.
        # No automatic HTTP retries: keep source requests bounded.
        url = f"https://www.adafruit.com/api/product/{product_id}"
        result = subprocess.run(
            [
                "curl", "--fail", "--silent", "--show-error",
                "--location", "--max-time", "30", url,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=35,
            check=True,
        )

        observed_at = (
            datetime.now(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )
        product = json.loads(result.stdout)

        if not isinstance(product, dict):
            raise ValueError("Expected a product JSON object")
        if str(product.get("product_id")) != product_id:
            raise ValueError("Returned product ID does not match request")

        event = normalize(product, currency, observed_at)
        validator.validate(event)

        LOGGER.info(
            "Validated product=%s price=%s currency=%s availability=%s",
            product_id,
            event["price"],
            event["currency"],
            event["availability"],
        )

        producer = KafkaProducer(
            bootstrap_servers=[
                item.strip() for item in brokers.split(",")
                if item.strip()
            ],
            client_id="commerceradar-adafruit-collector",
            acks="all",
            retries=3,
            max_in_flight_requests_per_connection=1,
            max_block_ms=15000,
            request_timeout_ms=10000,
        )

        key = json.dumps(
            [event["source"], event["source_product_id"]],
            separators=(",", ":"),
        ).encode("utf-8")
        payload = json.dumps(
            event, ensure_ascii=False, allow_nan=False
        ).encode("utf-8")

        metadata = producer.send(
            topic, key=key, value=payload
        ).get(timeout=30)

        LOGGER.info(
            "Delivered event=%s topic=%s partition=%s offset=%s",
            event["event_id"],
            metadata.topic,
            metadata.partition,
            metadata.offset,
        )
        return 0

    except KeyboardInterrupt:
        LOGGER.warning("Collection interrupted")
        return 130
    except subprocess.CalledProcessError as error:
        LOGGER.error("API request failed: %s", error.stderr.strip())
        return 1
    except Exception as error:
        LOGGER.error("Collection failed: %s", error)
        return 1
    finally:
        if producer is not None:
            try:
                producer.close(timeout=10)
            except Exception as error:
                LOGGER.warning("Producer cleanup failed: %s", error)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("kafka").setLevel(logging.WARNING)
    sys.exit(main())
