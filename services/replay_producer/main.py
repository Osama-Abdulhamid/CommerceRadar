"""Validate and replay product observations to Kafka or stdout."""

import json
import logging
import math
import os
import sys
import time
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import SchemaError
from kafka import KafkaProducer
from kafka.errors import KafkaError

ROOT = Path(__file__).resolve().parents[2]
LOGGER = logging.getLogger("replay_producer")


def main() -> int:
    producer = None
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        print("Invalid LOG_LEVEL", file=sys.stderr)
        return 1

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    logging.getLogger("kafka").setLevel(logging.WARNING)

    try:
        interval = float(os.getenv("REPLAY_INTERVAL_SECONDS", "1"))
        if not math.isfinite(interval) or interval < 0:
            raise ValueError("Replay interval must be finite and non-negative")

        output = os.getenv("REPLAY_OUTPUT", "stdout")
        if output not in {"stdout", "kafka"}:
            raise ValueError("REPLAY_OUTPUT must be stdout or kafka")

        input_path = Path(os.getenv(
            "REPLAY_INPUT_PATH",
            "data/samples/product_observations.v1.jsonl",
        ))
        if not input_path.is_absolute():
            input_path = ROOT / input_path

        schema_path = ROOT / "contracts/v1/product_observation.schema.json"
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)

        if os.getenv("SCHEMA_VERSION", "1.0") != schema["properties"]["schema_version"]["const"]:
            raise ValueError("Configured schema version does not match the schema")

        validator = Draft202012Validator(schema, format_checker=FormatChecker())
        events = []
        seen_ids = set()

        with input_path.open(encoding="utf-8") as input_file:
            for line_number, line in enumerate(input_file, start=1):
                event = json.loads(line)
                errors = list(validator.iter_errors(event))
                if errors:
                    raise ValueError(f"Line {line_number}: {errors[0].message}")
                if event["event_id"] in seen_ids:
                    raise ValueError(f"Line {line_number}: duplicate event_id")
                seen_ids.add(event["event_id"])
                events.append(event)

        if not events:
            raise ValueError("Input file contains no events")

        LOGGER.info("Validated %d events; output=%s", len(events), output)

        topic = os.getenv("KAFKA_TOPIC", "product-observations.v1")
        if output == "kafka":
            brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
            if not brokers or not topic.strip():
                raise ValueError("Kafka bootstrap servers and topic are required")

            producer = KafkaProducer(
                bootstrap_servers=[item.strip() for item in brokers.split(",")],
                client_id="commerceradar-replay-producer",
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1,
                max_block_ms=15000,
                request_timeout_ms=10000,
            )

        for index, event in enumerate(events):
            if index:
                time.sleep(interval)

            if producer is None:
                print(json.dumps(event, ensure_ascii=False), flush=True)
                LOGGER.info("Emitted event %s", event["event_id"])
            else:
                key = json.dumps(
                    [event["source"], event["source_product_id"]],
                    separators=(",", ":"),
                )
                payload = json.dumps(event, ensure_ascii=False).encode("utf-8")
                metadata = producer.send(
                    topic, key=key.encode("utf-8"), value=payload
                ).get(timeout=30)
                LOGGER.info(
                    "Delivered event=%s topic=%s partition=%s offset=%s",
                    event["event_id"],
                    metadata.topic,
                    metadata.partition,
                    metadata.offset,
                )

        LOGGER.info("Replay completed: %d events; output=%s", len(events), output)
        return 0

    except (OSError, ValueError, SchemaError, KafkaError) as error:
        LOGGER.error("Replay failed: %s", error)
        return 1
    except KeyboardInterrupt:
        LOGGER.warning("Replay interrupted by user")
        return 130
    finally:
        if producer is not None:
            producer.close(timeout=10)


if __name__ == "__main__":
    sys.exit(main())
