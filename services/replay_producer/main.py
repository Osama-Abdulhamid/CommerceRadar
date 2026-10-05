"""Replay validated sample events to standard output."""

import json
import logging
import math
import os
import sys
import time
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import SchemaError


ROOT = Path(__file__).resolve().parents[2]
LOGGER = logging.getLogger("replay_producer")


def main() -> int:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        print("Invalid LOG_LEVEL", file=sys.stderr)
        return 1

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    try:
        interval = float(os.getenv("REPLAY_INTERVAL_SECONDS", "1"))
        if not math.isfinite(interval) or interval < 0:
            raise ValueError("Replay interval must be finite and non-negative")

        schema_version = os.getenv("SCHEMA_VERSION", "1.0")
        input_path = Path(
            os.getenv(
                "REPLAY_INPUT_PATH",
                "data/samples/product_observations.v1.jsonl",
            )
        )
        if not input_path.is_absolute():
            input_path = ROOT / input_path

        schema_path = ROOT / "contracts/v1/product_observation.schema.json"
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)

        if schema_version != schema["properties"]["schema_version"]["const"]:
            raise ValueError("Configured schema version does not match the schema")

        validator = Draft202012Validator(
            schema,
            format_checker=FormatChecker(),
        )

        events = []
        seen_ids = set()

        with input_path.open(encoding="utf-8") as input_file:
            for line_number, line in enumerate(input_file, start=1):
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as error:
                    raise ValueError(
                        f"Line {line_number}: invalid JSON"
                    ) from error

                errors = list(validator.iter_errors(event))
                if errors:
                    raise ValueError(
                        f"Line {line_number}: {errors[0].message}"
                    )

                if event["event_id"] in seen_ids:
                    raise ValueError(
                        f"Line {line_number}: duplicate event_id"
                    )

                seen_ids.add(event["event_id"])
                events.append(event)

        if not events:
            raise ValueError("Input file contains no events")

        LOGGER.info(
            "Validated %d events; replay interval is %s seconds",
            len(events),
            interval,
        )

        for index, event in enumerate(events):
            if index > 0:
                time.sleep(interval)

            print(json.dumps(event, ensure_ascii=False), flush=True)
            LOGGER.info("Emitted event %s", event["event_id"])

        LOGGER.info("Replay completed: %d events emitted", len(events))
        return 0

    except (OSError, ValueError, SchemaError) as error:
        LOGGER.error("Replay failed: %s", error)
        return 1
    except KeyboardInterrupt:
        LOGGER.warning("Replay interrupted by user")
        return 130


if __name__ == "__main__":
    sys.exit(main())
