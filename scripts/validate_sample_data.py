"""Validate sample events against the CommerceRadar data contract."""

import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import SchemaError


ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = ROOT / "contracts/v1/product_observation.schema.json"
DATA_PATH = ROOT / "data/samples/product_observations.v1.jsonl"


def main() -> int:
    try:
        schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
        lines = DATA_PATH.read_text(encoding="utf-8").splitlines()
    except (OSError, ValueError, SchemaError) as error:
        print(f"FAIL: Could not load valid schema or data: {error}")
        return 1

    validator = Draft202012Validator(
        schema,
        format_checker=FormatChecker(),
    )

    seen_ids = set()
    failures = 0
    valid_events = 0

    if not lines:
        print("FAIL: Sample data file is empty.")
        return 1

    for line_number, line in enumerate(lines, start=1):
        try:
            event = json.loads(line)
        except json.JSONDecodeError as error:
            print(f"FAIL line {line_number}: Invalid JSON: {error}")
            failures += 1
            continue

        errors = list(validator.iter_errors(event))

        if errors:
            for error in errors:
                field = ".".join(str(part) for part in error.absolute_path)
                print(
                    f"FAIL line {line_number}, "
                    f"field {field or '<root>'}: {error.message}"
                )
            failures += 1
            continue

        event_id = event["event_id"]

        if event_id in seen_ids:
            print(f"FAIL line {line_number}: Duplicate event_id {event_id}")
            failures += 1
            continue

        seen_ids.add(event_id)
        valid_events += 1
        print(f"PASS line {line_number}: {event_id}")

    if failures:
        print(f"FAIL: {failures} invalid lines; {valid_events} valid events.")
        return 1

    print(f"OK: All {valid_events} events match schema v1.0; event IDs are unique.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
