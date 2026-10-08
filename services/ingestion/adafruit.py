"""Convert a saved Adafruit response to a validated observation."""

import json
import os
import sys
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[2]


def normalize(product, currency, observed_at):
    raw_price = product.get("product_price")
    try:
        price = Decimal(str(raw_price))
    except InvalidOperation:
        raise ValueError(f"Invalid product price: {raw_price!r}")

    if not price.is_finite() or price < 0:
        raise ValueError("Price must be finite and non-negative")

    rounded = price.quantize(Decimal("0.01"))
    if price != rounded:
        raise ValueError("Price has more than two decimal places")

    stock = str(product.get("product_stock", "")).strip().lower()
    availability = {
        "in stock": "in_stock",
        "out of stock": "out_of_stock",
    }.get(stock, "unknown")

    event = {
        "schema_version": "1.0",
        "event_id": str(uuid.uuid4()),
        "observed_at": observed_at,
        "source": "adafruit",
        "source_product_id": str(product["product_id"]),
        "product_url": product["product_url"],
        "title": product["product_name"],
        "price": format(rounded, ".2f"),
        "currency": currency,
        "availability": availability,
    }

    brand = product.get("product_manufacturer")
    if isinstance(brand, str) and brand.strip():
        event["brand"] = brand.strip()

    return event


def main():
    currency = os.getenv("ADAFRUIT_CURRENCY", "").strip().upper()
    if not currency:
        raise ValueError("Set ADAFRUIT_CURRENCY after verifying the currency")

    path = ROOT / "data/samples/adafruit_5813.raw.json"
    product = json.loads(path.read_text(encoding="utf-8"))

    # Approximate collection time: when the downloaded file was saved.
    observed_at = (
        datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )

    event = normalize(product, currency, observed_at)

    schema_path = ROOT / "contracts/v1/product_observation.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(
        schema, format_checker=FormatChecker()
    )
    validator.validate(event)

    print(json.dumps(event, ensure_ascii=False, indent=2))
    print("OK: Observation matches contract v1.0.", file=sys.stderr)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
