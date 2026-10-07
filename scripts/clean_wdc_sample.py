"""Conservative cleaning of the WDC development sample."""

import html
import json
import re
from collections import Counter
from decimal import Decimal
from pathlib import Path

SOURCE = Path("data/samples/wdc_english_v2.raw.1000.jsonl")
TARGET = Path("data/samples/wdc_english_v2.cleaned.1000.jsonl")

CURRENCIES = re.compile(
    r"\b(USD|EUR|GBP|CAD|AUD|NZD|JPY|COP|DKK|EGP|SAR|AED)\b",
    re.IGNORECASE,
)


def clean_text(value):
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    value = html.unescape(value)
    value = re.sub(r'"@[\w-]+', '"', value)
    value = " ".join(value.split())
    return value if value else None


def parse_price(raw):
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return None, None, "missing"
    if not isinstance(raw, str):
        return None, None, "unsupported_type"

    text = re.sub(r'@[\w-]+', "", html.unescape(raw))
    currencies = {item.upper() for item in CURRENCIES.findall(text)}

    if re.search(r"\bUS\$", text, re.IGNORECASE):
        currencies.add("USD")
    if "€" in text:
        currencies.add("EUR")
    if "£" in text:
        currencies.add("GBP")
    if "ج.م" in text:
        currencies.add("EGP")

    if len(currencies) > 1:
        return None, None, "conflicting_currencies"
    currency = next(iter(currencies), None)

    # Remove commas separating quoted values, retain commas inside amounts.
    text = re.sub(r'"\s*,\s*"', '" "', text)
    tokens = re.findall(r"[-+]?\d+(?:[.,]\d+)*", text)

    if not tokens:
        return None, currency, "missing_amount"
    if len(tokens) != 1:
        return None, currency, "multiple_amounts"
    if currency is None:
        return None, None, "unknown_currency"

    token = tokens[0]
    if "," in token or token.count(".") > 1:
        return None, currency, "ambiguous_number_format"

    amount = Decimal(token)
    if amount < 0:
        return None, currency, "negative_amount"

    # Three decimal places can also represent a thousands separator.
    if "." in token and len(token.split(".")[1]) > 2:
        return None, currency, "precision_or_separator_ambiguity"

    return format(amount, ".2f"), currency, "parsed"


def main():
    if TARGET.exists():
        raise SystemExit("Cleaned sample already exists; no overwrite performed.")

    output = []
    statuses = Counter()

    with SOURCE.open(encoding="utf-8") as source:
        for line in source:
            raw = json.loads(line)
            price, currency, status = parse_price(raw.get("price"))
            title = clean_text(raw.get("title"))
            statuses[status] += 1

            output.append({
                "batch_schema_version": "1.0",
                "dataset": "wdc_english_v2_non_norm",
                "source_record_id": str(raw["id"]),
                "cluster_id": raw.get("cluster_id"),
                "title": title,
                "brand": clean_text(raw.get("brand")),
                "category": clean_text(raw.get("category")),
                "identifiers": raw.get("identifiers"),
                "price": price,
                "currency": currency,
                "price_parse_status": status,
                "quality_flags": ["missing_title"] if title is None else [],
                "raw_record": raw,
            })

    assert len(output) == 1000, f"Expected 1000 records, found {len(output)}"

    with TARGET.open("w", encoding="utf-8") as target:
        for record in output:
            target.write(
                json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n"
            )

    print("Output:", TARGET)
    print("Records:", len(output))
    print("Price parsing:", dict(statuses))
    print("First five parsed prices:")
    for record in [r for r in output if r["price_parse_status"] == "parsed"][:5]:
        print(record["source_record_id"], record["price"], record["currency"])


if __name__ == "__main__":
    main()
