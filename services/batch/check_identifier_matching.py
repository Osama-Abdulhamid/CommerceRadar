import sys
import json
import re
from collections import defaultdict
from pathlib import Path

GTIN_LENGTHS = {
    "/gtin8": 8, "/gtin12": 12, "/gtin13": 13, "/gtin14": 14,
}

def values(value):
    if not isinstance(value, str):
        return []
    text = value.strip()
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1]
    return [
        part.strip().strip("\"'")
        for part in text.split(",")
        if part.strip()
    ]

def valid_gtin(value, length):
    if not re.fullmatch(r"[0-9]+", value) or len(value) != length:
        return False
    if not value.strip("0"):
        return False
    total = sum(
        int(digit) * (3 if index % 2 == 0 else 1)
        for index, digit in enumerate(reversed(value[:-1]))
    )
    return (10 - total % 10) % 10 == int(value[-1])

def normalize(value):
    return re.sub(r"\s+", " ", str(value or "").strip().strip("\"'").lower())

groups = defaultdict(dict)
covered = set()
invalid_gtins = 0
total = 0

with Path(sys.argv[1] if len(sys.argv) > 1 else "data/samples/wdc_english_v2.cleaned.1000.jsonl").open(
    encoding="utf-8"
) as stream:
    for line in stream:
        if not line.strip():
            continue
        row = json.loads(line)
        total += 1
        record_id = str(row["source_record_id"])
        brand = normalize(row.get("brand"))
        keys = set()

        for item in row.get("identifiers") or []:
            if not isinstance(item, dict):
                continue
            for kind, raw in item.items():
                for value in values(raw):
                    if kind in GTIN_LENGTHS:
                        if valid_gtin(value, GTIN_LENGTHS[kind]):
                            if brand and brand not in {
                                "unknown", "n/a", "none", "null"
                            }:
                                keys.add((
                                    "gtin_brand_category",
                                    value.zfill(14),
                                    brand,
                                    normalize(row.get("category")),
                                ))
                        else:
                            invalid_gtins += 1
                    elif kind == "/mpn" and brand:
                        mpn = normalize(value)
                        if mpn not in {"", "unknown", "n/a", "none", "null", "0"}:
                            keys.add(("brand_mpn", brand, mpn))

        # Keep explicitly stated pack quantities separate.
        # Missing pack information is unknown, not necessarily one unit.
        title = str(row.get("title") or "").lower()
        quantities = set(
            re.findall(r"\b([0-9]{1,3})\s*-?\s*pack\b", title)
            + re.findall(r"\bpack\s+of\s+([0-9]{1,3})\b", title)
        )
        if len(quantities) > 1:
            keys = set()
        else:
            pack = next(iter(quantities), "unspecified")
            keys = {key + ("pack", pack) for key in keys}

        for key in keys:
            groups[key][record_id] = row.get("cluster_id")
        if keys:
            covered.add(record_id)

repeated = {key: rows for key, rows in groups.items() if len(rows) > 1}
conflicts = []
for key, rows in repeated.items():
    labels = {
        str(label) for label in rows.values()
        if label is not None and str(label).strip()
    }
    if len(labels) > 1:
        conflicts.append((key, rows))

print("Input records:", total)
print("Records with eligible identifier keys:", len(covered))
print("Invalid GTIN values rejected:", invalid_gtins)
print("Repeated identifier keys:", len(repeated))
print("Repeated keys conflicting with reference clusters:", len(conflicts))
for key, rows in list(repeated.items())[:10]:
    print("GROUP:", key, "records/reference:", rows)
print("OK: Sample identifier check completed; no tables changed.")
