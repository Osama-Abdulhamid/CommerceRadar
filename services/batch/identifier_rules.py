import json
import re

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

def matching_key(payload):
    row = json.loads(payload)
    brand = normalize(row.get("brand"))
    if brand in {"", "unknown", "n/a", "none", "null"}:
        return None

    title = str(row.get("title") or "").lower()
    quantities = set(
        re.findall(r"\b([0-9]{1,3})\s*-?\s*pack\b", title)
        + re.findall(r"\bpack\s+of\s+([0-9]{1,3})\b", title)
    )
    if len(quantities) > 1:
        return None
    pack = next(iter(quantities), "unspecified")
    category = normalize(row.get("category"))
    keys = set()

    for item in row.get("identifiers") or []:
        if not isinstance(item, dict):
            continue
        for kind, raw in item.items():
            for value in values(raw):
                if kind in GTIN_LENGTHS:
                    if valid_gtin(value, GTIN_LENGTHS[kind]):
                        keys.add((
                            "gtin_brand_category",
                            value.zfill(14), brand, category, pack,
                        ))
                elif kind == "/mpn":
                    mpn = normalize(value)
                    if mpn not in {"", "unknown", "n/a", "none", "null", "0"}:
                        keys.add(("brand_mpn", brand, mpn, pack))

    if not keys:
        return None
    # Prefer GTIN; deterministic choice when multiple keys exist.
    chosen = min(keys, key=lambda key: (key[0] != "gtin_brand_category", key))
    return json.dumps(chosen, ensure_ascii=False, separators=(",", ":"))
