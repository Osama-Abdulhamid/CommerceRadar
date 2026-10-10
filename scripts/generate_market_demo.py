import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from jsonschema import Draft202012Validator, FormatChecker

schema = json.loads(
    Path("contracts/v1/product_observation.schema.json").read_text()
)
validator = Draft202012Validator(schema, format_checker=FormatChecker())

products = [
    ("DEMO-PHONE-128", "Demo Phone 128GB Black", "Electronics", 15000),
    ("DEMO-LAPTOP-16", "Demo Laptop 16GB 512GB Silver", "Computers", 30000),
    ("DEMO-HEADPHONES", "Demo Headphones Wireless Black", "Electronics", 2000),
]
start = datetime.now(timezone.utc) - timedelta(seconds=30)
events = []

for cycle in range(3):
    for code, title, category, initial in products:
        for shop, adjustment in (
            ("demo_market_a", 0),
            ("demo_market_b", -100),
        ):
            price = initial + adjustment - cycle * 50
            availability = (
                "out_of_stock"
                if cycle == 1 and shop == "demo_market_a"
                else "in_stock"
            )
            event = {
                "schema_version": "1.0",
                "event_id": str(uuid4()),
                "observed_at": (
                    start + timedelta(seconds=len(events))
                ).isoformat(timespec="seconds").replace("+00:00", "Z"),
                "source": shop,
                "source_product_id": code,
                "product_url": f"https://{shop.replace('_', '-')}.example/products/{code}",
                "title": title,
                "price": f"{price:.2f}",
                "currency": "EGP",
                "availability": availability,
                "brand": "DemoBrand",
                "category": category,
            }
            validator.validate(event)
            events.append(event)

path = Path("data/samples/market_demo.generated.jsonl")
path.write_text(
    "".join(json.dumps(event) + "\n" for event in events),
    encoding="utf-8",
)
print(f"OK: {len(events)} validated simulated events; 3 products; 2 stores.")
