import csv
import json
import os
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
root = Path("/mnt/e/CommerceRadarData/powerbi")
output = root / f"CommerceRadar_{stamp}"
output.mkdir(parents=True, exist_ok=False)
base = os.getenv("API_BASE_URL", "http://localhost:18000").rstrip("/")

def save_csv(name, rows):
    if isinstance(rows, dict):
        rows = [rows]
    (output / f"{name}.json").write_text(
        json.dumps(rows, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    if not rows:
        print(f"EMPTY: {name}; JSON saved, no CSV created")
        return
    columns = list(dict.fromkeys(key for row in rows for key in row))
    with (output / f"{name}.csv").open(
        "w", newline="", encoding="utf-8-sig"
    ) as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                key: json.dumps(value, ensure_ascii=False)
                if isinstance(value, (dict, list)) else value
                for key, value in row.items()
            })
    print(f"OK: {name}: {len(rows)} rows")

routes = {
    "wdc_overview": "/wdc/overview",
    "wdc_quality": "/wdc/quality",
    "wdc_categories": "/wdc/categories",
    "wdc_category_prices": "/wdc/category-prices",
    "streaming_current": "/streaming/current?limit=500",
    "streaming_changes": "/streaming/changes?limit=500",
    "matching_coverage": "/matching/overview",
    "matching_categories": "/matching/categories",
}

for name, route in routes.items():
    with urllib.request.urlopen(base + route, timeout=90) as response:
        save_csv(name, json.load(response))

evaluation = Path(
    "/mnt/e/CommerceRadarData/curated/"
    "wdc_identifier_full_v1/evaluation"
)
parts = [p for p in evaluation.glob("part-*") if p.is_file()]
assert len(parts) == 1, "Expected one full matching evaluation file"
metrics = json.loads(parts[0].read_text(encoding="utf-8"))
assert metrics["input_records"] == 16451499
save_csv("matching_evaluation", metrics)

(output / "READ_ME.txt").write_text(
    f"""CommerceRadar Power BI snapshot
Exported at UTC: {stamp}

Import CSV files using Get Data > Text/CSV.
Use UTF-8 and set numeric/date types explicitly.
Keep record IDs as Text.
Precision and recall are fractions; format them as Percentage.
Separate currencies when comparing prices.

WDC contains historical dataset offers, not a time series.
Identifier-eligible offers are not the count of matched offers.
Matching is experimental; high precision accompanies very low recall.
Evaluation is not an independent holdout assessment.

Adafruit observations are real collected data.
demo_shop sources are simulated demonstration data.
Streaming files contain up to 500 recent rows each.
The files are a snapshot; they do not refresh automatically.
No dedicated price-history export is included yet.
""",
    encoding="utf-8",
)

archive = root / f"{output.name}.zip"
with ZipFile(archive, "x", ZIP_DEFLATED) as bundle:
    for path in sorted(output.iterdir()):
        bundle.write(path, arcname=f"{output.name}/{path.name}")

print("READY:", archive)
