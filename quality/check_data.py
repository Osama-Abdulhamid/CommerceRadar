"""Validate full-table quality aggregates with Great Expectations."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import great_expectations as gx
import pandas as pd

from clickhouse import ClickHouseClient

STATUSES = [
    "missing", "parsed", "unknown_currency", "ambiguous_number_format",
    "missing_amount", "multiple_amounts", "precision_or_separator_ambiguity",
    "conflicting_currencies", "negative_amount",
]


def main():
    client = ClickHouseClient()
    allowed = ", ".join("'" + value + "'" for value in STATUSES)
    metrics = client.query_json_each_row(
        f"""
        SELECT
            count() AS total_offers,
            countIf(empty(trimBoth(source_record_id))) AS empty_ids,
            countIf(price_parse_status NOT IN ({allowed})) AS invalid_statuses,
            countIf(price_parse_status = 'parsed' AND (
                price IS NULL OR currency IS NULL
                OR coalesce(price < 0, false)
                OR NOT match(coalesce(currency, ''), '^[A-Z]{{3}}$')
            )) AS invalid_parsed_prices,
            countIf(price IS NOT NULL AND price_parse_status != 'parsed')
                AS unexpected_prices,
            countIf(price_parse_status = 'parsed') AS priced_offers
        FROM commerceradar.wdc_cleaned_offers
        """
    )[0]
    metrics = {key: int(value) for key, value in metrics.items()}

    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas("wdc_quality")
    asset = source.add_dataframe_asset(name="full_table_metrics")
    definition = asset.add_batch_definition_whole_dataframe("metrics")
    batch = definition.get_batch(
        batch_parameters={"dataframe": pd.DataFrame([metrics])}
    )

    expectations = [
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="total_offers", min_value=1,
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="priced_offers", min_value=1,
            max_value=metrics["total_offers"],
        ),
    ]
    for column in (
        "empty_ids", "invalid_statuses",
        "invalid_parsed_prices", "unexpected_prices",
    ):
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column=column, value_set=[0],
            )
        )

    results = []
    for expectation in expectations:
        result = batch.validate(expectation)
        results.append(result.to_json_dict())
        print(
            f"{expectation.column}: "
            f"{'PASS' if result.success else 'FAIL'}",
            flush=True,
        )

    report = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "scope": "Full WDC table: SQL aggregate checks validated by GX",
        "success": all(item["success"] for item in results),
        "metrics": metrics,
        "priced_fraction": metrics["priced_offers"] / max(metrics["total_offers"], 1),
        "validation_results": results,
    }
    directory = Path(os.getenv("QUALITY_REPORT_DIR", "/reports"))
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    body = json.dumps(report, indent=2, ensure_ascii=False)
    (directory / f"wdc_quality_{stamp}.json").write_text(body)
    temporary = directory / "latest.tmp"
    temporary.write_text(body)
    temporary.replace(directory / "latest.json")

    print("Metrics:", metrics, flush=True)
    print(f"Report saved: {directory / 'latest.json'}", flush=True)
    if not report["success"]:
        raise SystemExit("FAILED: Data quality expectations did not pass.")
    print("OK: Full-table quality checks passed.", flush=True)


if __name__ == "__main__":
    main()
