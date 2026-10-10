# CommerceRadar checkpoint — 2026-10-10

## Verified
- Streaming ingestion, Kafka, Spark and ClickHouse integration.
- Full cleaned WDC table: 16,451,499 offers.
- Parsed-price offers: 1,522,526.
- API analytics, accounts, settings and user-scoped alerts.
- Scheduled Airflow operations and quality workflows.
- Prometheus targets and provisioned Grafana operations dashboard.
- Airflow HDFS sample batch pipeline:
  1000 matching rows, 1000 unique source IDs,
  106 price summaries, 106 source summaries.
- Sample matching at threshold 0.90:
  87 predicted pairs, 68 true positives, 19 false positives.

## Full-data matching investigation
The original category/token blocking estimate was:
2,566,265,002,190 pairs before brand filtering and deduplication.
Do not run the full fuzzy join with safeguards disabled.

The initial exact-group profile failed with Cannot allocate memory.
The retry completed using one Spark task, disk-only persistence,
and disabled vectorized Parquet reading.

Exact normalized title/brand/category profile:
- Groups: 11,269,342.
- Largest group: 39,612 offers.
- Multi-offer groups: 1,038,427.
- Offers in multi-offer groups: 5,933,754.
- Groups with multiple reference cluster IDs: 292,778.
- Missing normalized titles: 286,830.

These are diagnostic groups, not verified product matches.
No predictions were published by the profile.

Sample JSON contains identifiers and raw_record.
The first inspected records contain /productID and /sku.
These must not be assumed to be global product identifiers.
Identifier coverage and full Parquet schema inspection remain pending.
Reference cluster IDs must not be used as prediction inputs.

## Local locations
Full cleaned Parquet:
 /mnt/e/CommerceRadarData/processed/wdc_full_cleaned_v1

Successful exact-group profile log:
 /mnt/e/CommerceRadarData/logs/wdc_exact_group_profile_v2.log

Quality reports:
 /mnt/e/CommerceRadarData/quality-reports

Application databases and other named volumes remain local.
Git does not contain runtime databases or full datasets.

## Next steps
1. Inspect identifier coverage and full Parquet schema.
2. Select and evaluate a scalable full-data matching approach.
3. Build the Power BI business dashboard.
4. Verify restart behavior and complete final documentation.
5. Review deliverables against the actual proposal.

## Shutdown
Use docker compose stop with ingestion, quality and batch profiles.
Do not remove volumes or runtime data.
Collector and streaming continuous settings must be explicitly
restored when recreating their containers.
