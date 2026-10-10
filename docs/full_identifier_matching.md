# Full WDC identifier matching

Experimental matching: one deterministic identifier key per offer.
GTIN requires validation, brand and category; MPN requires brand.
Explicit pack quantities separate keys; missing quantity is unspecified.
Offers without eligible keys remain unresolved singletons.
Reference cluster IDs are used only for evaluation.

## Verified results
{
  "input_records": 16451499,
  "unique_ids": 16451499,
  "eligible_records": 2203388,
  "canonical_groups": 15808804,
  "invalid_ids": 0,
  "predicted_pairs": 16522258,
  "evaluated_predicted_pairs": 16522258,
  "pairs_without_complete_labels": 0,
  "true_positive_pairs": 16359847,
  "false_positive_pairs": 162411,
  "false_negative_pairs": 20832341011,
  "reference_pairs": 20848700858,
  "precision": 0.9901701692347378,
  "recall": 0.000784693833511571,
  "method": "one deterministic identifier key; pack-aware; experimental"
}

Precision approximately 99.02%; recall approximately 0.078%.
Coverage is limited; this is not comprehensive product matching.
Development samples influenced the rules; no independent holdout test.

## Storage verification
Local output read back: 16,451,499 offers.
HDFS fsck: HEALTHY; zero missing or corrupt blocks.
Spark HDFS read-back: 16,451,499 offers.
Existing ClickHouse tables unchanged; serving integration pending.

Local: /mnt/e/CommerceRadarData/curated/wdc_identifier_full_v1
HDFS: /commerceradar/curated/wdc_identifier_full_v1

No store identity or historical observation timestamps were inferred.
Runtime datasets are not committed to Git.
