# Batch Integration Verification

## Verified behavior

- Connected matching chains share one canonical product ID.
- Regression check: records 1-2-3 form one group; record 4 remains separate.
- The 1000-record cleaned WDC sample was processed with threshold 0.90.
- Sample outputs were loaded into ClickHouse using staging tables.
- Repeating the load preserved these active-table counts:
  - batch_product_matches: 1000 rows, 1000 distinct source_record_id values.
  - batch_product_price_summary: 106 rows.
  - batch_product_source_summary: 106 rows.
  - batch_matching_evaluation: 1 row.
- API health, product listing, product details, price summaries,
  latest evaluation and streaming observations passed live checks.

## Experimental matching quality

On this development sample at threshold 0.90:
- Candidate pairs: 4163.
- Predicted matching pairs: 87.
- True positive pairs: 68.
- False positive pairs: 19.
- False negative pairs: 0.
- Precision: 0.781609.
- Recall: 1.0.
- Canonical groups: 953.

These are development-sample results, not independent validation.
The sample was used to choose the threshold.
Title similarity produces false matches for generic stock-image titles.
A broad exclusion was tested and removed because it lost 67 true matches.
Matching remains experimental and requires independent evaluation.
The CLI default threshold remains 0.65; use --threshold 0.90 to reproduce
the reported experiment.

## Loading behavior

The loader replaces the active batch snapshot rather than appending.
Only the Spark driver sends inserts, in bounded request batches.
All four staging tables pass row-count checks before publishing begins.
Each table exchange is atomic separately; the four exchanges are not
one transaction. Avoid concurrent loader runs.
A failure during publishing can leave tables from different snapshots.
Inspect the run log and retained tables before retrying such a failure.
A failure before publishing leaves active tables unchanged.

The --truncate option is disabled.
Previous table contents remain under the run-specific staging names
after a successful exchange. Failed staging tables also remain.
Retained tables need deliberate cleanup after verification.

## Scope

Only the 1000-record intelligence sample was processed and loaded here.
The full cleaned WDC dataset already exists locally and in HDFS,
but full-dataset matching and loading have not been validated.
Candidate-block sizes must be measured and controlled before that run.
The WDC source label identifies the dataset, not individual stores.
Its price summaries do not establish price changes over time.

## Reproduction locations

Sample input:
data/samples/wdc_english_v2.cleaned.1000.jsonl

Verified local output:
/mnt/e/CommerceRadarData/curated/wdc_intelligence_sample_t090_v1

Connected-group check:
tests/check_matching_groups.py

API:
http://localhost:18000/docs
