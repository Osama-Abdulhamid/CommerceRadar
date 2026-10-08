# Member 4 Visualization Strategy

## Current Serving State

The batch intelligence path is sample-integrated:

```text
Committed cleaned WDC sample
-> HDFS
-> Spark batch matching and analytics
-> HDFS curated Parquet
-> ClickHouse batch tables
-> FastAPI JSON endpoints
```

FastAPI is available at:

```text
http://localhost:18000/docs
```

Useful endpoints:

- `GET /batch/evaluation/latest`
- `GET /batch/products`
- `GET /batch/products?q=<search>`
- `GET /batch/products/{canonical_product_id}`
- `GET /batch/products/{canonical_product_id}/price-summary`
- `GET /batch/price-summary`
- `GET /streaming/observations`

## Recommended Power BI Pages

1. Batch quality overview
   - Input records
   - Candidate pairs
   - Matched pairs
   - Canonical product groups
   - Precision, recall and F1

2. Product catalog intelligence
   - Searchable product table
   - Offer count per canonical product
   - Brand and category filters
   - Drill-through to product offers

3. Price intelligence
   - Minimum, average and maximum price by product
   - Currency filter
   - Price range ranking
   - Products with the widest price spread

4. Source comparison
   - Source-level price summaries
   - Observation count by source
   - Lowest and highest observed prices per source

5. Streaming observations
   - Latest real-time observations from `product_observations`
   - Availability distribution
   - Recent price observations

## Integration Options

Power BI can connect directly to ClickHouse if a connector is available in
the team environment. Otherwise, use FastAPI endpoints as JSON sources.

For the sample demo, use the loaded 1,000-record WDC sample. For the final
demo, the teammate who has the full WDC cleaned Parquet can run the same
Spark commands against the full dataset and reload the same ClickHouse
tables without changing Power BI visuals.

## Known Data Limitations

- WDC cleaned records do not include trustworthy timestamps, so batch
  latest-price charts should not be presented as time series.
- WDC cleaned records do not include availability, so availability should
  be shown only for streaming observations.
- The current sample is small and intended for pipeline verification, not
  final market conclusions.
