CREATE TABLE IF NOT EXISTS commerceradar.wdc_identifier_matches
(
    source_record_id String,
    cluster_id Nullable(String),
    title Nullable(String),
    brand Nullable(String),
    category Nullable(String),
    price Nullable(Decimal(18, 2)),
    currency Nullable(String),
    price_parse_status LowCardinality(String),
    canonical_product_id String,
    identifier_key Nullable(String),
    match_method LowCardinality(String),
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (canonical_product_id, source_record_id);
