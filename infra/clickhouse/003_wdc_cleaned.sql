CREATE TABLE IF NOT EXISTS commerceradar.wdc_cleaned_offers
(
    source_record_id String,
    cluster_id Nullable(String),
    title Nullable(String),
    brand Nullable(String),
    category Nullable(String),
    price Nullable(Decimal(18, 2)),
    currency Nullable(String),
    price_parse_status LowCardinality(String),
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY source_record_id;
