CREATE TABLE IF NOT EXISTS commerceradar.product_observations
(
    schema_version LowCardinality(String),
    event_id UUID,
    observed_at DateTime64(3, 'UTC'),

    source LowCardinality(String),
    source_product_id String,
    product_url String,
    title String,

    price Decimal(18, 2),
    currency LowCardinality(String),
    availability Enum8(
        'in_stock' = 1,
        'out_of_stock' = 2,
        'unknown' = 3
    ),

    brand Nullable(String),
    category Nullable(String),

    kafka_topic LowCardinality(String),
    kafka_partition Int32,
    kafka_offset Int64,
    kafka_timestamp DateTime64(3, 'UTC'),

    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (source, source_product_id, observed_at, event_id);
