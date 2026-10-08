CREATE TABLE IF NOT EXISTS commerceradar.batch_product_matches
(
    canonical_product_id String,
    source LowCardinality(String),
    source_product_id String,
    source_record_id String,
    cluster_id Nullable(String),
    original_product_title Nullable(String),
    normalized_title Nullable(String),
    brand Nullable(String),
    normalized_brand Nullable(String),
    category Nullable(String),
    normalized_category Nullable(String),
    price Nullable(Decimal(18, 2)),
    currency Nullable(String),
    price_parse_status LowCardinality(String),
    match_confidence Float64,
    match_method LowCardinality(String),
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (canonical_product_id, source, source_product_id);

CREATE TABLE IF NOT EXISTS commerceradar.batch_product_price_summary
(
    canonical_product_id String,
    currency LowCardinality(String),
    min_price Decimal(18, 2),
    max_price Decimal(18, 2),
    avg_price Decimal(18, 2),
    observation_count UInt64,
    source_count UInt64,
    offer_count UInt64,
    price_range Decimal(18, 2),
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (canonical_product_id, currency);

CREATE TABLE IF NOT EXISTS commerceradar.batch_product_source_summary
(
    canonical_product_id String,
    source LowCardinality(String),
    currency LowCardinality(String),
    min_price Decimal(18, 2),
    max_price Decimal(18, 2),
    avg_price Decimal(18, 2),
    observation_count UInt64,
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (canonical_product_id, source, currency);

CREATE TABLE IF NOT EXISTS commerceradar.batch_matching_evaluation
(
    input_records UInt64,
    candidate_pairs UInt64,
    matched_pairs UInt64,
    canonical_groups UInt64,
    unmatched_records UInt64,
    true_positive_pairs UInt64,
    false_positive_pairs UInt64,
    false_negative_pairs UInt64,
    precision Nullable(Float64),
    recall Nullable(Float64),
    f1 Nullable(Float64),
    blocking_strategy String,
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY loaded_at;
