CREATE VIEW IF NOT EXISTS commerceradar.current_products AS
SELECT
    source,
    source_product_id,
    latest.1 AS event_id,
    latest.2 AS observed_at,
    latest.3 AS title,
    latest.4 AS product_url,
    latest.5 AS price,
    latest.6 AS currency,
    latest.7 AS availability,
    latest.8 AS brand,
    latest.9 AS category
FROM
(
    SELECT
        source,
        source_product_id,
        argMax(
            tuple(
                event_id, observed_at, title, product_url,
                price, currency, availability, brand, category
            ),
            tuple(observed_at, kafka_topic, kafka_partition, kafka_offset, event_id)
        ) AS latest
    FROM commerceradar.product_observations FINAL
    GROUP BY source, source_product_id
);

CREATE VIEW IF NOT EXISTS commerceradar.product_changes AS
SELECT
    event_id,
    observed_at,
    source,
    source_product_id,
    title,
    previous_price,
    price,
    previous_currency,
    currency,
    previous_availability,
    availability,
    (currency = previous_currency AND price != previous_price)
        AS price_changed,
    (availability != previous_availability) AS stock_changed,
    (currency != previous_currency) AS currency_changed
FROM
(
    SELECT
        event_id, observed_at, source, source_product_id,
        title, price, currency, toString(availability) AS availability,
        row_number() OVER product_window AS observation_number,
        lagInFrame(price, 1, price)
            OVER product_window AS previous_price,
        lagInFrame(currency, 1, currency)
            OVER product_window AS previous_currency,
        lagInFrame(toString(availability), 1, toString(availability))
            OVER product_window AS previous_availability
    FROM commerceradar.product_observations FINAL
    WINDOW product_window AS
    (
        PARTITION BY source, source_product_id
        ORDER BY observed_at, kafka_topic, kafka_partition, kafka_offset, event_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
)
WHERE observation_number > 1
  AND (
      price != previous_price
      OR availability != previous_availability
      OR currency != previous_currency
  );

CREATE VIEW IF NOT EXISTS commerceradar.product_first_seen AS
SELECT
    source,
    source_product_id,
    min(observed_at) AS first_observed_at
FROM commerceradar.product_observations FINAL
GROUP BY source, source_product_id;
