CREATE VIEW IF NOT EXISTS commerceradar.wdc_quality_summary AS
SELECT
    price_parse_status,
    count() AS offer_count
FROM commerceradar.wdc_cleaned_offers
GROUP BY price_parse_status;

CREATE VIEW IF NOT EXISTS commerceradar.wdc_category_price_summary AS
SELECT
    coalesce(category, 'Unknown') AS category,
    currency,
    count() AS priced_offer_count,
    min(price) AS min_price,
    avg(price) AS avg_price,
    max(price) AS max_price
FROM commerceradar.wdc_cleaned_offers
WHERE price_parse_status = 'parsed'
  AND price IS NOT NULL
  AND currency IS NOT NULL
GROUP BY category, currency;

CREATE VIEW IF NOT EXISTS commerceradar.wdc_category_summary AS
SELECT
    coalesce(category, 'Unknown') AS category,
    count() AS offer_count,
    countIf(price_parse_status = 'parsed' AND price IS NOT NULL)
        AS priced_offer_count
FROM commerceradar.wdc_cleaned_offers
GROUP BY category;
