CREATE VIEW IF NOT EXISTS commerceradar.wdc_identifier_coverage AS
SELECT
    coalesce(category, 'Unknown') AS category,
    count() AS total_offers,
    countIf(match_method = 'experimental_identifier_key')
        AS identifier_eligible_offers,
    countIf(match_method = 'unresolved_singleton')
        AS unresolved_offers
FROM commerceradar.wdc_identifier_matches
GROUP BY category;
