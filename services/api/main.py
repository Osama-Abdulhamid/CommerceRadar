"""FastAPI serving layer for CommerceRadar analytical data."""

from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from clickhouse import (
    ClickHouseClient,
    ClickHouseError,
    bounded_limit,
    bounded_offset,
    sql_string,
)


app = FastAPI(
    title="CommerceRadar API",
    version="1.0.0",
    description="Read API for streaming observations and batch product intelligence.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
    allow_headers=["*"],
)


def get_clickhouse() -> ClickHouseClient:
    return ClickHouseClient()


def run_query(client: ClickHouseClient, sql: str):
    try:
        return client.query_json_each_row(sql)
    except ClickHouseError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error


@app.get("/health")
def health(client: ClickHouseClient = Depends(get_clickhouse)):
    try:
        client.query_scalar("SELECT 1")
    except ClickHouseError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    return {"status": "ok"}


@app.get("/batch/evaluation/latest")
def latest_batch_evaluation(client: ClickHouseClient = Depends(get_clickhouse)):
    rows = run_query(
        client,
        """
        SELECT
            input_records,
            candidate_pairs,
            matched_pairs,
            canonical_groups,
            unmatched_records,
            true_positive_pairs,
            false_positive_pairs,
            false_negative_pairs,
            precision,
            recall,
            f1,
            blocking_strategy,
            loaded_at
        FROM commerceradar.batch_matching_evaluation
        ORDER BY loaded_at DESC
        LIMIT 1
        """,
    )
    if not rows:
        raise HTTPException(status_code=404, detail="No batch evaluation has been loaded")
    return rows[0]


@app.get("/batch/products")
def list_batch_products(
    q: str | None = Query(default=None, max_length=120),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    client: ClickHouseClient = Depends(get_clickhouse),
):
    safe_limit = bounded_limit(limit)
    safe_offset = bounded_offset(offset)
    where = ""
    if q:
        needle = sql_string(f"%{q.lower()}%")
        where = f"WHERE lower(coalesce(normalized_title, '')) LIKE {needle}"

    return run_query(
        client,
        f"""
        SELECT
            canonical_product_id,
            anyLast(original_product_title) AS title,
            anyLast(brand) AS brand,
            anyLast(category) AS category,
            max(match_confidence) AS max_match_confidence,
            count() AS offer_count
        FROM commerceradar.batch_product_matches
        {where}
        GROUP BY canonical_product_id
        ORDER BY offer_count DESC, title ASC
        LIMIT {safe_limit}
        OFFSET {safe_offset}
        """,
    )


@app.get("/batch/products/{canonical_product_id}")
def get_batch_product(
    canonical_product_id: str,
    client: ClickHouseClient = Depends(get_clickhouse),
):
    product_id = sql_string(canonical_product_id)
    rows = run_query(
        client,
        f"""
        SELECT
            canonical_product_id,
            source,
            source_product_id,
            source_record_id,
            cluster_id,
            original_product_title,
            normalized_title,
            brand,
            category,
            price,
            currency,
            price_parse_status,
            match_confidence,
            match_method
        FROM commerceradar.batch_product_matches
        WHERE canonical_product_id = {product_id}
        ORDER BY source, source_product_id
        LIMIT 500
        """,
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Product not found")
    return {"canonical_product_id": canonical_product_id, "offers": rows}


@app.get("/batch/products/{canonical_product_id}/price-summary")
def get_batch_product_price_summary(
    canonical_product_id: str,
    client: ClickHouseClient = Depends(get_clickhouse),
):
    product_id = sql_string(canonical_product_id)
    rows = run_query(
        client,
        f"""
        SELECT
            canonical_product_id,
            currency,
            min_price,
            max_price,
            avg_price,
            price_range,
            observation_count,
            source_count,
            offer_count
        FROM commerceradar.batch_product_price_summary
        WHERE canonical_product_id = {product_id}
        ORDER BY currency
        """,
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Price summary not found")
    return rows


@app.get("/batch/price-summary")
def list_price_summaries(
    currency: str | None = Query(default=None, max_length=8),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    client: ClickHouseClient = Depends(get_clickhouse),
):
    safe_limit = bounded_limit(limit)
    safe_offset = bounded_offset(offset)
    where = f"WHERE currency = {sql_string(currency.upper())}" if currency else ""
    return run_query(
        client,
        f"""
        SELECT
            canonical_product_id,
            currency,
            min_price,
            max_price,
            avg_price,
            price_range,
            observation_count,
            source_count,
            offer_count
        FROM commerceradar.batch_product_price_summary
        {where}
        ORDER BY observation_count DESC, avg_price DESC
        LIMIT {safe_limit}
        OFFSET {safe_offset}
        """,
    )


@app.get("/streaming/observations")
def list_streaming_observations(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    client: ClickHouseClient = Depends(get_clickhouse),
):
    safe_limit = bounded_limit(limit)
    safe_offset = bounded_offset(offset)
    return run_query(
        client,
        f"""
        SELECT
            event_id,
            observed_at,
            source,
            source_product_id,
            title,
            price,
            currency,
            availability,
            brand,
            category
        FROM commerceradar.product_observations FINAL
        ORDER BY observed_at DESC
        LIMIT {safe_limit}
        OFFSET {safe_offset}
        """,
    )


@app.get("/wdc/overview")
def wdc_overview(client: ClickHouseClient = Depends(get_clickhouse)):
    return run_query(
        client,
        """
        SELECT count() AS total_offers,
               countIf(price_parse_status = 'parsed' AND price IS NOT NULL)
                   AS priced_offers,
               uniqExact(category) AS categories
        FROM commerceradar.wdc_cleaned_offers
        """,
    )[0]


@app.get("/wdc/quality")
def wdc_quality(client: ClickHouseClient = Depends(get_clickhouse)):
    return run_query(
        client,
        "SELECT * FROM commerceradar.wdc_quality_summary "
        "ORDER BY offer_count DESC",
    )


@app.get("/wdc/categories")
def wdc_categories(client: ClickHouseClient = Depends(get_clickhouse)):
    return run_query(
        client,
        "SELECT * FROM commerceradar.wdc_category_summary "
        "ORDER BY offer_count DESC",
    )


@app.get("/wdc/category-prices")
def wdc_category_prices(
    currency: str | None = Query(default=None, min_length=3, max_length=3),
    client: ClickHouseClient = Depends(get_clickhouse),
):
    where = (
        f"WHERE currency = {sql_string(currency.upper())}"
        if currency else ""
    )
    return run_query(
        client,
        "SELECT * FROM commerceradar.wdc_category_price_summary "
        f"{where} ORDER BY priced_offer_count DESC",
    )


@app.get("/streaming/current")
def streaming_current(
    limit: int = Query(default=50, ge=1, le=500),
    client: ClickHouseClient = Depends(get_clickhouse),
):
    return run_query(
        client,
        "SELECT * FROM commerceradar.current_products "
        f"ORDER BY observed_at DESC, source, source_product_id LIMIT {limit}",
    )


@app.get("/streaming/changes")
def streaming_changes(
    limit: int = Query(default=50, ge=1, le=500),
    client: ClickHouseClient = Depends(get_clickhouse),
):
    return run_query(
        client,
        "SELECT * FROM commerceradar.product_changes "
        f"ORDER BY observed_at DESC, event_id LIMIT {limit}",
    )


@app.get("/health/postgres")
def postgres_health():
    from postgres import connection

    with connection() as conn:
        row = conn.execute(
            "SELECT current_database() AS database, 1 AS connected"
        ).fetchone()
    return {"status": "ok", **row}


from accounts import router as accounts_router
app.include_router(accounts_router)


from alert_routes import router as alert_router
app.include_router(alert_router)


@app.post("/internal/alerts/evaluate")
def evaluate_all_alerts(
    x_service_key: str | None = __import__("fastapi").Header(default=None),
):
    import hmac
    import os
    from alert_engine import evaluate_user
    from postgres import connection

    expected = os.environ.get("INTERNAL_SERVICE_KEY", "")
    if not expected or not hmac.compare_digest(x_service_key or "", expected):
        raise HTTPException(status_code=401, detail="Invalid service key")

    with connection() as conn:
        users = conn.execute(
            """
            SELECT DISTINCT r.user_id
            FROM alert_rules r
            JOIN user_settings s ON s.user_id = r.user_id
            WHERE r.enabled AND s.alerts_enabled
            """
        ).fetchall()

    created = 0
    checked = 0
    try:
        for user in users:
            result = evaluate_user(user["user_id"])
            created += result["alerts_created"]
            checked += result["rules_checked"]
    except ClickHouseError as error:
        raise HTTPException(
            status_code=503, detail="Analytics database is unavailable",
        ) from error

    return {
        "users_checked": len(users),
        "rules_checked": checked,
        "alerts_created": created,
    }


from metrics import install_metrics
install_metrics(app)


@app.get("/matching/overview")
def matching_overview(client: ClickHouseClient = Depends(get_clickhouse)):
    return run_query(
        client,
        """
        SELECT
            sum(total_offers) AS total_offers,
            sum(identifier_eligible_offers) AS identifier_eligible_offers,
            sum(unresolved_offers) AS unresolved_offers
        FROM commerceradar.wdc_identifier_coverage
        SETTINGS max_threads = 1
        """,
    )[0]


@app.get("/matching/categories")
def matching_categories(client: ClickHouseClient = Depends(get_clickhouse)):
    return run_query(
        client,
        """
        SELECT *
        FROM commerceradar.wdc_identifier_coverage
        ORDER BY total_offers DESC
        SETTINGS max_threads = 1
        """,
    )


@app.get("/streaming/price-history")
def streaming_price_history(
    source: str | None = Query(default=None, max_length=100),
    source_product_id: str | None = Query(default=None, max_length=200),
    limit: int = Query(default=500, ge=1, le=5000),
    client: ClickHouseClient = Depends(get_clickhouse),
):
    filters = []
    if source:
        filters.append(f"source = {sql_string(source)}")
    if source_product_id:
        filters.append(
            f"source_product_id = {sql_string(source_product_id)}"
        )
    where = "WHERE " + " AND ".join(filters) if filters else ""
    return run_query(
        client,
        f"""
        SELECT event_id, observed_at, source, source_product_id,
               title, price, currency, availability
        FROM commerceradar.product_observations FINAL
        {where}
        ORDER BY observed_at DESC, event_id
        LIMIT {limit}
        """,
    )


@app.get("/demo/competitor-prices")
def demo_competitor_prices(
    client: ClickHouseClient = Depends(get_clickhouse),
):
    return run_query(
        client,
        """
        SELECT
            source_product_id AS demo_product_id,
            currency,
            min(price) AS lowest_price,
            max(price) AS highest_price,
            max(price) - min(price) AS price_gap,
            uniqExact(source) AS store_count,
            'simulated' AS data_origin
        FROM commerceradar.current_products
        WHERE source IN ('demo_market_a', 'demo_market_b')
          AND source_product_id IN (
              'DEMO-PHONE-128', 'DEMO-LAPTOP-16', 'DEMO-HEADPHONES'
          )
          AND availability = 'in_stock'
        GROUP BY source_product_id, currency
        HAVING store_count = 2
        ORDER BY demo_product_id, currency
        """,
    )
