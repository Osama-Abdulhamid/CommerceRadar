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
    allow_methods=["GET"],
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
