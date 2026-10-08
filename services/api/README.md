# CommerceRadar API

## Purpose

FastAPI read layer for CommerceRadar analytical data stored in ClickHouse.
The API serves the batch product-intelligence tables and the existing
streaming observations table. Visualization and Power BI dashboard design
remain Member 4 work.

## Run

Start ClickHouse first and make sure `.env` contains `CLICKHOUSE_PASSWORD`.

```bash
docker compose up -d --build api
```

Open:

```text
http://localhost:18000/docs
```

## Endpoints

- `GET /health`
- `GET /batch/evaluation/latest`
- `GET /batch/products`
- `GET /batch/products?q=samsung&limit=20`
- `GET /batch/products/{canonical_product_id}`
- `GET /batch/products/{canonical_product_id}/price-summary`
- `GET /batch/price-summary`
- `GET /batch/price-summary?currency=USD`
- `GET /streaming/observations`

## Sample Verification

After loading the committed WDC sample through the batch pipeline, the API
should return:

```bash
curl http://localhost:18000/health
curl http://localhost:18000/batch/evaluation/latest
curl "http://localhost:18000/batch/products?limit=5"
curl "http://localhost:18000/batch/price-summary?limit=5"
```

The streaming observations endpoint requires the streaming table to exist
and contain data. It may return an empty list in a batch-only sample run.
