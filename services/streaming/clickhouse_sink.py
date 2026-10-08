"""Write validated Spark observations to ClickHouse over HTTP."""

import base64
import json
import os
import re
from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

COLUMNS = [
    "schema_version", "event_id", "observed_at",
    "source", "source_product_id", "product_url", "title",
    "price", "currency", "availability", "brand", "category",
    "kafka_topic", "kafka_partition", "kafka_offset",
    "kafka_timestamp",
]

MAX_ROWS = 500
MAX_BYTES = 1_000_000


def utc_timestamp(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))

    # Spark's session timezone is UTC; its collected timestamps are naive.
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)

    return (
        value.astimezone(timezone.utc)
        .strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    )


def write_observations(valid):
    endpoint = os.environ["CLICKHOUSE_HTTP_URL"].rstrip("/")
    database = os.environ["CLICKHOUSE_DB"]
    user = os.environ["CLICKHOUSE_USER"]
    password = os.environ["CLICKHOUSE_PASSWORD"]

    parsed_url = urlsplit(endpoint)
    if (
        parsed_url.scheme not in {"http", "https"}
        or not parsed_url.netloc
        or parsed_url.username is not None
        or parsed_url.query
        or parsed_url.fragment
        or parsed_url.path not in {"", "/"}
    ):
        raise ValueError("Invalid CLICKHOUSE_HTTP_URL")

    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database):
        raise ValueError("Invalid CLICKHOUSE_DB")
    if not user or not password:
        raise ValueError("ClickHouse credentials are required")

    query = (
        f"INSERT INTO {database}.product_observations "
        f"({', '.join(COLUMNS)}) FORMAT JSONEachRow"
    )
    url = endpoint + "/?" + urlencode({
        "query": query,
        "wait_end_of_query": "1",
        "async_insert": "0",
        "date_time_input_format": "best_effort",
    })
    token = base64.b64encode(
        f"{user}:{password}".encode("utf-8")
    ).decode("ascii")

    def send(lines):
        request = Request(
            url,
            data=b"".join(lines),
            headers={
                "Authorization": f"Basic {token}",
                "Content-Type": "application/x-ndjson",
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=60) as response:
                response.read()
        except HTTPError as error:
            detail = error.read(2000).decode("utf-8", errors="replace")
            # Avoid exposing credentials in server error messages.
            detail = detail.replace(password, "[REDACTED]")
            raise RuntimeError(
                f"ClickHouse insert failed: HTTP {error.code}: {detail}"
            ) from None

    selected = valid.select(
        "schema_version", "event_id", "observed_at",
        "source", "source_product_id", "product_url", "title",
        "price_decimal", "currency", "availability",
        "brand", "category",
        "topic", "partition", "offset", "kafka_timestamp",
    )

    lines = []
    size = 0
    written = 0

    for row in selected.toLocalIterator(prefetchPartitions=False):
        record = {
            "schema_version": row.schema_version,
            "event_id": row.event_id,
            "observed_at": utc_timestamp(row.observed_at),
            "source": row.source,
            "source_product_id": row.source_product_id,
            "product_url": row.product_url,
            "title": row.title,
            "price": str(row.price_decimal),
            "currency": row.currency,
            "availability": row.availability,
            "brand": row.brand,
            "category": row.category,
            "kafka_topic": row.topic,
            "kafka_partition": row.partition,
            "kafka_offset": row.offset,
            "kafka_timestamp": utc_timestamp(row.kafka_timestamp),
        }
        line = (
            json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n"
        ).encode("utf-8")

        if len(line) > MAX_BYTES:
            raise ValueError("Observation exceeds the HTTP chunk size limit")

        if lines and (
            len(lines) >= MAX_ROWS or size + len(line) > MAX_BYTES
        ):
            send(lines)
            written += len(lines)
            lines = []
            size = 0

        lines.append(line)
        size += len(line)

    if lines:
        send(lines)
        written += len(lines)

    return written
