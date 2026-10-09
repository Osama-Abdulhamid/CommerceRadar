"""PostgreSQL connection for application data."""

import os
from contextlib import contextmanager

import psycopg
from fastapi import HTTPException
from psycopg.rows import dict_row


@contextmanager
def connection():
    try:
        with psycopg.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "commerceradar_app"),
            user=os.getenv("POSTGRES_USER", "commerceradar"),
            password=os.environ["POSTGRES_PASSWORD"],
            connect_timeout=5,
            options="-c statement_timeout=10000",
            row_factory=dict_row,
        ) as conn:
            yield conn
    except psycopg.OperationalError as error:
        raise HTTPException(
            status_code=503,
            detail="Application database is unavailable",
        ) from error
