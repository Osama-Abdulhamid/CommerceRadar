"""Minimal ClickHouse HTTP client for the CommerceRadar API."""

from __future__ import annotations

import base64
import json
import os
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


class ClickHouseError(RuntimeError):
    """Raised when ClickHouse rejects or cannot serve a query."""


@dataclass(frozen=True)
class ClickHouseSettings:
    http_url: str
    database: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "ClickHouseSettings":
        return cls(
            http_url=os.getenv("CLICKHOUSE_HTTP_URL", "http://localhost:18123"),
            database=os.getenv("CLICKHOUSE_DB", "commerceradar"),
            user=os.getenv("CLICKHOUSE_USER", "commerceradar"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        )


class ClickHouseClient:
    def __init__(self, settings: ClickHouseSettings | None = None) -> None:
        self.settings = settings or ClickHouseSettings.from_env()

    def query_json_each_row(self, sql: str) -> list[dict[str, Any]]:
        body = self._post(f"{sql.rstrip(';')} FORMAT JSONEachRow")
        if not body.strip():
            return []
        return [json.loads(line) for line in body.splitlines()]

    def query_scalar(self, sql: str) -> Any:
        body = self._post(sql)
        return body.strip()

    def _post(self, sql: str) -> str:
        if not self.settings.password:
            raise ClickHouseError("CLICKHOUSE_PASSWORD is not configured")

        url = f"{self.settings.http_url.rstrip('/')}?query={urllib.parse.quote(sql)}"
        request = urllib.request.Request(url, method="POST")
        token = base64.b64encode(
            f"{self.settings.user}:{self.settings.password}".encode("utf-8")
        ).decode("ascii")
        request.add_header("Authorization", f"Basic {token}")

        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                return response.read().decode("utf-8")
        except Exception as error:  # urllib wraps useful details in several types.
            raise ClickHouseError(str(error)) from error


def sql_string(value: str) -> str:
    """Quote a string literal for ClickHouse SQL."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def bounded_limit(value: int, default: int = 50, maximum: int = 500) -> int:
    if value <= 0:
        return default
    return min(value, maximum)


def bounded_offset(value: int) -> int:
    return max(value, 0)
