"""Evaluate a user's enabled rules and persist notifications."""

from datetime import timezone
from decimal import Decimal
from uuid import uuid4

from psycopg.types.json import Jsonb

from clickhouse import ClickHouseClient, sql_string
from postgres import connection


def evaluate_user(user_id):
    with connection() as conn:
        rules = conn.execute(
            """
            SELECT r.* FROM alert_rules r
            JOIN user_settings s ON s.user_id = r.user_id
            WHERE r.user_id = %s AND r.enabled AND s.alerts_enabled
            ORDER BY r.created_at, r.id
            """,
            (user_id,),
        ).fetchall()

    client = ClickHouseClient()
    inserted = 0
    matched = 0

    for rule in rules:
        source = sql_string(rule["source"])
        product = sql_string(rule["source_product_id"])
        kind = rule["rule_type"]

        if kind == "price_below":
            rows = client.query_json_each_row(
                f"""
                SELECT event_id, observed_at, price, currency, availability
                FROM commerceradar.current_products
                WHERE source = {source} AND source_product_id = {product}
                """
            )
            rows = [
                row for row in rows
                if row["currency"] == rule["currency"]
                and Decimal(str(row["price"])) < rule["target_price"]
            ]
        else:
            created = rule["created_at"].astimezone(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )
            if kind == "price_drop":
                condition = (
                    "price_changed = 1 AND price < previous_price "
                    f"AND currency = {sql_string(rule['currency'])}"
                )
            else:
                condition = (
                    "availability_changed = 1 "
                    "AND availability = 'in_stock' "
                    "AND previous_availability = 'out_of_stock'"
                )

            rows = client.query_json_each_row(
                f"""
                SELECT event_id, observed_at, price, currency, availability
                FROM commerceradar.product_changes
                WHERE source = {source} AND source_product_id = {product}
                  AND observed_at >= parseDateTime64BestEffort(
                      {sql_string(created)}, 3, 'UTC'
                  )
                  AND ({condition})
                ORDER BY observed_at, event_id
                """
            )

        matched += len(rows)
        with connection() as conn:
            # Recheck ownership and enabled state before saving.
            active = conn.execute(
                """
                SELECT r.id FROM alert_rules r
                JOIN user_settings s ON s.user_id = r.user_id
                WHERE r.id = %s AND r.user_id = %s
                  AND r.enabled AND s.alerts_enabled
                FOR UPDATE OF r, s
                """,
                (rule["id"], user_id),
            ).fetchone()
            if active is None:
                continue

            for row in rows:
                message = (
                    f"{kind}: {rule['source']}/{rule['source_product_id']} "
                    f"price={row['price']} {row['currency']}; "
                    f"availability={row['availability']}"
                )
                result = conn.execute(
                    """
                    INSERT INTO alerts
                        (id, rule_id, event_id, observed_at, message, details)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (rule_id, event_id) DO NOTHING
                    RETURNING id
                    """,
                    (
                        uuid4(), rule["id"], row["event_id"],
                        str(row["observed_at"]).replace(" ", "T") + "+00:00",
                        message, Jsonb(row),
                    ),
                ).fetchone()
                inserted += int(result is not None)

    return {
        "rules_checked": len(rules),
        "matching_observations": matched,
        "alerts_created": inserted,
    }
