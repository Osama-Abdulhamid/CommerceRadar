"""Per-user alert rules and stored notifications."""

from decimal import Decimal
from typing import Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, model_validator

from accounts import current_user
from postgres import connection

router = APIRouter()
Currency = Literal["EGP", "USD", "EUR", "GBP", "SAR", "AED"]


class RuleInput(BaseModel):
    source: str = Field(min_length=1, max_length=100, pattern=r"^\S+$")
    source_product_id: str = Field(min_length=1, max_length=200, pattern=r"^\S+$")
    rule_type: Literal["price_below", "price_drop", "back_in_stock"]
    target_price: Decimal | None = Field(
        default=None, ge=0, max_digits=18, decimal_places=2,
    )
    currency: Currency | None = None

    @model_validator(mode="after")
    def check_rule(self):
        if self.rule_type == "price_below":
            if self.target_price is None:
                raise ValueError("price_below requires target_price")
        elif self.target_price is not None:
            raise ValueError("Only price_below accepts target_price")
        if self.rule_type == "back_in_stock":
            if self.currency is not None:
                raise ValueError("Stock rules do not accept currency")
        elif self.currency is None:
            raise ValueError("Price rules require currency")
        return self


class RuleState(BaseModel):
    enabled: bool


@router.post("/alert-rules", status_code=201)
def create_rule(body: RuleInput, user=Depends(current_user)):
    with connection() as conn:
        return conn.execute(
            """
            INSERT INTO alert_rules
                (id, user_id, source, source_product_id,
                 rule_type, target_price, currency)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                uuid4(), user["id"], body.source, body.source_product_id,
                body.rule_type, body.target_price, body.currency,
            ),
        ).fetchone()


@router.get("/alert-rules")
def list_rules(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    user=Depends(current_user),
):
    with connection() as conn:
        return conn.execute(
            """
            SELECT * FROM alert_rules WHERE user_id = %s
            ORDER BY created_at DESC, id
            LIMIT %s OFFSET %s
            """,
            (user["id"], limit, offset),
        ).fetchall()


@router.patch("/alert-rules/{rule_id}")
def set_rule_state(rule_id: UUID, body: RuleState, user=Depends(current_user)):
    with connection() as conn:
        row = conn.execute(
            """
            UPDATE alert_rules SET enabled = %s
            WHERE id = %s AND user_id = %s RETURNING *
            """,
            (body.enabled, rule_id, user["id"]),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Rule not found")
    return row


@router.delete("/alert-rules/{rule_id}")
def delete_rule(rule_id: UUID, user=Depends(current_user)):
    with connection() as conn:
        row = conn.execute(
            "DELETE FROM alert_rules WHERE id = %s AND user_id = %s RETURNING id",
            (rule_id, user["id"]),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Rule not found")
    return {"status": "deleted"}


@router.get("/alerts")
def list_alerts(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    unread_only: bool = False,
    user=Depends(current_user),
):
    with connection() as conn:
        return conn.execute(
            """
            SELECT a.*, r.source, r.source_product_id, r.rule_type
            FROM alerts a JOIN alert_rules r ON r.id = a.rule_id
            WHERE r.user_id = %s AND (%s = false OR a.is_read = false)
            ORDER BY a.created_at DESC, a.id
            LIMIT %s OFFSET %s
            """,
            (user["id"], unread_only, limit, offset),
        ).fetchall()


@router.patch("/alerts/{alert_id}/read")
def mark_read(alert_id: UUID, user=Depends(current_user)):
    with connection() as conn:
        row = conn.execute(
            """
            UPDATE alerts a SET is_read = true
            FROM alert_rules r
            WHERE a.rule_id = r.id AND r.user_id = %s AND a.id = %s
            RETURNING a.*
            """,
            (user["id"], alert_id),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Alert not found")
    return row


@router.post("/alerts/evaluate")
def evaluate_alerts(user=Depends(current_user)):
    from alert_engine import evaluate_user
    from clickhouse import ClickHouseError

    try:
        return evaluate_user(user["id"])
    except ClickHouseError as error:
        raise HTTPException(
            status_code=503, detail="Analytics database is unavailable",
        ) from error
