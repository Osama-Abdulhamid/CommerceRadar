"""User accounts, expiring sessions and preferences."""

import hashlib
import hmac
import re
import secrets
from datetime import datetime, timedelta, timezone
from typing import Literal
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, field_validator
from psycopg.errors import UniqueViolation

from postgres import connection

router = APIRouter()
bearer = HTTPBearer(auto_error=False)


class Registration(BaseModel):
    email: str = Field(min_length=3, max_length=254)
    display_name: str = Field(min_length=1, max_length=100)
    password: str = Field(min_length=12, max_length=128)

    @field_validator("email")
    @classmethod
    def validate_email(cls, value):
        value = value.strip().lower()
        if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", value):
            raise ValueError("Invalid email")
        return value

    @field_validator("display_name")
    @classmethod
    def validate_name(cls, value):
        value = value.strip()
        if not value:
            raise ValueError("Display name is required")
        return value


class Login(BaseModel):
    email: str = Field(min_length=3, max_length=254)
    password: str = Field(min_length=1, max_length=128)


class Settings(BaseModel):
    preferred_currency: Literal["EGP", "USD", "EUR", "GBP", "SAR", "AED"]
    alerts_enabled: bool


def hash_password(password, salt=None):
    salt = salt or secrets.token_hex(16)
    digest = hashlib.scrypt(
        password.encode(), salt=bytes.fromhex(salt),
        n=16384, r=8, p=1, dklen=32,
    ).hex()
    return f"scrypt${salt}${digest}"


def token_hash(token):
    return hashlib.sha256(token.encode()).hexdigest()


def current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
):
    if credentials is None:
        raise HTTPException(
            status_code=401, detail="Login required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    with connection() as conn:
        user = conn.execute(
            """
            SELECT u.id, u.email, u.display_name, u.created_at
            FROM app_users u
            JOIN user_sessions s ON s.user_id = u.id
            WHERE s.token_hash = %s AND s.expires_at > now()
            """,
            (token_hash(credentials.credentials),),
        ).fetchone()
    if user is None:
        raise HTTPException(
            status_code=401, detail="Invalid or expired session",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


@router.post("/auth/register", status_code=201)
def register(body: Registration):
    user_id = uuid4()
    password_hash = hash_password(body.password)
    try:
        with connection() as conn:
            user = conn.execute(
                """
                INSERT INTO app_users (id, email, display_name, password_hash)
                VALUES (%s, %s, %s, %s)
                RETURNING id, email, display_name, created_at
                """,
                (user_id, body.email, body.display_name, password_hash),
            ).fetchone()
            conn.execute(
                "INSERT INTO user_settings (user_id) VALUES (%s)",
                (user_id,),
            )
    except UniqueViolation:
        raise HTTPException(status_code=409, detail="Email already registered")
    return user


@router.post("/auth/login")
def login(body: Login):
    with connection() as conn:
        user = conn.execute(
            "SELECT id, password_hash FROM app_users WHERE email = %s",
            (body.email.strip().lower(),),
        ).fetchone()
        # Perform password hashing even when the email is unknown.
        stored = user["password_hash"] if user else hash_password("dummy-password")
        _, salt, _ = stored.split("$")
        valid = hmac.compare_digest(hash_password(body.password, salt), stored)
        if user is None or not valid:
            raise HTTPException(status_code=401, detail="Invalid email or password")
        token = secrets.token_urlsafe(32)
        expires = datetime.now(timezone.utc) + timedelta(hours=24)
        conn.execute(
            """
            INSERT INTO user_sessions (token_hash, user_id, expires_at)
            VALUES (%s, %s, %s)
            """,
            (token_hash(token), user["id"], expires),
        )
    return {
        "access_token": token,
        "token_type": "bearer",
        "expires_at": expires,
    }


@router.get("/auth/me")
def me(user=Depends(current_user)):
    return user


@router.post("/auth/logout")
def logout(
    user=Depends(current_user),
    credentials=Depends(bearer),
):
    with connection() as conn:
        conn.execute(
            "DELETE FROM user_sessions WHERE token_hash = %s AND user_id = %s",
            (token_hash(credentials.credentials), user["id"]),
        )
    return {"status": "logged_out"}


@router.get("/settings")
def get_settings(user=Depends(current_user)):
    with connection() as conn:
        return conn.execute(
            """
            SELECT preferred_currency, alerts_enabled, updated_at
            FROM user_settings WHERE user_id = %s
            """,
            (user["id"],),
        ).fetchone()


@router.put("/settings")
def update_settings(body: Settings, user=Depends(current_user)):
    with connection() as conn:
        return conn.execute(
            """
            UPDATE user_settings
            SET preferred_currency = %s, alerts_enabled = %s, updated_at = now()
            WHERE user_id = %s
            RETURNING preferred_currency, alerts_enabled, updated_at
            """,
            (body.preferred_currency, body.alerts_enabled, user["id"]),
        ).fetchone()
