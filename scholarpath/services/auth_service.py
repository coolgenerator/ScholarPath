"""Email + OTP authentication service (no passwords)."""

from __future__ import annotations

import logging
import secrets
import uuid
from datetime import datetime, timezone, timedelta

import jwt
import redis.asyncio as aioredis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.db.models.user import User

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OTP helpers
# ---------------------------------------------------------------------------

_OTP_PREFIX = "otp:"
_OTP_ATTEMPTS_PREFIX = "otp_attempts:"


async def send_otp(email: str, redis: aioredis.Redis, *, ttl: int = 600) -> None:
    """Generate a 6-digit OTP, store in Redis and log it.

    Rate-limit: reject if the existing key's TTL indicates it was created
    less than 60 seconds ago.
    """
    key = f"{_OTP_PREFIX}{email}"
    existing_ttl = await redis.ttl(key)

    # If key exists and was set less than 60s ago, reject.
    if existing_ttl > (ttl - 60):
        raise ValueError("OTP already sent recently. Please wait before requesting a new one.")

    code = f"{secrets.randbelow(1_000_000):06d}"
    await redis.set(key, code, ex=ttl)
    # Reset attempt counter
    await redis.delete(f"{_OTP_ATTEMPTS_PREFIX}{email}")

    # In production, send via email provider. For now, log to console.
    logger.info("OTP for %s: %s", email, code)


async def verify_otp(
    email: str,
    code: str,
    redis: aioredis.Redis,
    *,
    max_attempts: int = 5,
) -> bool:
    """Verify the OTP code. Returns True on success.

    Enforces a max-attempt counter. Deletes OTP on success.
    """
    key = f"{_OTP_PREFIX}{email}"
    attempts_key = f"{_OTP_ATTEMPTS_PREFIX}{email}"

    attempts = int(await redis.get(attempts_key) or 0)
    if attempts >= max_attempts:
        await redis.delete(key, attempts_key)
        raise ValueError("Too many failed attempts. Please request a new OTP.")

    stored = await redis.get(key)
    if stored is None:
        raise ValueError("OTP expired or not found. Please request a new one.")

    if stored != code:
        await redis.incr(attempts_key)
        # Align attempts TTL with OTP TTL
        otp_ttl = await redis.ttl(key)
        if otp_ttl > 0:
            await redis.expire(attempts_key, otp_ttl)
        raise ValueError("Invalid OTP code.")

    # Success – clean up
    await redis.delete(key, attempts_key)
    return True


# ---------------------------------------------------------------------------
# User helpers
# ---------------------------------------------------------------------------


async def get_or_create_user(email: str, session: AsyncSession) -> User:
    """Look up a user by email; create a new one if not found."""
    result = await session.execute(select(User).where(User.email == email))
    user = result.scalar_one_or_none()
    if user is not None:
        return user

    user = User(email=email)
    session.add(user)
    await session.flush()
    return user


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------


def create_access_token(
    user_id: uuid.UUID,
    secret_key: str,
    expire_hours: int = 24,
) -> str:
    """Create a signed JWT with sub=user_id."""
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "exp": now + timedelta(hours=expire_hours),
        "iat": now,
    }
    return jwt.encode(payload, secret_key, algorithm="HS256")


def decode_access_token(token: str, secret_key: str) -> uuid.UUID:
    """Decode a JWT and return the user_id as UUID.

    Raises jwt.PyJWTError on any validation failure.
    """
    payload = jwt.decode(token, secret_key, algorithms=["HS256"])
    return uuid.UUID(payload["sub"])
