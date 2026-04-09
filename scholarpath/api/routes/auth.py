"""Authentication routes (email + OTP, no passwords)."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select

from scholarpath.api.deps import CurrentUserDep, RedisDep, SessionDep
from scholarpath.api.models.auth import (
    AuthResponse,
    OTPRequest,
    OTPVerify,
    UserMeResponse,
)
from scholarpath.config import settings
from scholarpath.db.models.student import Student
from scholarpath.services.auth_service import (
    create_access_token,
    get_or_create_user,
    send_otp,
    verify_otp,
)

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/otp/request", status_code=status.HTTP_200_OK)
async def request_otp(body: OTPRequest, redis: RedisDep) -> dict[str, str]:
    """Send a one-time password to the given email."""
    try:
        await send_otp(body.email, redis, ttl=settings.AUTH_OTP_TTL_SECONDS)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=str(exc))
    return {"detail": "OTP sent. Check your email (or server logs in dev)."}


@router.post("/otp/verify", response_model=AuthResponse)
async def verify_otp_route(
    body: OTPVerify,
    redis: RedisDep,
    session: SessionDep,
) -> AuthResponse:
    """Verify OTP, create or find the user, and return an access token."""
    try:
        await verify_otp(
            body.email,
            body.code,
            redis,
            max_attempts=settings.AUTH_OTP_MAX_ATTEMPTS,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

    user = await get_or_create_user(body.email, session)
    await session.commit()

    # Check if there's an associated student profile
    result = await session.execute(
        select(Student.id).where(Student.user_id == user.id)
    )
    student_id = result.scalar_one_or_none()

    token = create_access_token(
        user.id,
        settings.AUTH_SECRET_KEY,
        expire_hours=settings.AUTH_TOKEN_EXPIRE_HOURS,
    )
    return AuthResponse(
        access_token=token,
        user_id=user.id,
        student_id=student_id,
    )


@router.get("/me", response_model=UserMeResponse)
async def get_me(user: CurrentUserDep, session: SessionDep) -> UserMeResponse:
    """Return the currently authenticated user's info."""
    result = await session.execute(
        select(Student.id).where(Student.user_id == user.id)
    )
    student_id = result.scalar_one_or_none()

    return UserMeResponse(
        id=user.id,
        email=user.email,
        phone=user.phone,
        is_active=user.is_active,
        student_id=student_id,
    )
