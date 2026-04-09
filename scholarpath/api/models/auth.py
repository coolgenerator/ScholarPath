"""Pydantic schemas for email + OTP authentication."""

from __future__ import annotations

import uuid

from pydantic import BaseModel, EmailStr, Field


class OTPRequest(BaseModel):
    email: EmailStr


class OTPVerify(BaseModel):
    email: EmailStr
    code: str = Field(..., min_length=6, max_length=6)


class AuthResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user_id: uuid.UUID
    student_id: uuid.UUID | None = None


class UserMeResponse(BaseModel):
    id: uuid.UUID
    email: str
    phone: str | None = None
    is_active: bool
    student_id: uuid.UUID | None = None
