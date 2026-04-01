"""Chat session management routes."""

from __future__ import annotations

import uuid
from datetime import datetime

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, update

from scholarpath.api.deps import SessionDep
from scholarpath.db.models.chat_session import ChatSession
from scholarpath.db.models.student import Student

router = APIRouter(prefix="/sessions", tags=["sessions"])


class SessionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    student_id: uuid.UUID
    session_id: str
    title: str
    preview: str | None = None
    message_count: int
    school_count: int
    created_at: datetime
    last_active_at: datetime


class SessionCreate(BaseModel):
    student_id: uuid.UUID
    session_id: str
    title: str = "New Session"


class SessionUpdate(BaseModel):
    title: str | None = None
    preview: str | None = None
    message_count: int | None = None
    school_count: int | None = None


@router.get("/student/{student_id}", response_model=list[SessionResponse])
async def list_sessions(student_id: uuid.UUID, session: SessionDep) -> list:
    """List all chat sessions for a student, most recent first."""
    stmt = (
        select(ChatSession)
        .where(ChatSession.student_id == student_id)
        .order_by(ChatSession.last_active_at.desc())
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


@router.post("/", response_model=SessionResponse, status_code=status.HTTP_201_CREATED)
async def create_session(payload: SessionCreate, session: SessionDep) -> ChatSession:
    """Create or return existing chat session."""
    # Check if session_id already exists
    stmt = select(ChatSession).where(ChatSession.session_id == payload.session_id)
    result = await session.execute(stmt)
    existing = result.scalars().first()
    if existing:
        return existing

    # Verify student exists
    student = await session.get(Student, payload.student_id)
    if student is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Student not found")

    chat_session = ChatSession(
        student_id=payload.student_id,
        session_id=payload.session_id,
        title=payload.title,
    )
    session.add(chat_session)
    await session.flush()
    await session.refresh(chat_session)
    return chat_session


@router.put("/{session_id}", response_model=SessionResponse)
async def update_session(
    session_id: str,
    payload: SessionUpdate,
    session: SessionDep,
) -> ChatSession:
    """Update session metadata (title, preview, counts)."""
    stmt = select(ChatSession).where(ChatSession.session_id == session_id)
    result = await session.execute(stmt)
    chat_session = result.scalars().first()
    if chat_session is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Session not found")

    update_data = payload.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(chat_session, field, value)

    await session.flush()
    await session.refresh(chat_session)
    return chat_session


@router.delete("/{session_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_session(session_id: str, session: SessionDep) -> None:
    """Delete a chat session."""
    stmt = select(ChatSession).where(ChatSession.session_id == session_id)
    result = await session.execute(stmt)
    chat_session = result.scalars().first()
    if chat_session is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Session not found")
    await session.delete(chat_session)
