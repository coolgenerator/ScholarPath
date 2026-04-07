"""Profile read/update operations for Advisor V2 capabilities."""

from __future__ import annotations

import logging
import re
import uuid
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.api.models.student import StudentPortfolioPatch
from scholarpath.chat.memory import ChatMemory
from scholarpath.llm.client import LLMClient
from scholarpath.services.portfolio_service import apply_portfolio_patch, get_portfolio

logger = logging.getLogger(__name__)

PENDING_PROFILE_PATCH_KEY = "pending_profile_patch"
PROFILE_PATCH_TTL_USER_TURNS = 1

PROFILE_CONFIRM_COMMAND_PREFIX = "confirm_profile_patch:"
PROFILE_REEDIT_COMMAND_PREFIX = "reedit_profile_patch:"

_CONFIRM_TEXT_RE = re.compile(
    r"(确认(?:修改|提交)|confirm(?:\s+the)?\s+(?:profile|patch)|apply\s+(?:profile\s+)?patch)",
    re.IGNORECASE,
)
_REEDIT_TEXT_RE = re.compile(
    r"(重新编辑|重新改|重改|re-?edit|edit\s+(?:profile|patch)\s+again)",
    re.IGNORECASE,
)
_UPDATE_HINT_RE = re.compile(
    r"(更新|修改|改成|设置|set|update|change|gpa|sat|act|toefl|major|budget|aid|profile|档案|预算|专业|成绩)",
    re.IGNORECASE,
)


@dataclass(slots=True)
class ProfileUpdateGate:
    action: Literal["propose", "commit", "reedit", "noop"]
    can_commit: bool
    reason: str
    proposal_id: str | None
    pending: dict[str, Any] | None


def _extract_command_id(message: str, prefix: str) -> str | None:
    pattern = re.compile(re.escape(prefix) + r"([a-f0-9-]{8,64})", re.IGNORECASE)
    match = pattern.search(message)
    if not match:
        return None
    return str(match.group(1)).strip().lower()


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalized_pending(context: dict[str, Any]) -> dict[str, Any] | None:
    pending = context.get(PENDING_PROFILE_PATCH_KEY)
    if isinstance(pending, dict):
        return pending
    return None


def validate_profile_patch(patch: Any) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(patch, dict):
        return None, "patch must be an object"
    try:
        parsed = StudentPortfolioPatch.model_validate(patch)
    except ValidationError as exc:
        return None, f"patch schema validation failed: {exc}"
    normalized = parsed.model_dump(exclude_unset=True)
    if not normalized:
        return None, "patch is empty"
    return normalized, None


def resolve_profile_update_gate(*, message: str, context: dict[str, Any]) -> ProfileUpdateGate:
    pending = _normalized_pending(context)
    user_turn_index = _coerce_int(context.get("history_user_turn_count"), default=0)
    confirm_id = _extract_command_id(message, PROFILE_CONFIRM_COMMAND_PREFIX)
    reedit_id = _extract_command_id(message, PROFILE_REEDIT_COMMAND_PREFIX)
    has_confirm_text = bool(_CONFIRM_TEXT_RE.search(message))
    has_reedit_text = bool(_REEDIT_TEXT_RE.search(message))

    pending_id = None
    pending_expired = False
    if pending:
        pending_id = str(pending.get("proposal_id", "")).strip().lower() or None
        created_turn = _coerce_int(pending.get("created_user_turn_index"), default=-10_000)
        pending_expired = user_turn_index > (created_turn + PROFILE_PATCH_TTL_USER_TURNS)

    if reedit_id or has_reedit_text:
        if pending is None:
            return ProfileUpdateGate(
                action="reedit",
                can_commit=False,
                reason="no pending proposal to re-edit",
                proposal_id=None,
                pending=None,
            )
        if reedit_id and pending_id and reedit_id != pending_id:
            return ProfileUpdateGate(
                action="reedit",
                can_commit=False,
                reason="re-edit command proposal id mismatch",
                proposal_id=pending_id,
                pending=pending,
            )
        return ProfileUpdateGate(
            action="reedit",
            can_commit=False,
            reason="pending proposal cleared for re-edit",
            proposal_id=pending_id,
            pending=pending,
        )

    if confirm_id or has_confirm_text:
        if pending is None:
            return ProfileUpdateGate(
                action="commit",
                can_commit=False,
                reason="no pending profile patch to confirm",
                proposal_id=confirm_id,
                pending=None,
            )
        if pending_expired:
            return ProfileUpdateGate(
                action="commit",
                can_commit=False,
                reason="pending profile patch expired",
                proposal_id=pending_id,
                pending=pending,
            )
        if confirm_id and pending_id and confirm_id != pending_id:
            return ProfileUpdateGate(
                action="commit",
                can_commit=False,
                reason="confirm command proposal id mismatch",
                proposal_id=pending_id,
                pending=pending,
            )
        _, err = validate_profile_patch(pending.get("patch"))
        if err:
            return ProfileUpdateGate(
                action="commit",
                can_commit=False,
                reason=err,
                proposal_id=pending_id,
                pending=pending,
            )
        return ProfileUpdateGate(
            action="commit",
            can_commit=True,
            reason="ready to apply pending profile patch",
            proposal_id=pending_id,
            pending=pending,
        )

    if _UPDATE_HINT_RE.search(message):
        return ProfileUpdateGate(
            action="propose",
            can_commit=False,
            reason="message appears to request profile update",
            proposal_id=pending_id,
            pending=pending,
        )

    return ProfileUpdateGate(
        action="noop",
        can_commit=False,
        reason="message does not look like a profile update instruction",
        proposal_id=pending_id,
        pending=pending,
    )


def _build_patch_summary(patch: dict[str, Any]) -> str:
    changed: list[str] = []
    for group_name, fields in patch.items():
        if isinstance(fields, dict):
            for field_name in fields:
                changed.append(f"{group_name}.{field_name}")
    if not changed:
        return "Update profile fields."
    shown = ", ".join(changed[:6])
    if len(changed) > 6:
        shown += ", ..."
    return f"Proposed updates: {shown}"


def _extract_partial_patch_from_message(message: str) -> dict[str, Any]:
    """Best-effort fallback extraction when LLM patch parsing fails."""
    patch: dict[str, Any] = {}
    text = message.strip()

    gpa_match = re.search(r"\bgpa\s*[:=]?\s*(\d(?:\.\d+)?)", text, re.IGNORECASE)
    if gpa_match:
        patch.setdefault("academics", {})["gpa"] = float(gpa_match.group(1))

    sat_match = re.search(r"\bsat\s*[:=]?\s*(\d{3,4})", text, re.IGNORECASE)
    if sat_match:
        patch.setdefault("academics", {})["sat_total"] = int(sat_match.group(1))

    budget_match = re.search(r"(budget|预算)\s*[:=]?\s*\$?(\d{4,6})", text, re.IGNORECASE)
    if budget_match:
        patch.setdefault("finance", {})["budget_usd"] = int(budget_match.group(2))

    majors: list[str] = []
    major_match = re.search(
        r"(major|专业|intended major|目标专业)\s*(?:to|改成|为|:|=)?\s*([A-Za-z0-9+\-&,/\s]{2,64})",
        text,
        re.IGNORECASE,
    )
    if major_match:
        raw = major_match.group(2).strip(" .，。")
        if raw:
            majors.append(raw)
    if majors:
        patch.setdefault("academics", {})["intended_majors"] = majors

    ed_match = re.search(r"\b(ed|ea|rea|rd)\b", text, re.IGNORECASE)
    if ed_match:
        patch.setdefault("strategy", {})["ed_preference"] = str(ed_match.group(1)).lower()

    return patch


def _infer_missing_fields_from_message(message: str, patch: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    text = message.lower()
    if ("gpa" in text or "成绩" in text) and "gpa" not in (patch.get("academics") or {}):
        missing.append("academics.gpa")
    if "sat" in text and "sat_total" not in (patch.get("academics") or {}):
        missing.append("academics.sat_total")
    if ("budget" in text or "预算" in text) and "budget_usd" not in (patch.get("finance") or {}):
        missing.append("finance.budget_usd")
    if ("major" in text or "专业" in text) and "intended_majors" not in (patch.get("academics") or {}):
        missing.append("academics.intended_majors")
    if ("ed" in text or "ea" in text or "早申" in text) and "ed_preference" not in (patch.get("strategy") or {}):
        missing.append("strategy.ed_preference")
    return missing[:8]


def _diff_portfolio_fields(
    before: dict[str, Any],
    after: dict[str, Any],
    *,
    parent: str = "",
) -> list[str]:
    out: list[str] = []
    for key in sorted(set(before) | set(after)):
        left = before.get(key)
        right = after.get(key)
        path = f"{parent}.{key}" if parent else key
        if isinstance(left, dict) and isinstance(right, dict):
            out.extend(_diff_portfolio_fields(left, right, parent=path))
            continue
        if left != right:
            out.append(path)
    return out


def _snapshot_text(portfolio: dict[str, Any]) -> str:
    identity = portfolio.get("identity") or {}
    academics = portfolio.get("academics") or {}
    finance = portfolio.get("finance") or {}
    completion = portfolio.get("completion") or {}
    majors = academics.get("intended_majors") or []
    majors_text = ", ".join(str(item) for item in majors[:3]) if majors else "N/A"
    pct = float(completion.get("completion_pct", 0.0) or 0.0)
    return (
        f"Profile snapshot for {identity.get('name', 'student')}: "
        f"GPA {academics.get('gpa', 'N/A')} ({academics.get('gpa_scale', 'N/A')}), "
        f"SAT {academics.get('sat_total', 'N/A')}, "
        f"budget ${finance.get('budget_usd', 'N/A')}, "
        f"majors: {majors_text}. "
        f"Completion: {pct:.0%}."
    )


async def build_profile_snapshot(
    *,
    session: AsyncSession,
    student_id: uuid.UUID,
) -> dict[str, Any]:
    portfolio = await get_portfolio(session, student_id)
    return {
        "content": _snapshot_text(portfolio),
        "payload": {
            "portfolio": portfolio,
            "completion": portfolio.get("completion") or {},
        },
    }


async def create_profile_patch_proposal(
    *,
    llm: LLMClient,
    session: AsyncSession,
    memory: ChatMemory,
    session_id: str,
    student_id: uuid.UUID,
    message: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    portfolio = await get_portfolio(session, student_id)
    prompt = (
        "You convert a user chat message into a StudentPortfolioPatch JSON payload.\n"
        "Return JSON only. Output schema:\n"
        "{\"patch\": <StudentPortfolioPatch object>, \"summary\": <short text>}\n"
        "Rules:\n"
        "- Only include fields explicitly requested by the user.\n"
        "- Group fields by: identity, academics, activities, finance, strategy, preferences.\n"
        "- Do not include unknown keys.\n"
        "- If no clear update is requested, return patch as {}.\n"
    )
    messages = [
        {"role": "system", "content": prompt},
        {
            "role": "user",
            "content": (
                f"Current portfolio:\n{portfolio}\n\n"
                f"Recent context:\n{context.get('recent_messages', '')}\n\n"
                f"User request:\n{message}"
            ),
        },
    ]
    extracted = await llm.complete_json(
        messages,
        temperature=0.0,
        max_tokens=512,
        caller="chat.profile_update_extract",
    )
    patch_raw = extracted.get("patch", {})
    normalized_patch, err = validate_profile_patch(patch_raw)
    if err:
        partial_patch = _extract_partial_patch_from_message(message)
        normalized_patch, partial_err = validate_profile_patch(partial_patch)
        if partial_err or normalized_patch is None:
            return {
                "content": (
                    "我没能从这条消息里提取到可写入档案的字段。"
                    "请按“字段 + 目标值”重发，例如：`GPA 改成 3.85，SAT 改成 1500，预算改成 70000`。"
                ),
                "proposal": None,
                "meta": {
                    "proposal_created": False,
                    "reason": err,
                    "partial_repair": False,
                    "missing_fields": [
                        "academics.gpa",
                        "academics.sat_total",
                        "finance.budget_usd",
                    ],
                },
            }
        summary = _build_patch_summary(normalized_patch)
        missing_fields = _infer_missing_fields_from_message(message, normalized_patch)
        proposal_id = str(uuid.uuid4())
        user_turn_index = _coerce_int(context.get("history_user_turn_count"), default=0)
        pending = {
            "proposal_id": proposal_id,
            "patch": normalized_patch,
            "summary": summary,
            "created_user_turn_index": user_turn_index,
            "expires_after_user_turns": PROFILE_PATCH_TTL_USER_TURNS,
        }
        await memory.save_context(session_id, PENDING_PROFILE_PATCH_KEY, pending)
        context[PENDING_PROFILE_PATCH_KEY] = pending

        confirm_command = f"{PROFILE_CONFIRM_COMMAND_PREFIX}{proposal_id}"
        reedit_command = f"{PROFILE_REEDIT_COMMAND_PREFIX}{proposal_id}"
        proposal_payload = {
            "proposal_id": proposal_id,
            "patch": normalized_patch,
            "summary": summary,
            "confirm_command": confirm_command,
            "reedit_command": reedit_command,
            "expires_after_user_turns": PROFILE_PATCH_TTL_USER_TURNS,
            "missing_fields": missing_fields,
        }
        content = (
            f"我先根据已识别信息生成了部分档案补丁：{summary}\n"
            f"仍需你补充/确认的字段：{', '.join(missing_fields) if missing_fields else '无'}。\n"
            f"回复 `{confirm_command}` 提交，或回复 `{reedit_command}` 重新编辑。"
        )
        return {
            "content": content,
            "proposal": proposal_payload,
            "meta": {
                "proposal_created": True,
                "proposal_id": proposal_id,
                "partial_repair": True,
                "missing_fields": missing_fields,
            },
        }

    proposal_id = str(uuid.uuid4())
    summary = str(extracted.get("summary", "")).strip() or _build_patch_summary(normalized_patch)
    user_turn_index = _coerce_int(context.get("history_user_turn_count"), default=0)
    pending = {
        "proposal_id": proposal_id,
        "patch": normalized_patch,
        "summary": summary,
        "created_user_turn_index": user_turn_index,
        "expires_after_user_turns": PROFILE_PATCH_TTL_USER_TURNS,
    }
    await memory.save_context(session_id, PENDING_PROFILE_PATCH_KEY, pending)
    context[PENDING_PROFILE_PATCH_KEY] = pending

    confirm_command = f"{PROFILE_CONFIRM_COMMAND_PREFIX}{proposal_id}"
    reedit_command = f"{PROFILE_REEDIT_COMMAND_PREFIX}{proposal_id}"
    proposal_payload = {
        "proposal_id": proposal_id,
        "patch": normalized_patch,
        "summary": summary,
        "confirm_command": confirm_command,
        "reedit_command": reedit_command,
        "expires_after_user_turns": PROFILE_PATCH_TTL_USER_TURNS,
        "missing_fields": [],
    }
    content = (
        f"{summary}\n"
        f"Reply with `{confirm_command}` to apply, or `{reedit_command}` to discard and edit again."
    )
    return {
        "content": content,
        "proposal": proposal_payload,
        "meta": {"proposal_created": True, "proposal_id": proposal_id},
    }


async def clear_pending_profile_patch(
    *,
    memory: ChatMemory,
    session_id: str,
    context: dict[str, Any],
) -> None:
    await memory.save_context(session_id, PENDING_PROFILE_PATCH_KEY, None)
    context[PENDING_PROFILE_PATCH_KEY] = None


async def apply_pending_profile_patch(
    *,
    session: AsyncSession,
    memory: ChatMemory,
    session_id: str,
    student_id: uuid.UUID,
    context: dict[str, Any],
    pending: dict[str, Any],
) -> dict[str, Any]:
    proposal_id = str(pending.get("proposal_id", "")).strip()
    patch, err = validate_profile_patch(pending.get("patch"))
    if err or patch is None:
        raise ValueError(err or "pending profile patch invalid")

    before = await get_portfolio(session, student_id)
    after = await apply_portfolio_patch(session, student_id, patch)
    changed_fields = _diff_portfolio_fields(before, after)

    await clear_pending_profile_patch(
        memory=memory,
        session_id=session_id,
        context=context,
    )
    content = (
        f"Applied profile update ({proposal_id}). "
        f"Updated fields: {', '.join(changed_fields[:8]) or 'none'}."
    )
    return {
        "content": content,
        "payload": {
            "proposal_id": proposal_id,
            "applied": True,
            "changed_fields": changed_fields,
            "portfolio": after,
        },
        "meta": {
            "proposal_id": proposal_id,
            "applied": True,
            "changed_fields": changed_fields,
        },
    }
