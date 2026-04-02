"""Promote latest Stage4 champion if staged + shadow gates are satisfied."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scholarpath.config import settings
from scholarpath.scripts.causal_staged_train import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SHADOW_HISTORY_PATH,
    PromotionDecision,
    _promote_if_ready,
)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        return list(csv.DictReader(path.read_text(encoding="utf-8").splitlines()))
    except Exception:
        return []


def _parse_generated_at(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _latest_stage4_champion(staged_output_dir: Path) -> str | None:
    history_rows = _read_csv_rows(staged_output_dir / "history.csv")
    candidates = [
        row
        for row in history_rows
        if str(row.get("stage", "")).strip() == "4"
        and str(row.get("champion_model_version", "")).strip()
    ]
    if not candidates:
        return None
    latest = max(
        candidates,
        key=lambda row: _parse_generated_at(str(row.get("generated_at", ""))),
    )
    model_version = str(latest.get("champion_model_version", "")).strip()
    return model_version or None


def _clamp_percent(value: int) -> int:
    return max(0, min(100, int(value)))


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    staged_output_dir = Path(args.staged_output_dir)
    champion_model_version = _latest_stage4_champion(staged_output_dir)
    if not champion_model_version:
        decision = PromotionDecision(
            attempted=False,
            promoted=False,
            model_version=None,
            reasons=["no stage4 champion found in staged history"],
        )
    else:
        decision = await _promote_if_ready(
            output_root=staged_output_dir,
            champion_model_version=champion_model_version,
            promote_on_final_pass=True,
            require_shadow_window_hours=int(args.shadow_window_hours),
            shadow_history_path=Path(args.shadow_history_path),
            shadow_target_percent=_clamp_percent(int(args.shadow_target_percent)),
            shadow_min_rows=max(1, int(args.shadow_min_rows)),
            shadow_refresh_before_promote=bool(args.shadow_refresh_before_promote),
        )

    payload = {
        **asdict(decision),
        "staged_output_dir": str(staged_output_dir),
        "shadow_history_path": str(Path(args.shadow_history_path)),
        "shadow_window_hours": int(args.shadow_window_hours),
        "shadow_target_percent": _clamp_percent(int(args.shadow_target_percent)),
        "shadow_min_rows": max(1, int(args.shadow_min_rows)),
        "shadow_refresh_before_promote": bool(args.shadow_refresh_before_promote),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Promote latest Stage4 champion when staged + shadow gates pass.",
    )
    parser.add_argument(
        "--staged-output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Staged run output root (default: .benchmarks/causal_staged).",
    )
    parser.add_argument(
        "--shadow-window-hours",
        type=int,
        default=24,
        help="Shadow gate lookback window in hours (default: 24).",
    )
    parser.add_argument(
        "--shadow-min-rows",
        type=int,
        default=3,
        help="Minimum rollout rows required in shadow window (default: 3).",
    )
    parser.add_argument(
        "--shadow-target-percent",
        type=int,
        default=_clamp_percent(int(settings.CAUSAL_PYWHY_PRIMARY_PERCENT)),
        help="Target rollout percent filter (default: CAUSAL_PYWHY_PRIMARY_PERCENT).",
    )
    parser.add_argument(
        "--shadow-history-path",
        default=str(DEFAULT_SHADOW_HISTORY_PATH),
        help="Rollout history path (default: .benchmarks/causal_rollout/history.csv).",
    )
    parser.add_argument(
        "--shadow-refresh-before-promote",
        dest="shadow_refresh_before_promote",
        action="store_true",
        default=True,
        help="Refresh rollout gate once before promote check (default: enabled).",
    )
    parser.add_argument(
        "--no-shadow-refresh-before-promote",
        dest="shadow_refresh_before_promote",
        action="store_false",
        help="Skip rollout gate refresh before promote check.",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
