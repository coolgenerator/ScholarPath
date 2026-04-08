"""Ingest IPEDS/CIP program completions into `programs` (Phase A)."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scholarpath.db.session import async_session_factory
from scholarpath.services.causal_data_service import ingest_ipeds_program_facts


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest IPEDS program completion facts into Program rows (Phase A).",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=3,
        help="Lookback window in years from latest dataset year (default: 3).",
    )
    parser.add_argument(
        "--min-completions",
        type=int,
        default=5,
        help="Minimum completions required to keep a program row (default: 5).",
    )
    parser.add_argument(
        "--award-levels",
        default="bachelor",
        help="Comma-separated award levels filter (e.g. bachelor,master). Empty means no filter.",
    )
    parser.add_argument(
        "--schools",
        default="",
        help="Optional comma-separated school names to scope ingestion.",
    )
    parser.add_argument(
        "--schools-file",
        default="",
        help="Optional text file (one school per line) for scope.",
    )
    parser.add_argument(
        "--output-dir",
        default=".benchmarks/official_phase4",
        help="Directory to write run report JSON.",
    )
    parser.add_argument(
        "--run-id",
        default="",
        help="Optional run id; default uses UTC timestamp.",
    )
    return parser


def _load_school_scope(args: argparse.Namespace) -> list[str]:
    names: list[str] = []
    if args.schools:
        names.extend([piece.strip() for piece in str(args.schools).split(",") if piece.strip()])
    if args.schools_file:
        path = Path(str(args.schools_file))
        if path.exists():
            names.extend([line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()])
    deduped: list[str] = []
    seen: set[str] = set()
    for name in names:
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(name)
    return deduped


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    run_id = args.run_id or f"causal-ipeds-program-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}"
    award_levels = [
        piece.strip()
        for piece in str(args.award_levels or "").split(",")
        if piece.strip()
    ]
    school_scope = _load_school_scope(args)

    async with async_session_factory() as session:
        result = await ingest_ipeds_program_facts(
            session,
            run_id=run_id,
            years=int(args.years),
            min_completions=int(args.min_completions),
            award_levels=award_levels,
            school_names=school_scope or None,
        )
        await session.commit()

    out_dir = Path(args.output_dir) / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "ipeds_program_facts_report.json"
    report_payload = {
        "run_id": run_id,
        "config": {
            "years": int(args.years),
            "min_completions": int(args.min_completions),
            "award_levels": award_levels,
            "school_scope_size": len(school_scope),
        },
        "result": result,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    report_path.write_text(json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return report_payload


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    payload = asyncio.run(_run(args))
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

