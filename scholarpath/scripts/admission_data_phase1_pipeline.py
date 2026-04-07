"""CLI entrypoint for admission data phase-1 pipeline."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from uuid import uuid4

from scholarpath.db.session import async_session_factory
from scholarpath.services.admission_data_phase1_service import run_admission_phase1_pipeline


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run phase-1 admissions data pipeline (Scorecard + IPEDS) into Bronze/Silver "
            "tables with rule cleaning and helper-LLM judge."
        )
    )
    parser.add_argument(
        "--run-id",
        default="",
        help="Optional run id. Auto-generated when empty.",
    )
    parser.add_argument(
        "--scope",
        default="existing_65",
        choices=["existing_65", "all"],
        help="School scope. existing_65 targets current in-db school set.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Compute/report only; do not persist Bronze/Silver rows.",
    )
    parser.add_argument(
        "--resume-run-id",
        default="",
        help="Optional prior run id for resume traceability metadata.",
    )
    parser.add_argument(
        "--metric-year",
        type=int,
        default=datetime.now(timezone.utc).year,
        help="Metric year used when source row does not provide an explicit year.",
    )
    parser.add_argument(
        "--output-dir",
        default=".benchmarks/official_phase1",
        help="Run report output directory.",
    )
    parser.add_argument(
        "--no-gate",
        action="store_true",
        default=False,
        help="Disable default phase1 closure gate checks.",
    )
    parser.add_argument(
        "--gate-min-admit-rate-coverage",
        type=float,
        default=0.95,
        help="Minimum school coverage for admit_rate required by gate.",
    )
    parser.add_argument(
        "--gate-min-net-price-coverage",
        type=float,
        default=0.95,
        help="Minimum school coverage for avg_net_price required by gate.",
    )
    return parser


async def _run(args: argparse.Namespace) -> int:
    run_id = args.run_id or f"admission-phase1-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}-{uuid4().hex[:6]}"
    async with async_session_factory() as session:
        payload = await run_admission_phase1_pipeline(
            session,
            run_id=run_id,
            scope=args.scope,
            dry_run=bool(args.dry_run),
            resume_run_id=(args.resume_run_id or "").strip() or None,
            metric_year=int(args.metric_year),
            output_dir=args.output_dir,
            run_gate=not bool(args.no_gate),
            min_admit_rate_coverage=float(args.gate_min_admit_rate_coverage),
            min_net_price_coverage=float(args.gate_min_net_price_coverage),
        )
        if not args.dry_run:
            await session.commit()
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    if not args.no_gate and not bool(payload.get("gate", {}).get("passed", False)):
        return 2
    return 0


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
