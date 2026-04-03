"""Build causal dataset version from current outcome assets."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone

from scholarpath.services.causal_data_service import build_dataset_version, run_mini_gate
from scholarpath.db.session import async_session_factory


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build causal dataset version + optional mini gate.")
    parser.add_argument(
        "--version",
        default="",
        help="Dataset version id (default auto timestamp).",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=540,
        help="Lookback window for outcome rows (default: 540).",
    )
    parser.add_argument(
        "--include-proxy",
        dest="include_proxy",
        action="store_true",
        default=True,
        help="Include proxy labels in dataset stats (default: true).",
    )
    parser.add_argument(
        "--no-include-proxy",
        dest="include_proxy",
        action="store_false",
        help="Exclude proxy labels.",
    )
    parser.add_argument(
        "--min-true-per-outcome",
        type=int,
        default=100,
        help="Minimum true labels required per outcome.",
    )
    parser.add_argument(
        "--active-outcomes",
        default="",
        help=(
            "Comma-separated active outcome names. Empty means all outcomes; "
            "use admission_probability for true-only admission training."
        ),
    )
    parser.add_argument(
        "--mini-gate",
        dest="mini_gate",
        action="store_true",
        default=True,
        help="Run mini gate after dataset build (default: true).",
    )
    parser.add_argument(
        "--no-mini-gate",
        dest="mini_gate",
        action="store_false",
        help="Skip mini gate.",
    )
    parser.add_argument(
        "--schema-valid-rate",
        type=float,
        default=1.0,
    )
    parser.add_argument(
        "--extraction-success",
        type=float,
        default=1.0,
    )
    parser.add_argument(
        "--unresolved-conflict-rate",
        type=float,
        default=0.0,
    )
    parser.add_argument(
        "--quarantine-rate",
        type=float,
        default=0.0,
    )
    parser.add_argument(
        "--rpm-actual-avg",
        type=float,
        default=0.0,
    )
    parser.add_argument(
        "--rate-limit-error-count",
        type=int,
        default=0,
    )
    return parser


async def _run(args: argparse.Namespace) -> dict:
    version = args.version or f"causal-dataset-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}"
    active_outcomes = [
        item.strip()
        for item in str(args.active_outcomes or "").split(",")
        if item.strip()
    ]
    async with async_session_factory() as session:
        dataset = await build_dataset_version(
            session,
            version=version,
            lookback_days=args.lookback_days,
            include_proxy=args.include_proxy,
            min_true_per_outcome=args.min_true_per_outcome,
            active_outcomes=active_outcomes or None,
        )
        gate = None
        if args.mini_gate:
            gate = await run_mini_gate(
                session,
                run_id=f"dataset-mini-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}",
                dataset_version=version,
                metrics={
                    "schema_valid_rate": args.schema_valid_rate,
                    "extraction_success": args.extraction_success,
                    "unresolved_conflict_rate": args.unresolved_conflict_rate,
                    "quarantine_rate": args.quarantine_rate,
                    "rpm_actual_avg": args.rpm_actual_avg,
                    "rate_limit_error_count": args.rate_limit_error_count,
                },
            )
        await session.commit()
    payload = {"dataset": dataset, "mini_gate": gate}
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return payload


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
