"""CLI entrypoint for phase-4 strict true-only multi-outcome training prep."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from scholarpath.db.session import async_session_factory
from scholarpath.services.admission_data_phase4_service import run_phase4_training_prep


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Prepare strict true-only multi-outcome labels from official school-year facts "
            "for staged causal training."
        )
    )
    parser.add_argument("--run-id", default="", help="Optional run id (auto-generated when empty).")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=540,
        help="Lookback window for snapshots/outcomes (default: 540).",
    )
    parser.add_argument(
        "--target-eligible-snapshots",
        type=int,
        default=3500,
        help="Auto scope target for eligible snapshots (default: 3500).",
    )
    parser.add_argument(
        "--schools",
        default="",
        help="Legacy explicit schools list. Use '|' as delimiter when names contain commas.",
    )
    parser.add_argument(
        "--school",
        action="append",
        default=[],
        help="Repeatable explicit school name.",
    )
    parser.add_argument(
        "--cycle-year",
        type=int,
        default=datetime.now(timezone.utc).year,
        help="Cycle year for official completions fallback (default: current year).",
    )
    parser.add_argument(
        "--ingest-official-facts",
        dest="ingest_official_facts",
        action="store_true",
        default=True,
        help="Refresh official structured facts before materialization (default: true).",
    )
    parser.add_argument(
        "--no-ingest-official-facts",
        dest="ingest_official_facts",
        action="store_false",
        help="Skip official facts refresh.",
    )
    parser.add_argument(
        "--ingest-ipeds-completions",
        dest="ingest_ipeds_completions",
        action="store_true",
        default=True,
        help="Ingest IPEDS completions dataset for doctoral_completions_share (default: true).",
    )
    parser.add_argument(
        "--no-ingest-ipeds-completions",
        dest="ingest_ipeds_completions",
        action="store_false",
        help="Skip IPEDS completions ingestion.",
    )
    parser.add_argument(
        "--school-concurrency-initial",
        type=int,
        default=6,
        help="Initial school-level concurrency for official ingestion.",
    )
    parser.add_argument(
        "--school-concurrency-max",
        type=int,
        default=20,
        help="Max school-level concurrency for official ingestion.",
    )
    parser.add_argument(
        "--target-rpm-total",
        type=float,
        default=180.0,
        help="Total RPM target for helper LLM usage (default: 180).",
    )
    parser.add_argument(
        "--rpm-band-low",
        type=float,
        default=170.0,
        help="Lower RPM band for official ingestion controller.",
    )
    parser.add_argument(
        "--rpm-band-high",
        type=float,
        default=185.0,
        help="Upper RPM band for official ingestion controller.",
    )
    parser.add_argument(
        "--max-auto-schools",
        type=int,
        default=500,
        help="Safety cap for auto-selected schools by snapshot density.",
    )
    parser.add_argument(
        "--output-dir",
        default=".benchmarks/official_phase4",
        help="Output directory for phase4 report artifacts.",
    )
    return parser


def _parse_explicit_schools(args: argparse.Namespace) -> list[str]:
    parsed: list[str] = []
    for item in list(getattr(args, "school", []) or []):
        text = str(item or "").strip()
        if text:
            parsed.append(text)

    raw = str(getattr(args, "schools", "") or "").strip()
    if not raw:
        return parsed

    delimiter = "|" if "|" in raw else ","
    parsed.extend([item.strip() for item in raw.split(delimiter) if item.strip()])
    return parsed


async def _run(args: argparse.Namespace) -> int:
    run_id = args.run_id or f"admission-phase4-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}-{uuid4().hex[:6]}"
    schools = _parse_explicit_schools(args)

    async with async_session_factory() as session:
        payload = await run_phase4_training_prep(
            session,
            run_id=run_id,
            output_dir=str(args.output_dir),
            lookback_days=int(args.lookback_days),
            target_eligible_snapshots=int(args.target_eligible_snapshots),
            school_names=schools or None,
            cycle_year=int(args.cycle_year),
            ingest_official_facts_enabled=bool(args.ingest_official_facts),
            ingest_ipeds_completions_enabled=bool(args.ingest_ipeds_completions),
            school_concurrency_initial=int(args.school_concurrency_initial),
            school_concurrency_max=int(args.school_concurrency_max),
            target_rpm_total=float(args.target_rpm_total),
            rpm_band_low=float(args.rpm_band_low),
            rpm_band_high=float(args.rpm_band_high),
            max_auto_schools=int(args.max_auto_schools),
        )
        await session.commit()

    run_root = Path(args.output_dir).expanduser().resolve() / run_id
    run_root.mkdir(parents=True, exist_ok=True)
    report_json = run_root / "stage_readiness.json"
    report_md = run_root / "stage_readiness.md"

    report_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    stage = payload.get("stage1_readiness") or {}
    mat = payload.get("materialization") or {}
    strict = payload.get("strict_true_only") or {}
    school_scope = payload.get("school_scope") or {}
    quality = payload.get("quality_gate") or {}

    lines = [
        f"# Phase4 Training Prep `{run_id}`",
        "",
        f"- status: `{payload.get('status')}`",
        f"- selected_schools: `{int(school_scope.get('selected_schools_count') or 0)}`",
        f"- selected_snapshots_raw: `{int(school_scope.get('selected_snapshots_raw') or 0)}`",
        f"- eligible_snapshots: `{int(mat.get('eligible_snapshots') or 0)}`",
        f"- created_non_admission_true: `{int(mat.get('created') or 0)}`",
        f"- deduped_non_admission_true: `{int(mat.get('deduped') or 0)}`",
        f"- strict_true_only_passed: `{bool(strict.get('passed'))}`",
        f"- stage1_data_gate_passed: `{bool(stage.get('passed'))}`",
        f"- quality_gate_passed: `{bool(quality.get('passed'))}`",
        f"- quality_gate_reasons: `{', '.join(quality.get('reasons') or []) or 'none'}`",
        "",
        "## Stage1 Data Gate Reasons",
        f"- `{', '.join(stage.get('reasons') or []) or 'none'}`",
        "",
        "## Next",
        f"- `{payload.get('next_command')}`",
    ]
    report_md.write_text("\n".join(lines), encoding="utf-8")

    payload["artifacts"] = {
        "stage_readiness_json": str(report_json),
        "stage_readiness_md": str(report_md),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))

    return 0 if bool(stage.get("passed")) and bool(quality.get("passed")) else 2


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
