"""Backfill real-only causal training assets from existing business data.

This script keeps the real-truth path explicit:
- build ``CausalFeatureSnapshot`` rows from real student/school/offer/event data;
- create ``CausalOutcomeEvent`` only from admission truth stages;
- optionally ingest CSV/JSON admission histories for future expansion.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import func, select

from scholarpath.db.models import (
    AdmissionEvent,
    CausalDatasetVersion,
    CausalFeatureSnapshot,
    CausalOutcomeEvent,
    School,
    Student,
)
from scholarpath.db.session import async_session_factory
from scholarpath.services.causal_real_asset_service import backfill_real_admission_assets
from scholarpath.services.causal_data_service import (
    ingest_common_app_trends,
    ingest_ipeds_school_pool,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill real-only causal assets and build an admission-only dataset version.",
    )
    parser.add_argument("--student-ids", default="", help="Comma-separated student UUIDs.")
    parser.add_argument("--student-names", default="", help="Comma-separated student names.")
    parser.add_argument("--school-ids", default="", help="Comma-separated school UUIDs.")
    parser.add_argument("--school-names", default="", help="Comma-separated school names.")
    parser.add_argument(
        "--import-file",
        default="",
        help="Optional CSV/JSON file of admission history rows to ingest.",
    )
    parser.add_argument(
        "--import-format",
        default="auto",
        choices=["auto", "csv", "json"],
        help="Import file format (default: auto-detect).",
    )
    parser.add_argument(
        "--active-outcomes",
        default="admission_probability",
        help="Comma-separated active outcomes for the dataset version.",
    )
    parser.add_argument(
        "--cycle-year",
        type=int,
        default=datetime.now(timezone.utc).year,
        help="Default cycle year for official facts / imported rows.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=540,
        help="Dataset lookback window in days.",
    )
    parser.add_argument(
        "--min-true-per-outcome",
        type=int,
        default=1,
        help="Minimum true labels required per active outcome.",
    )
    parser.add_argument(
        "--dataset-version",
        default="",
        help="Optional dataset version id (default auto timestamp).",
    )
    parser.add_argument(
        "--run-id",
        default="",
        help="Optional run id for metadata and traceability.",
    )
    parser.add_argument(
        "--include-school-evaluations",
        dest="include_school_evaluations",
        action="store_true",
        default=True,
        help="Use existing school_evaluations as pair discovery (default: true).",
    )
    parser.add_argument(
        "--no-include-school-evaluations",
        dest="include_school_evaluations",
        action="store_false",
        help="Skip school_evaluations.",
    )
    parser.add_argument(
        "--include-offers",
        dest="include_offers",
        action="store_true",
        default=True,
        help="Use existing offers as truth sources (default: true).",
    )
    parser.add_argument(
        "--no-include-offers",
        dest="include_offers",
        action="store_false",
        help="Skip offers.",
    )
    parser.add_argument(
        "--include-admission-events",
        dest="include_admission_events",
        action="store_true",
        default=True,
        help="Use existing admission_events as truth sources (default: true).",
    )
    parser.add_argument(
        "--no-include-admission-events",
        dest="include_admission_events",
        action="store_false",
        help="Skip admission events.",
    )
    parser.add_argument(
        "--ingest-official-facts",
        dest="ingest_official_facts",
        action="store_true",
        default=True,
        help="Refresh official admission facts before building snapshots (default: true).",
    )
    parser.add_argument(
        "--no-ingest-official-facts",
        dest="ingest_official_facts",
        action="store_false",
        help="Skip official admission fact refresh.",
    )
    parser.add_argument(
        "--ingest-ipeds",
        dest="ingest_ipeds",
        action="store_true",
        default=True,
        help="Ingest IPEDS/College Navigator top-school pool (default: true).",
    )
    parser.add_argument(
        "--no-ingest-ipeds",
        dest="ingest_ipeds",
        action="store_false",
        help="Skip IPEDS/College Navigator pool ingestion.",
    )
    parser.add_argument(
        "--ingest-common-app-trends",
        dest="ingest_common_app_trends",
        action="store_true",
        default=True,
        help="Ingest Common App trend-only signals (default: true).",
    )
    parser.add_argument(
        "--no-ingest-common-app-trends",
        dest="ingest_common_app_trends",
        action="store_false",
        help="Skip Common App trend-only ingestion.",
    )
    parser.add_argument(
        "--top-schools",
        type=int,
        default=1000,
        help="Top N schools selected from IPEDS bulk data (default: 1000).",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=5,
        help="Lookback years for IPEDS/Common App expansions (default: 5).",
    )
    parser.add_argument(
        "--school-selection",
        default="applicants",
        choices=["applicants", "enrollment"],
        help="Top school selector for IPEDS expansion (default: applicants).",
    )
    parser.add_argument(
        "--build-dataset",
        dest="build_dataset",
        action="store_true",
        default=True,
        help="Build an admission-only dataset version after backfill (default: true).",
    )
    parser.add_argument(
        "--no-build-dataset",
        dest="build_dataset",
        action="store_false",
        help="Skip dataset version build.",
    )
    return parser


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    student_ids = [item.strip() for item in str(args.student_ids or "").split(",") if item.strip()]
    student_names = [item.strip() for item in str(args.student_names or "").split(",") if item.strip()]
    school_ids = [item.strip() for item in str(args.school_ids or "").split(",") if item.strip()]
    school_names = [item.strip() for item in str(args.school_names or "").split(",") if item.strip()]
    active_outcomes = [item.strip() for item in str(args.active_outcomes or "").split(",") if item.strip()]
    run_id = args.run_id or f"causal-backfill-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}"

    import_rows = await _load_import_rows(args.import_file, args.import_format)

    async with async_session_factory() as session:
        resolved_student_ids = await _resolve_student_ids(session, student_ids, student_names)
        ipeds_result: dict[str, Any] | None = None
        common_app_trend_result: dict[str, Any] | None = None

        if args.ingest_ipeds:
            ipeds_result = await ingest_ipeds_school_pool(
                session,
                run_id=f"{run_id}:ipeds-pool",
                top_schools=args.top_schools,
                years=args.years,
                selection_metric=(
                    "applicants_total"
                    if str(args.school_selection).strip().lower() == "applicants"
                    else "enrollment"
                ),
            )

        if args.ingest_common_app_trends:
            common_app_trend_result = await ingest_common_app_trends(
                session,
                run_id=f"{run_id}:common-app-trends",
                years=args.years,
            )

        school_name_scope = list(school_names)
        if (
            not school_ids
            and not school_names
            and ipeds_result is not None
            and isinstance(ipeds_result.get("school_names"), list)
        ):
            school_name_scope = [str(item).strip() for item in ipeds_result["school_names"] if str(item).strip()]

        resolved_school_ids = await _resolve_school_ids(session, school_ids, school_name_scope)

        result = await backfill_real_admission_assets(
            session,
            run_id=run_id,
            student_ids=resolved_student_ids or None,
            school_ids=resolved_school_ids or None,
            import_rows=import_rows,
            include_school_evaluations=args.include_school_evaluations,
            include_offers=args.include_offers,
            include_admission_events=args.include_admission_events,
            ingest_official_facts_enabled=args.ingest_official_facts,
            cycle_year=args.cycle_year,
            active_outcomes=active_outcomes or None,
            lookback_days=args.lookback_days,
            min_true_per_outcome=args.min_true_per_outcome,
            build_dataset=args.build_dataset,
            dataset_version=args.dataset_version or None,
        )
        await session.commit()

        counts = await _count_training_assets(session)

    truth_ratio = {}
    dataset_result = result.get("dataset_result") or {}
    if isinstance(dataset_result, dict):
        truth_ratio = dict(dataset_result.get("truth_ratio_by_outcome") or {})

    mini_gate = {
        "passed": (
            counts["causal_feature_snapshots"] > 0
            and counts["causal_outcome_events"] > 0
            and counts["proxy_outcome_events"] == 0
            and truth_ratio.get("admission_probability", 0.0) == 1.0
        ),
        "checks": {
            "causal_feature_snapshots_gt_0": counts["causal_feature_snapshots"] > 0,
            "causal_outcome_events_gt_0": counts["causal_outcome_events"] > 0,
            "proxy_outcome_events_eq_0": counts["proxy_outcome_events"] == 0,
            "admission_truth_ratio_eq_1": truth_ratio.get("admission_probability", 0.0) == 1.0,
        },
    }

    payload = {
        "status": "ok" if mini_gate["passed"] else "watch",
        "run_id": run_id,
        "request": {
            "student_ids": resolved_student_ids,
            "student_names": student_names,
            "school_ids": resolved_school_ids,
            "school_names": school_names,
            "import_file": args.import_file or None,
            "import_format": args.import_format,
            "active_outcomes": active_outcomes,
            "cycle_year": args.cycle_year,
            "lookback_days": args.lookback_days,
            "min_true_per_outcome": args.min_true_per_outcome,
            "build_dataset": args.build_dataset,
            "ingest_official_facts": args.ingest_official_facts,
            "ingest_ipeds": args.ingest_ipeds,
            "ingest_common_app_trends": args.ingest_common_app_trends,
            "top_schools": args.top_schools,
            "years": args.years,
            "school_selection": args.school_selection,
        },
        "backfill_result": result,
        "ipeds_pool_result": ipeds_result,
        "common_app_trend_result": common_app_trend_result,
        "database_counts": counts,
        "dataset_result": dataset_result,
        "mini_gate": mini_gate,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return payload


async def _count_training_assets(session) -> dict[str, int]:
    counts = {
        "students": int((await session.scalar(select(func.count()).select_from(Student))) or 0),
        "schools": int((await session.scalar(select(func.count()).select_from(School))) or 0),
        "admission_events": int((await session.scalar(select(func.count()).select_from(AdmissionEvent))) or 0),
        "causal_feature_snapshots": int((await session.scalar(select(func.count()).select_from(CausalFeatureSnapshot))) or 0),
        "causal_outcome_events": int((await session.scalar(select(func.count()).select_from(CausalOutcomeEvent))) or 0),
        "causal_dataset_versions": int((await session.scalar(select(func.count()).select_from(CausalDatasetVersion))) or 0),
    }
    proxy_outcome_events = int(
        (
            await session.scalar(
                select(func.count()).select_from(CausalOutcomeEvent).where(CausalOutcomeEvent.label_type != "true")
            )
        )
        or 0
    )
    counts["proxy_outcome_events"] = proxy_outcome_events
    return counts


async def _resolve_student_ids(
    session,
    student_ids: list[str],
    student_names: list[str],
) -> list[str]:
    resolved: list[str] = []
    if student_ids:
        resolved.extend(student_ids)
    if student_names:
        rows = (
            (
                await session.execute(
                    select(Student).where(func.lower(Student.name).in_([name.lower() for name in student_names]))
                )
            )
            .scalars()
            .all()
        )
        resolved.extend(str(row.id) for row in rows)
    if not resolved:
        rows = (await session.execute(select(Student))).scalars().all()
        resolved.extend(str(row.id) for row in rows)
    return list(dict.fromkeys(resolved))


async def _resolve_school_ids(
    session,
    school_ids: list[str],
    school_names: list[str],
) -> list[str]:
    resolved: list[str] = []
    if school_ids:
        resolved.extend(school_ids)
    if school_names:
        rows = (
            (
                await session.execute(
                    select(School).where(func.lower(School.name).in_([name.lower() for name in school_names]))
                )
            )
            .scalars()
            .all()
        )
        resolved.extend(str(row.id) for row in rows)
    if not resolved:
        rows = (await session.execute(select(School))).scalars().all()
        resolved.extend(str(row.id) for row in rows)
    return list(dict.fromkeys(resolved))


async def _load_import_rows(path_value: str, import_format: str) -> list[dict[str, Any]]:
    if not path_value:
        return []
    path = Path(path_value)
    if not path.exists():
        raise FileNotFoundError(path)

    fmt = import_format
    if fmt == "auto":
        suffix = path.suffix.lower()
        fmt = "csv" if suffix == ".csv" else "json"

    rows: list[dict[str, Any]] = []
    if fmt == "csv":
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                if isinstance(row, dict):
                    rows.append(row)
    else:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            source_rows = payload
        elif isinstance(payload, dict):
            source_rows = payload.get("rows") or payload.get("events") or payload.get("items") or []
        else:
            source_rows = []
        for row in source_rows:
            if isinstance(row, dict):
                rows.append(row)
    return rows


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
