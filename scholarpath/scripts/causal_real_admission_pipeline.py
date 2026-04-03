"""Execute real-admission causal pipeline with strict mini-before-full gating."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import select

from scholarpath.db.models import TokenUsage
from scholarpath.db.session import async_session_factory
from scholarpath.scripts.causal_staged_train import _run as run_staged_train
from scholarpath.services.causal_data_service import (
    build_dataset_version,
    ingest_common_app_trends,
    ingest_ipeds_school_pool,
    ingest_official_facts,
    register_admission_event,
    run_mini_gate,
)

_REQUIRED_SERVICES = (
    "app",
    "postgres",
    "redis",
    "celery_worker",
    "celery_causal_train_worker",
    "celery_beat",
)
_REQUIRED_TABLES = (
    "admission_events",
    "evidence_artifacts",
    "canonical_facts",
    "fact_lineage",
    "causal_dataset_versions",
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Gate0 -> mini gate -> full K=3 for real-admission causal pipeline.",
    )
    parser.add_argument(
        "--schools",
        default="",
        help="Comma-separated school names for official-fact ingestion.",
    )
    parser.add_argument(
        "--schools-file",
        default="",
        help="Optional text file with one school name per line.",
    )
    parser.add_argument(
        "--ingest-ipeds",
        action="store_true",
        default=True,
        help="Seed and expand school pool from IPEDS/College Navigator (default: true).",
    )
    parser.add_argument(
        "--no-ingest-ipeds",
        dest="ingest_ipeds",
        action="store_false",
        help="Skip IPEDS/College Navigator school pool ingestion.",
    )
    parser.add_argument(
        "--ingest-common-app-trends",
        action="store_true",
        default=True,
        help="Ingest Common App trend-only signals (default: true).",
    )
    parser.add_argument(
        "--no-ingest-common-app-trends",
        dest="ingest_common_app_trends",
        action="store_false",
        help="Skip Common App trend ingestion.",
    )
    parser.add_argument(
        "--top-schools",
        type=int,
        default=1000,
        help="Top N schools selected from IPEDS by school-selection (default: 1000).",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=5,
        help="Lookback years for IPEDS/Common App ingestion (default: 5).",
    )
    parser.add_argument(
        "--school-selection",
        default="applicants",
        choices=["applicants", "enrollment"],
        help="Top school selector metric (default: applicants).",
    )
    parser.add_argument(
        "--events-file",
        default="",
        help="Optional JSON file with admission event rows.",
    )
    parser.add_argument(
        "--cycle-year",
        type=int,
        default=datetime.now(timezone.utc).year,
        help="Cycle year used for official facts.",
    )
    parser.add_argument(
        "--dataset-version",
        default="",
        help="Dataset version id (default auto timestamp).",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=540,
        help="Dataset lookback window days.",
    )
    parser.add_argument(
        "--min-true-per-outcome",
        type=int,
        default=100,
        help="Minimum true labels required per outcome.",
    )
    parser.add_argument(
        "--max-rpm-total",
        type=int,
        default=180,
        help="RPM cap for full stage evaluation (must be <=200).",
    )
    parser.add_argument(
        "--judge-concurrency",
        type=int,
        default=2,
        help="Judge concurrency for full stage eval.",
    )
    parser.add_argument(
        "--full-candidates",
        type=int,
        default=3,
        help="Number of stage4 candidates for full run.",
    )
    parser.add_argument(
        "--output-dir",
        default=".benchmarks/causal_staged",
        help="Output root for stage4 artifacts.",
    )
    parser.add_argument(
        "--run-id",
        default="",
        help="Optional pipeline run id.",
    )
    return parser


def _shell(cmd: list[str]) -> tuple[int, str, str]:
    completed = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.returncode, completed.stdout.strip(), completed.stderr.strip()


def _extract_revision(text: str) -> str | None:
    match = re.search(r"\b([0-9a-f]{12,})\b", text.lower())
    return match.group(1) if match else None


def run_gate0_checks() -> dict[str, Any]:
    checks: dict[str, Any] = {}
    ok = True

    code, out, err = _shell(["docker", "compose", "ps", "--format", "json"])
    checks["docker_compose_ps"] = {
        "ok": code == 0,
        "stdout": out,
        "stderr": err,
    }
    if code != 0:
        return {"ok": False, "checks": checks}

    service_state: dict[str, str] = {}
    if out:
        try:
            parsed = json.loads(out)
            rows = parsed if isinstance(parsed, list) else [parsed]
            for row in rows:
                service = str(row.get("Service") or "")
                state = str(row.get("State") or "")
                if service:
                    service_state[service] = state
        except json.JSONDecodeError:
            for line in out.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                service = str(row.get("Service") or "")
                state = str(row.get("State") or "")
                if service:
                    service_state[service] = state
    missing_or_down = [
        service
        for service in _REQUIRED_SERVICES
        if service_state.get(service, "").lower() != "running"
    ]
    checks["services_running"] = {
        "ok": len(missing_or_down) == 0,
        "service_state": service_state,
        "missing_or_down": missing_or_down,
    }
    ok = ok and len(missing_or_down) == 0

    code_cur, out_cur, err_cur = _shell(
        ["docker", "compose", "exec", "-T", "app", "alembic", "current"],
    )
    code_head, out_head, err_head = _shell(
        ["docker", "compose", "exec", "-T", "app", "alembic", "heads"],
    )
    rev_cur = _extract_revision(out_cur) if code_cur == 0 else None
    rev_head = _extract_revision(out_head) if code_head == 0 else None
    revision_ok = bool(rev_cur and rev_head and rev_cur == rev_head)
    checks["alembic_revision"] = {
        "ok": revision_ok,
        "current": rev_cur,
        "head": rev_head,
        "current_raw": out_cur,
        "head_raw": out_head,
        "current_error": err_cur,
        "head_error": err_head,
    }
    ok = ok and revision_ok

    code_tbl, out_tbl, err_tbl = _shell(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "postgres",
            "psql",
            "-U",
            "scholarpath",
            "-d",
            "scholarpath",
            "-At",
            "-c",
            "select tablename from pg_tables "
            "where schemaname='public' and tablename in "
            "('admission_events','evidence_artifacts','canonical_facts','fact_lineage','causal_dataset_versions') "
            "order by tablename;",
        ]
    )
    tables = [line.strip() for line in out_tbl.splitlines() if line.strip()]
    missing_tables = [table for table in _REQUIRED_TABLES if table not in tables]
    checks["required_tables"] = {
        "ok": code_tbl == 0 and len(missing_tables) == 0,
        "tables_found": tables,
        "missing_tables": missing_tables,
        "stderr": err_tbl,
    }
    ok = ok and code_tbl == 0 and len(missing_tables) == 0

    return {"ok": ok, "checks": checks}


def _load_school_names(args: argparse.Namespace, *, allow_empty: bool = False) -> list[str]:
    names: list[str] = []
    if args.schools:
        names.extend([piece.strip() for piece in args.schools.split(",") if piece.strip()])
    if args.schools_file:
        path = Path(args.schools_file)
        lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        names.extend(lines)
    unique = list(dict.fromkeys(names))
    if not unique and not allow_empty:
        raise ValueError("at least one school name is required (--schools or --schools-file)")
    return unique


def _load_events(path_value: str) -> list[dict[str, Any]]:
    if not path_value:
        return []
    path = Path(path_value)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("events file must be a JSON array")
    events: list[dict[str, Any]] = []
    for row in payload:
        if isinstance(row, dict):
            events.append(row)
    return events


async def _collect_usage_metrics(*, run_id: str) -> dict[str, Any]:
    pattern = f"%#{run_id}"
    async with async_session_factory() as session:
        rows = (
            (
                await session.execute(
                    select(TokenUsage).where(TokenUsage.caller.ilike(pattern))
                )
            )
            .scalars()
            .all()
        )

    if not rows:
        return {"rpm_actual_avg": 0.0, "rate_limit_error_count": 0}

    timestamps = [row.created_at for row in rows if row.created_at is not None]
    if not timestamps:
        return {"rpm_actual_avg": 0.0, "rate_limit_error_count": 0}

    start = min(timestamps)
    end = max(timestamps)
    duration_minutes = max((end - start).total_seconds() / 60.0, 1.0 / 60.0)
    rpm_actual_avg = round(len(rows) / duration_minutes, 2)
    rate_limit_errors = 0
    for row in rows:
        err_text = str(row.error or "").lower()
        if "rate limit" in err_text or "429" in err_text:
            rate_limit_errors += 1
    return {
        "rpm_actual_avg": rpm_actual_avg,
        "rate_limit_error_count": rate_limit_errors,
    }


async def _run_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    if args.max_rpm_total > 200:
        raise ValueError("max-rpm-total must be <= 200")

    gate0 = run_gate0_checks()
    if not gate0["ok"]:
        return {
            "status": "blocked",
            "stage": "gate0",
            "gate0": gate0,
        }

    run_id = args.run_id or f"causal-real-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}-{uuid4().hex[:6]}"
    dataset_version = args.dataset_version or f"causal-dataset-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}"
    school_names = _load_school_names(args, allow_empty=bool(args.ingest_ipeds))
    events = _load_events(args.events_file)

    async with async_session_factory() as session:
        ipeds_result: dict[str, Any] | None = None
        common_app_result: dict[str, Any] | None = None
        if args.ingest_ipeds:
            ipeds_result = await ingest_ipeds_school_pool(
                session,
                run_id=f"{run_id}:ipeds",
                top_schools=args.top_schools,
                years=args.years,
                selection_metric=(
                    "applicants_total"
                    if str(args.school_selection).strip().lower() == "applicants"
                    else "enrollment"
                ),
            )
            if not school_names:
                school_names = [str(name).strip() for name in (ipeds_result.get("school_names") or []) if str(name).strip()]
        if args.ingest_common_app_trends:
            common_app_result = await ingest_common_app_trends(
                session,
                run_id=f"{run_id}:common-app",
                years=args.years,
            )

        if not school_names:
            return {
                "status": "blocked",
                "stage": "school_scope",
                "reason": "no_school_scope_after_ipeds_seed",
                "ipeds_result": ipeds_result,
            }

        ingest_result = await ingest_official_facts(
            session,
            school_names=school_names,
            cycle_year=args.cycle_year,
            run_id=run_id,
        )

        inserted_events = 0
        event_errors: list[dict[str, str]] = []
        for event in events:
            try:
                await register_admission_event(
                    session,
                    student_id=str(event["student_id"]),
                    school_id=str(event["school_id"]),
                    cycle_year=int(event.get("cycle_year") or args.cycle_year),
                    major_bucket=event.get("major_bucket"),
                    stage=str(event["stage"]),
                    happened_at=event.get("happened_at"),
                    evidence_ref=str(event["evidence_ref"]) if event.get("evidence_ref") else None,
                    source_name=str(event.get("source_name") or "batch"),
                    metadata=event.get("metadata"),
                )
                inserted_events += 1
            except Exception as exc:
                event_errors.append({"event": str(event), "error": str(exc)})

        dataset_result = await build_dataset_version(
            session,
            version=dataset_version,
            lookback_days=args.lookback_days,
            include_proxy=False,
            min_true_per_outcome=args.min_true_per_outcome,
            active_outcomes=["admission_probability"],
        )

        usage_metrics = await _collect_usage_metrics(run_id=run_id)
        raw_facts = int(ingest_result.get("raw_facts", 0) or 0)
        gate_metrics = {
            "schema_valid_rate": float(ingest_result.get("schema_valid_rate", 0.0)),
            "extraction_success": float(ingest_result.get("extraction_success", 0.0)),
            "unresolved_conflict_rate": float(ingest_result.get("unresolved_conflict_rate", 1.0)),
            "quarantine_rate": float(ingest_result.get("quarantine_rate", 1.0)),
            "evidence_coverage_rate": 1.0 if raw_facts > 0 else 0.0,
            "rpm_actual_avg": float(usage_metrics.get("rpm_actual_avg", 0.0)),
            "rate_limit_error_count": int(usage_metrics.get("rate_limit_error_count", 0)),
            "official_source_coverage_rate": float(ingest_result.get("official_source_coverage_rate", 0.0)),
            "external_id_match_rate": float(ingest_result.get("external_id_match_rate", 0.0)),
            "trend_coverage_rate": float((common_app_result or {}).get("trend_coverage_rate", 0.0)),
        }
        mini_gate = await run_mini_gate(
            session,
            run_id=f"mini-{run_id}",
            dataset_version=dataset_version,
            metrics=gate_metrics,
        )
        await session.commit()

    if not mini_gate.get("mini_gate_passed"):
        return {
            "status": "blocked",
            "stage": "mini_gate",
            "run_id": run_id,
            "dataset_version": dataset_version,
            "ingest_result": ingest_result,
            "event_ingest": {
                "inserted": inserted_events,
                "errors": event_errors,
            },
            "dataset_result": dataset_result,
            "mini_gate": mini_gate,
            "ipeds_result": ipeds_result,
            "common_app_result": common_app_result,
        }

    staged_args = argparse.Namespace(
        stage="4",
        train_candidates_per_stage=args.full_candidates,
        max_rpm_total=args.max_rpm_total,
        judge_concurrency=args.judge_concurrency,
        promote_on_final_pass=False,
        output_dir=args.output_dir,
    )
    full_result = await run_staged_train(staged_args)
    stage4 = full_result.get("stage_summary", {}).get("stage_4", {})
    champion = stage4.get("champion") if isinstance(stage4, dict) else None

    return {
        "status": "ok",
        "run_id": run_id,
        "dataset_version": dataset_version,
        "gate0": gate0,
        "ingest_result": ingest_result,
        "event_ingest": {
            "inserted": inserted_events,
            "errors": event_errors,
        },
        "dataset_result": dataset_result,
        "mini_gate": mini_gate,
        "ipeds_result": ipeds_result,
        "common_app_result": common_app_result,
        "full_run": {
            "run_id": full_result.get("run_id"),
            "passed": bool(stage4.get("passed")) if isinstance(stage4, dict) else False,
            "champion": champion,
            "gate_results": full_result.get("gate_results", {}).get("stage_4", {}),
            "promotion_decision": full_result.get("promotion_decision", {}),
        },
    }


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    result = asyncio.run(_run_pipeline(args))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
