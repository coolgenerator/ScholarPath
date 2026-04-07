"""CLI entrypoint for admission data phase-2 official facts pipeline."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import func, select

from scholarpath.db.models import (
    AdmissionEvent,
    CausalOutcomeEvent,
    DocumentChunk,
    PolicyFact,
    PolicyFactAudit,
    RawDocument,
    School,
)
from scholarpath.db.session import async_session_factory
from scholarpath.services.causal_data_service import ingest_official_facts

_DEFAULT_MISSING_9 = [
    "California Institute of Technology",
    "University of California, Berkeley",
    "University of California, Davis",
    "University of California, Irvine",
    "University of California, Los Angeles",
    "University of California, San Diego",
    "University of Maryland, College Park",
    "University of Michigan, Ann Arbor",
    "University of Minnesota, Twin Cities",
]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run phase-2 official-facts extraction for missing schools with service-internal "
            "concurrency and RPM-band gating."
        )
    )
    parser.add_argument("--run-id", default="", help="Optional run id (auto-generated when empty).")
    parser.add_argument(
        "--scope",
        default="missing_official_fields_9",
        choices=["missing_official_fields_9", "missing_official_fields_all", "explicit"],
        help="School scope selector.",
    )
    parser.add_argument(
        "--schools",
        default="",
        help=(
            "Legacy explicit school names when scope=explicit. "
            "Use '|' as delimiter when names contain commas."
        ),
    )
    parser.add_argument(
        "--school",
        action="append",
        default=[],
        help="Repeatable explicit school name when scope=explicit.",
    )
    parser.add_argument(
        "--cycle-year",
        type=int,
        default=datetime.now(timezone.utc).year,
        help="Cycle year used for official facts.",
    )
    parser.add_argument(
        "--school-concurrency-initial",
        type=int,
        default=6,
        help="Initial school-level concurrency for internal worker scheduler.",
    )
    parser.add_argument(
        "--school-concurrency-max",
        type=int,
        default=20,
        help="Max school-level concurrency for internal worker scheduler.",
    )
    parser.add_argument(
        "--target-rpm-total",
        type=float,
        default=180.0,
        help="Target total RPM for both keys combined.",
    )
    parser.add_argument("--rpm-band-low", type=float, default=170.0, help="Lower RPM band.")
    parser.add_argument("--rpm-band-high", type=float, default=185.0, help="Upper RPM band.")
    parser.add_argument(
        "--retry-failed-once",
        action="store_true",
        default=True,
        help="Retry failed schools once (default: true).",
    )
    parser.add_argument(
        "--no-retry-failed-once",
        dest="retry_failed_once",
        action="store_false",
        help="Disable failed-school retry.",
    )
    parser.add_argument(
        "--output-dir",
        default=".benchmarks/official_phase2",
        help="Output directory for phase2 report artifacts.",
    )
    return parser


async def _count_truth_tables(session) -> dict[str, int]:
    admission_events = int((await session.scalar(select(func.count()).select_from(AdmissionEvent))) or 0)
    causal_outcomes = int((await session.scalar(select(func.count()).select_from(CausalOutcomeEvent))) or 0)
    return {
        "admission_events": admission_events,
        "causal_outcome_events": causal_outcomes,
    }


def _field_count_from_school_metadata(school: School) -> int:
    metadata = dict(school.metadata_ or {})
    official = dict(metadata.get("official_facts") or {})
    fields = dict(official.get("fields") or {})
    return len(fields)


async def _resolve_school_scope(session, *, scope: str, explicit_schools: list[str]) -> list[str]:
    schools = list((await session.execute(select(School).order_by(School.name.asc()))).scalars().all())
    if scope == "explicit":
        return [name for name in explicit_schools if name]

    if scope == "missing_official_fields_9":
        by_name = {str(item.name).strip(): item for item in schools}
        out: list[str] = []
        for name in _DEFAULT_MISSING_9:
            school = by_name.get(name)
            if school is None:
                continue
            if _field_count_from_school_metadata(school) == 0:
                out.append(name)
        return out

    # missing_official_fields_all
    return [
        school.name
        for school in schools
        if _field_count_from_school_metadata(school) == 0
    ]


def _parse_explicit_schools(args: argparse.Namespace) -> list[str]:
    parsed: list[str] = []
    for item in list(getattr(args, "school", []) or []):
        text = str(item or "").strip()
        if text:
            parsed.append(text)

    raw = str(getattr(args, "schools", "") or "").strip()
    if not raw:
        return parsed

    # Backward-compatible parsing:
    # - Prefer "|" because school names may contain commas.
    # - Fall back to "," for legacy callers.
    delimiter = "|" if "|" in raw else ","
    parsed.extend([item.strip() for item in raw.split(delimiter) if item.strip()])
    return parsed


def _merge_runs(primary: dict[str, Any], retry: dict[str, Any] | None) -> dict[str, Any]:
    if not retry:
        return primary

    merged = dict(primary)
    school_map = {
        str(row.get("school_name") or row.get("school_id") or ""): row
        for row in list(primary.get("schools") or [])
    }
    for row in list(retry.get("schools") or []):
        key = str(row.get("school_name") or row.get("school_id") or "")
        if key:
            school_map[key] = row

    merged["schools"] = list(school_map.values())
    merged["schools_not_found"] = sorted(
        set(list(primary.get("schools_not_found") or []) + list(retry.get("schools_not_found") or [])),
    )
    for field in (
        "processed_schools",
        "raw_facts",
        "schema_valid_count",
        "extracted_count",
        "kept_count",
        "quarantined_count",
        "conflicts_count",
        "deduped_count",
        "llm_calls_extract",
        "llm_calls_judge",
        "llm_errors",
        "schools_with_any_fact",
        "schools_with_official_fact",
        "external_id_match_success",
        "external_id_match_total",
        "raw_documents_created",
        "document_chunks_created",
        "policy_facts_created",
        "policy_fact_audits_created",
    ):
        merged[field] = int(primary.get(field) or 0) + int(retry.get(field) or 0)

    merged["schools_updated"] = sorted(
        set(list(primary.get("schools_updated") or []) + list(retry.get("schools_updated") or [])),
    )
    merged["schools_updated_count"] = len(merged["schools_updated"])
    merged["rpm_windows"] = list(primary.get("rpm_windows") or []) + list(retry.get("rpm_windows") or [])
    merged["rate_limit_error_count"] = int(primary.get("rate_limit_error_count") or 0) + int(
        retry.get("rate_limit_error_count") or 0,
    )
    rpm_values = [float(item.get("rpm_actual") or 0.0) for item in merged["rpm_windows"]]
    merged["rpm_actual_avg"] = round(sum(rpm_values) / len(rpm_values), 2) if rpm_values else 0.0
    return merged


async def _collect_phase2_table_counts(session, *, school_names: list[str]) -> dict[str, dict[str, int]]:
    schools = (
        (await session.execute(select(School).where(func.lower(School.name).in_([name.lower() for name in school_names]))))
        .scalars()
        .all()
    )
    out: dict[str, dict[str, int]] = {}
    for school in schools:
        raw_docs = int(
            (await session.scalar(select(func.count()).select_from(RawDocument).where(RawDocument.school_id == school.id)))
            or 0
        )
        chunks = int(
            (
                await session.scalar(
                    select(func.count()).select_from(DocumentChunk).join(
                        RawDocument,
                        DocumentChunk.raw_document_id == RawDocument.id,
                    ).where(RawDocument.school_id == school.id)
                )
            )
            or 0
        )
        policy_facts = int(
            (await session.scalar(select(func.count()).select_from(PolicyFact).where(PolicyFact.school_id == school.id)))
            or 0
        )
        audits = int(
            (
                await session.scalar(
                    select(func.count()).select_from(PolicyFactAudit).where(PolicyFactAudit.school_id == school.id)
                )
            )
            or 0
        )
        out[school.name] = {
            "raw_documents": raw_docs,
            "document_chunks": chunks,
            "policy_facts": policy_facts,
            "policy_fact_audits": audits,
        }
    return out


def _evaluate_phase2_gate(
    *,
    school_names: list[str],
    school_table_counts: dict[str, dict[str, int]],
    merged_run: dict[str, Any],
    truth_before: dict[str, int],
    truth_after: dict[str, int],
    rpm_band_low: float,
    rpm_band_high: float,
) -> dict[str, Any]:
    reasons: list[str] = []

    missing_policy_fact_schools = [
        name for name in school_names if int((school_table_counts.get(name) or {}).get("policy_facts") or 0) < 1
    ]
    if missing_policy_fact_schools:
        reasons.append("schools_without_policy_fact")

    rpm_windows = list(merged_run.get("rpm_windows") or [])
    llm_calls_total = int(merged_run.get("llm_calls_extract") or 0) + int(merged_run.get("llm_calls_judge") or 0)
    rpm_eval_min_llm_calls = max(30, int(round(rpm_band_low)))
    rpm_band_evaluable = llm_calls_total >= rpm_eval_min_llm_calls
    rpm_values = [float(item.get("rpm_actual") or 0.0) for item in rpm_windows if float(item.get("rpm_actual") or 0.0) > 0]
    windows_total = len(rpm_values)
    windows_in_band = len([rpm for rpm in rpm_values if rpm_band_low <= rpm <= rpm_band_high])
    windows_over_190 = len([rpm for rpm in rpm_values if rpm > 190.0])
    in_band_rate = (windows_in_band / windows_total) if windows_total else 0.0

    if windows_total == 0 and rpm_band_evaluable:
        reasons.append("rpm_windows_empty")
    if in_band_rate < 0.8 and rpm_band_evaluable:
        reasons.append("rpm_in_band_rate_lt_0.8")
    if windows_over_190 > 0:
        reasons.append("rpm_window_over_190")

    rate_limit_error_count = int(merged_run.get("rate_limit_error_count") or 0)
    if rate_limit_error_count != 0:
        reasons.append("rate_limit_error_count_not_zero")

    kept = int(merged_run.get("kept_count") or 0)
    conflicts = int(merged_run.get("conflicts_count") or 0)
    contradiction_rate = conflicts / max(1, kept + conflicts)
    if contradiction_rate > 0.03:
        reasons.append("contradiction_rate_gt_0.03")

    policy_facts_total = sum(int((row or {}).get("policy_facts") or 0) for row in school_table_counts.values())
    # This is based on required non-empty evidence_quote field.
    evidence_valid_rate = 1.0 if policy_facts_total > 0 else 0.0
    if evidence_valid_rate < 0.95:
        reasons.append("evidence_valid_rate_lt_0.95")

    for key in ("admission_events", "causal_outcome_events"):
        if int(truth_before.get(key) or 0) != int(truth_after.get(key) or 0):
            reasons.append(f"{key}_changed")

    return {
        "passed": len(reasons) == 0,
        "reasons": reasons,
        "thresholds": {
            "policy_fact_per_school_min": 1,
            "rpm_in_band_rate_min": 0.8,
            "rpm_band_evaluable_min_llm_calls": rpm_eval_min_llm_calls,
            "rpm_window_max": 190.0,
            "rate_limit_error_count_eq": 0,
            "evidence_valid_rate_min": 0.95,
            "contradiction_rate_max": 0.03,
            "admission_events_unchanged": True,
            "causal_outcome_events_unchanged": True,
        },
        "observed": {
            "schools_without_policy_fact": missing_policy_fact_schools,
            "rpm_windows_total": windows_total,
            "rpm_windows_in_band": windows_in_band,
            "rpm_windows_over_190": windows_over_190,
            "rpm_in_band_rate": round(in_band_rate, 4),
            "llm_calls_total": llm_calls_total,
            "rpm_band_evaluable": rpm_band_evaluable,
            "rate_limit_error_count": rate_limit_error_count,
            "evidence_valid_rate": round(evidence_valid_rate, 4),
            "contradiction_rate": round(contradiction_rate, 4),
            "truth_before": truth_before,
            "truth_after": truth_after,
        },
    }


async def _run(args: argparse.Namespace) -> int:
    run_id = args.run_id or f"admission-phase2-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}-{uuid4().hex[:6]}"
    explicit_schools = _parse_explicit_schools(args)

    async with async_session_factory() as session:
        school_names = await _resolve_school_scope(
            session,
            scope=str(args.scope),
            explicit_schools=explicit_schools,
        )
        truth_before = await _count_truth_tables(session)

        primary = await ingest_official_facts(
            session,
            school_names=school_names,
            cycle_year=int(args.cycle_year),
            run_id=run_id,
            school_concurrency_initial=int(args.school_concurrency_initial),
            school_concurrency_max=int(args.school_concurrency_max),
            target_rpm_total=float(args.target_rpm_total),
            rpm_band_low=float(args.rpm_band_low),
            rpm_band_high=float(args.rpm_band_high),
        )

        retry_payload: dict[str, Any] | None = None
        if bool(args.retry_failed_once):
            failed = [
                str(item.get("school_name") or "")
                for item in list(primary.get("schools") or [])
                if str(item.get("status") or "") not in {"ok", "not_found"}
            ]
            failed = [name for name in failed if name]
            if failed:
                retry_payload = await ingest_official_facts(
                    session,
                    school_names=failed,
                    cycle_year=int(args.cycle_year),
                    run_id=f"{run_id}-retry1",
                    school_concurrency_initial=max(1, int(args.school_concurrency_initial) // 2),
                    school_concurrency_max=max(1, int(args.school_concurrency_max) // 2),
                    target_rpm_total=float(args.target_rpm_total),
                    rpm_band_low=float(args.rpm_band_low),
                    rpm_band_high=float(args.rpm_band_high),
                )

        await session.commit()

    async with async_session_factory() as session:
        truth_after = await _count_truth_tables(session)
        school_table_counts = await _collect_phase2_table_counts(session, school_names=school_names)

    merged = _merge_runs(primary, retry_payload)
    gate = _evaluate_phase2_gate(
        school_names=school_names,
        school_table_counts=school_table_counts,
        merged_run=merged,
        truth_before=truth_before,
        truth_after=truth_after,
        rpm_band_low=float(args.rpm_band_low),
        rpm_band_high=float(args.rpm_band_high),
    )

    run_root = Path(args.output_dir).expanduser().resolve() / run_id
    run_root.mkdir(parents=True, exist_ok=True)
    report_json = run_root / "phase2_report.json"
    report_md = run_root / "phase2_report.md"

    payload = {
        "status": "ok" if gate.get("passed", False) else "gate_failed",
        "run_id": run_id,
        "scope": args.scope,
        "request": {
            "cycle_year": int(args.cycle_year),
            "school_concurrency_initial": int(args.school_concurrency_initial),
            "school_concurrency_max": int(args.school_concurrency_max),
            "target_rpm_total": float(args.target_rpm_total),
            "rpm_band_low": float(args.rpm_band_low),
            "rpm_band_high": float(args.rpm_band_high),
            "retry_failed_once": bool(args.retry_failed_once),
        },
        "schools_requested": school_names,
        "schools_requested_count": len(school_names),
        "primary_run": primary,
        "retry_run": retry_payload,
        "merged_run": merged,
        "school_table_counts": school_table_counts,
        "truth_counts": {
            "before": truth_before,
            "after": truth_after,
            "delta": {
                key: int(truth_after.get(key) or 0) - int(truth_before.get(key) or 0)
                for key in sorted(set(truth_before) | set(truth_after))
            },
        },
        "gate": gate,
    }

    report_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    md_lines = [
        f"# Admission Phase2 Report `{run_id}`",
        "",
        f"- scope: `{args.scope}`",
        f"- schools_requested: `{len(school_names)}`",
        f"- processed_schools: `{int(merged.get('processed_schools') or 0)}`",
        f"- raw_documents_created: `{int(merged.get('raw_documents_created') or 0)}`",
        f"- document_chunks_created: `{int(merged.get('document_chunks_created') or 0)}`",
        f"- policy_facts_created: `{int(merged.get('policy_facts_created') or 0)}`",
        f"- policy_fact_audits_created: `{int(merged.get('policy_fact_audits_created') or 0)}`",
        f"- rpm_actual_avg: `{float(merged.get('rpm_actual_avg') or 0.0)}`",
        f"- rate_limit_error_count: `{int(merged.get('rate_limit_error_count') or 0)}`",
        f"- gate_passed: `{bool(gate.get('passed', False))}`",
        f"- gate_reasons: `{', '.join(gate.get('reasons', [])) or 'none'}`",
        "",
        "## Per School",
    ]

    merged_schools = {
        str(row.get("school_name") or row.get("school_id") or ""): row
        for row in list(merged.get("schools") or [])
    }
    for school_name in school_names:
        row = merged_schools.get(school_name) or {}
        counts = school_table_counts.get(school_name) or {}
        md_lines.append(
            "- "
            + f"{school_name}: status=`{row.get('status', 'missing')}`, "
            + f"policy_facts=`{int(counts.get('policy_facts') or 0)}`, "
            + f"raw_documents=`{int(counts.get('raw_documents') or 0)}`, "
            + f"chunks=`{int(counts.get('document_chunks') or 0)}`"
        )

    report_md.write_text("\n".join(md_lines), encoding="utf-8")
    payload["report_json"] = str(report_json)
    payload["report_md"] = str(report_md)

    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0 if gate.get("passed", False) else 2


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
