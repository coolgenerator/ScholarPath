"""CLI entrypoint for admission data phase-3 quality closure on policy facts."""

from __future__ import annotations

import argparse
import asyncio
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import and_, func, select

from scholarpath.db.models import FactQuarantine, PolicyFact, School
from scholarpath.db.session import async_session_factory
from scholarpath.search.canonical_merge import normalise_variable_name

_DEFAULT_PHASE3_9 = [
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

_DEFAULT_REQUIRED_KEYS = [
    "acceptance_rate",
    "avg_net_price",
    "student_faculty_ratio",
    "enrollment",
    "tuition_out_of_state",
    "graduation_rate_4yr",
]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run phase-3 policy-fact quality closure for a school scope. "
            "This pass evaluates evidence integrity, contradictions, and key coverage, "
            "then exports a curated fact set."
        )
    )
    parser.add_argument("--run-id", default="", help="Optional run id (auto-generated when empty).")
    parser.add_argument(
        "--scope",
        default="target_9",
        choices=["target_9", "explicit", "all_with_policy_facts"],
        help="School scope selector.",
    )
    parser.add_argument(
        "--schools",
        default="",
        help="Legacy explicit school names, delimiter '|' preferred when names contain commas.",
    )
    parser.add_argument(
        "--school",
        action="append",
        default=[],
        help="Repeatable explicit school name when scope=explicit.",
    )
    parser.add_argument(
        "--required-key",
        action="append",
        default=[],
        help="Repeatable required fact key for per-school coverage checks.",
    )
    parser.add_argument(
        "--min-evidence-valid-rate",
        type=float,
        default=0.98,
        help="Gate threshold for curated evidence validity rate.",
    )
    parser.add_argument(
        "--max-contradiction-rate",
        type=float,
        default=0.05,
        help="Gate threshold for contradiction rate (on grouped duplicate keys).",
    )
    parser.add_argument(
        "--min-required-key-coverage",
        type=float,
        default=0.9,
        help="Gate threshold for each required key's school coverage rate.",
    )
    parser.add_argument(
        "--max-unresolved-quarantine-rate",
        type=float,
        default=0.2,
        help="Gate threshold for unresolved quarantine rate (unresolved / curated facts).",
    )
    parser.add_argument(
        "--auto-resolve-quarantine",
        action="store_true",
        default=True,
        help="Auto-resolve unresolved quarantines shadowed by accepted policy facts (default: true).",
    )
    parser.add_argument(
        "--no-auto-resolve-quarantine",
        dest="auto_resolve_quarantine",
        action="store_false",
        help="Disable shadowed quarantine auto-resolution.",
    )
    parser.add_argument(
        "--output-dir",
        default=".benchmarks/official_phase3",
        help="Output directory for phase3 report artifacts.",
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


async def _resolve_school_scope(session, *, scope: str, explicit_schools: list[str]) -> list[str]:
    if scope == "explicit":
        return [name for name in explicit_schools if name]
    if scope == "target_9":
        existing_names = set(
            (
                await session.execute(
                    select(School.name).where(School.name.in_(_DEFAULT_PHASE3_9))
                )
            )
            .scalars()
            .all()
        )
        return [name for name in _DEFAULT_PHASE3_9 if name in existing_names]

    rows = (
        await session.execute(
            select(School.name)
            .join(PolicyFact, PolicyFact.school_id == School.id)
            .group_by(School.name)
            .order_by(School.name.asc())
        )
    ).scalars()
    return list(rows)


def _normalized_value_signature(row: PolicyFact) -> str:
    if row.value_numeric is not None:
        return f"n:{round(float(row.value_numeric), 4)}"
    return f"t:{str(row.value_text or '').strip().lower()}"


def _pick_curated_fact(rows: list[PolicyFact]) -> PolicyFact:
    def _rank(row: PolicyFact) -> tuple[int, int, float, datetime]:
        reviewed = int(bool(row.reviewed_flag))
        accepted = int(str(row.status or "").strip().lower() == "accepted")
        confidence = float(row.confidence or 0.0)
        created_at = row.created_at or datetime(1970, 1, 1, tzinfo=timezone.utc)
        return (reviewed, accepted, confidence, created_at)

    return sorted(rows, key=_rank, reverse=True)[0]


def _is_evidence_valid(row: PolicyFact) -> bool:
    source_url_ok = bool(str(row.source_url or "").strip())
    quote_ok = bool(str(row.evidence_quote or "").strip())
    extractor_ok = bool(str(row.extractor_version or "").strip())
    confidence = float(row.confidence or 0.0)
    confidence_ok = 0.0 <= confidence <= 1.0
    return source_url_ok and quote_ok and extractor_ok and confidence_ok


async def _run(args: argparse.Namespace) -> int:
    run_id = args.run_id or f"admission-phase3-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}-{uuid4().hex[:6]}"
    explicit_schools = _parse_explicit_schools(args)
    required_keys = (
        [str(item).strip() for item in list(args.required_key or []) if str(item).strip()]
        or list(_DEFAULT_REQUIRED_KEYS)
    )

    async with async_session_factory() as session:
        school_names = await _resolve_school_scope(
            session,
            scope=str(args.scope),
            explicit_schools=explicit_schools,
        )
        schools = (
            (
                await session.execute(
                    select(School).where(School.name.in_(school_names)).order_by(School.name.asc())
                )
            )
            .scalars()
            .all()
        )
        school_ids = [row.id for row in schools]
        school_name_by_id = {row.id: row.name for row in schools}

        policy_rows = []
        if school_ids:
            policy_rows = (
                (
                    await session.execute(
                        select(PolicyFact)
                        .where(PolicyFact.school_id.in_(school_ids))
                        .order_by(PolicyFact.created_at.desc())
                    )
                )
                .scalars()
                .all()
            )

        auto_resolved_quarantine = 0
        if bool(args.auto_resolve_quarantine) and school_ids:
            accepted_keys = {
                (str(row.school_id), str(row.fact_key or "").strip())
                for row in policy_rows
                if str(row.status or "").strip().lower() == "accepted" and str(row.fact_key or "").strip()
            }
            unresolved_rows = (
                (
                    await session.execute(
                        select(FactQuarantine).where(
                            and_(
                                FactQuarantine.resolved.is_(False),
                                FactQuarantine.school_id.in_(school_ids),
                            )
                        )
                    )
                )
                .scalars()
                .all()
            )
            for row in unresolved_rows:
                key = (
                    str(row.school_id),
                    normalise_variable_name(str(row.outcome_name or "").strip()),
                )
                if key in accepted_keys:
                    row.resolved = True
                    auto_resolved_quarantine += 1
            if auto_resolved_quarantine > 0:
                await session.flush()
                await session.commit()

        unresolved_quarantine = 0
        if school_ids:
            unresolved_quarantine = int(
                (
                    await session.scalar(
                        select(func.count()).select_from(FactQuarantine).where(
                            and_(
                                FactQuarantine.resolved.is_(False),
                                FactQuarantine.school_id.in_(school_ids),
                            )
                        )
                    )
                )
                or 0
            )

    grouped: dict[tuple[str, str], list[PolicyFact]] = defaultdict(list)
    for row in policy_rows:
        school_name = school_name_by_id.get(row.school_id)
        if not school_name:
            continue
        key = (school_name, str(row.fact_key or "").strip())
        grouped[key].append(row)

    curated_rows: list[PolicyFact] = []
    contradictions = 0
    duplicates_total = 0
    per_school_counts = {name: {"raw": 0, "curated": 0, "contradictions": 0, "duplicates": 0} for name in school_names}
    for (school_name, _fact_key), rows in grouped.items():
        rows = [row for row in rows if str(row.fact_key or "").strip()]
        if not rows:
            continue
        per_school_counts[school_name]["raw"] += len(rows)
        distinct_values = {_normalized_value_signature(row) for row in rows}
        if len(rows) > 1:
            per_school_counts[school_name]["duplicates"] += len(rows) - 1
            duplicates_total += len(rows) - 1
        if len(distinct_values) > 1:
            contradictions += 1
            per_school_counts[school_name]["contradictions"] += 1
        curated = _pick_curated_fact(rows)
        curated_rows.append(curated)
        per_school_counts[school_name]["curated"] += 1

    evidence_valid_count = sum(1 for row in curated_rows if _is_evidence_valid(row))
    evidence_valid_rate = evidence_valid_count / max(1, len(curated_rows))
    contradiction_rate = contradictions / max(1, len(grouped))
    unresolved_quarantine_rate = unresolved_quarantine / max(1, len(curated_rows))

    schools_without_curated = [name for name, counts in per_school_counts.items() if int(counts["curated"]) <= 0]
    required_key_coverage: dict[str, dict[str, Any]] = {}
    for key in required_keys:
        hit_schools = sorted(
            {
                school_name_by_id.get(row.school_id, "")
                for row in curated_rows
                if str(row.fact_key or "").strip() == key
            }
            - {""}
        )
        coverage_rate = len(hit_schools) / max(1, len(school_names))
        required_key_coverage[key] = {
            "covered_schools": len(hit_schools),
            "coverage_rate": round(coverage_rate, 4),
            "missing_schools": sorted(set(school_names) - set(hit_schools)),
        }

    reasons: list[str] = []
    if schools_without_curated:
        reasons.append("schools_without_curated_policy_fact")
    if evidence_valid_rate < float(args.min_evidence_valid_rate):
        reasons.append("evidence_valid_rate_lt_threshold")
    if contradiction_rate > float(args.max_contradiction_rate):
        reasons.append("contradiction_rate_gt_threshold")
    if unresolved_quarantine_rate > float(args.max_unresolved_quarantine_rate):
        reasons.append("unresolved_quarantine_rate_gt_threshold")
    if any(
        float(item.get("coverage_rate") or 0.0) < float(args.min_required_key_coverage)
        for item in required_key_coverage.values()
    ):
        reasons.append("required_key_coverage_lt_threshold")

    gate = {
        "passed": len(reasons) == 0,
        "reasons": reasons,
        "thresholds": {
            "min_evidence_valid_rate": float(args.min_evidence_valid_rate),
            "max_contradiction_rate": float(args.max_contradiction_rate),
            "min_required_key_coverage": float(args.min_required_key_coverage),
            "max_unresolved_quarantine_rate": float(args.max_unresolved_quarantine_rate),
        },
        "observed": {
            "schools_without_curated_policy_fact": schools_without_curated,
            "curated_fact_total": len(curated_rows),
            "raw_fact_total": len(policy_rows),
            "grouped_fact_keys_total": len(grouped),
            "duplicates_total": duplicates_total,
            "contradictions_total": contradictions,
            "contradiction_rate": round(contradiction_rate, 4),
            "evidence_valid_count": evidence_valid_count,
            "evidence_valid_rate": round(evidence_valid_rate, 4),
            "unresolved_quarantine": unresolved_quarantine,
            "unresolved_quarantine_rate": round(unresolved_quarantine_rate, 4),
            "required_key_coverage": required_key_coverage,
        },
    }

    run_root = Path(args.output_dir).expanduser().resolve() / run_id
    run_root.mkdir(parents=True, exist_ok=True)
    report_json = run_root / "phase3_report.json"
    report_md = run_root / "phase3_report.md"
    curated_jsonl = run_root / "phase3_curated_policy_facts.jsonl"

    curated_lines = []
    for row in sorted(
        curated_rows,
        key=lambda item: (
            school_name_by_id.get(item.school_id, ""),
            str(item.fact_key or ""),
            -(float(item.confidence or 0.0)),
        ),
    ):
        school_name = school_name_by_id.get(row.school_id, "")
        payload = {
            "run_id": run_id,
            "school_name": school_name,
            "cycle_year": int(row.cycle_year),
            "fact_key": str(row.fact_key or "").strip(),
            "value_text": str(row.value_text or "").strip(),
            "value_numeric": row.value_numeric,
            "source_url": str(row.source_url or "").strip(),
            "evidence_quote": str(row.evidence_quote or "").strip(),
            "extractor_version": str(row.extractor_version or "").strip(),
            "confidence": float(row.confidence or 0.0),
            "reviewed_flag": bool(row.reviewed_flag),
            "status": str(row.status or "").strip(),
            "policy_fact_id": str(row.id),
            "created_at": (row.created_at.isoformat() if row.created_at else None),
        }
        curated_lines.append(json.dumps(payload, ensure_ascii=False))

    curated_jsonl.write_text("\n".join(curated_lines), encoding="utf-8")

    payload = {
        "status": "ok" if gate["passed"] else "gate_failed",
        "run_id": run_id,
        "scope": str(args.scope),
        "schools_requested": school_names,
        "schools_requested_count": len(school_names),
        "required_keys": required_keys,
        "per_school_counts": per_school_counts,
        "auto_resolved_quarantine": auto_resolved_quarantine,
        "gate": gate,
        "artifacts": {
            "curated_jsonl": str(curated_jsonl),
        },
    }
    report_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    md_lines = [
        f"# Admission Phase3 Report `{run_id}`",
        "",
        f"- scope: `{args.scope}`",
        f"- schools_requested: `{len(school_names)}`",
        f"- curated_fact_total: `{gate['observed']['curated_fact_total']}`",
        f"- raw_fact_total: `{gate['observed']['raw_fact_total']}`",
        f"- contradiction_rate: `{gate['observed']['contradiction_rate']}`",
        f"- evidence_valid_rate: `{gate['observed']['evidence_valid_rate']}`",
        f"- unresolved_quarantine_rate: `{gate['observed']['unresolved_quarantine_rate']}`",
        f"- gate_passed: `{gate['passed']}`",
        f"- gate_reasons: `{', '.join(gate['reasons']) or 'none'}`",
        "",
        "## Per School",
    ]
    for school_name in school_names:
        counts = per_school_counts.get(school_name) or {}
        md_lines.append(
            "- "
            + f"{school_name}: raw=`{int(counts.get('raw') or 0)}`, "
            + f"curated=`{int(counts.get('curated') or 0)}`, "
            + f"duplicates=`{int(counts.get('duplicates') or 0)}`, "
            + f"contradictions=`{int(counts.get('contradictions') or 0)}`"
        )

    report_md.write_text("\n".join(md_lines), encoding="utf-8")
    payload["report_json"] = str(report_json)
    payload["report_md"] = str(report_md)

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if gate["passed"] else 2


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
