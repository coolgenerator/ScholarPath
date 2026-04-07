"""Relink alias schools to canonical schools for public admission truth assets.

This script is designed for one-off/periodic data hygiene runs:
- keep Strict True-Only labels untouched
- only move records across school_id when alias->canonical mapping is deterministic
- avoid changing API contracts or schema
"""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import func, select, update

from scholarpath.db.models import AdmissionEvent, CausalFeatureSnapshot, CausalOutcomeEvent, School
from scholarpath.db.session import async_session_factory
from scholarpath.scripts.admission_truth_public_shared import (
    _SCHOOL_ALIAS_MAP,
    load_ranked_school_allowlist,
    normalise_school_key,
)


@dataclass(slots=True)
class RelinkCounts:
    admission_events: int = 0
    feature_snapshots: int = 0
    outcome_events: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "admission_events": int(self.admission_events),
            "feature_snapshots": int(self.feature_snapshots),
            "outcome_events": int(self.outcome_events),
        }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Relink alias schools to canonical schools for admission truth assets.",
    )
    parser.add_argument(
        "--run-id",
        default="",
        help="Optional run id; auto-generated when empty.",
    )
    parser.add_argument(
        "--output-dir",
        default=".benchmarks/admission_truth_public",
        help="Directory for report artifacts.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview mapping and moved row counts without writing DB changes.",
    )
    return parser.parse_args()


async def _count_rows_for_school(session, model, school_id: UUID) -> int:
    value = await session.scalar(
        select(func.count(model.id)).where(model.school_id == school_id),
    )
    return int(value or 0)


async def _build_mapping() -> tuple[dict[UUID, UUID], dict[str, str]]:
    async with async_session_factory() as session:
        school_rows = list((await session.execute(select(School.id, School.name))).all())

    by_key: dict[str, tuple[UUID, str]] = {}
    for school_id, school_name in school_rows:
        key = normalise_school_key(school_name or "")
        if key and key not in by_key:
            by_key[key] = (school_id, str(school_name or "").strip())

    ranked_ctx = load_ranked_school_allowlist()
    alias_map = dict(ranked_ctx.alias_map)
    alias_map.update(_SCHOOL_ALIAS_MAP)

    # Additional high-frequency noisy forms observed in public intake snapshots.
    alias_map.update(
        {
            "university of michigan": "University of Michigan, Ann Arbor",
            "university of maryland": "University of Maryland, College Park",
            "university of california berkeley": "University of California, Berkeley",
            "university of california san diego": "University of California, San Diego",
            "university of california los angeles": "University of California, Los Angeles",
            "university of california davis": "University of California, Davis",
            "university of california irvine": "University of California, Irvine",
            "the ohio state university": "Ohio State University",
            "penn state university": "Pennsylvania State University",
            "georgia tech": "Georgia Institute of Technology",
            "washu st louis": "Washington University in St. Louis",
        }
    )

    mapping: dict[UUID, UUID] = {}
    mapping_preview: dict[str, str] = {}

    for alias_key, canonical_name in alias_map.items():
        src = by_key.get(normalise_school_key(alias_key))
        dst = by_key.get(normalise_school_key(canonical_name))
        if not src or not dst:
            continue
        src_id, src_name = src
        dst_id, dst_name = dst
        if src_id == dst_id:
            continue
        if src_id in mapping and mapping[src_id] != dst_id:
            # Keep deterministic one-to-one mapping by first hit.
            continue
        mapping[src_id] = dst_id
        mapping_preview[str(src_id)] = f"{src_name} -> {dst_name}"

    return mapping, mapping_preview


async def _run() -> int:
    args = _parse_args()
    run_id = (args.run_id or f"alias-relink-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}")
    output_dir = Path(args.output_dir).resolve() / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    mapping, mapping_preview = await _build_mapping()
    counts = RelinkCounts()
    moved_by_pair: dict[str, dict[str, int]] = {}

    async with async_session_factory() as session:
        for src_id, dst_id in mapping.items():
            pair_key = f"{src_id}->{dst_id}"
            pair_counts = {"admission_events": 0, "feature_snapshots": 0, "outcome_events": 0}

            ae_count = await _count_rows_for_school(session, AdmissionEvent, src_id)
            fs_count = await _count_rows_for_school(session, CausalFeatureSnapshot, src_id)
            oe_count = await _count_rows_for_school(session, CausalOutcomeEvent, src_id)

            pair_counts["admission_events"] = ae_count
            pair_counts["feature_snapshots"] = fs_count
            pair_counts["outcome_events"] = oe_count

            counts.admission_events += ae_count
            counts.feature_snapshots += fs_count
            counts.outcome_events += oe_count

            if not args.dry_run:
                if ae_count:
                    await session.execute(
                        update(AdmissionEvent)
                        .where(AdmissionEvent.school_id == src_id)
                        .values(school_id=dst_id),
                    )
                if fs_count:
                    await session.execute(
                        update(CausalFeatureSnapshot)
                        .where(CausalFeatureSnapshot.school_id == src_id)
                        .values(school_id=dst_id),
                    )
                if oe_count:
                    await session.execute(
                        update(CausalOutcomeEvent)
                        .where(CausalOutcomeEvent.school_id == src_id)
                        .values(school_id=dst_id),
                    )

            if ae_count or fs_count or oe_count:
                moved_by_pair[pair_key] = pair_counts

        if args.dry_run:
            await session.rollback()
        else:
            await session.commit()

    payload: dict[str, Any] = {
        "status": "ok",
        "run_id": run_id,
        "dry_run": bool(args.dry_run),
        "mapping_count": len(mapping),
        "mapped_pairs_preview": mapping_preview,
        "moved_rows_total": counts.as_dict(),
        "moved_rows_by_pair": moved_by_pair,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    report_json = output_dir / "admission_truth_alias_relink_report.json"
    report_md = output_dir / "admission_truth_alias_relink_report.md"
    report_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    md_lines = [
        "# Admission Truth Alias Relink Report",
        "",
        f"- run_id: `{run_id}`",
        f"- dry_run: `{bool(args.dry_run)}`",
        f"- mapping_count: `{len(mapping)}`",
        "",
        "## Moved Rows Total",
        "",
        f"- admission_events: `{counts.admission_events}`",
        f"- feature_snapshots: `{counts.feature_snapshots}`",
        f"- outcome_events: `{counts.outcome_events}`",
        "",
        "## Notes",
        "",
        "- This run only relinks school_id across admission/snapshot/outcome tables.",
        "- No synthetic/proxy labels are created.",
    ]
    report_md.write_text("\n".join(md_lines), encoding="utf-8")

    payload["report_json"] = str(report_json)
    payload["report_md"] = str(report_md)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_run()))

