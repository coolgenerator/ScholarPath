"""Map missing `SchoolExternalId(provider=ipeds)` rows from IPEDS institution data."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scholarpath.db.session import async_session_factory
from scholarpath.services.causal_data_service import map_ipeds_external_ids


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Map missing school ipeds external ids from HD institution dataset.",
    )
    parser.add_argument(
        "--run-id",
        default="",
        help="Optional run id; default uses UTC timestamp.",
    )
    parser.add_argument(
        "--schools",
        default="",
        help="Optional comma-separated school names to scope mapping.",
    )
    parser.add_argument(
        "--schools-file",
        default="",
        help="Optional file with one school name per line.",
    )
    parser.add_argument(
        "--fuzzy-threshold",
        type=float,
        default=0.88,
        help="Fuzzy similarity threshold for same-state fallback (default: 0.88).",
    )
    parser.add_argument(
        "--max-samples",
        type=int,
        default=200,
        help="Max mapped/skipped sample rows in report (default: 200).",
    )
    parser.add_argument(
        "--output-dir",
        default=".benchmarks/official_phase4",
        help="Directory to write ipeds_mapping_report.json",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write DB changes (default mode when --apply not set).",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help="Apply mapping writes into school_external_ids.",
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
    run_id = args.run_id or f"causal-ipeds-map-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}"
    school_scope = _load_school_scope(args)
    dry_run = not bool(args.apply)

    async with async_session_factory() as session:
        result = await map_ipeds_external_ids(
            session,
            run_id=run_id,
            dry_run=dry_run,
            fuzzy_threshold=float(args.fuzzy_threshold),
            school_names=school_scope or None,
            max_samples=int(args.max_samples),
        )
        if dry_run:
            await session.rollback()
        else:
            await session.commit()

    out_dir = Path(args.output_dir) / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "ipeds_mapping_report.json"
    payload = {
        "run_id": run_id,
        "config": {
            "dry_run": dry_run,
            "fuzzy_threshold": float(args.fuzzy_threshold),
            "school_scope_size": len(school_scope),
            "max_samples": int(args.max_samples),
        },
        "result": result,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    payload = asyncio.run(_run(args))
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
