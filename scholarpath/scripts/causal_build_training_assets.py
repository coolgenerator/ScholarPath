"""Build causal training assets (real + synthetic) without training/promotion."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scholarpath.evals.causal_gold_live import (
    DEFAULT_CAUSAL_GOLD_DATASET_PATH,
    load_causal_gold_dataset,
)
from scholarpath.scripts.causal_activate_pywhy import (
    build_augmented_seed_cases,
    choose_seed_cases,
    ensure_seed_prerequisites,
    reset_causal_assets,
    seed_training_assets,
)


async def run_build(args: argparse.Namespace) -> dict[str, Any]:
    dataset = load_causal_gold_dataset(args.dataset)
    selected = choose_seed_cases(
        dataset_cases=dataset.cases,
        seed_cases=args.seed_cases,
    )
    augmented = build_augmented_seed_cases(
        seed_cases=selected,
        synthetic_multiplier=args.synthetic_multiplier,
    )
    precheck = await ensure_seed_prerequisites(
        student_id=args.student_id,
        seed_cases=len(selected),
    )

    deleted_rows = {
        "causal_shadow_comparisons": 0,
        "causal_outcome_events": 0,
        "causal_feature_snapshots": 0,
        "causal_model_registry": 0,
    }
    if args.reset_causal_assets:
        deleted_rows = await reset_causal_assets()

    seeded = await seed_training_assets(
        student_id=precheck.student_id,
        school_ids=precheck.school_ids,
        seed_cases=augmented,
    )
    now = datetime.now(UTC).isoformat()
    return {
        "status": "ok",
        "generated_at": now,
        "dataset": str(args.dataset),
        "student_id": str(precheck.student_id),
        "seed_case_count_real": len(selected),
        "seed_case_count_total": len(augmented),
        "synthetic_multiplier": args.synthetic_multiplier,
        "deleted_rows": deleted_rows,
        "seeded": seeded,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build causal training assets from gold dataset (real + synthetic).",
    )
    parser.add_argument(
        "--dataset",
        default=str(DEFAULT_CAUSAL_GOLD_DATASET_PATH),
        help="Path to causal gold dataset JSON.",
    )
    parser.add_argument(
        "--student-id",
        default=None,
        help="Optional student UUID for seeded snapshots/outcomes.",
    )
    parser.add_argument(
        "--seed-cases",
        type=int,
        default=40,
        help="How many real cases to seed from dataset.",
    )
    parser.add_argument(
        "--synthetic-multiplier",
        type=int,
        default=0,
        help="Synthetic variants generated per real case.",
    )
    parser.add_argument(
        "--reset-causal-assets",
        action="store_true",
        help="Clear causal_* assets before seeding.",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    payload = asyncio.run(run_build(args))
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
