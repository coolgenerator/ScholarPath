"""Gold-set evaluation runner for recommendation prefilter + scenario inference."""

from __future__ import annotations

import asyncio
import csv
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scholarpath.services.recommendation_scenarios import build_scenario_pack

DEFAULT_DATASET_PATH = (
    Path(__file__).resolve().parent / "datasets" / "recommendation_gold_v1.json"
)
DEFAULT_OUTPUT_DIR = Path(".benchmarks/recommendation_gold")
SCENARIO_IDS = [
    "budget_first",
    "risk_first",
    "major_first",
    "geo_first",
    "roi_first",
]


@dataclass(slots=True)
class RecommendationGoldCase:
    case_id: str
    description: str
    student_budget_usd: int | None
    budget_cap_override: int | None
    student_majors: list[str]
    preferences: dict[str, Any]
    schools: list[dict[str, Any]]
    expectations: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RecommendationGoldCaseResult:
    case_id: str
    status: str
    checks: dict[str, bool]
    reasons: list[str]
    metrics: dict[str, Any]
    prefilter_meta: dict[str, Any]
    excluded: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RecommendationGoldEvalReport:
    run_id: str
    generated_at: str
    status: str
    config: dict[str, Any]
    metrics: dict[str, Any]
    failed_cases: list[str]
    cases: list[RecommendationGoldCaseResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["cases"] = [item.to_dict() for item in self.cases]
        return payload


def load_recommendation_gold_dataset(
    path: str | Path = DEFAULT_DATASET_PATH,
) -> list[RecommendationGoldCase]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = payload.get("cases")
    if not isinstance(rows, list):
        raise ValueError("Dataset must contain top-level 'cases' list")

    out: list[RecommendationGoldCase] = []
    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"Case {idx} is not an object")
        case_id = str(row.get("case_id") or "").strip()
        if not case_id:
            raise ValueError(f"Case {idx} missing case_id")
        schools = row.get("schools") or []
        if not isinstance(schools, list) or not schools:
            raise ValueError(f"Case {case_id} missing schools list")
        out.append(
            RecommendationGoldCase(
                case_id=case_id,
                description=str(row.get("description") or "").strip(),
                student_budget_usd=_to_int_or_none(row.get("student_budget_usd")),
                budget_cap_override=_to_int_or_none(row.get("budget_cap_override")),
                student_majors=_to_str_list(row.get("student_majors")),
                preferences=_to_dict(row.get("preferences")),
                schools=[_to_dict(item) for item in schools if isinstance(item, dict)],
                expectations=_to_dict(row.get("expectations")),
            )
        )
    return out


def select_cases(
    cases: list[RecommendationGoldCase],
    *,
    sample_size: int | None = None,
    case_ids: list[str] | None = None,
) -> list[RecommendationGoldCase]:
    if case_ids:
        ids_map = {item.case_id: item for item in cases}
        missing = [cid for cid in case_ids if cid not in ids_map]
        if missing:
            raise ValueError(f"Unknown case_ids: {missing}")
        return [ids_map[cid] for cid in case_ids]

    ordered = sorted(cases, key=lambda item: item.case_id)
    if sample_size is None or sample_size >= len(ordered):
        return ordered
    return ordered[: max(1, int(sample_size))]


async def run_recommendation_gold_eval(
    *,
    dataset_path: str | Path = DEFAULT_DATASET_PATH,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    sample_size: int | None = None,
    case_ids: list[str] | None = None,
    eval_run_id: str | None = None,
) -> RecommendationGoldEvalReport:
    run_id = eval_run_id or f"recommendation-gold-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}"
    cases = load_recommendation_gold_dataset(dataset_path)
    selected = select_cases(cases, sample_size=sample_size, case_ids=case_ids)

    case_results = [await _evaluate_case(case) for case in selected]
    metrics = _aggregate_metrics(case_results)
    status, reasons = _grade_status(metrics)
    report = RecommendationGoldEvalReport(
        run_id=run_id,
        generated_at=datetime.now(timezone.utc).isoformat(),
        status=status,
        config={
            "dataset_path": str(dataset_path),
            "sample_size": sample_size,
            "case_ids": list(case_ids or []),
        },
        metrics={
            **metrics,
            "status_reasons": reasons,
        },
        failed_cases=[item.case_id for item in case_results if item.status != "ok"],
        cases=case_results,
    )
    _write_artifacts(Path(output_dir), report)
    return report


async def _evaluate_case(case: RecommendationGoldCase) -> RecommendationGoldCaseResult:
    baseline_1, scenario_pack_1, excluded_1 = build_scenario_pack(
        case.schools,
        student_budget_usd=case.student_budget_usd,
        budget_cap_override=case.budget_cap_override,
        stretch_quota=int(case.expectations.get("max_stretch", 3) or 3),
        student_majors=case.student_majors,
        preferences=case.preferences,
    )
    baseline_2, scenario_pack_2, _ = build_scenario_pack(
        case.schools,
        student_budget_usd=case.student_budget_usd,
        budget_cap_override=case.budget_cap_override,
        stretch_quota=int(case.expectations.get("max_stretch", 3) or 3),
        student_majors=case.student_majors,
        preferences=case.preferences,
    )

    checks: dict[str, bool] = {}
    reasons: list[str] = []
    meta = _to_dict(scenario_pack_1.get("meta"))
    excluded = [dict(item) for item in excluded_1 if isinstance(item, dict)]

    checks["determinism"] = _check_determinism(
        baseline_1=baseline_1,
        baseline_2=baseline_2,
        pack_1=scenario_pack_1,
        pack_2=scenario_pack_2,
    )
    checks["scenario_shape"] = _check_scenario_shape(scenario_pack_1)

    budget_cap = meta.get("budget_cap_used")
    if case.expectations.get("enforce_budget_hard_gate", True):
        checks["budget_hard_gate"] = _check_budget_hard_gate(
            baseline=baseline_1,
            budget_cap=budget_cap,
        )
    else:
        checks["budget_hard_gate"] = True

    max_stretch = int(case.expectations.get("max_stretch", 3) or 3)
    checks["stretch_quota"] = int(meta.get("stretch_count") or 0) <= max_stretch

    checks["expectations"] = _check_case_expectations(
        case=case,
        baseline=baseline_1,
        scenario_pack=scenario_pack_1,
        excluded=excluded,
    )

    for name, passed in checks.items():
        if not passed:
            reasons.append(name)

    return RecommendationGoldCaseResult(
        case_id=case.case_id,
        status="ok" if not reasons else "failed",
        checks=checks,
        reasons=reasons,
        metrics={
            "baseline_count": len(baseline_1),
            "excluded_count": len(excluded),
            "stretch_count": int(meta.get("stretch_count") or 0),
        },
        prefilter_meta=meta,
        excluded=excluded,
    )


def _check_determinism(
    *,
    baseline_1: list[dict[str, Any]],
    baseline_2: list[dict[str, Any]],
    pack_1: dict[str, Any],
    pack_2: dict[str, Any],
) -> bool:
    ids_1 = [str(item.get("school_id")) for item in baseline_1]
    ids_2 = [str(item.get("school_id")) for item in baseline_2]
    if ids_1 != ids_2:
        return False

    scenarios_1 = _to_dict_list(pack_1.get("scenarios"))
    scenarios_2 = _to_dict_list(pack_2.get("scenarios"))
    if len(scenarios_1) != len(scenarios_2):
        return False
    for s1, s2 in zip(scenarios_1, scenarios_2, strict=False):
        if str(s1.get("id")) != str(s2.get("id")):
            return False
        rows_1 = [str(item.get("school_id")) for item in _to_dict_list(s1.get("schools"))]
        rows_2 = [str(item.get("school_id")) for item in _to_dict_list(s2.get("schools"))]
        if rows_1 != rows_2:
            return False
    return True


def _check_scenario_shape(scenario_pack: dict[str, Any]) -> bool:
    scenarios = _to_dict_list(scenario_pack.get("scenarios"))
    ids = [str(item.get("id")) for item in scenarios]
    if ids != SCENARIO_IDS:
        return False
    baseline = _to_dict_list(scenario_pack.get("baseline"))
    if not baseline:
        return False
    required_fields = ("rank", "baseline_rank", "rank_delta", "scenario_reason", "outcome_breakdown")
    for row in baseline:
        for field in required_fields:
            if field not in row:
                return False
    for scenario in scenarios:
        for row in _to_dict_list(scenario.get("schools")):
            for field in required_fields:
                if field not in row:
                    return False
            if row.get("prefilter_tag") is None:
                return False
    return True


def _check_budget_hard_gate(
    *,
    baseline: list[dict[str, Any]],
    budget_cap: Any,
) -> bool:
    if not isinstance(budget_cap, int) or budget_cap <= 0:
        return True
    for row in baseline:
        tag = str(row.get("prefilter_tag") or "")
        net_price = _to_int_or_none(_to_dict(row.get("school_info")).get("avg_net_price"))
        if tag == "eligible":
            if net_price is None or net_price > budget_cap:
                return False
        if tag == "stretch":
            if net_price is None or net_price <= budget_cap:
                return False
    return True


def _check_case_expectations(
    *,
    case: RecommendationGoldCase,
    baseline: list[dict[str, Any]],
    scenario_pack: dict[str, Any],
    excluded: list[dict[str, Any]],
) -> bool:
    exp = case.expectations
    scenario_by_id = {
        str(item.get("id")): _to_dict_list(item.get("schools"))
        for item in _to_dict_list(scenario_pack.get("scenarios"))
    }

    top1_by_scenario = _to_dict(exp.get("top1_by_scenario"))
    for scenario_id, expected_school_id in top1_by_scenario.items():
        schools = scenario_by_id.get(str(scenario_id))
        if not schools:
            return False
        top_school_id = str(_to_dict(schools[0]).get("school_id"))
        if top_school_id != str(expected_school_id):
            return False

    topk_contains = _to_dict(exp.get("topk_contains_by_scenario"))
    topk = int(exp.get("topk", 3) or 3)
    for scenario_id, wanted in topk_contains.items():
        schools = scenario_by_id.get(str(scenario_id))
        if not schools:
            return False
        seen = {str(_to_dict(item).get("school_id")) for item in schools[:topk]}
        for expected_school_id in _to_str_list(wanted):
            if str(expected_school_id) not in seen:
                return False

    forbidden_baseline = set(_to_str_list(exp.get("baseline_forbidden_ids")))
    if forbidden_baseline:
        baseline_ids = {str(row.get("school_id")) for row in baseline}
        if forbidden_baseline & baseline_ids:
            return False

    min_eligible = _to_int_or_none(exp.get("min_eligible_count"))
    if min_eligible is not None:
        eligible_count = sum(1 for row in baseline if str(row.get("prefilter_tag")) == "eligible")
        if eligible_count < min_eligible:
            return False

    expected_excluded_reasons = _to_dict(exp.get("excluded_reason_at_least"))
    if expected_excluded_reasons:
        reason_counts: dict[str, int] = {}
        for row in excluded:
            reason = str(row.get("reason") or "unknown")
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        for reason, min_count in expected_excluded_reasons.items():
            if int(reason_counts.get(str(reason), 0)) < int(min_count):
                return False
    return True


def _aggregate_metrics(case_results: list[RecommendationGoldCaseResult]) -> dict[str, Any]:
    case_count = len(case_results)
    passed_count = sum(1 for item in case_results if item.status == "ok")
    checks = ["determinism", "scenario_shape", "budget_hard_gate", "stretch_quota", "expectations"]
    pass_rates = {
        f"{name}_pass_rate": round(
            (sum(1 for item in case_results if item.checks.get(name)) / case_count) if case_count else 0.0,
            6,
        )
        for name in checks
    }
    return {
        "case_count": case_count,
        "passed_case_count": passed_count,
        "case_pass_rate": round((passed_count / case_count) if case_count else 0.0, 6),
        **pass_rates,
    }


def _grade_status(metrics: dict[str, Any]) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if float(metrics.get("case_pass_rate", 0.0)) < 0.9:
        reasons.append("case_pass_rate<0.9")
    if float(metrics.get("determinism_pass_rate", 0.0)) < 1.0:
        reasons.append("determinism_pass_rate<1.0")
    if float(metrics.get("scenario_shape_pass_rate", 0.0)) < 1.0:
        reasons.append("scenario_shape_pass_rate<1.0")
    if float(metrics.get("budget_hard_gate_pass_rate", 0.0)) < 1.0:
        reasons.append("budget_hard_gate_pass_rate<1.0")
    return ("ok" if not reasons else "failed"), reasons


def _write_artifacts(output_root: Path, report: RecommendationGoldEvalReport) -> None:
    run_dir = output_root / report.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_json(run_dir / "report.json", report.to_dict())
    _write_json(run_dir / "cases.json", [item.to_dict() for item in report.cases])
    _write_markdown_summary(run_dir / "summary.md", report)
    _append_history(output_root / "history.csv", report)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_markdown_summary(path: Path, report: RecommendationGoldEvalReport) -> None:
    lines = [
        f"# Recommendation Gold Eval {report.run_id}",
        "",
        f"- status: `{report.status}`",
        f"- case_count: `{report.metrics.get('case_count', 0)}`",
        f"- passed_case_count: `{report.metrics.get('passed_case_count', 0)}`",
        f"- case_pass_rate: `{report.metrics.get('case_pass_rate', 0)}`",
        f"- determinism_pass_rate: `{report.metrics.get('determinism_pass_rate', 0)}`",
        f"- scenario_shape_pass_rate: `{report.metrics.get('scenario_shape_pass_rate', 0)}`",
        f"- budget_hard_gate_pass_rate: `{report.metrics.get('budget_hard_gate_pass_rate', 0)}`",
        f"- stretch_quota_pass_rate: `{report.metrics.get('stretch_quota_pass_rate', 0)}`",
        f"- expectations_pass_rate: `{report.metrics.get('expectations_pass_rate', 0)}`",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _append_history(path: Path, report: RecommendationGoldEvalReport) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "run_id": report.run_id,
        "generated_at": report.generated_at,
        "status": report.status,
        "case_count": report.metrics.get("case_count", 0),
        "passed_case_count": report.metrics.get("passed_case_count", 0),
        "case_pass_rate": report.metrics.get("case_pass_rate", 0),
        "determinism_pass_rate": report.metrics.get("determinism_pass_rate", 0),
        "scenario_shape_pass_rate": report.metrics.get("scenario_shape_pass_rate", 0),
        "budget_hard_gate_pass_rate": report.metrics.get("budget_hard_gate_pass_rate", 0),
        "stretch_quota_pass_rate": report.metrics.get("stretch_quota_pass_rate", 0),
        "expectations_pass_rate": report.metrics.get("expectations_pass_rate", 0),
    }
    exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def _to_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _to_dict_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


def _to_str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _to_int_or_none(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def main() -> None:
    report = asyncio.run(
        run_recommendation_gold_eval(
            dataset_path=DEFAULT_DATASET_PATH,
            output_dir=DEFAULT_OUTPUT_DIR,
        )
    )
    print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
