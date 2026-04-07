# ScholarPath Quick Start

## Project Goal
- Build an undergrad admissions assistant with three core layers:
  - Advisor: chat-based guidance, re-edit, complex outputs, and structured recommendations.
  - Dashboard: portfolio/profile editing and school preference persistence.
  - Causal / DeepSearch: real-data admissions modeling and official-fact enrichment.
- The long-term goal is practical admissions planning with high trust, clear UI, and a causal model trained on real truth first.

## Current North Star
- Use **real, verifiable truth** first.
- For causal training, `admission_probability` should stay `true-only`.
- Official facts should come from:
  - College Scorecard
  - IPEDS / College Navigator
  - school official admissions / financial-aid / CDS pages
  - direct verified user admission events and offers
- Do **not** treat proxy or synthetic labels as primary supervision.

## Important Operating Rules
- Keep all LLM usage under the project RPM policy:
  - two xcode endpoints, 100 RPM each
  - total <= 200 RPM
  - prefer smooth, low-spike execution
- Clean surrounding technical debt when replacing business logic.
- Keep UI copy Chinese-first unless a product reason says otherwise.
- Prefer canonical contracts over one-off fields.
- If a task touches causal / deepsearch / advisor data, check the true model and data path first, not just the UI.

## Data Strategy
- Best training sources, in order:
  - real `admission_events` / `offers` / imported historical admissions data
  - College Scorecard
  - school official admissions / CDS pages
  - IPEDS / College Navigator
  - Common App trend reports only for macro context, not as school-level truth
- `cds_url` may be empty in the DB. In that case, use `website_url` to probe official admissions/CDS pages with bounded direct fetch fallback.
- Official ingestion must still flow through:
  - normalize
  - LLM extract
  - LLM judge
  - canonical merge
  - dataset build
- IPEDS/CN is now wired as a first-class `official` source (`ipeds_college_navigator`) and uses persistent `school_external_ids` mapping.
- Common App is now wired as trend-only (`causal_trend_signals`) and must never write canonical facts or causal outcome labels.

## Phase1 Closure Gate
- Scorecard bulk should be pinned to:
  - `SCORECARD_BULK_URL=https://ed-public-download.scorecard.network/downloads/Most-Recent-Cohorts-Institution_05192025.zip`
- Run Phase1 with default strong gate:
  - `python -m scholarpath.scripts.admission_data_phase1_pipeline --scope existing_65 --output-dir .benchmarks/official_phase1`
- Default gate pass criteria:
  - `mapped_school_rate == 1.0`
  - `admit_rate_school_coverage >= 0.95`
  - `avg_net_price_school_coverage >= 0.95`
  - `college_scorecard_bulk.rows_read > 0`
  - `admission_events` and `causal_outcome_events` unchanged pre/post run
- On gate failure:
  - CLI exits non-zero
  - Inspect `phase1_report.json` `gate.reasons` and `truth_counts` for root cause

## Phase4 Training Prep (Strict True-Only, Multi-Outcome)
- Prepare non-admission `true` labels from official school-year facts before staged training:
  - `python -m scholarpath.scripts.admission_data_phase4_training_prep --output-dir .benchmarks/official_phase4`
- This prep writes only `label_type=\"true\"` for:
  - `academic_outcome <- graduation_rate_4yr`
  - `career_outcome <- percentile_rank(median_earnings_10yr, by metric_year)`
  - `life_satisfaction <- retention_rate`
  - `phd_probability <- doctoral_completions_share`
- Optional completions source for PhD metric:
  - `IPEDS_COMPLETIONS_DATASET_PATH` / `IPEDS_COMPLETIONS_DATASET_URL`
- Artifacts:
  - `stage_readiness.json`
  - `stage_readiness.md`
- Then run stage1:
  - `python -m scholarpath.scripts.causal_staged_train --stage 1 --max-rpm-total 180 --judge-concurrency 2 --train-candidates-per-stage 3`

## Recommendation Gold Eval (Prefilter + Multi-Scenario)
- Run deterministic gold eval for recommendation inference framework:
  - `python -m scholarpath.scripts.recommendation_gold_eval`
- Optional:
  - `--sample-size 6`
  - `--case-ids budget_hard_gate_001,major_priority_001`
  - `--eval-run-id recommendation-gold-<tag>`
- Artifacts:
  - `.benchmarks/recommendation_gold/<run_id>/report.json`
  - `.benchmarks/recommendation_gold/<run_id>/summary.md`
  - `.benchmarks/recommendation_gold/history.csv`
- Default pass semantics:
  - `case_pass_rate >= 0.9`
  - `determinism_pass_rate == 1.0`
  - `scenario_shape_pass_rate == 1.0`
  - `budget_hard_gate_pass_rate == 1.0`

## Recommendation UX Gold Eval V2 (Persona + AB Judge)
- Baseline collection (no judge):
  - `python -m scholarpath.scripts.recommendation_ux_gold_eval --dataset mini --no-judge --candidate-run-id recommendation-ux-baseline-<tag>`
- Candidate run with A/B judge:
  - `python -m scholarpath.scripts.recommendation_ux_gold_eval --dataset mini --baseline-run-id recommendation-ux-baseline-<tag> --judge-concurrency 2 --max-rpm-total 180`
- Artifacts:
  - `.benchmarks/recommendation_ux/<run_id>/report.json`
  - `.benchmarks/recommendation_ux/<run_id>/case_results.jsonl`
  - `.benchmarks/recommendation_ux/<run_id>/judge_case_results.jsonl`
  - `.benchmarks/recommendation_ux/<run_id>/summary.md`
  - `.benchmarks/recommendation_ux/history.csv`

## Current Product Threads
- Portfolio contract is being unified.
  - Dashboard manual edits, Advisor guided intake, and SchoolList preferences should all use the same canonical portfolio contract.
  - Avoid dual-write logic or drifting preference keys.
- Causal pipeline is centered on admission truth.
  - `admission_probability` is the only active true-only supervision.
  - Other outcomes may exist in schema but should not be mixed into primary truth.
- DeepSearch official facts are being thickened.
  - Keep bounded official direct fetch fallback.
  - Do not turn it into a broad web crawler.
- Advisor still has structured outputs and re-edit flows.
  - Complex outputs need style/polish layers only if they do not change values or contract shape.

## Route Contract Notes
- Seed routes are normalized to `/api/seed/*` (not `/api/api/seed/*`).
- Task routes are normalized to `/api/tasks/{task_id}` and `/api/tasks/{task_id}/result`.
- `/api/causal-data/*` write compatibility routes are removed; write through `/api/students/{id}/admission-evidence` and `/api/students/{id}/admission-events`.
- `POST /api/simulations/students/{id}/compare-scenarios` requires `school_id` in each scenario.
- `POST /api/schools/students/{id}/school-list` is synchronous and returns completed payload directly.

## Causal V2 (Shadow Module)
- A standalone future-replacement module is available at `scholarpath/causal_v2`.
- It is intentionally **not** wired into current API/service mainline.
- Current capabilities in V2:
  - typed `evaluate` (dimension + outcome scores + tier)
  - typed `what_if` (baseline vs modified + deltas)
  - typed `compare_scenarios` (requires explicit school profile per scenario)
- Integration seam is prepared through:
  - `CausalEngineProtocol`
  - model adapters (`student_to_causal_v2_profile`, `school_to_causal_v2_profile`)

## Key Files
- `scholarpath/services/causal_data_service.py`
- `scholarpath/services/causal_real_asset_service.py`
- `scholarpath/search/orchestrator.py`
- `scholarpath/search/official_direct_fetch.py`
- `scholarpath/search/sources/college_scorecard.py`
- `scholarpath/search/sources/ipeds_college_navigator.py`
- `scholarpath/search/trends/common_app.py`
- `scholarpath/services/portfolio_service.py`
- `scholarpath/api/routes/students.py`
- `scholarpath/chat/handlers/guided_intake.py`

## Before Editing
- Run `codex-memory status --workspace "$PWD"` if the task is medium or large.
- Inspect `git status` first; this repo often has many unrelated changes already in flight.
- Do not revert unrelated edits.
- If Docker is needed for a causal run, verify the stack is actually up.

## Good Defaults
- Prefer small, explicit helpers over hidden behavior.
- Prefer bounded fallback over generic crawling.
- Prefer test coverage for:
  - idempotency
  - provenance
  - canonical dedupe
  - truth-ratio checks
  - contract stability

## What To Be Careful About
- Do not mix `proxy` and `true` labels in causal training.
- Do not let UI-only changes hide data inconsistencies.
- Do not add new public API shapes unless the current plan explicitly calls for it.
- Do not expand source coverage in a way that increases latency or token cost without a clear gain.
