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
  - gateway policy file controls endpoint-by-endpoint RPM
  - default beecode profile: three endpoints, ~66 RPM each
  - total <= 200 RPM
  - prefer smooth, low-spike execution
- LLM gateway is config-driven:
  - `.env` chooses `LLM_ACTIVE_MODE` + `LLM_ACTIVE_POLICY`
  - `scholarpath/data/llm_gateway_policies.json` defines endpoint routing and method policies
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
