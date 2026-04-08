# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

ScholarPath is an AI-powered US college admissions assistant for Chinese students. Three core layers: **Advisor** (chat-based guidance with structured recommendations), **Dashboard** (portfolio/profile editing), and **Causal/DeepSearch** (real-data admissions modeling and official-fact enrichment).

## Commands

### Backend (Python 3.12, FastAPI)

```bash
# Run all services via Docker
docker compose up --build -d

# Run tests (from repo root, needs .env loaded)
python -m pytest -q                # single-process
python -m pytest -n auto -q        # parallel (recommended)
python -m pytest tests/test_foo.py -q  # single file
python -m pytest tests/test_foo.py::test_bar -q  # single test

# Lint
ruff check scholarpath/
ruff format --check scholarpath/

# DB migrations
alembic upgrade head
alembic revision --autogenerate -m "description"

# Local dev (without Docker)
uvicorn scholarpath.main:app --reload --port 8000

# Seed data
curl -X POST http://localhost:8000/api/api/seed/schools
curl -X POST http://localhost:8000/api/api/seed/demo-student

# Celery workers
celery -A scholarpath.tasks.celery_app worker --loglevel=info -Q deep_search,conflict,celery
celery -A scholarpath.tasks.celery_app worker --loglevel=info -Q causal_train
celery -A scholarpath.tasks.celery_app beat --loglevel=info
```

### Frontend (React 18, Vite, pnpm)

```bash
cd frontend
pnpm install
pnpm dev          # http://localhost:5173
pnpm build
```

### Environment

Copy `.env.example` to `.env`. Key variables: `DATABASE_URL`, `REDIS_URL`, `LLM_GATEWAY_POLICIES_PATH`, `LLM_ACTIVE_MODE`, `GOOGLE_API_KEY`, `SCORECARD_API_KEY`. Local ports: PostgreSQL `55432`, Redis `56379`.

## Architecture

**Monorepo**: `scholarpath/` (Python backend) + `frontend/` (React SPA) + `alembic/` (migrations).

**Backend services** (docker-compose runs 7 containers):
- `app` — FastAPI (REST + WebSocket)
- `celery_worker` — queues: `deep_search`, `conflict`
- `celery_causal_train_worker` — queue: `causal_train`
- `celery_beat` — scheduler
- `postgres` — PostgreSQL 16 + pgvector
- `redis` — session state, chat memory, Celery broker
- `frontend` — Vite dev server with API proxy

**LLM Gateway** (`scholarpath/llm/client.py`): All LLM calls go through a policy-driven gateway. Policies defined in `scholarpath/data/llm_gateway_policies.json` control endpoint routing, per-caller RPM limits, and method-level tuning. `.env` selects active mode (`LLM_ACTIVE_MODE`) and policy (`LLM_ACTIVE_POLICY`). Total RPM must stay <= 200.

**Causal Engine** (`scholarpath/causal/`): Domain-constrained DAG (16 nodes, 22 edges) with Noisy-OR belief propagation, do-calculus for what-if simulation, mediation analysis (4 causal pathways), and Go/No-Go composite scoring. Built on networkx + numpy.

**Chat Agent** (`scholarpath/chat/`): WebSocket at `/api/chat/chat/{sessionId}` for real-time chat. HTTP `POST /api/chat/route-turn` for deterministic RoutePlan-controlled turns. Guided intake (7 steps) with intent classification -> handler dispatch.

**DeepSearch** (`scholarpath/search/`): Bounded orchestrator fetching official facts from College Scorecard, IPEDS, CDS pages, school websites. Pipeline: normalize -> LLM extract -> LLM judge -> canonical merge -> dataset build.

**Frontend**: React Router SPA at `/s/{sessionId}/{nav}`. Panels: Advisor, SchoolList (Reach/Target/Safety tiers), Discover, Offers, Decisions, History, Profile. Bilingual EN/ZH with auto-detection. State via React context + Redis-backed sessions.

## Data Strategy Rules

These are critical invariants:

- `admission_probability` is **true-only** supervision. Never mix proxy or synthetic labels into causal training.
- Truth source priority: real admission_events/offers > College Scorecard > school official/CDS pages > IPEDS > Common App trends.
- **Common App is trend-only** — macro context signals only, never canonical facts or causal outcome labels.
- IPEDS/CN is a first-class `official` source using persistent `school_external_ids` mapping.
- Portfolio contract (dashboard edits, advisor intake, school preferences) must use one canonical contract — no dual-write.

## Coding Conventions

- Python: ruff, line-length 99, target Python 3.12
- Tests: pytest with `asyncio_mode = "auto"`. Tests use fakeredis and httpx async client.
- UI copy is Chinese-first unless there's a product reason otherwise.
- Prefer canonical contracts over one-off fields.
- Prefer bounded fallback over generic crawling in data sources.
- Do not add new public API shapes unless the current plan explicitly calls for it.
- Do not expand source coverage in ways that increase latency or token cost without clear gain.
- If a task touches causal/deepsearch/advisor data, verify the data model and data path first, not just the UI.
