# ScholarPath

AI-powered US college admissions agent with causal inference, semantic search, and guided chat for personalized school recommendations.

![Recommendation Cards](static/recommendation-cards.png)

## What it does

ScholarPath helps Chinese students navigate US undergraduate admissions through two core decision points:

1. **School Selection & Matching** — Guided conversational intake collects student profile (GPA, SAT, interests, budget, preferences), then uses a causal inference engine + pgvector semantic search to generate a personalized, tiered school list (Reach / Target / Safety) with explainable reasoning.

2. **Offer Comparison & Decision** — When students receive multiple offers, the system runs causal what-if analysis across academic, financial, career, and life dimensions to produce a Go/No-Go recommendation with confidence intervals.

## Screenshots

<table>
<tr>
<td width="50%">

**Guided Chat Intake**
![Chat](static/chat-guided-intake.png)

</td>
<td width="50%">

**Causal Recommendation Cards**
![Recommendations](static/recommendation-target.png)

</td>
</tr>
<tr>
<td>

**Offer Tracking & Comparison**
![Offers](static/offers-panel.png)

</td>
<td>

**School Discovery**
![Discover](static/discover-panel.png)

</td>
</tr>
<tr>
<td colspan="2">

**Session History**
![History](static/session-history.png)

</td>
</tr>
</table>

## Architecture

```
┌─────────────┐     ┌──────────────────────────────────────────┐
│   Frontend   │     │              Backend (FastAPI)            │
│  React/Vite  │◄───►│                                          │
│  TailwindCSS │ WS  │  Chat Agent ─► Intent Classification     │
│              │     │       │              │                    │
│  - Advisor   │     │       ▼              ▼                    │
│  - School    │     │  Guided Intake  School Query              │
│    List      │ REST│       │              │                    │
│  - Discover  │◄───►│       ▼              ▼                    │
│  - Offers    │     │  ┌─────────────────────────────┐         │
│  - Decisions │     │  │   Causal Inference Engine    │         │
│  - History   │     │  │  (CurioCat)                  │         │
│              │     │  │  - DAG Builder               │         │
└─────────────┘     │  │  - Noisy-OR Propagation       │         │
                    │  │  - do-calculus What-If         │         │
                    │  │  - Mediation Analysis          │         │
                    │  │  - Go/No-Go Scorer             │         │
                    │  └──────────┬──────────────────────┘         │
                    │             │                                │
                    │  ┌──────────▼──────────────────────┐         │
                    │  │  PostgreSQL + pgvector           │         │
                    │  │  - 64 schools (real data)        │         │
                    │  │  - Gemini 3072-dim embeddings    │         │
                    │  │  - Token usage tracking          │         │
                    │  └─────────────────────────────────┘         │
                    │             │                                │
                    │  ┌──────────▼──────────┐                     │
                    │  │  Redis               │                     │
                    │  │  - Chat memory       │                     │
                    │  │  - Session state     │                     │
                    │  └─────────────────────┘                     │
                    └──────────────────────────────────────────────┘
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React 18, Vite, TailwindCSS 4, React Router, react-markdown |
| Backend | Python 3.12, FastAPI, WebSocket, SQLAlchemy 2.0 (async) |
| LLM | OpenAI-compatible API (gpt-5.4-mini via xcode.best) |
| Embeddings | Google Gemini `gemini-embedding-001` (3072-dim) |
| Database | PostgreSQL 16 + pgvector for semantic search |
| Cache | Redis 7 (chat memory, session state, Celery broker) |
| Task Queue | Celery (deep_search/conflict + causal_train workers + beat) |
| Causal Engine | networkx + numpy (DAG, Noisy-OR, do-calculus) |
| Deploy | Docker Compose (7 services) |

## Key Features

- **Causal Inference Engine** — Domain-constrained DAG with 16 admission-relevant nodes, Noisy-OR belief propagation, do-calculus for what-if simulation, mediation analysis decomposing school effects into 4 causal pathways (research opportunities, peer network, brand signal, career services)
- **Guided Conversational Intake** — 7-step profile builder with interactive option cards (click to select or type custom answers), auto-detects user language (EN/ZH)
- **pgvector Semantic Search** — Gemini embeddings for student profiles and schools, cosine similarity pre-filtering before causal evaluation
- **Structured Recommendation Cards** — Tiered school list (Reach/Target/Safety) with per-school score bars, admission probability, net price, causal reason pills, and 4-dimension fit analysis
- **Session Persistence** — Redis-backed chat history with URL routing (`/s/{sessionId}/{nav}`), survives page reloads
- **Token Usage Tracking** — Every LLM call logged to DB with model, caller, tokens, latency, errors; queryable via `/api/usage/summary`
- **Rate Limiting** — 100 RPM sliding window on LLM calls
- **i18n** — Full EN/ZH bilingual UI, auto-detects from user input
- **Collapsible Sidebar** — Icon-only mode for more content space

## Quick Start

```bash
# Clone and start all services
git clone https://github.com/your-username/ScholarPath.git
cd ScholarPath
docker compose up --build -d

# Verify Celery queues
docker compose exec celery_worker celery -A scholarpath.tasks.celery_app inspect active_queues
docker compose exec celery_causal_train_worker celery -A scholarpath.tasks.celery_app inspect active_queues

# One-time cleanup before a fresh rollout (records + clears backlog)
docker compose exec redis redis-cli -n 0 LLEN deep_search
docker compose exec redis redis-cli -n 0 LLEN conflict
docker compose exec redis redis-cli -n 0 DEL deep_search conflict

# Services:
#   http://localhost:5173  — Frontend (Vite)
#   http://localhost:8000  — Backend API (FastAPI)
#   localhost:55432        — PostgreSQL + pgvector
#   localhost:56379        — Redis

# Seed school data + demo student
curl -X POST http://localhost:8000/api/seed/schools
curl -X POST http://localhost:8000/api/seed/demo-student
curl -X POST http://localhost:8000/api/seed/demo-evaluations

# Enrich with real data via LLM
curl -X POST http://localhost:8000/api/enrich/schools

# Open http://localhost:5173 and start chatting
```

### Testing

```bash
# Single-process
python -m pytest -q

# Parallel (recommended on multi-core machines)
python -m pytest -n auto -q
```

### Environment Variables

Copy `.env.example` to `.env` and fill in:

```
ZAI_API_KEY=your-openai-compatible-api-key
# Optional: load-balance requests across multiple keys
ZAI_API_KEYS=["key-1","key-2"]
ZAI_BASE_URL=https://api.xcode.best/v1
ZAI_MODEL=gpt-5.4-mini
# Per-key limiter (effective in distributed mode across app/worker)
LLM_RATE_LIMIT_RPM=100
# Optional: enable DeepSearch web source
WEB_SEARCH_API_URL=
WEB_SEARCH_API_KEY=
GOOGLE_API_KEY=your-gemini-api-key
SCORECARD_API_KEY=your-data-gov-college-scorecard-api-key
SCORECARD_BULK_URL=https://ed-public-download.scorecard.network/downloads/Most-Recent-Cohorts-Institution_05192025.zip
SCORECARD_BULK_PATH=
# Optional: IPEDS/CN official bulk dataset (CSV/JSON)
IPEDS_DATASET_URL=
IPEDS_DATASET_PATH=
IPEDS_COMPLETIONS_DATASET_URL=
IPEDS_COMPLETIONS_DATASET_PATH=
# Optional: Common App trend-only dataset (CSV/JSON)
COMMON_APP_TREND_URL=
COMMON_APP_TREND_PATH=
# Optional: tune DeepSearch throughput
DEEPSEARCH_SCHOOL_CONCURRENCY=8
DEEPSEARCH_SOURCE_HTTP_CONCURRENCY=16
DEEPSEARCH_SELF_EXTRACT_CONCURRENCY=12
DEEPSEARCH_INTERNAL_WEBSEARCH_CONCURRENCY=8
```

### Real Admission Pipeline (strict mini-before-full)

```bash
python -m scholarpath.scripts.causal_real_admission_pipeline \
  --ingest-ipeds \
  --top-schools 1000 \
  --years 5 \
  --school-selection applicants \
  --ingest-common-app-trends \
  --events-file scholarpath/data/admission_events_seed.json \
  --cycle-year 2026 \
  --max-rpm-total 180 \
  --judge-concurrency 2 \
  --full-candidates 3
```

The script enforces `Gate0 (docker+alembic+tables) -> mini gate -> full stage4 K=3` and does **not** auto-promote the full-run champion. Common App signals are trend-only and are not used as causal truth labels.

### Phase4 Training Prep (strict true-only multi-outcome)

```bash
python -m scholarpath.scripts.admission_data_phase4_training_prep \
  --lookback-days 540 \
  --target-eligible-snapshots 3500 \
  --target-rpm-total 180 \
  --rpm-band-low 170 \
  --rpm-band-high 185 \
  --output-dir .benchmarks/official_phase4
```

This prep materializes non-admission `true` labels from official school-year facts only:
- `academic_outcome <- graduation_rate_4yr`
- `career_outcome <- percentile_rank(median_earnings_10yr, by metric_year)`
- `life_satisfaction <- retention_rate`
- `phd_probability <- doctoral_completions_share`

Artifacts are written to `stage_readiness.json` and `stage_readiness.md`.

### Admission Phase1 (Bronze/Silver + closure gate)

```bash
python -m scholarpath.scripts.admission_data_phase1_pipeline \
  --scope existing_65 \
  --output-dir .benchmarks/official_phase1
```

Default gate is strict and exits non-zero on failure:
- `mapped_school_rate == 1.0`
- `admit_rate_school_coverage >= 0.95`
- `avg_net_price_school_coverage >= 0.95`
- `college_scorecard_bulk.rows_read > 0`
- `admission_events` and `causal_outcome_events` must remain unchanged

Troubleshooting:
- Check `phase1_report.json` `gate.reasons` and `truth_counts`.
- Use `--no-gate` only for diagnostics; do not use it for normal batch closure.

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `WS /api/chat/chat/{sessionId}` | Real-time chat via WebSocket |
| `GET /api/chat/history/{sessionId}` | Load chat history |
| `GET /api/schools/` | Search & list schools |
| `POST /api/schools/students/{id}/school-list` | Generate school list (synchronous) |
| `GET /api/evaluations/students/{id}/tiers` | Tiered school list |
| `POST /api/offers/students/{id}/offers` | Record admission offers |
| `POST /api/simulations/students/{id}/schools/{id}/what-if` | Causal what-if simulation |
| `POST /api/reports/students/{id}/offers/{id}/go-no-go` | Generate Go/No-Go report |
| `POST /api/vectors/search/schools` | pgvector semantic school search |
| `GET /api/usage/summary` | Token usage analytics (`?days=` optional) |
| `GET /api/sessions/student/{id}` | List chat sessions |
| `POST /api/students/{id}/admission-evidence` | Write evidence artifact (authoritative) |
| `POST /api/students/{id}/admission-events` | Write admission event (authoritative) |
| `GET /api/causal/datasets/{version}` | Read causal dataset version |
| `GET /api/tasks/{task_id}` | Poll task status |
| `GET /api/tasks/{task_id}/result` | Read completed task result |

`/api/causal-data/*` write routes were removed. Use `/api/students/{id}/admission-evidence` and `/api/students/{id}/admission-events`.

`/api/simulations/students/{id}/compare-scenarios` now requires each scenario item to include `school_id`.

## Project Structure

```
scholarpath/
├── api/              # FastAPI routes + Pydantic schemas
├── causal/           # CurioCat causal inference engine
│   ├── dag_builder.py        # Domain-constrained DAG (16 nodes, 22 edges)
│   ├── belief_propagation.py # Noisy-OR propagation
│   ├── do_calculus.py        # Interventional what-if analysis
│   ├── mediation.py          # Causal pathway decomposition
│   ├── backdoor.py           # Confounder adjustment
│   └── go_no_go.py           # Composite scoring engine
├── chat/             # Chat agent + guided intake handlers
├── search/           # Open DeepSearch engine
├── services/         # Business logic (recommendation, evaluation)
├── llm/              # LLM client + embeddings + usage tracking
├── db/               # SQLAlchemy models (11 tables + pgvector)
└── tasks/            # Celery async tasks

frontend/src/
├── app/components/   # React components (ChatPanel, RecommendationCard, etc.)
├── hooks/            # useChat, useEvaluations, useStudent
├── context/          # AppContext (session, student, sidebar state)
├── lib/              # API client + TypeScript types
└── i18n/             # EN/ZH translations
```

## License

MIT
