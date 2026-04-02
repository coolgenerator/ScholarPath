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
| Task Queue | Celery (async DeepSearch, conflict detection, causal training lifecycle) |
| Causal Engine | Legacy DAG (networkx) + PyWhy stack (causal-learn, DoWhy, EconML) |
| Deploy | Docker Compose (6 services, incl. isolated `causal_train` worker) |

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

# Playwright full blocking regression (contract + live)
npm --prefix frontend run e2e:full

# Playwright legacy route probe only (fail-fast hard-cut check)
npm --prefix frontend run e2e:live:legacy

# CI-equivalent gate (requires docker-compose frontend/app stack already running)
npm --prefix frontend run e2e:full:ci

# Advisor orchestrator gold eval (dual lane: stub + real)
python -m scholarpath.scripts.advisor_orchestrator_eval \
  --execution-lane both \
  --real-capabilities undergrad.school.recommend,undergrad.school.query,offer.compare,offer.what_if \
  --warning-gate
```

### Causal Training (High-Quality Local Profile)

```bash
# Recommended Docker Desktop resources on this machine profile:
# CPU=8, Memory=12GB, Swap>=4GB

# Optional: seed real+synthetic training assets only
python -m scholarpath.scripts.causal_build_training_assets \
  --seed-cases 40 \
  --synthetic-multiplier 4

# Activate/seed/train/promote via Celery with parallel bootstrap + checkpoint + early stop
python -m scholarpath.scripts.causal_activate_pywhy \
  --seed-cases 40 \
  --synthetic-multiplier 4 \
  --bootstrap-iters 300 \
  --stability-threshold 0.75 \
  --lookback-days 540 \
  --bootstrap-parallelism 4 \
  --checkpoint-interval 25 \
  --early-stop-patience 40 \
  --resume-from-checkpoint
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
CAUSAL_ENGINE_MODE=shadow
CAUSAL_MODEL_VERSION=latest_stable
CAUSAL_PROXY_LABELS_ENABLED=true
CAUSAL_SHADOW_LOGGING=true
# Optional: tune DeepSearch throughput
DEEPSEARCH_SCHOOL_CONCURRENCY=8
DEEPSEARCH_SOURCE_HTTP_CONCURRENCY=16
DEEPSEARCH_SELF_EXTRACT_CONCURRENCY=12
DEEPSEARCH_INTERNAL_WEBSEARCH_CONCURRENCY=8
# Advisor internal school-query DeepSearch补齐（缺关键字段时触发）
ADVISOR_INTERNAL_DEEPSEARCH_ENABLED=true
ADVISOR_INTERNAL_DEEPSEARCH_FRESHNESS_DAYS=90
ADVISOR_INTERNAL_DEEPSEARCH_MAX_INTERNAL_WEBSEARCH_PER_SCHOOL=1
ADVISOR_INTERNAL_DEEPSEARCH_BUDGET_MODE=balanced
ADVISOR_STYLE_POLISH_ENABLED=true
ADVISOR_STYLE_POLISH_CAPABILITIES=undergrad.school.recommend,offer.compare,offer.what_if
ADVISOR_STYLE_POLISH_MAX_TOKENS=600
ADVISOR_STYLE_POLISH_TEMPERATURE=0.2
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `WS /api/advisor/v1/sessions/{session_id}/stream` | Advisor v1 unified streaming endpoint |
| `GET /api/advisor/v1/sessions/{session_id}/history` | Load advisor session history |
| `GET /api/schools/` | Search & list schools |
| `GET /api/evaluations/students/{id}/tiers` | Tiered school list |
| `POST /api/offers/students/{id}/offers` | Record admission offers |
| `GET /api/offers/students/{id}/offers/compare` | Financial + causal offer comparison matrix |
| `POST /api/simulations/students/{id}/schools/{id}/what-if` | Causal what-if simulation |
| `POST /api/simulations/students/{id}/compare-scenarios` | Multi-scenario (cross-school) causal comparison |
| `POST /api/reports/students/{id}/offers/{id}/go-no-go` | Generate Go/No-Go report |
| `POST /api/vectors/search/schools` | pgvector semantic school search |
| `POST /api/tasks/causal/train` | Trigger full-graph PyWhy training task |
| `POST /api/tasks/causal/promote/{modelVersion}` | Promote trained model to active |
| `POST /api/tasks/causal/shadow-audit` | Trigger shadow comparison quality audit |
| `GET /api/tasks/{task_id}` | Poll queued task status |
| `GET /api/tasks/{task_id}/result` | Fetch completed task result |
| `GET /api/usage/summary` | Token usage analytics (`?days=` optional) |
| `GET /api/sessions/student/{id}` | List chat sessions |

`/api/chat/*` has been fully removed and is no longer mounted.
Legacy duplicated paths are hard-cut and intentionally return `404`:
`/api/api/seed/*`, `/api/offers/offers/*`, `/api/reports/reports/*`, `/api/tasks/tasks/*`,
`/api/schools/schools/*`, `/api/evaluations/evaluations/*`, `/api/sessions/sessions/*`.

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
├── advisor/          # Advisor orchestrator + contracts + adapters
├── chat/             # Shared memory + handler layer reused by advisor adapters
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
