# ScholarPath Architecture V2 (RoutePlan + All-Chat Gateway)

## 1. Layered Architecture

ScholarPath runtime is explicitly split into 4 layers:

1. API/Channel
- WebSocket: `/api/chat/chat/{session_id}` for legacy client compatibility.
- HTTP: `POST /api/chat/route-turn` for external orchestrator deterministic routing.

2. Orchestration
- `ChatAgent.process_turn(...)` executes one turn with optional `route_plan + skill_id`.
- `route_plan` contract:
  - `primary_task`
  - `modifiers`
  - `required_capabilities`
  - `required_outputs`
  - `route_lock`
- output contract:
  - `route_meta`
  - `execution_digest`

3. Domain Services
- recommendation / strategy / what-if / offer compare / profile-intake handlers
- recommendation scenario behaviors via skill profile mapping

4. Infrastructure
- LLM gateway (`scholarpath/llm/client.py`)
- PostgreSQL + Redis
- Celery tasks (deepsearch/conflict/causal workers)

## 2. Route-Turn Enforcement

`route-turn` is now treated as the controlled runtime entry for orchestrators.

Enforcement behavior:
- `required_capabilities` is execution-enforced (not metadata only).
- `required_outputs` is execution-enforced (generic, not recommendation-only special case).
- missing required output/capability triggers one forced same-task retry.
- if still missing after retry, runtime returns degraded but actionable response (no silent failure / no hard crash).

`execution_digest` fields:
- `required_output_missing`
- `required_capability_missing`
- `forced_retry_count`
- `cap_retry_count`
- `cap_degraded`
- `reason_code`
- `failure_reason_code`
- `needs_input`
- `next_steps`

## 3. LLM Gateway V2 (Policy-Driven, All-Chat)

### 3.1 Configuration model

Gateway is policy-driven via:
- `.env`
  - `LLM_GATEWAY_POLICIES_PATH`
  - `LLM_ACTIVE_MODE`
  - `LLM_ACTIVE_POLICY`
- policy file (`scholarpath/data/llm_gateway_policies.json`)

Policy resolution precedence (unchanged):
- `caller_overrides` > `endpoint_overrides` > `call_defaults`

### 3.2 All-Chat JSON path

Structured generation uses Chat Completions JSON schema path:
- `chat.completions`
- `response_format.type = json_schema`

`complete_json_with_web_search` is no longer a primary runtime path.
If invoked, it returns deterministic degradation:
- `status = web_search_unavailable`
- `reason_code = responses_web_search_disabled`

### 3.3 Strict JSON governance

Policy supports `strict_json_callers`.
For strict callers:
- force `json_schema.strict = true`
- force `parse_mode = strict`
- no relaxed parse fallback
- schema mismatch becomes hard failure

Unknown caller governance:
- empty/`unknown` caller is rejected at runtime for `complete/complete_json/stream/...`.

## 4. DeepSearch Web Fallback

`internal_web_search` now uses:
- external search source + chat extraction (all-chat-compatible)
- deterministic unavailable status when search API is not configured:
  - `web_search_unavailable`

Search metadata now includes source-level status diagnostics when available.

## 5. Observability

`/api/usage/llm-endpoints` exposes per-endpoint runtime counters including JSON quality diagnostics:
- `parse_fail`
- `non_json`
- `schema_mismatch`
- plus retry/failover counters and policy/mode identifiers.

Token usage provider is mode-driven (`active_mode`) rather than hardcoded provider labels.

## 6. Rollout Notes

- default active mode remains `.env`-driven (`beecode` recommended for this rollout).
- strategy changes are applied by editing policy + `.env` and restarting services.
- no DB schema migration required for this refactor.
