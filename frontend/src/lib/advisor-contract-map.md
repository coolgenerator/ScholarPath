# Advisor Contract Mapping (Frontend)

Source of truth: `scholarpath/advisor/contracts.py`

## Request mapping (WS)

- `turn_id` -> generated in `frontend/src/hooks/useChat.ts`
- `session_id` -> current app session id
- `student_id` -> optional app student id
- `message` -> user input or structured action prompt
- `domain_hint` -> from `next_actions[].payload.domain_hint`
- `capability_hint` -> from `next_actions[].payload.capability_hint`
- `client_context.trigger` -> action id (`queue.run_pending`, `step.retry`, `route.clarify`, etc.)
- `edit.target_turn_id + edit.mode=overwrite` -> user message re-edit and overwrite regeneration

## Response mapping

- `assistant_text` -> bubble markdown text
- `domain` + `capability` -> route badges
- `artifacts[]` -> type-dispatched cards (`guided_intake`, `school_recommendation`, `offer_comparison`, `strategy_plan`, `what_if_result`, `info_card`)
- `done[]` -> executed section
- `pending[]` -> pending section
- `next_actions[]` -> executable action buttons (primary)
- `actions[]` -> compatibility-only labels (non-executable)
- `route_meta` -> guard/executed/pending summary and observability metadata
- `error` -> recoverable error panel

## Rendering policy

- `next_actions` has priority over `actions` for interaction.
- No marker-text parsing is used for artifacts.
- History replay now prefers DB timeline metadata (`message_id/turn_id/editable/edited/created_at`).
- Legacy Redis-only history entries are read-only fallback (`editable=false`).
