# ScholarPath xcode JSON Methodology

## Scope
- Provider mode: `xcode`
- Endpoint: `https://api.xcode.best/v1`
- Model: `gpt-5.4-mini`
- Goal: stable JSON output for helper/judge/recommendation orchestration.

## Key Findings
1. Use `/v1` base URL only.
- `https://api.xcode.best` (no `/v1`) returns HTML page, not API JSON.

2. Non-stream chat/responses are unreliable on xcode for content extraction.
- `chat.completions` non-stream can return `choices[0].message.content = null`.
- `responses` can return `output = []` and `output_text = null`.

3. Streamed chat is stable for JSON output.
- `chat.completions` with `stream=true` consistently emits text chunks.
- Works with `response_format=json_schema` and `response_format=json_object`.

4. xcode validates JSON schema strictly.
- If object schema has no `properties`, provider can reject with
  `invalid_json_schema` / `object schema missing properties`.
- If caller passes malformed schema (for example `{"type": null}`), provider can reject with
  `schema must be a JSON Schema of 'type: "object"'`.

## Production Strategy (Current)
1. Always request JSON via `response_format.type = "json_schema"` in `complete_json`.
2. For xcode mode, route `complete_json` through streamed chat and aggregate chunks.
3. Parse aggregated text as JSON object.
4. Normalize malformed caller schema before request; invalid schema falls back to default object schema.
5. Keep same-task retry/failover behavior for transient provider errors.

## Default Schema Rule
When caller does not provide a schema, use a permissive object schema that still includes `properties`:

```json
{
  "type": "object",
  "properties": {
    "_": {
      "type": "string",
      "description": "Optional placeholder. Real output can use any keys."
    }
  },
  "additionalProperties": true
}
```

Rationale: keeps output flexible while satisfying xcode's schema validation.

## Recommended JSON Call Pattern
1. Keep system instruction explicit: `Return JSON only`.
2. Provide tight schema whenever possible (`required`, `additionalProperties`).
3. Use low temperature (`0.0-0.2`) for deterministic structured outputs.
4. For xcode path, prefer stream aggregation for JSON transport.

## Validation Checklist Before Training/Eval
1. Config sanity:
- `LLM_ACTIVE_MODE=xcode`
- `base_url=https://api.xcode.best/v1`
- expected keys count and RPM budget.

2. Live probe:
- run 10-20 `complete_json` calls with schema.
- pass threshold: success rate >= 0.98.

3. No-schema probe:
- run 10 calls without schema.
- expect no `invalid_json_schema` error.

4. Endpoint health:
- both keys have traffic in `endpoint_health(window=120s)`.
- no persistent single-key starvation.

## Known Failure Signatures
1. `message.content = null` (chat non-stream path).
2. `output_text = null`, `output = []` (responses path).
3. `invalid_json_schema` with `object schema missing properties`.
4. `invalid_json_schema` with `schema ... got 'type: "None"'` when upstream schema is malformed.

## Recovery Playbook
1. Confirm base URL contains `/v1`.
2. Force streamed chat for JSON path on xcode.
3. Ensure default schema has `properties`, and malformed caller schema is normalized to object schema.
4. Re-run live probes (with schema + no schema).
5. If still unstable, stop training and escalate with probe artifacts.

## File References
- LLM client: `/Users/lishehao/Desktop/Project/ScholarPath/scholarpath/llm/client.py`
- JSON tests: `/Users/lishehao/Desktop/Project/ScholarPath/tests/test_llm_client_complete_json.py`
