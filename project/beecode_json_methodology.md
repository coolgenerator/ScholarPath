# ScholarPath beecode JSON Methodology

## Scope
- Provider mode: `beecode`
- Endpoint: `https://beecode.cc/v1`
- Model: `gpt-5.4-mini`
- Goal: helper/judge/recommendation 链路稳定返回可解析 JSON。

## Runtime Baseline
- `LLM_GATEWAY_POLICIES_PATH=scholarpath/data/llm_gateway_policies.json`
- `LLM_ACTIVE_MODE=beecode`
- `LLM_ACTIVE_POLICY=default`
- `BEECODE_API_KEY_1/2/3` 已设置（由策略文件 endpoint 的 `api_key_env` 引用）
- `LLM_RATE_LIMIT_RPM=200`（总控）

## Recommended Transport
1. 统一使用 `chat.completions`。
2. JSON 输出统一使用 `response_format.type=json_schema`。
3. 业务 prompt 明确写 `Return JSON only`。
4. 温度建议 `0.0 ~ 0.2`（推荐 `0.0`）。

## Schema Rules
1. 能提供 schema 就提供完整 schema（`type=object` + `properties` + `required`）。
2. `additionalProperties` 按场景收紧（评测/判分建议 `false`）。
3. 上游 schema 异常时，客户端应回退到默认 object schema，避免请求失败。

## Client-side Compatibility Rules
1. beecode 路径不加额外 JSON transport hints（避免网关兼容问题）。
2. 使用精简默认 headers：
   - `Accept: application/json`
   - `Content-Type: application/json`
3. 保留 endpoint 轮询 + failover。
4. 命中 provider 上限语义时：
   - 先 `sleep 5s` 重试同任务一次；
   - 再失败才切下一个 endpoint。

## Live Probe Checklist (Before Eval/Train)
1. 配置检查
   - active mode 必须是 `beecode`
   - key 数量 = 3
   - endpoint 必须含 `/v1`
2. JSON 稳定性检查
   - schema probe: 20 次，成功率 >= 0.98
   - no-schema probe: 20 次，成功率 >= 0.98
   - 并发 probe（建议 60 次，concurrency=5）成功率 >= 0.98
3. 负载检查
   - `endpoint_health(window=120s)` 三 key 均有请求
   - 不出现单 key 长期独占

## Current Observed Result (2026-04-07)
- schema probe: `20/20` 成功
- no-schema probe: `20/20` 成功
- stress probe (`60`, concurrency `5`): `60/60` 成功
- 近窗口无 rate-limit/timeout，三 key 请求分布均衡（约 `34/33/33`）

## Failure Signatures To Watch
1. 返回非 JSON 文本（解析失败）。
2. JSON 可解析但 shape 不匹配 schema（字段缺失/类型错）。
3. provider 报 `too many pending requests` / `request reached limit`。
4. 单 key 连续失败导致可用吞吐下降。

## Recovery Playbook
1. 先确认 `LLM_ACTIVE_MODE=beecode` 且 base_url 是 `https://beecode.cc/v1`。
2. 强制 `chat.completions + response_format=json_schema`。
3. schema 收紧后仍抖动时，先降温到 `0.0` 并缩短 prompt。
4. 检查 `endpoint_health`：
   - 若某 key 持续异常，先隔离该 key 再测。
5. 若仍不稳定，暂停训练，先留存 probe 输出与失败样本再定位。

## Minimal Call Template
```python
result = await llm.complete_json(
    messages=[
        {"role": "system", "content": "Return JSON only."},
        {"role": "user", "content": "请输出结构化结果。"},
    ],
    schema={
        "type": "object",
        "properties": {
            "ok": {"type": "boolean"},
            "items": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["ok", "items"],
        "additionalProperties": False,
    },
    temperature=0.0,
    max_tokens=400,
    caller="your.caller.id",
)
```
