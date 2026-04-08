# ScholarPath Recommendation 低分 Case Study（Mini30 live）

## 1) 评测上下文
- Run ID: `reco-gold-live-mini30-20260407-v2final2`
- 报告: `/Users/lishehao/Desktop/Project/ScholarPath/.benchmarks/recommendation_gold/reco-gold-live-mini30-20260407-v2final2/report.json`
- 总体结果:
  - `recommendation_route_hit_rate=1.0`
  - `recommendation_payload_exists_rate=1.0`
  - `hard_check_pass_rate=1.0`
  - `judge_case_score_avg=24.5667`（接近目标 25）
  - `overall_user_feel_mean=1.2283`（未达目标 1.5）
  - `deepsearch_fallback_trigger_rate=1.0`

结论：结构正确，但“可信度 + 个性化”仍弱，导致用户体感分数偏低。

## 2) 低分样本（代表）

### A. `geo_first`（最严重）
- 样本: `rec_024`（score 6）, `rec_019`（score 9）
- 共同症状:
  - `constraint_status=degraded`, `fails=geo_alignment_low`
  - 学校列表中 `geo_match` 基本全是 `0.5`（中性），未体现地域偏好
  - 高选择性学校被标成 `safety`
- Judge 反馈高频词:
  - `No geographic alignment`
  - `mislabeled as safety`
  - `trustworthiness low`

### B. `budget_first`（表面通过，实质低信任）
- 样本: `rec_005`（score 8）
- 共同症状:
  - 预算门槛字段通过，但 tier 可信度差（顶校被标 safety）
  - major/geo 没进入核心排序信号（judge 认为推荐“generic”）

### C. `risk_first`
- 样本: `rec_009`（score 12）
- 共同症状:
  - `fails=risk_tier_mix_insufficient`
  - 列表几乎全 target/safety，缺稳定的 reach-target-safety 结构
  - 约束是“事后判失败”，但前面重排阶段没有真正强制满足

### D. `roi_first`
- 样本: `rec_029`（score 8）
- 共同症状:
  - 虽然 `constraint_status=pass`，但 judge 仍给低分
  - 原因是 tier 可信度和个性化解释弱，ROI 解释停留在通用措辞

## 3) 横向证据（跨全部 bucket）

### 3.1 个性化信号塌陷
- Mini30 全 bucket 平均:
  - `avg_major_match=0.5`
  - `avg_geo_match=0.5`
- 说明 major/geo 基本没有形成有效区分。

### 3.2 数据覆盖与口径问题直接伤害可信度
- `programs` 表覆盖:
  - `schools=2593`
  - `schools_with_programs=0`
  - `program_rows=0`
- 直接导致 major 匹配回退中性（`0.5`）居多。

### 3.3 SAT 口径混用，放大录取概率
- `sat_75` 同时存在两种量纲:
  - 部分学校为 1600 制（如 Berkeley `1530`）
  - 部分学校为 800 制（如 Harvard `790`）
- 用 `student_sat_total(1600制)` 去对比 `sat_75=790` 时，`sat_fit` 会长期被打满。
- 实测 Harvard（`acceptance_rate=3.65`, `sat_25=755`, `sat_75=790`）:
  - 对 `sat=1240/1320/1400/1460/1510/1560`，`sat_fit` 全是 `1.0`
  - 校准概率约 `0.671205`（显著偏高），导致 tier 被推向 safety/target。

### 3.4 acceptance_rate 覆盖非常稀疏
- `total schools=2593`
- `acceptance_rate non-null=65`
- `null=2528`
- 绝大多数学校缺录取率，guard 难以稳定生效。

## 4) 根因归类（按影响大小）

1. **Tier 可信度根因（最大）**
   - SAT 量纲混用（800/1600）导致 `sat_fit` 虚高；
   - acceptance_rate 稀疏，且个别值异常（例如 `University of Chicago=77.35`）；
   - 共同造成 “超低录取率学校也进 safety/target”。

2. **个性化根因**
   - major 数据源为空（`programs` 无覆盖）；
   - geo 偏好输入在评测 seed 里主要用 `preferred_region`，但 canonical preference 仅保留 `location`，导致 geo 信号丢失或弱化；
   - 最终 major/geo 长期停在中性分。

3. **场景约束执行位置不对**
   - 当前更多是“重排后校验 + 降级标记”，不是“重排中硬过滤”；
   - 因此会出现“约束失败已知，但结果仍是低质量列表”的现象。

4. **DeepSearch fallback 当前无法真正补数**
   - fallback 触发率高，但本地无 celery worker 时只会记录 `celery_unavailable`；
   - 因此没有形成“下一轮质量回升”的闭环。

## 5) 针对低分的修复优先级（只列可直接执行）

### P0（先修，直接拉分）
1. SAT 量纲归一化（800/1600 自动识别并统一）：
   - 若 `sat_75<=800`，按 section 口径转换到 total 等价再算 fit，或把 student SAT拆分逻辑统一。
2. Tier guard 加强为硬上限：
   - 即使 SAT 高分，`acceptance_rate<8%` 不得进入 safety/likely（除非命中明确白名单条件）。
3. geo 偏好 canonical 补齐：
   - 把 `preferred_region` 映射进 canonical `location`（至少在 eval runner/portfolio canonical 二选一修）。

### P1（次优先）
4. Risk/Geo 场景在重排阶段做硬约束（非事后判定）：
   - risk: 先按配额抽样再排序；
   - geo: 不命中区域阈值的候选先过滤再排序。

### P2（补完闭环）
5. DeepSearch fallback worker 可用化（celery）或提供本地 async fallback 执行路径：
   - 否则 `deepsearch_fallback_triggered` 只能作为告警，不能提升推荐质量。

## 6) 为什么这能提分（对齐 judge 维度）
- `trustworthiness`：Tier 口径修正后会明显提升。
- `personalization_fit`：major/geo 不再长期 `0.5`，场景分数会拉开。
- `actionability`：risk/geo 场景先约束后排序，输出更可执行。
- `overall_user_feel_mean`：预计主要靠上述三项带动上升。
