# AI 选校 / 选专业顾问产品规格说明书（Spec）

**版本**：v0.1  
**日期**：2026-04-03  
**状态**：Draft  
**作者**：OpenAI / ChatGPT（基于本轮公开信息研究整理）

---

## 1. 文档目的

本文档用于定义一个面向学生的 **AI 选校 / 选专业顾问产品** 的产品范围、核心能力、数据策略、因果推理路径、系统架构、训练规模与落地路线。

目标不是做“会聊天的 AI”，而是做一个可解释、可持续迭代的 **decision engine（决策引擎）**：

- 帮学生完成 **专业方向探索**
- 基于预算、职业目标、环境偏好、录取风险做 **学校-专业对推荐**
- 用结构化事实、规则与实验数据支持推荐
- 随着用户行为和实验积累，逐步建立个体化偏好模型和因果估计能力

---

## 2. 背景与市场结论

### 2.1 市场定义

“AI college application / admissions” 并不是单一市场，而是三个互相连接的市场：

1. **学生侧**：AI 选校、选专业、文书、申请流程管理
2. **顾问/家长侧**：顾问增效、协作、决策支持
3. **高校侧**：招生 CRM、身份核验、转化、申请审理、录取支持

本产品聚焦 **学生侧的选校 / 选专业顾问**，但在设计上预留向顾问协作和学校端扩展的能力。

### 2.2 核心市场判断

- 真正可持续的价值，不在“AI 帮写文书”，而在：
  - 数据
  - 决策逻辑
  - 工作流
  - 可解释性
  - 真实性与合规
- 学生端最容易同质化，平台型护城河弱
- 高校端预算更稳定，但个人开发不适合一开始进入 B2B
- 对个人开发者，最现实的路径是：
  - 切窄人群
  - 做深层非标准化事实库
  - 做高质量 decision engine
  - 用第一方交互数据形成偏好与实验闭环

### 2.3 代表竞品类型

#### A. 学生端匹配/顾问型
- CollegeVine / Sage
- MatchMyMajor.ai
- CollegeNAV
- PersonaPick
- PortScholar
- Ambitio
- Counsely
- Goomi

#### B. 中文/留学顾问型
- 新东方 AI 智能选校
- 慧选校
- AI 选校互动

#### C. 学校 / 平台型
- CollegeVine（高校 AI）
- Niche
- Element451
- Slate AI
- Advisor.AI

### 2.4 竞争结论

学生侧公开数据和基础匹配逻辑已经高度普及，单纯“给你推荐几所学校”的产品没有长期护城河。

本产品需要把护城河放在：

1. **学校-专业对级别的推荐**，而不是学校排名
2. **非标准化规则信息的结构化**
3. **偏好学习与解释系统**
4. **第一方实验日志与因果估计能力**

---

## 3. 产品愿景与定位

### 3.1 产品一句话定义

一个面向学生的 **AI 选校 / 选专业决策系统**，基于兴趣、能力、预算、职业目标和录取风险，推荐最匹配的 **学校-专业组合**，并说明推荐原因、约束条件与替代方案。

### 3.2 不做什么

- 不做纯聊天问答机器人
- 不做代写文书产品
- 不做简单排行榜聚合器
- 不承诺“录取概率精确预测”
- 不在早期进入复杂学校端 CRM/招生系统市场

### 3.3 核心价值主张

对学生：
- 从“我不知道学什么/去哪”到“我有一组可解释的 shortlist”
- 推荐基于专业、预算、职业目标和环境偏好，而不是泛学校排名

对家长：
- 能看到推荐依据、成本因素、风险层级
- 降低“只看名气/排名”带来的误判

对未来顾问合作方：
- 能用系统生成结构化 shortlist 和对比分析

---

## 4. 目标用户

### 4.1 初始目标用户（推荐）

建议先切一个窄人群，而不是“所有高中生”：

**优先推荐切法之一**：
- 国际生 / 华人家庭
- 预算明确
- 重视 ROI / 实习 / 就业城市
- 专业集中在 CS / Business / Econ / Data / Engineering 这类高决策密度方向

### 4.2 次级用户

- undecided major 学生
- 正在做 school list 的高中生
- 正在比较 offer 的学生
- 协助决策的家长

### 4.3 典型用户问题

- 我适合什么专业方向？
- 哪些学校既适合我，又上得起？
- 我应该优先考虑学校名气、专业强度还是就业结果？
- 如果我以后想转专业，这所学校是不是更灵活？
- A 校 CS 和 B 校 Econ，哪个对我更合理？

---

## 5. 产品原则

1. **先专业，后学校**：先确定专业簇，再推荐学校-专业对
2. **先结构化，后生成**：LLM 用于追问和解释，不直接决定排序
3. **先可解释，后复杂化**：每个推荐都必须可回溯
4. **先决策闭环，后泛覆盖**：先做深，再做广
5. **先事实层，后因果层**：因果模型必须建立在清洁的时间序列和 treatment 定义上
6. **先实验可控变量，后远期反事实**：前期只对自己可随机化的产品动作做因果学习

---

## 6. 核心功能范围

### 6.1 MVP 必须功能

#### A. 用户画像与偏好采集
- 基本背景：年级、身份、预算、地理偏好、学术成绩、活动背景
- 兴趣/价值观测评
- 专业开放度评估（是否已确定专业）
- 风险偏好（reach/match/likely 倾向）
- pairwise tradeoff（如：收入 vs 兴趣、城市 vs 成本）

#### B. 专业方向探索
- 输出 3–5 个专业簇
- 每个专业簇解释：
  - 适合原因
  - 对应职业方向
  - 典型课程 / 能力要求
  - 替代专业

#### C. 学校-专业 shortlist 推荐
- 推荐对象是 `(school, major, credential level)`
- 显示：
  - major fit
  - affordability
  - outcome fit
  - environment fit
  - admission fit
  - optionality（转专业、双专业、undeclared 灵活性）

#### D. 解释与 what-if
- 为什么推荐这所学校的这个专业
- 为什么它排在前面
- 如果预算减少 / 更重视城市 / 更重视实习，会发生什么变化

#### E. 比较器
- A 校 X 专业 vs B 校 Y 专业
- 成本、录取风险、专业强度、灵活性、职业结果对比

### 6.2 第二阶段功能

- 家长协作视图
- 顾问协作链接
- NPC / 净价估算记录
- shortlist 历史版本
- 申请计划与 deadline 工作流
- 奖学金匹配
- offer 对比

### 6.3 不在 MVP 内的功能

- 自动代写文书
- 自动提交申请
- 完整高校 CRM
- 学校端招生 agent
- 大规模 B2B 多租户系统

---

## 7. 决策与推理框架

### 7.1 核心设计思想

系统不直接输出“学校名单”，而是走这条路径：

**兴趣 / 价值观 / 擅长领域 / 约束条件 → 专业簇 → 学校-专业对 → 排序与解释**

### 7.2 推荐对象

推荐对象定义为：

```text
(student, school, major, credential level)
```

而不是：

```text
(student, school)
```

### 7.3 初版打分公式

```text
score = hard_filter * (
  w1 * major_fit +
  w2 * affordability +
  w3 * outcome_fit +
  w4 * environment_fit +
  w5 * admission_fit +
  w6 * optionality
)
```

### 7.4 各项含义

#### hard_filter
强约束过滤：
- 预算上限
- 地理限制
- 是否提供该专业
- 身份要求（国际生/州内生）
- 校园类型禁忌
- 语言/学分/特殊限制

#### major_fit
- 兴趣测评结果
- 学科强项
- 职业目标
- 价值观与专业气质的匹配

#### affordability
- 平均净价
- aid / scholarship 友好度
- 成本波动风险
- 预算适配度

#### outcome_fit
- 毕业结果
- 债务/收入分位
- 专业到职业的关联度
- 用户目标与结果数据的匹配

#### environment_fit
- 城市 / 郊区 / 校园环境
- 学校规模
- 校园氛围
- research / internship / co-op 机会

#### admission_fit
- reach / match / likely
- 风险不是主排序依据，而是结果解释中的风险标签

#### optionality
- undeclared 支持
- 转专业难度
- 双专业 / minor 灵活性
- pre-major / capped major 风险

### 7.5 LLM 的角色

LLM 仅用于：
- 自然语言约束解析
- 问卷动态追问
- 解释推荐结果
- 生成 what-if 场景摘要
- 非标准化页面信息抽取

LLM 不用于：
- 直接决定最终排名
- 自由发挥式给名单
- 替代事实层数据库

---

## 8. 数据策略

## 8.1 数据总体原则

数据分为四层：

1. **公开结构化事实库**
2. **半结构化规则和政策库**
3. **公开纵向微观 / 聚合结果数据**
4. **第一方行为与实验数据**

### 8.2 第一层：公开结构化事实库

#### A. College Scorecard
用途：
- 学校层成本、录取、毕业、结果
- 专业层 field-of-study 成本/债务/收入
- institution-level 与 field-of-study-level 主干数据

注意事项：
- 必须按年份/队列快照落库
- 不能盲目使用 latest 进行跨指标拼接

#### B. IPEDS
用途：
- admissions
- completions
- financial aid
- outcomes
- institution characteristics

#### C. College Navigator
用途：
- 学校基础检索
- 项目 offered
- 学校面向消费者展示信息

#### D. CIP（专业分类）
用途：
- 专业标准化主键

### 8.3 第二层：职业与结果层

#### A. O*NET
用途：
- 兴趣测评（Interest Profiler）
- 职业技能、知识、价值观建模
- 职业向量库

#### B. BLS / OOH
用途：
- 职业增长
- 中位收入
- 地区工资

#### C. ACS PUMS / field-of-degree 变量
用途：
- 学位领域与收入、就业的微观关联先验

#### D. PSEO
用途：
- 学校 × 专业 × 学位层级 × 毕业 cohort 的收入结果
- 适合做 outcome prior

限制：
- 为 experimental data product
- 不是全美全覆盖的个体级反事实数据

### 8.4 第三层：半结构化规则与政策库

重点抓取：
- admissions page
- course catalog
- department page
- change-of-major policy
- AP/IB/DE credit policy
- honors / research / co-op 页面
- merit scholarship 页面
- NPC 页面
- Common Data Set

### 8.5 第四层：第一方数据（护城河）

系统要从 day-1 收集以下用户行为：

- 保存学校 / 删除学校
- A vs B 的比较选择
- 调节权重行为
- shortlist 形成过程
- 申请意向
- 实际申请结果
- 最终就读选择
- 事后满意度 / 是否转专业 / 是否持续就读

这是未来个性化排序和因果推理最重要的数据层。

---

## 9. LLM 在数据清洗中的角色

### 9.1 可用场景

LLM 非常适合清洗和抽取以下非标准化信息：

1. direct admit / pre-major
2. capped / impacted major
3. 转专业规则
4. undeclared 支持情况
5. 双专业 / minor 规则
6. honors 是否本科可及
7. research 机会
8. co-op / internship 是否制度化
9. AP/IB/DE 学分政策
10. merit scholarship 规则
11. NPC 链接与要求字段
12. 专业页面和 department 限制条件

### 9.2 不可直接用作 ground truth

LLM 输出不能直接作为事实入库，必须经过：

```text
raw doc -> chunk -> schema extraction -> validation -> confidence -> evidence -> audit -> fact table
```

### 9.3 抽取结果必须保留

- 原始 URL
- 原始文档 ID
- chunk ID
- 证据原文片段
- extractor version
- confidence
- human review status

---

## 10. 数据仓与表结构设计

## 10.1 分层架构

### Bronze（原始层）
只做保存，不做业务判断。

```text
bronze.raw_api_snapshot
- source_name
- pulled_at
- source_version
- request_key
- raw_payload_json

bronze.raw_document
- doc_id
- unitid
- url
- doc_type
- fetched_at
- checksum
- raw_html_or_pdf_text
- language
- status_code

bronze.raw_document_chunk
- chunk_id
- doc_id
- section_title
- chunk_text
- token_count
- embedding_id
```

### Silver（规范化事实层）

```text
silver.dim_institution
- unitid
- opeid6
- school_name
- control
- level
- state
- locale
- carnegie
- student_size
- website_url
- npc_url
- effective_date

silver.dim_program
- program_key
- unitid
- cip4
- cip6_best_effort
- credlev
- program_name_norm
- distance_flag
- dept_url
- active_flag
- effective_date

silver.dim_major_taxonomy
- cip4
- cip_title
- stem_flag
- cip_version

silver.dim_occupation
- onet_soc
- occupation_title
- riasec_profile
- job_zone
- skills_vector
- knowledge_vector
- values_vector
- bls_median_pay
- bls_growth

silver.bridge_major_occupation_prior
- cip4
- onet_soc
- prior_strength
- prior_type
- notes

silver.fact_school_admissions_year
- unitid
- data_year
- admit_rate
- sat_act_ranges
- retention_rate
- grad_rate
- transfer_out_rate
- source_name
- cohort_note

silver.fact_cost_aid_year
- unitid
- data_year
- sticker_price
- avg_net_price
- avg_net_price_income_bands
- avg_aid_amount
- pct_receive_grant
- pct_receive_loan
- source_name

silver.fact_program_outcome_cohort
- program_key
- cohort_year
- completions
- debt_median
- earnings_y1
- pseo_earnings_y1_p25_p50_p75
- pseo_earnings_y5_p25_p50_p75
- pseo_earnings_y10_p25_p50_p75
- source_name

silver.fact_policy_extracted
- policy_fact_id
- unitid
- program_key_nullable
- policy_type
- value_json
- evidence_doc_id
- evidence_chunk_id
- evidence_quote
- extractor_version
- confidence
- human_review_status
- valid_from
- valid_to
```

### Gold（训练与实验层）

```text
gold.student_profile_snapshot
- student_id
- snapshot_ts
- grade_level
- citizenship_bucket
- residence_state
- budget_cap
- geo_preferences
- academic_profile_json
- test_optional_flag
- major_openness_score
- risk_tolerance
- parent_constraints_json

gold.student_assessment_event
- event_id
- student_id
- event_ts
- assessment_type
- input_json
- output_json

gold.recommendation_snapshot
- rec_id
- student_id
- snapshot_ts
- candidate_program_keys
- feature_vector_version
- model_version
- ranked_list_json
- explanation_json

gold.preference_event
- event_id
- student_id
- event_ts
- rec_id
- event_type
- target_program_key
- pair_left_program_key
- pair_right_program_key
- dwell_time
- reason_tag

gold.experiment_assignment
- assignment_id
- student_id
- event_ts
- experiment_name
- arm
- eligibility_json
- random_seed

gold.application_outcome
- student_id
- program_key
- applied_at
- admitted_flag
- aid_offer_bucket
- enrolled_flag
- enrolled_at

gold.post_enrollment_followup
- student_id
- followup_ts
- enrolled_program_key
- changed_major_flag
- persistence_1y_flag
- internship_flag
- satisfaction_scores_json
```

---

## 11. 因果推理策略

## 11.1 核心判断

公开数据可以帮助建立：
- 世界模型
- 先验关系
- 群体层异质性假设

但公开数据通常不足以直接支持稳定的 **个体级反事实推荐**。

真正可落地的因果能力，来自：
- 明确的 treatment
- treatment 前协变量
- 时间顺序干净的日志
- 可控实验

### 11.2 因果问题拆分

#### A. 可立即做的因果问题（产品内）
- major-first onboarding 是否提高 shortlist 完成率？
- 先展示 affordability 会不会降低高价学校误选？
- 解释风格 A vs B 哪个更能提高保存率？
- 对 undecided 用户展示 optionality 是否会提高满意度？

#### B. 中期可做的因果问题（产品闭环后）
- 某类学生更适合先看专业还是先看环境？
- 哪种推荐排序策略更能提高最终申请匹配度？
- 哪种信息展示更能降低后续转专业 regret？

#### C. 暂不建议过早做的问题
- 对单个学生精确估计“去 A 校读 X 专业比去 B 校读 Y 专业未来多赚多少钱”
- 没有长期 outcome 支撑的个体级高置信反事实推荐

### 11.3 因果技术路线

#### 第一阶段
- A/B test
- uplift modeling（针对短期行为）
- doubly robust / meta-learners（仅用于可控 treatment）

#### 第二阶段
- heterogeneous treatment effect learning
- policy learning（对排序/解释策略）

#### 第三阶段
- 结合长期 follow-up 的个体化 intervention policy

---

## 12. 训练与数据规模评估

## 12.1 基本结论

不建议一开始训练端到端大模型。

更现实的训练对象：

1. **非标准化信息抽取器**
2. **偏好排序模型**
3. **因果估计 / uplift 模型**
4. **解释和解析所需的轻量 LLM 工作流**

### 12.2 事实仓规模

#### 公开结构化数据
- 量级较小，单机可处理
- College Scorecard 数据包体量在百 MB 级
- O*NET / BLS / CIP / PSEO 也远低于大模型训练数据规模

#### 学校官网半结构化数据
工程估算：

##### MVP 深做版
- 200–500 所学校
- 1 万到 7 万页有效文档
- 5 万到 40 万 chunk

##### 全美扩展版
- 7,000+ 学校
- 50 万到 300 万 chunk（工程估算）

这在数据工程上仍然是中小规模问题，不是 foundation model 级问题。

### 12.3 抽取器训练规模

推荐起点：
- 3,000–10,000 条 gold snippets
- 每个关键字段 300–800 条高质量标注
- 先覆盖 10–15 类高价值字段

结论：
- 前期优先使用 API LLM + schema extraction + human audit
- 不建议早期投入大规模微调

### 12.4 排序模型训练规模

推荐目标：
- 5,000–20,000 个有效用户
- 50,000–200,000 条 preference events
- 100,000+ 条 pairwise preference signal

可支持：
- LightGBM / CatBoost
- pairwise ranker
- 简单 two-tower / sequence ranking（后期）

### 12.5 因果模型训练规模

产品内实验的现实门槛：

#### 短期行为指标
- 每实验臂 5,000–10,000 用户分配
- 可看 save / compare / shortlist 等指标

#### uplift / CATE 初步建模
- 25,000–100,000 次有效 treatment exposures 更稳妥

#### 长期结果
- 需要跨申请季累积
- 不适合在产品早期作为主训练目标

### 12.6 算力建议

第一年推荐配置：
- Postgres + Parquet + DuckDB
- 16–32 vCPU
- 64–128GB RAM
- 1 张 24GB 显存 GPU 即可

适用任务：
- ETL
- embedding
- 向量索引
- LGBM / CatBoost / causal forest / DR learner
- 小规模 reranker / sequence model

### 12.7 何时再考虑微调较大模型

在满足以下条件前，不建议做 7B 级别微调：

- 10,000–30,000 条自己验证过的高质量 extraction pairs
- 或 100 万+ 高质量行为事件，且任务边界非常明确

---

## 13. 护城河设计

### 13.1 不是护城河的东西

- 单纯调用基础大模型
- 单纯聚合公开高校数据
- 一般性的学校问答机器人
- 泛 school ranking

### 13.2 真正的护城河

#### A. 深层非标准化事实库
把下列内容长期结构化维护：
- 转专业规则
- pre-major / capped major
- optionality
- scholarship 细则
- AP/IB 认定
- research / co-op 的本科可达性

#### B. 偏好学习
通过用户行为学习：
- 用户真正重视什么
- 用户为何保存 / 丢弃某类学校
- 哪类解释更能帮助做决定

#### C. 工作流锁定
通过：
- shortlist
- 比较器
- 家长共享
- NPC 记录
- 申请追踪
- 解释历史

形成 switching cost。

#### D. 第一方实验日志
这是未来因果与个体化推荐的真正 moat。

---

## 14. 技术架构建议

### 14.1 技术分层

#### 在线服务层
- 用户会话
- profile / questionnaire
- 推荐 API
- 比较器 API
- 解释 API

#### 特征与决策层
- rules engine
- ranking engine
- preference learner
- experimentation service

#### 数据层
- structured warehouse
- document store
- vector index
- event log

#### LLM 工作流层
- constraint parser
- schema extractor
- explanation generator
- what-if summarizer

### 14.2 推荐技术选型

#### 起步阶段
- PostgreSQL
- DuckDB
- object storage（文档）
- 向量库（pgvector / Qdrant / Weaviate 任一即可）
- Python data pipeline
- LGBM / CatBoost
- LLM API（而非自训）

#### 不建议早期引入
- 多模型复杂编排
- 端到端 RL 系统
- 大规模知识图谱平台
- 重型 MLOps 平台

---

## 15. 评估体系

## 15.1 数据层评估
- 文档抓取成功率
- 字段抽取准确率
- 证据可追溯率
- 人工抽检通过率
- 政策事实更新时间

### 15.2 推荐层评估
- shortlist 生成率
- save rate
- compare click rate
- shortlist completion rate
- 用户主观满意度
- why explanation usefulness

### 15.3 长期效果评估
- 申请提交率
- 录取覆盖合理性
- 最终入学率
- 转专业率
- 1 年后满意度
- regret rate

### 15.4 因果评估
- A/B 实验 lift
- uplift model Qini / AUUC
- treatment assignment balance
- pre-treatment covariate balance
- long-term proxy alignment

---

## 16. 风险与约束

### 16.1 数据风险
- 学校官网信息经常变化
- 非标准化页面抽取存在错误
- 不同来源 cohort / 年份口径不一致
- 公开微观数据不能直接支撑强个体反事实

### 16.2 产品风险
- 用户过度依赖“推荐分数”
- 家长与学生偏好冲突
- 市场上容易被误解为“又一个选校聊天机器人”

### 16.3 合规与伦理风险
- 不应伪装为官方录取预测
- 不应夸大因果结论
- 不应在缺乏证据时输出确定性未来回报承诺
- 必须保留不确定性说明与数据来源透明度

---

## 17. 路线图建议

## 17.1 Phase 0：2–4 周
- 明确目标人群
- 定义 50–100 所学校深度名单
- 搭 Bronze/Silver 基础表
- 跑通问卷 → 专业簇 → shortlist 的最小流程

### 17.2 Phase 1：1–2 个月
- 上线 MVP
- 完成推荐、比较器、explanation、what-if
- 开始采集 preference events
- 建立文档抓取与 LLM 抽取流水线

### 17.3 Phase 2：2–4 个月
- 扩展到 200–500 所学校
- 做 10–15 类关键政策字段抽取
- 加入实验系统
- 做第一批排序优化

### 17.4 Phase 3：4–8 个月
- 累积 5,000–20,000 用户
- 训练偏好排序器
- 对推荐解释和流程做 uplift / causal 优化
- 增加家长 / 顾问协作

### 17.5 Phase 4：8–18 个月
- 扩大学校覆盖
- 建立长期 follow-up 机制
- 探索与外部高质量数据合作
- 视数据量评估更高阶个体化因果模型

---

## 18. MVP 成功标准

满足以下条件即可视为 MVP 成功：

1. 用户能在一次会话内得到 1 份可解释的 shortlist
2. 推荐对象是学校-专业对，而不是空泛学校列表
3. 每条推荐都有结构化依据和解释
4. 用户可以调整权重并看到结果变化
5. 系统开始积累可训练的 preference events
6. 抽取系统对关键字段有可审计证据链

---

## 19. 最终结论

这个产品的核心不是训练一个“大而聪明的模型”，而是构建一个：

**事实仓 + 非标准化规则抽取 + 学校-专业排序 + 偏好学习 + 实验/因果闭环**

公开数据足够支撑你做出一个强大的世界模型和高质量 shortlist；真正的护城河，来自你如何：

- 选窄场景
- 做深规则数据
- 收集第一方偏好信号
- 用可控实验构建因果能力

因此，本产品在技术和数据上最优先的建设顺序应为：

1. 事实层
2. 抽取层
3. 排序层
4. 实验层
5. 因果层

而不是相反。

---

## 20. 附录：建议优先抓取的字段清单

### 学校层
- school_name
- unitid
- state
- locale
- control
- size
- admit_rate
- grad_rate
- retention_rate
- avg_net_price
- scholarship friendliness

### 专业层
- cip4
- program_name_norm
- offered_flag
- direct_admit_flag
- premajor_flag
- capped_major_flag
- change_major_rule
- undeclared_friendly_flag
- double_major_friendly_flag
- research_access_flag
- coop_flag

### 用户层
- major_openness
- budget_cap
- geo_preferences
- career_goal_vector
- riasec_vector
- risk_tolerance
- cost_sensitivity
- prestige_sensitivity
- flexibility_sensitivity

### 事件层
- save
- dismiss
- compare
- shortlist
- reorder
- apply_intent
- apply
- admit
- enroll
- followup

---

## 21. 附录：一句话实施建议

**先把 100 所学校做透，再把 1 万个用户看懂。**

这比先做“全美最全数据库”更可能形成真正的产品价值与护城河。
