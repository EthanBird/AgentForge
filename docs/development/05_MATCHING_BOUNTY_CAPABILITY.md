# 能力画像、悬赏与异构 Agent 匹配

> 状态：MVP 可实现规范  
> 输入：已发布 AFWP 的 `routing`、`permissions`、`scheduling` 和实时资源状态  
> 输出：可审计的候选集、选择决策、Lease 与结算记录

## 1. 核心原则

调度器选择的是一个**可复现 Executor**，不是一个营销意义上的模型名。Executor 指纹至少包含：

```text
provider endpoint
+ model family/version/build
+ system prompt pack digest
+ jcode version and configuration
+ tool adapters and versions
+ context/budget policy
+ OS/container/hardware class
+ network/security zone
```

同一模型更换提示词包、jcode 版本、工具或推理配置后，必须生成新 `executor_fingerprint`。信誉不直接继承；只能作为有折扣、有高不确定性的先验。

“让合适的 Agent 做合适的事”在系统中被分解成：

1. 能不能做：硬约束；
2. 做成的概率：按任务类别、风险、仓库和 Executor 估计；
3. 何时、多少成本做成：时延与总成本预测；
4. 做错的代价：逃逸缺陷、安全与集成风险；
5. 如何获得新证据：Canary 与受控探索。

任何模型—岗位映射都只是冷启动先验，不得成为永久硬编码。

## 2. 身份与数据模型

### 2.1 Node、Agent 与 Executor

| 实体 | 含义 | 典型变化 |
| --- | --- | --- |
| Node | 运行 Worker Daemon 的物理机/虚拟机 | 上下线、负载、硬件、网络区 |
| Agent Adapter | 对 jcode/模型 API 的会话与工具适配 | 版本、Prompt Compiler、权限策略 |
| Executor | Node 可启动的确定配置指纹 | 模型、Agent、工具、OS、预算配置任一改变 |
| Worker slot | Executor 的一个并发执行容量 | busy/idle/draining/faulted |

Lease 必须绑定 `node_id + executor_id + executor_fingerprint + attempt_id`。Executor 不得在 Lease 中途无记录地替换模型或提示词；需要替换时创建新 Attempt，或通过明确的 continuation policy 产生新 provenance 段并重新验收。

### 2.2 Executor Profile

推荐控制面记录：

```json
{
  "executor_id": "executor-rust-frontier-03",
  "fingerprint": "sha256:...",
  "provider": "configured-provider",
  "model_family": "frontier-reasoning",
  "model_revision": "pinned-revision-or-endpoint-build",
  "agent_runtime": {"name": "jcode", "version": "0.9.2"},
  "prompt_pack_digest": "sha256:...",
  "declared": {
    "capabilities": ["architecture.system", "rust", "postgresql"],
    "task_classes": ["architecture.rfc", "backend.distributed-concurrency"],
    "modalities": ["text"],
    "tools": ["git", "cargo", "postgres-test-container"],
    "os": ["linux"],
    "security_levels": ["public", "project_private"]
  },
  "limits": {
    "context_tokens": 200000,
    "max_parallel_slots": 2,
    "max_task_budget_units": 64
  }
}
```

声明能力只决定是否允许进入预检，不直接等于实测能力。`project_private` 等安全等级还必须由 Node 证明、安全策略和身份授权共同满足。

### 2.3 能力命名

能力使用层级 slug，维护版本化 taxonomy：

```text
architecture.system
architecture.api-contract
backend.crud
backend.distributed-concurrency
database.migration
frontend.react
frontend.visual-fidelity
vision.image-understanding
vision.asset-generation
review.security
review.correctness
integration.git-conflict
```

父能力不能自动证明所有子能力。例如 `backend` 不能替代 `backend.distributed-concurrency`。别名只在 taxonomy migration 中解析，决策记录保存解析后的 canonical capability IDs 和 taxonomy version。

## 3. 任务画像

AFWP `routing` 是任务画像的权威输入；Planner 可以另外计算但不得静默覆盖：

- task class：主类别，例如 `frontend.component`；
- risk：low/medium/high/critical；
- complexity：0–1，表示认知、接口、测试和集成复杂度的综合估计；
- required：能力、模态、工具、OS、硬件、安全域；
- preferred capability weights：用于质量排序；
- deadline、预算和最低首轮通过概率；
- scope/write-set：用于仓库亲和与并发冲突；
- scheduling mode：exclusive、sealed bid、redundant、pair、tournament。

Planner 应使用独立 Critic 检查画像。若硬约束导致候选为空，不得自动放宽安全域、权限或验收；返回 `AF_NO_FEASIBLE_EXECUTOR`，让 Boss 修改任务、补充 Runner 或拆分任务。

## 4. 用户设想的初始岗位映射

以下映射应以配置文件形式作为**待验证先验**进入系统，而不是写死在业务代码：

| 岗位 | 用户给出的首选起点 | 初始 task classes | 必须独立验证的维度 |
| --- | --- | --- | --- |
| Root/Domain Boss、架构代码、任务包规划 | GPT-5.6 Sol 类 Executor | `architecture.*`、`planning.decomposition`、`backend.distributed-concurrency` | 需求覆盖、DAG 质量、正确性、成本 |
| 图像识别与绘图 | MiniMax M3 类多模态 Executor | `vision.image-understanding`、`vision.asset-generation` | 视觉量表、格式、版权/安全、稳定性 |
| 前端实现 | Kimi K3 类 Executor | `frontend.*` | E2E、可访问性、视口截图、视觉差异 |
| 常规 CRUD 后端 | DeepSeek V4 Flash 类 Executor | `backend.crud`、`test.unit` | 契约、幂等、鉴权、迁移、返工率 |

这些名字只表示部署者期望的 bootstrap route。首次上线时必须对实际可用端点、版本、jcode 配置和目标仓库运行基准包。若实测数据反转，路由必须跟随证据而不是跟随上表。

同一大任务可以按角色使用不同 Executor：架构 Agent 先冻结接口，CRUD Agent 实现机械部分，专长 Reviewer 做边界审查，Integrator 处理最新主分支。禁止把“最聪明模型包办全流程”当默认方案，也禁止为了便宜把 critical 任务交给未经验证的 Executor。

## 5. 实测能力统计

### 5.1 统计切片

信誉至少按以下键保存：

```text
(executor_fingerprint, task_class, risk_band, repository_id)
```

样本少时按层级回退：repository -> repository family -> task class -> capability family -> global prior。不得用一个总分掩盖“前端强、并发弱”的差异。

每个切片保存：

- first-pass accepted / failed / inconclusive；
- 最终 accepted、返工轮数、Review finding 严重度；
- escaped defect（按观察窗口归因）；
- 估时与实际时长、报价与实际成本；
- P50/P90/P95 时延；
- Runner 基础设施失败与 `AF_TASK_INVALID`，两者不计为 Executor 质量失败；
- 样本权重、最后更新时间、漂移状态。

`first-pass PASS` 必须定义为：第一次 candidate submission 在未改变 AFWP revision、未经过 Reviewer 退回修改的条件下，通过全部 hard criteria 与独立 review。只跑过 Worker 本地测试不算。

### 5.2 Beta-Binomial 通过率

对首轮通过概率使用带先验的 Beta 后验：

\[
\alpha=\alpha_0+\sum_i w_i y_i,\qquad
\beta=\beta_0+\sum_i w_i(1-y_i),
\]

其中 \(y_i\in\{0,1\}\)，近期、相似且同仓库样本权重更高：

\[
w_i=w_{similarity}\cdot 2^{-\Delta t_i/h}\cdot w_{evidence},
\]

`h` 是半衰期；只有完整独立证据的任务 `w_evidence = 1`，历史迁移样本和弱证据必须折扣。调度时使用可信下界而非均值：

\[
P_{pass}^{LB}=Q_{0.10}(\operatorname{Beta}(\alpha,\beta)).
\]

critical 任务建议使用 10% 或更低分位点；low-risk Canary 可使用均值以增加探索。MVP 若暂不引入统计库，可用 Wilson lower bound，但 API 要保留 `mean/lower_bound/sample_weight/method`。

### 5.3 成本、时延与缺陷

预测目标不是第一次模型调用价格，而是总接受成本：

\[
E[C_{total}]=C_{first}+(1-P_{pass})E[C_{rework}]+P_{escape}C_{defect}+C_{infra}.
\]

分别训练/统计：

- `duration_p50/p95`：从正式 Lease 到 candidate 或明确 blocked，排除 `WAITING_INPUT`；
- `cost_first`：模型、Runner、存储和带宽；
- `rework_cost`：Reviewer 退回到新 Submission；
- `escape_probability`：集成后观察期中的归因缺陷；
- `estimate_ratio = actual / bid`：校准 Worker 自报。

数据量不足时使用分桶分位数，不急于上复杂 ML。所有预测记录训练窗口和 model version，确保决策可重放。

## 6. 冷启动与模型升级

### 6.1 先验来源

新 Executor 的先验按可信度排序：

1. 同 fingerprint 在标准基准任务上的签名结果；
2. 同模型/Prompt/工具配置、不同相近 Node 的结果；
3. 父 fingerprint（只改变明确非语义配置）的折扣结果；
4. 同模型家族与任务类的历史；
5. 部署者声明的岗位映射；
6. 保守全局先验。

声明不能越过硬约束，也不能让 `P_pass^LB` 达到 critical 阈值。新模型版本、端点无版本地漂移、system prompt 或 jcode 更新均触发新 fingerprint。

### 6.2 Canary 晋级

推荐状态：

```text
UNVERIFIED -> BENCHMARKED -> CANARY -> QUALIFIED -> TRUSTED
                         \-> SUSPENDED
```

| 等级 | 可承接任务 |
| --- | --- |
| UNVERIFIED | 仅沙箱基准，无项目 Secret |
| BENCHMARKED | public、low-risk、可完全机器验收 |
| CANARY | project-private low-risk，小预算，强 Reviewer |
| QUALIFIED | 达到 task-class 样本和下界阈值的 medium/high 任务 |
| TRUSTED | 指定类别的 critical 候选；仍必须独立验收 |

默认晋级参考（应按项目校准）：

- 至少 5 个标准基准 PASS 才能进入 CANARY；
- 至少 10 个同类有效项目样本、`P_pass^LB >= 0.75`、无逃逸 High/Critical，进入 QUALIFIED；
- 至少 30 个同类有效样本、`P_pass^LB >= 0.85`、估时 P90 校准在 0.5–2.0，才能进入该类别 TRUSTED；
- 任一可归因 Critical escape 立即 SUSPENDED，等待复盘和新 fingerprint。

晋级是 `(fingerprint, task_class, security_level)` 级别，不是全局头衔。

### 6.3 探索预算

完全贪心会让新 Executor 永远没有数据。每个项目可给 low-risk 任务 5%–10% 探索预算，使用 Thompson sampling 或上置信界选 Canary，但必须：

- 满足全部硬约束；
- 不在 deadline 临界路径；
- 有确定性验收和已验证 Reviewer；
- 限制成本和外部副作用；
- 探索失败可快速重派。

critical 和不可逆任务不使用探索路由。

## 7. 四阶段匹配算法

### 7.1 阶段 A：硬约束过滤

候选必须同时满足：

```text
required capabilities subset of declared+verified capabilities
required tools available at pinned versions
modality / OS / hardware / context capacity satisfied
node security zone >= task security level
permission and network policy satisfiable
max task budget <= executor/node policy
slot available before deadline
qualification level sufficient for risk
repository/artifacts reachable in preflight path
no author-reviewer independence conflict
```

硬过滤输出带原因的 rejected list。不能因候选为空而忽略某个 required 字段。

### 7.2 阶段 B：结果预测

为每个候选输出：

```json
{
  "pass_probability": {"mean": 0.88, "lower_bound": 0.81, "method": "beta-p10"},
  "deadline_probability": 0.93,
  "duration_p95_minutes": 112,
  "expected_total_cost_units": 7.4,
  "escape_probability": 0.012,
  "uncertainty": 0.18,
  "repo_affinity": 0.76
}
```

自报 confidence 只能作为一个特征。调度器必须按历史做校准，可用按 task class 的 isotonic regression；样本少时对自报值向 0.5 收缩。

### 7.3 阶段 C：Pareto 前沿

在以下维度移除被严格支配候选：

- 最大化通过率下界、deadline probability、repo affinity；
- 最小化总成本、P95 时延、逃逸风险和不确定性。

保存 Pareto 集而非只保存胜者，便于主候选预检失败时快速回退，也用于解释“为什么没有选择某个更便宜模型”。

### 7.4 阶段 D：风险调整选择

归一化后使用：

\[
S(e,t)=w_qP_{pass}^{LB}+w_dP_{deadline}+w_aA_{repo}
-w_c\widehat C_{total}-w_l\widehat L_{95}
-w_uU-w_xP_{escape}.
\]

先应用约束：

\[
P_{pass}^{LB}\ge p_{min},\qquad
P_{deadline}\ge d_{min},\qquad
C_{total}^{p90}\le budget.
\]

建议初始权重：

| 风险 | `w_q` | `w_d` | `w_c` | `w_l` | `w_u` | `w_x` | 策略 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| low | .30 | .15 | .25 | .15 | .10 | .05 | 允许 Canary |
| medium | .40 | .15 | .15 | .10 | .10 | .10 | 默认独立 Reviewer |
| high | .45 | .15 | .10 | .05 | .10 | .15 | 使用可信下界与 pair |
| critical | .45 | .10 | .05 | .05 | .15 | .20 | trusted + 异构双 Review |

权重只在所有硬约束通过后工作。控制面必须保存 normalization snapshot，否则日后不能重放分数。

## 8. 调度模式

| 模式 | 选择与结束条件 |
| --- | --- |
| `exclusive` | 最高风险调整分候选获单；预检失败后选下一候选 |
| `sealed_bid` | 在固定窗口收集 Bid；服务端校准估时/置信度后选择，不公开他人报价 |
| `redundant(N)` | 选择 N 个故障相关性较低的 Executor；每个有独立 Attempt/Lease/branch |
| `author_reviewer_pair` | 联合优化作者质量和 Reviewer 独立性；Reviewer 不得共享会话/工作区 |
| `tournament` | 多个架构/RFC 独立产出，Critic 依据统一量表比较，再产生 synthesis 包 |

### 8.1 多样性约束

高风险 pair/redundant/tournament 的选择加入相关性惩罚：

```text
same provider/model family
same prompt pack
same Node failure domain
shared authored context
historically correlated failure signature
```

critical 任务至少要求作者和最终语义 Reviewer 不同模型家族；若当前部署无法满足，任务保持 BLOCKED 或需要显式风险 waiver。

## 9. Bid 与两阶段接单

### 9.1 Bid

```json
{
  "bid_id": "bid-01J4...",
  "offer_id": "offer-01J4...",
  "executor_id": "executor-rust-frontier-03",
  "executor_fingerprint": "sha256:...",
  "estimated_cost_units": 7.0,
  "estimated_minutes": 95,
  "self_confidence": 0.84,
  "earliest_start_at": "2026-08-07T10:00:00Z",
  "capacity_reserved_until": "2026-08-07T10:03:00Z",
  "plan_digest": "sha256:...",
  "baseline_cache": "hit",
  "key_risks": ["PostgreSQL concurrency fixture may require hydration"]
}
```

Bid 不授予源码、Secret 或写权限；Worker 只得到去敏摘要和必要的容量信息。服务端验证报价在预算和节点策略内，并使用历史 `actual/bid` 校准。

### 9.2 Provisional Lease

胜者先获得短时 provisional Lease，只能：

1. 获取精确 base commit 与声明输入；
2. 验证 Runner、工具、磁盘和安全域；
3. 执行只读 baseline build/test；
4. 输出 `PRECHECK_PASS` 或结构化失败。

通过后才原子创建 Attempt 的正式 TaskLease 与 generation。若基线、任务规格或 Artifact 本身无效，标记 `TASK_INVALID/BASELINE_UNAVAILABLE`，不计 Executor 失败；若 Worker 声明能力不实，则计 admission failure。

### 9.3 容量与超卖

- `capacity_reserved_until` 到期自动释放，不等于 TaskLease；
- 一个 slot 同时只能绑定一个 active compute phase；WAITING_INPUT 可释放 compute slot但不释放工作区；
- Node 宣告 draining 后不接新 Lease，现有 Lease按策略完成或 checkpoint/release；
- 调度器为长 Runner 预留资源，不因模型会话空闲而超卖 CPU/GPU/磁盘。

## 10. 悬赏与结算

“赏金单位”是内部归一化成本/价值，不应直接等于模型 token。MVP 账本不可变，使用 double-entry ledger：项目预算扣款与 Executor/基础设施记账成对发生。

### 10.1 预算组成

```text
max_budget = author_compute + runner + reviewer + artifact + integration_reserve
```

Planner 不得把全部预算都分给作者而无验收资源。推荐默认保留 20%–35% 给 Reviewer、Runner 与一次 rework；critical 任务更高。

### 10.2 结算阶段

| 阶段 | 建议比例 | 条件 |
| --- | ---: | --- |
| 有效 Candidate | 20% | L1 通过、证据格式完整、未越界 |
| 独立验收 PASS | 45% | 所有 hard criteria、review、clean reproduction 通过 |
| 成功 INTEGRATED | 25% | 最新目标分支 L5 通过并合并 |
| 观察期 | 10% | 无可归因逃逸缺陷 |

架构 Tournament 未胜方案若被最终方案引用，可按 `value_absorbed` 获 10%–40% 部分积分。`AF_TASK_INVALID`、Runner 基础设施故障和被 Boss 取消不能作为质量惩罚，但已经消耗的第三方资源仍进入项目成本。

### 10.3 反激励设计

- 只奖励“最快提交”会诱发漏测，因此速度只在通过质量门槛后比较；
- Worker 不得通过拆成大量无价值子包增加赏金，子包由 DelegationGrant 和 Critic 审核；
- 同一证据 digest 不能在不相关任务重复结算；
- 未披露外部副作用、伪造 Runner/签名、故意绕过验收会冻结 Executor；
- Reviewer 的奖励与发现真实问题和低误报共同相关，不能只按 finding 数量计价。

## 11. Reviewer 匹配

Reviewer 做独立路由，附加硬约束：

- 与作者不共享会话、workspace、Attempt Journal；
- 默认不读取作者推理与自我解释，只读 AFWP、基线、候选 diff、证据和仓库规范；
- critical 任务模型家族不同；
- Reviewer 必须拥有任务域 review 能力，不是“任意更贵模型”；
- 历史统计单独计算 review recall、误报率、逃逸缺陷与审查时延；
- Reviewer 不直接修改候选。发现问题产生 rework finding 或新测试 Artifact。

若 Reviewer 与作者结论冲突，不能用分数简单平均：确定性 hard failure 优先；语义争议创建 adjudication 包，由独立 Critic 依据相同 AFWP 裁决。

## 12. 决策可解释性

每次匹配保存 `RoutingDecision`：

```json
{
  "decision_id": "route-01J4...",
  "package": ["agentforge", "wp-lease-fencing-001", 3, "sha256:..."],
  "taxonomy_version": "capabilities/1.0",
  "policy_version": "routing/1.0",
  "feature_snapshot_at": "2026-08-07T09:59:00Z",
  "hard_filter": {
    "accepted": ["executor-rust-frontier-03"],
    "rejected": [{"executor_id": "executor-crud-fast-04", "codes": ["CAPABILITY_MISSING", "RISK_QUALIFICATION_LOW"]}]
  },
  "predictions": {},
  "pareto_set": ["executor-rust-frontier-03"],
  "selected": "executor-rust-frontier-03",
  "score": 0.817,
  "exploration": false
}
```

`hard_filter.rejected[].codes` 是版本化的路由解释 reason code，不是 HTTP ErrorEnvelope；只有整个路由命令无可行 Executor 时，wire error 使用共享码 `AF_NO_FEASIBLE_EXECUTOR`。

必须保留用于预测的数据窗口/模型 digest、归一化边界和权重。不要记录 Secret 或完整 Prompt；只记录 digest。

## 13. API 与实现切片

MVP API：

```text
PUT  /v1/executors/{id}/profiles/{fingerprint}
POST /v1/nodes/{id}/sessions
POST /v1/nodes/{id}/capacity
POST /v1/offers/{id}/bids
POST /v1/offers/{id}/select
POST /v1/bids/{id}/provisional-lease
POST /v1/attempts/{id}/precheck
POST /v1/attempts/{id}/activate
GET  /v1/routing-decisions/{id}
GET  /v1/executors/{fingerprint}/scores?task_class=...
POST /v1/outcomes
POST /v1/settlements
```

推荐数据库表：

```text
executor_profiles(fingerprint PK, immutable profile, lifecycle_state)
executor_claims(fingerprint, capability_id, taxonomy_version)
node_sessions(node_id, session_id, expires_at, status)
capacity_slots(node_id, executor_fingerprint, slot_id, state)
offers / bids / routing_decisions
leases(attempt_id, executor_fingerprint, generation, ...)
outcomes(submission_id, task_slice, metrics, evidence_digest)
capability_posteriors(slice_key, alpha, beta, effective_n, updated_at)
latency_cost_stats(slice_key, quantiles, calibration)
settlement_ledger(debit_account, credit_account, amount, cause_id)
```

原始 outcome 是事实来源；posterior 和分位数可重建。更新 outcome 与 Outbox 必须同事务，统计聚合可以异步。

匹配服务建议保持纯函数边界：

```rust
fn route(
    task: &TaskProfile,
    candidates: &[ExecutorSnapshot],
    policy: &RoutingPolicy,
    now: DateTime<Utc>,
) -> Result<RoutingDecision, NoFeasibleExecutor>;
```

同一输入 snapshot 必须得到同一决策；随机探索需把 seed 写入决策。

## 14. 失败与降级

| 情况 | 行为 |
| --- | --- |
| 无候选满足 required | BLOCKED + `AF_NO_FEASIBLE_EXECUTOR`，不自动放宽 |
| 统计服务不可用 | 使用最后签名 snapshot；过期后只允许低风险 conservative route |
| Executor 端点无版本漂移 | 暂停新 Lease，重新 fingerprint 和 Canary |
| 主候选 provisional precheck 失败 | 选择 Pareto 下一候选，保留失败原因 |
| critical Reviewer 不独立 | BLOCKED 或显式风险 waiver |
| 节点掉线 | TaskLease 按 TTL 到期；不依据 NodeSession 直接认定任务失败 |
| Bid 均超预算 | Boss 拆包、加预算或降低非硬目标；不得减少 hard acceptance |
| 预测不确定性过高 | low risk 使用 Canary；high/critical 使用 redundant/pair 或阻塞 |

## 15. 验收测试

### 15.1 确定性单测

- 缺 OS、tool、modality、security level 的候选必须被硬过滤；
- 分数再高也不能让不满足 required 的候选入选；
- 同一 snapshot + seed 产生字节一致 decision；
- Pareto 算法正确移除每类被支配候选；
- risk policy 的最低资格和 `P_pass^LB` 生效；
- author-reviewer 同家族在 critical 包被拒绝；
- `AF_TASK_INVALID` 和 infra failure 不更新 Beta 的失败计数；
- 同 fingerprint 的重复 outcome 通过 submission ID 去重。

### 15.2 仿真

至少构造 10,000 个任务的离线仿真：

- 质量、成本、时延具有不同真实分布的四类 Executor；
- 新 Executor 在探索预算内能获得样本但不占用 critical 任务；
- 一个模型在版本升级后能力骤降，漂移检测和 Canary 能阻止关键路由；
- 自报 confidence 系统性偏高，校准后不再获得不当优势；
- 最便宜 Executor 高返工时，优化总成本会选择首价更高但更稳的候选；
- 节点容量、deadline、候选为空和 provisional failure 不造成任务丢失。

### 15.3 MVP 出口指标

- 每个路由决策 100% 可解释到硬过滤、预测 snapshot、Pareto 集与最终权重；
- 未满足硬约束的选择数为 0；
- simulated critical route 的未验证 Executor 数为 0；
- 重放相同决策输入，结果一致率 100%；
- 统计 outcome 与聚合分数可从事件/原始记录完全重建；
- 首轮线上任务保留人工可见的 route explanation，发现错误可一键 suspend fingerprint。
