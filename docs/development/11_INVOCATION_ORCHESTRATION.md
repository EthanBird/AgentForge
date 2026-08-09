# 11：Agent 调用编排、运行账本与会话恢复规范

> 状态：M1-A 冻结规格
> 适用范围：`agentforge-domain`、`agentforge-application`、`agentforge-storage-postgres`、
> `agentforge-control-plane`、`agentforge-obligation-engine`、Worker Gateway 与 Agent Adapter
> 架构决定：[ADR-0006](../adr/ADR-0006-INVOCATION-ORCHESTRATION.md)
> 前置规范：[01_DOMAIN_MODEL_AND_STATE_MACHINES.md](01_DOMAIN_MODEL_AND_STATE_MACHINES.md)、
> [03_CONTROL_PLANE_IMPLEMENTATION.md](03_CONTROL_PLANE_IMPLEMENTATION.md)、
> [04_WORKER_RUNTIME_JCODE.md](04_WORKER_RUNTIME_JCODE.md)

## 1. 目标与边界

本规范增加一个位于领域任务与具体 Agent Runtime 之间的耐久调用层，必须做到：

1. 保存每次调用的可验证触发原因；
2. 安全合并重复 Signal，不吞掉新 revision、generation、审批或取消；
3. 原子创建 InvocationRun、RunClaim 和预算 reservation；
4. 固定调用时的 AFWP、Attempt、Workspace、Executor、策略和 Capsule；
5. 处理 Adapter 请求 outcome unknown，不盲重发非幂等模型调用；
6. 允许一个 Attempt 有多个激活窗口，同时保持 Candidate-first 和作者 fencing；
7. 为 Control Room 提供低频、可重建、可解释的运行事实；
8. 在 Boss/Worker/控制平面重启后继续推进，不依赖常驻会话或每秒 heartbeat。

M1 不实现：任意 cron 工作流编辑器、多区域调度、跨组织现金结算、浏览器内 Agent Runtime、完整
Plugin Marketplace、自动修改路由策略、保存完整 chain-of-thought。

## 2. 强制不变量

| ID | 不变量 |
| --- | --- |
| `INV-I01` | `Attempt` 是语义开发尝试；`InvocationRun` 是激活窗口；二者不得合并 |
| `INV-I02` | InvocationRun 结束不能直接完成 Attempt、WorkPackage、VerificationRun 或 Submission |
| `INV-I03` | 恢复创建新的 InvocationRun；旧 Run 不从终态恢复到运行态 |
| `INV-I04` | RunClaim 不能代替作者 Lease；所有作者侧正式 mutation 继续校验 Lease/generation/fencing |
| `INV-I05` | 同义 Signal 可合并，但原始 Signal 永不删除或覆盖 |
| `INV-I06` | revision、generation、权限 Decision、取消、安全事件和策略变化不得与旧 Intent 合并 |
| `INV-I07` | Run 上下文在创建后不可原地覆盖；新输入只能生成新 Signal/Run |
| `INV-I08` | Claim/Run 与预算预留同事务；事务提交前不得调用 Adapter |
| `INV-I09` | 非幂等外部调用结果未知时先对账，禁止原样盲重发 |
| `INV-I10` | Capsule、事件、日志和投影不得包含 Secret、可用 bearer 或隐藏 chain-of-thought |
| `INV-I11` | heartbeat/NodeSession 只表示连接健康，不产生语义进度或自动模型调用 |
| `INV-I12` | UI、SSE、Agent 自报和进程 PID 都不是 InvocationRun 授权事实 |

## 3. 统一术语

### 3.1 RunSignal

`RunSignal` 是 append-only 的输入事实。建议种类：

```rust
pub enum RunSignalKind {
    AssignmentGranted,
    WakeConditionSatisfied,
    QuestionAnswered,
    PermissionDecided,
    ArtifactAvailable,
    SemanticDeadlineReached,
    BudgetThresholdReached,
    NudgeRequested,
    DiagnoseRequested,
    PolicyChanged,
    PackageRevisionChanged,
    LeaseGenerationChanged,
    CancelRequested,
    SecurityTermination,
    RoutineDue,
    ReconcileRequested,
}
```

Signal 只说明“某个事实可能需要处理”，不授予 Lease、RunClaim、预算或能力。

### 3.2 InvocationIntent

`InvocationIntent` 是调度器对同义 Signal 的合并结果：

```rust
pub enum InvocationIntentState {
    Pending,
    Claimed,
    Dispatched,
    Satisfied,
    Cancelled,
    DeadLetter,
}
```

每个 active Intent 绑定一个 subject snapshot 和 dedup digest。一个 Intent 最多创建一个
InvocationRun；需要重试/恢复时创建新的 Signal 和 Intent，并引用旧 Run。

### 3.3 InvocationRun

```rust
pub enum InvocationRunState {
    Reserved,
    Starting,
    Running,
    Reconciling,
    Completed,
    Failed,
    Cancelled,
}

pub enum InvocationOutcome {
    Progressed,
    WaitingInput,
    CandidateProposed,
    PlanProposed,
    DecisionRequested,
    NoProgress,
    InfrastructureFailure,
    OutcomeUnknown,
    Cancelled,
}
```

`Completed` 只说明本次激活得到了一个合法结构化 outcome；`CandidateProposed` 仍须经过本地门禁、
有效作者 Lease、COMPLETE CandidateArtifact 和 `RecordCandidate` 才形成正式 Candidate。

### 3.4 RunClaim

RunClaim 是执行/报告指定 InvocationRun 的短期权利：

```rust
pub enum RunClaimState { Active, Completed, Expired, Revoked, Superseded }
```

每次成功领取产生新的历史行和严格递增 `claim_generation`。同一个 Run 同时最多一个 ACTIVE claim。
Run 进入终态后所有 claim 终结；旧 generation 不能报告新的 receipt-miss outcome。

### 3.5 SessionCapsule

Capsule 是不可变、内容寻址的恢复输入，不是聊天历史数据库。允许内容：

- Adapter session opaque ref；
- package/revision/attempt/run binding；
- checkpoint、Worktree/Workspace Head、AC matrix digest；
- 开放 blocker、typed question、已批准 Decision 引用；
- 最近结构化 outcome 和下一安全动作；
- Executor/runtime/prompt fingerprint；
- 可选加密 transcript Artifact 的 URI+digest（受项目策略控制）。

禁止内容：可用 token、Secret、Git credential、provider key、隐藏 chain-of-thought、未经 ACL 批准的
源码副本、任意宿主路径。

## 4. 领域数据结构

### 4.1 强类型 ID

在 `agentforge-domain` 增加：

```rust
id_type!(RunSignalId);
id_type!(InvocationIntentId);
id_type!(InvocationRunId);
id_type!(RunClaimId);
id_type!(SessionCapsuleId);
id_type!(BudgetAccountId);
id_type!(BudgetReservationId);
```

所有 ID 由受信应用边界注入 UUID v7。协议展示 key 与内部 UUID 分离，规则继承文档 01。

### 4.2 RunSignal 记录

```rust
pub struct RunSignal {
    pub id: RunSignalId,
    pub project_id: ProjectId,
    pub subject: InvocationSubject,
    pub kind: RunSignalKind,
    pub cause_event_id: Option<EventId>,
    pub causation_id: Option<Uuid>,
    pub source_actor_id: ActorId,
    pub snapshot: InvocationBinding,
    pub normalized_reason: String,
    pub payload_digest: Sha256Digest,
    pub not_before: ServerInstant,
    pub deadline: Option<ServerInstant>,
    pub priority: u8,
}
```

`InvocationSubject` 是 tagged union，M1 至少支持 `Attempt`、`BossSession`、`VerificationJob`、
`Obligation`。subject 不是自由文本。

### 4.3 InvocationBinding

```rust
pub struct InvocationBinding {
    pub package_id: Option<PackageId>,
    pub package_revision_id: Option<PackageRevisionId>,
    pub package_hash: Option<Sha256Digest>,
    pub attempt_id: Option<AttemptId>,
    pub author_fencing_token: Option<FencingToken>,
    pub workspace_head: Option<GitObjectId>,
    pub policy_revision_id: Uuid,
    pub executor_fingerprint: Option<Sha256Digest>,
    pub input_capsule_id: Option<SessionCapsuleId>,
    pub input_capsule_digest: Option<Sha256Digest>,
}
```

字段适用关系由构造器验证。例如 Attempt subject 必须有 package/revision/hash/attempt/generation；
纯规划 subject 可以没有作者 fencing，但仍必须有 Project/Policy binding。

### 4.4 InvocationIntent 聚合

```rust
pub struct InvocationIntent {
    pub id: InvocationIntentId,
    pub project_id: ProjectId,
    pub subject: InvocationSubject,
    pub binding: InvocationBinding,
    pub dedup_digest: Sha256Digest,
    pub state: InvocationIntentState,
    pub primary_signal_id: RunSignalId,
    pub signal_count: u32,
    pub claimed_by: Option<Uuid>,
    pub claim_until: Option<ServerInstant>,
    pub invocation_run_id: Option<InvocationRunId>,
    pub version: AggregateVersion,
}
```

合法转移：

| 当前 | 命令 | 下一状态 | 约束 |
| --- | --- | --- | --- |
| 无 | `CreateIntent` | `Pending` | active dedup digest 唯一 |
| `Pending` | `AttachEquivalentSignal` | `Pending` | binding/dedup 完全相同；signal count +1 |
| `Pending` | `ClaimIntent` | `Claimed` | `not_before <= db_now`，claim token/expiry |
| `Claimed` | `DispatchIntent` | `Dispatched` | 同事务创建 Run/RunClaim/预算 |
| `Pending/Claimed` | `CancelIntent` | `Cancelled` | subject 已取消/被替代；保存原因 |
| `Dispatched` | `SatisfyIntent` | `Satisfied` | 绑定 Run 已终结并登记 outcome |
| `Claimed` | `RetryIntentClaim` | `Pending` | claim 到期，旧 claim token 失效 |
| `Pending/Claimed` | `DeadLetterIntent` | `DeadLetter` | 重试耗尽并创建治理/运维 Case |

`Dispatched` 不返回 `Pending`；Run 失败产生新 Signal/Intent。

### 4.5 InvocationRun 聚合

```rust
pub struct InvocationRun {
    pub id: InvocationRunId,
    pub intent_id: InvocationIntentId,
    pub project_id: ProjectId,
    pub subject: InvocationSubject,
    pub binding: InvocationBinding,
    pub adapter_id: String,
    pub executor_id: Uuid,
    pub executor_fingerprint: Sha256Digest,
    pub routing_decision_id: Option<Uuid>,
    pub input_capsule_id: Option<SessionCapsuleId>,
    pub input_capsule_digest: Option<Sha256Digest>,
    pub budget_reservation_id: BudgetReservationId,
    pub state: InvocationRunState,
    pub current_claim_generation: u64,
    pub external_invocation_key: String,
    pub output_capsule_id: Option<SessionCapsuleId>,
    pub output_capsule_digest: Option<Sha256Digest>,
    pub outcome: Option<InvocationOutcome>,
    pub outcome_digest: Option<Sha256Digest>,
    pub version: AggregateVersion,
}
```

合法转移：

| 当前 | 命令 | 下一状态 | 强制条件 |
| --- | --- | --- | --- |
| 无 | `ReserveInvocationRun` | `Reserved` | Intent claim 当前；预算可用；binding 固定 |
| `Reserved` | `MarkDispatchStarted` | `Starting` | ACTIVE RunClaim 与 Outbox dispatch ID |
| `Starting` | `ObserveAdapterStarted` | `Running` | invocation key/session proof 匹配 |
| `Starting/Running` | `BeginReconciliation` | `Reconciling` | claim/dispatcher 超时或 outcome unknown |
| `Running/Reconciling` | `CompleteInvocationRun` | `Completed` | 结构化 outcome、usage、输出 Capsule/digest |
| `Reserved/Starting/Running/Reconciling` | `FailInvocationRun` | `Failed` | 分类、证据和预算结算齐全 |
| 非终态 | `CancelInvocationRun` | `Cancelled` | typed 原因；停止/对账义务已创建 |

`Completed/Failed/Cancelled` 为不可逆终态。终态 outcome 不能由 Adapter 自报直接产生；application
handler 必须校验 claim、Run version、binding、usage 和 Capsule。

## 5. PostgreSQL 物理模型基线

以下为迁移必须表达的约束；具体列名可在实现包中机械调整，但不得弱化唯一性和不可变性。

```sql
CREATE TABLE run_signals (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    subject_type text NOT NULL,
    subject_id uuid NOT NULL,
    kind text NOT NULL,
    cause_event_id uuid REFERENCES domain_events(event_id),
    causation_id uuid,
    source_actor_id uuid NOT NULL,
    binding jsonb NOT NULL,
    binding_digest bytea NOT NULL CHECK (octet_length(binding_digest)=32),
    normalized_reason text NOT NULL,
    payload_digest bytea NOT NULL CHECK (octet_length(payload_digest)=32),
    not_before timestamptz NOT NULL,
    deadline timestamptz,
    priority smallint NOT NULL CHECK (priority BETWEEN 0 AND 100),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (project_id, cause_event_id, kind, subject_type, subject_id,
            binding_digest, payload_digest)
);

CREATE TRIGGER run_signals_are_immutable
BEFORE UPDATE OR DELETE ON run_signals
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

CREATE TABLE invocation_intents (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    subject_type text NOT NULL,
    subject_id uuid NOT NULL,
    binding jsonb NOT NULL,
    binding_digest bytea NOT NULL CHECK (octet_length(binding_digest)=32),
    dedup_digest bytea NOT NULL CHECK (octet_length(dedup_digest)=32),
    state text NOT NULL CHECK (state IN (
      'PENDING','CLAIMED','DISPATCHED','SATISFIED','CANCELLED','DEAD_LETTER')),
    primary_signal_id uuid NOT NULL REFERENCES run_signals(id),
    signal_count integer NOT NULL DEFAULT 1 CHECK (signal_count > 0),
    claimed_by uuid,
    claim_token_hash bytea,
    claim_until timestamptz,
    invocation_run_id uuid,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    event_seq bigint NOT NULL DEFAULT 0 CHECK (event_seq >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE UNIQUE INDEX invocation_intents_active_dedup_idx
    ON invocation_intents(project_id, dedup_digest)
    WHERE state IN ('PENDING','CLAIMED','DISPATCHED');
CREATE INDEX invocation_intents_due_idx
    ON invocation_intents(state, updated_at, id)
    WHERE state IN ('PENDING','CLAIMED');

CREATE TABLE session_capsules (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    subject_type text NOT NULL,
    subject_id uuid NOT NULL,
    previous_capsule_id uuid REFERENCES session_capsules(id),
    adapter_id text NOT NULL,
    adapter_session_ref_ciphertext bytea,
    content_uri text NOT NULL,
    content_digest bytea NOT NULL CHECK (octet_length(content_digest)=32),
    manifest jsonb NOT NULL,
    security_level text NOT NULL,
    created_by_run_id uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (project_id, content_digest)
);

CREATE TRIGGER session_capsules_are_immutable
BEFORE UPDATE OR DELETE ON session_capsules
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

CREATE TABLE invocation_runs (
    id uuid PRIMARY KEY,
    intent_id uuid NOT NULL UNIQUE REFERENCES invocation_intents(id),
    project_id uuid NOT NULL REFERENCES projects(id),
    subject_type text NOT NULL,
    subject_id uuid NOT NULL,
    binding jsonb NOT NULL,
    binding_digest bytea NOT NULL CHECK (octet_length(binding_digest)=32),
    adapter_id text NOT NULL,
    executor_id uuid NOT NULL,
    executor_fingerprint bytea NOT NULL CHECK (octet_length(executor_fingerprint)=32),
    routing_decision_id uuid,
    input_capsule_id uuid REFERENCES session_capsules(id),
    input_capsule_digest bytea,
    budget_reservation_id uuid NOT NULL,
    state text NOT NULL CHECK (state IN (
      'RESERVED','STARTING','RUNNING','RECONCILING','COMPLETED','FAILED','CANCELLED')),
    current_claim_generation bigint NOT NULL DEFAULT 0 CHECK (current_claim_generation >= 0),
    external_invocation_key text NOT NULL UNIQUE,
    output_capsule_id uuid REFERENCES session_capsules(id),
    output_capsule_digest bytea,
    outcome text,
    outcome_digest bytea,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    event_seq bigint NOT NULL DEFAULT 0 CHECK (event_seq >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    terminalized_at timestamptz,
    CHECK ((state IN ('COMPLETED','FAILED','CANCELLED')) =
           (terminalized_at IS NOT NULL)),
    CHECK (outcome_digest IS NULL OR octet_length(outcome_digest)=32)
);

ALTER TABLE invocation_intents
  ADD CONSTRAINT invocation_intents_run_fk
  FOREIGN KEY (invocation_run_id, id)
  REFERENCES invocation_runs(id, intent_id)
  DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE run_signal_intents (
    signal_id uuid PRIMARY KEY REFERENCES run_signals(id),
    intent_id uuid NOT NULL REFERENCES invocation_intents(id),
    folded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE invocation_run_claims (
    id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES invocation_runs(id),
    claim_request_id uuid NOT NULL,
    claim_generation bigint NOT NULL CHECK (claim_generation > 0),
    holder_id uuid NOT NULL,
    token_hash bytea NOT NULL CHECK (octet_length(token_hash)=32),
    state text NOT NULL CHECK (state IN (
      'ACTIVE','COMPLETED','EXPIRED','REVOKED','SUPERSEDED')),
    expires_at timestamptz NOT NULL,
    result_digest bytea,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    UNIQUE (run_id, claim_request_id),
    UNIQUE (run_id, claim_generation),
    CHECK (result_digest IS NULL OR octet_length(result_digest)=32)
);

CREATE UNIQUE INDEX invocation_run_claims_one_active_idx
    ON invocation_run_claims(run_id) WHERE state='ACTIVE';
CREATE INDEX invocation_run_claims_expiry_idx
    ON invocation_run_claims(expires_at, id) WHERE state='ACTIVE';
```

终态 InvocationRun 不得 DELETE；更新触发器必须拒绝从终态回到非终态。Capsule 的
`adapter_session_ref_ciphertext` 仅保存 AEAD ciphertext 或外部 secret ref，普通日志/JSON 不得输出。

## 6. 预算账户与 Reservation

### 6.1 预算单位和类别

所有金额/资源使用整数最小单位，不使用浮点。M1 至少支持：

```text
AUTHOR_MODEL
AUTHOR_COMPUTE
RUNNER
REVIEWER
ARTIFACT
INTEGRATION
RECOVERY
```

AFWP 发布时把预算拆为不可互相挪用的 pool。作者 pool 不得消费 Runner/Reviewer/Integration reserve。

### 6.2 预留树

```sql
CREATE TABLE budget_accounts (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    scope_type text NOT NULL,
    scope_id uuid NOT NULL,
    category text NOT NULL,
    limit_units bigint NOT NULL CHECK (limit_units >= 0),
    reserved_units bigint NOT NULL DEFAULT 0 CHECK (reserved_units >= 0),
    spent_units bigint NOT NULL DEFAULT 0 CHECK (spent_units >= 0),
    version bigint NOT NULL DEFAULT 0,
    UNIQUE (scope_type, scope_id, category),
    CHECK (reserved_units + spent_units <= limit_units)
);

CREATE TABLE budget_reservations (
    id uuid PRIMARY KEY,
    account_id uuid NOT NULL REFERENCES budget_accounts(id),
    parent_reservation_id uuid REFERENCES budget_reservations(id),
    purpose_type text NOT NULL,
    purpose_id uuid NOT NULL,
    amount_units bigint NOT NULL CHECK (amount_units > 0),
    allocated_units bigint NOT NULL DEFAULT 0 CHECK (allocated_units >= 0),
    spent_units bigint NOT NULL DEFAULT 0 CHECK (spent_units >= 0),
    state text NOT NULL CHECK (state IN (
      'ACTIVE','SETTLED','RELEASED','EXPIRED','CANCELLED')),
    expires_at timestamptz,
    version bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (purpose_type, purpose_id, account_id),
    CHECK (allocated_units + spent_units <= amount_units)
);
```

- 根 reservation 从 account 原子增加 `reserved_units`；
- child reservation 只消耗 parent 未分配额度，不再次增加 account reserved，避免双计；
- ClaimPackage 创建 Attempt author envelope；InvocationRun 创建其 child reservation；
- 完成时以签名 usage/cost event 结算，未使用额度释放到 parent；
- outcome unknown 时额度保持 HELD/ACTIVE，直到 reconciliation 或有上限超时；
- 预算追加必须走 GovernanceCase，不允许管理员直接改 account 行；
- `limit_units` 改变通过新 Policy/Package revision 或审计命令，不能低于已 spent+reserved。

### 6.3 原子启动顺序

创建 InvocationRun 的事务锁序为：

```text
project
-> budget accounts（scope rank + UUID）
-> work_graph/package（若适用）
-> attempt
-> author lease（若需要作者 mutation）
-> invocation intent
-> parent budget reservation
-> invocation run
-> run claim
```

事务内：

1. 命中幂等回执；
2. 重算 Intent binding 和 current policy；
3. 锁/验证作者 Lease 或 service job claim；
4. 检查 parent reservation、category 和余额；
5. 插入 child reservation、InvocationRun、RunClaim；
6. Intent CAS 到 `DISPATCHED`；
7. 追加事件、Outbox 和回执；
8. Commit 后 Dispatcher 才能调用 Adapter。

任何一步失败，外部调用数必须为零。

## 7. Signal 归一化与 Coalescing

### 7.1 dedup digest

```text
SHA-256(JCS({
  schema,
  project_id,
  subject_type,
  subject_id,
  package_revision_id,
  package_hash,
  attempt_id,
  fencing_generation,
  signal_class,
  normalized_reason,
  policy_revision_id,
  workspace_head,
  input_capsule_digest
}))
```

`payload_digest` 不直接等于 dedup digest：多个相同“有新评论”的 Signal 可以合并，但评论内容仍以独立
Artifact/Note 保存并由 signal mapping 引用。

### 7.2 归一化算法

```text
BEGIN
  lock actor/idempotency key
  authenticate + authorize source
  validate typed signal and source event
  insert immutable RunSignal (duplicate returns existing)
  recompute current binding from authoritative rows
  if signal binding is stale:
      store SignalRejectedAsStale event; do not create Intent
  else:
      compute dedup digest
      SELECT active intent FOR UPDATE by (project,dedup)
      if found and kind is coalescible:
          attach signal mapping; increment signal_count with CAS
      else:
          create new PENDING Intent + mapping
  append events/outbox/receipt
COMMIT
```

取消、安全终止、permission Decision 和 generation change 永远走 non-coalescible 分支；它们可以在同一
事务取消/标记旧 Intent，并创建新 Intent。

### 7.3 防唤醒风暴

- 同 subject active Intent 上限默认 4；超过时创建 `invocation.signal_storm` GovernanceCase；
- 每项目每分钟新 Intent 和新 Run 有 token bucket；安全终止/Lease revoke 不受普通限流阻断；
- 多个 Signal 只合并唤醒，不丢失优先级：Intent 使用 max priority、最早 deadline；
- coalescing 不延长旧 Intent 的最大等待时间；
- `@mention`、重复评论和 SSE 重投不能直接绕过预算产生 Run。

## 8. Dispatcher 与 AgentAdapter

### 8.1 Application port

M2 冻结以下通用边界：

```rust
#[async_trait::async_trait]
pub trait AgentAdapter {
    async fn prepare(&self, input: PrepareInvocation) -> AdapterResult<PreparedInvocation>;
    async fn start(&self, input: StartInvocation) -> AdapterResult<AdapterStartReceipt>;
    async fn query(&self, key: &ExternalInvocationKey) -> AdapterResult<AdapterObservation>;
    async fn resume(&self, input: ResumeInvocation) -> AdapterResult<AdapterStartReceipt>;
    async fn soft_interrupt(&self, input: InterruptInvocation) -> AdapterResult<()>;
    async fn cancel(&self, input: CancelInvocation) -> AdapterResult<AdapterCancelReceipt>;
    async fn snapshot(&self, input: SnapshotInvocation) -> AdapterResult<AdapterSnapshot>;
}
```

Adapter 不得直接写 WorkPackage/Attempt/Lease/Candidate 表。结果通过 application command 回写。

### 8.2 Adapter capability

每个 Adapter profile 声明：

```text
structured_output
session_resume
query_by_invocation_key
soft_interrupt
hard_cancel
usage_receipt
snapshot
```

缺 `query_by_invocation_key` 的 Adapter 只能承接 `safe_to_repeat=true` 的显式任务，或在 unknown 时终结并
升级人工诊断；不得对代码写入/外部副作用任务盲重试。

### 8.3 jcode 映射

- 一个 InvocationRun 对应一次 Worker Turn Pump 激活窗口；
- 窗口内多个 `turn_id` 只写 Worker Journal，不逐 Turn 写中央表；
- `external_invocation_key` 映射 Attempt/Run marker；
- 输入 Capsule 引用固定 jcodeHome/session/checkpoint；
- `turn_done` 不是 InvocationRun outcome；只有 Turn Pump 产生结构化 StopReason 后才能终结 Run；
- 作者正式副作用仍由 Worker daemon 携带当前作者 Lease proof 调用控制 API。

## 9. 恢复与对账矩阵

| 故障点 | 权威观察 | 恢复动作 |
| --- | --- | --- |
| 创建 Intent 前崩溃 | Signal 已提交/未提交 | Outbox/Inbox 重放，active dedup 唯一 |
| Run 事务 COMMIT 前崩溃 | 无 Run/Outbox | 不调用 Adapter，安全重试同 key |
| COMMIT 后、dispatch 前崩溃 | Run=`RESERVED`，Outbox pending | 新 Dispatcher 领取并发送相同 invocation key |
| Adapter 收到前网络失败且可证明未执行 | query=`NOT_FOUND` | 终结旧 Run 或按策略创建新 Run；不复活旧 Run |
| Adapter 已启动、ACK 丢失 | query/session marker 存在 | `STARTING -> RUNNING`，attach/继续观察 |
| Adapter 已完成、结果丢失 | history/result digest 可证明 | `RECONCILING -> COMPLETED`，登记原结果 |
| 无法判断是否执行 | 无可靠 query/history | `FAILED/OUTCOME_UNKNOWN`，保留预算并创建诊断 Signal |
| RunClaim 过期，旧 holder 回写 | current generation 更高/Run terminal | receipt miss 返回 `AF_INVOCATION_CLAIM_STALE` |
| Capsule 上传成功、DB bind 失败 | orphan content digest | TTL GC；不得猜测绑定 |
| DB bind 成功、响应丢失 | command receipt/Capsule row | 同 key 原样回放 |
| 作者 Lease 在 Run 中过期 | Lease server-time invalid | 关闭作者 mutation；Run 可只读收尾/salvage，不得登记 Candidate |

reconciler 使用有上限 backoff 和 deterministic jitter。连续失败创建 Operation/Governance Case，不能永久
保持 `RECONCILING` 烧预算。

## 10. API 契约

### 10.1 Query

```text
GET /v1/run-signals?project_id=&subject=&cursor=
GET /v1/invocation-intents?project_id=&state=&cursor=
GET /v1/invocation-intents/{id}
GET /v1/invocation-runs?project_id=&attempt_id=&state=&cursor=
GET /v1/invocation-runs/{id}
GET /v1/invocation-runs/{id}/timeline
GET /v1/session-capsules/{id}/metadata
GET /v1/budgets/{scope_type}/{scope_id}/summary
```

Capsule query 默认只返回 metadata/digest；内容下载使用短期、scope-bound capability。

### 10.2 Command

```text
POST /v1/run-signals
POST /v1/invocation-intents/{id}:claim
POST /v1/invocation-intents/{id}:dispatch
POST /v1/invocation-runs/{id}:started
POST /v1/invocation-runs/{id}:complete
POST /v1/invocation-runs/{id}:fail
POST /v1/invocation-runs/{id}:cancel
POST /v1/invocation-runs/{id}:reconcile
POST /v1/invocation-runs/{id}/capsules
POST /v1/budget-reservations/{id}:settle
POST /v1/budget-reservations/{id}:release
```

内部 scheduler/dispatcher endpoint 只对 service identity 开放。操作者不直接 claim/dispatch；UI 使用
治理文档定义的 typed command。

### 10.3 完成请求摘要

```json
{
  "claim_id": "uuid",
  "claim_generation": 2,
  "expected_run_version": 3,
  "outcome": "progressed",
  "outcome_digest": "sha256:...",
  "output_capsule": {
    "id": "uuid",
    "digest": "sha256:..."
  },
  "usage": {
    "model_units": 8,
    "cpu_millis": 1200,
    "wall_millis": 8200
  },
  "evidence_refs": ["artifact://..."]
}
```

服务端读取当前 Run/claim/budget/binding，不能相信客户端提供的 project/Attempt/revision。

## 11. 稳定错误码

| Code | HTTP | 含义 |
| --- | ---: | --- |
| `AF_SIGNAL_STALE` | 409 | Signal binding 已被新 revision/generation 替代 |
| `AF_SIGNAL_NOT_COALESCIBLE` | 409 | 客户端试图强行合并必须独立的 Signal |
| `AF_INTENT_NOT_DISPATCHABLE` | 409 | Intent 状态、deadline 或 binding 不允许 dispatch |
| `AF_INVOCATION_CLAIM_STALE` | 409 | RunClaim ID/generation/holder/version 不再当前 |
| `AF_INVOCATION_OUTCOME_UNKNOWN` | 409 | 必须先执行 reconciliation，不能盲重发 |
| `AF_CAPSULE_BINDING_MISMATCH` | 422 | Capsule subject/digest/runtime binding 不匹配 |
| `AF_BUDGET_RESERVATION_FAILED` | 422 | 指定 category 可用额度不足 |
| `AF_BUDGET_RESERVATION_STALE` | 409 | reservation/parent/account version 已变化 |
| `AF_ADAPTER_CAPABILITY_MISSING` | 422 | Adapter 缺少任务要求的恢复/结构化能力 |

所有写命令还继承 `AF_IDEMPOTENCY_*`、`AF_VERSION_STALE`、`AF_FORBIDDEN` 等通用码。

## 12. 领域事件

M1 最小事件集：

```text
RunSignalRecorded
RunSignalRejectedAsStale
RunSignalFoldedIntoIntent
InvocationIntentCreated
InvocationIntentClaimed
InvocationIntentDispatched
InvocationIntentCancelled
InvocationIntentDeadLettered
InvocationRunReserved
InvocationRunStarted
InvocationRunReconciliationStarted
InvocationRunCompleted
InvocationRunFailed
InvocationRunCancelled
RunClaimGranted
RunClaimExpired
RunClaimSuperseded
SessionCapsuleRecorded
BudgetReserved
BudgetSettled
BudgetReleased
BudgetThresholdReached
```

事件带 correlation/causation、schema version 和 digest，不带 token、Prompt 或 Capsule 内容。

## 13. 可观测性和 2C4G 限制

指标：

```text
agentforge_run_signals_total{kind,result}
agentforge_invocation_intents{state}
agentforge_intent_coalesced_total{kind}
agentforge_invocation_runs{state,adapter}
agentforge_invocation_duration_seconds{adapter,outcome}
agentforge_invocation_reconciliation_total{result}
agentforge_run_claim_stale_total
agentforge_budget_units{category,state}
agentforge_capsule_bytes_total{security_level}
```

不得以 project/run/actor ID 作 Prometheus label。M1 默认：Intent/Run dispatch 并发 4、reconcile 并发 2、
每项目 active Intent 100、每 subject active Intent 4、Signal body 64 KiB、Capsule manifest 256 KiB；大内容
外置。容量超限返回 429/治理 Case，不扩张无界内存队列。

## 14. 测试与验收矩阵

### 14.1 Domain/property

- 任意命令序列不能让终态 Run 回到运行态；
- 一个 Intent 最多一个 InvocationRun；
- 同 Run 最多一个 ACTIVE claim；generation 严格递增；
- Run 完成不改变 Attempt/Package 状态；
- coalescing key 任一 binding 改变都产生新 Intent；
- child reservation 之和与 spent 不超过 parent；account 不超 limit；
- `OutcomeUnknown` 不能映射为 `Completed/Progressed`。

### 14.2 PostgreSQL concurrency

| ID | 场景 | 通过条件 |
| --- | --- | --- |
| `DB-INV-001` | 100 路插入同因果 Signal | 1 Signal 事实/1 active Intent；请求回放一致 |
| `DB-INV-002` | 32 路 claim 同 Intent | 1 个 claim/Run；其余稳定冲突 |
| `DB-INV-003` | 32 路启动共享预算 Run | 成功总额不超余额；无负数/超卖 |
| `DB-INV-004` | cancel 与 dispatch 并发 | 只有一个线性化结果；取消后无新 Adapter dispatch |
| `DB-INV-005` | claim expiry 与 complete 并发 | 旧 generation 不能在新 claim 后完成 |
| `DB-INV-006` | Capsule bind 相同 digest 并发 | 一个不可变 Capsule；所有引用一致 |
| `DB-INV-007` | policy/revision/fence 变化同时 coalesce | 旧 Intent 取消/保留审计，新事实独立 Intent |

### 14.3 Crash points

在 Run 事务各写点、Outbox publish、Adapter start、start ACK、result、Capsule upload/bind、预算 settle 前后
注入 kill。每个 seed 重启后满足：外部调用至多符合 Adapter 幂等能力；无双 budget spend；无旧 claim
正式回写；无孤儿 active Intent；无法判定时形成明确 OutcomeUnknown/治理任务。

### 14.4 Security

- 使用 RunClaim 调作者 Candidate API必须返回 forbidden/stale lease；
- Capsule 中植入 JWT、SSH key、provider key 和 chain-of-thought fixture 均被拒绝/隔离；
- Signal payload 的任意 URL/path/capability 不能扩大 AFWP；
- 跨项目 Signal/Intent/Run/Capsule 查询返回不可枚举的 404/403；
- restricted Capsule URI 不能被 public Executor 获得。

## 15. 实现切片和完成定义

| 工单 | 本规范范围 |
| --- | --- |
| `WP-M1-008` | RunSignal、InvocationIntent、InvocationRun、RunClaim 领域模型与 coalescing/reference scheduler |
| `WP-M1-009` | BudgetAccount/Reservation、Claim/Run 原子预算、usage settle |
| `WP-M1-011` | Invocation query projection、timeline 和 sparse SSE invalidation |
| `WP-M2-008` | AgentAdapter、jcode 映射、SessionCapsule、unknown outcome 对账 |

本模块完成必须同时满足：

- ADR-0006 的全部验证标准；
- `DB-INV-001..007` 在真实 PostgreSQL 通过；
- Scripted Adapter 的全部 crash seed 可确定性重放；
- Run 完成不会绕过 AFWP、作者 Lease、Candidate-first 或独立验收；
- 2C4G 压力下 Signal storm 会背压/coalesce，不出现无界 Run；
- 文档 08 新增的 domain、DB、adapter、chaos 与 security 门禁均有 Evidence。
