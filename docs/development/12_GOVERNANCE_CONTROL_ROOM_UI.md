# 12：治理决策台与 Control Room UI 实施规范

> 状态：M1-A 冻结规格
> 适用范围：`agentforge-domain`、`agentforge-application`、`agentforge-storage-postgres`、
> `agentforge-control-plane`、Control Room Web UI、审计与投影 Worker
> 架构决定：[ADR-0007](../adr/ADR-0007-GOVERNANCE-DECISION-DESK.md)
> 调用编排：[11_INVOCATION_ORCHESTRATION.md](11_INVOCATION_ORCHESTRATION.md)
> 安全基线：[07_SECURITY_THREAT_MODEL.md](07_SECURITY_THREAT_MODEL.md)

## 1. 目标、范围与阶段

本规范把人工审批、异常处置、运行观察和日常协作变成可审计的控制面能力。目标是：操作者先看到
“现在需要我决定什么”，再看到运行和图表，而不是在事件日志、Agent 对话和数据库之间寻找真相。

M1 Thin Control Room 必须实现：

1. `GovernanceCase`、不可变 `Decision` 和动作摘要；
2. 基础 Decision Desk、Project Control Room、Run/Attempt timeline 和预算摘要；
3. 可重建 read model、分页 Query API 和稀疏 SSE invalidation；
4. 批准、拒绝、请求修改、回答问题、nudge、项目 dispatch 暂停、Run 取消、Lease 撤销、Executor drain/
   quarantine 等受限 typed command；
5. 桌面与窄屏关键路径、键盘操作、安全渲染和 projection replay 测试。

M5 再实现：PolicyRevision simulation/activation/rollback、多审批人 quorum、同质 Case 批处理、完整路由/
预算解释、策略影响图和跨项目治理队列。

不在 M1 范围：自由表单工作流编辑器、可变 Ticket 取代 AFWP、浏览器直接修改 aggregate、浏览器折叠
原始 Event Ledger、任意 shell/URL 审批、聊天内容自动扩大权限、保存完整 Prompt 或 chain-of-thought、
原生移动客户端。

## 2. 强制不变量

| ID | 不变量 |
| --- | --- |
| `GOV-I01` | WorkPackage/PackageRevision/AFWP 是执行契约；GovernanceCase、Note 和 UI 卡片不得原地修改它 |
| `GOV-I02` | Decision 绑定精确 action digest、subject version、PolicyRevision 和 expiry；任一变化使旧批准失效 |
| `GOV-I03` | Decision 是授权事实，不是副作用成功事实；只有验证过的 ExecutionReceipt/Event 才能使 Case `APPLIED` |
| `GOV-I04` | UI projection 可丢弃重建，绝不是 Lease、fencing、Candidate、验证、路由或审批的授权输入 |
| `GOV-I05` | 所有 mutation 使用 typed command、RBAC、Idempotency-Key、`If-Match` 和审计 correlation |
| `GOV-I06` | 作者 Lease、RunClaim、GovernanceCase、Decision 和一次性执行 capability 互不替代 |
| `GOV-I07` | 权限等待使用 `Attempt::WaitingInput + PermissionDecided`，不得增加 `WAITING_PERMISSION` 核心状态 |
| `GOV-I08` | 普通评论是不可执行 OperatorNote；`@mention` 最多产生去重 RunSignal，不授予预算、Lease 或能力 |
| `GOV-I09` | 取消 Run 不等于撤销作者 Lease；撤销 Lease 不改写已封存 Candidate；所有动作保持 Candidate-first |
| `GOV-I10` | critical Case 的 requester/author 不得是唯一 approver；独立 Reviewer/Runner 规则不因人工批准而放宽 |
| `GOV-I11` | 浏览器不接收 bearer、fencing token、节点私钥、完整私有 Prompt、Secret 或隐藏 chain-of-thought |
| `GOV-I12` | 所有可操作状态由服务端事实推导；前端不得以 Agent 自报、SSE 到达顺序或本地计时器推断授权 |

## 3. 角色、责任与默认权限

| 角色 | 默认读取 | 允许的命令 | 明确禁止 |
| --- | --- | --- | --- |
| Project Operator | 所属项目的裁剪投影、Evidence 摘要 | AnswerQuestion、RequestNudge、Pause/ResumeProjectDispatch、低风险 Case 决策 | 修改 AFWP 行、伪造 receipt、查看 Secret |
| Approver | 分配给自己的 Case、action preview、必要证据 | DecideGovernanceCase | 改 action 参数后沿用旧批准、审批自己发起的 critical 请求 |
| Architect/Planner | WorkGraph、requirements、PlanPatch 影响 | ProposeRevision、提交 PlanPatch Case | 原地编辑 ACTIVE PackageRevision |
| Security Operator | 安全 Case、Node/Executor、审计摘要 | Quarantine、Revoke、security Case 决策 | 绕过 quorum、修改已封存 Candidate |
| Auditor | 全部获授权的审计投影 | 无 mutation | 下载未授权 Capsule/Prompt/credential |
| Service Actor | scope-bound 内部队列/执行 API | 执行已批准 typed action、写 ExecutionReceipt | 使用浏览器 session、扩大 action scope |

RBAC 只决定 actor 是否可能执行命令；handler 仍必须读取 Case、action、subject、policy、generation、预算和
当前 aggregate version 重新授权。列表中“可读”也要经过 tenant/project/field-level ACL 裁剪。

## 4. 治理领域模型

### 4.1 GovernanceCase

安全文档中的早期名称 `ReviewItem` 统一迁移为 `GovernanceCase`，不得保留两个并行审批聚合。

```rust
pub enum GovernanceCaseKind {
    PermissionRequest,
    BudgetChange,
    PlanPatchApproval,
    PolicyActivation,
    RoutingException,
    SecurityResponse,
    ConflictResolution,
    OutcomeUnknown,
    ManualIntegration,
    OperationalIntervention,
}

pub enum GovernanceRisk { Low, Medium, High, Critical }

pub enum GovernanceCaseState {
    NeedsDecision,
    QuorumReached,
    Executing,
    Reconciling,
    Applied,
    Denied,
    ChangesRequested,
    Deferred,
    Expired,
    Cancelled,
    Superseded,
}
```

`GovernanceCase` 至少冻结：

```rust
pub struct GovernanceCase {
    pub id: GovernanceCaseId,
    pub project_id: ProjectId,
    pub kind: GovernanceCaseKind,
    pub risk: GovernanceRisk,
    pub subject: VersionedSubject,
    pub requested_by: ActorId,
    pub package_binding: Option<PackageBinding>,
    pub attempt_binding: Option<AttemptFencingBinding>,
    pub invocation_run_id: Option<InvocationRunId>,
    pub policy_revision_id: PolicyRevisionId,
    pub normalized_action: TypedGovernanceAction,
    pub action_digest: Sha256Digest,
    pub resource_snapshot_digest: Sha256Digest,
    pub evidence_refs: Vec<ArtifactRef>,
    pub required_quorum: ApprovalQuorum,
    pub due_at: Option<ServerInstant>,
    pub expires_at: ServerInstant,
    pub timeout_behavior: TimeoutBehavior,
    pub state: GovernanceCaseState,
    pub execution_receipt_id: Option<ExecutionReceiptId>,
    pub version: AggregateVersion,
}
```

`normalized_action` 是受版本控制的 tagged union，不得保存任意命令行、任意 SQL 或任意 URL。M1 支持：

```text
GrantCapability(scope, capability, limits, until)
RejectPermission(request_id, reason_code)
ApprovePlanPatch(plan_patch_id, expected_graph_version)
ApproveBudgetChange(account_id, delta, category, expiry)
PauseProjectDispatch(project_id, expected_project_version)
ResumeProjectDispatch(project_id, expected_project_version)
CancelInvocationRun(run_id, expected_run_version, stop_mode)
RevokeAuthorLease(lease_id, expected_generation, salvage_policy)
DrainExecutor(executor_id, mode)
QuarantineNode(node_id, evidence_digest)
ApproveRoutingException(decision_id, allowed_fingerprint, expiry)
```

自然语言说明、建议和替代方案不参与执行参数解析；执行只使用已校验的 typed action。

### 4.2 Case 状态机

| 当前 | 命令/事实 | 下一状态 | 强制条件 |
| --- | --- | --- | --- |
| 无 | `OpenGovernanceCase` | `NeedsDecision` | action schema、subject、policy、snapshot 和 digest 有效 |
| `NeedsDecision/Deferred` | `RecordDecision(APPROVE)` | `NeedsDecision` 或 `QuorumReached` | actor 独立性、权限、expiry、digest、幂等、quorum |
| `NeedsDecision/Deferred` | `RecordDecision(DENY)` | `Denied` | fallback Signal/Obligation 同事务 |
| `NeedsDecision/Deferred` | `RecordDecision(REQUEST_CHANGES)` | `ChangesRequested` | 创建 revision/PlanPatch proposal，不改原 AFWP |
| `NeedsDecision` | `RecordDecision(DEFER)` | `Deferred` | 不延长 Lease、RunClaim、case expiry 或批准 capability |
| `QuorumReached` | `BeginApprovedAction` | `Executing` | 重算 action digest/subject/policy；一次性 execution claim |
| `Executing` | `RecordExecutionReceipt(SUCCEEDED)` | `Applied` | receipt action digest、effect digest 和事件证明匹配 |
| `Executing` | outcome unknown | `Reconciling` | 创建有上限对账义务；禁止盲重发 |
| `Reconciling` | 已证明成功 | `Applied` | 远端 receipt/query proof 匹配 |
| `Reconciling` | 已证明未执行/失败 | `Denied` 或 `Superseded` | 记录 failure/fallback；需要重试时开新 Case |
| 非终态 | server time 超期 | `Expired` | 执行 timeout behavior，旧 capability 作废 |
| 非终态 | subject/action/policy 已变化 | `Superseded` | 保存 superseding case/revision 引用 |
| 非终态 | `CancelGovernanceCase` | `Cancelled` | 有权限 actor、reason、CAS；不撤回已发生副作用 |

`Applied/Denied/ChangesRequested/Expired/Cancelled/Superseded` 为业务终态。已进入 `Executing` 的外部动作不能
用 `CancelGovernanceCase` 假装未执行，必须走 `Reconciling`。

### 4.3 Decision

```rust
pub enum DecisionConclusion { Approve, Deny, RequestChanges, Defer }

pub struct Decision {
    pub id: DecisionId,
    pub case_id: GovernanceCaseId,
    pub case_version: AggregateVersion,
    pub actor_id: ActorId,
    pub actor_role_snapshot: RoleSnapshotDigest,
    pub conclusion: DecisionConclusion,
    pub action_digest: Sha256Digest,
    pub policy_revision_id: PolicyRevisionId,
    pub rationale_code: String,
    pub note_ref: Option<OperatorNoteId>,
    pub decided_at: ServerInstant,
    pub expires_at: ServerInstant,
    pub signature: DecisionSignature,
}
```

Decision 是 append-only。`rationale_code` 使用稳定枚举；人类解释存为 ACL 控制的 OperatorNote。重复
`Idempotency-Key + actor + case + payload_digest` 返回原 Decision；同 key 不同 payload 返回
`AF_IDEMPOTENCY_KEY_REUSED`。

### 4.4 ExecutionReceipt

ExecutionReceipt 与 Decision 分离：

```rust
pub struct ExecutionReceipt {
    pub id: ExecutionReceiptId,
    pub case_id: GovernanceCaseId,
    pub action_digest: Sha256Digest,
    pub execution_claim_generation: u64,
    pub executor_actor_id: ActorId,
    pub status: ExecutionReceiptStatus,
    pub external_effect_key: Option<String>,
    pub effect_digest: Sha256Digest,
    pub evidence_refs: Vec<ArtifactRef>,
    pub started_at: ServerInstant,
    pub observed_at: ServerInstant,
}
```

一个 Case 只有一个当前 execution claim。旧 generation receipt 被 fencing；外部调用发生在 Case/claim/Outbox
事务提交之后。非幂等副作用必须提供稳定 effect key 和 query/reconciliation 方式，否则只允许人工执行并
登记双人验证 evidence。

### 4.5 OperatorNote、Question 与 Mention

```rust
pub enum NoteSubject { Project, WorkPackageRevision, Attempt, InvocationRun, GovernanceCase, Submission }

pub struct OperatorNote {
    pub id: OperatorNoteId,
    pub subject: NoteSubject,
    pub author_id: ActorId,
    pub body_artifact: SanitizedMarkdownRef,
    pub mentions: Vec<ActorOrAgentRef>,
    pub created_at: ServerInstant,
    pub supersedes_note_id: Option<OperatorNoteId>,
}
```

- Note 不包含 executable payload，也不触发状态 mutation；
- 编辑以新 Note supersede 表达，旧正文仍留审计；
- `@mention` 经权限、rate limit 和 dedup 后产生 `NudgeRequested` RunSignal；
- 回答开放问题必须使用 `AnswerQuestion(question_id, answer_artifact_digest, expected_version)`；
- 新 scope/interface/hard AC 必须使用 `ProposeRevision`/`SubmitPlanPatch`；
- 附件先做类型、大小、恶意内容与 ACL 检查，再以内容摘要引用，不能直接拼接进 Agent Prompt。

## 5. 动作预览与 action digest

### 5.1 ActionPreview

危险或授权类命令分两阶段：

```text
POST ...:preview  -> ActionPreview（短期、不可执行）
POST ...:confirm  -> 服务端重算并执行/开 Case
```

`ActionPreview` 至少包含：

- typed action 与人类可读 diff；
- 当前/请求后 capability、预算、路由或状态差异；
- 受影响的 Package revision、Attempt/generation、Run/claim、Executor、Git ref；
- 将继续、drain、取消、quarantine、salvage 或重新验收的对象；
- 预计预算变化和不可逆外部副作用；
- 当前 PolicyRevision、命中规则、所需 quorum；
- expiry、timeout default、rollback/fallback；
- `resource_snapshot_digest`、`action_digest`、`preview_expires_at`。

Preview 不是授权 token，不能被 service actor 直接执行。

### 5.2 Canonical digest

```text
action_digest = SHA-256(JCS({
  schema_version,
  tenant_id,
  project_id,
  case_kind,
  subject: { type, id, expected_version },
  package: { id, revision_id, package_hash },
  attempt: { id, author_fencing_generation },
  invocation: { run_id, run_version, run_claim_generation },
  normalized_typed_action,
  capability_delta,
  policy_revision_id,
  resource_snapshot_digest,
  required_quorum,
  expires_at,
  timeout_behavior
}))
```

数值必须是整数最小单位，时间统一 RFC 3339 UTC，集合按规范 key 排序，路径先按仓库规范归一化。禁止把
本地化文案、前端排序、Note 正文或不稳定预测分数放入 digest。

Confirm/execute 时服务端重新读取权威表并计算 digest。以下任一项变化返回 `AF_GOVERNANCE_ACTION_STALE`，
原 Decision 不可迁移：

- subject/package/graph/Attempt/Lease/Run/claim version 或 generation；
- action 参数、能力差异、路径、Git ref、网络域、预算；
- PolicyRevision、quorum 或操作者权限；
- 受影响资源集合/digest；
- expiry 或 timeout behavior。

## 6. PostgreSQL 物理模型基线

实现迁移可以机械调整列名，但不得弱化不可变性、唯一性、fencing 和 digest 约束。

```sql
CREATE TABLE governance_cases (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    kind text NOT NULL,
    risk text NOT NULL CHECK (risk IN ('LOW','MEDIUM','HIGH','CRITICAL')),
    subject_type text NOT NULL,
    subject_id uuid NOT NULL,
    subject_version bigint NOT NULL CHECK (subject_version >= 0),
    requested_by uuid NOT NULL,
    package_binding jsonb,
    attempt_binding jsonb,
    invocation_run_id uuid,
    policy_revision_id uuid NOT NULL,
    normalized_action jsonb NOT NULL,
    action_digest bytea NOT NULL CHECK (octet_length(action_digest)=32),
    resource_snapshot_digest bytea NOT NULL
      CHECK (octet_length(resource_snapshot_digest)=32),
    evidence_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    required_quorum jsonb NOT NULL,
    due_at timestamptz,
    expires_at timestamptz NOT NULL,
    timeout_behavior text NOT NULL,
    state text NOT NULL CHECK (state IN (
      'NEEDS_DECISION','QUORUM_REACHED','EXECUTING','RECONCILING','APPLIED',
      'DENIED','CHANGES_REQUESTED','DEFERRED','EXPIRED','CANCELLED','SUPERSEDED')),
    execution_receipt_id uuid,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    event_seq bigint NOT NULL DEFAULT 0 CHECK (event_seq >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    terminalized_at timestamptz,
    CHECK (expires_at > created_at),
    CHECK ((state IN (
      'APPLIED','DENIED','CHANGES_REQUESTED','EXPIRED','CANCELLED','SUPERSEDED'))
      = (terminalized_at IS NOT NULL))
);

CREATE INDEX governance_cases_inbox_idx
  ON governance_cases(project_id, state, due_at, id)
  WHERE state IN ('NEEDS_DECISION','QUORUM_REACHED','EXECUTING','RECONCILING','DEFERRED');

CREATE TABLE governance_decisions (
    id uuid PRIMARY KEY,
    case_id uuid NOT NULL REFERENCES governance_cases(id),
    case_version bigint NOT NULL,
    actor_id uuid NOT NULL,
    actor_role_snapshot_digest bytea NOT NULL
      CHECK (octet_length(actor_role_snapshot_digest)=32),
    conclusion text NOT NULL CHECK (conclusion IN (
      'APPROVE','DENY','REQUEST_CHANGES','DEFER')),
    action_digest bytea NOT NULL CHECK (octet_length(action_digest)=32),
    policy_revision_id uuid NOT NULL,
    rationale_code text NOT NULL,
    note_id uuid,
    idempotency_key text NOT NULL,
    request_digest bytea NOT NULL CHECK (octet_length(request_digest)=32),
    decided_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    signature bytea NOT NULL,
    UNIQUE (case_id, actor_id, idempotency_key)
);

CREATE TRIGGER governance_decisions_are_immutable
BEFORE UPDATE OR DELETE ON governance_decisions
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

CREATE TABLE governance_execution_receipts (
    id uuid PRIMARY KEY,
    case_id uuid NOT NULL REFERENCES governance_cases(id),
    action_digest bytea NOT NULL CHECK (octet_length(action_digest)=32),
    execution_claim_generation bigint NOT NULL CHECK (execution_claim_generation > 0),
    executor_actor_id uuid NOT NULL,
    status text NOT NULL CHECK (status IN (
      'STARTED','SUCCEEDED','FAILED','OUTCOME_UNKNOWN')),
    external_effect_key text,
    effect_digest bytea NOT NULL CHECK (octet_length(effect_digest)=32),
    evidence_refs jsonb NOT NULL DEFAULT '[]'::jsonb,
    started_at timestamptz NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (case_id, execution_claim_generation, effect_digest)
);

CREATE TRIGGER governance_execution_receipts_are_immutable
BEFORE UPDATE OR DELETE ON governance_execution_receipts
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

CREATE TABLE operator_notes (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    subject_type text NOT NULL,
    subject_id uuid NOT NULL,
    author_id uuid NOT NULL,
    body_artifact_uri text NOT NULL,
    body_digest bytea NOT NULL CHECK (octet_length(body_digest)=32),
    mentions jsonb NOT NULL DEFAULT '[]'::jsonb,
    supersedes_note_id uuid REFERENCES operator_notes(id),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (project_id, body_digest, author_id, created_at)
);
```

Case 的 action/binding/digest 字段创建后不可更新；状态/version/receipt 引用由领域命令更新。数据库 trigger
或 repository compare-before-write 必须拒绝 action snapshot 变化。Decision/Receipt/Note 禁止 DELETE；依法
需要删除正文时只加 tombstone/redaction Artifact，仍保留 digest、actor 和审计事实。

## 7. PolicyRevision、模拟与回滚

### 7.1 模型

```rust
pub enum PolicyRevisionState { Draft, Staged, Active, Superseded }

pub struct PolicyRevision {
    pub id: PolicyRevisionId,
    pub scope: PolicyScope,
    pub base_revision_id: Option<PolicyRevisionId>,
    pub canonical_document_digest: Sha256Digest,
    pub schema_version: String,
    pub created_by: ActorId,
    pub reason_code: String,
    pub state: PolicyRevisionState,
}
```

策略类别至少区分：routing、budget、governance/quorum、security/capability、retention。一个 scope/category
同时只有一个 ACTIVE revision；activation 使用 current revision/version CAS。

### 7.2 激活流程

```text
Draft revision
  -> Schema + semantic lint
  -> deterministic simulation against immutable snapshot
  -> impact report + digest
  -> GovernanceCase（按风险）
  -> CAS activate
  -> PolicyActivated event + RunSignal/Obligation
```

simulation 必须保存输入 snapshot digest、引擎/规则版本、seed、输出 digest、允许/拒绝/路由/预算变化和
受影响对象列表。相同 revision + snapshot + engine + seed 输出必须字节一致。

激活不得原地改写运行中的 InvocationRun、Attempt 或 Lease。handler 根据新策略创建 Signal/Case，显式
选择 continue、drain、cancel、reassign、reverify 或新 Attempt。

回滚通过新的 `PolicyActivated(previous_revision, reason, expected_current_version)` 事件重新激活已知 revision，
保留完整激活历史、影响报告和后继检查；不得 DELETE 或覆盖“错误”revision。

M1 只实现 current PolicyRevision binding 和 Case 校验；完整 simulation/activation/rollback 属于 WP-M5-010。

## 8. Read model 契约

### 8.1 通用要求

所有 projection：

- 可从 Event Ledger + 规范关系表清空重建；
- 保存 `projection_version`、`last_event_id`、`last_event_sequence`、`source_digest`、`rebuilt_at`；
- handler 按 event ID 幂等，重复、乱序和断线不能改变最终 digest；
- 只保存 UI 所需的裁剪字段，不复制 token、Secret、Prompt、CoT 或 Capsule 正文；
- 可延迟但必须展示 `as_of`、`staleness_ms` 和 `degraded_reason`；
- 不被 command handler 用作授权或 CAS 依据；
- tenant/project ACL 在查询层和字段 materialization 两层执行；
- 使用稳定游标 `(sort_key, id, projection_version)`，不使用 offset 做无限列表。

### 8.2 必需 projection

| Projection | 主键/粒度 | 必需字段 | 主要消费者 |
| --- | --- | --- | --- |
| `project_control_room_projection` | Project | status、Ready/Active/Blocked、decision/risk counts、budget、critical path、as_of | 首页/项目总览 |
| `work_graph_projection` | Package revision | dependency state、blocker reason、Attempt、Candidate/Integration、criticality | DAG/列表 |
| `invocation_run_projection` | InvocationRun | Intent/Signal reason、Attempt、Run/claim state、adapter、capsule digest、usage、timeline | Run drawer |
| `governance_inbox_projection` | GovernanceCase | risk、reason、action summary、evidence、quorum、due/expiry、assignee、state | Decision Desk |
| `agent_fleet_projection` | Node/Executor fingerprint | session health、capacity、drain/quarantine、current Run/Lease、qualification | Fleet |
| `budget_rollup_projection` | Project/Package/category | limit/reserved/spent/forecast、reserve floor、risk threshold | Budget |
| `lineage_projection` | Candidate | revision/Attempt/generation/Candidate/Tested/Reviewed/Submitted/Integration OID | Evidence lineage |
| `activity_projection` | Project timeline item | actor、typed action/event、subject、result、safe summary、correlation | Activity |

`RUNNING` 只能在权威关系证明 InvocationRun 非终态且有当前 ACTIVE RunClaim 时投影；Worker heartbeat 或
Agent 文本不能单独产生该状态。`BLOCKED` 必须有 typed blocker/WakeCondition。`BUDGET_RISK` 来自明确
PolicyRevision threshold，而不是浏览器本地计算。

### 8.3 Control Room summary JSON

```json
{
  "project_id": "uuid",
  "project_version": 42,
  "as_of": "2026-08-10T03:00:00Z",
  "projection_version": 7,
  "status": "active",
  "attention": {
    "needs_decision": 3,
    "at_risk": 1,
    "blocked": 2,
    "budget_risk": 1
  },
  "execution": {
    "ready_packages": 4,
    "active_attempts": 3,
    "active_invocation_runs": 2
  },
  "budget": {
    "limit_units": 1000,
    "reserved_units": 350,
    "spent_units": 420,
    "protected_reserve_units": 150
  },
  "critical_path": ["PKG-7", "PKG-11"],
  "links": {
    "governance_inbox": "/v1/projects/uuid/governance-cases",
    "work_graph": "/v1/projects/uuid/work-graph"
  }
}
```

计数不得泄漏调用者无权读取的对象；无权对象从分母和汇总中一并排除或按 policy 返回明确 redacted bucket。

## 9. Query API 与实时更新

### 9.1 Query endpoint

```text
GET /v1/projects/{project_id}/control-room
GET /v1/projects/{project_id}/work-graph?view=list|graph&cursor=&limit=
GET /v1/projects/{project_id}/activity?subject=&actor=&cursor=&limit=
GET /v1/projects/{project_id}/lineage?package_id=&candidate_id=&cursor=
GET /v1/projects/{project_id}/invocation-runs?state=&attempt_id=&cursor=&limit=
GET /v1/invocation-runs/{run_id}/timeline
GET /v1/projects/{project_id}/governance-cases?state=&risk=&assignee=&cursor=&limit=
GET /v1/governance-cases/{case_id}
GET /v1/governance-cases/{case_id}/action-preview
GET /v1/projects/{project_id}/fleet?state=&capability=&cursor=&limit=
GET /v1/projects/{project_id}/budgets?scope=&category=&cursor=&limit=
GET /v1/projects/{project_id}/projection-status
```

每个集合 response 返回 `items`、`next_cursor`、`as_of`、`projection_version`。默认 `limit=50`，最大 200；
稳定排序必须以唯一 ID 收尾。单项/summary 支持 `ETag` 和 `If-None-Match`。Evidence、Capsule 与 Artifact
下载另走短期 scope-bound capability，列表不内嵌大正文。

### 9.2 Sparse SSE invalidation

```text
GET /v1/projects/{project_id}/control-room-stream?cursor=

event: projection.invalidated
id: signed-cursor
data: {"resource":"governance_case","id":"uuid","version":9}
```

- SSE 只发送 resource/id/version、server time 和 cursor，不发送完整事件、token delta、Prompt 或 Evidence；
- 浏览器收到 invalidation 后按 ETag 拉对应 projection；重复/乱序通知无副作用；
- cursor 由服务端签名并绑定 actor/tenant/project，篡改返回 `AF_CURSOR_INVALID`；
- 超出热窗口返回 `409 AF_CURSOR_EXPIRED`，客户端重新拉 snapshot 后续订；
- SSE 断开不影响任何命令或领域状态；退避使用有上限 jitter；
- 低带宽模式默认只订 Decision/critical risk，每 30 秒最多一次合并刷新，不做每秒轮询；
- Node heartbeat、Agent token 和 Worker Journal Turn 不直接映射成 SSE UI 流。

## 10. Typed command API

所有 endpoint 要求认证、授权、`Idempotency-Key`、`If-Match`、`X-Correlation-Id` 和 CSRF/Origin 防护。
请求体禁止客户端提交 tenant/project/actor 的权威值；服务端从 route 和身份获取并重算。

### 10.1 决策与问题

```text
POST /v1/governance-cases/{id}:preview-decision
POST /v1/governance-cases/{id}:decide
POST /v1/questions/{id}:answer
POST /v1/subjects/{type}/{id}:request-nudge
POST /v1/work-packages/{id}/revisions:propose
POST /v1/projects/{id}/plan-patches:submit
POST /v1/subjects/{type}/{id}/notes
```

Decision 请求：

```json
{
  "conclusion": "approve",
  "action_digest": "sha256:...",
  "preview_id": "uuid",
  "rationale_code": "evidence_sufficient",
  "note_id": "uuid"
}
```

`AnswerQuestion` 只满足请求中的 `question_id/case_id` 对应
`WakeCondition::PermissionDecided` 或 `WakeCondition::QuestionAnswered`；不能广播唤醒其他 Attempt。

### 10.2 安全运行控制

```text
POST /v1/projects/{id}:preview-pause-dispatch
POST /v1/projects/{id}:pause-dispatch
POST /v1/projects/{id}:resume-dispatch
POST /v1/invocation-runs/{id}:preview-cancel
POST /v1/invocation-runs/{id}:cancel
POST /v1/leases/{id}:preview-revoke
POST /v1/leases/{id}:revoke
POST /v1/executors/{id}:preview-drain
POST /v1/executors/{id}:drain
POST /v1/nodes/{id}:preview-quarantine
POST /v1/nodes/{id}:quarantine
```

语义必须分开：

| 命令 | 影响新工作 | 影响已有 Run | 影响作者正式 mutation | 影响已封存 Candidate |
| --- | --- | --- | --- | --- |
| PauseProjectDispatch | 停止新 Intent dispatch | 默认不取消 | 不撤销 Lease | 无 |
| DrainExecutor | 停止新分配 | 按 mode 完成或迁移 | 仅在另发 revoke 后失效 | 无 |
| CancelInvocationRun | 无 | 取消指定 Run/RunClaim并对账 | 不自动撤销 Lease | 无 |
| RevokeAuthorLease | 可触发 reoffer | Run 可只读收尾 | generation/fencing 立即失效 | 不改写；可触发重验/隔离 |
| QuarantineNode | 停止新分配 | 取消/迁移并吊销 capability | 对关联 Lease 发显式 revoke | 不改写；未集成结果触发重验 |

UI 不提供通用 `StopAgent`、`PATCH status` 或直接数据库管理入口。

### 10.3 Policy 命令（M5）

```text
POST /v1/policies/{scope}/revisions
POST /v1/policy-revisions/{id}:lint
POST /v1/policy-revisions/{id}:simulate
POST /v1/policy-revisions/{id}:preview-activate
POST /v1/policy-revisions/{id}:activate
POST /v1/policies/{scope}:preview-rollback
POST /v1/policies/{scope}:rollback
```

activation/rollback 使用 scope current version `If-Match`。Policy document 上传后内容寻址；后续命令只引用
revision/digest，不能在 activate 请求中夹带修改后的 policy。

## 11. 信息架构与关键交互

### 11.1 全局导航

```text
Decision Desk
Projects
  └─ Control Room
       ├─ Work
       ├─ Runs
       ├─ Decisions
       ├─ Budget
       ├─ Fleet
       ├─ Lineage
       └─ Activity
Policies（M5）
Audit
```

默认落地页为 Decision Desk；没有待决 Case 时显示风险和下一里程碑，而非空白 Dashboard。

### 11.2 桌面布局

Control Room 使用三层信息密度，而不是一次渲染整个项目：

1. 顶部状态条：projection `as_of`、项目 dispatch 状态、needs decision/at risk/blocked/budget risk；
2. 左/中主列表：按当前 tab 显示 Work、Run 或 Case，支持服务端筛选和游标分页；
3. 右侧详情 drawer：binding、证据、timeline、action preview 和 typed command。

WorkGraph 默认先提供可排序列表与 blocker/critical path；仅在节点数量阈值内显示 DAG。超过阈值按
subgraph/package group 延迟加载，浏览器不得一次读取全量事件或 Evidence。

### 11.3 Decision Card

首屏卡片必须显示：

- risk、Case kind、项目/subject、due/expiry；
- “为何现在出现”的因果摘要和来源事件；
- 精确 proposed action；
- 当前状态与 action 后差异；
- Policy rule/quorum、已有 Decision；
- 关键 Evidence 状态与 lineage；
- 不处理、拒绝、超时和请求修改的结果；
- server-derived stale/degraded 标记。

点击 Approve/Deny 先打开 Preview。Confirm 前强制重新拉 digest；critical 动作要求显式输入/选择稳定
rationale code，不能把按钮文案当确认。批处理只允许 `kind + action schema + risk + policy revision +
timeout behavior` 全部相同的 Case，并由后端逐项重算；一个 stale 不得让其余项绕过独立验证。

### 11.4 Run 与 lineage 表达

UI 必须并列而非混写：

```text
PackageRevision -> Attempt -> author Lease/generation
                            -> InvocationRun #1 -> Capsule
                            -> InvocationRun #2 -> Capsule -> Candidate
Candidate -> VerificationRun -> Review -> Submission -> Integration
```

`InvocationRun Completed` 不能显示成 `Task Done`；只有 Candidate/Verification/Submission/Integration 的
对应领域状态才显示验收或集成完成。任何 OID 不一致用高显著性 lineage break 展示，不能用绿色状态掩盖。

### 11.5 窄屏与离线

- 320 CSS px 宽度先显示 Decision、risk、due 和主动作；Evidence/impact 使用可展开区；
- 窄屏不渲染大 DAG，改为 blocker/critical path 列表；
- Approve/Deny/安全暂停可在窄屏完成，但危险动作仍必须 Preview/Confirm；
- 离线只允许读取明确标注 `as_of` 的已缓存裁剪投影；所有 mutation 按钮禁用；
- 恢复联网后先刷新 Case/version/digest，不自动回放离线批准；
- 不把 Decision、Evidence 正文或审批 capability 永久存入 localStorage/IndexedDB。

## 12. 状态文案与解释规则

UI 标签是 projection，不增加领域状态：

| UI 标签 | 服务端推导条件 | 不允许的替代依据 |
| --- | --- | --- |
| `NEEDS_DECISION` | Case=`NeedsDecision/Deferred` 且当前 actor 可决策 | 未读评论数 |
| `RUNNING` | Run 非终态且当前 ACTIVE RunClaim 未过期 | Node heartbeat、Agent 自报“working” |
| `WAITING_FOR_PERMISSION` | Attempt=`WaitingInput` 且 wake kind=`PermissionDecided` | 新增 Attempt 状态 |
| `BLOCKED` | typed blocker/wake condition 或依赖未满足 | 无模型 token 输出 |
| `AT_RISK` | policy threshold 命中并带 rule ID | 前端本地猜测 |
| `BUDGET_RISK` | authoritative budget rollup 命中阈值 | Bid/模型估算文本 |
| `DEGRADED` | projection lag、Adapter query/依赖健康异常 | 单次 SSE 断线 |

路由解释先展示硬过滤拒绝码，再展示入选 snapshot、Pareto/score 和预算影响；绝不只显示一个不透明
“AI confidence”。治理 UI 不能用组织图或 Agent 头像代替 capability/fingerprint/qualification 证据。

## 13. 前端安全、隐私与可访问性

### 13.1 安全与隐私

- 使用安全、HttpOnly、SameSite Cookie 或平台身份，不把 bearer/审批 capability 写入 Web Storage；
- 所有 mutation 校验 CSRF token、Origin、RBAC、project scope、idempotency、CAS 和 action digest；
- Markdown 使用 allowlist AST 渲染；HTML 默认转义；SVG 作为不可信 Artifact 隔离；链接限制协议/域；
- CSP 默认 `default-src 'self'`，禁止任意 inline script、远端字体和第三方分析脚本；
- restricted Evidence 下载使用单对象、短时、一次性或低 TTL capability，禁止 index/list bucket；
- 错误/telemetry 不上传 Note/Evidence/Prompt 正文；仅记录稳定 code、digest 和 correlation；
- 管理员 impersonation 显示持续 banner，所有读取和命令记录真实 actor/impersonated actor；
- 前端日志、DOM snapshot、错误报告和 projection fixture 做 Secret/token/Prompt/CoT 扫描。

### 13.2 WCAG 2.2 AA hard requirements

- 所有功能仅键盘可达，焦点顺序与视觉顺序一致，drawer/modal 有正确 focus trap/restore；
- 每个 icon-only action 有可访问名称；状态不只靠颜色，风险同时有文本/图标；
- 正文/控件对比达到 AA，200% zoom 和 320 px reflow 无水平关键路径滚动；
- SSE 更新使用非打断式 live region；Decision 到期不以不断跳动倒计时抢焦点；
- 尊重 `prefers-reduced-motion`，禁用非必要动画；
- 表格有 caption/header，DAG 提供等价可访问列表；
- 错误关联到字段并在 summary 中可导航；危险确认不会因 Enter 重复提交；
- 自动 axe 类检查之外，必须人工完成键盘、屏幕阅读器、缩放和窄屏关键路径。

## 14. 稳定错误码

| Code | HTTP | 含义 |
| --- | ---: | --- |
| `AF_GOVERNANCE_CASE_STALE` | 409 | Case expected version 已变化 |
| `AF_GOVERNANCE_ACTION_STALE` | 409 | action/resource/policy digest 与当前事实不一致 |
| `AF_GOVERNANCE_DECISION_EXPIRED` | 409 | Case/Decision/preview 已过期 |
| `AF_GOVERNANCE_QUORUM_UNMET` | 409 | 执行前审批数量、角色或独立性不足 |
| `AF_GOVERNANCE_SELF_APPROVAL_FORBIDDEN` | 403 | requester/author 不得作为 critical 唯一审批者 |
| `AF_GOVERNANCE_EXECUTION_CLAIM_STALE` | 409 | execution claim generation 不再当前 |
| `AF_GOVERNANCE_OUTCOME_UNKNOWN` | 409 | 必须先对账，不能盲重试副作用 |
| `AF_POLICY_REVISION_STALE` | 409 | scope current PolicyRevision/version 已改变 |
| `AF_PROJECTION_REBUILDING` | 503 | 投影不可安全服务，返回 retry-after/as_of |
| `AF_CURSOR_INVALID` | 400 | cursor 签名/scope 不合法 |
| `AF_CURSOR_EXPIRED` | 409 | cursor 超出热窗口，必须重拉 snapshot |
| `AF_NOTE_NOT_EXECUTABLE` | 422 | 客户端试图把 Note/附件当 typed action |

错误 details 只包含安全的 subject/digest/version 提示，不回显 token、Note/Evidence 正文或未授权对象。

## 15. 领域事件与审计

M1 最小事件集：

```text
GovernanceCaseOpened
GovernanceCaseSuperseded
GovernanceDecisionRecorded
GovernanceQuorumReached
GovernanceActionExecutionStarted
GovernanceActionExecutionReconciliationStarted
GovernanceActionApplied
GovernanceActionFailed
GovernanceCaseDenied
GovernanceChangesRequested
GovernanceCaseDeferred
GovernanceCaseExpired
OperatorNoteRecorded
OperatorNoteSuperseded
QuestionAnswered
ProjectDispatchPaused
ProjectDispatchResumed
ExecutorDrainRequested
NodeQuarantined
AuthorLeaseRevoked
ProjectionInvalidated
```

M5 增加 `PolicyRevisionCreated/Simulated/Staged/Activated/RolledBack`。每个事件包含 tenant/project、actor、
subject、causation/correlation、schema version、request/effect digest 和安全摘要；不包含 Decision capability、
Secret、完整 Note/Prompt/CoT。批量命令每个 Case 都有独立事件/receipt/correlation child。

## 16. 可观测性与资源限制

指标至少包括：

```text
governance_cases_open{kind,risk}
governance_decision_latency_seconds{kind,risk}
governance_cases_expired_total{kind,timeout_behavior}
governance_action_stale_total{reason}
governance_execution_reconciling_total{kind}
projection_lag_seconds{projection}
projection_rebuild_duration_seconds{projection}
projection_replay_errors_total{projection,event_schema}
control_room_sse_connections
control_room_sse_invalidation_total{resource}
control_room_query_latency_seconds{endpoint}
```

2C4G MVP 限制：

- projection 采用批量 checkpoint，单 handler 每事务事件上限可配置；
- WorkGraph 默认不返回超过 500 个节点的全图，使用 group/subgraph 分页；
- activity/Run timeline 必须游标分页，不做无界 JSON aggregate；
- 每 actor/project SSE 连接有上限，共享 invalidation fan-out，不为每浏览器保留完整事件队列；
- rebuild 在影子表执行，完成 digest 检查后原子切换；失败继续服务旧投影并标记 degraded；
- Decision、安全终止和 Lease revoke 队列优先于普通 Note/Activity projection。

## 17. 验证矩阵

### 17.1 领域、并发与恢复

1. action 参数、Package revision/hash、Attempt/Lease generation、Run version/claim、PolicyRevision、resource
   snapshot 任一变化后，旧批准执行返回 `AF_GOVERNANCE_ACTION_STALE`；
2. 同 actor/idempotency key/payload 重放 100 次只产生一条 Decision；同 key 不同 payload 稳定失败；
3. 32 个并发 approver 对同一单人 quorum Case，只有符合 policy 的一个终结路径，Decision 仍全部 append-only；
4. critical Case 中 requester、author、唯一 approver 任一重合都不能达到 quorum；
5. `APPROVE` 写入后、外部执行前 crash，Outbox 恢复只执行一次；外部 ACK 丢失先 query/reconcile；
6. receipt action digest 或 execution claim generation 不匹配时不能 `APPLIED`；
7. permission Case 只唤醒绑定 question/case 的 WaitingInput Attempt，其他 Attempt event 数为零；
8. Note、附件和 `@mention` fuzz 不能修改 PackageRevision、能力、预算、Lease 或 Candidate；
9. CancelInvocationRun、RevokeAuthorLease、QuarantineNode 分别验证独立语义和 fencing；旧作者迟到 Candidate
   只能拒绝或 salvage/quarantine；
10. Decision 或 Case response 丢失重试仍返回首次 receipt，不产生第二个副作用。

### 17.2 Projection 与 API

1. 以固定 seed 写入 100,000 个事件；在线投影与从空表 replay 的每个 projection digest 完全一致；
2. 重复、乱序、handler crash 和 projection checkpoint 丢响应不产生重复计数或丢失 lineage；
3. 清空任一 projection 不影响 Lease、RunClaim、Candidate、Submission、Decision 或 Policy 授权；
4. cursor 37 断线后重连，snapshot + invalidation 最终无缺口；重复 invalidation 不重复执行命令；
5. cursor 被篡改、跨 actor/project 复用或过期时返回对应稳定错误；
6. 分页遍历期间有新事件时不重复/跳过同一 snapshot 的 item，响应包含明确 `as_of`；
7. 100 个并发 ETag read 对未变资源返回 304，不触发 aggregate write；
8. tenant/project/field ACL 矩阵覆盖列表、计数、搜索、SSE 和错误 details，不泄漏红acted object existence；
9. projection/JSON/log/trace/fixture Secret、token、Prompt、CoT scanner 为零发现；
10. 命令 handler 测试替换/删除 projection 后仍只从权威表得到相同授权结果。

### 17.3 UI E2E

Playwright 或等价浏览器测试至少覆盖：

1. 桌面从 Decision Desk 打开 Case、检查 Evidence/impact、Preview、Approve，等待 receipt 后显示 `APPLIED`；
2. action 在 Preview 后变 stale，Confirm 显示结构化 diff 并强制重开，不暗中重新批准；
3. Approve 成功但执行失败/unknown 时显示 `EXECUTING/RECONCILING`，绝不显示 Applied；
4. Request Changes 创建 revision/PlanPatch proposal，原 PackageRevision digest 不变；
5. 普通评论和 `@mention` 只产生 Note/Signal，UI 无“保存后修改 AFWP”的路径；
6. Cancel Run 与 Revoke Lease 的 Preview 影响列表不同，不能由同一个 Stop 按钮代替；
7. SSE 在 cursor 37 断开、重复和乱序后页面与重新载入 snapshot 一致；
8. 离线时 mutation 禁用，联网后不自动提交缓存 Decision；
9. 320 px、200% zoom、键盘-only、screen reader label、focus restore 和 reduced-motion 关键路径通过；
10. Markdown/XSS/SVG/link/path、CSRF、跨租户 IDOR、批量 stale Case、安全 Cookie 与 CSP fixture 全通过。

## 18. Work Package 映射与完成定义

| Work Package | 本规范责任 |
| --- | --- |
| `WP-M1-010` | GovernanceCase、Decision、action digest、ExecutionReceipt、permission wake 基础 |
| `WP-M1-011` | 八类 read model、Query API、ETag/cursor、projection replay、稀疏 SSE |
| `WP-M1-012` | Thin Control Room、Decision Desk、Run/lineage、受限 typed command、桌面/窄屏/a11y |
| `WP-M5-010` | PolicyRevision lint/simulation/CAS activation/rollback |
| `WP-M5-011` | 完整 Decision Desk、quorum、批处理、路由/预算/策略解释和跨项目治理 |

本规范完成必须同时满足：

- ADR-0007 与本文件的术语、状态、API 和错误码无冲突；
- AFWP、Attempt、作者 Lease、InvocationRun、Candidate、VerificationRun、Submission、Integration 仍为独立对象；
- 权限等待未增加第五套 Attempt 状态；
- 所有 UI mutation 都能追踪到 typed command、actor、action digest、Decision/receipt 和 domain event；
- projection 从空表重建、SSE 恢复、ACL、Secret scanning、桌面/窄屏和 WCAG hard AC 全通过；
- 未参与实现的独立 Reviewer 从干净环境复现领域、API 和 UI E2E 证据。
