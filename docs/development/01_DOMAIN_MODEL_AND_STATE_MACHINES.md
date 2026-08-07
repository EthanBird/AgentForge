# AgentForge 领域模型与状态机实现规范

> 文档状态：可实现草案
>
> 目标版本：MVP / `afwp/1.0`
>
> 适用组件：`agentforge-domain`、`agentforge-application`、`agentforge-storage-postgres`
>
> 规范关键词：`必须`、`不得`、`应当`具有约束力；`可以`表示可选实现。

## 1. 目的与设计边界

本文把总设计中的概念收敛为可以直接编码、迁移和测试的领域模型。MVP 必须把以下对象分开持久化：

| 聚合 | 回答的问题 | 不负责的问题 |
| --- | --- | --- |
| `WorkPackage` | 这个固定任务版本目前处于哪个业务阶段？ | 当前由谁执行、某次执行走到哪一步 |
| `Attempt` | 某个 Worker 对固定任务版本的一次执行走到哪一步？ | 执行权是否仍有效 |
| `Lease` | 哪个 Attempt 在什么期限、什么代次内拥有写入权？ | 代码是否正确、任务是否验收通过 |
| `CandidateArtifact` | 哪个预留 Candidate ID 对应一份在有效 Lease 下完成校验的 Git Bundle/Author Evidence？ | 独立验收结论或 Git 分支状态 |
| `Candidate` | 作者在有效 Lease 下封存了哪个不可变 Commit？ | 独立验收是否通过 |
| `VerificationRun` | 某个 Candidate 的来源、复现、Review 和逐项 AC 目前执行到哪一步？ | 用可变运行记录冒充最终证明 |
| `Submission` | 哪份签名 Manifest 固化了某个 VerificationRun 的终态结论？ | 主分支是否已经合并 |
| `Integration` | Candidate 与哪个目标基线合成、在哪个 IntegrationHead 上通过 L5 并合并？ | 改写 Candidate/Submission |
| `Obligation` | 哪项监督责任在何时、由谁、以什么证据履行？ | 用 LLM 会话充当定时器 |

MVP 的一致性边界是 PostgreSQL 单库事务。领域层不依赖 Axum、SQLx、NATS、Git 或 jcode；它只接收命令和当前事实，产生新状态与领域事件。

## 2. 统一类型、时间和版本规则

### 2.1 强类型标识符

所有数据库主键、聚合 ID 与控制 API 的不透明资源 ID 使用 UUID v7；不得在领域 API 中裸传 `String`。UUID v7 便于按产生时间排序，但排序只用于运维，不构成因果顺序。

AFWP/Submission 文档为了可移植和便于人工审计，保留 Schema 规定的稳定协议键（如 `WP-M0-001`、`wp-lease-fencing-001`、`att-*`、`sub-*`）。这些字段虽然因兼容性仍命名为 `*_id`，在实现中必须解析为 `ProtocolKey`，并通过唯一映射解析到内部 UUID；不得把自由文本直接当数据库主键，也不得把协议键与内部 ID 互换比较。

```rust
use std::num::{NonZeroU32, NonZeroU64};
use time::OffsetDateTime;
use uuid::Uuid;

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub Uuid);
    };
}

id_type!(ProjectId);
id_type!(PackageId);
id_type!(PackageRevisionId);
id_type!(AttemptId);
id_type!(LeaseId);
id_type!(CandidateArtifactId);
id_type!(CandidateId);
id_type!(VerificationRunId);
id_type!(VerificationStageResultId);
id_type!(SubmissionId);
id_type!(IntegrationId);
id_type!(RelayTicketId);
id_type!(ObligationId);
id_type!(ActorId);
id_type!(EventId);

#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ProtocolKey(String); // 构造器按对应 JSON Schema pattern 校验

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize)]
pub struct PackageRevision(pub NonZeroU32);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize)]
pub struct AggregateVersion(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, serde::Serialize, serde::Deserialize)]
pub struct FencingToken(pub NonZeroU64);

impl FencingToken {
    pub fn get(self) -> u64 { self.0.get() }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServerInstant(pub OffsetDateTime);
```

约束如下：

- 内部 UUID v7 一律由控制平面生成，客户端不能指定或建议；AFWP 创建时由客户端提供 `project_id/package.id` 协议键，分别在租户内/项目内唯一且创建后不可变；Attempt、Lease、Submission、Relay Ticket 的协议键由服务端生成，Candidate、VerificationRun 和 Integration 只对外暴露服务端 UUID 资源 ID；
- `PackageRevision` 对同一 `PackageId` 从 1 严格递增；
- `AggregateVersion` 每成功执行一个领域命令加 1，即使命令产生多个领域事件也只增加一次；
- `aggregate_seq` 对单个聚合内每个事件加 1；
- `FencingToken` 对同一 `PackageRevisionId` 每次成功授予 Lease 严格递增，永不回退、永不复用；
- 所有授权、到期和调度判断只使用数据库服务器时间；Worker 时间只可进入展示字段。

### 2.2 内容寻址值

```rust
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Sha256Digest([u8; 32]);

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct GitObjectId(String); // MVP 允许 SHA-1 或 SHA-256，写入时同时记录 object_format

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ArtifactRef {
    pub artifact_id: String,
    pub uri: String,
    pub digest: Sha256Digest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PackageSnapshot {
    pub package_hash: Sha256Digest,
    pub base_commit: GitObjectId,
    pub toolchain_lock_hash: Sha256Digest,
    pub input_artifacts: Vec<ArtifactRef>,
}
```

`package_hash` 必须由规范化后的 AFWP JSON 计算：UTF-8、对象键按字典序、无无意义空白、数字禁止 NaN/Infinity。数据库保存原始文档、规范化文档与摘要；校验摘要时不得对 YAML 文本直接哈希。

### 2.3 领域 crate 边界

```text
agentforge-domain
  ids.rs              强类型 ID、摘要、版本
  package.rs          WorkPackage 聚合和状态机
  attempt.rs          Attempt 聚合和状态机
  lease.rs            Lease 聚合和 fencing 规则
  candidate.rs        CandidateArtifact/Candidate 不可变来源链
  verification.rs     VerificationRun 与不可变阶段/审查/复现事实
  submission.rs       Submission 聚合和验收结论
  integration.rs      Integration、Relay Ticket/claim 与 L5 规则
  obligation.rs       Obligation 聚合和升级规则
  workgraph.rs        边、就绪判定、PlanPatch 纯函数
  command.rs          领域命令值对象
  event.rs            领域事件值对象
  error.rs            稳定领域错误

agentforge-application
  handlers/           一个命令一个处理器
  ports/              Repository、Clock、Outbox、Authorizer trait
  dto/                API 与领域之间的显式转换
```

依赖方向必须为：

```text
HTTP / Jobs -> Application -> Domain
PostgreSQL / Git / Object Store -> Application ports
```

`agentforge-domain` 不得出现 `sqlx::*`、HTTP 状态码、数据库表名或外部客户端；数据库枚举与领域枚举之间由 storage adapter 做穷尽映射。

## 3. WorkPackage 聚合

### 3.1 数据结构

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkPackageState {
    Draft,
    Validating,
    Blocked,
    Offered,
    Active,
    Verifying,
    ReworkReady,
    Accepted,
    Integrating,
    RebaseRequired,
    Integrated,
    Closed,
    Cancelled,
    Superseded,
    Failed,
}

pub struct WorkPackage {
    pub id: PackageId,
    pub project_id: ProjectId,
    pub selected_revision_id: PackageRevisionId,
    pub state: WorkPackageState,
    pub graph_version: u64,
    pub priority: i16,
    pub max_attempts: u16,
    pub attempts_started: u16,
    pub next_fencing_token: u64,
    pub active_attempt_id: Option<AttemptId>,
    pub accepted_submission_id: Option<SubmissionId>,
    pub integrated_integration_id: Option<IntegrationId>,
    pub integrated_commit: Option<GitObjectId>,
    pub version: AggregateVersion,
}
```

内容字段不存入聚合状态行，而存入不可变 `PackageRevision`：

```rust
pub struct PackageRevisionRecord {
    pub id: PackageRevisionId,
    pub package_id: PackageId,
    pub revision: PackageRevision,
    pub schema_version: String,
    pub canonical_document: serde_json::Value,
    pub package_hash: Sha256Digest,
    pub snapshot: PackageSnapshot,
    pub created_by: ActorId,
    pub created_at: ServerInstant,
}
```

一旦 revision 行写入，其规范化文档、哈希和输入快照不得 `UPDATE`。修正规格必须创建下一 revision，并显式 `Supersede` 旧 revision 对应的运行结果。

### 3.2 状态转移表

表中的“同事务动作”属于命令处理的一部分，不允许依赖异步消费者补齐核心状态。

| 当前状态 | 命令 / 事件 | 必要前置条件 | 下一状态 | 同事务动作 | 失败错误 |
| --- | --- | --- | --- | --- | --- |
| `Draft` | `RequestValidation` | revision 存在且哈希匹配 | `Validating` | 创建 validation obligation | `DomainError::NotFound`、`DomainError::PackageHashMismatch` |
| `Validating` | `ValidationFailed` | validator 与目标 revision 匹配 | `Blocked` | 保存结构化 findings | `DomainError::StaleVersion` |
| `Blocked` | `SelectNewRevisionAndValidate` | 新 revision 编号更大 | `Validating` | 切换 selected revision，保留旧 findings | `DomainError::InvalidArgument` |
| `Validating` | `PublishValidatedPackage` | DoR 全通过、DAG/预算/权限有效 | `Offered` | 写 `PackagePublished`、offer、outbox | `DomainError::PackageNotReady` |
| `Offered` | `GrantLease` | 无有效 Lease、依赖满足、预算足够 | `Active` | 增 token、建 Attempt 与 Lease | `DomainError::PackageNotClaimable` |
| `Active` | `RecordCandidate` | 当前 token 有效，候选已封存且来源预检可启动 | `Verifying` | 记录不可变 Candidate、创建 VerificationRun、关闭 Lease | `DomainError::StaleLease` |
| `Active` | `LoseAttempt` | 当前 Attempt 丢失或失败 | `ReworkReady` / `Failed` | 清 active attempt；判断尝试上限 | `DomainError::StaleVersion` |
| `Verifying` | `FinalizeVerification` | VerificationRun 终态为 FAIL/INCONCLUSIVE；`terminal_stage` 与不可变阶段事实匹配；签名 Manifest 已在事务外上传并登记为未过期 staging fact | `ReworkReady` / `Failed` | 同事务绑定 staging、创建终态 Submission、清 current attempt、生成 Failure Dossier 与返工义务 | `DomainError::InvalidTransition` |
| `ReworkReady` | `GrantLease` | 未超过 `max_attempts` | `Active` | 新 Attempt、新 token；旧候选不改写 | `DomainError::AttemptLimitReached` |
| `Verifying` | `FinalizeVerification` | VerificationRun PASS、无高危 finding、Candidate/三 Head 相等；签名 Manifest 已在事务外上传并登记为匹配 staging fact | `Accepted` | 同事务绑定 staging、创建并固定 accepted Submission、清 current attempt；只创建 `RequestIntegrationEnqueue` obligation | `DomainError::SubmissionNotAcceptable` |
| `Accepted` | `EnqueueIntegration` | `accepted_submission_id` 指向 PASS/CandidateReady Submission；其 CandidateArtifact 仍为 COMPLETE，Submission/Candidate/Artifact/VerificationRun/Package/revision lineage 完全一致；尚无当前 Integration | `Integrating` | 原子创建 `Integration(Queued)`、限定该 lineage 的 Relay Ticket 与 `RelayCandidate` obligation | `DomainError::SubmissionNotAcceptable`、`DomainError::InvariantViolation` |
| `Integrating` | `ReportIntegrationConflict` | 针对当前 accepted submission | `RebaseRequired` | 生成 RebasePackage 提案 | `DomainError::StaleVersion` |
| `RebaseRequired` | `BeginReintegration` | 新 rebase candidate 已通过任务级验收 | `Integrating` | 关联新的集成候选 | `DomainError::InvalidTransition` |
| `Integrating` | `ReportIntegrationFailed` | 当前 Integration 尚未终态；签名 failure receipt 有效，失败不是可在同一 queue lease 内安全重试的瞬态错误 | `ReworkReady` / `Failed` | 同事务置 Integration FAILED、保留 Receipt；显式清除当前 accepted 指针；按尝试和失败预算创建返工或升级义务 | `DomainError::StaleVersion`、`DomainError::InvalidTransition` |
| `Integrating` | `MarkIntegrated` | 目标基线复验通过且签名合并成功 | `Integrated` | 写 merge commit 和 rollback point | `DomainError::InvalidTransition` |
| `Integrated` | `ClosePackage` | 追踪矩阵和后继图已更新 | `Closed` | 履行 package completion obligation | `DomainError::InvalidTransition` |

旁路命令：

- `CancelPackage`：`Draft`、`Validating`、`Blocked`、`Offered`、`ReworkReady` 可直接到 `Cancelled`；`Active` 必须先撤销 Lease 并向 Worker 发协作式取消事件；
- `SupersedePackage`：除 `Integrated`、`Closed` 外可到 `Superseded`，必须记录替代 revision/package 和已有制品复用策略；
- `FailPackage`：只允许系统策略或有权限的 Boss 在不可恢复且证据充分时执行；
- `Integrated` 和 `Closed` 不允许取消或覆盖，只能通过新补偿任务修复。

### 3.3 WorkPackage 不变量

1. `active_attempt_id.is_some()` 只允许出现在 `Active` 或 `Verifying`；`Active` 时必须有当前有效 Lease，`Verifying` 时指向已封存 Candidate 的原 Attempt 且作者 Lease 已关闭。MVP 每个 revision 只允许一条当前正式执行/验收链。
2. `accepted_submission_id` 只能在 `Accepted`、`Integrating`、`RebaseRequired`、`Integrated` 或 `Closed` 出现，且不得静默替换。终态 Integration 失败进入 `ReworkReady`/`Failed` 时可以在同事务显式清除该指针，但必须保留 Submission、Integration 与失败事件；后续通过新 Attempt/Candidate/Submission 建立新 lineage。
3. `integrated_integration_id` 与 `integrated_commit` 必须同时出现且只能在 `Integrated`/`Closed` 出现。
4. `attempts_started <= max_attempts`；管理员提高上限必须产生审计事件。
5. 已发布 revision 不可变；运行中的规格变更通过新 revision 和显式 supersede 实现。
6. `Accepted` 不等于 `Integrated`；下游代码包默认只把 `Integrated` 视为 hard dependency 已满足。

## 4. Attempt 聚合

### 4.1 数据结构与等待条件

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptState {
    Created,
    Leased,
    Preparing,
    Planning,
    Implementing,
    LocalVerify,
    WaitingInput,
    Candidate,
    IsolatedReview,
    CleanReproduce,
    Submitted,
    Passed,
    Rejected,
    Lost,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WakeCondition {
    ArtifactAccepted { artifact_id: String, digest: Sha256Digest },
    QuestionAnswered { question_id: String },
    PermissionDecided { request_id: String },
    DependencyIntegrated { package_id: PackageId },
    NotBefore { at: OffsetDateTime },
}

pub struct Attempt {
    pub id: AttemptId,
    pub package_id: PackageId,
    pub revision_id: PackageRevisionId,
    pub executor_id: Uuid,
    pub node_id: Uuid,
    pub state: AttemptState,
    pub lease_id: Option<LeaseId>,
    pub fencing_token: FencingToken,
    pub base_commit: GitObjectId,
    pub wake_condition: Option<WakeCondition>,
    pub semantic_progress_seq: u64,
    pub last_checkpoint_digest: Option<Sha256Digest>,
    pub candidate_commit: Option<GitObjectId>,
    pub version: AggregateVersion,
}
```

`wake_condition` 必须是受版本控制的 tagged union，不得保存成供人阅读但无法求值的自由文本。

### 4.2 状态转移表

| 当前状态 | 命令 | 前置条件 | 下一状态 | 说明 |
| --- | --- | --- | --- | --- |
| `Created` | `AttachLease` | Lease 与 Attempt/package/revision/token 完全匹配 | `Leased` | 与 grant 同一事务完成 |
| `Leased` | `StartPreparation` | token 有效，输入快照可下载 | `Preparing` | 创建工作目录不算语义进度 |
| `Preparing` | `BaselineReady` | base commit、锁文件和输入摘要一致 | `Planning` | 基线失败不得继续编码 |
| `Planning` | `ApproveExecutionPlan` | 计划覆盖全部 MUST/AC，修改范围合法 | `Implementing` | 计划存为 checkpoint artifact |
| `Implementing` | `StartLocalVerification` | 至少有一个候选变更 | `LocalVerify` | 固定本轮 candidate tree |
| `LocalVerify` | `RequestFix` | 存在可修复失败且预算足够 | `Implementing` | 失败摘要进入下一 Turn Pump |
| `LocalVerify` | `RecordCandidate` | 本地 hard checks 全部通过 | `Candidate` | 固定 candidate commit |
| `Candidate` | `StartIsolatedReview` | reviewer 身份与 author 不同 | `IsolatedReview` | Reviewer 使用只读 checkout |
| `IsolatedReview` | `StartCleanReproduce` | 无未解决 Critical/High finding | `CleanReproduce` | 必须 checkout 精确 candidate |
| `CleanReproduce` | `FinalizeSubmission` | VerificationRun 终态且最终 Manifest 已签名；PASS 时 Candidate/三 Head 相等 | `Submitted` | Coordinator 一次性创建终态 Submission；不要求作者 Lease 此刻仍存活 |
| `Candidate`/`IsolatedReview`/`CleanReproduce` | `FinalizeRejectedSubmission` | VerificationRun 已在当前或此前阶段得出 FAIL/INCONCLUSIVE，Failure Dossier 完整 | `Submitted` | 允许 provenance 早期失败；保存真实已完成阶段，禁止伪造未执行的 Head/criterion |
| `Submitted` | `MarkAttemptPassed` | Submission 终态为 PASS | `Passed` | 只由 Verification handler 调用 |
| `Submitted` | `MarkAttemptRejected` | Submission 为 FAIL/INCONCLUSIVE | `Rejected` | INCONCLUSIVE 不能转 Passed |
| 可运行状态 | `WaitFor` | wake condition 合法且已释放模型资源 | `WaitingInput` | Lease 是否保留由包策略决定 |
| `WaitingInput` | `Wake` | 指定条件已经由事实事件满足 | 原先安全状态 | 恢复点必须在 journal 中 |
| 可运行状态 | `MarkLost` | Lease 过期/节点会话失效/撤销 | `Lost` | 旧结果只可 salvage |
| 非终态 | `CancelAttempt` | 包已取消/替代或管理员撤销 | `Cancelled` | 不删除 checkpoint |

MVP 可将“原先安全状态”只允许为 `Planning` 或 `Implementing`，并在进入等待时保存 `resume_state`。禁止从 `WaitingInput` 直接跳到 `Submitted`。

`RecordCandidate` 已关闭作者 Lease 并把作者 Worker 置于 `AuthorComplete`，因此 Review finding 绝不能让同一 Attempt 返回 `Implementing`。Reviewer 必须先把当前 VerificationRun 终结为 FAIL，Coordinator 创建 Failure Dossier/终态 Submission，并让 Package 经 `ReworkReady` 授予一个新 Attempt；新代码只能形成新的 Candidate lineage。

### 4.3 语义进度

续租证明节点仍在线，但不证明任务有推进。`ReportProgress` 只有包含以下至少一项时才增加 `semantic_progress_seq`：

- 新 checkpoint digest；
- 新的可验证 AC 结果；
- candidate tree/commit 变化；
- 新问题或新 blocker；
- 计划中的下一个里程碑发生变化并附证据。

纯心跳、重复日志、token 流、相同摘要不得刷新语义进度截止时间。服务器应对 `(attempt_id, client_progress_id)` 做唯一去重。

### 4.4 Attempt 不变量

1. Attempt 永久绑定一个 `PackageRevisionId`、`base_commit` 和输入制品摘要集合。
2. 一个 Attempt 只能绑定一个 fencing token；重新授予必须创建新 Attempt。
3. candidate commit 一旦进入 `Candidate`，后续任何代码变化必须产生新 commit 并使旧验证结果失效。
4. author executor 不能是唯一 Reviewer；测试 Runner 与 Reviewer 均不得拥有保护分支写权限。
5. `WaitingInput` 必须同时存在 `wake_condition` 与 `resume_state`，其他状态不得保留过期的 wake condition。
6. jcode 的 `turn_done` 只是工具事件，不是任何 Attempt 状态转移的充分条件。

## 5. Lease 聚合与 fencing

### 5.1 状态与结构

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseState { Active, Released, Revoked, Expired }

pub struct Lease {
    pub id: LeaseId,
    pub package_id: PackageId,
    pub revision_id: PackageRevisionId,
    pub attempt_id: AttemptId,
    pub holder_node_id: Uuid,
    pub fencing_token: FencingToken,
    pub state: LeaseState,
    pub granted_at: ServerInstant,
    pub expires_at: ServerInstant,
    pub max_expires_at: ServerInstant,
    pub version: AggregateVersion,
}
```

`Renewed` 是事件，不是稳定状态。续租成功后状态仍为 `Active`，只改变 `expires_at` 与版本。

### 5.2 状态转移

| 当前状态 | 命令 | 判断 | 下一状态 |
| --- | --- | --- | --- |
| 无 | `GrantLease` | package 可认领、无有效 Lease、token 单调增加 | `Active` |
| `Active` | `RenewLease` | token/holder/version 匹配，数据库当前时间早于 `expires_at` 与 `max_expires_at` | `Active` |
| `Active` | `ReleaseLease` | holder 与 token 匹配 | `Released` |
| `Active` | `RevokeLease` | 系统策略/管理员有权，原因非空 | `Revoked` |
| `Active` | `ExpireLease` | 数据库当前时间大于等于 `expires_at` | `Expired` |

终态 Lease 不得复活。Worker 在续租请求到达时已经过期，即使 sweeper 尚未把行改成 `Expired`，续租也必须失败。

幂等只由命令回执定义：相同 `Idempotency-Key` 重放 `ReleaseLease`、`RevokeLease` 或 `ExpireLease` 时原样返回首次结果；换用新 key 再对终态 Lease 发终结命令则返回 `DomainError::InvalidTransition`（HTTP 为 `AF_TRANSITION_INVALID`），不会伪造第二个成功事件。

### 5.3 所有作者侧副作用 API 的统一守卫

```rust
pub struct LeaseProof {
    pub lease_id: LeaseId,
    pub attempt_id: AttemptId,
    pub fencing_token: FencingToken,
    pub expected_lease_version: AggregateVersion,
}

pub fn authorize_attempt_write(
    lease: &Lease,
    proof: &LeaseProof,
    now: ServerInstant,
) -> Result<(), DomainError> {
    if lease.id != proof.lease_id
        || lease.attempt_id != proof.attempt_id
        || lease.fencing_token != proof.fencing_token
        || lease.state != LeaseState::Active
    {
        return Err(DomainError::StaleLease);
    }
    if now.0 >= lease.expires_at.0 {
        return Err(DomainError::LeaseExpired);
    }
    if lease.version != proof.expected_lease_version {
        return Err(DomainError::StaleVersion);
    }
    Ok(())
}
```

`progress`、`checkpoint`、`artifact registration`、`question`、`blocker`、`expansion proposal`、`candidate registration` 与作者侧 Git Bundle 交付都必须调用同一守卫。只在 HTTP 层验证 token 不够；最终 CAS 必须发生在 SQL 事务中。独立验证方创建终态 Submission 时不要求作者 Lease 仍存活，但必须验证 Candidate 中永久保存的 Lease/token 来源，且不得接受 Candidate 之后的新作者写入。

### 5.4 NodeSessionLease 与 TaskLease 分离

节点会话租约只表示 Gateway 会话/节点身份仍有效，Task Lease 表示某任务的写入权。二者使用不同表、不同 token、不同 TTL：

- 节点会话失效可以触发 Task Lease 的加速回收，但不得让任务 Lease 自动续期；
- 节点会话有效不能覆盖已过期 Task Lease；
- 重连后的同一物理节点得到新 NodeSession，不会恢复旧 Task Lease 的权限。

## 6. CandidateArtifact、Candidate 与 VerificationRun 聚合

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CandidateArtifactState {
    Uploading,
    Assembling,
    Complete,
    Rejected,
    Quarantined,
    Expired,
}

pub struct CandidateArtifact {
    pub id: CandidateArtifactId,
    pub reserved_candidate_id: CandidateId,
    pub attempt_id: AttemptId,
    pub lease_id: LeaseId,
    pub fencing_token: FencingToken,
    pub candidate_commit: GitObjectId,
    pub tree_hash: GitObjectId,
    pub author_evidence_digest: Sha256Digest,
    pub bundle: Option<ArtifactRef>,
    pub state: CandidateArtifactState,
    pub version: AggregateVersion,
}

pub struct Candidate {
    pub id: CandidateId,
    pub attempt_id: AttemptId,
    pub package_revision_id: PackageRevisionId,
    pub lease_id: LeaseId,
    pub fencing_token: FencingToken,
    pub base_commit: GitObjectId,
    pub candidate_commit: GitObjectId,
    pub tree_hash: GitObjectId,
    pub branch: String,
    pub author_evidence_digest: Sha256Digest,
    pub bundle_artifact_id: CandidateArtifactId,
    pub bundle: ArtifactRef,
    pub sealed_at: ServerInstant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerificationRunState {
    Queued,
    ProvenanceCheck,
    Reviewing,
    Reproducing,
    Pass,
    Fail,
    Inconclusive,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VerificationStage {
    ProvenanceCheck,
    Reviewing,
    Reproducing,
}

pub struct VerificationRun {
    pub id: VerificationRunId,
    pub candidate_id: CandidateId,
    pub state: VerificationRunState,
    pub terminal_stage: Option<VerificationStage>,
    pub terminal_stage_result_id: Option<VerificationStageResultId>,
    pub tested_head: Option<GitObjectId>,
    pub reviewed_head: Option<GitObjectId>,
    pub evidence_digest: Option<Sha256Digest>,
    pub terminalized_at: Option<ServerInstant>,
    pub version: AggregateVersion,
}
```

作者在有效 Lease 下先调用 Candidate Artifact init，取得服务器预留的 `CandidateId`，再上传 Git Bundle/Author Evidence 并完成内容寻址校验；`RecordCandidate` 只能绑定状态为 Complete、Attempt/Lease/token/OID/tree 全匹配的 Artifact，然后才关闭作者 Lease。未完成、过期或不匹配的上传只能 GC/quarantine，不能供 VerificationRun 使用。

Candidate 在 `RecordCandidate` 事务中以预留 ID 创建后不可修改或删除；它永久保存登记时已通过的 Lease/token 来源和 Bundle Artifact。每个 Candidate 只能有一个正式 VerificationRun。VerificationRun 允许按 CAS 从 `Queued -> ProvenanceCheck -> Reviewing -> Reproducing` 推进，并从任一已开始的检查阶段确定性进入 `Fail`/`Inconclusive`，或从 `Reproducing` 进入 `Pass`；终结时必须固定 `terminal_stage/terminalized_at`，终态 run 不可更新或删除。取消只适用于非终态 run。`Pass` 必须满足 `tested_head = reviewed_head = candidate_commit`。

每个阶段先插入由对应 service identity 签署的不可变聚合事实，再与 run CAS 同事务推进：Provenance 使用 `VerificationStageResult`；Review 使用一个或多个 `ReviewReport` 及其 `ReviewFinding`，再形成 Reviewing 聚合结果；Clean Reproduction 使用 `ReproductionResult` 和 Reproducing 聚合结果；逐项 AC 使用不可变 `CriterionResult`。Reviewer actor 必须不同于 Attempt executor，高风险包还要满足模型家族数量。相同输入的 Verifier Job 瞬态重试发生在同一个非终态 run 内，原始尝试保存在签名 Evidence；一旦 run 已终结并生成 Submission，MVP 不重新打开旧 Attempt/Candidate，任何代码、Runner 或 Review 输入变化都经 `ReworkReady` 创建新 Attempt、Candidate、run 和 lineage。

## 7. Submission 聚合

### 7.1 数据结构

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubmissionState {
    Pass,
    Fail,
    Inconclusive,
    Quarantined,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CriterionOutcome { Pass, Fail, Inconclusive, Skipped }

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompletedStage {
    ProvenanceCheck,
    Reviewing,
    Reproducing,
    CandidateReady,
    SalvageRegistration,
}

pub struct Submission {
    pub id: SubmissionId,
    pub protocol_key: ProtocolKey,
    pub attempt_id: AttemptId,
    pub package_revision_id: PackageRevisionId,
    pub candidate_id: Option<CandidateId>,
    pub candidate_artifact_id: Option<CandidateArtifactId>,
    pub verification_run_id: Option<VerificationRunId>,
    pub submitted_head: Option<GitObjectId>,
    pub tested_head: Option<GitObjectId>,
    pub reviewed_head: Option<GitObjectId>,
    pub manifest_digest: Sha256Digest,
    pub evidence_digest: Option<Sha256Digest>,
    pub lease_fencing_token_hash: Sha256Digest,
    pub state: SubmissionState,
    pub completed_stage: CompletedStage,
    pub failure_dossier: Option<FailureDossier>,
    pub version: AggregateVersion,
}
```

### 7.2 状态转移与判定顺序

| 当前状态 | 动作 | 下一状态 | 强制条件 |
| --- | --- | --- | --- |
| 无 | `FinalizeCandidateSubmission` | `Pass` | 绑定的 VerificationRun 已 PASS；来源、复现、Review、hard criteria、Candidate/三 Head 与签名全部通过 |
| 无 | `FinalizeCandidateSubmission` | `Fail` | 绑定的 VerificationRun 已 FAIL，Manifest 保存全部失败证据 |
| 无 | `FinalizeCandidateSubmission` | `Inconclusive` | 绑定的 VerificationRun 因基础设施/证据/flaky 无法得出结论 |
| 无 | `RegisterSalvage` | `Quarantined` | 独立 salvage API；只含隔离制品与来源，不执行 Verification/Review/Integration |

Submission 创建即终态且不可原地重开。ProvenanceCheck、Reviewing、Reproducing 属于独立 `VerificationRun` 的状态，不属于 Submission。瞬态 Job 只能在 run 终结前按固定输入和预算重试；终态 `Inconclusive` 已结束当前 Attempt，后续通过新 Attempt/Candidate/VerificationRun 和新的 Submission lineage 重做，审计永久保留旧 run/manifest。

### 7.3 `candidate_ready` 的确定性谓词

```text
candidate_ready(s, candidate, run, attempt) =
  s.state = PASS
  AND s.completed_stage = CANDIDATE_READY
  AND s.candidate_id = candidate.id
  AND s.candidate_artifact_id = candidate.bundle_artifact_id
  AND s.verification_run_id = run.id
  AND run.state = PASS
  AND candidate.lease_was_current_at_registration
  AND s.package_revision_id = attempt.revision_id
  AND s.submitted_head = candidate.candidate_commit
  AND s.tested_head = s.reviewed_head
  AND s.reviewed_head = s.submitted_head
  AND every(hard criterion) = PASS
  AND no unresolved finding severity IN {CRITICAL, HIGH}
  AND clean_reproduce = PASS
  AND evidence_signature = VALID
```

`SKIPPED` 对 hard criterion 按 `FAIL` 处理；`INCONCLUSIVE` 永远不按 `PASS` 处理。

### 7.4 隔离结果的 salvage

`Quarantined` Submission 可以登记为 `SalvageBundle`，但必须满足：

- 与正式 Submission 使用不同 API 和权限；
- 不触发 Package 的 `Verifying`、`Accepted` 或 Integration；
- 新 Attempt 必须显式选择要复用的文件/commit，并重新基于其固定 base 验证；
- 来源 Attempt、旧 token 和隔离原因永久保留。

## 8. Integration 聚合（Submission 下游）

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegrationState {
    Queued,
    Synthesizing,
    L5Running,
    Conflict,
    Pass,
    Failed,
    Merged,
    Cancelled,
}

pub struct Integration {
    pub id: IntegrationId,
    pub package_id: PackageId,
    pub submission_id: SubmissionId,
    pub candidate_id: CandidateId,
    pub candidate_artifact_id: CandidateArtifactId,
    pub verification_run_id: VerificationRunId,
    pub current_relay_ticket_id: RelayTicketId,
    pub state: IntegrationState,
    pub target_before: GitObjectId,
    pub integration_head: Option<GitObjectId>,
    pub l5_tested_head: Option<GitObjectId>,
    pub target_after: Option<GitObjectId>,
    pub receipt_digest: Option<Sha256Digest>,
    pub version: AggregateVersion,
}
```

`CandidateHead` 不因集成而改写。`EnqueueIntegration` 是 PASS Submission 到 Relay 的唯一入口：它在一个数据库事务内固定完整 lineage，创建 `Integration(Queued)`、首张 Relay Ticket 和 `RelayCandidate` obligation，并把 `current_relay_ticket_id` 指向当前适用的 Ticket（首次入队时也是该 Submission/Relay 唯一 live Ticket）；真正的 Bundle 下载、Git push 与目标仓库访问在事务外由 Relay 完成。Ticket 在 `(submission_id, relay_id)` 内过期重签必须使用连续 `ticket_version` 和 `supersedes_ticket_id`，同事务把旧票置 `Superseded`、更新 current 指针并使旧 queue claim 失效；已 Relayed 的票不得重签。进入 `Merged` 必须同时证明 `l5_tested_head = integration_head = target_after`，并保存签名 Receipt、L5 Evidence 与 rollback ref。目标分支从 `target_before` 前移时创建新的 Integration 记录并复用已有 `Relayed` Ticket/任务分支 Receipt，不重复推送 Candidate；不得覆盖旧冲突或失败记录。只有 `Merged` Integration 才能把 WorkPackage 置为 `Integrated`。

Relay/队列的瞬态失败只能在同一 Integration、固定 Ticket 输入和有上限的 queue lease 内重试；冲突进入 `RebaseRequired`。当 L5、来源或策略 failure receipt 要把 Integration 终结为 `Failed` 时，`ReportIntegrationFailed` 必须在同一事务同步把 Package 转入 `ReworkReady` 或 `Failed`，不得让 Package 永久停在 `Integrating`，也不得原地修改已验收 Candidate。

## 9. Obligation 聚合

### 9.1 状态与结构

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObligationState {
    Pending,
    Claimed,
    Executing,
    Fulfilled,
    RetryScheduled,
    Escalated,
    Failed,
    Cancelled,
}

pub enum ObligationKind {
    ValidatePackage,
    ReofferUnclaimedPackage,
    DiagnoseStalledAttempt,
    ExpireLease,
    VerifyCandidate,
    ReviewCandidate,
    ReworkAttempt,
    DiagnoseDeadlock,
    ReviewBudget,
    IntegrateCandidate,
    ReplanSubgraph,
}

pub struct Obligation {
    pub id: ObligationId,
    pub project_id: ProjectId,
    pub kind: ObligationKind,
    pub subject_type: String,
    pub subject_id: Uuid,
    pub dedup_key: String,
    pub state: ObligationState,
    pub due_at: ServerInstant,
    pub claimed_by: Option<Uuid>,
    pub claim_until: Option<ServerInstant>,
    pub attempt_count: u16,
    pub max_attempts: u16,
    pub escalation_level: u8,
    pub fulfillment_evidence: Option<Sha256Digest>,
    pub version: AggregateVersion,
}
```

### 9.2 转移规则

| 当前状态 | 命令 | 下一状态 | 约束 |
| --- | --- | --- | --- |
| 无 | `CreateObligation` | `Pending` | 活跃状态下 `dedup_key` 唯一 |
| `Pending`/`RetryScheduled` | `ClaimDueObligation` | `Claimed` | `due_at <= db_now`；使用 `SKIP LOCKED` |
| `Claimed` | `StartObligation` | `Executing` | claimant 和 claim token 匹配 |
| `Executing` | `FulfillObligation` | `Fulfilled` | 必须有可验证 evidence/reference |
| `Claimed`/`Executing` | `RetryObligation` | `RetryScheduled` | 重试未超限，`due_at` 按退避计算 |
| `Claimed`/`Executing` | `EscalateObligation` | `Escalated` | 记录新义务 ID 与升级原因 |
| `Claimed`/`Executing` | `FailObligation` | `Failed` | 已超限且无更高升级路径 |
| 非终态 | `CancelObligation` | `Cancelled` | subject 已终止或义务已失效 |
| `Claimed`/`Executing` | claim 超时回收 | `Pending` | attempt_count 增加，旧 claim token 失效 |

LLM/Boss 的一次回复不能直接把义务标为 Fulfilled。必须由确定性 handler 检查其交付物，例如 PlanPatch 已落库、Review 结论已登记、Merge commit 已在 Git 目标引用可解析。

## 10. WorkGraph 与就绪投影

### 10.1 边类型

```rust
pub enum EdgeKind {
    HardDependency,
    ArtifactDependency,
    SoftContext,
    ReviewOf,
    Gate,
    ConflictsWith,
    Mutex,
    IntegrationAfter,
    Supersedes,
}
```

MVP 必须实现 `HardDependency`、`ArtifactDependency`、`ReviewOf`、`Mutex`、`Supersedes`；其余可以先持久化但不自动调度。

### 10.2 就绪判定

```rust
pub struct ReadinessFacts {
    pub revision_valid: bool,
    pub hard_dependencies_integrated: bool,
    pub artifacts_resolvable: bool,
    pub mutex_available: bool,
    pub budget_available: bool,
    pub terminal_or_superseded: bool,
}

pub fn is_ready(f: &ReadinessFacts) -> bool {
    f.revision_valid
        && f.hard_dependencies_integrated
        && f.artifacts_resolvable
        && f.mutex_available
        && f.budget_available
        && !f.terminal_or_superseded
}
```

就绪是服务器根据事实计算的投影，不接受 Worker/Boss 直接写 `ready=true`。`PlanPatch` 应针对精确 `base_graph_version`；检查 DAG 和权限后在 Serializable 事务中一次性写入新图版本、节点、边与事件。

## 11. 命令、事件与并发语义

### 11.1 命令元数据

```rust
pub struct CommandMeta {
    pub command_id: Uuid,
    pub actor_id: ActorId,
    pub idempotency_key: String,
    pub correlation_id: Uuid,
    pub causation_id: Option<Uuid>,
    pub expected_version: Option<AggregateVersion>,
}
```

- 所有外部写命令必须有 `idempotency_key`；同一 actor + key + request hash 返回首次结果；
- 相同 key 不同 request hash 在 HTTP 边界返回 `AF_IDEMPOTENCY_KEY_REUSED`；
- 有目标聚合的更新必须携带 `expected_version`；创建命令除外；
- CAS 失败返回领域错误 `DomainError::StaleVersion`，客户端必须重新读取，不得在服务器内盲重试业务决定；HTTP 边界映射为 `AF_VERSION_STALE`；
- 单命令的状态行、领域事件、Outbox、命令回执必须在同一事务提交。

### 11.2 事件信封

```rust
pub struct DomainEventEnvelope<E> {
    pub event_id: EventId,
    pub aggregate_type: &'static str,
    pub aggregate_id: Uuid,
    pub aggregate_seq: u64,
    pub event_type: &'static str,
    pub schema_version: u16,
    pub actor_id: ActorId,
    pub correlation_id: Uuid,
    pub causation_id: Option<Uuid>,
    pub occurred_at: ServerInstant,
    pub payload: E,
}
```

事件是 append-only 审计事实，但 MVP 不要求每次读取都从事件重放聚合。关系状态行是命令处理的当前投影，事件用于审计、Outbox、重建派生投影和调试。

### 11.3 跨聚合事务规则

允许的跨聚合原子操作只有明确列出的业务用例：

1. `ClaimPackage`：锁 WorkPackage，创建 Attempt 与 Lease，并把 Package 置 Active；
2. `RecordCandidate`：验证 Lease 与 COMPLETE CandidateArtifact，创建不可变 Candidate 与 VerificationRun，把 Package 置 Verifying，并关闭作者 Lease；
3. `FinalizeVerification`：锁定终态 VerificationRun，一次性创建终态 Submission，同时更新 Attempt 和 Package；PASS 只创建 `RequestIntegrationEnqueue` obligation，不创建 Integration 或 Relay Ticket；
4. `ExpireLease`：结束 Lease、Attempt，并重新投影 Package；
5. `ApplyPlanPatch`：创建新 graph version、边和 package revisions；
6. `EnqueueIntegration`：验证 PASS/Accepted、COMPLETE Artifact 和完整 lineage，原子创建 Integration、Relay Ticket、Relay obligation，并把 Package 置 Integrating；
7. `ReportIntegrationFailed`：固定失败 Integration，清 accepted 指针并把 Package 置 ReworkReady/Failed；
8. `MarkIntegrated`：写 integration 结果并更新 Package/依赖就绪投影。

其他跨系统动作（Git push、对象上传、消息发布、调用模型）不得放在数据库事务中，通过 Outbox/Obligation 驱动。

### 11.4 锁顺序

为避免死锁，所有实现按以下固定顺序取得行锁：

```text
project -> work_graph -> work_package -> attempt -> lease -> candidate_artifact -> candidate -> verification_run -> submission_manifest_staging -> submission -> integration -> relay_ticket -> relay_claim -> obligation
```

一次事务不需要的锁不得提前获取。遇到两个 Package（如 PlanPatch/mutex）时按 UUID 字节序排序后加锁。

## 12. 稳定领域错误

```rust
pub enum DomainError {
    InvalidArgument { field: String, reason: String },
    NotFound { resource: &'static str },
    StaleVersion,
    InvalidTransition { from: String, command: String },
    PackageNotReady { failed_checks: Vec<String> },
    PackageNotClaimable,
    AttemptLimitReached,
    PackageHashMismatch,
    StaleLease,
    LeaseExpired,
    WakeConditionUnsatisfied,
    SubmissionNotAcceptable { failed_checks: Vec<String> },
    InvariantViolation { invariant: &'static str },
}
```

领域错误不得包含 SQL、密钥、内部路径或模型原始输出。HTTP 映射和稳定外部错误码在控制平面文档定义。

## 13. 测试策略与可执行验收

### 13.1 领域单元测试

每个状态机使用表驱动测试，至少覆盖：

- 表中每条合法转移；
- 每个状态到所有不合法目标的拒绝；
- 终态不可复活；
- revision、version、token 的边界值和溢出；
- `INCONCLUSIVE`/`SKIPPED` 不会变成 PASS；
- `Accepted` 不会被误判为 `Integrated`；
- PASS Submission 未经 `EnqueueIntegration` 不会产生 Relay Ticket；入队时任何 lineage 不匹配或 Artifact 非 COMPLETE 都被拒绝；
- terminal Integration failure 必然使 Package 离开 `Integrating`，且只能经新 Attempt lineage 返工；
- `WaitingInput` 缺 wake condition 时构造失败；
- 任意命令序列都不破坏章节 3.3、4.4、7.3 的不变量。

建议用 `proptest` 生成命令序列：

```rust
proptest! {
    #[test]
    fn no_command_sequence_can_reuse_fence(commands in arb_lease_commands()) {
        let history = run(commands);
        let tokens: Vec<u64> = history.grants().map(|g| g.token.get()).collect();
        prop_assert!(tokens.windows(2).all(|w| w[0] < w[1]));
    }
}
```

### 13.2 数据库并发测试

真实 PostgreSQL 集成测试必须验证：

| 编号 | 场景 | 通过标准 |
| --- | --- | --- |
| `DB-CON-01` | 32 个请求同时 claim 同一 package revision | 恰好 1 个成功；仅 1 个 Active Lease；token 只增加 1 |
| `DB-CON-02` | 续租与过期回收同时发生 | 线性化结果只有“续租成功”或“过期成功”之一，不存在复活 |
| `DB-CON-03` | 新 Lease 产生后旧 Attempt 登记 Candidate | API 返回 `AF_LEASE_STALE`；正式 candidates/verification runs 无旧写入；可单独 salvage |
| `DB-CON-04` | 相同幂等键并发提交 20 次 | 副作用只发生 1 次，20 次响应 body/status 等价 |
| `DB-CON-05` | 两个 PlanPatch 针对同一 graph version | 仅 1 个提交，另一个返回 `AF_VERSION_STALE` |
| `DB-CON-06` | Outbox publisher 在 publish 后、ack 前崩溃 | 消息可重复，但消费者幂等，业务状态不重复变化 |

### 13.3 MVP 完成门槛

```bash
cargo test -p agentforge-domain --all-features
cargo test -p agentforge-storage-postgres --test state_machine_concurrency
cargo test -p agentforge-control-plane --test api_contract
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

验收时还必须导出一份机器可读状态覆盖报告，证明：

- 所有合法转移至少执行一次；
- 所有聚合终态均测试不可逆；
- 六项数据库并发场景全部通过且无 flaky retry；
- 所有状态枚举在数据库映射中穷尽处理，不使用未知值回退为默认状态。

## 14. MVP 明确简化项

为了在 2 核 4 GB 中央服务器上可靠运行，MVP 做以下限制：

- 单个 package revision 只允许一个正式 Active Attempt，不做竞赛式并行候选；
- 关系状态投影与 append-only 事件并存，不实现完整事件溯源框架；
- 不引入分布式锁，唯一协调者是 PostgreSQL 行锁、唯一索引和 CAS；
- Obligation 使用数据库到期扫描，不依赖 Temporal 或每秒心跳；
- NATS 仅作为后续可替换的 Outbox 下游，不能成为状态来源；
- 高频 Worker 日志和 token 不进入领域事件表，只登记分块制品摘要。

这些限制不改变协议对象和状态机，将来扩展多个 Active Attempt、JetStream 或 Temporal 时不需要改写 AFWP 的核心语义。
