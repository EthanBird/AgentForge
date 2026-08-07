# AgentForge 控制平面实施规范

> 文档状态：MVP 可执行设计
>
> 目标：在 2 核 4 GB 中央服务器上实现可验证的任务包发布、认领、租约、提交、监督义务与事件投递闭环
>
> 前置阅读：`01_DOMAIN_MODEL_AND_STATE_MACHINES.md`、ADR-0001、ADR-0002

## 1. MVP 交付范围

首个可运行版本必须完成以下纵向链路：

```text
创建项目和不可变 PackageRevision
  -> 校验并发布 Offer
  -> Worker 并发认领，服务器授予 Attempt + Lease + fencing token
  -> 续租、进度、Checkpoint、等待/恢复
  -> 登记不可变 Candidate + Author Evidence
  -> 独立 VerificationRun 完成后一次性创建终态 Submission
  -> Accepted
  -> 模拟/真实 Git Relay 回执
  -> Integrated/Closed
```

MVP 不包含：复杂机器学习路由、多活 PostgreSQL、Temporal、必须依赖 NATS 的业务状态、中央服务器执行编译、内置完整代码托管。路由器先使用硬能力过滤 + 加权分数；Git Relay 可以先实现为受认证的回执 API，但状态语义不得简化。

## 2. Rust Workspace 与模块边界

### 2.1 目录

```text
crates/
  domain/                 # package agentforge-domain；无 I/O 聚合/WorkGraph
  application/            # package agentforge-application；用例与 ports
  protocol/               # package agentforge-protocol；DTO/Schema/版本
  persistence-postgres/   # package agentforge-storage-postgres；SQLx/迁移/锁
  control-plane/          # package agentforge-control-plane；Axum 组合根
  obligation-engine/      # package agentforge-obligation-engine
  outbox/                 # package agentforge-outbox；发布/SSE/Inbox
  matcher/                # package agentforge-matcher
  test-support/           # package agentforge-test-support
```

这是总计划中权威 Workspace 的控制平面子集；目录用于文件路径，`package` 名用于 `cargo -p`，不得再创造第三套 crate 名称。

### 2.2 依赖规则

| crate | 可以依赖 | 不得依赖 |
| --- | --- | --- |
| `agentforge-domain` | `serde`、`uuid`、`time`、`thiserror`、纯算法库 | SQLx、Axum、网络/Git/对象存储 SDK |
| `agentforge-application` | `agentforge-domain`、`agentforge-protocol`、`async-trait` | 具体 PostgreSQL/Axum 类型 |
| `agentforge-storage-postgres` | application ports、`agentforge-domain`、SQLx | Axum handler、jcode |
| `agentforge-obligation-engine` | `agentforge-application`、领域命令 | 直接 UPDATE 业务表绕过 handler |
| `agentforge-outbox` | storage ports、协议信封 | 将 broker ACK 当业务完成 |
| `agentforge-control-plane` | 以上所有 crate、Axum/Tower | 在 route handler 内手写领域规则 |

CI 增加依赖边检查；最低限度使用 `cargo metadata` 脚本断言 `domain` 的依赖闭包中没有 `sqlx`、`axum`、`reqwest`、Git SDK。

### 2.3 Application ports

事务必须由 application use case 控制，repository 不得偷偷开启彼此独立的事务：

```rust
#[async_trait::async_trait]
pub trait UnitOfWork: Send {
    type Packages: PackageRepository;
    type Attempts: AttemptRepository;
    type Leases: LeaseRepository;
    type CandidateArtifacts: CandidateArtifactRepository;
    type Candidates: CandidateRepository;
    type VerificationRuns: VerificationRunRepository;
    type VerificationFacts: VerificationFactRepository;
    type ManifestStaging: SubmissionManifestStagingRepository;
    type Submissions: SubmissionRepository;
    type Integrations: IntegrationRepository;
    type RelayTickets: RelayTicketRepository;
    type RelayClaims: RelayClaimRepository;
    type Obligations: ObligationRepository;

    fn packages(&mut self) -> &mut Self::Packages;
    fn attempts(&mut self) -> &mut Self::Attempts;
    fn leases(&mut self) -> &mut Self::Leases;
    fn candidate_artifacts(&mut self) -> &mut Self::CandidateArtifacts;
    fn candidates(&mut self) -> &mut Self::Candidates;
    fn verification_runs(&mut self) -> &mut Self::VerificationRuns;
    fn verification_facts(&mut self) -> &mut Self::VerificationFacts;
    fn manifest_staging(&mut self) -> &mut Self::ManifestStaging;
    fn submissions(&mut self) -> &mut Self::Submissions;
    fn integrations(&mut self) -> &mut Self::Integrations;
    fn relay_tickets(&mut self) -> &mut Self::RelayTickets;
    fn relay_claims(&mut self) -> &mut Self::RelayClaims;
    fn obligations(&mut self) -> &mut Self::Obligations;

    async fn append_events(&mut self, events: &[DomainEvent]) -> AppResult<()>;
    async fn enqueue_outbox(&mut self, messages: &[OutboxMessage]) -> AppResult<()>;
    async fn store_receipt(&mut self, receipt: CommandReceipt) -> AppResult<()>;
    async fn commit(self) -> AppResult<()>;
    async fn rollback(self) -> AppResult<()>;
}

#[async_trait::async_trait]
pub trait UnitOfWorkFactory {
    type Uow: UnitOfWork;
    async fn begin(&self, isolation: Isolation) -> AppResult<Self::Uow>;
}
```

`ReadCommitted` 是普通命令默认隔离级别，依靠行锁、唯一约束与 CAS 保证线性化；`ApplyPlanPatch` 和需要一次检查整个子图的操作使用 `Serializable`，遇到 `40001` 最多重试 3 次，并在每次重试中重新执行完整确定性校验。

## 3. 进程组成与启动顺序

单个 `agent-factoryd` 二进制内部启动以下任务：

1. HTTP API 与 SSE；
2. Lease expiry reconciler；
3. Obligation scheduler；
4. Outbox publisher；
5. 卡死 claim/outbox 的 recovery sweeper；
6. 指标和健康检查。

它们共享 SQLx pool，但各自有并发上限和取消令牌。启动顺序：

```text
加载配置 -> 连接 DB -> 检查 schema version
-> 版本匹配后启动后台 reconciler -> 开放 readiness -> 接收流量
```

生产服务不得自动执行 migration；版本不匹配时保持 not-ready 并退出或等待运维。`agentforge-admin migrate` 才能获取 migration advisory lock 并显式执行迁移。开发模式可以通过明确配置启用 auto-migrate，默认仍关闭。

关闭顺序：先把 readiness 置 false，停止新 claim，给正在执行的短事务最多 10 秒完成，停止 scheduler/outbox 领取新批次，最后关闭连接池。不得等待 Worker Lease 自然到期才退出服务。

## 4. PostgreSQL 物理模型

以下 DDL 是实现基线。生产迁移应拆为编号文件；示例省略项目合同、Agent Registry 的非关键展示字段，但不得省略约束。

### 4.1 项目、图与任务包

```sql
CREATE TABLE projects (
    id uuid PRIMARY KEY,
    protocol_key text NOT NULL UNIQUE,
    name text NOT NULL,
    state text NOT NULL CHECK (state IN ('ACTIVE','PAUSED','CLOSED')),
    graph_version bigint NOT NULL DEFAULT 0 CHECK (graph_version >= 0),
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    event_seq bigint NOT NULL DEFAULT 0 CHECK (event_seq >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE graph_versions (
    project_id uuid NOT NULL REFERENCES projects(id),
    graph_version bigint NOT NULL CHECK (graph_version > 0),
    base_graph_version bigint NOT NULL CHECK (base_graph_version >= 0),
    patch jsonb NOT NULL,
    patch_hash bytea NOT NULL CHECK (octet_length(patch_hash) = 32),
    created_by uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (project_id, graph_version),
    UNIQUE (project_id, patch_hash)
);

CREATE TABLE work_packages (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    protocol_key text NOT NULL,
    selected_revision_id uuid,
    state text NOT NULL CHECK (state IN (
      'DRAFT','VALIDATING','BLOCKED','OFFERED','ACTIVE','VERIFYING',
      'REWORK_READY','ACCEPTED','INTEGRATING','REBASE_REQUIRED',
      'INTEGRATED','CLOSED','CANCELLED','SUPERSEDED','FAILED'
    )),
    priority smallint NOT NULL DEFAULT 50 CHECK (priority BETWEEN 0 AND 100),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 100),
    attempts_started integer NOT NULL DEFAULT 0 CHECK (attempts_started >= 0),
    next_fencing_token bigint NOT NULL DEFAULT 0 CHECK (next_fencing_token >= 0),
    active_attempt_id uuid,
    accepted_submission_id uuid,
    integrated_integration_id uuid,
    integrated_commit text,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    event_seq bigint NOT NULL DEFAULT 0 CHECK (event_seq >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (attempts_started <= max_attempts),
    CHECK ((state IN ('INTEGRATED','CLOSED')) =
           (integrated_commit IS NOT NULL AND integrated_integration_id IS NOT NULL)),
    CHECK ((state IN ('ACTIVE','VERIFYING')) = (active_attempt_id IS NOT NULL)),
    CHECK ((state IN ('ACCEPTED','INTEGRATING','REBASE_REQUIRED','INTEGRATED','CLOSED'))
           = (accepted_submission_id IS NOT NULL)),
    UNIQUE (project_id, protocol_key)
);

CREATE INDEX work_packages_market_idx
    ON work_packages (priority DESC, created_at, id)
    WHERE state IN ('OFFERED','REWORK_READY');
CREATE INDEX work_packages_project_state_idx
    ON work_packages (project_id, state, id);

CREATE TABLE package_revisions (
    id uuid PRIMARY KEY,
    package_id uuid NOT NULL REFERENCES work_packages(id),
    revision integer NOT NULL CHECK (revision > 0),
    schema_version text NOT NULL,
    canonical_document jsonb NOT NULL,
    package_hash bytea NOT NULL CHECK (octet_length(package_hash) = 32),
    base_commit text NOT NULL,
    git_object_format text NOT NULL CHECK (git_object_format IN ('sha1','sha256')),
    input_snapshot jsonb NOT NULL,
    created_by uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id, package_id),
    UNIQUE (package_id, revision),
    UNIQUE (package_id, package_hash)
);

-- 由迁移 owner 创建；运行账号只能 INSERT/SELECT，触发器再提供一道防线。
CREATE FUNCTION reject_immutable_row_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'immutable relation: %', TG_TABLE_NAME USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER package_revisions_are_immutable
BEFORE UPDATE OR DELETE ON package_revisions
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

ALTER TABLE work_packages
  ADD CONSTRAINT work_packages_selected_revision_fk
  FOREIGN KEY (selected_revision_id, id)
  REFERENCES package_revisions(id, package_id);

CREATE TABLE package_edges (
    project_id uuid NOT NULL REFERENCES projects(id),
    graph_version bigint NOT NULL,
    from_package_id uuid NOT NULL REFERENCES work_packages(id),
    to_package_id uuid NOT NULL REFERENCES work_packages(id),
    kind text NOT NULL CHECK (kind IN (
      'HARD_DEPENDENCY','ARTIFACT_DEPENDENCY','SOFT_CONTEXT','REVIEW_OF',
      'GATE','CONFLICTS_WITH','MUTEX','INTEGRATION_AFTER','SUPERSEDES'
    )),
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (project_id, graph_version, from_package_id, to_package_id, kind),
    FOREIGN KEY (project_id, graph_version)
      REFERENCES graph_versions(project_id, graph_version),
    CHECK (from_package_id <> to_package_id)
);

CREATE INDEX package_edges_to_idx
    ON package_edges (project_id, graph_version, to_package_id, kind);
```

图版本不可原地修改。MVP 为便于查询可以每一版本保存完整边集合；图规模超过 10 万边后再评估 patch + snapshot，不提前优化。

`protocol_key` 保存协议文档中便于人读的稳定键，应用层按对应 Schema pattern 校验；`id` 才是内部 UUID v7 主键。MVP 单租户中 `projects.protocol_key` 全局唯一，`work_packages.protocol_key` 在项目内唯一；这两个键来自创建时提交的 AFWP 且不可变。Attempt、Lease、Submission、Relay Ticket 的 `protocol_key` 由服务器生成并全局唯一。入口先把协议键解析成 UUID，后续事务、外键和锁全部只使用 UUID，避免字符串别名歧义。

### 4.2 Attempt 与 Lease

```sql
CREATE TABLE attempts (
    id uuid PRIMARY KEY,
    protocol_key text NOT NULL UNIQUE,
    package_id uuid NOT NULL REFERENCES work_packages(id),
    revision_id uuid NOT NULL REFERENCES package_revisions(id),
    executor_id uuid NOT NULL,
    node_id uuid NOT NULL,
    state text NOT NULL CHECK (state IN (
      'CREATED','LEASED','PREPARING','PLANNING','IMPLEMENTING','LOCAL_VERIFY',
      'WAITING_INPUT','CANDIDATE','ISOLATED_REVIEW','CLEAN_REPRODUCE',
      'SUBMITTED','PASSED','REJECTED','LOST','FAILED','CANCELLED'
    )),
    lease_id uuid,
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    base_commit text NOT NULL,
    wake_condition jsonb,
    resume_state text,
    semantic_progress_seq bigint NOT NULL DEFAULT 0,
    last_semantic_progress_at timestamptz,
    last_checkpoint_digest bytea CHECK (
      last_checkpoint_digest IS NULL OR octet_length(last_checkpoint_digest) = 32),
    candidate_commit text,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    event_seq bigint NOT NULL DEFAULT 0 CHECK (event_seq >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((state = 'WAITING_INPUT') = (wake_condition IS NOT NULL)),
    CHECK ((state = 'WAITING_INPUT') = (resume_state IS NOT NULL)),
    UNIQUE (id, package_id),
    UNIQUE (id, package_id, revision_id, fencing_token),
    UNIQUE (id, package_id, revision_id, lease_id, fencing_token,
            base_commit, candidate_commit),
    FOREIGN KEY (revision_id, package_id)
      REFERENCES package_revisions(id, package_id)
);

CREATE INDEX attempts_package_created_idx
    ON attempts (package_id, created_at DESC);
CREATE INDEX attempts_stalled_idx
    ON attempts (last_semantic_progress_at, id)
    WHERE state IN ('PLANNING','IMPLEMENTING','LOCAL_VERIFY');

CREATE TABLE leases (
    id uuid PRIMARY KEY,
    protocol_key text NOT NULL UNIQUE,
    package_id uuid NOT NULL REFERENCES work_packages(id),
    revision_id uuid NOT NULL REFERENCES package_revisions(id),
    attempt_id uuid NOT NULL UNIQUE REFERENCES attempts(id),
    holder_node_id uuid NOT NULL,
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    state text NOT NULL CHECK (state IN ('ACTIVE','RELEASED','REVOKED','EXPIRED')),
    granted_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    max_expires_at timestamptz NOT NULL,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    event_seq bigint NOT NULL DEFAULT 0 CHECK (event_seq >= 0),
    terminal_reason text,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (granted_at < expires_at),
    CHECK (expires_at <= max_expires_at),
    UNIQUE (revision_id, fencing_token),
    UNIQUE (id, attempt_id, fencing_token),
    UNIQUE (id, attempt_id, package_id, revision_id, fencing_token),
    FOREIGN KEY (revision_id, package_id)
      REFERENCES package_revisions(id, package_id),
    FOREIGN KEY (attempt_id, package_id, revision_id, fencing_token)
      REFERENCES attempts(id, package_id, revision_id, fencing_token)
      DEFERRABLE INITIALLY DEFERRED
);

CREATE UNIQUE INDEX leases_one_active_revision_idx
    ON leases (revision_id) WHERE state = 'ACTIVE';
CREATE INDEX leases_expiry_idx
    ON leases (expires_at, id) WHERE state = 'ACTIVE';

ALTER TABLE attempts
  ADD CONSTRAINT attempts_lease_fk
  FOREIGN KEY (lease_id, id, package_id, revision_id, fencing_token)
  REFERENCES leases(id, attempt_id, package_id, revision_id, fencing_token)
  DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE work_packages
  ADD CONSTRAINT work_packages_active_attempt_fk
  FOREIGN KEY (active_attempt_id, id) REFERENCES attempts(id, package_id);
```

部分唯一索引只识别 `state='ACTIVE'`，不会自动看 `expires_at`。因此 claim handler 锁定 package 后必须先把已经到期但尚未 sweep 的 Active Lease 原子终结，再决定是否授予新 Lease。

### 4.3 Progress、Checkpoint、Candidate、VerificationRun、Submission 与验收结果

```sql
CREATE TABLE attempt_progress (
    attempt_id uuid NOT NULL REFERENCES attempts(id),
    client_progress_id text NOT NULL,
    fencing_token bigint NOT NULL,
    semantic boolean NOT NULL,
    summary jsonb NOT NULL,
    artifact_digest bytea CHECK (artifact_digest IS NULL OR octet_length(artifact_digest) = 32),
    received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (attempt_id, client_progress_id)
);

CREATE TABLE checkpoints (
    id uuid PRIMARY KEY,
    attempt_id uuid NOT NULL REFERENCES attempts(id),
    sequence bigint NOT NULL CHECK (sequence > 0),
    fencing_token bigint NOT NULL,
    digest bytea NOT NULL CHECK (octet_length(digest) = 32),
    artifact_uri text NOT NULL,
    metadata jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (attempt_id, sequence),
    UNIQUE (attempt_id, digest)
);

CREATE TABLE candidate_artifacts (
    id uuid PRIMARY KEY,
    candidate_id uuid NOT NULL UNIQUE,
    attempt_id uuid NOT NULL REFERENCES attempts(id),
    lease_id uuid NOT NULL REFERENCES leases(id),
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    candidate_commit text NOT NULL,
    tree_hash text NOT NULL,
    state text NOT NULL CHECK (state IN (
      'UPLOADING','ASSEMBLING','COMPLETE','REJECTED','QUARANTINED','EXPIRED'
    )),
    bundle_uri text,
    bundle_digest bytea CHECK (
      bundle_digest IS NULL OR octet_length(bundle_digest) = 32),
    bundle_size_bytes bigint CHECK (
      bundle_size_bytes IS NULL OR bundle_size_bytes > 0),
    chunk_manifest jsonb,
    author_evidence_digest bytea NOT NULL CHECK (
      octet_length(author_evidence_digest) = 32),
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    CHECK (state <> 'COMPLETE' OR (
      bundle_uri IS NOT NULL AND bundle_digest IS NOT NULL
      AND bundle_size_bytes IS NOT NULL AND chunk_manifest IS NOT NULL
      AND completed_at IS NOT NULL
    )),
    UNIQUE (id, candidate_id, attempt_id, candidate_commit),
    UNIQUE (id, candidate_id, attempt_id, lease_id, fencing_token,
            candidate_commit, tree_hash, author_evidence_digest),
    FOREIGN KEY (lease_id, attempt_id, fencing_token)
      REFERENCES leases(id, attempt_id, fencing_token)
);

CREATE INDEX candidate_artifact_gc_idx
    ON candidate_artifacts (created_at, id)
    WHERE state IN ('UPLOADING','ASSEMBLING','REJECTED','EXPIRED','QUARANTINED');

-- 非终态上传行仍可 CAS；一旦 OLD.state=COMPLETE，连状态回退和 DELETE 都拒绝。
CREATE FUNCTION reject_complete_candidate_artifact_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.state = 'COMPLETE' THEN
    RAISE EXCEPTION 'complete candidate artifact is immutable: %', OLD.id
      USING ERRCODE = '55000';
  END IF;
  IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER complete_candidate_artifacts_are_immutable
BEFORE UPDATE OR DELETE ON candidate_artifacts
FOR EACH ROW EXECUTE FUNCTION reject_complete_candidate_artifact_change();

CREATE TABLE candidates (
    id uuid PRIMARY KEY,
    attempt_id uuid NOT NULL REFERENCES attempts(id),
    package_id uuid NOT NULL REFERENCES work_packages(id),
    revision_id uuid NOT NULL REFERENCES package_revisions(id),
    lease_id uuid NOT NULL REFERENCES leases(id),
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    base_commit text NOT NULL,
    candidate_commit text NOT NULL,
    tree_hash text NOT NULL,
    branch text NOT NULL,
    bundle_artifact_id uuid NOT NULL,
    author_evidence_digest bytea NOT NULL CHECK (octet_length(author_evidence_digest) = 32),
    sealed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (attempt_id, candidate_commit),
    UNIQUE (id, bundle_artifact_id),
    UNIQUE (id, bundle_artifact_id, attempt_id, package_id, revision_id,
            candidate_commit),
    FOREIGN KEY (attempt_id, package_id, revision_id, lease_id, fencing_token,
                 base_commit, candidate_commit)
      REFERENCES attempts(id, package_id, revision_id, lease_id, fencing_token,
                          base_commit, candidate_commit)
      DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (lease_id, attempt_id, package_id, revision_id, fencing_token)
      REFERENCES leases(id, attempt_id, package_id, revision_id, fencing_token),
    FOREIGN KEY (bundle_artifact_id, id, attempt_id, lease_id, fencing_token,
                 candidate_commit, tree_hash, author_evidence_digest)
      REFERENCES candidate_artifacts(
        id, candidate_id, attempt_id, lease_id, fencing_token,
        candidate_commit, tree_hash, author_evidence_digest)
);

CREATE TRIGGER candidates_are_immutable
BEFORE UPDATE OR DELETE ON candidates
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

CREATE TABLE verification_runs (
    id uuid PRIMARY KEY,
    candidate_id uuid NOT NULL UNIQUE REFERENCES candidates(id),
    state text NOT NULL CHECK (state IN (
      'QUEUED','PROVENANCE_CHECK','REVIEWING','REPRODUCING',
      'PASS','FAIL','INCONCLUSIVE','CANCELLED'
    )),
    terminal_stage text CHECK (terminal_stage IN (
      'PROVENANCE_CHECK','REVIEWING','REPRODUCING'
    )),
    terminal_stage_result_id uuid,
    tested_head text,
    reviewed_head text,
    evidence_digest bytea CHECK (evidence_digest IS NULL OR octet_length(evidence_digest) = 32),
    conclusion_reason jsonb,
    terminalized_at timestamptz,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    event_seq bigint NOT NULL DEFAULT 0 CHECK (event_seq >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id, candidate_id),
    CHECK (
      (state IN ('QUEUED','PROVENANCE_CHECK','REVIEWING','REPRODUCING')
       AND terminal_stage IS NULL AND terminal_stage_result_id IS NULL
       AND terminalized_at IS NULL)
      OR (state = 'PASS' AND terminal_stage = 'REPRODUCING'
          AND terminal_stage_result_id IS NOT NULL AND terminalized_at IS NOT NULL)
      OR (state IN ('FAIL','INCONCLUSIVE') AND terminal_stage IS NOT NULL
          AND terminal_stage_result_id IS NOT NULL AND terminalized_at IS NOT NULL)
      OR (state = 'CANCELLED' AND terminal_stage_result_id IS NULL
          AND terminalized_at IS NOT NULL)
    )
);

CREATE INDEX verification_runs_queue_idx
    ON verification_runs (created_at, id)
    WHERE state IN ('QUEUED','PROVENANCE_CHECK','REVIEWING','REPRODUCING');

CREATE FUNCTION reject_terminal_verification_run_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'verification run cannot be deleted: %', OLD.id
      USING ERRCODE = '55000';
  END IF;
  IF OLD.state IN ('PASS','FAIL','INCONCLUSIVE','CANCELLED') THEN
    RAISE EXCEPTION 'terminal verification run is immutable: %', OLD.id
      USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER terminal_verification_runs_are_immutable
BEFORE UPDATE OR DELETE ON verification_runs
FOR EACH ROW EXECUTE FUNCTION reject_terminal_verification_run_change();

-- 每个阶段只有一条被控制面接受的聚合事实；瞬态 job 的原始尝试保存在签名 Evidence 中。
CREATE TABLE verification_stage_results (
    id uuid PRIMARY KEY,
    verification_run_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    stage text NOT NULL CHECK (stage IN (
      'PROVENANCE_CHECK','REVIEWING','REPRODUCING'
    )),
    outcome text NOT NULL CHECK (outcome IN ('PASS','FAIL','INCONCLUSIVE')),
    service_actor_id uuid NOT NULL,
    job_id uuid NOT NULL,
    head text,
    evidence_digest bytea NOT NULL CHECK (octet_length(evidence_digest) = 32),
    report_uri text NOT NULL,
    signature jsonb NOT NULL,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (verification_run_id, stage),
    UNIQUE (id, verification_run_id, candidate_id, stage),
    FOREIGN KEY (verification_run_id, candidate_id)
      REFERENCES verification_runs(id, candidate_id)
);

ALTER TABLE verification_runs
  ADD CONSTRAINT verification_runs_terminal_stage_result_fk
  FOREIGN KEY (terminal_stage_result_id, id, candidate_id, terminal_stage)
  REFERENCES verification_stage_results(
    id, verification_run_id, candidate_id, stage)
  DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE review_reports (
    id uuid PRIMARY KEY,
    verification_run_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    reviewer_actor_id uuid NOT NULL,
    reviewer_model_family text NOT NULL,
    reviewed_head text,
    verdict text NOT NULL CHECK (verdict IN ('PASS','FAIL','INCONCLUSIVE')),
    evidence_digest bytea NOT NULL CHECK (octet_length(evidence_digest) = 32),
    report_uri text NOT NULL,
    signature jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (verdict <> 'PASS' OR reviewed_head IS NOT NULL),
    UNIQUE (verification_run_id, reviewer_actor_id),
    UNIQUE (id, verification_run_id, candidate_id),
    FOREIGN KEY (verification_run_id, candidate_id)
      REFERENCES verification_runs(id, candidate_id)
);

CREATE TABLE review_findings (
    id uuid PRIMARY KEY,
    review_report_id uuid NOT NULL,
    verification_run_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    severity text NOT NULL CHECK (severity IN ('CRITICAL','HIGH','MEDIUM','LOW','INFO')),
    criterion_id text,
    rule_id text,
    location jsonb NOT NULL DEFAULT '{}'::jsonb,
    expected text NOT NULL,
    actual text NOT NULL,
    reproduction jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (review_report_id, id),
    FOREIGN KEY (review_report_id, verification_run_id, candidate_id)
      REFERENCES review_reports(id, verification_run_id, candidate_id)
);

CREATE TABLE reproduction_results (
    id uuid PRIMARY KEY,
    verification_run_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    runner_actor_id uuid NOT NULL,
    runner_image_digest bytea NOT NULL CHECK (octet_length(runner_image_digest) = 32),
    tested_head text,
    outcome text NOT NULL CHECK (outcome IN ('PASS','FAIL','INCONCLUSIVE')),
    evidence_digest bytea NOT NULL CHECK (octet_length(evidence_digest) = 32),
    report_uri text NOT NULL,
    signature jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (outcome <> 'PASS' OR tested_head IS NOT NULL),
    UNIQUE (verification_run_id),
    UNIQUE (id, verification_run_id, candidate_id),
    FOREIGN KEY (verification_run_id, candidate_id)
      REFERENCES verification_runs(id, candidate_id)
);

CREATE TABLE criterion_results (
    verification_run_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    criterion_id text NOT NULL,
    hard boolean NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('PASS','FAIL','INCONCLUSIVE','SKIPPED')),
    service_actor_id uuid NOT NULL,
    evidence_digest bytea NOT NULL CHECK (octet_length(evidence_digest) = 32),
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (verification_run_id, criterion_id),
    FOREIGN KEY (verification_run_id, candidate_id)
      REFERENCES verification_runs(id, candidate_id)
);

-- INSERT 也必须在 run 终结前线性化；防止 terminal_stage 固定后追加“补做”事实。
CREATE FUNCTION ensure_verification_run_accepts_fact() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE run_state text;
BEGIN
  SELECT state INTO run_state
  FROM verification_runs
  WHERE id = NEW.verification_run_id
  FOR UPDATE;
  IF run_state IS NULL OR run_state IN ('PASS','FAIL','INCONCLUSIVE','CANCELLED') THEN
    RAISE EXCEPTION 'verification run does not accept new facts: %',
      NEW.verification_run_id USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER verification_stage_results_before_terminal
BEFORE INSERT ON verification_stage_results
FOR EACH ROW EXECUTE FUNCTION ensure_verification_run_accepts_fact();
CREATE TRIGGER review_reports_before_terminal
BEFORE INSERT ON review_reports
FOR EACH ROW EXECUTE FUNCTION ensure_verification_run_accepts_fact();
CREATE TRIGGER review_findings_before_terminal
BEFORE INSERT ON review_findings
FOR EACH ROW EXECUTE FUNCTION ensure_verification_run_accepts_fact();
CREATE TRIGGER reproduction_results_before_terminal
BEFORE INSERT ON reproduction_results
FOR EACH ROW EXECUTE FUNCTION ensure_verification_run_accepts_fact();
CREATE TRIGGER criterion_results_before_terminal
BEFORE INSERT ON criterion_results
FOR EACH ROW EXECUTE FUNCTION ensure_verification_run_accepts_fact();

CREATE TRIGGER verification_stage_results_are_immutable
BEFORE UPDATE OR DELETE ON verification_stage_results
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();
CREATE TRIGGER review_reports_are_immutable
BEFORE UPDATE OR DELETE ON review_reports
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();
CREATE TRIGGER review_findings_are_immutable
BEFORE UPDATE OR DELETE ON review_findings
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();
CREATE TRIGGER reproduction_results_are_immutable
BEFORE UPDATE OR DELETE ON reproduction_results
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();
CREATE TRIGGER criterion_results_are_immutable
BEFORE UPDATE OR DELETE ON criterion_results
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

-- Phase A：Coordinator 在数据库事务外上传并 HEAD 校验内容寻址 Manifest，
-- 再用短事务登记这条不可变 staging fact；未绑定对象字节可按 TTL GC，事实行保留审计。
CREATE TABLE submission_manifest_staging (
    id uuid PRIMARY KEY,
    submission_id uuid NOT NULL UNIQUE,
    submission_kind text NOT NULL CHECK (submission_kind IN ('CANDIDATE','SALVAGE')),
    attempt_id uuid NOT NULL REFERENCES attempts(id),
    package_id uuid NOT NULL REFERENCES work_packages(id),
    revision_id uuid NOT NULL REFERENCES package_revisions(id),
    candidate_id uuid,
    candidate_artifact_id uuid,
    verification_run_id uuid,
    terminal_outcome text NOT NULL CHECK (
      terminal_outcome IN ('PASS','FAIL','INCONCLUSIVE','QUARANTINED')),
    completed_stage text NOT NULL CHECK (completed_stage IN (
      'PROVENANCE_CHECK','REVIEWING','REPRODUCING',
      'CANDIDATE_READY','SALVAGE_REGISTRATION'
    )),
    submitted_head text,
    fact_set_digest bytea NOT NULL CHECK (octet_length(fact_set_digest) = 32),
    manifest_uri text NOT NULL,
    manifest_digest bytea NOT NULL CHECK (octet_length(manifest_digest) = 32),
    coordinator_actor_id uuid NOT NULL,
    signature jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    UNIQUE (id, submission_id),
    UNIQUE (id, submission_id, submission_kind, attempt_id, package_id,
            revision_id, candidate_id, candidate_artifact_id,
            verification_run_id, terminal_outcome, completed_stage,
            submitted_head, manifest_digest),
    CHECK (expires_at > created_at),
    CHECK (
      (submission_kind = 'CANDIDATE' AND candidate_id IS NOT NULL
       AND candidate_artifact_id IS NOT NULL AND verification_run_id IS NOT NULL
       AND submitted_head IS NOT NULL
       AND terminal_outcome IN ('PASS','FAIL','INCONCLUSIVE'))
      OR
      (submission_kind = 'SALVAGE' AND candidate_id IS NULL
       AND candidate_artifact_id IS NULL AND verification_run_id IS NULL
       AND submitted_head IS NULL AND terminal_outcome = 'QUARANTINED'
       AND completed_stage = 'SALVAGE_REGISTRATION')
    ),
    FOREIGN KEY (candidate_id, candidate_artifact_id, attempt_id, package_id,
                 revision_id, submitted_head)
      REFERENCES candidates(id, bundle_artifact_id, attempt_id, package_id,
                            revision_id, candidate_commit),
    FOREIGN KEY (verification_run_id, candidate_id)
      REFERENCES verification_runs(id, candidate_id)
);

CREATE TRIGGER submission_manifest_staging_is_immutable
BEFORE UPDATE OR DELETE ON submission_manifest_staging
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

CREATE TABLE submissions (
    id uuid PRIMARY KEY,
    protocol_key text NOT NULL UNIQUE,
    submission_kind text NOT NULL CHECK (submission_kind IN ('CANDIDATE','SALVAGE')),
    attempt_id uuid NOT NULL REFERENCES attempts(id),
    package_id uuid NOT NULL REFERENCES work_packages(id),
    revision_id uuid NOT NULL REFERENCES package_revisions(id),
    candidate_id uuid REFERENCES candidates(id),
    candidate_artifact_id uuid REFERENCES candidate_artifacts(id),
    verification_run_id uuid UNIQUE REFERENCES verification_runs(id),
    state text NOT NULL CHECK (state IN ('PASS','FAIL','INCONCLUSIVE','QUARANTINED')),
    completed_stage text NOT NULL CHECK (completed_stage IN (
      'PROVENANCE_CHECK','REVIEWING','REPRODUCING',
      'CANDIDATE_READY','SALVAGE_REGISTRATION'
    )),
    submitted_head text,
    tested_head text,
    reviewed_head text,
    manifest_staging_id uuid NOT NULL,
    manifest_uri text NOT NULL,
    manifest_digest bytea NOT NULL CHECK (octet_length(manifest_digest) = 32),
    evidence_digest bytea CHECK (evidence_digest IS NULL OR octet_length(evidence_digest) = 32),
    salvage_manifest jsonb,
    failure_dossier jsonb,
    conclusion_reason jsonb,
    version bigint NOT NULL DEFAULT 0 CHECK (version = 0),
    event_seq bigint NOT NULL DEFAULT 1 CHECK (event_seq = 1),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id, package_id),
    UNIQUE (id, package_id, candidate_id, candidate_artifact_id,
            verification_run_id, state),
    CHECK (
      (submission_kind = 'CANDIDATE' AND candidate_id IS NOT NULL
       AND candidate_artifact_id IS NOT NULL
       AND verification_run_id IS NOT NULL AND submitted_head IS NOT NULL
       AND evidence_digest IS NOT NULL AND salvage_manifest IS NULL
       AND state IN ('PASS','FAIL','INCONCLUSIVE')
       AND (
         (state = 'PASS' AND completed_stage = 'CANDIDATE_READY'
          AND tested_head IS NOT NULL AND reviewed_head IS NOT NULL
          AND failure_dossier IS NULL)
         OR
         (state IN ('FAIL','INCONCLUSIVE')
          AND completed_stage IN ('PROVENANCE_CHECK','REVIEWING','REPRODUCING')
          AND failure_dossier IS NOT NULL)
       ))
      OR
      (submission_kind = 'SALVAGE' AND candidate_id IS NULL
       AND candidate_artifact_id IS NULL
       AND verification_run_id IS NULL AND submitted_head IS NULL
       AND tested_head IS NULL AND reviewed_head IS NULL
       AND salvage_manifest IS NOT NULL AND failure_dossier IS NULL
       AND state = 'QUARANTINED' AND completed_stage = 'SALVAGE_REGISTRATION')
    ),
    FOREIGN KEY (candidate_id, candidate_artifact_id, attempt_id, package_id,
                 revision_id, submitted_head)
      REFERENCES candidates(id, bundle_artifact_id, attempt_id, package_id,
                            revision_id, candidate_commit),
    FOREIGN KEY (verification_run_id, candidate_id)
      REFERENCES verification_runs(id, candidate_id),
    FOREIGN KEY (manifest_staging_id, id)
      REFERENCES submission_manifest_staging(id, submission_id),
    FOREIGN KEY (manifest_staging_id, id, submission_kind, attempt_id, package_id,
                 revision_id, candidate_id, candidate_artifact_id,
                 verification_run_id, state, completed_stage, submitted_head,
                 manifest_digest)
      REFERENCES submission_manifest_staging(
        id, submission_id, submission_kind, attempt_id, package_id,
        revision_id, candidate_id, candidate_artifact_id,
        verification_run_id, terminal_outcome, completed_stage, submitted_head,
        manifest_digest)
);

CREATE TRIGGER submissions_are_immutable
BEFORE UPDATE OR DELETE ON submissions
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

ALTER TABLE work_packages
  ADD CONSTRAINT work_packages_accepted_submission_fk
  FOREIGN KEY (accepted_submission_id, id)
  REFERENCES submissions(id, package_id);
```

`CandidateArtifactInit` 先预留 `candidate_id` 和 upload row；chunk 写入对象存储不构成正式交付。`CompleteCandidateArtifact` 可先 CAS 到非终态 `ASSEMBLING`，但只有在每块/总 digest、大小、OID/tree 和对象可读性全部复核后，并再次以数据库时间确认作者 Lease 有效，才能 CAS 到 `COMPLETE`；验证失败进入 `REJECTED` 并短期保留隔离字节，显式 salvage/调查接管则进入 `QUARANTINED`。一旦 Complete，触发器拒绝其任何 UPDATE/DELETE。`RecordCandidate` 使用包含 Attempt/Lease/token/OID/tree/Author Evidence 的复合外键和同事务重读，把该 Artifact 绑定到同 ID Candidate；Candidate 到 Attempt/Lease/revision 的复合外键关闭其余 lineage。Lease 在 complete/record 之前失效时，不得创建 Candidate；上传字节只能经独立 salvage API 登记或按 TTL 清理。

每个 Candidate 由唯一约束绑定一个正式 VerificationRun。Verifier/Reviewer/Runner 先写不可变 `verification_stage_results`、`review_reports/review_findings`、`reproduction_results` 与 `criterion_results`，再在同一事务用 run version CAS 推进状态；失败终结时把原阶段写入 `terminal_stage`。Review 聚合必须验证 `reviewer_actor_id != attempts.executor_id`，高风险包还要验证模型家族数量；Reproducing 聚合必须绑定精确 Candidate Head。原始 job retry 保存在签名 Evidence，关系表只接受固定输入下的最终聚合事实，因此不通过 UPDATE 改写失败记录。

Candidate Submission 的 `state/completed_stage` 来自签名 Manifest 的 `terminal_outcome/completed_stage`。Provenance 早期失败允许 `tested_head/reviewed_head` 为 NULL，但必须有 Failure Dossier；禁止编造未执行的 test/review。PASS 才要求完整三 Head、criteria、Review、Clean Reproduction 和 Evidence Bundle。

数据库 CHECK 可以约束 Candidate Submission 自身三 Head 在 PASS 时相等：

```sql
ALTER TABLE submissions ADD CONSTRAINT pass_requires_same_head CHECK (
  state <> 'PASS'
  OR (submitted_head IS NOT NULL AND tested_head IS NOT NULL
      AND reviewed_head IS NOT NULL
      AND submitted_head = tested_head AND tested_head = reviewed_head)
);

ALTER TABLE submissions ADD CONSTRAINT early_terminal_has_no_fabricated_head CHECK (
  (completed_stage <> 'PROVENANCE_CHECK'
   OR (tested_head IS NULL AND reviewed_head IS NULL))
  AND (completed_stage <> 'REVIEWING' OR tested_head IS NULL)
);
```

它不能跨表证明该 Head 就是封存的 Candidate。`FinalizeVerification` 必须按固定锁序锁定 Attempt、CandidateArtifact、Candidate、terminal VerificationRun 与预登记 Manifest fact，重新验证 `submitted_head = candidates.candidate_commit = attempts.candidate_commit`，并在同一事务检查 hard criteria、finding、签名与结果状态，然后一次性插入不可变 Submission；不能相信客户端传来的 `overall_pass=true`。ProvenanceCheck/Reviewing/Reproducing 只更新 VerificationRun 和插入不可变验收事实，不创建或修改 Submission。

Manifest 使用明确的两阶段绑定，禁止持有领域行锁时访问对象存储：

1. **Phase A / stage**：Coordinator 读取已经终态且不可再修改的 run/facts，预分配 Submission UUID，计算 `fact_set_digest`，组装并签署 canonical Manifest；在数据库事务外上传到内容寻址 URI、HEAD 校验后，以短事务插入不可变 `submission_manifest_staging`。上传成功但 staging 失败只产生可 GC 的孤儿对象。
2. **Phase B / bind**：`FinalizeVerification` 取得幂等回执锁和领域行锁，重新计算同一 `fact_set_digest`，检查 staging 未过期、四 ID、结论、阶段、URI/digest/signature 全匹配，然后以复合外键插入 Submission 并更新 Attempt/Package。事务失败时 staging 仍可安全重用；事务内没有 HEAD/PUT/签名服务调用。

### 4.4 Integration 与 L5 Receipt

```sql
CREATE TABLE integrations (
    id uuid PRIMARY KEY,
    package_id uuid NOT NULL REFERENCES work_packages(id),
    submission_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    candidate_artifact_id uuid NOT NULL,
    verification_run_id uuid NOT NULL,
    current_relay_ticket_id uuid NOT NULL,
    submission_state text NOT NULL CHECK (submission_state = 'PASS'),
    state text NOT NULL CHECK (state IN (
      'QUEUED','SYNTHESIZING','L5_RUNNING','CONFLICT',
      'PASS','FAILED','MERGED','CANCELLED'
    )),
    target_ref text NOT NULL,
    target_before text NOT NULL,
    integration_head text,
    l5_tested_head text,
    target_after text,
    l5_evidence_digest bytea CHECK (
      l5_evidence_digest IS NULL OR octet_length(l5_evidence_digest) = 32),
    receipt_digest bytea CHECK (
      receipt_digest IS NULL OR octet_length(receipt_digest) = 32),
    receipt_signature jsonb,
    rollback_ref text,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    event_seq bigint NOT NULL DEFAULT 0 CHECK (event_seq >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (submission_id, target_before),
    UNIQUE (id, package_id),
    UNIQUE (id, submission_id, package_id, candidate_id,
            candidate_artifact_id, verification_run_id),
    FOREIGN KEY (submission_id, package_id, candidate_id,
                 candidate_artifact_id, verification_run_id, submission_state)
      REFERENCES submissions(id, package_id, candidate_id,
                             candidate_artifact_id, verification_run_id, state),
    CHECK (integration_head IS NULL OR l5_tested_head IS NULL
           OR integration_head = l5_tested_head),
    CHECK (state <> 'MERGED' OR (
      integration_head IS NOT NULL
      AND l5_tested_head = integration_head
      AND target_after = integration_head
      AND l5_evidence_digest IS NOT NULL
      AND receipt_digest IS NOT NULL
      AND receipt_signature IS NOT NULL
      AND rollback_ref IS NOT NULL
    ))
);

CREATE INDEX integrations_queue_idx
    ON integrations (created_at, id)
    WHERE state IN ('QUEUED','SYNTHESIZING','L5_RUNNING');

CREATE TABLE relay_tickets (
    id uuid PRIMARY KEY,
    protocol_key text NOT NULL UNIQUE,
    relay_id uuid NOT NULL,
    ticket_version integer NOT NULL CHECK (ticket_version > 0),
    supersedes_ticket_id uuid REFERENCES relay_tickets(id),
    submission_id uuid NOT NULL,
    submission_state text NOT NULL CHECK (submission_state = 'PASS'),
    package_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    candidate_artifact_id uuid NOT NULL,
    verification_run_id uuid NOT NULL,
    state text NOT NULL CHECK (state IN (
      'ISSUED','CLAIMED','RELAYED','REJECTED','RETRYABLE_FAILURE',
      'EXPIRED','SUPERSEDED'
    )),
    claim_generation bigint NOT NULL DEFAULT 0 CHECK (claim_generation >= 0),
    relay_job_version bigint NOT NULL DEFAULT 0 CHECK (relay_job_version >= 0),
    destination_ref text NOT NULL,
    expected_old_oid text NOT NULL,
    artifact_capability_digest bytea NOT NULL CHECK (
      octet_length(artifact_capability_digest) = 32),
    ticket_digest bytea NOT NULL UNIQUE CHECK (octet_length(ticket_digest) = 32),
    signature jsonb NOT NULL,
    expires_at timestamptz NOT NULL,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (expires_at > created_at),
    UNIQUE (id, ticket_version),
    UNIQUE (id, ticket_version, expires_at),
    UNIQUE (id, submission_id, package_id, candidate_id,
            candidate_artifact_id, verification_run_id),
    UNIQUE (submission_id, relay_id, ticket_version),
    FOREIGN KEY (submission_id, package_id, candidate_id,
                 candidate_artifact_id, verification_run_id, submission_state)
      REFERENCES submissions(id, package_id, candidate_id,
                             candidate_artifact_id, verification_run_id, state)
);

CREATE UNIQUE INDEX relay_tickets_one_live_idx
    ON relay_tickets (submission_id, relay_id)
    WHERE state IN ('ISSUED','CLAIMED','RETRYABLE_FAILURE');
CREATE INDEX relay_tickets_claim_idx
    ON relay_tickets (created_at, id)
    WHERE state IN ('ISSUED','RETRYABLE_FAILURE');

-- /started 的每次成功领取都创建历史 claim；不把 bearer 明文写入数据库。
CREATE TABLE relay_claims (
    claim_id uuid PRIMARY KEY,
    claim_request_id uuid NOT NULL,
    ticket_id uuid NOT NULL,
    ticket_version integer NOT NULL CHECK (ticket_version > 0),
    claim_generation bigint NOT NULL CHECK (claim_generation > 0),
    claim_token_hash bytea NOT NULL CHECK (octet_length(claim_token_hash) = 32),
    claim_holder_id uuid NOT NULL,
    claim_expires_at timestamptz NOT NULL,
    ticket_expires_at timestamptz NOT NULL,
    relay_job_version bigint NOT NULL CHECK (relay_job_version > 0),
    state text NOT NULL CHECK (state IN (
      'ACTIVE','COMPLETED','EXPIRED','REVOKED','SUPERSEDED'
    )),
    result_digest bytea CHECK (
      result_digest IS NULL OR octet_length(result_digest) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    CHECK (claim_expires_at > created_at),
    CHECK (claim_expires_at <= ticket_expires_at),
    CHECK ((state = 'COMPLETED') = (completed_at IS NOT NULL)),
    CHECK (state <> 'COMPLETED' OR result_digest IS NOT NULL),
    UNIQUE (ticket_id, ticket_version, claim_request_id),
    UNIQUE (ticket_id, ticket_version, claim_generation),
    FOREIGN KEY (ticket_id, ticket_version, ticket_expires_at)
      REFERENCES relay_tickets(id, ticket_version, expires_at)
);

CREATE UNIQUE INDEX relay_claims_one_active_idx
    ON relay_claims (ticket_id, ticket_version)
    WHERE state = 'ACTIVE';
CREATE INDEX relay_claims_expiry_idx
    ON relay_claims (claim_expires_at, claim_id)
    WHERE state = 'ACTIVE';

ALTER TABLE integrations
  ADD CONSTRAINT integrations_current_relay_ticket_fk
  FOREIGN KEY (current_relay_ticket_id, submission_id, package_id, candidate_id,
               candidate_artifact_id, verification_run_id)
  REFERENCES relay_tickets(id, submission_id, package_id, candidate_id,
                           candidate_artifact_id, verification_run_id)
  DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE work_packages
  ADD CONSTRAINT work_packages_integrated_integration_fk
  FOREIGN KEY (integrated_integration_id, id)
  REFERENCES integrations(id, package_id);
```

`EnqueueIntegration` 是创建首张正式 Relay Ticket 的唯一入口。它只读取已经持久化的目标引用投影，不在事务内访问 Git；Relay 随后仍须用 `expected_old_oid` 做 destination CAS。Integration 与 Ticket 分别用复合外键固定到同一条 PASS Submission/Package/Candidate/CandidateArtifact/VerificationRun lineage，Integration 的 current-ticket 外键再证明二者一致。Ticket 的版本域是 `(submission_id, relay_id)`，不是 Integration：尚未 `RELAYED` 的票过期重签时，必须在一个事务中把旧票置 `SUPERSEDED`、把其 ACTIVE `relay_claims` 置 `SUPERSEDED`、以连续 `ticket_version` 插入新票、设置 `supersedes_ticket_id` 并更新当前 Integration 的指针。partial unique index 保证每个 Submission/Relay 最多一张 live Ticket、同一 Ticket version 最多一个 ACTIVE claim，Ticket 与 claim 历史永久保留。目标分支前移发生在 Candidate 已经 Relay 之后；新建的 Integration 复用同一条 `RELAYED` Ticket/任务分支 Receipt，不重签 Ticket、不重复推送 Candidate。

`IntegrationHead` 通常不同于 CandidateHead。`MarkIntegrated` 先做非锁定关系解析，再按全局顺序锁定 Package、Candidate、Submission、Integration，证明 `l5_tested_head = integration_head = target_after`，保存第 06 文档定义的签名 Receipt 和 rollback ref，再在同一事务设置 `work_packages.integrated_integration_id/integrated_commit`。`target_before` 相同的重复入队由唯一约束和幂等回执消除；目标前移则创建新的 Integration row，不覆盖旧失败/冲突记录。终态 `FAILED` 必须由 `ReportIntegrationFailed` 同事务投影到 Package 的 `REWORK_READY`/`FAILED`，不得留下永久 `INTEGRATING`。

### 4.5 Obligation

```sql
CREATE TABLE obligations (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    kind text NOT NULL,
    subject_type text NOT NULL,
    subject_id uuid NOT NULL,
    dedup_key text NOT NULL,
    state text NOT NULL CHECK (state IN (
      'PENDING','CLAIMED','EXECUTING','FULFILLED','RETRY_SCHEDULED',
      'ESCALATED','FAILED','CANCELLED'
    )),
    due_at timestamptz NOT NULL,
    claimed_by uuid,
    claim_token uuid,
    claim_until timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 100),
    escalation_level smallint NOT NULL DEFAULT 0 CHECK (escalation_level BETWEEN 0 AND 20),
    payload jsonb NOT NULL,
    fulfillment_evidence bytea CHECK (
      fulfillment_evidence IS NULL OR octet_length(fulfillment_evidence) = 32),
    version bigint NOT NULL DEFAULT 0,
    event_seq bigint NOT NULL DEFAULT 0,
    last_error jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE UNIQUE INDEX obligations_active_dedup_idx
    ON obligations (project_id, dedup_key)
    WHERE state IN ('PENDING','CLAIMED','EXECUTING','RETRY_SCHEDULED');
CREATE INDEX obligations_due_idx
    ON obligations (due_at, id)
    WHERE state IN ('PENDING','RETRY_SCHEDULED');
CREATE INDEX obligations_claim_recovery_idx
    ON obligations (claim_until, id)
    WHERE state IN ('CLAIMED','EXECUTING');
```

`dedup_key` 示例：`verify:{candidate_id}:run:{verification_run_id}`、`expire:{lease_id}:g{token}`、`stall:{attempt_id}:p{semantic_progress_seq}`。这样相同事实的重复事件不会生成义务风暴，而真正的新进展会产生新 key。

### 4.5 Domain events、Outbox、Inbox 与命令回执

```sql
CREATE TABLE domain_events (
    event_id uuid PRIMARY KEY,
    aggregate_type text NOT NULL,
    aggregate_id uuid NOT NULL,
    aggregate_seq bigint NOT NULL CHECK (aggregate_seq > 0),
    event_type text NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version > 0),
    actor_id uuid NOT NULL,
    correlation_id uuid NOT NULL,
    causation_id uuid,
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    payload jsonb NOT NULL,
    UNIQUE (aggregate_type, aggregate_id, aggregate_seq)
);

CREATE INDEX domain_events_cursor_idx ON domain_events (occurred_at, event_id);
CREATE INDEX domain_events_correlation_idx ON domain_events (correlation_id, occurred_at, event_id);

CREATE TRIGGER domain_events_are_append_only
BEFORE UPDATE OR DELETE ON domain_events
FOR EACH ROW EXECUTE FUNCTION reject_immutable_row_change();

CREATE TABLE outbox (
    id uuid PRIMARY KEY,
    event_id uuid NOT NULL UNIQUE REFERENCES domain_events(event_id),
    topic text NOT NULL,
    partition_key text NOT NULL,
    payload jsonb NOT NULL,
    state text NOT NULL DEFAULT 'PENDING'
      CHECK (state IN ('PENDING','PUBLISHING','PUBLISHED','DEAD')),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    locked_by uuid,
    locked_until timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    published_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX outbox_pending_idx
    ON outbox (available_at, id)
    WHERE state IN ('PENDING','PUBLISHING');

CREATE TABLE inbox_messages (
    consumer text NOT NULL,
    message_id uuid NOT NULL,
    payload_hash bytea NOT NULL CHECK (octet_length(payload_hash) = 32),
    processed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    result jsonb,
    PRIMARY KEY (consumer, message_id)
);

-- 仅保存 AEAD ciphertext；明文 claim token 从不进入 PostgreSQL、日志或 trace。
CREATE TABLE sensitive_response_envelopes (
    id uuid PRIMARY KEY,
    key_id text NOT NULL,
    nonce bytea NOT NULL,
    ciphertext bytea,
    plaintext_digest bytea NOT NULL CHECK (octet_length(plaintext_digest) = 32),
    expires_at timestamptz NOT NULL,
    destroyed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (expires_at > created_at),
    CHECK ((ciphertext IS NULL) = (destroyed_at IS NOT NULL))
);

CREATE TABLE command_receipts (
    actor_id uuid NOT NULL,
    idempotency_key text NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    command_type text NOT NULL,
    response_status integer NOT NULL,
    -- token-free public body；敏感字段仅由下方 envelope 在回放时临时拼回。
    response_body jsonb NOT NULL,
    response_digest bytea NOT NULL CHECK (octet_length(response_digest) = 32),
    sensitive_response_ref uuid REFERENCES sensitive_response_envelopes(id),
    sensitive_response_expires_at timestamptz,
    resource_version bigint,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    replay_until timestamptz NOT NULL,
    CHECK ((sensitive_response_ref IS NULL) =
           (sensitive_response_expires_at IS NULL)),
    PRIMARY KEY (actor_id, idempotency_key)
);

CREATE INDEX command_receipts_replay_idx ON command_receipts (replay_until);
CREATE INDEX sensitive_response_envelopes_expiry_idx
    ON sensitive_response_envelopes (expires_at, id)
    WHERE ciphertext IS NOT NULL;
```

命令回执与业务变更同一事务写入，因此不需要持久化 `PROCESSING` 状态：事务崩溃会整体回滚；并发相同 key 依靠事务级 advisory lock 或插入占位行协调。建议使用 `pg_advisory_xact_lock(hashtextextended(actor_id::text || ':' || key, 0))`，取锁后再查 receipt。

`replay_until` 只表示保证原响应 body 可直接回放的期限，不表示可以删除去重事实。到期后应把响应归档或缩成 tombstone，保留 `actor_id`、key、request hash、command type 和 `response_digest`；再次收到相同 key 返回 `AF_IDEMPOTENCY_RESULT_EXPIRED`，不得重新执行命令。

`/started` 的 claim bearer 属于敏感回执：`response_body` 只保存不含 token 的公开字段，token 用进程内 AEAD 数据密钥加密为 `sensitive_response_envelopes`，receipt 只保存 ref/digest/expiry。envelope TTL 必须不晚于 `claim_expires_at`；回放时先命中 receipt，再在数据库事务结束后临时解密并校验完整 wire `response_digest`。envelope 到期/销毁后，相同 key/hash 返回 `AF_IDEMPOTENCY_RESULT_EXPIRED`，不得创建新 claim。生产若使用远程 KMS，只允许在事务外预取/解封数据密钥；事务内不得调用 KMS。日志、trace、审计事件和普通 JSONB 只记录 token hash/envelope ref，禁止记录明文、ciphertext 或可用 bearer。

## 5. 写命令统一处理模板

### 5.1 HTTP 入口

每个写请求必须提供：

```text
Authorization: Bearer <node-or-agent-token>
Idempotency-Key: 1..128 printable ASCII
If-Match: "<aggregate-version>"       # 更新命令必须
X-Correlation-Id: <uuid>              # 可省略，由服务器生成并回传
X-Causation-Id: <uuid>                # 内部派生命令必须
Content-Type: application/json
```

响应返回：

```text
ETag: "<new-version>"
X-Correlation-Id: <uuid>
X-Event-Cursor: <occurred_at>/<event_id>
```

### 5.2 处理伪代码

```rust
async fn execute<C: Command>(ctx: RequestContext, cmd: C) -> ApiResult<C::Output> {
    validate_headers_and_body(&ctx, &cmd)?;
    // 哈希必须覆盖 method、规范化 route、body、If-Match 和影响语义的请求头。
    let request_hash = canonical_request_hash(&ctx, &cmd)?;
    let mut tx = uow_factory.begin(cmd.isolation()).await?;

    tx.lock_idempotency_key(ctx.actor_id, &ctx.idempotency_key).await?;
    if let Some(receipt) = tx.find_receipt(ctx.actor_id, &ctx.idempotency_key).await? {
        if receipt.request_hash != request_hash {
            return Err(ApiError::idempotency_key_reused());
        }
        tx.rollback().await?;
        return receipt.replay_or_idempotency_result_expired();
    }

    authorizer.authorize(&mut tx, &ctx.actor, &cmd).await?;
    let locked_facts = cmd.load_and_lock(&mut tx).await?;
    let semantic_result = async {
        assert_expected_version(ctx.if_match, locked_facts.version())?;

        // 纯领域函数，不做网络 I/O。
        let decision = cmd.decide(locked_facts, tx.server_now().await?)?;

        decision.persist_projection_with_cas(&mut tx).await?;
        tx.append_events(&decision.events).await?;
        tx.enqueue_outbox(&decision.outbox_messages()).await?;
        tx.create_obligations(&decision.obligations).await?;
        Ok::<_, ApiError>(decision)
    }.await;

    match semantic_result {
        Ok(decision) => {
            let response = C::Output::from_decision(&decision);
            tx.store_receipt(CommandReceipt::success(
                ctx, request_hash, &response, decision.new_version,
            )).await?;
            tx.commit().await?;
            Ok(response)
        }
        Err(err) if err.is_stable_client_outcome() => {
            // 领域拒绝也固定首次结果；此分支没有业务 projection/event 变化。
            tx.store_receipt(CommandReceipt::rejection(ctx, request_hash, &err)).await?;
            tx.commit().await?;
            Err(err)
        }
        Err(err) => {
            // 数据库/网络/进程类失败不固化，允许相同 key 安全重试。
            tx.rollback().await?;
            Err(err)
        }
    }
}
```

`validate_headers_and_body` 必须先完成 bearer/mTLS 认证并得到不可伪造的 `actor_id`；回执命中只跳过已经提交命令的当前 Lease/capability 再判定，绝不跳过身份认证。`CommandReceipt::success` 对敏感响应必须生成 token-free `response_body`、完整 `response_digest` 和 AEAD envelope ref；回放先结束数据库事务，再临时解密，避免在持锁事务内调用外部密钥服务。

禁止事项：

- 在事务中调用模型、Git、对象存储、Webhook 或 NATS；
- CAS 更新影响 0 行后再用最后写入获胜的方式覆盖；
- 遇到 `DomainError::StaleVersion` 自动读取并替用户重做语义命令；
- 在 handler 中从多个 repository 各开一个事务；
- 先返回 2xx，再异步尝试写领域状态。

## 6. 关键命令事务

### 6.1 `ClaimPackage`

请求包含 `package_id`、`revision_id`、`executor_id`、`node_id`、预检摘要和 `expected_package_version`。线性化点是包含 WorkPackage CAS、Attempt 创建与 Active Lease 插入的数据库事务提交；未提交的行更新不是外部可观察结果。

```text
BEGIN;
  锁幂等键；命中回执则原样返回；
  SELECT package FOR UPDATE；
  校验 state in (OFFERED, REWORK_READY)、revision、依赖、预算、能力；
  非锁定查询该 revision 的 ACTIVE lease 元数据；
  若存在：按 Attempt -> Lease 的顺序锁行并重新校验；
  若锁定后确认 expires_at <= clock_timestamp()：
      将旧 lease -> EXPIRED，旧 attempt -> LOST，写对应事件；
  若仍存在 ACTIVE lease：返回 DomainError::PackageNotClaimable；
  校验 attempts_started < max_attempts；
  next_token = checked_add(next_fencing_token, 1)，溢出立即失败并告警；
  创建 Attempt(CREATED -> LEASED)；
  创建 Lease(ACTIVE, token=next_token, expires_at=db_now+ttl)；
  更新 package:
      state=ACTIVE,
      active_attempt_id=attempt_id,
      attempts_started=attempts_started+1,
      next_fencing_token=next_token,
      version=version+1
    WHERE id=? AND version=expected；
  写事件、Outbox、receipt；
COMMIT;
```

所有冲突响应都不得泄露其他 Worker 的凭据；可以返回 `retry_after_ms` 和市场状态。

### 6.2 `RenewLease`

续租使用单条 CAS，不能先 SELECT 判断再无条件 UPDATE：

```sql
WITH db_now AS (SELECT clock_timestamp() AS t)
UPDATE leases
SET expires_at = LEAST(
      max_expires_at,
      db_now.t + make_interval(secs => $requested_ttl_seconds)
    ),
    version = version + 1,
    event_seq = event_seq + 1,
    updated_at = db_now.t
FROM db_now
WHERE id = $lease_id
  AND attempt_id = $attempt_id
  AND holder_node_id = $node_id
  AND fencing_token = $token
  AND version = $expected_version
  AND state = 'ACTIVE'
  AND expires_at > db_now.t
  AND max_expires_at > db_now.t
RETURNING expires_at, version, event_seq;
```

0 行时在同一事务内读取最小必要字段以区分 `DomainError::StaleVersion`、`DomainError::StaleLease`、`DomainError::LeaseExpired`，HTTP 边界分别映射到稳定的 `AF_*` 错误码。失败的续租不得写 `LeaseRenewed`。成功续租只证明 liveness，不更新 `last_semantic_progress_at`。

### 6.3 `ReportProgress` / `CreateCheckpoint`

事务先通过非锁定查询解析 Attempt/Lease 关系，再按 `Attempt -> Lease` 顺序加锁并检查 token/到期，避免与 `Package -> Attempt -> Lease` 的提交事务形成反向死锁：

1. 插入 `(attempt_id, client_progress_id)`；冲突时返回首次接收结果；
2. 判断是否是新的语义进度；
3. 只有语义进度才 CAS 增加 `semantic_progress_seq` 和时间；
4. checkpoint 的 digest/URI 必须先由对象存储上传流程完成，数据库只登记已经可 HEAD 验证的不可变对象；
5. 对象存在性复查由异步 obligation 完成，API 事务内不访问对象存储。

当同一 digest 重复登记，返回已有 checkpoint，不增加序号。

### 6.4 `RecordCandidate`

作者交付与迟到 salvage 是两个 endpoint。正式 Candidate 登记步骤：

```text
锁 package -> attempt -> lease；
验证 attempt 是 active_attempt、state=LOCAL_VERIFY；
验证 lease ID、holder、token、state、expires_at > db_now；
锁定已 COMPLETE 的 Candidate Artifact，验证预留 candidate ID、package hash、revision、base commit、candidate commit、tree hash、Bundle digest 和 Author Evidence；
以预留 ID 插入不可变 Candidate 并绑定 Artifact；创建 VerificationRun(QUEUED)；
Attempt -> CANDIDATE；Package -> VERIFYING；Lease -> RELEASED；
创建 VerifyCandidate obligation；
写事件、Outbox、receipt；提交。
```

如果 token 已被替代/撤销，正式 endpoint 返回 `AF_LEASE_STALE`；若仍是当前 Lease 但服务器时间已过期则返回 `AF_LEASE_EXPIRED`。两者都不创建 Candidate/VerificationRun。Worker 可以调用 `/salvage-bundles` 登记已上传的隔离制品，后者只创建 `QUARANTINED` salvage Submission，永不改变 WorkPackage 状态。

独立 Verifier/Reviewer 随后按 CAS 推进 VerificationRun，并驱动 Attempt 的 `CANDIDATE -> ISOLATED_REVIEW -> CLEAN_REPRODUCE`；这些中间事实不能创建半成品 Submission。

### 6.5 `FinalizeVerification`

只允许受认证 Verification Coordinator 调用。请求携带 Phase A 已登记的 `manifest_staging_id`；对象上传、HEAD 与签名已在本数据库事务开始前完成：

1. 先非锁定解析 immutable lineage，再按 `package -> attempt -> candidate_artifact -> candidate -> verification_run -> submission_manifest_staging` 加锁；
2. 校验 run 已为 `PASS`、`FAIL` 或 `INCONCLUSIVE`，且同一 run 尚无 Submission；
3. PASS 路径从数据库读取该 revision 的全部 hard acceptance criteria 并做集合相等检查；FAIL/INCONCLUSIVE 的 `completed_stage` 必须等于 run 的 `terminal_stage`，并只读取截至该阶段已产生的不可变事实；
4. 始终检查 Candidate/Bundle 来源；PASS 再强制检查三 Head、clean reproduction、findings 和完整 Evidence；早期失败强制检查 stage-aware Failure Dossier，未执行字段保持 absent/NULL；
5. 从锁定事实重新计算 `fact_set_digest`，并验证 staging 的 Submission ID、四 lineage ID、结论、阶段、Manifest digest/signature、`expires_at > db_now` 全匹配；本步骤不得访问对象存储；
6. 一次性插入对应终态的不可变 Submission；Attempt 先到 `SUBMITTED`，再到 `PASSED` 或 `REJECTED`；
7. 清空 `active_attempt_id`；PASS：Package -> `ACCEPTED` 并固定 `accepted_submission_id`；FAIL/INCONCLUSIVE：在对应代码返工/验收基础设施预算内则 -> `REWORK_READY`，否则 -> `FAILED`；基础设施型 INCONCLUSIVE 单独计量，不记为 Executor 质量失败；
8. PASS 只创建去重键为 `integration-enqueue:{submission_id}` 的 `RequestIntegrationEnqueue` obligation；FAIL/INCONCLUSIVE 创建 Rework/升级 obligation；同事务写事件、Outbox、回执。此处不创建 Integration、Relay Ticket 或 Relay obligation。

Coordinator 不接受客户端直接选择 `PASS`，也不更新已有 Submission。响应丢失时以 run ID/幂等键查询已创建的终态记录。

### 6.6 `EnqueueIntegration`

只允许 Integration Coordinator 或确定性 `RequestIntegrationEnqueue` obligation handler 调用：

1. 首次入队时在事务外分配 Integration/Ticket UUID，并基于已持久化的 Package target/base 投影构造该 `(submission_id, relay_id)` 的 `ticket_version=1` Ticket；MVP 使用进程内服务签名密钥，不调用远程 KMS。若以后使用远程签名器，必须增加与 Submission Manifest 相同的预登记阶段；
2. 进入事务，先命中幂等回执，再非锁定解析 Submission lineage，按 `package -> candidate_artifact -> candidate -> verification_run -> submission` 加锁；
3. 要求 Package=`ACCEPTED` 且 `accepted_submission_id` 命中；Submission=`PASS`/`CANDIDATE_READY`；Artifact=`COMPLETE`；run=`PASS`；所有复合 lineage、三 Head、Manifest 签名仍匹配；
4. 要求该 Submission/target_before 尚无 Integration，且该 Submission/Relay 尚无 Ticket 历史；重新计算 Ticket digest，并校验 capability 只允许读取该 Artifact、写服务器生成的 task ref；目标前移后的再集成走独立 requeue 命令并复用已有 `RELAYED` Ticket，不重新调用本入口；
5. 原子插入 `Integration(QUEUED,current_relay_ticket_id=ticket_id)` 和 `RelayTicket(ISSUED,ticket_version=1)`，创建去重键 `relay:{integration_id}:ticket:{ticket_id}:v1` 的 `RelayCandidate` obligation，把 Package CAS 为 `INTEGRATING`；
6. 写事件、Outbox、命令回执并提交。Git fetch、Bundle 下载、push 和 branch HEAD 校验全部由事务后的 Relay job 完成。

因此完整顺序唯一为：terminal PASS Submission → `EnqueueIntegration` → Ticket/Relay → Merge Queue；不存在 PASS 前 Relay，也不存在“先 Relay 可解析再入队”的循环前置条件。

Ticket 领取不是作者 Lease。`started` 用服务器时间和 Relay service identity 创建一条 `relay_claims` 历史行，并在 Ticket 行上原子递增 `claim_generation/relay_job_version`；claim 行固定递增后的两个值。它必须先拒绝 `ticket.expires_at <= db_now`，并令 `claim_expires_at = min(db_now + requested_claim_ttl, ticket.expires_at)`；若剩余窗口小于一次安全 Git push 的最小预算则不发 claim。相同 `claim_request_id` 精确回放首次 `claim_id` 与 ACK，不产生新 generation。数据库只保存 token hash、holder 与 expiry。`result` 的 receipt miss 路径必须同时匹配 `ticket_id + ticket_version + claim_id + claim_generation + relay_job_version`，并以服务器时间重新验证 Ticket 和 claim 均未到期。Ticket 过期且尚未 `RELAYED` 时，重签 handler 先锁 Submission、当前 Integration/Ticket/ACTIVE claim，把旧票与旧 claim 置失效，再按 `(submission_id, relay_id)` 插入连续版本并更新 current 指针；旧票、跳号版本或旧 claim 的迟到结果返回 `AF_OBLIGATION_CLAIM_STALE`/Relay 扩展错误，不得更新 Integration。

### 6.6.1 `RequeueTargetMoved`

L5 运行结束到保护分支 CAS 之间若 target 已前移，Integrator 不能复用旧 synthetic Evidence，也不需要重复 Relay 已落到任务分支的 Candidate。它以 service identity、当前 Integration queue claim、旧 Integration version 和幂等键调用本命令：

1. 在事务外取得 Git CAS 的 `expected/actual` 签名失败回执，并重新 fetch 任务 ref；要求任务 ref 仍精确等于已 `RELAYED` Ticket Receipt 的 Candidate；
2. 事务内按 `package -> submission -> old integration -> current relay ticket` 加锁，重新证明 Package 仍为 `INTEGRATING`、旧 Integration 未终态、Ticket=`RELAYED` 且完整 lineage 未撤销；
3. 把旧 Integration 置 `CANCELLED`，原因固定为 `TARGET_MOVED`，保留旧 synthetic Head/Evidence；
4. 以新的 `target_before=actual` 创建 `Integration(QUEUED)`，复用同一 `current_relay_ticket_id`，Package 保持 `INTEGRATING`；
5. 创建新 Integration queue obligation 并写事件、Outbox、回执。不得创建 Relay Ticket/claim，也不得重新 push Candidate。

幂等业务键为 `old_integration_id + actual_target_oid`；并发 target-move 报告受 `UNIQUE(submission_id,target_before)` 与旧 Integration CAS 约束，至多创建一个新 Integration。

### 6.7 `ReportIntegrationFailed`

Relay/Integrator 先用自己的 service identity、限定 Ticket capability、queue lease 与 Integration version CAS 鉴权。瞬态失败只重试同一固定 Ticket，不调用本命令；确定为终态时在一个事务内：

1. 按 `package -> submission -> integration -> relay_ticket -> relay_claim` 加锁，验证 Integration 属于 Package 当前 accepted Submission，且尚未终态；
2. 验证签名 failure receipt、失败分类和 queue claim token，CAS `Integration -> FAILED`；
3. 显式清除 `accepted_submission_id`；仍有代码/验收尝试预算则 `Package -> REWORK_READY` 并创建新 Attempt lineage 的 Rework obligation，否则 `Package -> FAILED` 并升级；
4. 保存失败 Receipt、事件、Outbox 与幂等回执。旧 Submission/Integration 不删除、不重开。

冲突不走本命令，使用 `ReportIntegrationConflict -> REBASE_REQUIRED`。`ReportIntegrationFailed` 与 Package 投影同事务提交，因此不会留下 Integration 已 FAILED 而 Package 永久 INTEGRATING 的可见状态。

### 6.8 Lease 过期回收

每 5 秒运行一次小批扫描，事件驱动唤醒可以缩短延迟，但不影响正确性。为了遵守全局锁顺序，扫描阶段只读取候选 ID，不持有 Lease 行锁：

```sql
SELECT id, package_id, attempt_id
FROM leases
WHERE state = 'ACTIVE' AND expires_at <= clock_timestamp()
ORDER BY expires_at, id
LIMIT 100;
```

随后对每个候选开启短事务，按 `Package -> Attempt -> Lease` 加锁并重新判断 `state='ACTIVE' AND expires_at <= db_now`，再执行：Lease -> EXPIRED、Attempt -> LOST、Package 清空 active attempt 并转 `REWORK_READY` 或 `FAILED`，创建重新悬赏/升级义务。多个实例可能读到同一候选，但只有第一个重新校验成功的事务会产生状态变化；其余事务成为无副作用 no-op。批处理中一个坏数据不得回滚其他 99 个，实际实现逐个事务并限制总并发为 2。

### 6.9 `ApplyPlanPatch`

使用 Serializable 事务：

1. 锁 Project，验证 `base_graph_version == projects.graph_version`；
2. 加载 base graph 和 patch 涉及的 Package；
3. 纯函数验证节点存在、无环、委派/预算/权限、已完成节点不可隐改；
4. `new_version = base + 1`，写 `graph_versions` 和完整/增量边；
5. 更新 Project.graph_version CAS；
6. 重新计算受影响节点 readiness，创建/取消 Offer 与 Obligation；
7. 写事件、Outbox、回执。

Serializable serialization failure 可以安全重试，因为每轮都会重新检查 base version；最终若 base 已变化返回 `AF_GRAPH_VERSION_STALE`，而不是把 patch 自动套到新图。

## 7. Outbox 与事件分发

### 7.1 发布算法

MVP 默认 sink 为进程内事件分发 + 可选 HTTP/Webhook；加入 JetStream 后不改业务事务。

```text
claim transaction:
  SELECT PENDING due rows，或 locked_until 已过期的 PUBLISHING rows
    FOR UPDATE SKIP LOCKED LIMIT 100
  UPDATE state=PUBLISHING, locked_by=instance, locked_until=now+30s, attempts+=1
  COMMIT

for each row outside transaction:
  publish(message_id = outbox.id, partition_key, payload)
  on broker ack: UPDATE -> PUBLISHED
  on failure: UPDATE -> PENDING, available_at=backoff(attempts)
  after max attempts: -> DEAD + create OutboxRepair obligation
```

如果进程在 publish 成功、标记 PUBLISHED 前崩溃，消息会重复。所有内部消费者先插入 `inbox_messages`，相同 `(consumer,message_id)` 只返回首次结果。外部 Worker 消费事件也必须保存 cursor/message ID。

### 7.2 顺序

- 只保证单聚合 `aggregate_seq` 单调；
- partition key 使用 `aggregate_type:aggregate_id`；
- 不保证不同 Package 的墙钟顺序；
- 消费者发现 seq 缺口时暂停该聚合并从 `GET /v1/events` 补齐；
- SSE 是通知/同步接口，不是授权依据。作者侧 Attempt/CandidateArtifact/Candidate mutation 仍须提交当前 author fencing；Verifier/Reviewer/Coordinator/Relay/Integrator 则须提交各自 service capability、job/queue claim 与 version CAS。

### 7.3 SSE cursor

游标为服务器签名的不透明值，内部编码 `(occurred_at,event_id,filters_hash)`。客户端不得拼接时间戳猜测位置。断线重连发送 `Last-Event-ID`；服务端最多保留 24 小时热 SSE 窗口，更旧同步走分页事件 API。

## 8. Obligation Engine 实施

### 8.1 Claim

```sql
WITH due AS (
  SELECT id
  FROM obligations
  WHERE state IN ('PENDING','RETRY_SCHEDULED')
    AND due_at <= clock_timestamp()
  ORDER BY due_at, id
  FOR UPDATE SKIP LOCKED
  LIMIT $batch
)
UPDATE obligations o
SET state = 'CLAIMED',
    claimed_by = $instance_id,
    claim_token = $claim_token,
    claim_until = clock_timestamp() + interval '30 seconds',
    attempt_count = attempt_count + 1,
    version = version + 1,
    updated_at = clock_timestamp()
FROM due
WHERE o.id = due.id
RETURNING o.*;
```

执行器按 `kind` 分派到确定性 action：能直接执行的状态维护在本进程完成；需要模型/Reviewer/Git Relay 的义务转换为专门 WorkPackage 或 Outbox 请求，然后等待回执事件。不得在 scheduler loop 中同步等待模型完成。

### 8.2 重试与升级

退避：`min(5s * 2^(attempt_count-1), 15m) + deterministic_jitter(obligation_id, 0..20%)`。相同义务使用确定性 jitter，便于测试。

| 失败 | 第一次动作 | 达到上限 |
| --- | --- | --- |
| 临时数据库/网络错误 | `RetryScheduled` | `Escalated` 到运维义务 |
| Worker 无人接单 | 扩大候选/调整报价 | 创建规格诊断或 Boss 重规划包 |
| Attempt 无语义进展 | Nudge/Diagnosis | 撤销 Lease、保留 checkpoint、换 Executor |
| 验收基础设施失败 | 同一非终态 run 内以固定输入重领 Evaluation Job | 预算耗尽才 `INCONCLUSIVE` + 运维/重做义务；终结后 MVP 经新 Attempt/Candidate/run 重做，不误判代码失败 |
| Git 冲突 | RebasePackage | Domain/Integrator Boss |

Claim 超时 recovery 每 10 秒扫描，旧 `claim_token` 的迟到完成请求必须返回 `AF_OBLIGATION_CLAIM_STALE`。

## 9. HTTP API 契约

### 9.1 资源和命令 endpoint

| 方法与路径 | 命令 | 主要前置条件 | 成功 |
| --- | --- | --- | --- |
| `POST /v1/projects` | `CreateProject` | tenant 权限 | `201` |
| `POST /v1/projects/{id}/plan-patches` | `ApplyPlanPatch` | `If-Match` graph version | `201` |
| `POST /v1/work-packages` | `CreatePackage` | project active | `201` |
| `POST /v1/work-packages/{id}/revisions` | `CreateRevision` | revision/hash 唯一 | `201` |
| `POST /v1/work-packages/{id}:validate` | `RequestValidation` | Draft/Blocked | `202` |
| `POST /v1/work-packages/{id}:publish` | `PublishPackage` | DoR 全通过 | `200` |
| `GET /v1/market/offers` | 查询匹配 Offer | cursor 分页 | `200` |
| `POST /v1/work-packages/{id}:claim` | `ClaimPackage` | 可认领 | `201` Attempt+Lease |
| `POST /v1/leases/{id}:renew` | `RenewLease` | token+CAS | `200` |
| `POST /v1/leases/{id}:release` | `ReleaseLease` | token+CAS | `200` |
| `POST /v1/attempts/{id}/progress` | `ReportProgress` | 有效 Lease | `202` |
| `POST /v1/attempts/{id}/checkpoints` | `CreateCheckpoint` | 有效 Lease | `201` |
| `POST /v1/attempts/{id}:wait` | `WaitFor` | typed wake condition | `200` |
| `POST /v1/attempts/{id}/candidate-artifacts` | `CandidateArtifactInit` | local verify + 有效 Lease | `201` reserved Candidate+upload |
| `PUT /v1/candidate-artifacts/{id}/chunks/{index}` | `UploadCandidateArtifactChunk` | scoped upload capability + digest + 有效 Lease | `204` idempotent chunk |
| `POST /v1/candidate-artifacts/{id}:complete` | `CompleteCandidateArtifact` | digest/chunks + 有效 Lease | `200` immutable artifact |
| `POST /v1/attempts/{id}/candidates` | `RecordCandidate` | COMPLETE artifact + 有效 Lease | `201` Candidate+run |
| `POST /v1/salvage-bundles` | `RegisterSalvage` | 来源可验证 | `201` quarantine only |
| `POST /v1/verification-runs/{id}:advance` | `AdvanceVerification` | verifier/reviewer role + CAS | `200` |
| `POST /v1/verification-runs/{id}/submission-manifests` | `StageSubmissionManifest` | coordinator role + terminal run；事务外对象上传已完成 | `201` immutable staging fact |
| `POST /v1/verification-runs/{id}:finalize` | `FinalizeVerification` | coordinator role + terminal run + matching unexpired staging | `201` Submission |
| `POST /v1/submissions/{id}:enqueue-integration` | `EnqueueIntegration` | PASS/Accepted + COMPLETE Artifact + exact lineage | `202` Integration+Ticket |
| `POST /v1/integrations/{id}:requeue-target` | `RequeueTargetMoved` | signed target CAS mismatch + current integration claim | `202` new Integration, reused RELAYED Ticket |
| `POST /v1/integrations/{id}:fail` | `ReportIntegrationFailed` | relay/integrator role + terminal failure receipt + queue claim CAS | `200` ReworkReady/Failed |
| `POST /v1/integrations/{id}:complete` | `MarkIntegrated` | relay/integrator role | `200` |
| `GET /v1/events` | 事件补齐 | scope filter | `200` |
| `GET /v1/events/stream` | SSE | signed cursor | `200` |

动作 endpoint 使用冒号是为了明确它们不是 CRUD 覆盖。任何状态字段都不得通过通用 `PATCH` 直接修改。

### 9.2 错误响应

```json
{
  "error": {
    "code": "AF_LEASE_STALE",
    "message": "lease proof is no longer current",
    "retryable": false,
    "correlation_id": "0198...",
    "details": {
      "resource": "lease",
      "current_version": 9
    }
  }
}
```

| 稳定错误码 | HTTP | 可重试 | 含义 |
| --- | ---: | --- | --- |
| `AF_REQUEST_INVALID` | 400 | 否 | Schema/字段非法 |
| `AF_AUTH_REQUIRED` | 401 | 否 | 未认证或 token 无效 |
| `AF_FORBIDDEN` | 403 | 否 | actor 无命令权限 |
| `AF_RESOURCE_NOT_FOUND` | 404 | 否 | 资源不存在或不可见 |
| `AF_IDEMPOTENCY_KEY_REQUIRED` | 400 | 否 | 写命令缺 key |
| `AF_IDEMPOTENCY_KEY_REUSED` | 409 | 否 | 相同 key 对应不同 request hash |
| `AF_IDEMPOTENCY_RESULT_EXPIRED` | 409 | 否 | 去重事实仍在，但原响应已超过回放期；不得重执行 |
| `AF_PRECONDITION_REQUIRED` | 428 | 否 | 更新缺 `If-Match` |
| `AF_VERSION_STALE` | 412 | 读取后重试 | 聚合 CAS 版本过期 |
| `AF_GRAPH_VERSION_STALE` | 412 | 读取后重规划 | PlanPatch base 过期 |
| `AF_TRANSITION_INVALID` | 409 | 否 | 当前状态不允许该命令 |
| `AF_PACKAGE_NOT_READY` | 422 | 修正规格后 | DoR/依赖/预算未满足 |
| `AF_PACKAGE_NOT_CLAIMABLE` | 409 | 是 | 已被认领或状态改变 |
| `AF_ATTEMPT_LIMIT_REACHED` | 409 | 否 | 尝试次数耗尽 |
| `AF_LEASE_STALE` | 409 | 否 | lease/token/holder 不再当前 |
| `AF_LEASE_EXPIRED` | 410 | 否 | 服务器时间已过期 |
| `AF_WAKE_CONDITION_UNSATISFIED` | 409 | 是 | 唤醒事实尚未发生 |
| `AF_SUBMISSION_NOT_ACCEPTABLE` | 422 | 返工 | 验收谓词不成立 |
| `AF_OBLIGATION_CLAIM_STALE` | 409 | 否 | claim token 已被回收 |
| `AF_RATE_LIMITED` | 429 | 是 | 带 `Retry-After` |
| `AF_DEPENDENCY_UNAVAILABLE` | 503 | 是 | DB/对象元数据/Git Relay 暂不可用 |
| `AF_INTERNAL` | 500 | 是 | 未分类瞬态错误，details 不泄密；保持同幂等键做有上限退避 |

`retryable=true` 不表示可以原样重放非幂等副作用；客户端只能用相同 Idempotency-Key 重试同一请求。

本表定义控制平面通用码；AFWP、Submission、Verification 与 Git Relay 的扩展码以文档 02 第 12 节的统一表及对应组件规范为准。所有 wire code 必须使用 `AF_` 前缀并进入共享枚举/契约测试，组件不得私自发明无前缀同义码。

## 10. 认证、授权与输入限制

- Worker Node 和 Executor 身份分开；Node token 只能代表节点，具体命令同时校验 Attempt/Lease holder；
- Verifier、Reviewer、Verification Coordinator、Integrator 使用独立 service role；author token 不得推进或终结 VerificationRun；
- 每个 Package 的权限快照绑定 revision，不能由 Worker 请求扩权；
- 所有 JSON body 默认上限 1 MiB；只有 WorkPackage 发布 route 明确覆盖为 2 MiB，canonical AFWP 仍不得超过 1 MiB，剩余额度用于信封；大证据只传 URI+digest；
- `Idempotency-Key` 1..128 字节，禁止控制字符；
- 验收命令保存 `argv: string[]`，服务器不得拼为 shell 字符串；
- 日志对 bearer token、Git 凭据、signed URL 查询串做结构化脱敏；
- 公开市场查询只返回执行所需摘要，不返回其他节点报价或私有凭据。

## 11. 可观测性

### 11.1 必需指标

```text
agentforge_http_requests_total{route,status_class}
agentforge_http_duration_seconds{route}
agentforge_db_pool_in_use
agentforge_command_conflicts_total{code}
agentforge_lease_active
agentforge_lease_expired_total
agentforge_lease_renew_failures_total{reason}
agentforge_attempt_stalled
agentforge_obligations_due
agentforge_obligation_lag_seconds
agentforge_outbox_pending
agentforge_outbox_oldest_age_seconds
agentforge_outbox_publish_total{result}
agentforge_sse_clients
```

高基数字段（package_id、attempt_id、actor_id）只能进入 trace/log，不做 Prometheus label。每个命令 span 带 correlation ID、command type、aggregate type、结果码和事务耗时，不保存模型 chain-of-thought。

### 11.2 健康检查

- `/health/live`：进程 event loop 可响应，不访问外部依赖；
- `/health/ready`：数据库 `SELECT 1`、迁移版本匹配、后台任务最近 30 秒有 tick；
- Outbox/Obligation 积压不立即让 readiness false，但超过 SLO 发告警；
- 磁盘剩余低于 15% 进入告警，低于 5% 拒绝新 Package/大审计写入但继续处理续租和终结命令。

## 12. 2 核 4 GB MVP 部署预算

### 12.1 单机服务

```text
systemd / container runtime
  reverse proxy（可选，约 50-100 MiB）
  agent-factoryd（目标 RSS < 300 MiB）
  PostgreSQL 16/17/18（目标常态 RSS+cache 约 1.0-1.5 GiB）
  node exporter / OTel collector（约 100-200 MiB）
  OS page cache 与故障余量（至少 1.25 GiB）
```

MVP 不在该主机部署 MinIO、NATS、CI Runner 或模型。Evidence/Git Bundle 使用外部兼容对象存储或局域网 Relay；没有对象存储时只允许小于 10 MiB 的短期制品并设置严格 TTL。

### 12.2 建议配置

| 项目 | MVP 值 | 原因 |
| --- | ---: | --- |
| Tokio worker threads | 2 | 与 CPU 核数一致 |
| SQLx max connections | 16 | HTTP 10、后台任务 4、余量 2 |
| PostgreSQL `max_connections` | 30 | 留管理/迁移/备份连接 |
| PostgreSQL `shared_buffers` | 512 MiB | 避免挤压 OS 和 Rust 进程 |
| PostgreSQL `work_mem` | 4 MiB | 防止并发排序放大内存 |
| HTTP in-flight 写命令 | 64 | Tower semaphore 背压 |
| Obligation 并发 | 2 | 避免抢占 API CPU/DB |
| Outbox batch / 并发 | 100 / 2 | 小事务、可控突发 |
| Lease sweep | 每 5 秒，100 条/批 | 不需要每秒 ping |
| SSE clients | MVP 200 上限 | 超过则建议 Gateway/JetStream |
| API body | 1 MiB | 大制品外置 |
| DB statement timeout | API 5 秒；后台 15 秒 | 防止长事务拖垮连接池 |
| idle-in-transaction timeout | 10 秒 | 及时释放锁 |

PostgreSQL 必须保持 `fsync=on`、`synchronous_commit=on`、`full_page_writes=on`。不得为了跑分牺牲事实账本的耐久性。

### 12.3 发布基线与压力余量

2C4G 的目标不是承载模型推理，而是低频控制请求。以下全部是 `TARGET_NOT_VALIDATED`。发布基线必须在 100 Worker、30 Active Attempt、100,000 Package 下完成 24 小时 soak；下表的 200 Worker/100 Active Lease/30 分钟场景只是压力余量预检，不能替代发布基线。

| 指标 | 目标 |
| --- | --- |
| 注册 Worker | 200 个 |
| 同时 Active Lease | 100 个 |
| 控制 API 稳态 | 20 req/s，持续 30 分钟 |
| 10 秒突发 | 100 req/s，无数据错误；允许 429 |
| `ClaimPackage` p95 | < 250 ms（同地域 DB） |
| `RenewLease` p95 | < 100 ms |
| 到期回收滞后 p99 | < 15 秒 |
| Outbox 正常投递滞后 p99 | < 5 秒 |
| 常态 RSS 合计 | < 2.75 GiB，无 swap storm，至少保留 1.25 GiB OS/page-cache/故障余量 |
| 数据正确性 | 并发测试零双租约、零旧 token 正式写入 |

如果压力测试达到 CPU 80% 或 DB pool 等待 p95 > 100 ms，优先返回 429/背压，不扩张并发。将编译、Review、Git 操作放到 Worker/Relay，使 2C4G 设计大概率可行；只有目标机 24 小时 soak 通过后才能声称达到发布基线。

## 13. 测试矩阵

### 13.1 单元与契约测试

- 领域状态转移和不变量：见文档 01；
- API DTO 使用 JSON Schema golden tests；
- 每个错误码有固定 HTTP/status/body snapshot；
- canonical request hash 对 JSON 键顺序、空白不敏感；
- DB enum/text 到领域 enum 穷尽映射；未知数据库值必须启动失败或显式报错，不得回退默认。

### 13.2 PostgreSQL 集成测试

测试必须连接真实 PostgreSQL，不允许用 SQLite 模拟以下语义：`FOR UPDATE SKIP LOCKED`、partial unique index、Serializable、`clock_timestamp()`、advisory lock。

必测用例：

1. 32 路并发 claim 恰好一个成功；
2. 续租/expire 竞态无 Lease 复活；
3. token g1 过期、g2 授予后，g1 的 receipt-miss progress/checkpoint/CandidateArtifact/Candidate 作者写全部拒绝；已登记 Candidate 的 Coordinator/Relay 使用自身 service capability、job/queue claim 与 CAS 仍可终结 Submission/Integration；
4. g1 的作者命令已提交但 ACK 丢失时，原 actor 以相同 key/hash 重放会先命中 committed receipt、返回首次 2xx/body，且不产生新事件、副作用或 Lease 校验失败；
5. `/started` ACK 丢失后，在 claim/envelope TTL 内相同 actor/key/hash 精确回放原 `claim_id` 与 bearer 且不新增 claim；envelope 到期销毁后返回 `AF_IDEMPOTENCY_RESULT_EXPIRED` 且不创建新 claim，数据库/日志/trace 扫描无明文 token；
6. 相同幂等键并发时只有一个事件和一个 Outbox；
7. 相同 key 不同 body 返回 `AF_IDEMPOTENCY_KEY_REUSED`；
8. Outbox publish/ack 缝隙崩溃产生重复消息但不重复业务副作用；
9. Obligation claim 超时后旧 claim token 无法完成；
10. PlanPatch 并发冲突只有一个 graph version 生效；
11. hard criterion 缺失、Skipped 或 Inconclusive 都不能 PASS；
12. Accepted 包在集成冲突后仍不被标记 Integrated；
13. 任一 Candidate/Artifact/Attempt/Lease/run/Submission 复合 lineage 字段错配均被 FK 或 handler 拒绝；COMPLETE Artifact、terminal run 与验收事实无法 UPDATE/DELETE；
14. Provenance/Review/Reproduction 任一阶段 FAIL/INCONCLUSIVE 都固定 `terminal_stage` 并生成无伪造 Head 的终态 Submission；
15. Manifest 上传后进程崩溃只留下可重用 staging/orphan；Finalize 事务内无对象存储 I/O，相同 staging+幂等键只产生一个 Submission；
16. Ticket 在 `(submission_id, relay_id)` 内重签连续递增 `ticket_version`、旧票 SUPERSEDED 且旧 claim 无法提交；相同 `claim_request_id` 回放原 `claim_id`，并发重签/领取分别最多一张 live Ticket/一个 ACTIVE claim；每条 `claim_expires_at <= ticket.expires_at`，pre-push/result 在任一 expiry 后均拒绝；目标前移的新 Integration 复用已有 RELAYED Ticket；
17. terminal Integration failure 与 Package 离开 INTEGRATING 在同一事务可见。

### 13.3 故障注入

在以下精确位置注入进程终止，再重启验证恢复：

- 状态 UPDATE 前；
- 状态 UPDATE 后但事件 INSERT 前（应整体回滚）；
- 事务 COMMIT 后响应前（相同幂等键返回回执）；
- Outbox publish 后 ACK 前（允许重复）；
- Obligation 外部请求发出后回执前（以外部 idempotency key 查询）；
- Lease 到期 sweeper 锁行后 COMMIT 前（回滚后可再次领取）。

### 13.4 压力与长稳

提供 `control-plane-loadgen`，使用固定 seed 构造 200 Worker：认领、每 20~60 秒续租、每 30~120 秒语义进度、随机 5% 断线、Candidate 登记/验收。测试至少包括：

```bash
cargo run -p agentforge-control-plane --bin control-plane-loadgen -- \
  --workers 200 --active-leases 100 --duration 30m --seed 20260807
```

测试后执行数据库不变量查询：

```sql
-- 不得有一个 revision 多个 Active Lease
SELECT revision_id, count(*) FROM leases
WHERE state='ACTIVE' GROUP BY revision_id HAVING count(*) > 1;

-- Package 与 active Attempt 必须相互对应
SELECT w.id FROM work_packages w
LEFT JOIN attempts a ON a.id=w.active_attempt_id
WHERE w.state='ACTIVE'
  AND (a.id IS NULL OR a.package_id<>w.id OR a.state IN ('LOST','FAILED','CANCELLED'));

-- PASS 必须三 Head 相等
SELECT id FROM submissions
WHERE state='PASS' AND NOT (submitted_head=tested_head AND tested_head=reviewed_head);

-- Outbox 不得缺对应事件
SELECT o.id FROM outbox o LEFT JOIN domain_events e ON e.event_id=o.event_id
WHERE e.event_id IS NULL;
```

所有查询必须返回 0 行。

## 14. 分阶段实现工单

本节的 `CP-*` 是控制平面内部实施切片，不是第二套全局工单编号；排期、依赖和验收追踪必须映射到文档 09 的 `WP-M{里程碑}-{序号}`。

### CP-01：骨架与迁移

交付：workspace、crate 边界、配置、健康检查、迁移 runner、上述核心表。

验收：空库升级成功；重复迁移无变化；旧一版向前升级测试；domain 无 I/O 依赖。

### CP-02：幂等命令框架

交付：request canonical hash、receipt、敏感回执 AEAD envelope、ETag/CAS、统一错误。

验收：20 路相同请求只有一个事件；同 key 不同 body 为 409；commit 后断线可重放响应；claim bearer 不进入 JSONB/log/trace，envelope 销毁后不重做命令。

### CP-03：Package/Graph/Offer

交付：revision 不可变、DoR、PlanPatch、就绪投影、市场列表。

验收：并发 patch；DAG 环拒绝；已发布文档无法 UPDATE；依赖未集成不发布。

### CP-04：Attempt/Lease

交付：claim、renew、release、expiry、fencing guard。

验收：`DB-CON-01..03`；系统时间判断；终态 Lease 不复活。

### CP-05：Progress/Checkpoint/Candidate

交付：语义进度、typed wake condition、正式 Candidate 登记与 salvage 分流。

验收：重复心跳不刷新语义截止；旧 token 的全部作者侧 mutation API 拒绝；独立服务仍以自己的 capability/job lease/CAS 推进；正式与隔离数据不可混淆。

### CP-06：Verification/Integration 投影

交付：不可变 stage/review/finding/reproduction/criterion facts、terminal stage、Manifest 两阶段绑定、Accepted/Integrated 分离、版本化 Relay Ticket/claim 与 Integration failure 投影。

验收：缺项/Skipped/Inconclusive 均不 PASS；早期失败无伪造 Head；三 Head 不同必失败；lineage 错配被拒绝；并发重签/领取分别最多一张 live Ticket/一个 active claim；冲突进入 RebaseRequired，终态失败离开 Integrating。

### CP-07：Outbox/Obligation/SSE

交付：批量 claim、重试、claim 回收、Inbox、事件补齐。

验收：publish/ack 故障注入；重复消息无重复副作用；过期义务会升级而不永久卡住。

### CP-08：2C4G 压力余量预检

交付：loadgen、Grafana/文本指标基线、备份恢复演练报告。

验收：达到章节 12.3 的压力余量目标，30 分钟后不变量 SQL 零结果，重启后 Lease/Outbox/Obligation 均能恢复。最终 24 小时发布长稳由 `WP-M6-*` 系统门禁负责。

## 15. 完成定义

控制平面 MVP 只有在以下条件全部成立时才算可用：

- 所有写命令具备身份、幂等键、CAS 和审计事件；
- PostgreSQL 内不存在绕过状态机的通用状态 PATCH；
- claim、renew、expire、submit 的并发线性化测试通过；
- 所有作者侧 Attempt/CandidateArtifact/Candidate mutation 统一验证当前 author fencing；Verifier、Reviewer、Coordinator、Relay、Integrator 只接受各自 service identity、最小 capability、job/queue claim 与 CAS，并重验已保存 lineage；
- Outbox/Inbox 证明至少一次投递不会重复业务副作用；
- Manifest 对象上传与数据库绑定分阶段，领域锁事务内无对象存储/KMS I/O；
- 每个 Submission/Relay 最多一个 live Relay Ticket、每个 Ticket version 最多一个 active claim；目标前移的新 Integration 只复用已有 RELAYED Ticket，旧票/旧 claim 不能提交结果；
- Obligation 能在没有常驻 Boss 会话时推进到履行、重试或升级；
- `Accepted` 与 `Integrated` 在 API、表、事件和 UI 投影中均清楚分离；
- 2C4G 压力目标达到，且没有通过关闭 PostgreSQL 耐久性换取性能；
- 备份恢复后可从关系状态和事件确定每个 Package、Attempt、Lease、Submission 与 Obligation 的真实位置。
