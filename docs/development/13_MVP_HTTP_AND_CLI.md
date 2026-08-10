# MVP HTTP API 与管理 CLI

本文描述 `v0.1.0-mvp` 的本地/私网试点命令面。它不是公有网络认证方案：当前二进制只允许
loopback bind，并要求 Host/Origin 同源检查和 Project allowlist。

## 1. 启动前置

先显式执行迁移，再启动控制面；生产启动不会自动迁移：

```bash
export AGENTFORGE_DATABASE_URL='host=127.0.0.1 user=agentforge dbname=agentforge'
cargo run --locked -p agentforge-storage-postgres --bin af-migrate

export AGENTFORGE_BIND='127.0.0.1:8080'
export AGENTFORGE_DATABASE_SCHEMA='public'
export AGENTFORGE_CURSOR_HMAC_KEY='<64 lowercase hex characters>'
export AGENTFORGE_LOCAL_PROJECT_IDS='<comma-separated Project UUIDs>'
export AGENTFORGE_LEASE_RECONCILE_SECONDS='5'
export AGENTFORGE_LEASE_RECONCILE_BATCH='100'
cargo run --locked -p agentforge-control-plane --bin agentforge-control-plane
```

要创建的新 Project UUID 必须预先出现在 `AGENTFORGE_LOCAL_PROJECT_IDS`。这是本地试点的显式授权
边界，不是动态多租户目录。

## 2. HTTP 契约

所有写命令必须带：

- `Idempotency-Key`：同一 actor 内的稳定业务键；
- `If-Match: "<version>"`：Claim、Attempt Progress、Renew、Release 以及 Candidate Artifact 的
  Init、Chunk、Complete
  必须提供，创建 Project/Package 禁止提供；
- `Content-Type: application/json`；
- 可选 `X-AgentForge-Command-Id`、`X-AgentForge-Correlation-Id`、
  `X-AgentForge-Causation-Id`；缺省 ID 由服务器生成 UUIDv7。

HTTP actor 从已经验证的 request actor/Project grant 派生，客户端不能在 JSON 中指定 actor。

| 方法 | 路径 | 结果 |
|---|---|---|
| `POST` | `/api/v1/projects` | 创建预授权 Project |
| `POST` | `/api/v1/projects/{project_id}/packages` | 校验 JCS hash 并发布 Package |
| `GET` | `/api/v1/projects/{project_id}/offers?limit=50` | 列出 Offer |
| `POST` | `/api/v1/projects/{project_id}/packages/{package_id}/claim` | 原子创建 Attempt + Lease |
| `GET` | `/api/v1/projects/{project_id}/leases/{lease_id}` | 读取 Lease |
| `POST` | `/api/v1/projects/{project_id}/leases/{lease_id}/renew` | fencing Renew |
| `POST` | `/api/v1/projects/{project_id}/leases/{lease_id}/release` | fencing Release |
| `POST` | `/api/v1/projects/{project_id}/attempts/{attempt_id}/progress` | 一步推进中心 Attempt 并登记证据 |
| `POST` | `/api/v1/projects/{project_id}/attempts/{attempt_id}/candidate-artifacts` | 预留 Candidate 与 Artifact，返回 `201` |
| `PUT` | `/api/v1/projects/{project_id}/candidate-artifacts/{artifact_id}/chunks/{chunk_index}` | 上传一个已声明 chunk，返回 `204` |
| `POST` | `/api/v1/projects/{project_id}/candidate-artifacts/{artifact_id}/complete` | 重组并复算 Bundle，返回 COMPLETE Artifact |

Claim 成功响应除 Attempt/Lease/CAS 版本外，还包含 `granted_at`、`expires_at`、`max_expires_at` 与
`execution`。`execution` 固定 revision、JCS package hash、base commit、Git object format、canonical
AFWP 和 input snapshot；控制面在事务内从 typed revision 行读取并重算摘要，Worker 必须在落本地
Journal 前再次验证。这个内联快照只用于受控 MVP（请求/响应仍受 1 MiB 上限）；大任务后续改为内容
寻址 artifact URI，但字段绑定和摘要语义不变。

路径与 JSON 中重复的 Project/Package/Lease ID 必须完全相同。错误响应固定为：

```json
{
  "code": "AF_LEASE_STALE",
  "message": "the command conflicts with current durable state",
  "retryable": false
}
```

### 2.1 Attempt Progress wire

Progress body 使用严格的 `ReportAttemptProgressInput`，路径中的 Project/Attempt 必须与 body 相同；
`If-Match` 是当前中心 Attempt version。`stage` 只能依次为 `preparing`、`planning`、`implementing`、
`local_verify`，服务端不会接受跳阶段。每一步返回新的 `AttemptProgressView`，其 version 因 phase event 与
semantic-progress event 原子提交而前进 2，`semantic_progress_seq` 前进 1。`evidence_digest` 是阶段事实的
内容摘要，只写入不可变 typed ledger，不会被冒充为 workspace checkpoint。

四步命令都受当前 author Lease holder/fencing/expiry 保护，并采用 receipt-first：ACK 丢失后的原
actor/key/body 返回首次 view；receipt miss 的新请求必须重新通过当前 authority 与 CAS。Worker 必须保存
每一步完整请求与 response，取得最终 `LOCAL_VERIFY` version 后才可初始化 Candidate Artifact。

### 2.2 Candidate Artifact wire

Init body 使用 `InitCandidateArtifactInput` 的严格 JSON 结构，并同时绑定 Attempt、Lease、node、fencing、
Package hash、base/candidate/tree、作者证据摘要、Bundle 总摘要/大小与有序 chunk 摘要。路径中的 Project 和
Attempt 必须与 body 相同；`If-Match` 是当前 Attempt version。

Chunk body 使用 `UploadCandidateArtifactChunkInput`，其中 `content` 是带标准 padding 的 canonical RFC 4648
Base64 字符串，不接受 JSON byte array、无 padding 变体、空内容或解码后超过 1 MiB 的内容。路径中的
Project、Artifact 和 chunk index 必须与 body 相同；服务端复算内容 SHA-256，成功返回空 body 的 `204`。
Worker 可从已持久化请求与 `If-Match` 构造本地 chunk receipt，因为写 chunk 不推进 Artifact version。

Complete 的 `If-Match` 是当前 Artifact version。服务端按声明顺序读取全部 chunk，逐块及整体复算摘要和
大小，再原子执行 `UPLOADING -> ASSEMBLING -> COMPLETE`；缺块返回
`AF_CANDIDATE_ARTIFACT_NOT_COMPLETE/409`，错误修复后可在 Lease 仍有效时用新命令补齐。所有三类命令
均先查询 actor-scoped idempotency receipt；相同 key/hash 可在 ACK 丢失后回放，不因 Lease 随后关闭而
重新执行。

CAS 过期固定返回 `AF_VERSION_STALE/412`，不得使用旧拼写 `AF_STALE_VERSION`。

### 2.3 Worker Progress 与 Artifact command Journal

Worker SQLite Journal schema v5 增加 `candidate_artifact_command_intents`；schema v6 进一步把首次成功的
完整 `ClaimedWork`（含 Package/Attempt/Lease CAS version 与 execution snapshot）保存进原 Claim intent。
schema v7 增加 `attempt_progress_command_intents`，把四个中心 Attempt 进度命令的完整 typed body、actor、
idempotency key、stage、请求摘要与创建时间先于 HTTP 调用写入 WAL/FULL SQLite。每个 Attempt/stage 只能有
一条不可变记录；下一阶段只有在前一阶段已有受验证的成功回执后才能注册，expected version 从完整 Claim
回执或前一 Progress 回执推导，不能猜测或跳步。

Progress 成功回执必须逐字段绑定 Project、Package、Attempt、Lease/fencing、目标状态、semantic sequence、
服务端时间和 `expected_version + 2`。ACK 丢失时 executor 只重放原 actor/key/body；服务端返回首次成功 view
后，Journal 用由请求创建时间和响应 `updated_at` 确定的完成时间落账，因此重试时钟不能改写历史。请求、
回执与 pending→completed 转换受触发器保护且不可删除。schema v7 已提供 executor 与 pending 查询；daemon
自动规划四阶段属于下一纵切，当前不会悄悄在 Artifact adapter 内补报进度。

Init、每个 Chunk、Complete 都以
完整 typed command、actor、idempotency key、Attempt binding、请求摘要和创建时间先于 HTTP 调用提交；
成功后再以一个 SQLite 事务写入 typed response、响应摘要和完成时间。请求字段、完成回执和状态转换由
触发器保护，不能 UPDATE 改写或 DELETE。

重启时只允许按 `(created_at, intent_id)` 读取 pending intent，并重放原 command；不得重新生成 command ID、
key、chunk 内容或 Bundle binding。即使 ACK 丢失后本地 Worker 已观察到 Lease 丢失，远端 receipt-first
回放得到的原始成功响应仍可写入 Journal；但 receipt miss 的新命令必须重新通过当前 Lease/fencing 门禁。
Init 回执固定 `UPLOADING/version=1`，Chunk 回执必须逐字段匹配声明和内容摘要，Complete 回执必须与 Init 的
Candidate/Artifact/Bundle lineage 完全一致且为 `COMPLETE/version=3`。

v2、v3、v4、v5、v6 Journal 在独占 SQLite 迁移事务内逐级升级到 v7；未知版本 fail closed。v5 以前已经完成
的 Claim 没有可证明的中央 Attempt version，迁移会保留该历史行，但 Artifact workflow 返回“缺少可验证
Claim 回执”，不得猜测常量 version。

在显式 `driver_mode=fixture` 下，daemon 会在本地 hard verification 后持久化 Candidate seal，生成一个
确定性的单 chunk canonical JSON 测试 Bundle，并按 Init → Chunk → Complete 顺序自动创建、执行上述 intent。
启动时 pending Artifact command 的恢复优先于 Lease maintenance 和任何新规划；Init ACK 丢失后只回放原
key/body，不建立第二个 Artifact。该载荷仅验证 wire、fencing、CAS、摘要和宕机恢复，不是 Git Bundle，
也不能替代真实 jcode workspace 输出。Complete 后 Worker 仍停在 `HandingOffCandidate`；当前尚未调用
`RecordCandidate`、关闭作者 Lease或启动独立 VerificationRun。

## 3. 管理 CLI

CLI 与 HTTP 使用相同的 application port；它不会绕过领域状态机、typed rows、receipt、Event 或
Outbox。写命令的 JSON 文件是 `MvpCommand<T>` envelope，包含 `context` 和 `input`。

```bash
cargo run --locked -p agentforge-control-plane --bin af-cli -- \
  mvp --database-schema public project-create project-create.json

cargo run --locked -p agentforge-control-plane --bin af-cli -- \
  mvp offers <project-uuid> --limit 50

cargo run --locked -p agentforge-control-plane --bin af-cli -- \
  mvp package-claim claim.json

cargo run --locked -p agentforge-control-plane --bin af-cli -- \
  mvp lease-get <project-uuid> <lease-uuid>

cargo run --locked -p agentforge-control-plane --bin af-cli -- \
  mvp lease-reconcile-expired <project-uuid> --limit 100
```

如果未传 `--database-url`，CLI 使用 `AGENTFORGE_DATABASE_URL`。连接器只接受 loopback PostgreSQL；
远程生产数据库需要 TLS/身份 adapter，本 MVP 不做不安全降级。

## 4. Lease 收敛

主动 Release 与数据库时钟判定的 Expire 使用同一条事务路径，并按 Package → Attempt → Lease 固定
顺序加锁。成功后在一个事务内：

1. Lease 进入 `RELEASED` 或 `EXPIRED`；
2. 作者 Attempt 进入 `LOST`；
3. WorkPackage 在仍有尝试预算时进入 `REWORK_READY`，否则进入 `FAILED`；
4. 三个聚合各自追加 Event，并写出对应 Outbox；
5. 下一次 Claim 创建新 Attempt/Lease，fencing token 必须严格递增。

后台 sweeper 使用 PostgreSQL `clock_timestamp()` 选择到期 Lease，单批 1–1000 条。并发 Renew、Release
或其他 sweeper 实例只会产生 CAS/状态冲突，不会重复终结或复用旧 fencing token。

`GET /readyz` 只有在投影源与 PostgreSQL 命令面同时就绪时才返回 `204`。数据库检查会核对全部
迁移的版本、名称与源码摘要，并确认 MVP 所依赖的 typed tables 存在；未装配命令面、迁移漂移或
数据库不可达均返回 `503`。
