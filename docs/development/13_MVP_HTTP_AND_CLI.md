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
- `If-Match: "<version>"`：Claim、Renew、Release 必须提供，创建 Project/Package 禁止提供；
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
