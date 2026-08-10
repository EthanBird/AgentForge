# MVP 可恢复 Worker：Journal 与 Turn Pump

本文记录 `v0.1.0-mvp` 的 Worker 基础实现。权威行为仍以
[04_WORKER_RUNTIME_JCODE.md](04_WORKER_RUNTIME_JCODE.md) 为准；本文说明当前代码已经实现、验证和
尚未接线的边界。

## 1. 当前实现范围

代码位于 `crates/worker-daemon`：

- `runtime.rs`：纯函数式 Worker Attempt 状态机，支持 replay、严格版本 CAS、等待/唤醒、候选封存、
  Lease 丢失与 terminal guard；
- `journal.rs`：SQLite WAL Journal，把状态事实、物化状态、Inbox receipt、Outbox 与外部操作完成记录
  放在同一 `BEGIN IMMEDIATE` 事务中；
- `supervisor.rs`：确定性 Turn Pump。模型声明完成不会绕过本地 hard criteria；每次 Turn 后必须执行
  Local Verification；
- `lifecycle.rs`：Offer 选择、稳定 Claim intent、Claim 响应校验、本地 Grant handoff 与启动 Lease
  reconciliation；
- `config.rs`：严格、限长、拒绝未知字段的 daemon 配置，以及包含 runtime/policy 的稳定节点指纹；
- `daemon.rs`：单写者组合根，固定执行 pending Claim → pending Renew/Release → Lease maintenance →
  configured driver → capacity-bounded Claim；
- `http_control.rs`：有总超时、header/body 上限和严格错误契约的 loopback HTTP/1 adapter；
- `fixture_driver.rs`：显式 opt-in 的确定性演示驱动，把已领取工单推进到本地 `SealingCandidate`，并复用
  正式 Journal、Supervisor、operation ledger 和重启查询路径；
- `FakeTurnExecutor` / `FakeVerifier`：MVP 的确定性执行和故障注入边界，后续 jcode adapter 实现相同
  trait。

本检查点已经提供 transport-independent 的 Offer/Claim/Lease 端口、可恢复调度循环、loopback HTTP
adapter、可启动二进制和 fixture 端到端演示，但没有宣称 fixture 会调用 jcode、修改真实 workspace、
生成真实 Git 对象或向服务器提交 Candidate。LAN mTLS、Worker enrollment、workspace sandbox、jcode
进程桥接和 Candidate handoff 属于 MVP-02 后续纵切。

## 2. 状态机边界

Worker 的本地阶段为：

```text
Granted -> Preparing -> Baseline -> Planning -> Implementing
  -> LocalVerifying -> SealingCandidate -> HandingOffCandidate -> AuthorComplete
```

补充分支：

- `Planning` 或 `Implementing` 可以进入 `WaitingInput`，只有与持久化条件精确匹配的事实才能恢复到
  原语义阶段；
- Baseline blocker 直接进入 `LocalFailed`，不会把环境错误伪装成可继续的旧 Attempt；
- Lease 过期、撤销、服务器拒绝或更高 generation 会进入 `Salvaging`；
- `AuthorComplete`、`LocalFailed`、`LocalCancelled` 不可复活；
- Candidate 封存和 handoff 都必须携带当前 generation，Candidate tree 必须等于最近一次通过本地验证
  的工作树。

`WorkerAttemptState` 使用经过校验的自定义反序列化。不能通过构造 JSON 绕过 `version == journal_seq`、
等待态形状、Plan/Tree/Turn 关联或 Candidate 关联。

## 3. SQLite Journal

首次打开 Journal 会创建 schema version 4，并强制：

```text
PRAGMA journal_mode = WAL
PRAGMA synchronous = FULL
PRAGMA foreign_keys = ON
PRAGMA quick_check = ok
```

主要表：

| 表 | 用途 | 关键约束 |
| --- | --- | --- |
| `attempts` | 当前物化状态 | immutable binding、`version = journal_seq`、JCS digest |
| `journal_entries` | 不可变事实链 | `(attempt_id, seq)`、event ID 唯一、previous digest 链 |
| `execution_snapshots` | Claim 时固定的 AFWP/输入 | revision/hash/base 绑定、JCS digest、不可修改/删除 |
| `inbox` | 本地命令回执 | `(actor_id, idempotency_key)` 唯一、不可修改/删除 |
| `outbox` | 待发控制面事件 | destination + semantic key 唯一、payload 不可变 |
| `operations` | 外部副作用账本 | 先计划后执行、request immutable、Pending 只能单向完成 |
| `claim_intents` | 远端 Claim 前的节点级意图 | actor/key 唯一、request immutable、Pending 只能完成 |
| `lease_command_intents` | Renew/Release 远端意图与回执 | Attempt/fencing 绑定、request immutable、receipt JCS digest |

每个正式命令采用以下顺序：

1. 以 actor + idempotency key 查询 Inbox；
2. 命中且 request digest 相同则直接回放首次响应；
3. 命中但 digest 不同则返回 `AF_IDEMPOTENCY_KEY_REUSED`；
4. receipt miss 才执行状态转移；
5. 在一个事务中追加哈希链事实、更新物化状态、完成外部 operation、写 Outbox 和 Inbox；
6. 任一步失败或进程在提交前终止，全部回滚。

Journal 在恢复时重算每条事实的 JCS SHA-256、整条 previous-digest 链和物化状态摘要；任一不一致均
fail closed 为 `AF_WORKER_JOURNAL_INTEGRITY`。

Schema version 1 是尚未携带执行快照的预发布开发格式，无法安全补造 AFWP/input，因此明确拒绝打开；
version 2 到 version 3 使用单事务增加空的 `claim_intents` 账本，version 3 到 version 4 增加
`lease_command_intents`；跨级打开会在同一个排他事务中顺序执行两步，已有 Attempt、事实链和执行
快照保持不变。正式 `v0.1.0-mvp` 发布后，Journal 变更必须提供可验证迁移或显式导出/重新 Claim
流程。

## 4. Claim handoff 与 Lease reconciliation

控制面 Claim 响应现在携带不可变 `PackageExecutionSnapshot`：revision、package hash、base commit、
Git object format、canonical AFWP 和 input snapshot。PostgreSQL adapter 从 canonical typed rows 读取并
重新验证 JCS hash；Worker 再次复核响应与已选择 Offer、Git object format 和 hash，之后才把 Grant
与执行快照写入一个本地事务。

`ClaimIntentRecord` 固定 Offer、expected version、actor/executor/node、command/correlation ID、
idempotency key、Lease window 和本地 message ID。Worker 在远端 Claim 前先把它写入
`claim_intents`；远端失败或 ACK 丢失后，重启只会枚举 Pending record 并重放同一个 command，不能
重新从 Offer 列表挑选另一个 Package。远端响应、本地 Grant 和执行快照成功后，该 record 才单向
绑定 Attempt/Lease 并标记 Completed。

启动 reconciliation 对每个非终态 Attempt 查询服务器 Lease，并核对 Project、Package、Attempt、
Lease、holder node 和 generation：

- 完全一致且 expiry 相同：继续；
- 服务器显示一个此前漏收的合法 Renew：使用服务器 `updated_at` 和新 expiry 追加本地
  `LeaseRenewed`；
- Expired、Revoked、holder/generation 不匹配：追加 `LeaseLost` 并进入 `Salvaging`；
- 响应绑定错误、expiry 回退或时间形状异常：`AF_WORKER_CONTROL_RESPONSE_INVALID`，不启动副作用。

`maintain_attempt` 以注入的 `LeaseMaintenancePolicy` 决定动作：距离 expiry 大于窗口时不写任何远端
命令；进入窗口后把 exact Renew command 写入 `lease_command_intents` 再调用控制面，并把服务器
receipt 与摘要封存。若服务器已经续租但响应丢失，重启重放同一 idempotency key，只导入首次
expiry。`LocalFailed`/`LocalCancelled` 使用相同机制 Release；`AuthorComplete` 在 Candidate handoff
完成前仍按阈值 Renew，不能被维护循环错误地回收到 `REWORK_READY`。此时若 Lease 丢失，本地
Candidate 只保留为 salvage 输入，正式 `candidate_id` 授权被清除。

`WorkerDaemon::tick` 使用单个可信 `ServerInstant` 作为本轮观察时间，并按不可交换的顺序执行：

1. 重放全部 Pending Claim intent；任何一个失败即停止本轮，不能越过未知 Claim 再接新工单；
2. 重放全部 Pending Renew/Release intent；
3. 对本地 Lease 做 reconciliation、阈值续租或终态释放；Project 必须来自成功 Claim 的不可变记录，
   不能从多项目配置猜测；
4. 若显式启用 fixture driver，把现有 runnable Attempt 推进到本地 `SealingCandidate`；
5. 重新计算非 salvage 作者 Attempt 容量，按项目轮转 Claim，达到 capacity 后停止。

`SealingCandidate` 仍占用 `capacity`：在 Artifact/Candidate handoff 尚未完成时，它继续持有作者 Lease，
不能因为模型计算已经结束就无限积压本地候选并继续接单。

远端 mutation 之前一定已经存在 durable intent。正常 shutdown 只在一个 tick 完成后生效；硬崩溃或
调用取消则由同一意图和 idempotency key 在下一次启动恢复。定时器采用 delay 语义，慢请求不会触发
追赶式 mutation burst。

### 4.1 Loopback HTTP 与启动

`LoopbackHttpControlPlane` 只接受 literal `http://127.0.0.1:PORT` 或 `http://[::1]:PORT`。DNS 名称、
userinfo、path/query、端口 0 与非 loopback 地址一律在配置阶段拒绝；无 TLS adapter 不能被配置成 LAN
连接。每个 exchange 还强制：

- 整体 timeout，32 KiB response header 上限和 1 KiB–4 MiB 可配 body 上限；
- HTTP/1.1、唯一 `Content-Length`、`application/json`，拒绝 `Transfer-Encoding` 与重复长度/类型；
- strict JSON（重复 key、尾随输入、unsafe integer 均失败）和 typed response unknown-field 拒绝；
- HTTP status、稳定 AF error code 与 `retryable` 三者必须匹配，未知未来 code fail closed；
- Claim/Renew/Release 传递原 command/correlation/idempotency/If-Match，body 只包含服务器 API 规定的
  typed input，不能由客户端注入 actor。

安全默认示例位于 `examples/worker-loopback.json`，其 `driver_mode` 为 `lease_only`，只执行 Claim 与 Lease
维护。确定性演示使用独立 Journal 的 `examples/worker-loopback-fixture.json`。复制任一示例后至少替换
Project/actor/executor/node ID、runtime fingerprint 与 Journal 路径，然后启动：

```bash
install -d -m 0700 /var/lib/agentforge
export AGENTFORGE_WORKER_CONFIG=/etc/agentforge/worker.json
cargo run --locked -p agentforge-worker-daemon --bin agentforge-worker-daemon
```

二进制在打开 Journal 与建立任何远端请求前严格解析配置；SIGINT/SIGTERM 只在当前 tick 收敛后退出。
日志只打印 node ID、派生指纹、项目数和容量，不回显完整配置或凭据。

## 5. 外部副作用与重启恢复

模型 Turn 属于 `NON_REPEATABLE`：

1. 调用 Executor 前，先持久化 operation ID、semantic key、request digest、计划时间和 deadline；
2. Executor 返回结果后，operation completion 与 `TurnRecorded` 事实原子提交；
3. 若模型已执行但 ACK 丢失，Supervisor 只按原 operation ID 查询结果，不重新启动；
4. 若 daemon 在二、三步之间重启，启动后的 `drive_cycle` 会读取 Pending operation、重建并核对原
   request digest，然后查询原 Executor operation；
5. 结果仍未知时返回 `AF_EXECUTOR_OUTCOME_UNKNOWN`，保留 Pending 记录等待人工/adapter 对账。

Local Verification 标记为 `IDEMPOTENT`。重启后使用原 operation ID 和相同输入重新执行，并在同一
事务中完成 operation 与验证事实。Pending operation 与当前 Attempt phase、kind、class、semantic key、
request digest 或 deadline 任一不一致时，稳定返回 `AF_WORKER_RECOVERY_CONFLICT`。

### 5.1 Fixture driver 的诚实边界

`driver_mode=fixture` 仅用于在同机 MVP 控制面上验证“领取 → 本地阶段 → Turn operation → 本地验收”
这条可恢复调用链：

- Preparation、workspace、baseline 与 plan 事实使用稳定 JCS/SHA-256 派生值；
- 非幂等 Turn 的输出由已持久化 operation ID 确定性派生，因此 daemon 重启后的
  `query(operation_id)` 不依赖进程内存，也不会重新启动一次 Turn；
- Local Verification 始终产生一个显式 hard-pass evidence digest；
- 生成的 tree 只是 SHA-1 形状的内容寻址测试值，不写入 Git；SHA-256 object-format 仓库会在任何阶段
  变更前以 `AF_WORKER_FIXTURE_GIT_FORMAT_UNSUPPORTED` 拒绝；
- 驱动最多到本地 `SealingCandidate`。它不创建中央 `Candidate`、不上传 Artifact、不释放作者 Lease，
  更不代表独立 VerificationRun 或 `candidate_ready` 已通过。

因此 fixture 模式不能用于不可信工单或生产开发。`lease_only` 是当前安全默认；后续 jcode bridge 将实现
相同的 `TurnExecutor`/`LocalVerifier` 边界，并以真实 workspace sandbox 与 Git object evidence 取代
fixture 值。

## 6. 当前验证证据

本地定点门禁：

```bash
cargo test -p agentforge-worker-daemon --locked
cargo clippy -p agentforge-worker-daemon \
  --all-targets --all-features --locked -- -D warnings
cargo fmt --all -- --check
```

覆盖场景包括：

- 状态机 happy path、非法唤醒、Baseline blocker、higher-generation fencing、反序列化绕过；
- WAL/FULL 配置、事实哈希链、state/outbox digest、不可变触发器；
- receipt-first ACK-loss replay 与 changed-payload key reuse；
- Journal、projection、operation、Outbox、receipt 五个提交前崩溃点；
- 外部 operation 先计划后执行、completion 原子性和 completion ACK-loss；
- 模型提前声称完成但 hard criterion 失败时继续下一 Turn；
- Worker 重启后查询原非幂等 Turn，并恢复 Pending Verification；
- Claim 响应执行快照的双端摘要校验、远端调用前 durable intent、ACK-loss/restart exact replay、漏收
  Renew 导入、阈值续租、终态 Release 和 Revoke 停止；
- daemon 严格配置、节点指纹、pending-first 启动顺序、容量门禁、Claim ACK-loss 后重启恢复与自动续租；
- loopback HTTP 的真实 Axum contract、command header、远端错误码、重复 JSON、body 上限和总 timeout；
- fixture driver 的完整本地阶段推进、重启后确定性 operation query、SHA-256 仓库 fail-closed，以及
  `SealingCandidate` 继续占用容量、防止无界接单；
- Turn budget 与 Lease expiry 在启动 Executor 前阻止新副作用。

## 7. 下一纵切

MVP-02 的下一检查点按顺序接入：

1. Worker enrollment、本地节点身份与 LAN mTLS 控制面 adapter；
2. workspace/日志/凭据目录隔离与受控命令执行；
3. jcode bridge 版本握手、能力探测、operation query 与 sanitized transcript；
4. Candidate Artifact 上传及 `RecordCandidate` handoff。

当前二进制已经能连接同机控制面执行 Offer/Claim/Lease 循环，并可在明确的 fixture 模式走到本地候选；
在 enrollment、真实执行桥、sandbox 和 Candidate handoff 完成前，它仍不是可以接收不可信工单的最终
部署形态。
