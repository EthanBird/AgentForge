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
- `FakeTurnExecutor` / `FakeVerifier`：MVP 的确定性执行和故障注入边界，后续 jcode adapter 实现相同
  trait。

本检查点没有宣称完成 Offer poll、远端 Claim/Renew、Worker enrollment、workspace sandbox 或 jcode
进程桥接；这些属于 MVP-02 后续纵切。

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

首次打开 Journal 会创建 schema version 1，并强制：

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
| `inbox` | 本地命令回执 | `(actor_id, idempotency_key)` 唯一、不可修改/删除 |
| `outbox` | 待发控制面事件 | destination + semantic key 唯一、payload 不可变 |
| `operations` | 外部副作用账本 | 先计划后执行、request immutable、Pending 只能单向完成 |

每个正式命令采用以下顺序：

1. 以 actor + idempotency key 查询 Inbox；
2. 命中且 request digest 相同则直接回放首次响应；
3. 命中但 digest 不同则返回 `AF_IDEMPOTENCY_KEY_REUSED`；
4. receipt miss 才执行状态转移；
5. 在一个事务中追加哈希链事实、更新物化状态、完成外部 operation、写 Outbox 和 Inbox；
6. 任一步失败或进程在提交前终止，全部回滚。

Journal 在恢复时重算每条事实的 JCS SHA-256、整条 previous-digest 链和物化状态摘要；任一不一致均
fail closed 为 `AF_WORKER_JOURNAL_INTEGRITY`。

## 4. 外部副作用与重启恢复

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

## 5. 当前验证证据

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
- Turn budget 与 Lease expiry 在启动 Executor 前阻止新副作用。

## 6. 下一纵切

MVP-02 的下一检查点按顺序接入：

1. Worker enrollment、本地节点身份与控制面 `/api/v1` 客户端；
2. Offer poll、Claim、Renew、Release 和启动恢复时的服务器 Lease reconciliation；
3. workspace/日志/凭据目录隔离与受控命令执行；
4. jcode bridge 版本握手、能力探测、operation query 与 sanitized transcript；
5. Candidate Artifact 上传及 `RecordCandidate` handoff。

在这些入口完成前，`worker-daemon` 二进制仍是组合根骨架；本检查点交付的是可复用且经过故障测试的
runtime/library 边界，不是可以连接真实控制面的最终 daemon。
