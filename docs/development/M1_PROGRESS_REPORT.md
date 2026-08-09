# AgentForge M1 阶段进度报告

- 报告日期：2026-08-10
- 阶段：M1 控制平面与 Control Room
- `main` 中的 M0 merge commit：`0e532ac1757645958d03e7b4f18706913c39464b`
- Domain 复测基线：`aea5445dba2bcad5bb24ad1e43d6ae79183e28e4`
- Control Room 远端检查点：`b2cf06c2412d9e9f1568c61f385752c83a65ffb7`
- 开发分支：`agent/m1-control-room`
- 当前状态：M1-A～M1-D 已形成可合并检查点；M1-E/M1-F 仅完成受限参考实现，M1 整体未完成

## 1. 阶段目标

M1 在 M0 已冻结的 AFWP、Attempt、Lease、Candidate-first、receipt-first、CAS 与 fencing
语义上实现第一个可运行控制面纵向闭环。参考 Paperclip 的运行账本与治理交互，但不采用
可变 Ticket 作为任务真相，也不把 heartbeat、进程 PID 或 Agent 自报状态作为分布式事实。

本阶段增加以下能力：

1. PostgreSQL 事实源、Repository ports、事务 Unit of Work 与 Outbox/Inbox；
2. Package 发布、Offer/Claim、Lease Renew/Expire 与 generation fencing；
3. `RunSignal -> InvocationIntent -> InvocationRun` 调用账本；
4. 内容寻址 `SessionCapsule` 与作者 Lease 分离的短期 Run claim；
5. Claim/Run 原子 `BudgetReservation`，并为独立验收保留预算；
6. `GovernanceCase`、不可变 `Decision` 与精确 `action_digest`；
7. 可重建 UI projections、断线续传 SSE 和 Thin Control Room。

## 2. 不变量

- `Attempt` 表示一次语义开发尝试，`InvocationRun` 只表示一次 Agent 激活窗口；Run 结束不得
  直接完成 Attempt 或 WorkPackage。
- 一个 Attempt 可以有多个 InvocationRun；恢复必须创建新 Run，不得复活旧 Run。
- Run 启动后上下文不可原地覆盖；新评论、审批与修订追加为 `RunSignal`。
- AFWP revision、Attempt generation、策略版本、Workspace HEAD 与 Capsule digest 必须固定绑定。
- 相同因果事实可安全合并；新 revision、取消、重新分配、权限决定和 fencing 变化不得合并。
- Claim/Run 与预算 reservation 在同一事务内完成；并发启动不能超卖预算。
- Governance 决策必须绑定精确目标版本、影响预览和 action digest，并使用幂等键与 CAS。
- UI 只消费可重建投影；所有正式副作用继续走 typed command endpoint。

## 3. 阶段性提交计划

| 检查点 | 交付 | 提交与推送条件 |
| --- | --- | --- |
| M1-A | ADR、调用/治理规格、工单与本报告 | Markdown 链接和术语检查通过 |
| M1-B | Invocation、RunSignal、SessionCapsule、BudgetReservation 领域模型 | domain fmt/clippy/test 通过 |
| M1-C | GovernanceCase、Decision、PolicyRevision 领域模型 | domain 与属性测试通过 |
| M1-D | Application ports 与 8 个可重建 read-model projections | application tests 通过 |
| M1-E | PostgreSQL migrations/repositories、Outbox/Inbox | 数据库 contract/integration tests 通过 |
| M1-F | Control Plane HTTP/SSE 与 Thin Control Room | API/UI contract 与端到端测试通过 |
| M1-G | 独立 Critic、故障注入、进度报告冻结 | workspace 全门禁通过 |

每个可合并检查点必须独立 commit 并立即 push，不能把多个阶段长时间积压在本地。完成 push 后还要
核对远端分支 SHA；只有远端可见且对应 CI 已启动，才视为进度已保存。发现阻断缺陷时先在开发
分支提交可复现测试，再提交修复，避免未落盘工作仅存在于不可靠的临时机器。

## 4. 当前进度

| 项目 | 状态 | 证据 |
| --- | --- | --- |
| M0 远端恢复验证 | 完成 | 基线包含最终 97/97 测试报告及 early-failure stage-shape 修复 |
| M1 分支与进度账本 | 完成 | 本文件与文档索引 |
| M1-A 详细规格 | 完成 | [ADR-0006](../adr/ADR-0006-INVOCATION-ORCHESTRATION.md)、[ADR-0007](../adr/ADR-0007-GOVERNANCE-DECISION-DESK.md)、[开发规格 11](11_INVOCATION_ORCHESTRATION.md)、[开发规格 12](12_GOVERNANCE_CONTROL_ROOM_UI.md) 与 [WP-M1-008..012](09_MILESTONES_AND_WORK_PACKAGES.md) 已冻结 |
| M1-B Invocation 领域模型 | 完成 | RunSignal、InvocationIntent/Run、当前内嵌 RunClaim、SessionCapsule 与 BudgetReservation 已实现；当前快照 Domain 复测为 38 个 unit、2 个 replay property、12 个 state-contract 测试全通过 |
| M1-C Governance 领域模型 | 完成 | GovernanceCase、不可变 Decision、PolicyRevision 及 action-digest/CAS/终态不变量已实现；与 M1-B 共用上述 Domain 门禁 |
| M1-D Application ports/read models | 完成 | Repository、Unit of Work、Event append 等 ports 与 Project Control、Runs、Governance Inbox、Fleet、Budget、Lineage、Work Graph、Activity 共 8 个投影已实现 |
| M1-E PostgreSQL 持久化 | 部分完成 | 迁移基线、migration runner 与 PostgreSQL CI 门禁已接入；生产级 repositories、事务 Unit of Work 以及 Outbox/Inbox handler 仍待实现，不能标记 M1-E 完成 |
| M1-F HTTP/SSE/Control Room | 部分完成 | 已有仅限 localhost、只读的 Thin UI/SSE reference；它校验投影元数据、使用项目绑定的 HMAC cursor、稀疏 SSE invalidation、同源 loopback Host/Origin、防跨项目请求竞态，并展示 degraded/staleness/as-of；生产认证与项目授权、持久化 projection/checkpoint、可跨重启续传的 SSE，以及 typed mutation endpoint 仍待实现，不能标记 M1-F 完成 |
| M1-G 独立验收与冻结 | 部分完成 | 合并前 workspace 预验收和 Control Room Critic 已执行，Control Room 当前无开放 P0/P1；必须在 M1-E/M1-F 欠账关闭后再完成生产数据库集成、故障注入与最终独立 Critic 冻结 |

当前可合并范围是 M1-A～M1-D，以及明确标注为 reference/baseline 的 M1-E/M1-F 增量；这些增量
不代表 PostgreSQL 纵向闭环或生产控制面已经完成。M1 整体保持“进行中”。

## 5. 合并前验证快照

远端 Control Room 检查点形成后，在隔离的 Cargo target 目录执行了以下门禁：

- `cargo fmt --all -- --check`；
- `cargo metadata --locked --no-deps --format-version 1`；
- `bash tests/contract/workspace_layout.sh`；
- `cargo build --workspace --locked --all-targets`；
- `cargo clippy --workspace --locked --all-targets --all-features -- -D warnings`；
- `cargo test --workspace --locked`：152/152 通过，其中 10,000 × 64 故障调度用例约 16.55 秒；
- `node --check crates/control-plane/assets/app.js`；
- `git diff --check`。

Critic 指出的项目切换竞态、投影健康语义、折叠导航可访问性、WorkGraph 字段错配、风险计数错标和
DNS rebinding 读取面均已修复。最后一次投影滞后文案修复后，control-plane 5/5 测试、严格 clippy、
Node.js 语法检查与 diff 检查再次通过；独立定点复核结论为无开放 P0/P1。

真实 PostgreSQL migration/constraint 门禁由 GitHub Actions 的 PostgreSQL 17 service 执行；本地未配置
`AGENTFORGE_TEST_DATABASE_URL` 时，对应测试只验证可安全跳过，不作为真实数据库通过证据。

## 6. 下一阶段已知工单

### M1-NEXT-01：将 RunClaim 提取为独立 aggregate

- 从 InvocationRun 的内嵌状态中提取 RunClaim，使其拥有独立标识、版本、generation、holder、
  expiry 与终态；Task Lease 和 RunClaim 的权限与生命周期继续严格分离。
- 验收：claim/release/expire/fence 的 `decide -> event -> apply -> replay` 等价；旧 generation、错误
  holder、过期 claim 与终态复活均返回稳定错误；InvocationRun 只引用已验证 claim binding。

### M1-NEXT-02：实现生产 PostgreSQL Unit of Work

- 为已冻结的 application ports 实现 PostgreSQL repositories、事务 Unit of Work、CAS event append、
  Inbox 幂等与 Outbox relay handler；Claim/Run/BudgetReservation 必须在同一事务中提交。
- 验收：真实 PostgreSQL 集成测试覆盖并发 CAS、重复 command、事务回滚、预算防超卖、Outbox
  至少一次投递与消费者幂等；禁止用内存 reference repository 通过生产验收。

### M1-NEXT-03：生产级 durable SSE、认证与 typed mutation

- 将 8 个 read models 与全局事件序列持久化，支持服务重启后的 checkpoint/cursor 恢复；接入 actor、
  tenant/project 授权，并把所有副作用暴露为有幂等键、CAS 与策略校验的 typed command endpoint。
- 验收：SSE 在断线和进程重启后无静默丢失或越权串流；篡改/过期 cursor 稳定失败；非 localhost
  部署未配置认证时 fail closed；UI 不直接写数据库，也不把 read-model 状态当作命令成功事实。

## 7. 当前风险与处理

- 执行环境可能随时丢失：所有阶段必须先验证、后 commit、立即 push。
- M1-E/M1-F 容易因已有 reference implementation 被误判为完成：合并说明、进度报告和 UI 都必须
  明示生产 repositories/UoW、durable projection/SSE、认证授权与 typed mutation 尚未交付。
- 目标部署为 2C4G 中央服务器：投影采用服务器分页与稀疏 SSE invalidation，不做前端全量事件聚合。
- Worker 分布且可能长时间断线：恢复只依赖持久 Lease、Run、Signal、Checkpoint 与 Event，不依赖
  控制进程内存或高频 ping。
