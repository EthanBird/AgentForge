# AgentForge M1 阶段进度报告

- 报告日期：2026-08-10
- 阶段：M1 控制平面与 Control Room
- 基线提交：`90295a1be4acc1b018e0d927cd7ba3d851f96c2e`
- 开发分支：`agent/m1-control-room`
- 当前状态：M1-A 契约冻结；M1-B/M1-C/M1-D 并行实现中

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
| M1-D | Application ports、内存 reference repository 与 read models | application tests 通过 |
| M1-E | PostgreSQL migrations/repositories、Outbox/Inbox | 数据库 contract/integration tests 通过 |
| M1-F | Control Plane HTTP/SSE 与 Thin Control Room | API/UI contract 与端到端测试通过 |
| M1-G | 独立 Critic、故障注入、进度报告冻结 | workspace 全门禁通过 |

每个检查点独立 commit 并立即 push。发现阻断缺陷时先提交可复现测试，再提交修复，避免未落盘
工作仅存在于临时机器。

## 4. 当前进度

| 项目 | 状态 | 证据 |
| --- | --- | --- |
| M0 远端恢复验证 | 完成 | 基线包含最终 97/97 测试报告及 early-failure stage-shape 修复 |
| M1 分支与进度账本 | 完成 | 本文件与文档索引 |
| M1-A 详细规格 | 完成 | ADR-0006、ADR-0007、开发规格 11/12 与 WP-M1-008..012 已冻结；链接、围栏、工单 ID 与 workspace layout 检查通过 |
| M1-B Invocation 领域模型 | 进行中 | RunSignal、InvocationIntent/Run、RunClaim、SessionCapsule 与 BudgetReservation 正在实现 |
| M1-C Governance 领域模型 | 进行中 | GovernanceCase、Decision 与 PolicyRevision 正在实现 |
| M1-D Application/read models | 进行中 | Repository/UoW ports 与可重建 Control Room projections 正在实现 |
| M1-E 以后实现 | 未开始 | 按独立门禁完成后逐检查点提交并推送 |

## 5. 当前风险与处理

- 执行环境可能随时丢失：所有阶段必须先验证、后 commit、立即 push。
- 控制面目前多数 crate 仍是骨架：先冻结公共契约，再并行实现消费者，避免多套近义 API。
- 目标部署为 2C4G 中央服务器：投影采用服务器分页与稀疏 SSE invalidation，不做前端全量事件聚合。
- Worker 分布且可能长时间断线：恢复只依赖持久 Lease、Run、Signal、Checkpoint 与 Event，不依赖
  控制进程内存或高频 ping。
