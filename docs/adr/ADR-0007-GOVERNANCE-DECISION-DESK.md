# ADR-0007：以 GovernanceCase、Decision 与可重建投影构建治理决策台

- 状态：Accepted
- 日期：2026-08-10
- 决策者：AgentForge 架构组
- 影响范围：审批、策略、人工干预、Control Room、审计与安全动作
- 相关决策：ADR-0001、ADR-0002、ADR-0006
- 详细规范：[12_GOVERNANCE_CONTROL_ROOM_UI.md](../development/12_GOVERNANCE_CONTROL_ROOM_UI.md)

---

## 1. 背景

AgentForge 的安全基线已经描述了 `ReviewItem` 和一次性审批 token，但尚未把它提升为完整领域聚合，
也没有规定审批前影响预览、决策版本、策略回滚、UI read model 和操作者命令的统一语义。

分布式 Agent 工厂的治理不能只是一个“Approve”按钮。操作者必须在有限时间内判断：

- 为什么此刻需要决策；
- 精确要执行什么动作；
- 动作影响哪个 Project、AFWP revision、Attempt generation、Run、路径、权限和预算；
- 哪些证据支持系统建议；
- 不处理、拒绝或超时分别发生什么；
- 决策之后的实际副作用是否成功，能否回滚；
- 这项配置或策略改变如何影响未来路由和正在执行的任务。

如果 UI 直接修改数据库状态、把评论直接拼进运行中 Prompt，或允许批准后动作参数变化，治理会绕过
AFWP、fencing、独立验收和审计。如果 UI 直接消费原始事件并自行推断权威状态，断线、乱序和前端
版本差异又会产生互相矛盾的操作画面。

## 2. 决策

采用“治理事实 + 决策 + 执行 + 查询投影”四层模型：

```text
Policy evaluation / Agent request / Operator proposal / Security event
                              |
                              v
                       GovernanceCase
                              |
                    immutable Decision(s)
                              |
              typed command / one-time capability
                              |
                    ExecutionReceipt/Event
                              |
                rebuildable UI projections
```

1. 将安全文档中的 `ReviewItem` 概念提升并统一命名为 `GovernanceCase`；不再创建第二个近义审批聚合；
2. `Decision` 是 append-only 事实，绑定精确 action digest、目标版本、策略版本、决策者和 expiry；
3. Decision 本身不表示副作用已经执行；执行结果由 typed command 的 receipt/domain event 证明；
4. 项目、路由、预算和权限配置采用不可变 `PolicyRevision`，通过 CAS 激活、模拟和显式回滚；
5. Control Room 只读取可重建 projection，所有正式写入继续调用 typed command endpoint；
6. 原始 Event Ledger 用于审计和重建，不由浏览器直接折叠成业务状态；
7. Decision Desk 是统一的待办投影，聚合审批、PlanPatch、预算、安全、冲突和人工问题，但不把不同
   领域对象合并成一张可随意修改的 Ticket 表。

## 3. GovernanceCase 边界

一个 Case 必须包含：

- `case_id`、Project 和 tenant scope；
- case kind、risk、urgency、来源事件和 causation；
- subject type/id/version；
- AFWP revision/hash、Attempt/generation、InvocationRun（若适用）；
- normalized action 与唯一 `action_digest`；
- requested capabilities 和当前/新增权限差异；
- PolicyRevision 与匹配 rule IDs；
- 受影响的 Package、Run、预算、路径、Git ref 或外部系统；
- 证据引用、系统建议、替代方案和 fallback；
- due/expiry、超时默认动作和是否需要多人批准；
- CAS version 和终态执行引用。

Case 不得携带可用 Secret、完整 Prompt、隐藏 chain-of-thought、任意 shell、任意 URL 或自由形式权限。
自然语言理由只作上下文；授权以 normalized action、typed target 和 digest 为准。

## 4. 精确动作绑定

`action_digest` 至少覆盖：

```text
schema/version
+ tenant/project
+ case kind
+ subject IDs and expected versions
+ package revision/hash
+ attempt/lease generation（若适用）
+ normalized typed action and parameters
+ capability delta
+ policy revision
+ affected resource snapshot digest
+ expiry/default behavior
```

以下任一变化使旧 Decision 自动失效或 Case supersede：

- action 参数、目标路径、网络域、Git ref 或预算变化；
- Package/Project/Policy revision 变化；
- Attempt、Lease 或 Run claim generation 变化；
- 影响预览的资源版本变化；
- 批准超时、决策者权限撤销或审批策略变化。

UI 必须在确认前展示人类可读 diff，同时提交机器可重算的 digest。后端重算失败时返回稳定 stale 错误，
不得“尽力应用”到新状态。

## 5. Decision 与执行分离

Decision 的允许结论为：

```text
APPROVE | DENY | REQUEST_CHANGES | DEFER
```

- `APPROVE` 可以生成绑定 action digest 的一次性 capability 或触发一个 typed command；
- `DENY` 触发 Case 定义的 fallback、缩权或重规划 Signal；
- `REQUEST_CHANGES` 不能原地编辑 AFWP，必须产生新 Revision/PlanPatch proposal；
- `DEFER` 只改变操作者 inbox 的到期策略，不延长 Lease、Run claim 或原批准 token；
- Decision 写入成功不等于外部副作用完成；Case 只有在 ExecutionReceipt 被验证后才到 `APPLIED`；
- 外部副作用 outcome unknown 时 Case 进入 `EXECUTING/RECONCILING`，先查询再重试。

高风险/critical Case 的请求者、作者和唯一审批者默认不得是同一 actor。需要多人审批时，每个 Decision
独立签名，满足 policy quorum 后才产生执行 capability。

## 6. Attempt 等待语义

不新增 `WAITING_PERMISSION` Attempt 核心状态。权限请求使用既有：

```text
AttemptState::WaitingInput
+ WakeCondition::PermissionDecided { request_id/case_id }
+ resume_state in {Planning, Implementing}
```

Control Room 可以把该 typed reason 显示为“等待审批”。这样 UI 语言不会成为第五套状态机，Baseline
blocker 仍必须结束旧 Attempt，Candidate-first 和作者 Lease 规则保持不变。

## 7. 评论、提问与任务契约

为了获得任务管理式交互，Control Room 可以提供线程、评论和 `@mention`，但语义必须明确：

- 普通评论是不可执行的 `OperatorNote`，不得改变 AFWP 或权限；
- 回答既有问题使用 `AnswerQuestion` typed command，绑定 question/wake condition；
- “催一下”使用 `RequestNudge`，生成 RunSignal，由 Supervisor 决定安全的下一步；
- 修改 scope、接口或 hard AC 使用 `ProposeRevision`/`PlanPatch`，经过 Linter、影响分析和审批；
- 新输入 Artifact 使用内容引用和 digest，不把任意附件直接注入 Prompt；
- `@mention` 最多生成去重 Signal，不能绕过预算、路由、RunClaim 或作者 Lease。

因此线程改善沟通，但 WorkPackage/PackageRevision 仍是执行契约和唯一任务真相。

## 8. PolicyRevision 与回滚

策略文档不可原地覆盖。每个 revision 保存 canonical document、digest、base revision、creator、创建原因
和适用 scope。流程为：

```text
DRAFT -> STAGED -> ACTIVE -> SUPERSEDED
                  \-> ROLLED_BACK（由新的 activation 事件表达）
```

激活前必须：

1. Schema/Linter 通过；
2. 对固定历史 snapshot 和当前受影响资源做 deterministic simulation；
3. 展示允许/拒绝/路由/预算/权限变化；
4. 高风险变化形成 GovernanceCase；
5. 使用 scope current policy version CAS 激活。

回滚不是删除新 revision，而是以新 activation event 重新选择已知 revision，并记录原因、操作者、影响
和后续检查。正在运行的 InvocationRun 不原地换策略；新策略产生 Signal，由策略决定继续、drain、取消
或新建 Run/Attempt。

## 9. Decision Desk 与 Control Room

Control Room 的默认首页是可操作的 Decision Desk，而非仅展示总数的 Dashboard。首屏必须区分：

- `NEEDS_DECISION`：真正等待操作者；
- `AT_RISK`：预算、deadline、无进展或恢复异常；
- `RUNNING`：服务器证明有有效 RunClaim 的 InvocationRun；
- `BLOCKED`：有 typed blocker/wake condition；
- `BUDGET_RISK`：reservation/forecast 接近 policy 阈值。

每张决策卡展示：原因、精确动作、证据、影响、建议、替代方案、截止时间、默认结果和版本。危险动作
先 Preview，再 Confirm；批量操作只允许相同 action schema/risk/policy 的同质 Case，并逐项验证 digest。

## 10. 投影不是授权事实

以下 projection 由事件和关系事实可重建：

- Project Control Room；
- WorkGraph/critical path；
- InvocationRun timeline；
- Governance inbox；
- Agent/Executor/Node fleet；
- Budget rollup；
- Candidate/Verification/Submission/Integration lineage；
- Activity/actor attribution。

projection 可以延迟、丢弃并重建，但不能用于 Lease、fencing、Candidate、验收或批准授权。命令 handler
必须重新读取规范化业务表、PolicyRevision、Case/Decision 和 expected version。

浏览器通过分页 snapshot + 稀疏 SSE invalidation 工作。SSE 只通知资源 ID/version 变化，不发送 Secret、
完整 Event firehose、模型 token delta 或全量图。断线后使用签名 cursor；超出热窗口重新拉取 projection。

## 11. 暂停、终止和隔离

“Pause Agent”必须拆成有确定语义的 typed command：

- `DrainExecutor`：停止新分配，已有 Run/Lease按策略完成；
- `PauseProjectDispatch`：停止新 Intent dispatch，不撤销已有作者 Lease；
- `CancelInvocationRun`：撤销 RunClaim并安全停止当前 Adapter；
- `RevokeAuthorLease`：提升/终结 fencing generation，正式作者写立即失效；
- `QuarantineNode/Executor`：停止新分配、吊销 capability，并触发未集成结果重验；
- `CancelWorkPackage`：遵守 WorkPackage 状态机和 Candidate-first 规则。

UI 不提供语义模糊的统一“Stop”按钮。Preview 必须列出受影响 Run、Lease、Package、预算和可能产生的
salvage/rework。

## 12. 安全、隐私与可访问性

- 浏览器不接收 bearer、fencing token、节点 key、完整私有 Prompt 或原始 chain-of-thought；
- Evidence/Markdown/SVG/HTML 使用安全渲染与 CSP；
- 每个 mutation 需要 CSRF/Origin 防护、身份、RBAC、Idempotency-Key 和 `If-Match`；
- restricted 项目字段在 projection 层按 ACL 裁剪，计数也不得跨租户泄漏；
- 审批 token 不存 localStorage；短期 session 使用安全 Cookie/平台凭据；
- 决策、拒绝、回滚、批量操作和管理员 impersonation 全部记录 actor/correlation；
- 键盘、屏幕阅读器、焦点、颜色对比和 reduced-motion 是 hard AC；
- 手机端优先 Decision Desk 和安全动作，不渲染不可操作的超大 DAG。

## 13. 结果

### 13.1 正面结果

- 人工干预从聊天和后台改表变成可审计、可恢复的业务流程；
- 批准绑定精确动作和版本，参数漂移不能复用旧授权；
- 策略变化可模拟、回滚并解释对路由/预算/权限的影响；
- UI 可在低带宽和断线下工作，又不成为状态事实源；
- 任务管理式线程不会破坏不可变 AFWP 和 Candidate-first。

### 13.2 代价

- 增加 GovernanceCase、Decision、PolicyRevision、ExecutionReceipt 与多个 projection；
- 操作者需要理解 Preview/Decision/Applied 的区别；
- UI 不能通过通用 CRUD 快速实现，必须使用 typed command；
- projection 和命令读侧需要单独的契约、重建和权限测试。

## 14. 被否决的方案

### 14.1 可变 Ticket 作为任务真相

拒绝。会允许评论、字段编辑和 UI 状态覆盖不可变 revision、fencing、验收和 Git lineage。

### 14.2 Approval 按钮直接执行任意 payload

拒绝。批准与参数漂移、重放和权限扩大无法绑定，无法安全恢复 outcome unknown。

### 14.3 浏览器直接消费 Event Ledger 并自行折叠状态

拒绝。前端版本、乱序和断线会产生不一致状态，也会扩大敏感事件暴露。

### 14.4 组织图直接决定执行路由

拒绝。组织图表达责任和审批链；真正路由仍必须使用 Executor fingerprint、capability、实测 Outcome、
预算和安全资格。

## 15. 验证标准

1. action 参数、subject version、AFWP revision、generation 或 policy revision 任一变化后旧批准不能执行；
2. Decision ACK 丢失后相同 actor/key/hash 只回放首次结果，不产生第二条 Decision；
3. `APPROVE` 后外部动作失败时 Case 不会错误显示 `APPLIED`，并可从 receipt/outbox 恢复；
4. author/requester 不能成为 critical Case 的唯一审批者；
5. permission Case 决定后只满足对应 typed wake condition，不能唤醒其他 Attempt；
6. 普通评论、附件或 `@mention` 不能修改 AFWP、增加 capability 或绕过预算；
7. Policy simulation 对相同 snapshot/revision 字节一致，CAS 只允许一个并发 activation；
8. rollback 保留被回滚 revision、激活历史和影响报告，不原地删除；
9. 从空 projection 重放 100,000 个事件后，Decision Desk、Run、预算和 lineage digest 与在线结果一致；
10. SSE 在 cursor 37 断线，恢复后无状态缺口；重复 invalidation 无副作用；
11. UI mutation 只调用 typed command，数据库审计中不存在通用状态 PATCH；
12. 手机窄屏可完成查看证据、批准、拒绝和安全暂停，且误触危险动作前有 Preview/Confirm；
13. UI/日志/投影的 Secret、token、Prompt 和 chain-of-thought 扫描为零发现；
14. WCAG 2.2 AA 自动与人工关键路径检查通过。

## 16. 后续影响

- M1 实现 GovernanceCase、Decision、基础 projection 和 Thin Control Room；
- M2 的 jcode permission request 必须进入 GovernanceCase，而不是阻塞 Sidecar 对话框；
- M5 增加 PolicyRevision simulation/rollback、多方审批和完整 Decision Desk；
- 安全文档中的 `ReviewItem` 样例视为 GovernanceCase 的早期名称，实现不得同时保留两个聚合；
- Attempt 权限等待统一使用 `WaitingInput + PermissionDecided`，不增加 `WAITING_PERMISSION` 数据库状态。
