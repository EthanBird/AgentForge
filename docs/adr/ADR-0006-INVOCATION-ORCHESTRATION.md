# ADR-0006：以 Signal、Intent、InvocationRun 编排 Agent 调用

- 状态：Accepted
- 日期：2026-08-10
- 决策者：AgentForge 架构组
- 影响范围：控制平面、Obligation Engine、Worker Gateway、Agent Adapter、预算与恢复
- 相关决策：ADR-0001、ADR-0002、ADR-0003、ADR-0004
- 详细规范：[11_INVOCATION_ORCHESTRATION.md](../development/11_INVOCATION_ORCHESTRATION.md)

---

## 1. 背景

AgentForge 已经把 `WorkPackage`、`Attempt`、作者 `Lease`、`Candidate`、
`VerificationRun`、`Submission` 和 `Integration` 分开建模，也已经规定 jcode 的一次
`turn_done` 不等于任务完成。但是，仅有任务状态和 Worker 本地 Journal 仍不能完整回答以下问题：

- 哪个事实触发了这次 Agent 调用；
- 多个重复事件是否被安全合并；
- 这次调用固定了哪个 AFWP revision、Attempt generation、策略和上下文；
- 同一个 Attempt 为何以及何时产生了多次启动或恢复；
- 调用前是否原子预留了足够预算；
- 进程在外部 Adapter 已接收请求、控制面尚未收到结果时崩溃，应该怎样对账；
- Boss、Reviewer、Worker 和未来第三方 Agent 是否遵守同一套恢复、审计和预算语义。

把一次 Agent 激活直接当成 Attempt 会导致恢复时复活旧执行权；把每个模型 Turn 都提交到中央数据库
则会增加高频写入和控制耦合；用固定 heartbeat 周期不断唤醒又会制造空调用、预算浪费和错误的
“在线即工作”假象。

因此需要在任务事实和具体 Agent Adapter 之间增加一个耐久、低频、可审计的调用编排层。

## 2. 决策

调用链固定为：

```text
DomainEvent / OperatorCommand / Obligation / Typed Schedule
                          |
                          v
                      RunSignal
                          |
              normalize + authorize + coalesce
                          |
                          v
                   InvocationIntent
                          |
            route + reserve budget + create claim
                          |
                          v
                    InvocationRun
                          |
                     AgentAdapter
                          |
           structured outcome + SessionCapsule
```

采用以下独立对象：

1. `RunSignal` 是某个可验证因果事实产生的不可变调用输入；
2. `InvocationIntent` 是对一个或多个同义 Signal 合并后的待调度请求；
3. `InvocationRun` 是一次有边界的 Agent 激活窗口；
4. `RunClaim` 是执行某个 InvocationRun 的短期、可 fencing 权利；
5. `SessionCapsule` 是内容寻址、不可变、可恢复的最小上下文快照；
6. `BudgetReservation` 在 Claim/Run 启动前原子预留资源，并在终结时结算或释放。

这些对象以 PostgreSQL 为中央事实来源。模型会话、Worker 进程 PID、SSE 连接、Node 在线状态和
Adapter 自报状态都不是 InvocationRun 的权威状态。

## 3. 对现有对象的严格边界

| 对象 | 语义 | 明确不等于 |
| --- | --- | --- |
| `WorkPackage` | 不可变任务 revision 的业务进度 | Agent 调用、Ticket 或聊天线程 |
| `Attempt` | 某 Executor 对固定 revision 的一次语义开发尝试 | 进程启动次数或模型 Turn |
| 作者 `Lease` | 对作者侧正式 mutation 的当前执行权 | Agent 在线、Run claim 或预算 |
| `InvocationRun` | 一次启动、恢复或诊断激活窗口 | Attempt、Candidate 或任务完成 |
| `RunClaim` | 报告该 Run 结果的短期权利 | 作者侧 Artifact/Candidate 写权限 |
| Worker `Turn` | 本地 Turn Pump 的一次模型交互 | 中央 InvocationRun 或业务状态转移 |
| `VerificationRun` | Candidate 的独立验收流程 | Agent 调用账本中的 InvocationRun |

一个 Attempt 可以产生多个 InvocationRun；一个 InvocationRun 可以在 Worker 本地包含多个 Turn。
`InvocationRun` 完成只产生结构化 outcome 和后继 Signal，不能直接把 Attempt、WorkPackage、
VerificationRun 或 Submission 置为成功。

## 4. 事件驱动而非 heartbeat 驱动

正常唤醒来源只有：

- 新 Lease/Assignment；
- 已满足的 typed wake condition；
- 审批 Decision、问题答案或新 Artifact；
- 新的语义进度截止、预算阈值或安全事件；
- Obligation 到期；
- 明确配置的 Routine/Schedule；
- 有权限的人工 typed command。

固定 heartbeat 只允许作为 NodeSession 健康探测或极低频 reconciliation fallback，不能直接产生模型
调用，也不能刷新语义进度。Routine 到期也必须先创建可追踪的 Signal/WorkPackage，不能绕过预算、
能力路由和验收直接调用模型。

## 5. Signal 合并边界

同义 Signal 只有在以下 binding 全部相同时才可以合并到同一个 active Intent：

```text
tenant/project
+ subject type/id
+ package revision/hash（若适用）
+ attempt id/fencing generation（若适用）
+ signal class + normalized reason
+ policy revision
+ workspace/base/capsule generation（若适用）
```

以下事实永远不与旧 Intent 合并：

- AFWP 或 Project Contract revision 变化；
- 新 Attempt、重新分配或 fencing generation 变化；
- 取消、撤销、隔离和安全终止；
- 权限批准、拒绝、过期或 action digest 变化；
- 目标 Git baseline 变化；
- 操作者显式 `force_new_run` 且具有对应权限；
- 旧 Run outcome unknown 后的恢复诊断。

合并只减少重复唤醒，不删除原始 Signal。每个 Signal 与它被折叠到的 Intent 都必须可审计。

## 6. 上下文与会话

InvocationRun 启动时固定：

- Project/Package/Attempt 及其版本；
- AFWP hash、base Commit、Workspace Head；
- author Lease generation 或 service job/queue claim；
- Executor fingerprint、Adapter 和 Prompt pack digest；
- PolicyRevision 与 RoutingDecision；
- 输入 SessionCapsule digest；
- BudgetReservation；
- 本次 Signal 集合及 causation。

运行中到达的新评论、审批、Package revision 或策略变化不能原地改写上述上下文，只能追加新
RunSignal。当前 Run 可以安全结束、进入等待或被取消；后继调用创建新的 InvocationRun。

SessionCapsule 只保存恢复所需的结构化状态和内容引用，包括 checkpoint、AC 矩阵、开放 blocker、
已批准 Decision、Adapter session ref 和 runtime fingerprint。它不得默认保存完整 Prompt、隐藏
chain-of-thought、Secret、可用 bearer 或未授权源码。

## 7. Claim、fencing 与预算

作者 `Lease` 和 `RunClaim` 使用不同表、不同 generation、不同 TTL 和不同 capability：

- 作者 Lease 决定能否执行 progress/checkpoint/CandidateArtifact/Candidate 等正式 mutation；
- RunClaim 只决定谁可以启动 Adapter、报告 InvocationRun outcome 和登记下一个 Capsule；
- 只有 RunClaim 没有作者 Lease时，Run 可以做只读诊断，但不能创建正式作者制品；
- RunClaim 过期或被 supersede 后，旧执行器的 receipt-miss 结果必须被拒绝；
- 恢复创建新的 InvocationRun，不把旧 Run 状态改回运行态。

创建 Attempt/Lease 或 InvocationRun 时，预算 reservation 与领域状态在同一 PostgreSQL 事务提交。
作者预算不得占用 AFWP 明确保留给 Runner、Reviewer、Artifact 和 Integration 的额度。外部 Adapter
调用发生在事务提交后，任何预算不足都必须在调用前失败。

## 8. 外部调用与 outcome unknown

数据库事务内禁止调用模型、Agent Adapter、Git、对象存储或远程 KMS。流程为：

1. 事务内创建 Intent、InvocationRun、RunClaim、BudgetReservation、事件与 Outbox；
2. 事务提交后由 Dispatcher 调用 Adapter；
3. Adapter 使用稳定 `invocation_key`，并优先支持查询/恢复；
4. 结果以 RunClaim、generation、Run version、输入/输出 digest 回写；
5. 崩溃或超时后先进入 `RECONCILING`，查询 Adapter/session/history；
6. 能证明已完成则登记原结果；能证明未执行才创建后继 Run；无法证明时终结旧 Run 为
   `FAILED/OUTCOME_UNKNOWN`，生成恢复诊断 Signal，禁止原样盲重发非幂等调用。

旧 InvocationRun 不复活。后继 Run 必须引用旧 Run、reconciliation evidence 和新的预算 reservation。

## 9. 与 Obligation Engine 的关系

Obligation 表示“必须完成什么”，Invocation 表示“这次实际调用了谁”。

- 确定性 Obligation handler 可以直接执行状态维护；
- 需要 Agent 的 handler 创建 RunSignal/InvocationIntent 后释放 claim；
- InvocationRun outcome 不是 Obligation fulfilled 的充分条件；
- 只有确定性 handler 验证 PlanPatch、Candidate、Review、Decision 或 Git Receipt 等交付物后，才能
  `FulfillObligation`；
- 重复事件依靠 Obligation dedup 与 Intent coalescing 两层保护，但二者业务键不同，不可共用状态表。

## 10. 安全和隐私

- Signal、Intent、Run、Capsule 与 usage 记录不得包含 Secret 或完整 chain-of-thought；
- Adapter 只能得到本次 Run 的短期 capability 和最小内容引用；
- 操作者评论作为不可信上下文，不能扩大 AFWP、权限、路径或验收；
- `force_new_run`、取消、隔离和预算追加是显式 typed command，必须有 actor、理由、幂等键和 CAS；
- restricted 项目使用获批的 Adapter/模型路由和独立加密域；
- UI 展示的“Running/Waiting/Degraded”由服务器事实推导，不能信任 Agent 自报。

## 11. 结果

### 11.1 正面结果

- 所有 Agent 类型共享可审计的调用原因、预算和恢复语义；
- 重复事件不会制造调用风暴，新因果事实又不会被误吞；
- Attempt、作者权限和短期进程激活保持分离；
- Boss/Worker 退出后仍能依靠持久 Signal、Intent 和 Obligation 继续；
- 低频事件驱动适合 2C4G 控制面和不稳定边缘节点；
- SessionCapsule 使恢复不依赖隐藏聊天历史。

### 11.2 代价

- 增加 Signal、Intent、InvocationRun、RunClaim、Capsule 和预算 reservation 表；
- Adapter 必须实现稳定调用键、结果查询或明确的不可对账降级；
- UI/运维需要区分 Attempt、InvocationRun 和 VerificationRun；
- coalescing key 与恢复矩阵必须经过并发和崩溃属性测试。

## 12. 被否决的方案

### 12.1 定时 heartbeat 直接调用 Agent

拒绝。空唤醒浪费预算，heartbeat 不能表达因果、优先级和精确上下文，也容易把在线状态误当进展。

### 12.2 把 InvocationRun 合并进 Attempt

拒绝。进程重启、等待和诊断会复活或污染语义 Attempt，无法区分任务重做与会话恢复。

### 12.3 中央数据库持久化每个模型 Turn

拒绝。Turn Pump 已由 Worker Journal 保证耐久；中央只需要有边界的激活窗口和摘要，否则会造成
高频写、隐私扩大和对具体 Agent harness 的耦合。

### 12.4 常驻 Boss 会话充当调度器

拒绝。会话结束、上下文压缩和进程故障会丢监督义务；Obligation 与 Invocation 账本才是事实。

## 13. 验证标准

1. 同一因果 Signal 重放 100 次只创建一个 active Intent 和一次有效 InvocationRun；
2. revision、generation、权限 Decision 或取消变化分别产生新 Intent，不能与旧项合并；
3. 一个 Attempt 可连续产生多个 InvocationRun，但任一 Run 结束都不能直接把 Attempt/Package 置终态；
4. Claim/Run 与预算 reservation 同事务，32 路并发只允许预算内的 Run 成功；
5. RunClaim 到期或 supersede 后，旧执行器的 receipt-miss outcome 被 fencing；
6. Dispatcher 在事务提交前不会调用 Adapter；事务回滚时外部调用数为 0；
7. Adapter 已接收、ACK 丢失时恢复先查询 session/history，不重复发送同一非幂等调用；
8. 输入 Capsule digest、AFWP hash、Workspace Head 或 Executor fingerprint 任一改变，旧 Run 结果不能绑定；
9. Capsule、事件、日志和 UI projection 的 secret/chain-of-thought 扫描为零发现；
10. Boss 和 Worker 进程全部退出后，持久 Signal/Intent/Obligation 能由新实例恢复；
11. 节点没有每秒心跳时，事件、低频 reconciliation 和 Lease 服务器时间仍能正确推进；
12. PostgreSQL 从空 projection 重放后得到相同 Intent/Run/预算摘要。

## 14. 后续影响

- M1 必须先落领域状态、原子预算和投影，再开放实际 Adapter 调用；
- M2 的 jcode Bridge 实现 `AgentAdapter` 和 SessionCapsule 对账，但本地 Turn Pump 仍保持权威；
- M5 的 Boss、路由和策略复用同一 Invocation 账本；
- A2A Task 是 InvocationRun 的边界投影，不取代内部对象；
- 任何新增 Agent Adapter 若不能对 outcome unknown 提供查询/恢复，必须声明降级策略并限制为可安全重做的任务。
