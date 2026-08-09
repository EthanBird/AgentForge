# AgentForge 文档索引

## 阅读顺序

1. 先读[总体架构](architecture/AGENT_FACTORY_ARCHITECTURE.md)，理解系统哲学和边界。
2. 再读[执行总计划](development/00_EXECUTIVE_IMPLEMENTATION_PLAN.md)，确定 MVP 要实现和暂不实现的内容。
3. 开发控制平面前阅读[领域模型](development/01_DOMAIN_MODEL_AND_STATE_MACHINES.md)、AFWP 协议、
   [控制平面实现](development/03_CONTROL_PLANE_IMPLEMENTATION.md)与
   [调用编排规范](development/11_INVOCATION_ORCHESTRATION.md)。
4. 开发治理或 Web UI 前阅读[治理决策台与 Control Room UI](development/12_GOVERNANCE_CONTROL_ROOM_UI.md)，
   并同时遵守[安全威胁模型](development/07_SECURITY_THREAT_MODEL.md)。
5. 开发节点前阅读 Worker/jcode、验收/Git Relay 与安全文档。
6. 每个里程碑开始前，从[首批工单](development/09_MILESTONES_AND_WORK_PACKAGES.md)领取任务，并按[测试计划](development/08_TEST_AND_VALIDATION_PLAN.md)执行门禁。
7. 当前实现状态、M0 验收证据与后续边界见 [M0 进度报告](development/M0_PROGRESS_REPORT.md)。
8. M1 当前开发范围、提交检查点和 Paperclip 控制面改造见 [M1 进度报告](development/M1_PROGRESS_REPORT.md)。

## 文档权威级别

当文档发生冲突时，优先级从高到低为：

1. 已接受且未被替代的 ADR；
2. `schemas/` 中当前 major 版本的 JSON Schema；
3. 领域模型与状态机文档中的系统不变量；
4. 具体组件开发文档；
5. 总体架构中的解释性描述；
6. README 和示例。

Schema 与代码实现不一致时不得静默兼容：必须修正实现、升协议版本，或提交 ADR 明确改变契约。

## 目录

- `architecture/`：总体架构和系统级说明；
- `development/`：可直接实施的组件规格、验证与工单；
- `feasibility/`：技术选择的可行性证据和限制；
- `adr/`：不可被局部实现随意改变的架构决策。

## M1-A 冻结规格

- [ADR-0006：Invocation Orchestration](adr/ADR-0006-INVOCATION-ORCHESTRATION.md)：冻结
  `RunSignal -> InvocationIntent -> InvocationRun`、RunClaim、SessionCapsule、原子预算与 outcome-unknown 对账；
- [ADR-0007：Governance Decision Desk](adr/ADR-0007-GOVERNANCE-DECISION-DESK.md)：冻结
  GovernanceCase、不可变 Decision、PolicyRevision、投影与 typed command；
- [11：Agent 调用编排](development/11_INVOCATION_ORCHESTRATION.md)：给出领域结构、数据库、Adapter、API 和恢复矩阵；
- [12：治理决策台与 Control Room UI](development/12_GOVERNANCE_CONTROL_ROOM_UI.md)：给出治理聚合、read model、Query/Command API、桌面/窄屏交互和 hard AC。
