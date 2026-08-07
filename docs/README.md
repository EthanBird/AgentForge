# AgentForge 文档索引

## 阅读顺序

1. 先读[总体架构](architecture/AGENT_FACTORY_ARCHITECTURE.md)，理解系统哲学和边界。
2. 再读[执行总计划](development/00_EXECUTIVE_IMPLEMENTATION_PLAN.md)，确定 MVP 要实现和暂不实现的内容。
3. 开发控制平面前阅读领域模型、AFWP 协议与控制平面实现文档。
4. 开发节点前阅读 Worker/jcode、验收/Git Relay 与安全文档。
5. 每个里程碑开始前，从[首批工单](development/09_MILESTONES_AND_WORK_PACKAGES.md)领取任务，并按[测试计划](development/08_TEST_AND_VALIDATION_PLAN.md)执行门禁。

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
