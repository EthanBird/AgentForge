# AgentForge

AgentForge 是一套面向分布式 AI Agent 的异步软件工厂：高能力 Boss 把用户目标编译为可验证的任务包，悬赏控制平面按能力、质量、成本和可用性选择 Worker，Worker 在隔离环境中通过 jcode 等执行器完成开发、验证和提交，最终由独立 Runner 与 Git Merge Queue 把成果送入目标代码库。

项目当前处于 **协议与工程设计阶段**。仓库中的文档已经把总体思想细化为可直接实现的领域模型、JSON Schema、API、数据库事务、Worker 状态机、Git Relay、测试矩阵和首批开发工单。

## 核心原则

- 任务包是不可变的执行契约，模型只是可替换的执行资源。
- `WorkPackage`、`Attempt`、`Lease`、`CandidateArtifact`、`Candidate`、`VerificationRun`、`Submission` 分离建模。
- Boss 可以结束一次会话，但服务器中的监督义务必须持续存在。
- jcode 的一次回复结束不等于任务结束；确定性 Supervisor 驱动下一回合。
- 作者不能成为自己代码的唯一 Reviewer。
- 本地测试、独立审查和正式提交必须绑定同一个精确 Candidate Commit；集成可产生新的 Integration Commit，但必须记录 Candidate、目标基线并在该 Integration Commit 上重跑集成门禁。
- Worker 不直接写保护分支；所有代码经过任务分支、证据包与集成门禁。
- 至少一次消息投递配合幂等键、CAS 和 fencing token，而不是宣称虚假的“恰好一次”。

## 文档入口

| 文档 | 用途 |
| --- | --- |
| [总体架构](docs/architecture/AGENT_FACTORY_ARCHITECTURE.md) | 完整理念、角色、任务图、Worker、验收与路线图 |
| [执行总计划](docs/development/00_EXECUTIVE_IMPLEMENTATION_PLAN.md) | MVP 边界、代码结构、实施顺序和阶段门禁 |
| [领域模型与状态机](docs/development/01_DOMAIN_MODEL_AND_STATE_MACHINES.md) | Rust 聚合、状态转移、不变量和并发规则 |
| [AFWP 协议](docs/development/02_AFWP_PROTOCOL_SPEC.md) | 任务包协议、版本和规范化哈希 |
| [控制平面实现](docs/development/03_CONTROL_PLANE_IMPLEMENTATION.md) | PostgreSQL、API、Outbox、Obligation Engine |
| [Worker 与 jcode](docs/development/04_WORKER_RUNTIME_JCODE.md) | Worker Daemon、jcode sidecar、Turn Pump 和恢复 |
| [能力匹配与悬赏](docs/development/05_MATCHING_BOUNTY_CAPABILITY.md) | Executor 画像、报价、路由和信誉 |
| [验收与 Git Relay](docs/development/06_VERIFICATION_GIT_RELAY.md) | Evidence、独立 Runner、Bundle 与 Merge Queue |
| [安全威胁模型](docs/development/07_SECURITY_THREAT_MODEL.md) | 信任边界、凭据、沙箱和安全门禁 |
| [测试与验证计划](docs/development/08_TEST_AND_VALIDATION_PLAN.md) | 单测、属性测试、并发、混沌和端到端验收 |
| [里程碑与首批工单](docs/development/09_MILESTONES_AND_WORK_PACKAGES.md) | 可直接发布的 Epic、任务依赖和逐项验收 |
| [本地开发与部署](docs/development/10_LOCAL_DEV_AND_DEPLOYMENT.md) | 目标命令、配置、单机部署与运维 |
| [可行性报告](docs/feasibility/FEASIBILITY_REPORT.md) | 官方能力核验、实测结果、限制和 PoC 门禁 |
| [ADR 索引](docs/adr/README.md) | 已冻结的关键架构决定 |

协议 Schema 与样例位于 [`schemas/`](schemas/) 和 [`examples/`](examples/)。

## 第一阶段目标

第一阶段不追求完整多模型市场，而是证明最小纵向闭环：

```text
创建 AFWP
  -> 发布 Offer
  -> Worker 原子领单和获得 Lease
  -> 运行可恢复 Attempt
  -> 在有效 Lease 下完成 Candidate Artifact 并登记固定 Candidate
  -> 独立 Review 与干净复现
  -> 创建签名终态 Submission
  -> Git Relay 写入任务分支
  -> Merge Queue 集成
```

只有这个闭环在重复消息、Worker 重启、Lease 过期和分支冲突下仍保持正确，才进入动态 Boss、复杂路由和跨项目扩展。

## 许可证

[MIT License](LICENSE)
