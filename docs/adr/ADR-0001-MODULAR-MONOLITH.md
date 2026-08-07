# ADR-0001：MVP 采用 Rust 模块化单体控制平面

- 状态：Accepted
- 日期：2026-08-07
- 决策者：AgentForge 架构组
- 影响范围：中央悬赏服务器、控制平面部署、Rust workspace 边界
- 复审触发：达到本文第 8 节任一拆分阈值

## 1. 背景

AgentForge 的中央服务器负责项目、任务图、WorkPackage、Attempt、Lease、Candidate、VerificationRun、Submission、Integration、Obligation 与事件账本。它不运行大模型和主要编译负载。首期目标机器只有 2 核 4 GB，且最难的问题是先把任务契约、状态机、租约 fencing、验收和 Git 集成语义做正确。

如果首版直接拆成 Package Service、Lease Service、Graph Service、Verification Service、Obligation Service 等网络微服务，会立即引入：

- 分布式事务或补偿协议；
- 跨服务状态读取的竞态和延迟；
- 每个进程的 runtime、连接池、指标与 TLS 常驻开销；
- API/事件版本协调和本地开发复杂度；
- 2C4G 主机上的额外运维面；
- 在领域边界尚未稳定前的错误拆分成本。

另一方面，将全部代码写进一个 crate、让 HTTP handler 直接执行 SQL，也会导致状态规则重复、测试困难和未来无法拆分。

## 2. 决策

MVP 使用一个 Rust 可执行进程 `agent-factoryd`，部署为一个控制平面实例；进程内部采用严格模块化单体：

```text
HTTP / Background Jobs
          |
          v
   Application Use Cases
          |
          v
      Pure Domain
          ^
          |
PostgreSQL / Outbox / Git ports
```

部署单元是单体，代码与所有权不是“大泥球”。领域模块通过 Rust crate、显式 application ports 和数据库表所有权隔离。

### 2.1 固定 crate 边界

| crate | 所有权 | 允许的对外接口 |
| --- | --- | --- |
| `agentforge-domain` | 聚合、状态机、不变量、领域错误 | 纯 Rust 类型与纯函数 |
| `agentforge-application` | 命令处理、授权、事务编排 | use-case traits/handlers |
| `agentforge-protocol` | AFWP/API DTO 和版本化 Schema | 序列化 DTO，不暴露 DB row |
| `agentforge-storage-postgres` | repository、迁移、锁和 CAS | application port 实现 |
| `agentforge-matcher` | Offer/Bid 硬过滤和评分 | 确定性匹配输入/输出 |
| `agentforge-obligation-engine` | 到期义务领取、重试和升级 | 领域命令，不直改业务状态 |
| `agentforge-outbox` | Outbox publish、Inbox、SSE | 至少一次传递适配 |
| `agentforge-control-plane` | Axum route、配置、组合根 | 二进制，不承载领域规则 |

### 2.2 依赖方向

依赖只能向内：

```text
control-plane -> application -> domain
storage-postgres -> application ports + domain
obligation/outbox/matcher -> application + domain
```

禁止：

- `domain` 依赖 SQLx、Axum、NATS、Git、对象存储或 jcode；
- crate 通过彼此私有数据库表作为隐式 API；
- Axum handler 直接写状态表；
- 后台任务绕过 application handler 修改领域状态；
- 为了“解耦”在同一进程内部先引入 JSON/HTTP 自调用。

### 2.3 事务边界

一个业务命令由 application 层开启一个 Unit of Work。需要原子协调的领域对象可以在同一 PostgreSQL 事务内更新，例如 Claim 同时更新 WorkPackage 并创建 Attempt、Lease。模块化边界不阻止必要的本地事务一致性。

对 Git、模型、对象存储、Webhook 或消息 Broker 的调用一律在事务外，通过 Outbox 和 Obligation 驱动。这样既保留本地事务优势，又避免把慢外部 I/O 放进数据库锁区。

### 2.4 进程内后台任务

Lease expiry、Obligation scheduler、Outbox publisher 与 API 同进程运行，但必须：

- 使用独立 cancellation token；
- 有自己的并发 semaphore；
- 共享有限 SQLx pool，不各建连接池；
- 使用数据库 claim/锁支持多个实例重复运行；
- 不依赖“只有一个进程”保证正确性。

因此未来运行两个 `agent-factoryd` 实例时，`SKIP LOCKED`、唯一约束和 CAS 仍能保证正确结果。

## 3. 为什么该决策符合领域

AgentForge 的关键原子操作天然跨越多个概念：

- Claim：Package + Attempt + Lease；
- Candidate registration：Lease + Attempt + Candidate + VerificationRun + Package；
- Verification finalization：Candidate + VerificationRun + terminal Submission + Attempt + Package；
- Evaluation：Submission + Attempt + Package + Obligation；
- Integration：Submission + Git candidate + Package + dependency readiness。

在边界尚未通过真实负载验证前，将它们拆成网络服务会把清晰的数据库事务变成 Saga，并增加大量“半完成”状态。模块化单体允许先以一个线性化点证明核心不变量，再根据测量结果拆分无状态或弱一致部分。

## 4. 结果

### 4.1 正面结果

- 单一部署物，适合 2C4G；
- 核心状态变化可以用单库事务、行锁、CAS 和唯一约束证明；
- 本地开发与故障注入简单；
- 一个 trace 可覆盖完整命令；
- 不需要在领域尚未稳定时承诺远程服务协议；
- crate 边界保留未来拆分可能。

### 4.2 负面结果

- 任一模块的 panic/内存泄漏可能影响整个进程；
- 所有模块使用同一发布节奏；
- 一个数据库和一个进程仍是可用性故障域；
- 团队必须靠依赖规则和审查维持边界，编译器不会阻止所有数据库越界；
- CPU 密集工作若错误放进控制平面会拖慢其他模块。

### 4.3 缓解措施

- `panic=abort` 与 systemd/container 自动重启；handler 禁止未捕获 panic；
- 模型、构建、测试、Git pack 等 CPU/内存重活必须留在 Worker/Relay；
- route/body/并发限制和数据库 statement timeout；
- 用 `cargo metadata` CI 规则检查 crate 依赖；
- 数据库迁移按表所有者审查；跨模块查询进入专用 read model；
- 所有后台任务可重入、可恢复，进程重启不丢监督义务。

## 5. 模块交互规则

模块之间有三种合法交互：

1. application handler 直接调用纯 domain 函数；
2. 同一 Unit of Work 内通过 repository port 读取/写入必要聚合；
3. 事务提交后通过 domain event/Outbox 触发异步后继工作。

不允许的交互：

- crate A 读取 crate B 的私有 row struct；
- handler A 通过 localhost HTTP 调 handler B；
- 仅为调用顺序而发一个无法审计的进程内 channel 消息；
- 消费事件后无幂等保护地重复外部副作用。

读侧可以建立跨模块 projection，但 projection 只用于查询，不能作为 Lease/验收等授权判断的事实来源。

## 6. 数据库所有权

| 模块 | 主写表 |
| --- | --- |
| Project/WorkGraph | `projects`、`graph_versions`、`package_edges` |
| Package | `work_packages`、`package_revisions`、`acceptance_criteria` |
| Execution | `attempts`、`leases`、`progress`、`checkpoints` |
| Verification | `submissions`、`criterion_results`、`reviews`、`findings` |
| Supervision | `obligations` |
| Eventing | `domain_events`、`outbox`、`inbox_messages`、`command_receipts` |

“所有权”表示只有对应 application use case 可以修改；它不表示必须拆 schema 或数据库。跨表外键和事务在 MVP 中是刻意保留的优势。

## 7. 被否决的方案

### 7.1 首版微服务

否决原因：资源和运维开销高；Lease/Attempt/Package 需要 Saga；边界尚未被真实业务验证；测试复杂度超过收益。

### 7.2 单 crate + handler 直连 SQL

否决原因：状态机和权限会散落；无法进行纯领域属性测试；未来拆分代价高；极易出现后台任务绕过 fencing。

### 7.3 Temporal 作为首版控制核心

否决原因：当前关键风险是 AFWP、Lease、Submission 和 Git 语义，而非通用 workflow runtime；在 2C4G 上引入额外服务和认知负担不划算。未来可以把 Obligation 映射到 Temporal，但领域协议不依赖它。

### 7.4 Serverless Functions

否决原因：长连接/SSE、数据库连接管理、低延迟租约续期与本地部署不匹配；函数重试语义不能替代业务幂等。

## 8. 何时允许拆成独立服务

只有出现以下至少一项，并完成测量和新 ADR 后才能拆分：

- 单模块 CPU 或内存持续占控制平面资源 40% 以上，且不能通过 Worker 外移；
- 独立扩缩需求达到 10 倍以上差异；
- 安全域要求进程级/网络级隔离，例如公网 Gateway 与私有 Git Broker；
- 发布节奏冲突实际造成频繁事故；
- 单个 PostgreSQL 写入达到可证明瓶颈，且该模块能以清晰一致性边界独立；
- 运维要求多区域高可用，单体故障域不再可接受。

优先拆出的候选通常是无状态 Gateway、对象制品服务、Matcher 评测计算或 Git Relay，而不是紧耦合的 Package/Attempt/Lease 事务核心。

拆分前必须明确：

- 新服务拥有的事实和唯一写入者；
- API/事件版本与幂等键；
- 失败时的补偿和重放；
- 原本单事务不变量如何维持；
- 回滚到单体版本的方法。

## 9. 验证与验收

### 9.1 架构检查

- `agentforge-domain` 的传递依赖中无 I/O 框架；
- 所有 route 只调用 application command/query；
- 所有业务表 UPDATE 可追溯到一个命名 use case；
- 后台任务没有直接绕过 Lease guard；
- 外部 I/O 不发生在打开的数据库事务中。

### 9.2 运行验收

- 在 2C4G 环境下单进程 RSS 目标低于 300 MiB（`TARGET_NOT_VALIDATED`）；
- 一个实例崩溃并重启后，Lease、Outbox、Obligation 能从 PostgreSQL 恢复；
- 同时运行两个实例时，不出现双 Lease、重复履行或丢 Outbox；
- API 和后台任务共享 16 个连接时，先通过 20 req/s、100 Active Lease 的 30 分钟压力余量预检，再以 100 Worker、30 Active Attempt 完成 24 小时发布 soak；
- 关停时 readiness 先撤除，10 秒内完成短事务并退出。

## 10. 后续影响

所有新增控制平面功能默认加入现有模块化单体。提议新网络服务者负有举证责任：必须提供测量数据、事务语义、故障模型和回滚方案，而不能只以“微服务更先进”为理由拆分。
