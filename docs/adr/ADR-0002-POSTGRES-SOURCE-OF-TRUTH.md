# ADR-0002：PostgreSQL 是控制平面唯一业务事实来源

- 状态：Accepted
- 日期：2026-08-07
- 决策者：AgentForge 架构组
- 影响范围：状态持久化、并发控制、事件、队列、恢复与审计
- 相关决策：ADR-0001

## 1. 背景

AgentForge 必须在 Worker 断线、请求重试、服务器重启、消息重复和旧 Worker 迟到提交时仍能回答：

- 当前 WorkPackage revision 是什么；
- 哪个 Attempt 拥有执行权；
- 当前 fencing token 是多少；
- Lease 是否已经按服务器时间过期；
- 哪个精确 Candidate Commit 被测试、审查和接受，以及它最终绑定到哪个经 L5 复验的 Integration Commit；
- 哪项监督义务尚未履行；
- 某个副作用是否已经执行。

消息 Broker 的 ACK、Worker 本地 Journal、Boss 会话、Git 分支、内存队列都只能提供部分信息，无法单独维持这些跨对象不变量。MVP 运行在 2 核 4 GB 主机，也不适合同时维护多个强一致基础设施。

## 2. 决策

PostgreSQL 是中央控制平面唯一业务事实来源（source of truth）。

具体采用“关系当前状态 + append-only 领域事件 + Transactional Outbox”的混合模式：

```text
同一 PostgreSQL 事务
  1. 认证 actor、规范化请求，锁定 actor + 幂等键；命中同 request hash 立即回放首次结果
  2. 仅在回执未命中时校验权限、expected version、Lease token 和当前状态
  3. 更新关系状态投影
  4. 追加领域事件
  5. 写 Outbox 与新 Obligation
  6. 写命令回执
COMMIT

提交后
  Outbox 至少一次发布
  外部消费者用 Inbox 去重
```

不采用“Broker 中的消息状态即任务状态”，也不在 MVP 实现每次读取都必须重放的纯事件溯源。

## 3. 事实层级

| 数据 | 权威来源 | 说明 |
| --- | --- | --- |
| Package/Attempt/Lease/Candidate/VerificationRun/Submission/Integration 当前状态 | PostgreSQL 状态表 | 命令授权与状态判断只读这里 |
| 领域状态变化审计 | `domain_events` | append-only，可重建派生 projection |
| 待投递消息 | `outbox` | 与状态同事务写入 |
| 未履行监督责任 | `obligations` | 持久 timer/claim，不依赖 Boss 在线 |
| 幂等命令结果 | `command_receipts` | 相同 actor+key 原样重放 |
| Git 代码内容 | 局域网 Git | PostgreSQL 保存 commit/tree/base/签名元数据 |
| 大证据内容 | 对象存储 | PostgreSQL 保存 URI、摘要、大小、签名 |
| Worker 离线进度 | Worker Journal | 重连后提交，未被服务器接受前不是正式事实 |
| NATS/其他 Broker 消息 | 传递副本 | ACK 不代表业务完成 |

“唯一事实来源”不表示把所有二进制塞进 PostgreSQL。代码和大制品各有内容系统；但它们是否被某任务正式接受、对应哪个 digest/commit，由 PostgreSQL 记录决定。

## 4. 一致性与事务规则

### 4.1 隔离级别

- 普通命令使用 `READ COMMITTED` + `SELECT ... FOR UPDATE` + version CAS + 唯一约束；
- `ApplyPlanPatch`、需要整体检查图版本/无环性的命令使用 `SERIALIZABLE`；
- 不使用 `READ UNCOMMITTED`；
- 长报表使用只读事务或 replica，不持有业务行锁；
- 遇到 serialization failure 只可重试确定性事务，最多 3 次。

### 4.2 线性化点

| 命令 | 线性化点 |
| --- | --- |
| Claim Package | WorkPackage CAS 更新 + Active Lease 插入所在事务提交 |
| Renew Lease | 带 token/version/expiry 条件的 Lease CAS 更新 |
| Record Candidate | Lease 守卫、Candidate/VerificationRun 插入和 Package/Attempt 更新事务提交 |
| Finalize Verification | 终态 run 校验、不可变 Submission 插入和 Package/Attempt 更新事务提交 |
| Apply PlanPatch | Project.graph_version CAS 所在 Serializable 事务提交 |
| Fulfill Obligation | claim token CAS + evidence 写入事务提交 |

HTTP 2xx、Broker ACK、Worker 日志或 Git push 都不是上述业务动作的线性化点。

### 4.3 锁顺序

跨聚合事务按固定顺序：

```text
project -> work_graph -> work_package -> attempt -> lease -> candidate -> verification_run -> submission -> integration -> obligation
```

多个同类对象按 UUID 字节序排序后加锁。所有事务设置 `idle_in_transaction_session_timeout=10s`；API statement timeout 5 秒，后台任务 15 秒。

### 4.4 服务器时间

Lease、claim、Obligation 到期使用 `clock_timestamp()`。Worker 时间只作为 payload 的展示信息，不能延长执行权。续租 SQL 必须包含 `expires_at > clock_timestamp()`，即使过期 sweeper 尚未运行也不能复活 Lease。

## 5. CAS、唯一约束与 fencing

正确性不能只依赖 application 层 if 判断：

- 每个聚合状态行含 `version bigint`；更新使用 `WHERE version=$expected`；
- 同 revision 只有一个 `state='ACTIVE'` Lease，由 partial unique index保证；
- `(revision_id,fencing_token)` 唯一；token 在锁住 WorkPackage 后单调递增；
- `(aggregate_type,aggregate_id,aggregate_seq)` 唯一；
- `(actor_id,idempotency_key)` 唯一；
- `(consumer,message_id)` 唯一；
- 活跃 Obligation 的 `(project_id,dedup_key)` 唯一。

应用层校验提供清晰错误，数据库约束提供最后防线。约束冲突必须映射为稳定领域错误，不向客户端暴露 SQL constraint 名称。

## 6. 事件模型不是纯 Event Sourcing

### 6.1 决策理由

纯事件溯源可以保留完整历史，但首版会额外引入：

- 聚合重放和 snapshot 策略；
- 事件 upcaster 与长历史性能；
- 跨聚合事务/唯一约束表达困难；
- 运维人员查询当前状态的复杂度；
- 在领域仍快速变化时的事件设计成本。

因此 MVP 直接读取规范化关系状态，`domain_events` 保留不可变审计和投递来源。每个命令同时写二者，任何一方写失败则整个事务回滚。

### 6.2 事件约束

- 单 Aggregate 内 `aggregate_seq` 严格递增；
- 不宣称不同 Aggregate 的全局顺序；
- payload 有 `schema_version`，只追加字段不改旧事件；
- 破坏性结构变化增加新事件类型或 upcaster；
- 事件不得包含密钥、完整 chain-of-thought、大日志或大二进制；
- 修正错误事实用补偿事件，不 UPDATE/DELETE 旧事件。

状态行和事件是否一致由事务保证，并通过周期性审计任务抽样检查 `version/event_seq` 和最后事件。若发现不一致，控制平面应进入相应聚合的写保护并产生运维义务，而不是猜测修复。

## 7. Transactional Outbox 和 Inbox

### 7.1 Outbox

每个需要外部观察的领域事件在同事务写一条 Outbox。Publisher 使用 `FOR UPDATE SKIP LOCKED` 领取小批次，事务外发布，收到 ACK 后标记完成。

这提供“数据库提交后至少一次投递”，不提供恰好一次。以下崩溃窗口是设计内行为：

```text
broker 已接收 -> publisher 尚未标记 PUBLISHED -> 进程崩溃
```

恢复后消息会重复，这是允许的；消费者必须以 `message_id` 去重。

### 7.2 Inbox

内部消费者在执行业务副作用前先在同一事务插入 `(consumer,message_id,payload_hash)`：

- 首次插入：执行命令并保存 result；
- 相同 message ID + 相同 hash：返回已有结果；
- 相同 message ID + 不同 hash：隔离并告警，不能覆盖。

外部 Git Relay、Verifier 等也必须接受 AgentForge 提供的 idempotency key；若外部系统不支持幂等，需要在 adapter 中先查询可观察结果（例如目标 Git ref 是否已经指向 commit），不能盲目重做。

### 7.3 Broker 的位置

MVP 可以不部署 NATS，使用 PostgreSQL Outbox + SSE/长轮询。未来加入 JetStream 后：

- PostgreSQL 状态不迁移到 Stream；
- Stream retention/consumer cursor 不决定 Package 状态；
- ACK 只表示消费者收到消息；
- Worker 所有副作用仍调用写 API，验证 expected version 和 fencing token；
- Broker 丢失可由 Outbox/事件重新投递。

## 8. Obligation 作为持久定时器

监督义务保存在 PostgreSQL，不由内存 timer 或常驻 Boss 会话持有。`due_at` 索引和 `SKIP LOCKED` 支持多个 scheduler 实例并发领取；`claim_token` 阻止超时执行器迟到履行。

选择数据库 scheduler 的原因：

- MVP 定时器规模预计远低于百万级；
- 可与触发状态在同事务创建，避免“状态提交但 timer 丢失”；
- 无需在 2C4G 主机增加 Temporal/NATS 运维负担；
- 义务状态、重试、升级和证据可直接审计。

当 due obligation 持续超过 100 万、调度延迟 SLO 无法满足或多区域容灾成为硬要求时，再通过新 ADR 评估 Temporal；领域中的 Obligation ID、dedup key、evidence 和升级语义保持不变。

## 9. 耐久性、备份与恢复

### 9.1 PostgreSQL 配置底线

生产必须：

```text
fsync = on
synchronous_commit = on
full_page_writes = on
```

不得为了性能测试关闭耐久性。建议启用 WAL 归档或受管数据库 PITR；本地单机至少每日全量备份 + 连续 WAL，恢复点目标按部署等级配置。

### 9.2 恢复目标

MVP 建议目标：

- RPO：不超过 5 分钟；正式生产启用连续 WAL 后目标趋近 0；
- RTO：60 分钟内在备用主机恢复控制平面；
- 每月至少一次自动恢复演练；
- 备份必须加密并测试 schema migration 后可启动。

恢复后：

1. 所有 `ACTIVE` Lease 不立即相信，等待其原 `expires_at` 或由策略统一 revoke；
2. recovery sweeper 回收超时 Outbox/Obligation claim；
3. Outbox 允许重复投递，Inbox 去重；
4. 与 Git Relay/对象存储按 digest/commit 对账；
5. 检查 Package current attempt、Lease、Candidate/VerificationRun、Submission 三 Head 与 Integration receipt 等不变量后才开放 readiness。

### 9.3 数据保留

- 领域事件和命令审计默认长期保留；
- 高频进度只保留结构化里程碑，大日志在对象存储按策略过期；
- `command_receipts` 的完整响应至少保留 7 天；其后可以归档响应，但 actor/key/request hash/result 摘要的去重 tombstone 随审计事实长期保留；
- Outbox PUBLISHED 行可以在 30 天后归档，但对应 domain event 不删除；
- 删除项目采用 tombstone/保留策略，不级联删除审计事实。

## 10. 2 核 4 GB 可行性

单 PostgreSQL 避免额外 Broker/Workflow 数据库和多个连接池。MVP 配置：

- `shared_buffers=512MB`；
- `work_mem=4MB`；
- PostgreSQL `max_connections=30`；
- 应用 SQLx pool 最大 16；
- Outbox/Obligation 批次各不超过 100，并发各 2；
- 关键 partial index 只覆盖 Active/Due/Pending 小集合；
- 大 payload 与日志外置，控制行保持小而稳定。

在 200 注册 Worker、100 Active Lease、稳态 20 控制请求/秒的压力余量场景下，负载以短主键查询、CAS 和小批索引扫描为主，因此 2C4G 在设计上大概率可行，但当前仍为 `TARGET_NOT_VALIDATED`。发布基线是 100 Worker、30 Active Attempt、100,000 Package 的 24 小时 soak；压力余量的 30 分钟测试不能替代它。正确性优先于吞吐；池等待或 CPU 达阈值时 API 应背压/429，而不是放大连接数和内存。

## 11. 风险与缓解

| 风险 | 缓解 |
| --- | --- |
| PostgreSQL 单点故障 | WAL/PITR、备份恢复演练、无状态控制进程 |
| Outbox 表膨胀 | partial index、按月归档已发布行、payload 保持小 |
| 热门 Package 行锁竞争 | claim 短事务、市场预过滤、409+退避，不做自旋 |
| 长事务阻塞 expiry/renew | statement/idle timeout、事务外 I/O、监控锁等待 |
| 事件与状态模型演化 | schema version、upcaster、兼容迁移、golden tests |
| 数据库误操作 | 最小权限、迁移账号与运行账号分离、append-only trigger/权限 |
| 服务器时钟跳变 | NTP/chrony 告警；授权以 DB 时间为准；TTL 保守 |
| JSONB 滥用导致约束缺失 | 核心状态/ID/版本/摘要使用强列，JSONB 只放扩展 metadata |

## 12. 被否决的方案

### 12.1 NATS/Redis 作为任务真相

否决：队列 ACK 和 consumer cursor 无法表达跨 Package/Attempt/Lease/Submission 的原子不变量；重放和过期竞态会复杂化。

### 12.2 Redis 锁 + 任意数据库

否决：增加第二个一致性系统；锁有效期和数据库提交之间存在裂缝；PostgreSQL 行锁、唯一约束和 fencing 已足够。

### 12.3 纯 Event Sourcing

否决：MVP 复杂度过高；跨聚合约束与查询成本不合适。保留 append-only 事件以支持审计和未来投影即可。

### 12.4 SQLite

否决：中央多 Worker 并发、`SKIP LOCKED`、网络访问、并行 scheduler 和成熟备份需求不匹配。Worker 本地 Journal 可以使用 SQLite，但不是中央事实。

### 12.5 Temporal 作为事实来源

否决：Workflow history 适合耐久编排，但不能替代领域关系约束和 Git/Lease 验收模型；MVP 运维成本不合适。未来 Temporal 只能承载 Obligation 执行，领域事实仍回写 PostgreSQL。

## 13. 验收标准

该 ADR 的实现必须通过：

1. 状态、事件、Outbox、Obligation、receipt 在故障注入下要么全部提交、要么全部回滚；
2. 32 路并发 Claim 恰好产生一个 Active Lease；
3. Renew 与 Expire 竞态不存在 Lease 复活；
4. 新 token 授予后旧 token 的全部正式副作用被拒绝；
5. Outbox publish/ACK 间崩溃会重复消息，但 Inbox 阻止重复业务变化；
6. 两个相同 base graph 的 PlanPatch 只有一个成功；
7. 备份恢复演练后，不变量查询返回零异常行；
8. 压力测试期间 PostgreSQL 保持全部耐久选项开启；
9. 消息 Broker 完全停用时，数据库状态与 SSE/轮询仍能完成 MVP 闭环；
10. 运维人员只查询 PostgreSQL 和内容引用即可确定任意任务的权威状态，不需要读取某个 Boss/Worker 会话猜测。

## 14. 后续影响

任何新组件若希望持有业务状态，必须先回答它与 PostgreSQL 冲突时谁获胜。除非新 ADR 明确替代本决策，答案始终是 PostgreSQL；其他组件只能保存缓存、传递副本或内容制品。
