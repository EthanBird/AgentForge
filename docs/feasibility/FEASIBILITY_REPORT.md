# AgentForge MVP 可行性核验报告

> 核验日期：2026-08-07  
> 结论：**架构可行，文档与 Schema 交叉审查通过后可立即进入 M0；M1 必须等待 M0 的协议、状态机与 golden tests 门禁通过。jcode 端到端驱动、2C4G 容量和 rootless 沙箱强度仍是后续强制 PoC，不得把文档推断当作性能实测。**

## 1. 核验范围

本报告只判断 MVP 的关键机制能否由现有协议和工具实现：

- A2A 是否能承载异构 Agent 的发现与长任务边界；
- AFWP 能否以严格、可版本化的 Schema 表达；
- PostgreSQL 能否实现原子 Claim、Lease、Outbox 和并发 Worker；
- jcode 是否提供可编程 SDK、持久会话和结构化事件；
- Git Bundle 是否能在 Worker 与局域网 Git 不直连时中转增量 Commit；
- rootless 容器是否可作为 MVP 的第一层隔离；
- 2C4G 中央服务器是否适合模块化单体方案；
- 哪些结论仍需代码 PoC 和压力测试。

## 2. 总体判定

| 机制 | 判定 | 证据 | 尚需验证 |
| --- | --- | --- | --- |
| A2A 边界互操作 | 可行 | A2A 1.0 有 Agent Card、Task、Artifact、流式/异步状态和扩展 | Gateway 反向会话与 AFWP extension 的互操作测试 |
| AFWP Schema | 可行 | JSON Schema Draft 2020-12 支持 `$defs`、条件约束和版本化 `$id` | Rust validator 的规范一致性和 canonical hash |
| PostgreSQL 原子领单 | 可行 | 指定 Package 的行锁、事务、CAS 与唯一约束保证权威 Claim；`SKIP LOCKED` 适合内部 Outbox/Obligation 多消费者 | 高并发 claim/renew property 与压力测试 |
| Transactional Outbox | 可行 | 状态行、领域事件、Outbox 可在同一数据库事务提交 | 崩溃点和重复投递测试 |
| jcode 可编程接入 | 条件可行 | SDK v1.1.0 可安装、导入，公开 JcodeClient、结构化错误和事件 API | 带真实 Provider 的 launch/session/tool/interrupt/recovery E2E |
| Worker 隔离 | 条件可行 | Podman 支持 rootless 工作流 | 恶意构建、内核攻击面、Windows 节点和网络白名单测试 |
| Git Relay | 可行 | Git 官方支持 full/incremental bundle 和 prerequisite verify；本地增量验证通过 | 分块、签名、ref allowlist、超大仓库性能 |
| 2C4G 控制平面 | 大概率可行 | 领域负载以元数据和短事务为主，MVP 不运行模型和编译 | 必须在目标机完成 24h soak 与资源基准 |
| 动态模型路由 | 可行但后置 | 能力画像和历史结果可关系化存储 | 样本不足前只做硬约束和人工先验 |

## 3. A2A 与 AFWP

### 3.1 已确认能力

[A2A 1.0 官方规范](https://a2a-protocol.org/latest/specification/)定义了：

- Agent Card 和扩展能力声明；
- Message、Task、Artifact；
- 长任务状态、输入/认证中断状态；
- HTTP+JSON、JSON-RPC、gRPC 等绑定；
- 流式状态与异步通知；
- 自定义扩展和协议版本协商。

因此 A2A 足以作为 AgentForge 与第三方 Agent 之间的边界协议。

### 3.2 不应由 A2A 直接承担的内容

A2A Task 没有完整定义：

- WorkGraph edge；
- base commit 与 write set；
- Bid、Lease generation 与 fencing；
- 逐项 Acceptance Criterion；
- Evidence 与 Git Merge Queue；
- `ACCEPTED` 和 `INTEGRATED` 的差异。

因此 `ADR-0003` 的边界合理：AFWP 是内部执行契约，A2A 只做发现、协商、长任务和 Artifact envelope。

### 3.3 PoC 门禁

- 发布带 `urn:agentforge:extension:afwp:v1` 的 Agent Card；
- Worker 通过出站连接完成 send、status stream 和 Artifact 回传；
- 不支持强制 AFWP extension 的 Agent 明确拒绝；
- 网络重连后按 task/context ID 恢复，不重复创建 Attempt。

## 4. PostgreSQL 控制平面

### 4.1 原子 Claim

PostgreSQL 官方文档说明 `FOR UPDATE ... SKIP LOCKED` 会跳过已锁行，虽然不适合一般一致性查询，但适合多个消费者访问队列式表：[PostgreSQL SELECT](https://www.postgresql.org/docs/current/sql-select.html)。

建议 Claim 使用单个事务：

1. 选择仍为 `OFFERED` 且满足 expected revision 的包并加行锁；
2. 验证无有效排他 Lease；
3. 递增 server-owned generation；
4. 插入 Attempt 和 Lease；
5. 更新 WorkPackage 投影；
6. 插入 DomainEvent 与 Outbox；
7. Commit 后返回结果。

`SKIP LOCKED` 只用于减少 Outbox、Obligation 等内部队列的领取争用；MVP 公开 API 采用 `ListOffers -> ClaimPackage(package_id)`，同一任务的唯一执行权依赖唯一约束、CAS 和事务不变量，而不依赖列表结果或跳锁。

### 4.2 Outbox 与业务真相

业务状态、审计事件和 Outbox 在同一事务写入。Dispatcher 可以重复发布，但消费者以 `(actor_id, idempotency_key)` 和 event ID 去重。

消息 ACK 不能更新 WorkPackage 为完成；只有命令事务和验收结果可以改变业务投影。

### 4.3 PoC 门禁

- 20/100 个并发 Claim 针对同一排他包，只有一个有效 generation；
- 在事务的每个写入点注入崩溃，不能出现“状态已变但无 Outbox”或相反情况；
- Dispatcher 在发布后、标记已发送前崩溃，重复发布不会重复副作用；
- 旧 generation 对 renew/checkpoint/Candidate registration/artifact 的外部调用全部返回 `AF_LEASE_STALE`；
- 100,000 WorkPackage 数据量下 Offer 索引执行计划稳定。

## 5. jcode SDK 核验

### 5.1 官方能力

[jcode SDK 文档](https://jcode.sh/sdk)当前公开：

- `launch()` 私有实例和 `connect()` 现有实例；
- session 创建、列表、持久和恢复；
- token/tool/background/permission/turn 等流式事件；
- `runStructured()` JSON Schema 校验输出；
- `softInterrupt()` 安全点注入；
- model、reasoning effort 和 runtime info；
- owner-only 本地 socket。

文档同时明确：jcode 实例隔离**不是**不可信代码沙箱，应使用容器或 VM。

### 5.2 本次实际探测

在隔离临时目录完成：

```text
npm package: @1jehuang/jcode-sdk
observed version: 1.1.0
required Node: >=20
test Node: 24.14.0
install with lifecycle scripts disabled: PASS
ESM import: PASS
bundled Linux x64 binary resolution: PASS
```

成功导出的关键符号包括：

```text
JcodeClient
HarnessError
StructuredOutputError
KNOWN_EVENT_KINDS
launchInstance
bundledJcodeBinary
```

这证明 TypeScript sidecar 能在当前 Node 环境加载，并能定位随包分发的 Linux x64 jcode 二进制。

### 5.3 未完成的探测

当前容器没有可安全使用的模型 Provider 凭据，也不具备完整 Worker sandbox。未执行真实模型 turn、工具调用、soft interrupt 和进程重启恢复。因此判定是“接口与封装可行”，不是“完整 Worker 已验证”。

### 5.4 强制 PoC

文档 09 中 M2 的 jcode PoC 工单必须验证：

1. launch 私有实例；
2. 创建固定 working directory 的 session；
3. 运行一个只读 structured task；
4. 捕获 tool、usage、turn_done；
5. 在长工具调用中排队 soft interrupt；
6. 结束 Bridge 进程并从固定 `jcodeHome` 恢复 session；
7. 拒绝越过 working directory 的文件请求；
8. 验证 sidecar 与 Worker 的协议不受未知新增事件影响；
9. 确认 Windows 原生支持前，生产节点只标记 Linux/WSL2 eligible。

## 6. Git Relay 核验

### 6.1 官方能力

[Git Bundle 官方文档](https://git-scm.com/docs/git-bundle)说明 Bundle 用于没有活动 Git Server 的离线对象传输，支持：

- full bundle；
- 基于 prerequisite 的 incremental bundle；
- `git bundle verify` 检查格式、前置 Commit 和连通性；
- `list-heads` 检查可见 refs；
- 通过 fetch/unbundle 导入对象。

### 6.2 本次实际探测

本地建立 Source 与 Destination 仓库：

1. Source 创建 base commit；
2. full bundle 初始化 Destination；
3. Source 创建 delta commit；
4. 生成 `base..main` incremental bundle；
5. Destination 执行 `git bundle verify`；
6. fetch 到受限 `refs/remotes/relay/main`；
7. 对比 Source 和 Destination 的 tree hash。

结果：

```text
bundle verify: PASS
source tree:      9e7a2049b4f009f120b5f61199527c4f1c7526cd
destination tree: 9e7a2049b4f009f120b5f61199527c4f1c7526cd
tree equality: PASS
```

这证明增量对象中转机制成立。

### 6.3 仍需防护

- Bundle 内容哈希和 Worker 签名；
- 作者必须在有效 Lease 下完成 Candidate Artifact 分块上传与 OID/tree 校验，再由 `RecordCandidate` 绑定同一预留 Candidate ID；VerificationRun 只消费该不可变 Artifact，终态 Submission 在独立验收之后创建；
- 允许 ref 精确匹配 Attempt branch，拒绝 tags、notes 和保护分支；
- prerequisite 必须与登记 base commit 一致；
- Bundle 解包在临时 bare repo；
- 执行对象大小、数量、深度和总压缩比限制；
- 通过后由 Git Broker 使用自己的凭据 push；
- 同一 Submission digest 只能物化一次；
- 超大仓库的分块、续传和 GC 基准。

本次 Git 探测证明对象传输机制成立，但没有替代上述 Candidate Artifact/Lease/Verification 事务闭环的实现测试；该闭环仍由 M3 的并发与故障注入 AC 验收。

## 7. Rootless 容器

[Podman 官方教程](https://docs.podman.io/en/latest/Tutorials.html)提供 rootless 使用流程。它能减少 Worker 以 root 运行任务的必要性，但不是完整安全边界：

- rootless 仍共享宿主内核；
- 恶意构建可能利用内核或运行时漏洞；
- 宿主挂载、socket、设备和网络配置错误会破坏隔离；
- Windows/macOS 通常需要 Linux VM。

MVP 可用 rootless Podman，但高风险或外部不可信仓库必须允许策略升级为 VM。沙箱验收至少包含：

- 宿主凭据目录不可读；
- Docker/Podman socket 不挂载；
- 默认无网络，白名单通过代理；
- CPU、内存、PID、磁盘和 wall-clock 限制有效；
- 终止会清理完整进程树；
- 只读输入和唯一可写 worktree；
- 容器退出后不保留 secret material。

## 8. 2C4G 中央服务器

### 8.1 为什么架构上可行

MVP 中央机只执行：

- 短 HTTP/SSE 请求；
- PostgreSQL 元数据事务；
- Outbox 分发和持久 Timer；
- 小型 Schema 校验；
- Artifact 元数据和临时流控。

模型推理、编译、测试、图像处理和 Git 验收都在 Worker/Relay 节点，因此控制平面负载主要是 I/O 和小对象状态变更。模块化单体避免多服务、Temporal、Kafka 和服务网格的固定开销。

### 8.2 尚不能声称的内容

当前没有在目标 2C4G、40G 机器上运行真实 AgentForge。以下数字均不能写成已验证：

- 最大 Worker 数；
- 事件吞吐；
- P95 Claim/renew；
- PostgreSQL 内存与磁盘增长；
- SSE 长连接数；
- 临时 Bundle 对带宽和磁盘的影响。

### 8.3 上线门禁

- 100 个模拟 Worker；
- 30 个 Active Attempt；
- 50 events/s 持续、300 events/s 短时突发；
- 100,000 WorkPackage；
- 24 小时 soak；
- PostgreSQL、控制进程、临时文件和日志峰值资源报告；
- 数据库备份恢复和 Outbox 重放；
- 带宽限制下的 Worker 重连风暴。

若资源不足，优化顺序是：

1. 降低日志和 checkpoint 频率；
2. 修正索引与查询；
3. 调整连接池和 SSE fanout；
4. 把 Bundle 完全改为流式或外置对象存储；
5. 分离 Outbox dispatcher；
6. 最后才拆完整微服务。

## 9. JSON Schema 与哈希

[JSON Schema Draft 2020-12](https://json-schema.org/draft/2020-12)可表达 AFWP/Submission 的结构验证。但 Schema 验证不等于全部业务验证：

- DAG 无环；
- requirement 到 acceptance 的覆盖；
- base commit 存在；
- Artifact hash 可解析；
- allowed/forbidden path 交叉；
- Lease 与当前 generation；
- Reviewer independence；
- Commit SHA 一致性；

必须由 domain validator 完成。

任务包哈希不能直接对任意序列化 JSON 求 SHA-256。必须冻结 canonicalization 规范、排除服务器派生字段，并用跨语言 golden vectors 验证。

## 10. 最终 Go/No-Go

### Go

- 领域模型和状态机；
- AFWP/Submission Schema；
- Rust 模块化单体；
- PostgreSQL Claim/Lease/Outbox；
- Linux Worker + jcode sidecar PoC；
- Git Bundle Relay PoC；
- 独立 Evidence/Verifier；
- 2C4G 目标机压力验证。

### 暂缓

- 一开始引入 Temporal/NATS/Kafka；
- Windows 原生 Worker 正式承诺；
- 无沙箱自动批准全部 jcode 工具；
- 根据模型品牌硬编码岗位；
- 未经真实样本训练复杂路由器；
- 开放第三方 Worker 的真实金钱市场；
- Worker 直接写主分支。

### 进入编码的条件

文档与 Schema 通过交叉审查后，可立即开始 M0。进入 M1 前必须完成 Schema golden tests。M2 可以先用 fake Executor 实现 Journal、Supervisor 和 Adapter contract，但**不能退出 M2**，直到真实 jcode E2E PoC 通过；M4 可以先实现 Bundle/Relay，但**不能退出 M4**，直到恶意/畸形 Bundle 测试通过。rootless 隔离测试属于 M2 出口，目标 2C4G 的 24 小时 soak 属于 M6/MVP 发布出口。
