# AgentForge 测试与验证计划

> 目标：证明系统在 Agent 不可靠、消息重复、网络分区、进程崩溃和 Git 基线变化时仍保持业务不变量。

## 1. 测试原则

AgentForge 的测试重点不是“正常请求返回 200”，而是状态与证据能否收敛：

1. 每条系统不变量至少有一个自动测试；
2. 每个状态转移同时测试允许路径和禁止路径；
3. 每个外部副作用测试重复投递、响应丢失和进程崩溃；
4. 时间由虚拟 Server Clock 驱动，不用真实 sleep 验证 Lease；
5. 并发测试必须保留失败 seed，可确定性重放；
6. LLM 不进入核心正确性测试，使用 Scripted/Fake Executor；
7. jcode 真实 E2E 单独标记并受预算控制；
8. 任何 `INCONCLUSIVE` 都导致门禁未通过；
9. 验收日志必须绑定精确 Commit 和 Runner digest；
10. 性能结论只在固定环境、固定数据量和足够样本下成立。

## 2. 测试分层

| 层 | 目标 | 外部依赖 | 执行频率 |
| --- | --- | --- | --- |
| Domain Unit | 状态机、值对象、不变量、评分函数 | 无 | 每次提交 |
| Schema Contract | JSON Schema、示例、canonical hash | Schema validator | 每次提交 |
| Repository Integration | SQL、约束、事务、Outbox | 临时 PostgreSQL | 每次提交 |
| Adapter Contract | jcode/Git/Sandbox/Artifact 端口 | Fake + 少量真实工具 | 每次提交/每日 |
| Component | 控制平面或 Worker 独立闭环 | PostgreSQL/SQLite | 每次 PR |
| End-to-End | 从发布任务到集成 Git | 全部 MVP 组件 | 每日/发布 |
| Chaos | 崩溃、断网、延迟、重复和乱序 | 可故障注入环境 | 每日/发布 |
| Security | 越权、注入、恶意 Bundle、secret | 隔离 Runner | 每日/发布 |
| Performance | 容量、P95、资源、soak | 固定目标节点 | 里程碑/发布 |

## 3. Domain 与属性测试

推荐 `proptest` 生成命令序列，以纯状态机作为 reference model。

### 3.1 WorkPackage

| ID | 属性 |
| --- | --- |
| `DOM-WP-001` | 已发布 revision 内容不可改变 |
| `DOM-WP-002` | 新 revision 的 package ID 不变、revision 严格递增、hash 改变 |
| `DOM-WP-003` | `SUPERSEDED/CANCELLED/CLOSED` 不可重新进入 Offered |
| `DOM-WP-004` | Accepted 不自动等于 Integrated |
| `DOM-WP-005` | 代码类 hard dependency 默认要求上游 Integrated |

### 3.2 Attempt 与 Lease

| ID | 属性 |
| --- | --- |
| `DOM-LEASE-001` | 同一排他包任意时刻最多一个有效 generation |
| `DOM-LEASE-002` | generation 由服务器严格递增，不受客户端输入影响 |
| `DOM-LEASE-003` | 旧 generation 的正式副作用永远拒绝 |
| `DOM-LEASE-004` | 相同 Idempotency-Key 的 expire/revoke/release 重放返回首次结果；使用新 key 对终态重复终结返回 `AF_TRANSITION_INVALID` |
| `DOM-LEASE-005` | Lease 过期结果只能 quarantine/salvage |
| `DOM-ATT-001` | 非终态等待必须有 wake condition |
| `DOM-ATT-002` | terminal Attempt 不能回到 running |
| `DOM-ATT-003` | package revision/base commit 在 Attempt 生命周期固定 |

生成随机序列：

```text
claim | renew | progress | checkpoint | expire | reassign |
record_candidate | finalize_verification | revoke | release | duplicate(command) | advance_clock
```

断言实现状态与 reference model 一致；失败时保存 seed 和最小化后的命令序列。

### 3.3 Submission

| ID | 属性 |
| --- | --- |
| `DOM-SUB-001` | Submission 不可覆盖，只能新建 lineage |
| `DOM-SUB-002` | PASS 时 tested/reviewed/submitted/candidate head 必须一致；integration head 另行绑定并复验 |
| `DOM-SUB-003` | 任一 hard criterion 非 PASS 时不能 Accepted |
| `DOM-SUB-004` | INCONCLUSIVE 不是 PASS |
| `DOM-SUB-005` | Reviewer policy 排除作者身份/同实例 |
| `DOM-SUB-006` | provenance/review/reproduction 提前 FAIL 或 INCONCLUSIVE 可形成带 Failure Dossier 的终态 Submission；未执行阶段的 Head/结果必须缺席 |

## 4. Schema 与 Golden Tests

### 4.1 正例

- `examples/afwp-lease-fencing.json` 必须通过 `schemas/afwp.schema.json`；
- `examples/submission-lease-fencing.json` 必须通过 `schemas/submission.schema.json`；
- `examples/submission-salvage-lease-fencing.json` 必须通过同一 Submission Schema，且 candidate-only 字段全部被拒绝；
- candidate 与 salvage 的 `signer_role` 互换后，即使重算出密码学有效签名也必须被 Schema/语义校验拒绝；
- 从 candidate 正例派生的 provenance 早期失败夹具，删除未执行的 criteria/review/reproduction/evidence 字段并加入带 `evidence_digest` 的 Failure Dossier 后必须通过；补回伪造 Head、Review、Clean Reproduction 或 `SKIPPED` 占位必须失败；
- 每种 package kind 至少一个最小样例；
- optional 字段缺失仍能解析；
- 当前 minor 增加的未知非强制扩展由兼容 reader 保留。

### 4.2 负例

每个负例只破坏一项，文件名写出期望错误码：

```text
tests/fixtures/invalid-afwp/
  missing-package-id__SCHEMA_REQUIRED.json
  invalid-revision-zero__SCHEMA_RANGE.json
  missing-base-commit__DOMAIN_INPUT.json
  must-without-criterion__DOMAIN_TRACEABILITY.json
  overlapping-path-rules__DOMAIN_SCOPE.json
  invalid-lease-policy__DOMAIN_LEASE.json
```

### 4.3 Canonical Hash

Golden vectors 至少包含：

- 不同对象 key 顺序；
- Unicode 组合字符；
- 空数组与字段缺失的区别；
- 整数与浮点表示；
- 不同换行；
- server-derived 字段排除；
- extension 字段保留。

Rust、TypeScript 计算结果必须逐字节一致。

## 5. PostgreSQL 集成测试

每个测试启动全新数据库或独立 schema，运行真实 migration，不 mock SQL。

### 5.1 Claim 并发

`DB-CLAIM-001`：20 个并发连接 Claim 同一 `exclusive` WorkPackage。

预期：

- 1 个成功；
- 其余为 `AF_PACKAGE_NOT_CLAIMABLE` 或在刷新 Offer 后不可见；
- 仅一行 Active Lease；
- generation 为 1；
- 仅一条 LeaseGranted 领域事件；
- Outbox 中仅一条对应事件。

`DB-CLAIM-002`：100 个 Package、20 个消费者并发执行 `ListOffers -> ClaimPackage(package_id)`；`ListOffers` 可重复返回同一候选，最终权威性由指定 Package 的 Claim 事务保证。`SKIP LOCKED` 仅用于 Outbox/Obligation 等内部队列，不作为公开“claim-next” API 语义。

预期：每个包只分配一次；所有消费者最终没有锁等待泄漏；按稳定优先级排序。

### 5.2 CAS 和 fencing

- 同 expected_version 的两个 update 只有一个成功；
- version 过期返回 `AF_VERSION_STALE`；
- reassign 后 generation +1；
- generation-1 的 renew/progress/checkpoint/Candidate Artifact complete/RecordCandidate 全部失败；
- 作者 Lease 释放后，独立 Verification Coordinator 可凭 service identity、job capability 与 CAS 终结已绑定的 run；它不持有作者 fencing token，也不能替换 Candidate 或其历史来源；
- 失败事务不写 Outbox。

### 5.3 Idempotency

对每个写命令执行：

1. 正常调用；
2. 丢弃响应；
3. 使用相同 key 重放；
4. 使用相同 key 但不同 payload；

预期：步骤 3 返回相同业务结果；步骤 4 返回 `AF_IDEMPOTENCY_KEY_REUSED`，不能执行第二次。

### 5.4 Outbox 崩溃点

故障点：

- 状态写入前；
- 状态写入后、事件前；
- 事件后、Outbox 前；
- Commit 前；
- Publish 前；
- Publish 后、mark-sent 前。

预期：事务内故障全部回滚；最后一种允许重复发布但消费者去重。

### 5.5 Migration

- 空库从 0 升当前；
- 上一个 release snapshot 升当前；
- migration 重跑不允许静默成功；
- 大表 migration 有锁和时长预算；
- 所有 enum 演进有兼容策略；
- 备份恢复后的 schema version 正确。

## 6. Worker Runtime 测试

### 6.1 Scripted Executor

Fake Executor 由脚本控制每一回合：

```yaml
turns:
  - emit: [{kind: tool_start, name: read_file}, {kind: turn_done}]
    report: {status: continue, completed_acceptance_ids: []}
  - emit: [{kind: tool_done, result: pass}, {kind: turn_done}]
    report: {status: candidate_ready, completed_acceptance_ids: [AC-1]}
```

用它验证 Turn Pump，而不依赖 LLM 随机性。

### 6.2 Turn Pump

| ID | 场景 | 预期 |
| --- | --- | --- |
| `WRK-TURN-001` | turn_done，但 hard criteria 未满足 | 自动构造下一回合 |
| `WRK-TURN-002` | 模型声称完成，但 Verifier 失败 | 返回 Failure Dossier 并继续 |
| `WRK-TURN-003` | 有有效 blocker 和 wake condition | Checkpoint、释放 Executor、进入等待 |
| `WRK-TURN-004` | blocker 没有 wake condition | 拒绝等待报告 |
| `WRK-TURN-005` | 预算耗尽 | 封存失败 Evidence、释放 Lease |
| `WRK-TURN-006` | 收到 revoke | 安全点停止，禁止正式副作用 |

### 6.3 Watchdog

- 重复相同 failure fingerprint 达阈值；
- 持续输出 token 但无验收进度；
- 长编译已登记 operation deadline；
- 工具子进程失联；
- jcode socket 断开；
- sidecar event buffer overflow；
- unknown event kind。

预期：长操作不误杀；无进展依升级链处理；未知事件被记录但不改变状态。

### 6.4 Journal 恢复矩阵

在以下 **WorkerPhase** 写入后立即 `SIGKILL` Worker：

```text
Granted, Preparing, Baseline, Planning, Implementing,
LocalVerifying, WaitingInput, SealingCandidate,
HandingOffCandidate, AuthorComplete
```

`IsolatedReview`、`CleanReproduce` 与 `Submitted` 是控制面/独立验收链的 Attempt 状态，不得出现在作者 Worker Journal 恢复矩阵。

重启预期：

- 重放本地 Journal；
- 查询服务器 cursor 与 Lease generation；
- 有效 Lease 从最近安全检查点继续；
- 失效 Lease 停止副作用并生成 salvage；
- 不重复 Candidate handoff、Bundle 上传或终态 Submission finalization。

## 7. jcode Adapter Contract

### 7.1 不使用真实模型的协议测试

- handshake major/minor；
- session ID 格式和路径边界；
- structured output success/retry/exhaustion；
- tool event 顺序；
- permission request；
- soft interrupt 排队与取消；
- unknown event forward compatibility；
- sidecar crash/restart；
- stdout 噪声不得污染 frame protocol。

### 7.2 真实 jcode 每日 E2E

使用低成本、只读、固定仓库任务：

- 列出指定目录中的三个文件；
- 以给定 JSON Schema 输出摘要；
- 触发一个受控只读工具；
- 收集 token/tool/turn_done；
- 连接中断后恢复 session。

不把模型文本精确匹配作为通过条件，只验证 Schema、事件、工具边界和会话恢复。

## 8. Verification 与 Evidence

- command argv 不经 shell 拼接；
- timeout 会终止完整进程树；
- stdout/stderr 分块并计算 hash；
- secret redaction 在持久化前发生；
- Runner image digest 固定；
- 测试失败和基线已有失败可区分；
- Candidate 改动后旧 Evidence 拒绝；
- Reviewer 只读 checkout；
- 作者与 Reviewer policy；
- clean reproduction 不依赖未跟踪文件和本地 cache。

Evidence 验证器对 manifest、Artifact hash、签名、Commit 和 criterion result 做全量一致性检查。

## 9. Git Relay 测试

### 9.1 正常路径

- full bundle bootstrap；
- incremental bundle；
- prerequisite verify；
- list-heads 只含 Attempt ref；
- fetch 到临时 namespace；
- tree hash 与登记 candidate 相同；
- push 到唯一任务分支；
- duplicate submission 幂等。

### 9.2 恶意与异常 Bundle

- 缺 prerequisite；
- 声称错误 base commit；
- 包含 `refs/heads/main`；
- 包含 tag/notes/replace ref；
- 超大单对象；
- 极高对象数；
- 截断/损坏；
- hash 与 manifest 不符；
- 签名错误；
- 过期 fencing token；
- ref 指向非 candidate commit；
- 压缩炸弹式资源消耗。

所有导入先在临时 bare repo，限制 CPU、内存、磁盘和时间。

### 9.3 Merge Queue

- 主分支无变化时 fast path；
- 主分支变化但无冲突，合成 Commit 重跑；
- 文本冲突生成 RebasePackage；
- 无文本冲突但 API 契约失败；
- 两个候选顺序依赖；
- Integration Runner 失败；
- merge 响应丢失后的幂等查询；
- 回滚点和 Integrated event。

## 10. Obligation Engine

使用虚拟时钟测试：

- Lease 到期只生成一次回收义务；
- 长期无 Bid 生成 NoBid；
- Candidate 登记生成 VerifyCandidate 与对应 VerificationRun；
- 子图完成生成 Integration/Summary；
- 图未完成且无 Ready/Active 生成 DeadlockDiagnosis；
- 义务执行失败按策略退避；
- 超过重试上限升级到 Boss；
- 服务重启后到期义务继续；
- 已履行义务重复唤醒无副作用。

禁止在测试中使用长时间 sleep；推进 FakeClock 并触发 due scan。

这里的 FakeClock 只用于纯领域与 Obligation 调度测试。PostgreSQL 集成测试必须写入已到期夹具，或使用仅在测试构建启用的 DB clock seam；生产 SQL 的 Lease 授权与到期判断始终使用数据库服务器时间，绝不接收 Worker 时间。

## 11. 端到端验收场景

### E2E-001：最小成功闭环

```text
Project -> AFWP -> Offer -> Claim -> Attempt -> CandidateArtifact(COMPLETE) -> Candidate
-> Evidence -> Verify -> Relay -> Merge -> Integrated
```

断言所有 correlation/causation、revision、generation 和 Commit 一致。

### E2E-002：Worker 断网但 Lease 有效

- Worker 产生本地 checkpoint；
- 网络中断；
- 在 offline grace 内继续允许的本地计算；
- 重连后同步事件并续租；
- 无重复 Submission。

### E2E-003：Lease 过期并重授

- Worker A 断网；
- Lease g1 到期；
- Worker B 获得 g2 并完成；
- Worker A 恢复并上传 g1 结果；
- A 结果 quarantine/salvage，B 才能正式验收。

### E2E-004：Reviewer 退回

- 作者提交 Candidate C1；
- Reviewer 找到 High finding；
- 新候选 C2 完整重跑；
- C1 Evidence 不能用于 C2。

### E2E-005：主分支漂移

- Candidate 基于 B0 通过；
- 主分支变为 B1；
- Merge Queue 构造 B1+C；
- 集成测试失败并生成 IntegrationPackage；
- 原 Candidate 保持不可变。

### E2E-006：Boss 会话退出

- Boss 发布图后完全停止；
- Worker 完成子任务；
- Obligation Engine 自动创建验收、集成和后继 Ready；
- 只有出现架构冲突时才重新唤起 Boss。

## 12. 混沌测试

故障注入点：

- HTTP 响应丢失；
- 数据库连接在 Commit 前后断开；
- Outbox publisher 在 publish 前后崩溃；
- Worker 在所有状态被 kill；
- jcode sidecar crash；
- sandbox runner hang；
- Artifact 上传最后一块丢失；
- Relay 导入后、push 前崩溃；
- Git push 成功但响应丢失；
- 系统时钟前跳/后跳；
- 网络乱序、重复、延迟和分区。

通过标准不是“没有错误”，而是恢复后满足所有系统不变量、没有未解释的正式副作用。

## 13. 安全测试

- 未授权项目、package、Artifact 和 branch 的横向访问；
- 过期/错误 audience 的 capability token；
- path traversal 和 symlink escape；
- 仓库中伪造 `AGENTS.md` 请求扩大权限；
- 命令参数注入；
- secret 写入日志、Evidence 或 Commit；
- SSRF 和任意 Artifact URL；
- 恶意 archive/bundle；
- 宿主 socket 和凭据目录访问；
- fork bomb、磁盘填满和超大日志；
- Reviewer 与作者身份伪装；
- Agent Card/Executor Profile 签名篡改。

## 14. 性能与容量

### 14.1 数据集

固定生成：

- 100 projects；
- 100,000 WorkPackages；
- 每包 3 条 edge、5 个 criteria；
- 100 Workers；
- 30 Active Attempts；
- 每 Attempt 每分钟 2 个 progress/checkpoint 事件；
- 5% 任务重试，1% salvage。

### 14.2 测试

- Offer query P50/P95/P99；
- Claim/renew/Candidate registration/verification finalization 延迟；
- Outbox backlog catch-up；
- SSE 100 连接和重连风暴；
- PostgreSQL CPU、RSS、IOPS、WAL 和表/索引增长；
- 控制进程 CPU/RSS；
- 24 小时 soak；
- 备份/恢复时间；
- 1 MB、10 MB、100 MB 增量 Bundle 吞吐。

### 14.3 环境记录

报告必须包含：

- CPU/内存/磁盘/内核；
- PostgreSQL 版本和配置；
- AgentForge commit；
- 数据集 seed；
- 并发、连接池和限流；
- 预热、持续时间和样本量；
- 原始结果，不只给平均数。

## 15. CI 门禁

目标流水线：

```text
format
-> lint
-> domain + schema
-> postgres integration
-> adapter contract
-> component
-> security static checks
-> e2e smoke
```

每日增加：真实 jcode E2E、chaos 子集、Git 恶意 Bundle、依赖扫描。发布增加全量 chaos、固定节点性能、24h soak 报告和恢复演练。

任何 required job 不允许通过 retry 隐藏第一次失败；Flaky retry 的每次结果都进入报告。

## 16. 发布判定

`v0.1.0` 只有在以下条件同时满足时可发布：

- 所有 P0/P1 工单 Acceptance 通过；
- 系统不变量测试 100% 通过；
- E2E-001 至 E2E-006 通过；
- 关键 chaos 场景通过；
- threat model 无未接受的 Critical/High；
- 目标 2C4G soak 达标；
- 备份恢复和数据库升级演练通过；
- 文档、Schema、样例和实现版本一致。
