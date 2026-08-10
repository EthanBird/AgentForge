# AgentForge `v0.1.0-mvp` 开发进度

- 分支：`agent/v0.1.0-mvp`
- 更新日期：2026-08-10
- 总体状态：MVP-01 完成；MVP-02/MVP-03 试点纵切实施中，尚未达到可发布 MVP 门禁
- 发布计划：[V0_1_0_MVP_RELEASE_PLAN.md](V0_1_0_MVP_RELEASE_PLAN.md)

## MVP-01 / Checkpoint A：Typed Market 与 Lease 命令面

状态：完成；真实 PostgreSQL 17 与 workspace CI 均通过。

- 远端提交：`1ba8cdcb8d98014896c3f10e2aefd5b72123a61e`
- GitHub Actions：CI #49 / run `31344363783`
- PostgreSQL 17 job：migration、transactional UoW、MVP market/Lease contract 全部通过
- Rust job：format、metadata、workspace boundary、build、Clippy、workspace test 全部通过

本检查点完成：

- 新增稳定的 `MvpControlPlane` application port，以及 Project、Package、Offer、Claim 和 Lease 的
  transport-independent 请求/响应类型；
- 新增 PostgreSQL typed-table adapter；Package 发布、Claim、Renew 和 Release 均采用 receipt-first
  事务，并在同一事务内更新 canonical rows、追加 Domain Event、写 Outbox 与 Command Receipt；
- Claim 通过 WorkPackage、Attempt、Lease 三个领域聚合产生事件，使用单调 fencing token，并由数据库
  唯一索引保护同一 revision 仅一个 ACTIVE Lease；
- Package 发布前重算 canonical AFWP JSON 的 JCS SHA-256，拒绝 package hash 不一致；
- Idempotency-Key 的同请求重放返回首次响应，不同 payload 复用稳定返回
  `AF_IDEMPOTENCY_KEY_REUSED`；
- Lease 终态检查优先于版本 CAS，因此 receipt miss 的二次 Release 返回稳定的终态非法转换；
- 增加真实 PostgreSQL 条件合同：20 个并发 Claim 只能有一个成功，并覆盖 ACK-loss 重放、key reuse、
  Renew、Release、事件/Outbox/Receipt 数量和单一 Lease 行；
- CI PostgreSQL 17 job 已显式运行 `postgres_mvp`，不允许该合同只在无数据库环境中静默跳过。

本地证据：

```text
cargo test -p agentforge-storage-postgres -p agentforge-application \
  --all-features --locked --offline                         PASS
cargo clippy -p agentforge-storage-postgres -p agentforge-application \
  --all-targets --all-features --locked --offline -- -D warnings  PASS
cargo fmt --all -- --check                                  PASS
git diff --check                                             PASS
```

检查点 A 当时的限制（已由 Checkpoint B 关闭）：

- 当前机器没有 PostgreSQL/docker；真实事务与并发断言的权威证据为上述 PostgreSQL 17 CI；
- HTTP `/api/v1`、管理 CLI 与 Lease expiry/reconciliation 当时尚未完成；
- Release 当时只终结 Lease，尚未把 ACTIVE WorkPackage/Attempt 收敛到
  `REWORK_READY`/`LOST`。这两项现已由 Checkpoint B 的实现关闭；
- Worker Attempt 进度命令仍属于 MVP-02。

## MVP-01 / Checkpoint B：HTTP、管理 CLI 与 Lease 收敛

状态：完成；真实 PostgreSQL 17 与 workspace CI 均通过。

- 远端提交：`84b4434929df06447ca7af66e09168481fff212d`
- GitHub Actions：CI #53 / run `31345557078`
- PostgreSQL 17 job：Release→re-Claim、expiry sweeper→re-Claim、generation 1→2→3 fencing、
  migration digest readiness 失效/恢复全部通过
- Rust job：format、metadata、workspace boundary、build、Clippy、workspace test 全部通过

本检查点完成：

- `/api/v1` Project、Package、Offer、Claim、Lease Read/Renew/Release 路由；
- 请求级项目授权、服务器派生 Actor ID、强制 `Idempotency-Key` 与更新命令 `If-Match`；
- 稳定且不泄露内部细节的 HTTP 错误 envelope；
- `af-cli mvp` 等价管理命令，直接使用同一 application port 与 PostgreSQL adapter；
- control-plane 启动时显式装配 typed PostgreSQL command service。
- 主动 Release 在一个事务内把 Lease/Attempt/WorkPackage 收敛为
  `RELEASED`/`LOST`/`REWORK_READY`，随后可用更高 fencing generation 再次 Claim；
- 基于 PostgreSQL 时钟的有界 expiry sweeper 复用同一跨聚合事务路径，并提供 CLI 手动触发入口。
- `/readyz` 同时核验投影源和命令数据库；数据库检查覆盖迁移版本、名称、源码摘要及 MVP 所需
  typed tables，不能以单纯 TCP/`SELECT 1` 冒充就绪。

HTTP handler 合同测试、API/CLI 使用说明、本地完整 workspace 门禁与远端 PostgreSQL 17 合同均已
通过。

## MVP-02 / Checkpoint C：可恢复 Journal 与 Turn Pump

状态：完成；远端 workspace 与 PostgreSQL 17 CI 均通过。

- 远端提交：`287d5b340de30f8b91f5a6f577b4e0fbd844b915`
- GitHub Actions：CI #55 / run `31347134282`
- Rust job：bundled SQLite build、format、workspace boundary、Clippy、全部测试通过
- PostgreSQL 17 job：migration、UoW 与 MVP market/Lease 合同通过

当前完成：

- 纯 replayable Worker Attempt 状态机和经过校验的状态反序列化；
- SQLite WAL/FULL Journal，把不可变事实链、物化状态、Inbox、Outbox 和 operation completion 放在
  单个 `BEGIN IMMEDIATE` 事务；
- receipt-first exact replay、changed-payload key reuse 拒绝、JCS digest 和 previous-digest 完整性检查；
- 外部副作用先登记 operation，再调用 Executor；非幂等 Turn 的 ACK 丢失只查询、不重启；
- daemon 重启后恢复 Pending Turn/Verification，并核对 phase、kind、class、semantic key、输入摘要和
  deadline；
- 模型提前声称完成不能绕过 hard criteria；Turn budget 和 Lease expiry 在启动新副作用前生效；
- 14 项 Worker 测试覆盖状态机、五个事务崩溃点、ACK-loss 和跨进程恢复。

本检查点尚未完成：

- Worker enrollment、Offer poll、远端 Claim/Renew/Release 与启动 Lease reconciliation；
- workspace/日志/凭据隔离；
- jcode bridge 和真实 Candidate Artifact handoff；
- `worker-daemon` 二进制仍是组合根骨架。

详细实现与恢复契约见 [14_MVP_RECOVERABLE_WORKER.md](14_MVP_RECOVERABLE_WORKER.md)。

## MVP-02 / Checkpoint D：Claim 执行快照与启动 Lease 对账

状态：远端 Rust job 通过；PostgreSQL 17 job 暴露 Renew response/row 时间不一致，修复已进入
Checkpoint E，等待追加 CI 复核。

- 远端提交：`f4fa14a760118cb6d50763caf19544f3b31289ac`
- GitHub Actions：CI #57 / run `31348181584`
- Rust job：workspace build、format、boundary、Clippy、全部测试通过
- PostgreSQL 17 job：migration 与 UoW 通过；MVP contract 在 Renew 后发现 response 的 `updated_at`
  比 typed row 早约 0.8ms。根因是响应使用事务 `now`、SQL 更新使用稍后的 `clock_timestamp()`；
  Checkpoint E 改为 canonical row 与响应共用同一个数据库权威时间。

当前完成：

- Claim 响应增加 `PackageExecutionSnapshot`、权威 grant/max expiry；PostgreSQL 从 typed revision 行读取
  canonical AFWP/input/base/hash 并在返回前重算 JCS hash；
- Worker 再次核对 Offer/Claim/执行快照绑定，并把 Grant 与执行快照原子写入 Journal schema v2；
- 引入 transport-independent `WorkerControlPlane`，提供确定性 Offer poll 与持久化后可 exact retry 的
  `ClaimIntent`；
- 启动 reconciliation 校验 Project/Package/Attempt/Lease/node/generation，能导入漏收的 Renew，或在
  Expire/Revoke/authority mismatch 后进入 `Salvaging`；
- 本地 Lease renewal 事实强制 generation、旧/新 expiry、服务器更新时间和 replay shape；
- Worker 定点测试增至 18 项；application、storage 和 Worker 严格 Clippy/测试通过。

Checkpoint D 当时仍待 Claim intent 节点级预登记（现由 E 关闭）；其余仍待 HTTP/mTLS transport、自动
Renew/Release 调度、enrollment、sandbox 与 jcode bridge。

## MVP-02 / Checkpoint E：Durable Claim Intent 与 CI 时间一致性修复

状态：完成；远端 workspace 与 PostgreSQL 17 CI 均通过。

- 远端提交：`9ae930eedaf0973d73852e8f53e546a453d78d5b`
- GitHub Actions：CI #59 / run `31348766919`
- Rust job：format、metadata、workspace boundary、build、Clippy、workspace test 全部通过
- PostgreSQL 17 job：migration、transactional UoW、MVP market/Lease contract 全部通过；CI #57
  暴露的 Renew response/typed row `updated_at` 漂移已关闭

当前完成：

- Journal schema v3 增加不可变 `claim_intents` 账本；Offer、expected version、actor/executor/node、
  command/correlation ID、idempotency key 和 Lease window 在远端调用前持久化；
- 远端失败或 ACK 丢失后，重启从 Pending record 精确重放；成功后单向绑定 Attempt/Lease，changed
  payload key reuse、请求篡改和删除均 fail closed；
- 提供 v2→v3 单事务迁移，并验证已有 Attempt、事实链和执行快照保持可恢复；
- Lease Renew/Release 的 typed row `updated_at` 与响应改为共用同一个 PostgreSQL 权威 `now`，关闭
  CI #57 的亚毫秒时间漂移；
- Worker 定点测试增至 21 项，覆盖远端失败→进程重启→exact Claim replay。

Checkpoint E 当时仍待自动 Renew/Release（现由 F 关闭）；其余仍待 HTTP/mTLS Worker adapter、
enrollment、sandbox 与 jcode bridge。

## MVP-02 / Checkpoint F：可恢复 Lease Maintenance

状态：完成；远端 workspace 与 PostgreSQL 17 CI 均通过。

- 远端提交：`d4f98e57543b8676be64c7641cb11aa33612b76d`
- GitHub Actions：CI #61 / run `31349687337`

当前完成：

- Journal schema v4 增加 `lease_command_intents`；Renew/Release 请求在远端调用前绑定 Attempt、actor、
  idempotency key、expected Lease version 与 fencing token，响应以 JCS digest 封存；
- v2→v3→v4 可在一次启动中顺序迁移，已有 Attempt、执行快照和事实链保持不变；
- `LeaseMaintenancePolicy` 只在 expiry 阈值内续租，并按 `max_expires_at` 截断 extension；阈值外不产生
  远端 mutation；
- 服务器已 Renew 但响应丢失时，Pending intent 跨重启取回首次 receipt，本地只追加一次
  `LeaseRenewed`；
- `LocalFailed`/`LocalCancelled` 自动 Release，`AuthorComplete` 在 Candidate handoff 前继续 Renew；若
  Lease 丢失只保留本地 Candidate 做 salvage，并清除正式 candidate ID 授权；
- Worker 定点测试增至 24 项，覆盖 scheduler window、Renew ACK-loss/restart、Release 和 response
  binding。

Checkpoint F 当时仍待 daemon 定时组合根（现由 G 关闭）；其余仍待 HTTP/mTLS Worker adapter、
enrollment、sandbox 与 jcode bridge。

## MVP-02 / Checkpoint G：Pending-first Worker Daemon 组合根

状态：完成；远端 workspace 与 PostgreSQL 17 CI 均通过。

- 远端提交：`69d6ce97737d86856c63bb72368d74209cf8e1bb`
- GitHub Actions：CI #63 / run `31350376544`

当前完成：

- `WorkerDaemonConfig` 严格限制 schema、大小、未知字段、绝对 Journal 路径、项目集合、容量、Lease
  window 和 tick 周期；节点指纹使用 JCS 覆盖 runtime fingerprint 与调度策略；
- `WorkerDaemon::tick` 固定先恢复 Pending Claim，再恢复 Pending Renew/Release，然后维护已有 Lease，
  最后只对剩余 capacity 接单；任一远端错误停止本轮并保留 durable intent；
- Project binding 只从已完成 Claim intent 反查；旧 Journal 若无法证明 Attempt 所属 Project 就返回
  `AF_WORKER_PROJECT_BINDING_MISSING`，不会用配置猜测；
- 多项目 polling 按配置顺序逐轮公平推进；慢 tick 使用 delay 语义，不产生追赶式 mutation burst；
- 生产时钟与 UUID v7 只在 `SystemDaemonRuntime` 可信边界产生，测试可注入确定性时间/ID；
- Worker 定点测试增至 29 项，覆盖 capacity、自动 Renew、Claim ACK-loss、重启 pending-first exact replay
  和非法 runtime ID 在远端 mutation 前拒绝。

Checkpoint G 当时仍待 HTTP adapter（现由 H 的 loopback slice 关闭）；其余仍待 LAN mTLS、enrollment、
sandbox、jcode bridge 与 Candidate Artifact handoff。

## MVP-02 / Checkpoint H：真实 Loopback HTTP Worker

状态：完成；远端 workspace 与 PostgreSQL 17 CI 均通过。

- 远端提交：`772da360533553632363d505e212343b875001ba`
- GitHub Actions：CI #65 / run `31351154870`

当前完成：

- `LoopbackHttpControlPlane` 实现 Offer、Claim、Lease GET/Renew/Release 的真实 `/api/v1` HTTP contract；
- 配置只允许 literal IPv4/IPv6 loopback，无 TLS adapter 不能被误配到 LAN；请求整体 timeout、response
  header/body 上限、唯一 Content-Length、JSON Content-Type 与禁用 Transfer-Encoding 均 fail closed；
- response 先 strict JSON，再 typed decode；HTTP status、AF error code 与 retryable 必须是受支持的一致
  组合，未知 code、重复 JSON key、状态码替换和超限 body 均拒绝；
- command ID、correlation ID、idempotency key 与 If-Match 从 durable intent 原样传输，HTTP actor 仍由
  控制面已授权 request context 派生；
- `agentforge-worker-daemon` 二进制读取 `AGENTFORGE_WORKER_CONFIG`、建立 Journal/HTTP adapter、运行
  pending-first tick loop，并在 SIGINT/SIGTERM 后有序停止；
- `examples/worker-loopback.json` 受编译期测试约束；Worker 定点测试增至 34 项（含真实 Axum server
  contract、错误映射、body limit 和 timeout）。

仍待：Worker enrollment、LAN mTLS、sandbox、jcode bridge、把 Turn Pump 接入 daemon，以及 Candidate
Artifact handoff。

## MVP-02 / Checkpoint I：确定性 Fixture 执行纵切

状态：完成；远端 workspace 与 PostgreSQL 17 CI 均通过。

- 远端提交：`18a296bf902a0074747d73a5b2ba9d872bf8a4dd`
- GitHub Actions：CI #67 / run `31351823881`

当前完成：

- 新增显式 `driver_mode=fixture`，daemon 在 Lease maintenance 后驱动已领取 Attempt，并在下一轮 Claim
  前重新计算容量；安全默认 `lease_only` 不执行任何模拟开发动作；
- fixture 复用正式 Worker 状态机、SQLite Journal、Supervisor 与 operation ledger，依次持久化
  Preparation、workspace、baseline、plan、Turn 和 Local Verification；
- 非幂等 Turn 输出只由 durable operation ID 决定，进程重启后可查询相同结果，不依赖内存、不重复启动；
- hard-pass 后到达本地 `SealingCandidate`，但明确不生成真实 Git 对象、不上传 Artifact、不创建中央
  Candidate，也不冒充独立验收；
- fixture 仅支持 SHA-1-shaped 测试 tree；SHA-256 object-format 仓库在任何 Attempt 变更前 fail closed；
- `SealingCandidate` 在 handoff 完成前继续占用作者 Attempt capacity，防止 daemon 每轮继续接单造成无界
  本地候选与 Lease 积压；
- 新增独立 `examples/worker-loopback-fixture.json`，生产形态示例继续固定为 `lease_only`；
- Worker 定点测试增至 39 项，其中包含关闭并重新打开 SQLite Journal 后按原 operation ID 恢复
  非幂等 Turn 的端到端用例。

本地证据：

```text
cargo test -p agentforge-worker-daemon --all-features --locked --offline  PASS (39)
cargo clippy -p agentforge-worker-daemon --all-targets --all-features \
  --locked --offline -- -D warnings                                      PASS
cargo fmt --all -- --check                                               PASS
```

仍待：真实 workspace sandbox、jcode bridge、Candidate Artifact/`RecordCandidate` handoff、Worker
enrollment 与 LAN mTLS。

## MVP-03 / Checkpoint J：Candidate-first 领域聚合

状态：完成；远端 workspace 与 PostgreSQL 17 CI 均通过。

- 远端提交：`e288ce8c464fd64b617c3ca13aead22bbce98782`
- GitHub Actions：CI #69 / run `31352832910`

当前完成：

- 新增 `CandidateArtifact` 聚合：`Uploading -> Assembling -> Complete`，并提供 Reject、Quarantine、Expire
  终态；终态检查优先于 version CAS，COMPLETE 不可回退或替换；
- Artifact reservation 永久绑定 Project 内 Package/revision/hash、Attempt、Lease/fencing、base/candidate/
  tree、Author Evidence、Bundle digest/size/chunk digests 和过期窗口；
- Complete 同时核对 `artifact://` 引用、非零 digest、精确大小、精确 chunk 顺序与未过期的单调服务端
  时间；任一替换均 fail closed；
- `Candidate` 只能从同一 COMPLETE Artifact 封存，复制并复核完整 lineage、Bundle 和受限任务 ref，创建后
  无任何更新命令；
- `VerificationRun` 只接受与不可变 Candidate 完全一致的 ID/commit，严格执行
  `Queued -> ProvenanceCheck -> Reviewing -> Reproducing -> terminal`；
- PASS 强制 `CandidateHead = ReviewedHead = TestedHead`；Provenance/Review/Reproduction 的 FAIL 或
  INCONCLUSIVE 只允许携带当时真实存在的 Head，禁止未来阶段事实占位；
- Candidate 与 VerificationRun 不实现直接 `Deserialize`，必须分别携带 COMPLETE Artifact/Candidate
  重放；CandidateArtifact 使用经过完整 shape 校验的手写反序列化；
- 新增 7 项领域正反例与 2 个 compile-fail 门禁；domain 当前为 70 unit + 2 property + 12 contract +
  3 doc tests。

本地证据：

```text
cargo test -p agentforge-domain --locked --offline                  PASS
cargo clippy -p agentforge-domain --all-targets --locked --offline \
  -- -D warnings                                                    PASS
cargo fmt --all -- --check                                         PASS
```

明确边界：本检查点只冻结纯领域语义，尚未把新 aggregate type 写入 durable event envelope/数据库；下一
检查点必须通过 additive migration、typed PostgreSQL transaction 与 HTTP/Worker contract 原子接线，
不能以通用 JSON snapshot 代替 canonical Candidate 表。

## MVP-03 / Checkpoint K：Candidate-first PostgreSQL 事实源

状态：完成；远端 workspace 与 PostgreSQL 17 CI 均通过。

- 远端实现提交：`b1215043629e8ffe98ef2a64ca77acd00284168c`
- CI 期望值修复：`8d28715fe4d0ba39cd46385c1185979c872c6d14`
- GitHub Actions：CI #74 / run `31354262053`

当前完成：

- 新增 additive `0005_mvp_candidates.sql`，以 `candidate_artifacts`、不可变 chunk ledger、`candidates`
  和 `verification_runs` 四张 typed canonical 表承载领域事实；没有引入通用 JSON aggregate snapshot；
- durable event head 在同一迁移事务内扩展 `CANDIDATE_ARTIFACT`、`CANDIDATE`、
  `VERIFICATION_RUN`，Rust `AggregateType`/`AggregateId` 与 PostgreSQL label 同步，避免 wire/DB
  类型漂移；
- Artifact reservation 由复合外键绑定 Project、Package/revision/hash、Attempt、Lease/fencing；MVP
  Bundle 上限 16 MiB、单 chunk 上限 1 MiB，chunk 序号、预声明 SHA-256、实际内容 SHA-256、数量和
  总大小全部 fail closed；
- Artifact 的 `UPLOADING -> ASSEMBLING -> COMPLETE` 与 Reject/Quarantine/Expire 由数据库状态转换、
  单调 version/event sequence/time 和 terminal immutability 共同保护；COMPLETE 时重新核对当前作者
  Lease、完整 chunk 集与 Bundle binding；
- Candidate INSERT 重新读取并锁定 COMPLETE Artifact，逐字段核对 lineage、Bundle 与受限 branch；
  Candidate 创建后 UPDATE/DELETE 均被拒绝；
- VerificationRun 约束与领域状态机一致，禁止跳过阶段，PASS 必须三头相等；FAIL/INCONCLUSIVE 的
  stage-aware head shape 以及 CANCELLED 的事实缺席在 SQL 层再次约束；
- PostgreSQL readiness 已要求四张新表，迁移 manifest、typed label、升级文件清单与 CI migration
  fixture 同步到 version 5；真实 PostgreSQL 条件测试新增正向完整链及终态 Artifact、Candidate 变更、
  Verification 跳阶段负例。

本地证据：

```text
cargo test -p agentforge-storage-postgres --all-features --locked --offline  PASS
cargo clippy -p agentforge-storage-postgres --all-targets --all-features \
  --locked --offline -- -D warnings                                       PASS
cargo test -p agentforge-domain --locked --offline                       PASS
PGlite 0001..0005 migration + Candidate/Artifact/Verification contract    PASS
cargo fmt --all -- --check                                                PASS
```

本地 PostgreSQL 条件测试因没有 `AGENTFORGE_TEST_DATABASE_URL` 会显式 skip；PGlite 已实际执行迁移和
正反例，但 PostgreSQL 17 仍以本检查点推送后的 GitHub Actions 为发布证据。下一检查点把 Artifact
init/chunk/complete 与原子 `RecordCandidate + VerificationRun::Queued` 接入 application/HTTP/Worker，
并遵守 receipt-first 与固定锁顺序。

## MVP-03 / Checkpoint L：Candidate Artifact 应用与 PostgreSQL 命令纵切

状态：实现与本地全工作区门禁通过；CI #76 的 Rust job 全绿，但 PostgreSQL migration fixture 使用两次
volatile `clock_timestamp()` 构造本应相等的 `created_at/updated_at`，在真实 PostgreSQL 微秒级分离后被
正确拒绝。Checkpoint M 已改为单一 transaction timestamp，随下一次推送重新验证完整 PostgreSQL 合同。

当前完成：

- application 新增 `init_candidate_artifact`、`upload_candidate_artifact_chunk`、
  `complete_candidate_artifact` 三个 typed use case；返回值显式携带 Artifact/Candidate 预留 ID、lineage、
  chunk 声明、Bundle 与 aggregate version；
- init 在一个 Serializable 事务中固定按 Package → Attempt → Lease 加锁，先回放 receipt，随后核对当前
  ACTIVE Lease、holder node、fencing token、Package hash/base commit 与 Attempt CAS，再创建服务端 ID、
  typed Artifact row、Event、Outbox 和 Receipt；
- chunk 写入复算请求内容 SHA-256，限制单块 1 MiB，并逐项匹配 reservation；相同 artifact/index/content
  可安全恢复，changed payload 复用同一 idempotency key 稳定拒绝；
- complete 在同一事务内锁定 Package → Attempt → Lease → Artifact，按序读取并复算所有 chunk、总大小和
  Bundle SHA-256，再执行 `UPLOADING -> ASSEMBLING -> COMPLETE` 两个领域转换；两个 Event、Outbox 与
  首次成功 Receipt 原子提交；
- receipt 命中先于当前 Lease、version 与终态检查；因此 ACK 丢失后，即使作者 Lease 已关闭，完全相同的
  complete 仍返回首次结果；receipt miss 的新 mutation 则被 fencing/terminal guard 拒绝；
- PostgreSQL 17 条件合同覆盖 init/chunk/complete 精确回放、changed-body key reuse、缺块拒绝、Bundle
  持久化、终态不可变、Lease 关闭后的 exact replay 与 fresh mutation 拒绝；本地无 PostgreSQL 时明确
  skip，不冒充真实数据库证据。

本地证据：

```text
cargo fmt --all -- --check                                             PASS
bash tests/contract/workspace_layout.sh                                PASS
cargo build --workspace --locked --all-targets --offline              PASS
cargo test --workspace --locked --offline                             PASS
cargo clippy --workspace --locked --all-targets --all-features \
  --offline -- -D warnings                                             PASS
node --check crates/control-plane/assets/app.js                        PASS
git diff --check                                                       PASS
```

明确边界：本检查点只完成 application + PostgreSQL 的作者侧 Artifact 命令面；HTTP wire、Worker durable
upload intent、`RecordCandidate + VerificationRun::Queued` 仍未接通，不能据此宣称 Candidate handoff 或
独立验收闭环已经完成。

## MVP-03 / Checkpoint M：Candidate Artifact HTTP 与 Worker adapter

状态：实现完成，全工作区门禁以及 CI #78/#80 Rust job 通过。真实 PostgreSQL 17 连续揭示正向 fixture
中三组“必须相等”的字段使用了独立 volatile 时钟：Artifact 初态 `created_at/updated_at`、VerificationRun
初态 `queued_at/updated_at`、Artifact COMPLETE 的 `completed_at/updated_at`。M、M.1、M.2 已逐组改用同一
transaction timestamp；数据库的 fail-closed 强约束保持不变。包含 M.2 的 CI #84 已在真实 PostgreSQL
17 上全绿，三组 fixture 漂移全部关闭。

当前完成：

- application 的 chunk DTO 统一使用带标准 padding 的 canonical Base64；严格拒绝 byte array、非规范
  Base64、空值与解码后超过 1 MiB 的内容，HTTP 与 receipt request digest 不再存在二进制表示漂移；
- 控制面新增 Artifact Init、Chunk、Complete 三条 Project-scoped 路由；在授权和 command dispatch 前
  对 Project/Attempt/Artifact/chunk index 做 path/body 等值检查，并强制相应 Attempt/Artifact `If-Match`；
- Chunk 保持设计文档规定的 `PUT + 204`；Worker 在空响应后只从已持久化请求与不变的 Artifact version
  构造 receipt，不信任额外响应事实；Init 为 `201`，Complete 返回完整 typed Artifact view；
- `LoopbackHttpControlPlane` 支持 PUT、空 body 成功响应和三条 Artifact 命令，并继续限制 loopback peer、
  总交换超时、header/body 上限、JSON content type 与远端错误 code/status/retryable 三元组；
- HTTP wire 将 CAS 错误统一为规范的 `AF_VERSION_STALE/412`，新增 Artifact 路径会产生的
  `AF_ARGUMENT_INVALID`、`AF_PACKAGE_HASH_MISMATCH`、`AF_EVIDENCE_INVALID`、
  `AF_CANDIDATE_ARTIFACT_NOT_COMPLETE` allowlist；后者与 Package not claimable 的 retryable 位与服务端
  领域错误保持一致；
- CI #76 暴露的 Candidate migration 正向 fixture 时间不稳定已修：同一初始 Artifact 的
  `created_at/updated_at` 改用相同 transaction timestamp，避免测试数据偶然违反真实数据库不变量。
- CI #78 进一步证明 VerificationRun fixture 存在相同缺陷；`queued_at/updated_at` 也已改用相同
  transaction timestamp。两处都是测试数据修复，数据库的 fail-closed 时间不变量保持不变。
- CI #80 继续捕获 COMPLETE Artifact 的 `completed_at/updated_at` 双时钟漂移；M.2 已把最后一组改为
  同一 transaction timestamp，并独立推送触发真实 PostgreSQL 17 复验。

本地证据：

```text
cargo fmt --all -- --check                                             PASS
bash tests/contract/workspace_layout.sh                                PASS
cargo build --workspace --locked --all-targets --offline              PASS
cargo test --workspace --locked --offline                             PASS
cargo clippy --workspace --locked --all-targets --all-features \
  --offline -- -D warnings                                             PASS
node --check crates/control-plane/assets/app.js                        PASS
git diff --check                                                       PASS
```

当前边界：Worker 现在能按严格 wire 调用三条 Artifact API，但尚未把每个 init/chunk/complete intent 写入
SQLite Journal，也未由 fixture driver 触发上传；因此本检查点不宣称跨重启 ACK-loss 恢复已经闭环。

## MVP-03 / Checkpoint N：Worker Candidate Artifact Journal v5

状态：实现完成，Worker 定点门禁与全工作区 build/test/Clippy 通过；CI #84 全绿（含真实 PostgreSQL 17）。

当前完成：

- SQLite Journal schema v5 新增 `candidate_artifact_command_intents`，把 Init、Chunk、Complete 的完整 typed
  command、Attempt/actor/key、JCS 摘要、typed response 与完成时间持久化；pending/completed shape、请求
  不可变、状态单调和禁止删除由 SQLite CHECK/trigger 双重保护；
- v2→v3→v4→v5 与 v3→v4→v5、v4→v5 均在 `BEGIN IMMEDIATE` 内升级，旧 Attempt、hash-chain、Inbox、
  Outbox、Claim 与 Lease command intent 不丢失，未知 schema version 继续 fail closed；
- 注册 intent 时先做 exact ID/key replay，再核对 completed Claim 所固定的 Project、actor、node、Lease、
  fencing，以及本地已 Seal Candidate 的 package/base/candidate/tree/evidence；同一 Attempt 不允许用新 key
  建立第二个 Init；
- Chunk 只接受 canonical Base64 解码后的 1 MiB 以内非空内容，并在落 Journal 前复算 SHA-256、匹配 Init
  声明的序号和 digest；Complete 必须引用同一 Init 回执与 Artifact version；
- 完成回执逐字段验证：Init 必须为 `UPLOADING/version=1`，Chunk receipt 必须匹配 index/digest/size/version，
  Complete 必须为 `COMPLETE/version=3`，且 Candidate/Artifact/Bundle lineage、时间窗口和预声明摘要不能漂移；
- 重启测试覆盖 Init 精确回放、Chunk pending 恢复、Complete pending 恢复、changed-body key reuse、回执
  篡改、SQL UPDATE/DELETE 攻击；还覆盖远端 Complete 成功后本地转入 Salvaging，原 actor/key/body 的
  receipt-first 回放仍能安全写入首次回执。

定点证据：

```text
cargo test -p agentforge-worker-daemon --lib --locked --offline            PASS (40/40)
cargo clippy -p agentforge-worker-daemon --all-targets --all-features \
  --locked --offline -- -D warnings                                        PASS
cargo fmt --all -- --check                                                  PASS
cargo build --workspace --locked --all-targets --offline                    PASS
cargo test --workspace --locked --offline                                   PASS
cargo clippy --workspace --locked --all-targets --all-features \
  --offline -- -D warnings                                                   PASS
node --check crates/control-plane/assets/app.js                              PASS
git diff --check                                                             PASS
```

当前边界：本检查点提供 durable upload intent/receipt 与恢复原语，但 daemon 尚未自动从 fixture Candidate
生成真实 Bundle、依次注册/执行三类 intent；`RecordCandidate + VerificationRun::Queued` 也仍在后续，
因此不能宣称端到端 Candidate handoff 已闭环。

## MVP-03 / Checkpoint O.1：完整 Claim 回执与 Journal v6

状态：完成；Worker 41 个定点测试、全工作区门禁及 CI #86 / run `31357076090` 全绿。

当前完成：

- Claim 完成不再只保存 Attempt/Lease ID；Journal 原子保存完整 `ClaimedWork` JSON、JCS digest 与完成时间，
  ACK 丢失后的 exact completion 必须逐字段等于首次回执；
- 持久化边界重新验证 Project/Package/revision、Package version、Attempt/Lease ID、时间窗口、Git object
  format、1 MiB 内联上限、canonical AFWP 与 package hash，不能通过伪造 HTTP response 污染后续 CAS；
- `claimed_work_for_attempt` 提供后续 Artifact Init 所需的权威 `attempt_version`、Lease version 与 execution
  binding；旧 v5 completed Claim 的回执字段保持 NULL 并返回 `None`，调用者 fail closed 而不是推断 `v2`；
- v5→v6 只 additive 增加 Claim response/digest 列并重装单调 trigger；真实 v5 legacy completed row 保留，
  新 pending Claim 若不携完整 response 不能进入 completed；v2→v6 链同样通过；
- lifecycle 在本地 Grant、execution snapshot、hash-chain 和 Outbox 已原子成功后，用同一 remote Claim response
  完成 intent，保证后续恢复读取的是实际 ACK 而非重新构造的近似值。

定点证据：

```text
cargo test -p agentforge-worker-daemon --lib --locked --offline            PASS (41/41)
cargo clippy -p agentforge-worker-daemon --all-targets --all-features \
  --locked --offline -- -D warnings                                        PASS
cargo fmt --all -- --check                                                  PASS
cargo build --workspace --locked --all-targets --offline                    PASS
cargo test --workspace --locked --offline                                   PASS
cargo clippy --workspace --locked --all-targets --all-features \
  --offline -- -D warnings                                                   PASS
node --check crates/control-plane/assets/app.js                              PASS
git diff --check                                                             PASS
```

当前边界：v6 只补齐自动上传所需的权威 Claim/CAS 输入；pending Artifact 网络执行器与 fixture Bundle planner
将在下一检查点接入。

## MVP-03 / Checkpoint O.2：可恢复 Fixture Artifact 自动上传

状态：完成；Worker 42 个定点测试、全工作区门禁及 CI #88 / run `31357863100` 全绿。

当前完成：

- daemon 的 pending-first 顺序扩展为 Claim → Candidate Artifact → Lease command；Artifact ACK 丢失后，
  下一次 tick 在续租、执行新任务或规划后续上传步骤之前先重放原 actor/key/body；
- lifecycle 新增统一 Artifact command executor：Init、Chunk、Complete 均先登记 SQLite intent，再调用远端，
  最后原子保存经过 lineage 校验的 typed response；新规划与启动恢复不再有两套调用逻辑；
- Journal 提供按 Attempt 读取的受校验 Artifact 命令历史；每条记录重新核对请求/响应摘要、Claim/Lease/
  fencing 与本地 sealed Candidate，pending 行不会被误认为已完成步骤；
- fixture driver 在本地 hard verification 后持久化确定性 Candidate seal，再生成一个小型 canonical JSON
  测试 Bundle；Bundle 绑定 Project、Package/revision/hash、Attempt、base/candidate/tree 与作者证据摘要；
- fixture 上传严格执行单一 Init → 单一 Chunk → Complete，并使用 Claim 回执中的真实 Attempt version、Init
  回执中的 Artifact version；同一 Attempt 的第二个 Init 或不完整/重复历史均 fail closed；
- 不伪造 causation Event：MVP Artifact response 尚不返回服务端 Event ID，因此后续命令的 `causation_id`
  保持空值，而不是把 Command ID 强转为 Event ID；
- daemon 端到端测试覆盖 Init 远端已生效但 ACK 丢失：第一次进程只留下一个 pending Init，重启后取回
  同一 receipt，再完成 Chunk/Complete；远端三类副作用各发生一次，本地最终只有三条 completed ledger。

定点证据：

```text
cargo test -p agentforge-worker-daemon --locked --offline                  PASS (42/42)
cargo clippy -p agentforge-worker-daemon --all-targets --all-features \
  --locked --offline -- -D warnings                                        PASS
cargo fmt --all -- --check                                                  PASS
bash tests/contract/workspace_layout.sh                                     PASS
cargo build --workspace --locked --all-targets --offline                    PASS
cargo test --workspace --locked --offline                                   PASS
cargo clippy --workspace --locked --all-targets --all-features \
  --offline -- -D warnings                                                   PASS
node --check crates/control-plane/assets/app.js                              PASS
git diff --check                                                             PASS
```

明确边界：这里的 fixture Bundle 是确定性 canonical JSON 测试载荷，不是可由 Git 解包的真实 Bundle；
Artifact Complete 后本地仍停在 `HandingOffCandidate`。下一纵切必须实现原子的
`RecordCandidate + VerificationRun::Queued`，之后才能关闭作者 Lease；真实 workspace/jcode 产生 Git 对象、
LAN mTLS/enrollment 与独立 verifier 仍不属于本检查点。

## MVP-03 / Checkpoint P.1：中心 Attempt 进度与证据账本

状态：实现完成，本地 application/storage 定点门禁与 PGlite 0001..0006 实跑通过；等待本检查点推送后的
真实 PostgreSQL 17 CI。

当前完成：

- application 新增 `report_attempt_progress` typed use case；作者只能逐步上报
  `PREPARING -> PLANNING -> IMPLEMENTING -> LOCAL_VERIFY`，不能从刚 Claim 的 `LEASED` 直接声称本地
  验证完成；
- PostgreSQL adapter 延续 receipt-first 与固定 Package → Attempt → Lease 锁顺序；receipt miss 时才复核
  Attempt CAS、当前 ACTIVE WorkPackage/Lease、holder node、fencing token 与服务器时钟 Lease expiry；
- 每次成功进度命令在同一 Serializable 事务内执行一个 phase transition 和一个
  `SemanticProgressReported`，因此 Attempt version/event sequence 各前进两步，并原子追加两个 Domain
  Event、两个 Outbox、一个 Command Receipt；
- 新增 additive `0006_mvp_attempt_progress.sql`。`attempt_progress` 是 typed、不可变证据账本，以复合外键
  绑定 Project、Package/revision、Attempt、Lease/fencing；数据库 trigger 再次核对当前 author authority、
  phase shape、Attempt version/semantic sequence 以及同一 `updated_at/recorded_at`；
- 阶段 `evidence_digest` 只进入进度账本，不冒充 `last_checkpoint_digest`；以后接入真实 workspace/jcode
  checkpoint 时必须使用独立的内容寻址对象；
- PostgreSQL 条件合同已扩展四阶段正向路径、跳阶段拒绝、exact ACK-loss replay 与 changed-evidence key
  reuse；本机无 PostgreSQL 服务时测试明确 skip，不计作真实数据库证据；
- PGlite 已实际顺序执行 0001..0006，并确认 `attempt_progress`、2 个业务 trigger 与 13 个约束均安装。

本地证据：

```text
cargo check --workspace --all-targets --locked --offline                  PASS
cargo test -p agentforge-application -p agentforge-storage-postgres \
  --all-features --locked --offline                                        PASS
PGlite 0001..0006 migration / attempt_progress triggers+constraints        PASS
cargo build --workspace --locked --all-targets --offline                   PASS
cargo test --workspace --locked --offline                                  PASS
cargo clippy --workspace --locked --all-targets --all-features \
  --offline -- -D warnings                                                  PASS
cargo fmt --all -- --check / workspace layout / node --check / diff-check  PASS
```

明确边界：P.1 只建立中心 Attempt 的权威阶段前置条件；HTTP wire 与 Worker Journal 尚未调用此命令，
`RecordCandidate + VerificationRun::Queued` 也尚未实现。下一检查点必须先把四步进度接入 Worker 的
pending-first durable intent，再以最终 `LOCAL_VERIFY` Attempt version 提交 Candidate，不能在 adapter 内
伪造或跳过状态。

## MVP-03 / Checkpoint P.2：Attempt Progress HTTP 与 Worker adapter

状态：实现完成，Control Plane/Worker 定点 tests 与 strict Clippy 通过；等待本检查点推送后的 CI。

当前完成：

- 新增 `POST /api/v1/projects/{project_id}/attempts/{attempt_id}/progress`；Project/Attempt path-body 必须
  相等，请求必须携带 `Idempotency-Key` 与当前 Attempt `If-Match`，actor 继续由服务器授权上下文派生；
- handler 只负责授权、路径绑定和 transport envelope，业务状态机与 receipt-first 事务仍由同一个
  `MvpControlPlane` application port 执行；
- `WorkerControlPlane` 增加相同 typed 方法，`LoopbackHttpControlPlane` 使用有界 HTTP exchange、严格 JSON、
  loopback peer 检查和统一错误 allowlist 调用该路由；
- Control Plane 合同覆盖正确 path/CAS 的 typed response 及 Attempt path-body mismatch；Worker adapter
  合同覆盖真实 HTTP path、headers、body 和 `AttemptProgressView` 解码。

定点证据：

```text
cargo test -p agentforge-control-plane -p agentforge-worker-daemon \
  --locked --offline                                                   PASS
cargo clippy -p agentforge-control-plane -p agentforge-worker-daemon \
  --all-targets --all-features --locked --offline -- -D warnings       PASS
cargo fmt --all -- --check / git diff --check                          PASS
```

明确边界：P.2 只交付 wire/adapter，尚未把 Progress intent 写入 SQLite，也没有让 daemon 自动执行四阶段。
P.3 必须新增 additive Journal schema 与 pending-first replay；Artifact Init 仍不能直接使用旧 Claim response
里的 Attempt version。

## MVP-03 / Checkpoint P.3：Attempt Progress Journal v7

状态：实现完成，Worker Journal/lifecycle 定点测试与全工作区门禁通过。CI #94 的 Rust job 全绿；真实
PostgreSQL 17 job 在执行 Progress 正向路径时发现 `0006` trigger 引用了不存在的 WorkPackage 列，修复随
P.4 checkpoint 交付。

当前完成：

- SQLite Journal schema v7 新增不可变 `attempt_progress_command_intents`，按 Attempt/stage 唯一保存四步
  Progress 的完整 typed command、actor/key、JCS digest、pending/completed 状态、完整 typed response 与完成
  时间；请求字段不可改、状态只能单向完成、账本不可删除；
- v2 至 v6 都在原有独占迁移事务内逐级升级到 v7，新增 v6→v7 定点迁移测试并确认既有 Attempt 不被重写；
- 注册命令必须证明完整 Claim 回执、当前本地 Lease/fencing/actor/node/Project 绑定与未过期窗口；阶段固定为
  Preparing、Planning、Implementing、LocalVerify，前序必须完成且 expected version 必须来自 Claim 或上一条
  受验证回执；
- 回执逐字段核对中心 Attempt state、semantic sequence、Package/Lease/fencing、服务端时间以及每步 `+2`
  version；ACK 丢失后 exact replay 返回既有完成，不会发明新 key/body 或改写完成时间；
- lifecycle executor 统一执行“先注册 SQLite → 调 HTTP → 验证并完成 SQLite”。故障注入测试证明中心命令
  已成功但响应丢失时，重启只恢复原 pending command，服务端 mutation 计数保持一次；
- Journal 合同覆盖跳阶段拒绝、同 key 改 evidence 拒绝、pending 重启恢复、四阶段完整历史、请求/删除篡改
  触发器拒绝及 completion ACK-loss replay。

定点证据：

```text
cargo test -p agentforge-worker-daemon attempt_progress --locked --offline  PASS (2 tests)
cargo test -p agentforge-worker-daemon --locked --offline                   PASS (45 tests)
bash tests/contract/workspace_layout.sh                                     PASS
cargo build --workspace --locked --all-targets --offline                    PASS
cargo test --workspace --locked --offline                                   PASS
cargo clippy --workspace --locked --all-targets --all-features \
  --offline -- -D warnings                                                   PASS
node --check crates/control-plane/assets/app.js                              PASS
cargo fmt --all -- --check / git diff --check                               PASS
```

明确边界：P.3 交付 durable command ledger 与通用 executor，但 daemon 尚未自动规划/执行四阶段，Artifact Init
也尚未改为使用最终 `LOCAL_VERIFY` response version。P.4 必须把 pending Progress 恢复置于 Artifact 之前，
逐步驱动中心 Attempt，并只用第四条受验证回执的 version 初始化 Artifact。

## MVP-03 / Checkpoint P.4：Progress-first Fixture 与 Artifact CAS

状态：完成；CI #98 / run `31360986472` 的远端 workspace 与真实 PostgreSQL 17 job 全绿。

当前完成：

- daemon 的 pending-first 顺序升级为 Claim → Attempt Progress → Candidate Artifact → Lease command；任何
  pending Progress 都在 Artifact 恢复和新副作用前执行，tick 失败时原 intent 仍是重启后的第一条命令；
- fixture driver 在本地 Candidate seal 后读取完整 Claim 回执与 Progress history，只为缺失的下一阶段创建
  typed intent，严格完成 Preparing、Planning、Implementing、LocalVerify；每一步 expected version 来自前一
  受验证 response；
- fixture evidence 明确分型：Preparing/Implementing 使用绑定 Attempt、runtime fingerprint、Package/base/tree
  的 deterministic fixture attestation，Planning 使用 plan digest，LocalVerify 使用 hard-verification digest；
  这些测试摘要不宣称是真实 jcode Evidence Bundle；
- Candidate Artifact Init 的 Journal 门禁现在要求四条 Progress 全部完成，并把 `If-Match` 固定为最终
  `LOCAL_VERIFY/version=10`；旧 Claim 的初始 `attempt_version=2` 会被拒绝，不能越过中心 Attempt 状态机；
- daemon report 分开记录 resumed/completed Progress command；Fake Control Plane 维护真实的中心 Attempt
  version/semantic sequence，并同样拒绝旧 version 的 Artifact Init；
- 新故障注入覆盖首次 Progress 已在中心成功但 ACK 丢失：重启先恢复同一 key/body，中心 Progress effect
  最终恰好 4 次，恢复完成前 Artifact effect 始终为 0，随后 Artifact Init/Chunk/Complete 恰好各一次；
- 既有 Artifact Init ACK-loss 测试同步证明 Progress 四阶段不会在重启时重复执行。
- CI #94 精确暴露 `enforce_attempt_progress_insert()` 对不存在的 `work_packages.active_lease_id` 与
  `active_fencing_token` 的引用；trigger 已改为使用 schema 中真实存在且足够的 authority 链：active
  Package → `active_attempt_id` → Attempt `lease_id/fencing_token` → ACTIVE Lease holder/expiry；静态合同新增
  禁止这两个幽灵列；
- 修复后的 0001..0006 已在 PGlite 实际执行，并成功插入一条满足 Project/Package/Attempt/Lease/fencing/
  holder/expiry/version/semantic-sequence 全绑定的 `attempt_progress` 行。它验证迁移与 trigger 正向执行，但
  真实 PostgreSQL 17 的最终证据仍须由本检查点 CI 提供。
- CI #96 / run `31360691668` 进一步证明迁移与通用 UoW 已通过；失败来自 Progress service command 连续执行
  phase transition 与 `ReportProgress` 时，把两个各自拥有 aggregate version 的事件一次传给“同版本事件
  batch”接口。adapter 现按顺序执行两次 `append_events`，仍处于同一 Serializable transaction，receipt、
  outbox 与两条事件继续全有或全无。
- CI #98 复验通过 migration、transactional UoW、MVP market/Lease/Progress 全链合同；Rust build、Clippy、
  workspace tests 同样通过。P.4 的真实 PostgreSQL 发布证据至此闭合。

定点证据：

```text
cargo test -p agentforge-worker-daemon --locked --offline                   PASS (46 tests)
cargo test -p agentforge-storage-postgres --all-features \
  --locked --offline                                                        PASS
PGlite 0001..0006 + Attempt Progress trigger positive insert                PASS
bash tests/contract/workspace_layout.sh                                     PASS
cargo build --workspace --locked --all-targets --offline                    PASS
cargo test --workspace --locked --offline                                   PASS
cargo clippy --workspace --locked --all-targets --all-features \
  --offline -- -D warnings                                                   PASS
node --check crates/control-plane/assets/app.js                              PASS
cargo fmt --all -- --check / git diff --check                               PASS
```

明确边界：P.4 仍使用 deterministic fixture driver/fixture Bundle，尚未实现中心原子的
`RecordCandidate + VerificationRun::Queued`，也没有关闭 author Lease。下一纵切必须把已经 COMPLETE 的
Artifact、最终 Attempt version、Candidate commit/tree 与作者证据绑定成不可变 Candidate，再交给独立验收；
不能让 Worker 自行生成 Accepted Submission。

## MVP-03 / Checkpoint P.5：原子 Candidate Handoff

状态：完成；CI #102 / run `31362864591` 的远端 workspace 与真实 PostgreSQL 17 job 全绿。

当前完成：

- application 新增 typed `RecordCandidateInput` / `RecordedCandidate` 与 `MvpControlPlane::record_candidate`；
  外部作者只提交预留 Artifact ID、最终 Attempt CAS、当前 Lease/fencing 和受 AgentForge 命名空间约束的分支，不能替换
  Artifact 已冻结的 Candidate ID、commit、tree、Package hash、Bundle 或 Author Evidence；
- PostgreSQL adapter 保持 receipt-first，并按固定 Package → Attempt → Lease → COMPLETE Artifact 锁序重验
  Project/Attempt/Lease/node/fencing、服务器到期时间、最终 `LOCAL_VERIFY` version 及全部 Artifact lineage；
- 单个 Serializable transaction 内创建不可变 Candidate、`VerificationRun(QUEUED)` 与
  `VerifyCandidate(PENDING)` obligation，同时 CAS `Attempt -> CANDIDATE`、`Package -> VERIFYING`、
  `Lease -> RELEASED`，追加五条领域事件、五条 Outbox 消息与一条命令回执；任一写入失败全量回滚；
- ACK-loss exact replay 在读取当前 Lease 前返回首次 `RecordedCandidate`；相同 actor/key 改 branch 返回
  `AF_IDEMPOTENCY_KEY_REUSED`，新 key 在作者 Lease 已关闭后返回 `AF_LEASE_STALE`，不会创建第二条 lineage；
- additive `0007_mvp_candidate_handoff.sql` 为 `VerificationRun.candidate_id` 加一对一约束，并安装
  deferred commit trigger。任何 Candidate 若在提交点缺少匹配的 CANDIDATE Attempt、VERIFYING Package、
  RELEASED Lease、QUEUED run 或 PENDING obligation，数据库直接拒绝整个事务；
- Package loader 只把真正 ACTIVE 的 Lease 投影为 `active_lease_id/fencing_token`；进入 VERIFYING 后保留
  active Attempt，但不会把已经 RELEASED 的作者 Lease误报为仍可写。
- CI #100 首次真实 PG 复验发现 Chunk 与 COMPLETE Artifact 两个 authority helper 的调用点颠倒；
  独立修复提交把 Chunk 恢复为上传期 Lease/expiry 校验，把 `RecordCandidate` 收紧为 COMPLETE-only。
  CI #102 随后通过 0001..0007 migration、transactional UoW、market/Lease/Progress/Artifact/Candidate
  handoff 全链合同，以及 Rust build、Clippy 和 workspace tests。

定点证据：

```text
cargo check -p agentforge-application -p agentforge-storage-postgres \
  -p agentforge-control-plane --all-targets --locked --offline              PASS
cargo test -p agentforge-storage-postgres --all-features \
  --locked --offline                                                        PASS (本机 PG 条件用例明确 skip)
cargo clippy -p agentforge-application -p agentforge-storage-postgres \
  -p agentforge-control-plane --all-targets --all-features \
  --locked --offline -- -D warnings                                         PASS
PGlite 0001..0007 + partial-handoff rollback + complete five-state handoff   PASS
bash tests/contract/workspace_layout.sh                                      PASS
cargo build --workspace --locked --all-targets --offline                     PASS
cargo test --workspace --locked --offline                                    PASS
cargo clippy --workspace --locked --all-targets --all-features \
  --offline -- -D warnings                                                   PASS
node --check crates/control-plane/assets/app.js                              PASS
cargo fmt --all -- --check / git diff --check                               PASS
```

明确边界：P.5 只完成中心 application/PostgreSQL 权威事务，尚未暴露 RecordCandidate HTTP route，也尚未把
Worker Journal 的 `HandingOffCandidate` 接到该命令。下一 checkpoint 必须先交付 wire/adapter，再新增 durable
SQLite Candidate handoff intent 与 pending-first ACK-loss 恢复；独立 Verifier 仍是后续阶段，作者 Worker 绝不
生成 Submission 或验收结论。

## MVP-03 / Checkpoint P.6：RecordCandidate HTTP 与 Worker Adapter

状态：实现完成；全工作区本地门禁通过，等待本检查点推送后的 GitHub Actions 复验。

当前完成：

- 控制面新增 `POST /api/v1/projects/{project_id}/attempts/{attempt_id}/candidates`；Project/Attempt 的
  path/body 必须完全一致，request actor 必须拥有 Project grant，`Idempotency-Key` 与 Attempt
  `If-Match` 缺失或格式错误均在 application 调用前 fail closed；成功固定返回 `201 + RecordedCandidate`；
- Worker `WorkerControlPlane` 增加 typed `record_candidate` port，loopback HTTP adapter 使用同一路径、严格
  JSON body、完整 command headers、`201` 状态与 JSON content type；远端稳定错误码继续经过既有
  status/code/retryable 三元一致性校验，不能用 HTTP 状态替换领域错误；
- 控制面合同覆盖 authorized actor、path/body/CAS、Queued VerificationRun 响应及 mismatch-before-dispatch；
  Worker Axum fixture 覆盖实际 path、branch、Artifact/Lease/node/fencing、幂等键、If-Match 与完整响应解码。

验证证据：

```text
cargo test -p agentforge-control-plane -p agentforge-worker-daemon \
  --locked --offline                                                        PASS
cargo clippy -p agentforge-control-plane -p agentforge-worker-daemon \
  --all-targets --all-features --locked --offline -- -D warnings            PASS
bash tests/contract/workspace_layout.sh                                      PASS
cargo build --workspace --locked --all-targets --offline                     PASS
cargo test --workspace --locked --offline                                    PASS
cargo clippy --workspace --locked --all-targets --all-features \
  --offline -- -D warnings                                                   PASS
node --check crates/control-plane/assets/app.js                              PASS
cargo fmt --all -- --check / git diff --check                               PASS
```

明确边界：P.6 只交付 wire 与 adapter，不会在调用前临时生成 Candidate lineage。Worker 尚未把完整
`RecordCandidateInput` 先写入 SQLite，也尚未在 startup pending-first 阶段重放，因此 daemon 仍停在
`HandingOffCandidate`。下一 checkpoint 必须新增 schema v8 handoff intent、严格 response 绑定和 ACK-loss
恢复，然后才允许 fixture daemon 自动关闭作者 Lease。
