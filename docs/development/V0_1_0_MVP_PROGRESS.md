# AgentForge `v0.1.0-mvp` 开发进度

- 分支：`agent/v0.1.0-mvp`
- 更新日期：2026-08-10
- 总体状态：MVP-01 完成；MVP-02 基础层实施中
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

状态：实现完成，全工作区门禁与 CI #78 Rust job 通过。CI #78 的 PostgreSQL migration job 继续发现
同类正向 fixture 问题：VerificationRun 初态要求 `queued_at == updated_at`，测试却分别调用两次 volatile
`clock_timestamp()`；已在 Checkpoint M.1 改用同一 transaction timestamp，等待修复推送后的 PostgreSQL
17 复验。

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
