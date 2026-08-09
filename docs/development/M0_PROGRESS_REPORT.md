# AgentForge M0 阶段进度报告

- 报告日期：2026-08-08
- 阶段：M0 协议与状态机基础
- 结论：已完成，M1 尚未启动
- 依据：[`09_MILESTONES_AND_WORK_PACKAGES.md`](09_MILESTONES_AND_WORK_PACKAGES.md) 中 `WP-M0-001` 至 `WP-M0-008`

## 1. 本阶段交付

| 工单 | 状态 | 可执行交付与验收证据 |
| --- | --- | --- |
| `WP-M0-001` | 完成 | 固定 Rust toolchain、13 个 crate 的 Cargo workspace、`Cargo.lock`、CI、依赖边界与仓库卫生检查；`metadata/build/test --locked` 通过 |
| `WP-M0-002` | 完成 | AFWP/Submission 严格 Rust 类型、内嵌 Draft 2020-12 Schema、Schema digest API；重复键、未知字段、非法 ID/hash/OID 和递归 required-field 删除矩阵均有负例 |
| `WP-M0-003` | 完成 | `WorkPackage`、`Attempt`、`Lease`、`Submission` 四聚合的显式转移表、纯 `decide/apply/replay`、CAS 输入与终态不变量；Lease ledger 属性测试持续验证单一 ACTIVE generation |
| `WP-M0-004` | 完成 | `AFWP-C14N-1` JCS package hash；仓库样例命中固定摘要；Rust 与独立 JavaScript 实现的 1,000 个向量完全一致 |
| `WP-M0-005` | 完成 | 版本化 Event Envelope、命令元数据、receipt-first 幂等、稳定错误码与脱敏 details；同 key 不同 project/command/payload 被稳定拒绝 |
| `WP-M0-006` | 完成 | 固定时钟、确定性 UUID v7/RNG、Event Recorder、JSON/JUnit Evidence；临时 HOME 与 Git credential 隔离，Linux 子进程用 seccomp 强制禁止 socket，非支持平台 fail closed |
| `WP-M0-007` | 完成 | publish 与 candidate-ready 两个语义 profile、稳定 finding code/JSON Pointer、AFWP/Submission/WorkGraph CLI；覆盖 DAG、委派、路径门禁、四头一致、Reviewer 独立性与 early-failure stage shape |
| `WP-M0-008` | 完成 | 内存 aggregate/outbox/inbox 故障模拟器；10,000 个固定 seed 调度覆盖重放、重复、乱序、丢响应、过期和 generation fencing；失败 seed 可持久化单独重放；关键轨迹与真实 domain 聚合做差分 |

## 2. 已冻结的关键语义

1. 作者只能在有效 author Lease 下登记不可变 Candidate；登记成功即关闭作者写权限。
2. Verification Coordinator 使用独立 service identity、verification job capability 与 CAS 生成终态 Submission，不要求已经关闭的 author Lease 仍有效。
3. receipt lookup 先于当前 Lease 校验；相同 actor/key/request hash 在 ACK 丢失后返回首次结果，不产生第二个副作用。
4. 相同 actor/key 若 project、command type 或 payload 任一变化，返回 `AF_IDEMPOTENCY_KEY_REUSED`。
5. 新 generation 生效后，旧 generation 的 Renew、Artifact、Checkpoint 和 Candidate 新写入全部被 fencing 拒绝；仅已提交命令的精确 receipt replay 可以成功返回历史结果。
6. PASS Submission 必须满足 `CandidateHead = TestedHead = ReviewedHead = SubmittedHead`；早期 FAIL/INCONCLUSIVE 只能携带实际执行到该阶段的事实。
7. Candidate 封存后，同一 Attempt 不得回到作者开发态，也不得再以 `MarkLost`/`MarkFailed` 改写 Candidate lineage。

## 3. 独立审查发现与修复

M0 完成后执行了只读 Critic gate，并把发现作为发布阻断项处理：

- 修正 Candidate-first 边界，拆开作者 `RecordCandidate` 与 Coordinator `FinalizeSubmission`；
- 修正 receipt scope，使同 key 跨 project/command 的复用不能误回放；
- 把 Lease 终态检查放在 CAS 前，保证新 mutation 返回稳定的 `AF_TRANSITION_INVALID`；
- 删除领域层系统时钟 ID 生成，所有 ID 从受信应用边界注入；
- 拒绝超出 JavaScript safe-integer 范围的整数及其 `.0` 表示，消除 JCS 数字碰撞；
- 收紧 changed-path gate，拒绝无界 `**` 和保护前缀的字符串截断匹配；
- 增加 candidate-ready、provenance early-failure 和递归 required-path 正反例；
- 将故障模拟从自包含参考模型扩展到真实 domain 差分轨迹，并覆盖四类受 fencing 的正式作者写入；
- 将测试 helper 的网络禁用从环境约定升级为内核 seccomp 拒绝，并用真实 socket 调用验证。

发布前复核没有遗留 P0/P1 阻断项。

## 4. 发布门禁

以下命令在全新 `CARGO_TARGET_DIR` 中执行，最终结果记录在本次提交与 GitHub Actions 中：

```bash
cargo fmt --all -- --check
cargo metadata --locked --no-deps --format-version 1
bash tests/contract/workspace_layout.sh
cargo build --workspace --locked --all-targets
cargo clippy --workspace --locked --all-targets --all-features -- -D warnings
cargo test --workspace --locked
cargo run --locked -p agentforge-control-plane --bin af-cli -- schema list
cargo run --locked -p agentforge-control-plane --bin af-cli -- \
  afwp hash examples/afwp-lease-fencing.json
cargo run --locked -p agentforge-control-plane --bin af-cli -- \
  afwp lint examples/afwp-lease-fencing.json --profile publish
cargo run --locked -p agentforge-control-plane --bin af-cli -- \
  submission lint examples/submission-lease-fencing.json --profile candidate-ready
```

冻结快照的本地 clean-target 结果：

| 门禁 | 结果 |
| --- | --- |
| Workspace metadata / repository contract | 13 个预期 package，PASS |
| Build `--workspace --locked --all-targets` | PASS，19 秒 |
| Clippy `--all-targets --all-features -D warnings` | PASS，9 秒 |
| Workspace tests | 97/97 PASS，24 秒 |
| 10,000 × 64 seeded schedules | PASS，约 16 秒 |
| Protocol JavaScript JCS differential | 1,000 个 property vectors + 10 个边界向量，PASS |
| CLI publish / candidate-ready / self-contained graph | `valid: true`；candidate-ready 明示 5 类待可信基础设施核验的 external facts |
| Markdown / JSON / relative links / credential scan | PASS |

固定 AFWP 摘要为：

```text
sha256:e02271c1d96c4b82fa250eecaac34ea4a8542639b9d87d7cf4b26d11ac95b83f
```

## 5. 明确不在 M0 的范围

M0 提供可执行的协议、领域不变量和测试基础，但不宣称已经有可部署的 Agent 工厂。以下能力按权威里程碑留给后续阶段：

- M1：PostgreSQL source of truth、控制平面 API、Outbox/Inbox、Lease Claim/Renew/Expire 与 Obligation Engine；
- M2：Worker Daemon、jcode sidecar、Turn Pump、SQLite Journal 与恢复；
- M3：隔离 Runner、Evidence 签名信任注册表、Candidate Artifact、Git Relay 与 Merge Queue；
- M4/M5：能力路由、悬赏、信誉、Boss 分层规划和受控 PlanPatch。

`candidate-ready` 的 M0 实现负责静态 Schema/语义门禁；密钥授权、Ed25519 信任链、Runner 隔离和服务端数据库联结检查仍由 M3 工单实现，不能用 M0 CLI 代替。

## 6. 下一阶段建议

严格按 `WP-M1-001 -> WP-M1-002 -> WP-M1-003` 启动最小控制平面纵向闭环：先落迁移与 repository port，再实现 Package 发布/Claim/Lease 事务，最后接 Outbox/Inbox。M1 的任何写 API 都必须复用本阶段冻结的强类型 ID、receipt-first、CAS、fencing 和稳定错误码，不得另建一套近义协议。
