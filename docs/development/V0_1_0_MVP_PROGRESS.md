# AgentForge `v0.1.0-mvp` 开发进度

- 分支：`agent/v0.1.0-mvp`
- 更新日期：2026-08-10
- 总体状态：MVP-01 实施中
- 发布计划：[V0_1_0_MVP_RELEASE_PLAN.md](V0_1_0_MVP_RELEASE_PLAN.md)

## MVP-01 / Checkpoint A：Typed Market 与 Lease 命令面

状态：本地门禁通过，真实 PostgreSQL 17 合同等待 GitHub Actions。

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

限制与下一步：

- 当前机器没有 PostgreSQL/docker，因此 `postgres_mvp` 在本地仅完成编译，真实事务与并发断言必须以
  本次 push 后的 PostgreSQL 17 CI 结果为准；
- HTTP `/api/v1`、管理 CLI、Lease expiry/reconciliation 以及 Worker Attempt 进度命令尚未完成；
- Release 当前只终结 Lease；在 Checkpoint B 中必须加入 typed reconciliation，使仍为 ACTIVE 的
  WorkPackage/Attempt 收敛到 `REWORK_READY`/`LOST`，不得把该中间状态当作 MVP 完成态；
- Checkpoint B 完成后再宣布 MVP-01 退出条件通过。
