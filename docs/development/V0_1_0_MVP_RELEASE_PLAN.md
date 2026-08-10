# AgentForge `v0.1.0-mvp` 发布执行计划

- 启动日期：2026-08-10
- 开发基线：`main@0fcd53284f33b15622a071be4ad37ac1a63766ff`
- 开发分支：`agent/v0.1.0-mvp`
- 发布标签：`v0.1.0-mvp`
- 当前状态：实施中
- 最新进度：[V0_1_0_MVP_PROGRESS.md](V0_1_0_MVP_PROGRESS.md)

## 1. 发布定义

`v0.1.0-mvp` 是可在一台 Linux 控制服务器、一个 PostgreSQL 实例和至少一个 Linux
Worker 上运行的本地试点版本。它必须真实完成下面的纵向闭环，而不是用 UI 假数据或只通过
领域单元测试宣称完成：

```text
创建项目并发布 AFWP
  -> Worker 查询 Offer
  -> 原子 Claim，创建 Attempt 与 ACTIVE Lease
  -> Worker Journal 驱动假 Executor 或 jcode bridge 执行
  -> 登记不可变 Candidate
  -> 独立验证生成终态 Submission
  -> Git Relay 写入任务分支
  -> 在目标基线上完成集成门禁
```

MVP 允许首个发布只支持单控制平面实例、单租户、Linux Worker、本地/私网 PostgreSQL、假
Executor 和一个受控 Git 仓库；但所有正式命令仍必须遵守 receipt-first、CAS、fencing、不可变
Candidate 和独立验收边界。

## 2. 不得降低的验收事实

- PostgreSQL typed tables 是业务事实源；不得用通用 JSON snapshot 替代 canonical rows。
- 同一 revision 最多一个 ACTIVE Lease；旧 generation 的 receipt miss 写入稳定失败。
- Claim 必须在一个事务中写入 WorkPackage、Attempt、Lease、Event、Receipt 与 Outbox。
- Worker 重启后从本地 Journal 恢复，恢复前先向服务器确认 Lease；Lease 失效立即停止作者副作用。
- Candidate、reviewed head、tested head 和 submitted head 必须相同。
- Worker 不持有目标 Git 保护分支凭据；Relay 使用独立身份。
- 控制面重启后 Outbox、SSE cursor、Lease expiry 与待验收义务可以继续收敛。
- 发布工件不得包含数据库密码、节点私钥、Git 凭据或模型凭据。

## 3. 实施检查点

### MVP-01：Typed Control Plane

交付：

- Project、PackageRevision、WorkPackage、Attempt 与 Lease 的 typed PostgreSQL repositories；
- 发布、Offer 查询、Claim、Renew、Release、Lease 查询的 application use cases；
- receipt-first、CAS、fencing、Event 与 Outbox 原子事务；
- 版本化 `/api/v1` HTTP API 和等价管理 CLI；
- 真实 PostgreSQL 并发、ACK-loss、rollback 与 expiry 合同测试。

退出条件：20 个并发 Claim 只有一个成功；相同幂等键返回首次结果；不同 payload 复用同一键失败；
旧 token/generation 无法 Renew、Release 或创建 Candidate。

### MVP-02：Recoverable Worker

交付：

- Worker enrollment 的本地试点身份；
- SQLite WAL Journal、Offer poll、Claim、Lease renew 与恢复状态机；
- 假 Executor 的确定性 Turn Pump；
- jcode bridge 的版本握手和 feature-gated adapter；
- workspace、日志和凭据目录隔离。

退出条件：每个可恢复阶段强制终止后都能继续；`turn_done` 且任务未满足时 Supervisor 自动推进；
Lease 丢失后正式副作用计数保持不变。

### MVP-03：Candidate 与独立验收

交付：

- Candidate Artifact 创建、分块写入、摘要校验与 COMPLETE 封存；
- Candidate sealing、Criterion Runner、Reviewer 与 clean reproduction；
- Evidence Manifest 和终态 Submission；
- stage-aware failure dossier 与返工入口。

退出条件：篡改 Candidate、Evidence 或任一 head 都无法 Accepted；作者身份不能单独满足 Reviewer
策略；早期失败不会伪造未执行阶段的事实。

### MVP-04：Git Relay 与端到端演示

交付：

- Git Bundle 校验、ref allowlist、任务分支 push 和幂等 Relay receipt；
- 基础 Merge Queue、目标基线漂移检测和集成 commit L5 重跑；
- 从发布 AFWP 到任务分支/集成结果的真实 E2E fixture；
- Worker 断线、Lease 过期、重复消息、控制面重启和分支冲突场景。

退出条件：外部 Worker 无 Git 凭据仍能交付；重复 Bundle 不产生重复分支；目标分支变化后必须在
新的 Integration Commit 上重跑门禁。

### MVP-05：打包、运维与发布

交付：

- PostgreSQL + control-plane 的 Compose 本地试点配置；
- Worker 与控制面的 systemd 示例；
- `agentforge-admin demo bootstrap` 和可重复 smoke 脚本；
- 备份/恢复、升级、已知限制和故障排查说明；
- Linux x86_64 发布工件、SHA-256 校验和与 SBOM；
- GitHub prerelease `v0.1.0-mvp`。

退出条件：干净环境按文档可以启动并完成 E2E；发布工件与源码 tag 对应；CI 的 PostgreSQL 17、
workspace、E2E 和 artifact smoke 全绿。

## 4. 提交和远端保存纪律

每个检查点必须：

1. 先运行该切片的 fmt/test/clippy/合同测试；
2. 只提交该检查点声明的路径；
3. commit 后立即 push，并核对远端 SHA；
4. 等待对应 GitHub Actions 完成；
5. 将 SHA、run 编号、真实通过项和未验证项写回本文；
6. 发现 P0/P1 时追加修复提交，不改写已经推送的历史。

## 5. 发布门禁

```bash
cargo fmt --all -- --check
bash tests/contract/workspace_layout.sh
cargo build --workspace --locked --all-targets
cargo clippy --workspace --locked --all-targets --all-features -- -D warnings
cargo test --workspace --locked
node --check crates/control-plane/assets/app.js
```

另外必须在 CI PostgreSQL 17 service 中运行 migration、typed repository、并发 Claim、UoW rollback、
Outbox/Inbox 和 E2E 合同。本地没有数据库而跳过测试不算发布证据。

## 6. 明确不在本次发布内

- 公有悬赏市场、现金结算和多租户计费；
- 控制平面多副本与跨地域强一致；
- Windows 原生 Worker 正式支持；
- GPU 调度和自动学习路由；
- 任意不可信第三方代码的完整强隔离承诺；
- 全功能项目管理后台；
- 自动生产部署与无人值守证书签发。

这些能力可以在 `v0.1.0-mvp` 之后演进，但不得提前弱化本版本的租约、证据和 Git 权限边界。
