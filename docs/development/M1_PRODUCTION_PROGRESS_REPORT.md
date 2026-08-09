# AgentForge M1 生产化纵切进度报告

- 启动日期：2026-08-10
- 基线：`main@58ebbb674c57dd87320e8e0e4b3b23708b16be1b`
- 开发分支：`agent/m1-production-vertical-slice`
- 当前状态：三条 Phase 1 生产纵切已实现并通过本地与远端集成门禁；Phase 2 typed
  repositories、远端 PostgreSQL TLS adapter 与真正的 durable projection adapter 尚未开始

## 1. 本轮目标

本轮不新增近义概念，也不把 M1 reference implementation 宣称为生产就绪。目标是在已合并的
Invocation、Governance、投影和 PostgreSQL schema 上补齐三个最小生产纵切：

1. 独立 `RunClaim` aggregate，彻底分离 Task Lease 与单次模型激活授权；
2. PostgreSQL 事务 Unit of Work，实现 event、receipt、Inbox 与 Outbox 的原子闭环；
3. 可替换的 durable Control Room source 与认证授权边界，为重启续传和生产 adapter 留出强契约。

## 2. 并行任务包

### M1-PROD-01：独立 RunClaim

- 独立标识、版本、generation、holder、Run binding、expiry 与终态；
- Grant/Renew/Release/Expire/Revoke/Supersede 使用纯 `decide -> event -> apply -> replay`；
- InvocationRun 只持不可变且经过验证的 claim binding；
- 旧 generation、错误 holder、过期授权、恶意 event/replay 与终态复活必须稳定失败。

最终实现还包含以下恢复与兼容边界：

- `InvocationRun` 的公开 transition 必须消费经过验证的独立 `RunClaim` witness；takeover 以
  run-scoped current claim head 严格推进 generation，并支持服务器过期、撤销和治理抢占；
- 终结 Run 与终结 Claim 只通过 crate 内不可拆分的组合 effect 产生，交给后续 application UoW
  原子持久化；领域公共 API 不暴露只保存一侧的入口；
- 当前 v2 事件采用 JCS digest；真实 main-v1 事件先按历史字节校验 digest，再显式 upcast。
  `Running / Reconciling / terminal` 的 v1 snapshot 禁止猜测最后事件时间，只能借助已验证回放恢复；
- 权威 aggregate 不能绕过 snapshot validator 直接反序列化；终态 Claim 的时间与结果摘要必须与
  Completed、Failed 或 Cancelled Run 的终态事实严格对应。

### M1-PROD-02：PostgreSQL UoW Phase 1

- receipt-first 明确区分 `Missing / Replay / Expired / Legacy`；scope/payload reuse
  在 expiry 之前判冲突，到期或旧格式 tombstone 都不得重新执行；
- `aggregate_event_heads` 只维护事件流 version/sequence head；同一 command version 可原子追加
  多个连续事件，持久游标来自 PostgreSQL `global_sequence`，不得用 aggregate-local sequence 冒充；
  `domain_events.envelope_version` 显式区分历史 v1 digest 与当前 JCS v2 digest；
- `0004` 是需要协调 writer quiescence 的滚动发布边界：迁移在校验/派生事件历史前持有
  `domain_events` 的 `ACCESS EXCLUSIVE` 锁直至提交；旧 writer 若在迁移后继续省略
  `envelope_version` 会因默认值已删除而 fail closed。发布顺序必须是 quiesce legacy writers →
  apply `0004` → deploy new writers → resume traffic，不支持旧/新 writer 无协调混跑；
- Inbox 以 Outbox/broker message ID 去重；Outbox 使用 generation、TTL 和确定返回顺序的
  `SKIP LOCKED` claim；所有语义写错误将 UoW 标为 rollback-only；
- 本 Phase **不实现**通用 JSON `Repository<A>`，也不宣称 typed aggregate repository 已生产化。
  `work_packages`、`run_claims` 等 canonical typed tables 的逐类型 repository/跨表不变量写入属于
  M1-PROD-02 Phase 2；在它完成前，Phase 1 只能作为 event/receipt/outbox/inbox 事务基础设施；
- Phase 1 的 `NoTls` factory 名称和构造器显式标记为 local-only，并拒绝非 Unix socket/loopback
  地址（包括拒绝 `hostaddr` 覆盖到远端）；构造时必须给出 trusted schema，每个事务固定使用
  `pg_catalog, trusted_schema, pg_temp`。可连接远端 PostgreSQL 的证书校验/TLS factory 属于
  Phase 2，当前不得用于远端部署；
- CI 的 PostgreSQL service job 必须同时运行 migration 与 UoW 条件合同；无数据库环境时的本地
  skip 仅是可编译证据，不计作真实 PostgreSQL 验收通过。

### M1-PROD-03：Durable Control Plane Boundary

- snapshot 与 SSE 共同依赖可替换的 projection source，不直接依赖进程内 map；
- cursor 绑定 Project、store epoch 与 durable change sequence；
- actor/project authorization fail closed，本地 reference actor 必须显式命名；
- 测试覆盖进程重建、epoch 变化、跨项目 cursor、未授权 SSE 与篡改 cursor。

## 3. 合并与保存纪律

- 每条实现线通过自身 fmt/test/clippy 后形成独立 commit，并立即 push；
- 跨 crate 集成只在各自边界冻结后进行，不用一次大提交掩盖接口漂移；
- 远端 commit SHA 和 CI 状态写回本报告；本地 commit 不等于已保存；
- 若某条线未达到验收标准，保留为明确的 Phase 1，不以测试数量替代生产事实。

## 4. 检查点

| 检查点 | 状态 | 远端证据 |
| --- | --- | --- |
| M1-PROD-00 分支与执行账本 | 已保存，持续更新 | 远端 `404c23b` |
| M1-PROD-01 独立 RunClaim | Phase 1 完成，独立复核无 P0/P1 | 初始远端 `4cba219`；最终远端 `6900dcee`；domain 63 unit + 2 property + 12 contract + 1 compile-fail doctest 全绿；GitHub Actions run #41 success |
| M1-PROD-02 PostgreSQL UoW Phase 1 | Phase 1 完成，真实 PostgreSQL 17 合同通过 | 远端 `d2c53bd`；run #41 的 `postgres-migrations` job 实际通过 migration 重复执行/约束检查和 transactional UoW 合同；Phase 2 typed repositories/TLS adapter 未开始 |
| M1-PROD-03 Durable control boundary | Phase 1 完成，独立复核无 P0/P1 | 初始 `1cc715a`；最终远端 `8256a16`，GitHub Actions run #37 success；snapshot→SSE handoff、scope cursor、source contract reset 与移动端 inspector 合同全绿 |
| M1-PROD-GATE 集成验收 | 本地与远端全绿 | 最终纵切远端 `6900dcee`，GitHub Actions run #41 success；本地 locked workspace tests、workspace strict Clippy、fmt、UI JS syntax 与 diff-check 全绿；真实 PostgreSQL 17 job 全绿 |
