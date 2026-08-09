# AgentForge M1 生产化纵切进度报告

- 启动日期：2026-08-10
- 基线：`main@58ebbb674c57dd87320e8e0e4b3b23708b16be1b`
- 开发分支：`agent/m1-production-vertical-slice`
- 当前状态：三条互不阻塞的实现线并行开发中

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

### M1-PROD-02：PostgreSQL UoW Phase 1

- receipt-first：相同 actor/key/hash 精确回放，不同 payload 返回稳定 reuse 错误；
- aggregate version CAS、domain event 与 Outbox 在同一数据库事务提交；
- Inbox 消费幂等；Outbox 使用有 generation/TTL 的 `SKIP LOCKED` claim；
- 真实 PostgreSQL 测试覆盖并发 CAS、重复命令、回滚和至少一次投递。

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
| M1-PROD-00 分支与执行账本 | 进行中 | 本文件提交后回填 |
| M1-PROD-01 独立 RunClaim | 开发中 | 待回填 |
| M1-PROD-02 PostgreSQL UoW | 开发中 | 待回填 |
| M1-PROD-03 Durable control boundary | 开发中 | 待回填 |
| M1-PROD-GATE 集成验收 | 未开始 | 三条线冻结后执行 |
