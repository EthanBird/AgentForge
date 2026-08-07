# ADR-0004：使用 TypeScript Sidecar 集成 jcode

- 状态：Accepted
- 日期：2026-08-07
- 决策者：AgentForge 架构组
- 影响范围：`worker-daemon`、`adapters/jcode-bridge`、Worker 镜像、Attempt 恢复
- 详细规范：[04_WORKER_RUNTIME_JCODE.md](../development/04_WORKER_RUNTIME_JCODE.md)

---

## 1. 背景

AgentForge 的 Worker 需要驱动 jcode 完成代码理解、规划、编辑和工具调用，但系统的租约、预算、验收、故障恢复和 Git 来源必须由确定性 Runtime 控制。核心 Worker 选择 Rust，以便构建低开销、强类型的耐久状态机；jcode 当前提供官方 TypeScript SDK，稳定边界为 harness protocol v1，并提供：

- 私有实例 `launch()`；
- 持久 `jcodeHome` 和 session 恢复；
- `run`、`runStructured`、事件流、permission、`softInterrupt`；
- Unix socket 上的 NDJSON；
- Node.js 20+，Linux/macOS E2E，Windows 尚无完整实时 E2E 覆盖。

SDK 事件流不是历史事件账本；意外断线前发出的事件无法保证重放。因此无论选择何种接法，AgentForge 都必须有自己的本地 Journal 和状态对账。

直接让 jcode 成为 Worker 主进程存在以下问题：

- 一次 `turn_done` 会自然结束模型回合，但不等于任务满足验收；
- jcode 会话状态不能承担 lease/fencing 的事实源；
- jcode 进程不能持有宿主 Git/节点凭据；
- SDK、Runtime 与 AgentForge 发布周期需要解耦；
- jcode 实例隔离不等于不可信代码沙箱。

---

## 2. 决策驱动因素

按优先级排序：

1. 租约、状态和外部副作用的正确性不能依赖 LLM/jcode 会话。
2. 使用 jcode 官方稳定 API，避免复制内部协议和紧跟内部重构。
3. Worker daemon 崩溃或 Sidecar 崩溃后可恢复，并能处理 outcome unknown。
4. 每个 Attempt 的权限、文件和进程能被整体隔离、停止和回收。
5. jcode/SDK 升级可以 canary、回滚，不要求重写核心状态机。
6. 保持未来替换或并列接入其他 coding harness 的可能。
7. MVP 优先 Linux 的可验证可用性。

---

## 3. 备选方案

### 方案 A：Rust daemon 直接启动 `jcode run` CLI

优点：实现最少，Node 依赖较少。

缺点：

- CLI 文本输出比 SDK Schema 更脆弱；
- 事件、permission、structured output、session 恢复控制不足；
- 长任务需要解析 stdout/PTY；
- outcome unknown 和协议演进难以可靠处理。

结论：仅保留为诊断/开发 fallback，不作为生产适配器。

### 方案 B：Rust daemon 直接实现 jcode socket 协议或绑定 Rust SDK

优点：少一个 Node 进程和 AgentForge IPC 层；可能降低内存与延迟。

缺点：

- 直接实现协议会让核心 daemon 紧耦合 jcode 细节；
- jcode 的 SDK 能力和版本演进会扩大 Rust 核心的变更面；
- SDK/agent 崩溃与 Worker 状态机在同进程或同发布单元内，隔离较弱；
- 当前目标明确要求基于 TypeScript SDK 建设 jcode 适配层。

结论：不作为 MVP。未来官方 Rust SDK在真实 E2E、API 覆盖和升级稳定性上成熟后，可通过同一 AgentForge Adapter 契约重新评估。

### 方案 C：Rust daemon + TypeScript Sidecar + 官方 SDK

优点：

- 使用官方 TS SDK 的稳定协议、结构化输出和 session 能力；
- jcode 相关版本/崩溃被限制在 Sidecar 进程；
- AgentForge 对外只维护更窄、面向 WorkPackage 的协议；
- 可以独立 mock Sidecar，确定性测试 Worker；
- 容易把 Sidecar+jcode 放进每 Attempt 容器整体隔离。

缺点：

- 引入 Node.js 20 与额外 IPC；
- 协议和跨语言 Schema 需要维护；
- 每 Attempt 一个 Sidecar 有额外基线内存。

结论：选用。

### 方案 D：Fork/嵌入 jcode 源码为 AgentForge 子模块

优点：能深度定制。

缺点：维护负担、升级冲突和安全审计面最大；会把 AgentForge 与一个具体 harness 永久绑定。

结论：拒绝。

---

## 4. 决策

AgentForge 采用方案 C：

1. Rust `worker-daemon` 是 Attempt 状态、SQLite Journal、租约、预算、Turn Pump、Watchdog 和验收矩阵的唯一事实控制者。
2. 每个 Attempt 启动一个隔离容器；容器内运行一个 TypeScript `jcode-bridge` 和由 SDK `JcodeClient.launch()` 创建的私有 jcode 实例。
3. 使用固定 `jcodeHome` 持久化该 Attempt 的 session；不同 Attempt 不共享 jcodeHome。
4. `jcode-bridge` 使用精确锁定的 `@1jehuang/jcode-sdk` 与 jcode runtime 版本。
5. daemon 与 Sidecar 通过 owner-only Unix socket 上的 NDJSON `af-jcode/1` 通信。
6. 生产强制 `inheritLogins=false`；模型调用经宿主 Model Proxy/短期能力处理。
7. Sidecar 不拥有 Git、节点、对象存储或保护分支凭据。
8. `turn_done` 只上报一个回合结束；是否继续、进入仅可恢复到 `Planning/Implementing` 的等待，或以 `LocalCandidateReady` 封存候选，由 Rust Turn Pump 决定。
9. Sidecar 断连时模型 turn 进入 `UNKNOWN`，先用同一 jcodeHome/session history/Git tree 对账，禁止原样盲重放。
10. jcode 原始事件只作观察；AC PASS、Candidate 和 Submission 必须由 AgentForge Verifier/Git Broker 产生。
11. jcode/Author Worker 的责任止于 Candidate；它不能声称最终 IntegrationHead 已通过。只有中央链路得到 `PASS/candidate-ready` 时才要求 `Tested/Reviewed/Submitted = Candidate`；提前 `FAIL/INCONCLUSIVE` 只记录实际产生且有签名证据的 Head。随后对 `Candidate + 最新 TargetBaseline` 形成的新 IntegrationHead 运行 L5。
12. Baseline blocker 结束当前本地 Attempt 并请求控制面在条件修复后创建新 Attempt；MVP 不把 Baseline 快照保存为可唤醒的 `WaitingInput` 恢复点。

---

## 5. AgentForge Sidecar 契约摘要

### 5.1 Handshake

```text
daemon hello(protocol=af-jcode/1, required_capabilities, client_nonce)
  -> sidecar hello_ok(bridge/sdk/runtime versions, sdk protocol, capabilities,
                      server_nonce, HMAC bootstrap-secret proof)
```

major 不兼容或必需 capability 缺失时 fail closed。

### 5.2 命令面

只允许：

```text
start / resume
run_turn / run_structured
soft_interrupt / cancel
permission_decision
snapshot
shutdown
```

不暴露任意 SDK 反射调用，不暴露 `autoApprove`，不允许 Sidecar 请求宿主执行任意 shell。

### 5.3 事件面

```text
ready
turn_started / turn_completed / turn_failed
jcode_event（脱敏、可前向兼容）
permission_requested
snapshot_result
bridge_health / fatal
```

单帧上限 1 MiB；大内容用容器内 content ref + SHA-256。请求用 `request_id`，模型回合使用稳定 `turn_id`。

### 5.4 恢复语义

```text
Journal 写 TurnPlanned
-> Sidecar 发送 turn
-> Journal 写观察事件
-> TurnCompleted 后验证结构化结果
```

任一阶段崩溃：

- 未发送：从 pending operation 正常执行；
- 已发送且已完成：从 session/history 和 tree 重建摘要；
- 已发送且运行中：重新 attach；
- 无法判定：产生 recovery turn/人工诊断，不复制旧 turn。

---

## 6. 安全与部署约束

- Sidecar+jcode 必须在 Attempt 容器/VM 内；只把工具命令放容器而让 jcode 跑宿主不合规。
- UDS 目录 `0700`、socket `0600`，验证 peer credentials；一次性 bootstrap secret 通过继承 FD 交付，HMAC 握手后关闭且不传给 jcode 子进程。
- rootless、只读 rootfs、cap drop、no-new-privileges、cgroup、无 Docker socket。
- 网络默认 none；远程模型通过受限 Proxy，依赖通过 allowlist Proxy。
- Sidecar permission 必须由 daemon policy engine 决策；仓库内容不能改变权限。
- 完整 transcript 默认不进入普通日志；关键事件、usage、结果 digest 进入 Journal/Evidence。
- Linux 是首个生产支持平台；Windows WSL2 为实验路径，原生 Windows 在官方和 AgentForge E2E 完成前不进入关键池。

---

## 7. 结果与代价

### 正面结果

- Worker 的耐久性与 jcode 的语义能力明确分层。
- 可以在无真实模型的 fake Sidecar 下对状态机和故障点做完整测试。
- jcode 版本升级只影响 Adapter/镜像和契约测试。
- 一个 Attempt 的 Sidecar 失陷不会直接取得 Git/节点凭据。
- 未来可用相同 Worker Adapter 接入其他 harness。

### 负面结果

- 镜像包含 Rust daemon 之外的 Node runtime，构建与 SBOM 更复杂。
- 每 Attempt 多一个进程、UDS 和 version compatibility matrix。
- jcode 事件不是历史流，恢复代码必须额外读取 history/tree 并处理未知结果。
- 高频事件跨 IPC 有序列化成本，需要有界缓冲和关键事件筛选。

### 风险缓解

- package-lock 与 runtime hash 精确固定；
- 只 journal 关键事件，文本 delta 可采样/外置；
- golden protocol vectors 和 fake SDK 契约测试；
- Sidecar 内存/CPU 计入 Attempt 预算；
- 升级先跑 canary，再逐步扩大 Worker 池。

---

## 8. 验证与退出条件

本 ADR 实施完成需证明：

1. 真实 jcode 私有实例在 Linux 完成 plan、两轮开发和 structured result。
2. 第一个 `turn_done` 时仍有失败 AC，Rust daemon 能自动继续。
3. Sidecar 在 turn 中 SIGKILL 后，能用同一 jcodeHome 恢复且不复制 turn。
4. Sidecar 伪造 AC PASS 不能绕过 Verifier。
5. 容器内无法读取宿主 Git/节点凭据和其他 Attempt。
6. SDK/runtime 不兼容会在 handshake/启动时明确失败，不半工作。
7. `npm run check`、Rust bridge contract tests 和 crash matrix 全绿。
8. `WaitingInput` 的持久化恢复点只能是 `Planning/Implementing`；Baseline blocker 无法被 Wake 事件原地推进，必须形成新 Attempt。

重新评估本 ADR 的条件：

- 官方 Rust SDK 在 AgentForge 所需 API、Linux/Windows E2E 和 semver 稳定性上达到或超过 TS SDK；
- Node Sidecar 的实测资源/故障成本成为规模化瓶颈；
- jcode protocol major 变化使双层协议维护成本显著上升；
- 需要在无 Node 的嵌入式 Worker 上运行。

即使未来改用 Rust SDK，`af-jcode/1` 的上层语义、Turn Pump、Journal 和安全边界仍保留；迁移是 Adapter 替换，不是把 jcode 变成事实状态机。

---

## 9. 参考

- [jcode TypeScript SDK](https://github.com/1jehuang/jcode/tree/master/sdk/typescript)
- [jcode 项目](https://github.com/1jehuang/jcode)
- [Worker Runtime 开发规范](../development/04_WORKER_RUNTIME_JCODE.md)
- [安全威胁模型](../development/07_SECURITY_THREAT_MODEL.md)
