# AFWP/1.0 协议与一致性规范

> 状态：MVP 规范基线（Normative）  
> 配套 Schema：[`schemas/afwp.schema.json`](../../schemas/afwp.schema.json)、[`schemas/submission.schema.json`](../../schemas/submission.schema.json)  
> 完整示例：[`examples/afwp-lease-fencing.json`](../../examples/afwp-lease-fencing.json)、[`examples/submission-lease-fencing.json`](../../examples/submission-lease-fencing.json)

## 1. 目的与约束词

AFWP（AgentForge Work Package）是 Boss、悬赏服务器、Worker、Reviewer、Runner 与 Integrator 之间的**不可变工作契约**。它不是聊天提示词，也不是可被 Worker 自行解释和降低标准的待办事项。

本文中的“必须”“不得”“应当”“可以”分别对应 RFC 2119 的 MUST、MUST NOT、SHOULD、MAY。实现与本文冲突时，以本文、JSON Schema 和 ADR 中标记为 Normative 的不变量为准；自然语言示例不能覆盖 Schema。

AFWP 同时固定四类契约：

1. 目标契约：业务结果和原子需求；
2. 边界契约：写集、禁区、权限和系统不变量；
3. 证据契约：可复现验收及每项证据；
4. 集成契约：Git 基线、候选 Commit、依赖与回滚。

## 2. 线上的权威表示

### 2.1 JSON 是唯一签名表示

- API、事件、内容哈希和签名只接受 UTF-8 JSON；
- YAML 只允许作为人的编辑格式，发布前必须编译成 JSON、通过 Schema 和语义 Linter，再计算哈希；
- 解析器必须拒绝重复对象键、无效 UTF-8、`NaN`、`Infinity`、尾随逗号和超出实现安全范围的整数；
- 实现不得通过字段出现顺序推断语义；
- 验收命令必须使用 `argv: string[]`，不得把任意文本拼成 `sh -c`、`cmd /c` 或 PowerShell 命令。

推荐限制：单个 AFWP 未压缩大小不超过 1 MiB，单字符串不超过 Schema 上限；超大输入必须作为内容寻址 Artifact 引用。

### 2.2 版本规则

`schema_version` 的值为 `afwp/<major>.<minor>`。本版固定为 `afwp/1.0`。

| 变更 | 版本动作 | 兼容行为 |
| --- | --- | --- |
| 破坏字段语义、删除字段、收紧已发布实例 | major + 1 | 旧消费者必须显式拒绝 |
| 增加可选能力或消息，旧消费者无法安全忽略 | minor + 1 | 通过能力协商后使用 |
| 文档勘误、描述或示例修复，合法实例集合不变 | Schema artifact patch | `schema_version` 不变 |

Schema `$id` 使用完整工件版本，例如 `.../afwp/1.0.0/schema.json`。服务端必须把受支持的 `schema_version` 与 Schema SHA-256 一起发布；不得从可变 URL 静默替换验证规则。

消费者遇到未知 major 必须返回 `AF_SCHEMA_VERSION_UNSUPPORTED`；遇到未知 minor 时只有在协商得到明确兼容声明后才能继续。禁止“尽力解析然后执行”。

## 3. 身份、revision 与内容哈希

### 3.1 不可变身份

一个任务规格版本由以下四元组唯一确定：

```text
(project_id, package_id, revision, package_hash)
```

- `package_id` 在项目内永久稳定；
- AgentForge 自身开发账本使用 `WP-M{milestone}-{nnn}`（例如 `WP-M0-001`）；用户项目可使用 `wp-<slug>`。二者都属于同一个 `package_id` 字段，不能再建立 CP/W/V 等第二套权威编号；
- `revision` 从 1 开始严格递增，不得覆盖或复用；
- 同一 `(project_id, package_id, revision)` 只能对应一个 `package_hash`；
- 修改任何会影响执行、权限、输入或验收的字段，必须产生新 revision；
- 标题拼写等纯展示变更也建议产生新 revision，避免签名内容与展示内容分叉；
- 旧 revision 只能被标记为 `SUPERSEDED`，不得物理改写。

WorkPackage 的运行状态、报价、Attempt、Lease、问题、Submission 和集成结果不写回 AFWP；它们是引用上述四元组的独立记录。

### 3.2 `AFWP-C14N-1` 哈希算法

`package_hash` 按以下确定性流程计算：

1. 使用拒绝重复键的解析器读取 UTF-8 JSON；
2. 按对应版本 JSON Schema 校验；
3. 从顶层对象**移除** `package_hash` 成员；
4. 对剩余 JSON 值执行 RFC 8785 JSON Canonicalization Scheme（JCS）；
5. 对 canonical UTF-8 bytes 计算 SHA-256；
6. 输出小写十六进制并加前缀 `sha256:`。

定义为：

```text
package_hash = "sha256:" || hex_lower(
  SHA256(JCS(remove_top_level_member(document, "package_hash")))
)
```

实现不得先对 YAML、格式化后的 JSON、压缩包或数据库行求哈希。JCS 会排序对象键，但保留数组顺序，因此 requirements、criteria 和 deliverables 调序属于内容变更。

示例任务包的预期值为：

```text
sha256:e02271c1d96c4b82fa250eecaac34ea4a8542639b9d87d7cf4b26d11ac95b83f
```

服务端在 `VALIDATING -> OFFERED` 前必须重算。哈希不匹配返回 `AF_PACKAGE_HASH_MISMATCH`，不得自动改写客户端文档。

### 3.3 Artifact 与执行环境哈希

- 所有输入 Artifact、锁文件、Runner 镜像、证据包和交付物必须有 SHA-256 或不可变 OCI digest；
- Git `base_commit`、`candidate_commit` 和 `tree_hash` 必须使用仓库原生 object format 的完整 OID，不得使用短 SHA；
- `generation` 是服务端单调递增的 fencing ordinal；领域代码中的数值 `FencingToken` 指同一个值。Adapter 可另发 `ft.v1...` 形式的短期 capability credential，其中必须绑定 generation；Submission 的 `fencing_token_hash` 只对该 credential 做审计关联，不参与大小比较，也不得包含仍可使用的 bearer token；
- 内容缺失或 digest 不一致时，Attempt 必须停在 `WAITING_INPUT` 或预检失败，不得用“相近版本”替代。

## 4. 字段语义

Schema 负责结构约束，以下规则负责跨字段语义。

| 区域 | 权威语义 |
| --- | --- |
| 身份 | 固定任务 revision 和其所属 WorkGraph 版本；`parent_id` 表示规划血缘，不表示运行时进程父子关系 |
| `goal` | 只陈述一个可独立验收的结果；背景不能代替 objective |
| `requirements` | `REQ-*` 在本 revision 内唯一；`must` 需求必须被至少一个 hard `AC-*` 覆盖 |
| `scope` | `allowed_paths` 是 Worker 可改动上界；`forbidden_paths` 优先级更高；invariant 不得被验收豁免隐式取消 |
| `snapshot` | Worker 只能从精确 `base_commit` 与固定输入开始；预检需验证锁文件 digest |
| `dependencies` | 指向精确 revision；`condition` 决定何时 Ready；运行前必须验证依赖仍满足 |
| `interfaces` | 引用先行冻结的接口 Artifact；兼容策略必须由契约测试证明 |
| `routing` | required 是硬过滤，preferred 仅用于排序；不得把推荐模型名当作硬能力事实 |
| `permissions` | 默认拒绝；宿主 Broker 执行 Git/Secret/外部副作用，jcode 沙箱不持有长期凭据 |
| `scheduling` | 预算、Attempt 数和 Lease 是硬上限；超限只能由新授权或新 revision 改变 |
| `conflicts` | 预计写集用于冲突预测；实际 changed-path 验收仍是最终边界 |
| `communication` | 只有 `must_escalate` 的条件才可阻塞；问题必须带 wake condition |
| `delegation` | 子包权限、预算、深度和种类必须是父包授权的子集 |
| `acceptance` | 每个 criterion 是可独立执行与记录的判定；`INCONCLUSIVE` 不等于 PASS |
| `deliverables` | 声明最终必须登记的逻辑产物；Submission 为其提供路径和 digest |
| `completion` | 规定分支、Commit trailer、回滚和 DoD，不赋予 Worker 合并主分支权限 |

## 5. 发布前 Definition of Ready

`afwp lint --profile publish` 至少实现以下硬检查：

1. JSON Schema 2020-12 校验通过；
2. `package_hash` 按 `AFWP-C14N-1` 重算一致；
3. requirement、criterion、deliverable、interface ID 在各自命名空间唯一；
4. 每个 `must` requirement 至少被一个 `hard: true` criterion 覆盖；
5. `covers` 不得引用不存在的 requirement；
6. 至少存在一个 hard criterion，所有 criterion 都包含可判定的 Given/When/Then；
7. 命令型 criterion 有固定 Runner、参数数组、超时、期望和证据；
8. `changed_paths` criterion 覆盖 `scope.allowed_paths` 与 `scope.forbidden_paths`；
9. 同一 WorkGraph 中依赖引用存在、revision 精确、条件合法且 DAG 无环；
10. `base_commit`、输入、锁文件和接口 Artifact 均可解析且 digest 一致；
11. `renew_after_seconds < ttl_seconds <= max_execution_seconds`；
12. `redundant` 模式有 `redundancy >= 2`，其他模式不得利用该值扩大执行数；
13. required 能力、工具、OS、模态、安全域、网络域和 Runner 资源存在至少一个可行执行池；
14. `git_write_prefix` 与 completion branch pattern 不授予保护分支；
15. 委派上限不超过父 DelegationGrant，未授权包不得生成可发布子包；
16. 预算非零且足以完成基线预检与至少一次验收；
17. 一个未读 Boss 对话的冷启动 Critic 能只根据 AFWP 输出目标、边界、首个动作和验收映射。

任何硬检查失败时状态为 `BLOCKED`，附机器可读 lint finding。DQS 等综合评分不得覆盖硬失败。

### 5.1 主观要求的落地

“美观”“高性能”“安全”“代码优雅”不能单独成为 `then`。必须转换为：

- 固定视口、主题、组件状态、参考图 digest 与视觉差异阈值；
- 固定硬件等级、预热、样本数、随机种子、分位数与回归容忍；
- 明确威胁场景、攻击输入、扫描器版本与不可接受 findings；
- 可计算的复杂度/重复阈值，或由独立 Reviewer 执行的评分量表；
- 若仍需人工判断，使用 `manual_gate`，明确审批角色、输入证据和超时行为。

## 6. 验收执行语义

### 6.1 Criterion 状态

每次运行结果只能是：

| 状态 | 含义 | 能否满足 hard criterion |
| --- | --- | --- |
| `PASS` | 在声明环境中观察值满足期望 | 是 |
| `FAIL` | 观察值确定不满足期望 | 否 |
| `INCONCLUSIVE` | Runner、依赖或方法无法给出结论 | 否 |
| `SKIPPED` | 未执行 | 否 |

`flaky_retry_limit = N` 表示首次执行后最多再执行 N 次。所有尝试必须进入证据；默认判定策略是“全部尝试均 PASS 才 PASS”，性能类任务可在新协议中定义统计策略，但不得挑选一次成功。

### 6.2 命令执行

Runner 必须：

1. 从候选 Commit 干净 checkout；
2. 验证 toolchain locks 与输入 digest；
3. 在声明的 working directory 用参数数组直接 spawn 可执行文件；
4. 只注入白名单环境变量，移除宿主 Secret；
5. 强制资源、网络和 timeout；
6. 保存 argv、退出码、stdout/stderr digest、开始时间、时长和资源量；
7. 将原始证据上传到内容寻址存储，再写 Submission 引用。

客户端不得把 `given/when/then` 当作可执行指令；它们用于复述、审查和结果解释，真正的机器行为由结构化字段控制。

## 7. 四套生命周期

### 7.1 WorkPackage 投影

```text
DRAFT -> VALIDATING -> OFFERED -> ACTIVE -> VERIFYING -> ACCEPTED
                    \-> BLOCKED       \-> REWORK_READY -/
ACCEPTED -> INTEGRATING -> INTEGRATED -> CLOSED
                         \-> REBASE_REQUIRED -/
```

旁路终态：`CANCELLED`、`SUPERSEDED`、`FAILED`。`ACCEPTED` 仅表示候选通过包级验收；只有最新目标分支上的合成 Commit 复验并合并后才是 `INTEGRATED`。

### 7.2 Attempt

```text
CREATED -> LEASED -> PREPARING -> PLANNING -> IMPLEMENTING
-> LOCAL_VERIFY -> CANDIDATE -> SUBMITTED -> PASSED | REJECTED
```

两阶段接单中的 provisional claim/preflight reservation 是调度记录，不是 Attempt 或 TaskLease 的稳定状态；预检通过后在同一事务创建 Attempt 与正式 Lease。

旁路状态：`WAITING_INPUT`、`LOST`、`CANCELLED`。等待必须带结构化 `wake_condition`；进入等待后释放模型推理资源，但保存 Journal 与工作区检查点。

### 7.3 Lease

```text
ACTIVE -> RELEASED | REVOKED | EXPIRED
```

`Renewed` 是事件，成功续租后稳定状态仍为 ACTIVE；provisional claim 不是 Lease。终态不可返回 ACTIVE。重新授予不是复活旧 Lease，而是创建新 Lease 并递增 generation。

### 7.4 Candidate、VerificationRun 与 Submission

```text
Candidate:       SEALED（不可变）
VerificationRun: QUEUED -> PROVENANCE_CHECK -> REVIEWING -> REPRODUCING
                 -> PASS | FAIL | INCONCLUSIVE | CANCELLED
Submission:      PASS | FAIL | INCONCLUSIVE | QUARANTINED（创建即终态）
```

作者在有效 Lease 下先初始化、上传并完成内容寻址的 Candidate Artifact，再由 `RecordCandidate` 原子绑定同一预留 Candidate ID 并关闭作者 Lease；独立服务随后推进 VerificationRun。Coordinator 只在 run 终结且所有事实固定后一次性创建签名 Submission，不能先建半成品再补字段。相同固定输入的瞬态 Verifier Job 可在非终态 run 内重试；run 一旦终结，MVP 对代码、任务 revision、Runner、criterion 或审查输入的任何变化都创建新 Attempt、Candidate、run 和 Submission，并通过 lineage 指向旧记录，不重新打开旧 Attempt。`QUARANTINED` 只属于独立 salvage 流程，不触发 Verification 或 Integration。

签名 Manifest 必须自描述终态：candidate 类型携带服务器 UUID `candidate_id`、`candidate_artifact_id`、`verification_run_id`、`terminal_outcome` 和 `completed_stage`；salvage 携带 `terminal_outcome=QUARANTINED` 与 `completed_stage=salvage_registration`。PASS 必须是 `candidate_ready` 且包含完整验收事实；FAIL/INCONCLUSIVE 可以在 provenance/reviewing/reproducing 提前终结，但必须携带 Failure Dossier，未执行字段保持 absent，禁止填造 Head、criterion 或 Review。

Failure Dossier 的 `evidence_digest` 必须进入同一 `AF-SUB-SIG-1` 签名体，绑定外部失败证据 manifest 的规范化摘要；`evidence_refs` 只是定位符，不能替代摘要。Schema 按 `completed_stage` 禁止携带尚未执行阶段的 candidate-only 事实，语义校验还必须逐项验证引用与数据库中已完成的 stage 事实一致。

### 7.5 状态转换规则

- 命令处理器必须以 `(aggregate_id, expected_version)` 做 CAS；
- 无效转换返回 `AF_TRANSITION_INVALID`，不得容错跳步；
- 每个成功命令与 Outbox 事件必须处于同一数据库事务；
- 投影可重建，事件账本和不可变对象才是事实来源；
- 事件消费使用 Inbox 去重，业务唯一约束是最后一道防线。

## 8. Lease fencing

### 8.1 不变量

对任意 `(project_id, package_id, revision)`，服务端维护单调 generation：

```text
grant(k + 1).generation > grant(k).generation
```

任何带副作用操作 `op` 只有满足以下条件才能提交：

```text
lease.state = ACTIVE
AND request.lease_id = lease.id
AND request.generation = lease.generation
AND now_server < lease.expires_at
AND attempt.id = lease.attempt_id
```

校验和副作用必须在同一事务中完成。先查后写但不加锁/CAS 是不合格实现。

### 8.2 参考事务

```sql
BEGIN;

-- 认证 actor、规范化 request 后，先按 actor + key 串行化并命中历史结果。
SELECT pg_advisory_xact_lock(
  hashtextextended($actor_id::text || ':' || $idempotency_key, 0));

SELECT request_hash, response_status, response_body
FROM command_receipts
WHERE actor_id = $actor_id AND idempotency_key = $idempotency_key;

-- 已存在且 request_hash 不同：ROLLBACK -> AF_IDEMPOTENCY_KEY_REUSED。
-- 已存在且相同：COMMIT 并原样返回首次结果，不再校验当前 Lease/状态。
-- 这样 RecordCandidate 首次成功关闭 Lease 后，ACK 丢失重放仍可恢复同一 Candidate/run。

-- 仅 receipt 未命中时，才锁定并校验当前领域事实。
SELECT id, attempt_id, generation, state, expires_at
FROM task_leases
WHERE project_id = $1 AND package_id = $2 AND revision = $3
FOR UPDATE;

-- 应用层严格比较 lease_id、attempt_id、generation、ACTIVE 与 server now。
-- 任一不符：ROLLBACK，并返回 AF_LEASE_STALE 或 AF_LEASE_EXPIRED。

-- 执行业务写入与 outbox 写入。
INSERT INTO command_receipts(
  actor_id, idempotency_key, request_hash, command_type,
  response_status, response_body, resource_version, replay_until)
VALUES (...); -- 与业务状态和 Outbox 同一事务

COMMIT;
```

生产实现可用条件 UPDATE、advisory lock 或序列表，但必须通过并发与乱序属性测试证明上述不变量。

### 8.3 旧结果与 salvage

旧 generation 对**回执未命中的新副作用**发起 renew/checkpoint/artifact/normal Candidate registration 时一律返回 `AF_LEASE_STALE`。已认证的原 actor 使用相同 Idempotency-Key 与相同 request hash 命中已提交回执时，必须原样回放首次结果，即使该命令已关闭 Lease；这只恢复 ACK，不重新执行领域逻辑。服务端可以提供单独的 `RegisterSalvage` 命令：

- 只写隔离的内容寻址 Artifact；
- 生成 `submission_kind: salvage` 与 `QUARANTINED` 状态；
- 不产生 Verify、Review 或 Integrate 事件；
- 新 Worker 或 Boss 显式选择后，才可把其作为新 revision/Attempt 的只读输入。

## 9. 消息信封与命令

所有 HTTP、gRPC 或消息总线适配器都归一化到以下信封：

```json
{
  "protocol_version": "agentforge/1.0",
  "message_id": "0198f221-52f8-7d6b-92b4-2d89aa25a33e",
  "type": "attempt.checkpoint",
  "occurred_at": "2026-08-07T11:00:00Z",
  "project_id": "agentforge",
  "actor_id": "worker-tokyo-03",
  "correlation_id": "0198f221-52f8-7d6b-92b4-2d89aa25a33f",
  "idempotency_key": "checkpoint:att-04:g4:seq17",
  "aggregate_version": 12,
  "payload": {}
}
```

MVP 必须实现：

| 命令/事件 | 必需引用 | 说明 |
| --- | --- | --- |
| `package.publish` / `package.offered` | package 四元组 | Schema、hash、DoR 全通过才发布 |
| `bid.place` / `bid.recorded` | offer、executor fingerprint、估时/成本 | Bid 过期后不可 claim |
| `lease.provision` | bid、attempt | 仅允许拉基线和预检 |
| `lease.activate` / `lease.granted` | attempt、generation | 原子分配 generation |
| `lease.renew` / `lease.renewed` | lease、generation、progress_seq | 必须带语义检查点 |
| `attempt.checkpoint` | lease、generation、progress_seq | `progress_seq` 单调递增 |
| `attempt.wait` | wake condition | 不占用模型槽位 |
| `candidate_artifact.init` / `candidate_artifact.completed` | attempt、lease、预留 candidate ID、OID/tree/digest | 完成上传仍不等于正式 Candidate |
| `candidate.record` / `candidate.recorded` | package、attempt、lease、candidate | 有效 fencing 下不可变登记，并创建 VerificationRun |
| `submission.salvage` | 过期 lease、artifact digest | 必然隔离 |
| `verification.advance` | candidate、run、expected version | Reviewer/Runner 只读候选并以 CAS 推进 |
| `submission.finalize` / `submission.finalized` | terminal run、candidate、三 Head、Evidence | Coordinator 一次性创建终态记录 |
| `integration.request` | PASS submission | Merge Queue 再验证 fencing 与 head |

消息总线采用至少一次投递；消费者必须预期重复、乱序和重连。不能依赖“消息只来一次”。

## 10. 幂等、顺序与一致性

### 10.1 Idempotency key

- 所有有副作用命令必须有 idempotency key；
- 去重作用域至少包含项目、命令类型和 actor；
- 服务端保存 canonical request hash 与原响应；
- 同 key 同 payload 返回原响应；同 key 不同 payload 返回 `AF_IDEMPOTENCY_KEY_REUSED`；
- key 的保留期不得短于关联 aggregate 的可重试窗口；Submission/Git 集成 key 永久保留。

### 10.2 序列

- `progress_seq` 仅在 Attempt + generation 内单调；较小序号可回复原确认但不得回滚进度；
- aggregate event version 严格递增；投影发现缺口时暂停并补读，不得猜测中间状态；
- 时间戳用于观测而非互斥，权威顺序来自数据库事务、generation、aggregate version 和 Git OID。

### 10.3 Git 一致性

PASS Submission 登记后，以下值不可改变且必须相等；提前失败的 Submission 只携带实际产生的 Head：

```text
SubmittedHead = ReviewedHead = TestedHead = candidate_commit
```

Merge Queue 必须验证：当前 package revision、active/accepted lineage、candidate OID、evidence digest 与 integration target。若目标分支变化，只能创建合成 Commit 并重跑 L5；不得 amend 已验收候选。

## 11. Submission 判定

结构 Schema 通过后，`submission lint --profile candidate-ready` 必须验证：

1. package ID、revision、hash 与已发布 AFWP 精确一致；
2. Attempt 属于该 package revision；
3. CandidateRecord 证明其 Lease 在 **Candidate 登记事务** 中为 ACTIVE、未过服务器时间且 generation 当前；Submission finalization 只验证这份历史 fencing provenance，不要求已经关闭的作者 Lease 仍为 ACTIVE；
4. `base_commit` 等于 AFWP snapshot；
5. Git branch 满足 prefix/pattern，candidate 可达且 tree hash 正确；
6. deliverables 与 AFWP 中所有 `required: true` 逻辑产物一一对应；
7. AFWP 中每个 criterion 恰有一个结果，所有 hard 项为 PASS；
8. Runner digest、重试次数和证据引用符合 criterion；
9. 没有 open critical/high finding；review verdict 为 pass；
10. `reviewed_head = tested_head = candidate_commit`；
11. clean reproduction 为 pass；
12. Evidence Bundle digest、内部 manifest 和 Ed25519 签名有效；
13. changed paths 未越界，Commit trailer 完整且值一致；
14. provenance 中的 Executor fingerprint 与授予 Lease 时相同。
15. Submission 自身的 registrar/coordinator Ed25519 签名按下述算法验证通过。

### 11.1 Submission Manifest 签名

`provenance` 描述作者来源，但其中的 `signature` 由一次性组装不可变 Manifest 的 Verification Coordinator（candidate）或 Salvage Registrar（salvage）产生；作者对本地 Evidence 的签名另存，不得把作者早期签名冒充最终 Submission 签名。

签名算法固定为 `AF-SUB-SIG-1`：

1. 按严格 JSON 解析和当前 Submission Schema 校验；
2. 深拷贝文档，只移除 `provenance.signature.signed_digest` 与 `provenance.signature.value`，保留 `algorithm`、`key_id`、`signer_role` 和所有其他字段；
3. 对剩余文档执行 RFC 8785 JCS，得到 `canonical_bytes`；
4. 构造 `signing_input = UTF8("AgentForge Submission Manifest v1\\0") || canonical_bytes`，其中 `\\0` 是单个 NUL 字节；
5. `signed_digest = "sha256:" || hex_lower(SHA256(signing_input))`；
6. `value = base64url_no_pad(Ed25519.sign(signing_input, private_key))`；
7. 验证方按 `key_id` 从节点/服务身份注册表取得公钥，核对签名时角色、状态和吊销时间，再验证 digest 与 Ed25519；文档内自带但未受信的公钥不能建立信任。

错误归一化是协议的一部分：若严格 Schema 的唯一失败是 `submission_kind` 对 `provenance.signature.signer_role` 的条件 `const` 不匹配，外部稳定码必须归一为 `AF_SIGNATURE_INVALID`，不能暴露 AJV/实现细节；其他结构错误返回 `AF_SCHEMA_INVALID`。若同一对象同时存在角色错配和其他结构错误，优先返回 `AF_SCHEMA_INVALID`。角色正确但 key 未获该角色授权、已吊销、digest/Ed25519 无效也返回 `AF_SIGNATURE_INVALID`。

仓库示例使用公开的 fixture-only 公钥 [`examples/keys/agentforge-fixture-submission-signer-v1.json`](../../examples/keys/agentforge-fixture-submission-signer-v1.json)。该 key 只能证明测试向量自洽，任何生产配置都必须拒绝它。

`salvage` Submission 不执行 candidate-ready，而执行以下 quarantine profile：

1. `submission_kind=salvage`、`terminal_outcome=QUARANTINED`、`completed_stage=salvage_registration`；
2. package/revision/hash、Attempt、旧 Lease/generation 与服务器历史记录可解析，且 quarantine reason 与失效事实相符；
3. `salvage.artifact_uri/sha256/size_bytes/base_commit` 经 HEAD、digest、大小和仓库映射校验；可选 candidate commit 只表示待人工选择的隔离对象；
4. Manifest 不含 Candidate/VerificationRun、Git 验收、criteria、Review、Clean Reproduction、Evidence Bundle 或 Failure Dossier 字段；
5. `provenance.signature.signer_role=salvage_registrar`，签名 key 在登记时有效；candidate 类型则必须是 `verification_coordinator`；
6. lineage 合法，Registrar 权限只允许 quarantine；成功登记不得创建 VerificationRun、Relay Ticket、Integration 或改变 WorkPackage 状态；
7. normal Candidate endpoint 的 stale/expired 请求必须拒绝，绝不能自动转换为 salvage。调用者必须显式使用独立 API 和新的幂等键。

## 12. 错误模型

统一响应：

```json
{
  "error": {
    "code": "AF_LEASE_STALE",
    "message": "lease generation is not current",
    "retryable": false,
    "correlation_id": "0198f221-52f8-7d6b-92b4-2d89aa25a33f",
    "details": {
      "lease_id": "lease-fencing-03",
      "received_generation": 3,
      "current_generation": 4
    }
  }
}
```

`message` 面向日志，不允许客户端通过匹配 message 决策；只按稳定 wire code 决策。领域条件 `StaleLease/InvalidTransition` 分别映射为 `AF_LEASE_STALE/AF_TRANSITION_INVALID`。公开 API 的 details 不得泄露 Secret、提示词、源码片段或其他租户信息。

| code | HTTP | 可重试 | 处理 |
| --- | ---: | :---: | --- |
| `AF_SCHEMA_INVALID` | 422 | 否 | 修复实例 |
| `AF_SCHEMA_VERSION_UNSUPPORTED` | 426 | 否 | 协商/升级 |
| `AF_PACKAGE_HASH_MISMATCH` | 422 | 否 | 重算并创建正确 revision |
| `AF_REVISION_CONFLICT` | 409 | 否 | 拉取现有 revision，不得覆盖 |
| `AF_PACKAGE_NOT_READY` | 422 | 修正规格/等待事实 | 查看 failed checks |
| `AF_PACKAGE_NOT_CLAIMABLE` | 409 | 是 | 刷新 Offer/Claim 状态 |
| `AF_NO_FEASIBLE_EXECUTOR` | 422 | 注册表/约束变化后 | 增加合格 Executor、Runner，或修订任务硬约束 |
| `AF_BID_EXPIRED` | 409 | 是 | 重新报价 |
| `AF_DEPENDENCY_UNAVAILABLE` | 503 | 是 | 修复 Git/Artifact/基础设施后重试 |
| `AF_LEASE_STALE` | 409 | 否 | 停止正式写入，可登记 salvage |
| `AF_LEASE_EXPIRED` | 410 | 否 | 等待重新授予，不可自行续命 |
| `AF_IDEMPOTENCY_KEY_REUSED` | 409 | 否 | 使用新 key 或原 payload |
| `AF_TRANSITION_INVALID` | 409 | 否 | 刷新 aggregate 状态 |
| `AF_SCOPE_VIOLATION` | 422 | 否 | 回退越界改动 |
| `AF_EVIDENCE_INVALID` | 422 | 否 | 重建完整证据 |
| `AF_HEAD_MISMATCH` | 409 | 否 | 新候选、新审查、新复现 |
| `AF_CANDIDATE_ARTIFACT_NOT_COMPLETE` | 409 | Lease 仍有效且缺块可补时 | 查询缺块并完成同一 Artifact；不得先登记 Candidate |
| `AF_VERIFICATION_STAGE_INVALID` | 409 | 读取 run 后 | 按权威 stage/version 推进，不得跳步或伪造未执行事实 |
| `AF_SIGNATURE_INVALID` | 422 | 否 | 拒绝制品并重新签名/注册节点 |
| `AF_BASE_MISSING` | 422 | 否 | 提供声明的 prerequisite/base |
| `AF_BUNDLE_INVALID` | 422 | 否 | 重建受限 Git Bundle |
| `AF_DESTINATION_CONFLICT` | 409 | 重新读取后 | 不 force push；生成冲突/Rebase 流程 |
| `AF_TARGET_MOVED` | 409 | 是 | 在最新目标基线上重新构造 Integration Commit |
| `AF_POLICY_DENIED` | 403 | 否 | 不得绕过；申请显式策略变更 |
| `AF_TASK_INVALID` | 422 | 否 | Boss 修订 AFWP；不处罚 Worker |
| `AF_BUDGET_EXHAUSTED` | 402 | 否 | 请求新授权 |
| `AF_FORBIDDEN` | 403 | 否 | 请求最小权限 |
| `AF_RATE_LIMITED` | 429 | 是 | 尊重 `Retry-After` |
| `AF_INTERNAL` | 500 | 是 | 指数退避并保持同 idempotency key |

## 13. 安全边界

- Worker 身份、Node 身份和 Executor 指纹是不同概念，Lease 绑定三者；
- jcode 运行环境不得直接获得控制面数据库、保护分支或长期 Git 凭据；
- Git push、Secret 注入、Artifact 上传和外部副作用由受策略约束的宿主 Broker 执行；
- AFWP 和仓库内容均视为不可信输入，仓库中的提示不得扩大权限或修改验收；
- Runner 默认无网络；需要网络的 criterion 必须在权限中列出域名并记录实际访问摘要；
- 节点签名只证明某节点提交了 manifest，不自动证明内容正确，仍需独立 Runner 和 Reviewer。

## 14. MVP 一致性测试

协议实现合并前必须通过以下故障矩阵：

| 场景 | 注入 | 预期不变量 |
| --- | --- | --- |
| 重复 publish | 同 key 发送 100 次 | 仅一个 revision 与一个 offered 事件 |
| revision 冲突 | 同 revision 不同 hash | 第二个被拒绝，原内容不变 |
| 双 claim | 两节点同时 claim | 只有一个 provisional/策略允许数量 |
| Lease 过期重派 | generation 3 过期，授予 4 | 3 的所有正式写入均失败 |
| 乱序 checkpoint | seq 12 后到 seq 11 | 投影不回退 |
| 命令提交后掉线 | DB commit 后响应丢失 | 重试返回原响应，不重复 outbox |
| stale submission | 旧节点恢复上传 | QUARANTINED，无 verify 事件 |
| 候选被 amend | review 后 OID 改变 | HEAD_MISMATCH，旧证据失效 |
| Runner 故障 | hard criterion 无结论 | INCONCLUSIVE，不得接受 |
| 目标分支前移 | acceptance 后出现新 commit | 构造集成 commit 并运行 L5 |
| Outbox 重投 | 同事件多次消费 | Inbox 去重，投影一致 |
| 服务重启 | 任意事务边界 kill | 从账本恢复，无“半个 Lease” |

## 15. 开发者最小验证命令

使用支持 draft 2020-12 的校验器。以 AJV CLI 为例：

```bash
ajv validate --spec=draft2020 \
  -s schemas/afwp.schema.json \
  -d examples/afwp-lease-fencing.json

ajv validate --spec=draft2020 \
  -s schemas/submission.schema.json \
  -d examples/submission-lease-fencing.json

ajv validate --spec=draft2020 \
  -s schemas/submission.schema.json \
  -d examples/submission-salvage-lease-fencing.json
```

Schema 校验只是第一层。仓库中的 `afwp lint` 还必须实现第 5、8、10、11 节的跨字段、哈希、状态与 Git 检查。

## 16. MVP 出口条件

AFWP/1.0 可宣布实现完成，必须同时满足：

- 两个 Schema 通过 metaschema 检查，仓库全部正例通过、全部反例按预期失败；
- Go/Rust/TypeScript 至少两种独立实现对示例算出相同 package hash；
- 需求覆盖、DAG、Lease 约束和 candidate-ready 语义 Linter 可运行；
- 故障矩阵自动化，重复、乱序、断线和 stale generation 不产生双重副作用；
- 陌生 Worker 只读 AFWP 能复述目标、修改边界、首个动作和每条 MUST 的验收方式。
