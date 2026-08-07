# ADR-0005：使用 Pull-based Git Relay 将外网候选写入局域网 Git

- 状态：Accepted
- 日期：2026-08-07
- 决策者：AgentForge 架构组
- 影响范围：Git Bundle、对象存储、LAN Relay、任务分支、Merge Queue
- 详细规范：[06_VERIFICATION_GIT_RELAY.md](../development/06_VERIFICATION_GIT_RELAY.md)

---

## 1. 背景

AgentForge 的 Worker 可能位于不同电脑、网络和 NAT 后，但最终代码必须进入局域网中的 Forgejo/Gitea。直接让所有 Worker 访问 LAN Git 会带来：

- 需要暴露局域网入口、VPN 管理或长生命周期 SSH 凭据；
- 被攻陷 Worker 可扫描/攻击 LAN；
- 断网和低带宽下直接 Git push 的恢复/审计较弱；
- 很难在 Git 写入前统一核验 Lease、Evidence、候选 Commit 和允许 ref；
- 每个 Worker 的 Git 凭据生命周期和吊销面过大。

同时，中央悬赏服务器不应成为 LAN Git 的永久替代品。中央服务可以暂存内容寻址的候选对象，但正式仓库事实和保护分支仍在局域网。

需要一种兼容以下条件的交付方式：

- Worker 和 LAN 都只需出站连接；
- 候选能离线/断点传输；
- Relay 写入前可独立验证来源与 Git 对象；
- 重复上传/投递/ACK 丢失不会生成重复分支；
- Relay 不能合并保护分支；
- 低带宽下只传相对 base 的增量对象。

---

## 2. 决策驱动因素

1. 不把 LAN Git 暴露到公网。
2. 外网 Worker 不持有 LAN Git 凭据和网络可达性。
3. 被测试、审查、传输和落库的 Candidate OID 必须相同。
4. 所有写入使用确定目标 ref、CAS 和幂等业务 ID。
5. 网络中断、Relay 重启和 ACK 丢失可恢复。
6. 使用 Git 原生对象格式，不自造 patch 应用语义。
7. MVP 能在一台中央服务器 + 一台 LAN Relay 上实现。
8. 保留受信网络下的直接私网 push 作为可选优化。
9. Git 对象必须在终态 Submission 创建之前就能被独立 Runner 读取，且不能绕过作者 Lease fencing。

---

## 3. 备选方案

### 方案 A：所有 Worker 加入 WireGuard/Tailscale 并直接 push

优点：简单、Git 原生、延迟低。

缺点：

- Worker 获得 LAN 可达性，攻击面扩大；
- 需要为每个节点管理网络身份、路由和 Git 凭据；
- 外部/临时 Worker 的准入和吊销成本高；
- 在 Git 服务端 hook 之前很难统一执行完整 Evidence/Lease 校验。

结论：仅允许受信自有节点作为可选快速路径；不作为通用兼容路径。

### 方案 B：LAN Git 暴露公网入口

优点：Worker 直接 push，组件最少。

缺点：LAN 服务直接进入公网威胁面，凭据和防护要求最高，不符合最小暴露原则。

结论：拒绝。

### 方案 C：中央服务器托管完整 Git mirror，再双向同步 LAN

优点：外网 Worker 接入方便；中央可以运行常规 Git 流程。

缺点：形成第二权威仓库；双向 ref 冲突、权限、删除和保护分支语义复杂；中央泄露等同完整仓库泄露；低带宽持续同步成本高。

结论：不作为 MVP。中央只暂存候选 Bundle，不托管权威 refs。

### 方案 D：Worker 上传 patch/zip，LAN 重新应用

优点：实现表面简单。

缺点：重新应用后 Commit/tree 不再等于被测试对象；文件 mode、rename、submodule、二进制和 Git 属性容易丢失；来源链断裂。

结论：拒绝。

### 方案 E：Pull-based Relay + 签名增量 Git Bundle

优点：

- Git 原生对象保持 Commit/tree 精确一致；
- Worker 与 Relay 均只出站；
- Bundle 可按 base 增量、分块、断点和内容寻址；
- Relay 可在隔离区完成签名、digest、`git fsck`、scope 和 ref 验证；
- LAN Git 凭据只存在 Relay，且只允许任务分支。

缺点：

- 需要对象存储、Ticket、Relay 本地 Journal 和 GC；
- 增量 Bundle 要求 Relay 已有 prerequisite base；
- 中央临时保存私有 Git 对象，需要加密、ACL 和保留策略；
- Relay 增加一次传输和验证延迟。

结论：选用。

---

## 4. 决策

AgentForge 使用方案 E 作为通用 Git 交付路径：

1. Author Worker 在本地封存不可变 Candidate Commit；所有验收和 Review 绑定该 OID。
2. Worker 在有效作者 Lease 下调用 InitCandidateArtifact；控制面预留 `candidate_id + artifact_id`，并生成唯一 ref `refs/agentforge/candidates/{candidate_id}`。
3. Worker 生成相对 `base_commit` 的签名增量 Git Bundle，分块上传并 Complete CandidateArtifact；Init、chunk、complete 先验证 author identity/capability 并查询幂等回执，receipt miss 的新副作用再验证当前 fencing 和 CAS。
4. Worker 调用 `RecordCandidate`；控制面在一个事务中绑定 `COMPLETE` Artifact、创建不可变 Candidate 与 VerificationRun，并关闭作者 Lease。Submission 此时尚不存在。
5. 独立服务按 `PROVENANCE_CHECK -> REVIEWING -> REPRODUCING` 推进 VerificationRun；它们使用 service identity、capability、Job Lease 与 run CAS，不持有作者 fencing token。
6. Coordinator 在 run 终结后一次性创建签名、stage-aware 的终态 Submission。VerificationRun 的最后执行阶段是 `REPRODUCING`；成功终结时 candidate Submission 必须写入 `terminal_outcome=PASS`、`completed_stage=candidate_ready`。早期 FAIL/INCONCLUSIVE 使用 Failure Dossier，只记录实际终结的 `provenance_check/reviewing/reproducing` 阶段，未执行的 Head/结果不得伪造。
7. 只有 PASS Submission 才能签发短期 Relay Ticket；Ticket 同时绑定 `submission_id/candidate_id/verification_run_id/candidate_artifact_id` 及全部 digest。重签使用单调 `ticket_version` 和 `supersedes_ticket_id`，旧票在同一事务变为 `SUPERSEDED`。
8. LAN 内 `git-relay` 主动通过 mTLS 拉取 Ticket 和 Artifact；不监听公网端口。每次外部写先领取绑定 `ticket_id + ticket_version` 的 queue claim lease，持久化 generation/token hash/服务器 expiry，并在 push/result 前重新 fencing。
9. Relay 先在 quarantine object store 验证 Ticket、Submission、Evidence、Bundle、Git 对象、base、tree、scope 和目标 ref。
10. Relay 仅以 CAS 方式把 Candidate 推到唯一任务分支：

```text
refs/heads/task/{package_id}/attempt/{attempt_id}
```

11. Relay 没有 `main/master/release/tag` 写权限，也不负责合并。
12. Merge Queue 从 LAN Git 读取任务分支，在最新目标分支上构造新的 synthetic merge Commit，运行 L5 测试并以 expected old OID 合并。
13. 中央临时 Bundle 在 Relay 签名 ACK 且保留窗口结束后 GC；Submission/Evidence 摘要按审计策略保留。

对 `PASS/candidate-ready` 及其 Relay，OID 关系固定为：

```text
PASS: TestedHead = ReviewedHead = SubmittedHead = RelayedHead = CandidateHead
IntegrationHead = merge(latest TargetBaseline, CandidateHead)
L5TestedHead = IntegrationHead = TargetAfter
```

`FAIL/INCONCLUSIVE` 按 `completed_stage` 可以缺少尚未执行的 Tested/Reviewed/Relayed Head，禁止为套用 PASS 等式而伪造值。`IntegrationHead` 通常不等于 `CandidateHead`，这是合并父系变化的必然结果；不得为了追求相同 SHA 而 amend/重写 Candidate。

---

## 5. 协议摘要

### 5.1 Git Bundle

```text
base_commit:      Relay 必须已拥有的 prerequisite
candidate_commit: 被测试/审查的精确 OID
candidate_id:     Candidate 的预留稳定身份
artifact_id:      COMPLETE CandidateArtifact 身份
head_ref:         refs/agentforge/candidates/{candidate_id}
bundle_digest:    SHA-256
```

Relay 使用 `git bundle verify`、`git bundle list-heads` 和隔离 object store 的 `git fsck --strict`。Bundle 自校验不替代 AgentForge Manifest 签名和策略。

增量 Bundle 只在受信 LAN mirror 已证明拥有 prerequisite base 时生成。已 `COMPLETE` 的 Artifact 不可因 `AF_BASE_MISSING` 被扩大或替换：优先由管理员静态配置的受信 mirror/内部快照同步精确 base OID；若无受信来源，创建 ReworkPackage 与新 Attempt/Candidate lineage，自始生成自包含 Bundle。

### 5.2 Relay Ticket

Ticket 精确绑定：

```text
relay_id
repo_id（映射到 Relay 本地静态 remote）
ticket_id / ticket_version / supersedes_ticket_id
submission/candidate/verification_run/candidate_artifact
package/attempt
lease generation
base/candidate/tree
bundle/evidence digest 和 URI
唯一 destination ref + expected old OID
issued/expiry
control signing key
```

Ticket 不包含可执行命令，不允许提供任意 Git URL；Relay 根据本地管理员配置把 `repo_id` 映射到 LAN Git。`ticket_version` 在 `(submission_id, relay_id)` 内连续递增；同一 Submission 可以保留多个历史版本，但本地 partial unique 约束最多一个非终态 live 版本。已 `RELAYED` 的票不得重签。

### 5.3 幂等键

| 操作 | 业务键 |
| --- | --- |
| Init CandidateArtifact | `attempt_id + candidate_oid + author_evidence_digest` |
| 上传 chunk | `artifact_id + index + chunk_digest` |
| Complete CandidateArtifact | `artifact_id + bundle_digest` |
| RecordCandidate | `candidate_id + artifact_manifest_digest` |
| Finalize Verification | `verification_run_id + terminal_fact_digest` |
| 签发 Ticket | `submission_id + candidate_id + verification_run_id + candidate_artifact_id + relay_id + ticket_version` |
| Relay `started` | `ticket_id + ticket_version + relay_instance_id + claim_request_id`；未知结果复用 request ID，确认旧 claim 过期后才生成新 ID |
| Relay `result` | `ticket_id + ticket_version + claim_generation + terminal_result_digest` |
| 推送任务 ref | `destination_ref + expected_old_oid + candidate_oid` |
| Merge 入队 | `submission_id + target_ref` |

重复请求在回放窗口内返回首次业务结果；只剩 tombstone 时返回 `AF_IDEMPOTENCY_RESULT_EXPIRED` 且不得重执行。若相同 key 对应不同 payload digest，立即拒绝并产生安全事件。Relay 本地表以 `(submission_id, relay_id, ticket_version)` 唯一保存历史，并以 `WHERE state IN (live states)` 的 partial unique index保证同一 Submission/Relay 最多一个 live Ticket；接收新票时先将其 `supersedes_ticket_id` 指向的旧票置 `SUPERSEDED`，再插入新版本。

---

## 6. Relay 验证与权限

Relay fail closed 的固定顺序：

1. mTLS Relay service identity 和 action/resource capability；
2. 对同 actor/key/request hash 的已提交请求只回放首次回执；receipt miss 才进入新副作用门禁；
3. 控制面 Ticket 签名、expiry、relay_id、`ticket_id/version`、supersede lineage，并在线确认它仍是最新未撤销版本；
4. 当前 queue claim 的 ID/token hash/holder/generation/服务器 expiry、绑定的 `ticket_id/version` 与 Relay Job CAS；
5. 本地 Ticket 版本与 digest 幂等记录；
6. Submission 在线状态仍为 `PASS` 且未撤销，签名四 ID 有效，且 `terminal_outcome=PASS`、`completed_stage=candidate_ready`；
7. Submission -> VerificationRun -> Candidate -> COMPLETE CandidateArtifact 外键链与 Ticket 四 ID 完全一致；
8. Worker、Runner、Reviewer、Coordinator 的 Evidence/终态签名与 digest；
9. `tested_head = reviewed_head = submitted_head = ticket candidate`，且 VerificationRun 在 `REPRODUCING` 后终结为 PASS；
10. Bundle chunk/总大小/SHA-256；
11. Bundle 只有 `refs/agentforge/candidates/{candidate_id}`；
12. quarantine 中严格 Git 对象检查；
13. LAN mirror 存在 base，candidate parent/tree 与声明一致；
14. scope、secret、高风险路径和 Commit trailer 复检；
15. destination ref 模式和本地 repo policy；
16. expected old OID CAS push；
17. 从 Git server fetch 后确认 ref 精确等于 candidate；
18. 在当前 claim/CAS 下持久化并签名 Relay Receipt，原子终结 Relay Job。

`/started` 必须先验证 Ticket 未过期，并令 `claim_expires_at = min(server_now + requested_ttl, ticket.expires_at)`；剩余窗口不足一次安全 push 时拒绝领取。在第 16 步前必须再次在线确认 Ticket 仍是最新未撤销版本且未过期、claim 未被更高 generation 或新 Ticket 版本 supersede 且未过期，并要求二者较早的剩余 TTL 大于 Git push 硬时限；Git 子进程 deadline 不得越过该较早 expiry。push 后提交结果的 receipt-miss 路径再次验证 Ticket expiry 与 claim。旧 generation 的迟到结果返回 `AF_OBLIGATION_CLAIM_STALE`。

Relay Git 服务账户权限：

```text
ALLOW  create/update refs/heads/task/*（仅 CAS，通常只允许 create）
DENY   refs/heads/main, master, release/*
DENY   refs/tags/*
DENY   delete/force push
DENY   repo admin, hooks, deploy keys
```

即使 Ticket/控制面出错，Git 服务端权限仍是第二道边界。

---

## 7. 失败与恢复语义

| 故障点 | 恢复决策 |
| --- | --- |
| Worker 上传中断 | 作者 Lease 仍有效时查询 CandidateArtifact missing chunks，只补缺失块 |
| 服务在 Artifact=`ASSEMBLING` 时崩溃 | 以冻结 chunk manifest、原 complete request hash 和 Artifact version CAS 重做确定性校验；Lease 仍有效才到 COMPLETE，过期则 EXPIRED/QUARANTINED |
| complete 响应丢失 | 同业务 key查询同一 `candidate_id/artifact_id` 的总 digest/URI |
| complete 后、RecordCandidate 前 Lease 失效 | Artifact 只可 salvage/GC；旧 Attempt 不得创建 Candidate/run |
| RecordCandidate 提交后响应丢失 | 以 candidate ID/原幂等键返回既有 Candidate/run，不重复关闭 Lease |
| Provenance/Review/Reproduction 早期失败 | 创建 stage-aware FAIL/INCONCLUSIVE Submission + Failure Dossier；未执行字段缺省，绝不签发 Ticket |
| Relay claim 超时/实例失联 | 新实例通过 `started` 获得更高 generation；旧 claim 不能 push/提交 result，恢复实例先查询任务 ref |
| Relay 下载中断/重启 | 从本地 verified chunk bitmap 续传 |
| Bundle 验证失败 | 标记 REJECTED，保留短期 quarantine 取证，不触碰 LAN ref |
| base 缺失 | 返回 `AF_BASE_MISSING`；先从管理员静态配置的受信 mirror/内部快照同步精确 OID；否则新建 ReworkPackage 与 Attempt/Candidate lineage 的自包含 Bundle，绝不扩大已 COMPLETE Artifact |
| Git push 成功但 ACK 丢失 | 重启后查询 destination；相同 OID 返回原 RELAYED |
| destination 已是其他 OID | `AF_DESTINATION_CONFLICT`，绝不 force-push |
| Ticket 过期 | 拒绝；若尚未 RELAYED，控制面原子 supersede 旧票并签发连续 `ticket_version` 的新票；旧 claim/result 失效 |
| Submission/Ticket 撤销 | 未 push 时停止；已 push 的任务分支隔离并禁止入 Merge Queue |
| Merge 测试时 target 移动 | 旧 Integration 以 `TARGET_MOVED` 终结，创建新 Integration 并复用已有 RELAYED Ticket/任务分支 Receipt；基于新 target 重新合成与测试，不重签 Ticket、不重复 push Candidate |
| 合并冲突 | 创建 RebasePackage，不修改旧 Candidate/Submission |

外部命令返回超时/断线时，Relay 必须查询 Git ref 判断真实结果；不能假设非零/无响应等于 push 未生效。

作者 fencing 只保护 CandidateArtifact init/upload/complete 与 `RecordCandidate` 等作者侧新写入；已认证 actor 对原 key/request hash 的已提交精确重放在回放窗口内返回首次回执、只剩 tombstone 时返回 `AF_IDEMPOTENCY_RESULT_EXPIRED`，即使作者 Lease 已关闭也不重新执行。Verification、Relay 和 Integration 发生在作者 Lease 关闭后，分别使用 service identity、最小 capability、Job/Queue Lease 与 aggregate/Git CAS；Relay queue claim 必须绑定 Ticket ID/version。它们验证 Candidate 中保存的作者 generation 来源，但不得要求当前作者 Lease。

Relay claim bearer 的幂等回放使用 KMS/AEAD 加密、TTL 不超过 claim expiry 的敏感 response envelope；普通领域表、command receipt、日志和 trace 只保存 hash/ref。Relay 本地同样只在 OS key store/加密 secret store 保存 bearer，SQLite 保存 token hash 与 secret ref。

---

## 8. 安全与隐私结果

### 安全收益

- LAN Git 无需公网入口，外部 Worker 无 LAN 路由。
- Worker 不持有 LAN Git credential；Relay key 权限窄且可独立吊销。
- Bundle 保留原 Git OID，避免 patch 重应用破坏验收绑定。
- Ticket 和本地 repo map 共同限制目标，恶意 Worker 不能选择 arbitrary remote/ref。
- quarantine 将不可信 Git parser/pack 操作与正式 mirror 分开。

### 代价与残余风险

- 中央对象存储短暂持有私有源码对象，必须项目 ACL、加密和短保留。
- Relay 主机被 root 完全攻陷时仍可能滥用其任务分支 key；Git 服务端保护限制影响但不能消除。
- Git Bundle/pack 解析仍可能受 Git 漏洞影响；Relay 需要及时更新并在资源沙箱中解析。
- 控制面 Ticket key 失陷可签发假 Ticket；Relay 在线复核 Submission、Git 服务端权限和多层签名降低影响。
- 增量 prerequisite 管理会增加协议复杂度。

---

## 9. 运维约束

- Relay 以专用非 root 用户运行，数据目录 `0700`，私钥在 OS key store/HSM 或严格权限文件。
- Relay 只允许出站到控制面、对象存储和静态配置的 LAN Git。
- Git 使用干净 HOME，禁用 system/repo hooks、credential helper、external diff/filter 和不需要的 transport。
- 下载、解包和 `git fsck` 有 CPU、内存、磁盘、对象数、delta depth 和 timeout 限制。
- Ticket/Receipt/错误是结构化审计事件；日志不得包含凭据或私有 blob。
- Bundle retention 默认在 RELAYED 后 24 小时删除；活跃 Ticket/调查引用阻止 GC。
- Relay 更新采用签名镜像、固定 digest 和 canary；升级前运行恶意 Bundle 测试集。
- 定期演练对象存储不可用、Git 不可用、key rotation、Relay 全盘丢失和 ACK 丢失恢复。

---

## 10. 结果与代价

### 正面结果

- 分布式 Worker 拓扑不再受 LAN 入站或统一 VPN 约束。
- Git 交付成为可验证、可重放、可审计的任务包工作流。
- 网络层的至少一次传输不会变成 Git 层的重复副作用。
- Relay、Review 和 Merge 三个权限域分开，单点失陷不直接等于保护分支被合并。
- 低带宽可用增量对象、chunk 去重和断点续传。

### 负面结果

- 比直接 push 增加对象存储、Ticket、Relay DB、quarantine 和 GC 组件。
- 候选通过验收到 LAN 分支可见之间有额外延迟。
- base 缺失、仓库重写或超大二进制历史会降低增量效率。
- 运维需要同时观察 CandidateArtifact、Candidate、VerificationRun、Submission、Ticket、Relay 和 Integration 状态。

复杂性被接受，因为它集中在一个窄 Relay 边界，换取 LAN 不暴露和精确候选来源；不应把复杂性重新泄漏给每个 Worker。

---

## 11. 验证标准

本 ADR 实施完成必须证明：

1. 外网 Worker 没有 LAN 路由和 Git credential，仍可在有效 Lease 下 init/upload/complete CandidateArtifact，并让独立 Runner 解析精确 Candidate。
2. `RecordCandidate` 只接受 COMPLETE Artifact，在同一事务创建 Candidate/run 并关闭 Lease；任一点丢响应后重试仍只有一条链。
3. Lease 在 complete 后、RecordCandidate 前失效时，旧 Artifact 只能 salvage/GC，不能创建正式 Candidate。
4. VerificationRequest、Review/Evidence 全程使用 candidate/run/artifact ID，不依赖尚未存在的 submission ID。
5. Provenance 在 Review/Reproduction 前 FAIL/INCONCLUSIVE 时能形成终态 Submission + Failure Dossier，且没有伪造 tested/reviewed Head、不能签发 Ticket。
6. Bundle、Evidence、Submission 或 Ticket 任一字节变化都会在 push 前失败。
7. PASS Ticket 同时绑定 submission/candidate/run/artifact 四 ID；任一外键或 digest 错配都被拒绝。
8. 同一 Ticket 版本投递 10 次，LAN 只有一个 ref 且 OID 一致。
9. Relay 在下载、验证、push 后 ACK 前分别 SIGKILL，均能用持久 Ticket/claim 状态恢复到正确结果。
10. push 成功但响应丢失时，Relay 通过 ref 查询恢复，不 force-push。
11. Ticket 中的任意 URL、保护 ref 或非法 ref 都被本地策略拒绝。
12. Bundle 缺 base 返回明确 `AF_BASE_MISSING`，不会从不可信公网自动拉取，也不会修改已 COMPLETE Artifact；无受信 base 时通过新 Attempt/Candidate lineage 产生自包含 Bundle。
13. Merge Queue 只合并在最新 target 上测试的 synthetic Commit；target race 触发新 Integration 重测并复用同一 RELAYED Ticket，不产生第二次 Candidate push。
14. 合并冲突产生新 RebasePackage，旧 Candidate/Submission hash 不变。
15. Git fetch 后可验证 `RelayedHead = SubmittedHead`，Integration Receipt 可验证 `TestedSyntheticHead = TargetAfter`。
16. Relay claim 到期并被更高 generation 重领后，旧实例的 push/result proof 被拒；新实例对账后至多形成一个业务结果。
17. 未 RELAYED Ticket 过期重签时，新票版本连续、`supersedes_ticket_id` 正确、旧票/claim 失效，四 ID 与 Artifact/digest 均不改变。
18. `candidate_ready` 显式比较签名 Submission、Submission row 与 VerificationRun row 的 Evidence digest，并比较 run 与签名对象的 tested/reviewed Head；任一错配不能签发 Ticket。

---

## 12. 重新评估条件

出现以下情况时重新评估，但不自动撤销本决策：

- 所有 Worker 都进入同一受控私网且节点数量很小；
- LAN Git 提供等价的细粒度短期 OIDC/ref capability 与强来源验证；
- Bundle 传输成为主要性能瓶颈；
- 需要多 LAN/多 Git server 的跨站点复制；
- Git SHA-256 仓库迁移或新的对象传输协议成熟；
- 对象存储不能承载项目的数据驻留要求。

受信私网下可新增“直接 Git Broker push”优化，但必须产出等价的 COMPLETE CandidateArtifact Receipt，并继续满足 Candidate/Evidence/fencing/ref/CAS 不变量；不得因直连而跳过 Candidate-first 顺序。Relay 路径仍供 NAT/外部节点使用。

---

## 13. 参考

- [Git bundle 官方文档](https://git-scm.com/docs/git-bundle)
- [验收与 Git Relay 开发规范](../development/06_VERIFICATION_GIT_RELAY.md)
- [安全威胁模型](../development/07_SECURITY_THREAT_MODEL.md)
