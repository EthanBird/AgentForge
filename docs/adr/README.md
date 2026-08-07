# Architecture Decision Records

| ADR | 决定 | 状态 |
| --- | --- | --- |
| [ADR-0001](ADR-0001-MODULAR-MONOLITH.md) | MVP 使用 Rust 模块化单体 | Accepted |
| [ADR-0002](ADR-0002-POSTGRES-SOURCE-OF-TRUTH.md) | PostgreSQL 是业务事实来源 | Accepted |
| [ADR-0003](ADR-0003-A2A-BOUNDARY.md) | A2A 是边界协议，AFWP 是内部执行契约 | Accepted |
| [ADR-0004](ADR-0004-JCODE-SIDECAR.md) | jcode 通过受监督的 TypeScript sidecar 接入 | Accepted |
| [ADR-0005](ADR-0005-GIT-RELAY.md) | 跨网络提交采用受验证的 Git Relay | Accepted |

ADR 一经接受不得被普通任务包隐式修改。改变决定必须新增 ADR，并在 `Supersedes` 中引用被替代记录。
