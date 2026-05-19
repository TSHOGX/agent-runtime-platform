# harness-platform

每会话独立 Firecracker microVM 的数据分析 Agent 平台。Agent（Claude Code / OpenCode）跑在 VM 内，通过只读账号查询远端 Apache Doris，前端类 Claude.ai 三栏布局（会话列表 / 聊天 / artifact）。

## 文档

- [docs/PLAN.md](./docs/PLAN.md) — 总体技术方案 & 分阶段实施计划
- [docs/llm-harness-local-test.md](./docs/llm-harness-local-test.md) — 本机 Claude Code / OpenCode 通过 proxy 的连通性测试
- `docs/doris-connection.md` — Doris 连接凭据（**不入 git**，本机本地查看）

## 当前状态

项目刚立项，已完成本机 LLM harness 最小连通性验证：Claude Code Proxy、Claude Code、OpenCode 均可通过 Anthropic-compatible 方式返回 `hi`。下一步继续推进 **Phase 0**：EECS 环境检查、Doris 连通性自测、目标表 schema 整理。

## 计划仓库结构

```
harness-platform/
├── docs/               # 设计文档、运行手册、Doris schema
├── orchestrator/       # Go：VM 生命周期、warm pool、vsock、会话 API（Phase 3）
├── frontend/           # Next.js：三栏 UI（Phase 4）
├── vm-image/           # rootfs 构建脚本、init / uploader / stdio-bridge（Phase 1–2）
├── snapshot/           # bake / restore 脚本（Phase 2）
├── schema-pack/        # Doris 表 schema markdown（Phase 0）
└── infra/              # iptables、systemd unit、运行手册（Phase 6）
```
