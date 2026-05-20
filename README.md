# harness-platform

每会话独立 **gVisor sandbox** 的数据分析 Agent 平台。Agent（Claude Code / OpenCode）跑在 sandbox 内，通过只读账号查询远端 Apache Doris，前端类 Claude.ai 三栏布局（会话列表 / 聊天 / artifact）。

底层运行时是 [gVisor `runsc`](https://gvisor.dev/) 的 `systrap` 平台（用户态 syscall 拦截，不依赖 KVM / 嵌套虚拟化），host 上**不跑 Docker**，直接消费 OCI bundle（`config.json` + `rootfs/`）。

## 文档

### 核心文档
- [docs/architecture.md](./docs/architecture.md) — 当前架构设计与技术决策
- [docs/PLAN.md](./docs/PLAN.md) — 总体技术方案 & 分阶段实施计划

### 技术决策与研究
- [docs/gvisor-decision.md](./docs/gvisor-decision.md) — 运行时选型决策记录
- [docs/runsc-warm-sentry-research.md](./docs/runsc-warm-sentry-research.md) — runsc 与 warm sentry 调研
- [docs/llm-harness-local-test.md](./docs/llm-harness-local-test.md) — LLM harness 连通性测试

### 开发历史
- [docs/phase1-status.md](./docs/phase1-status.md) — Phase 1 手工 sandbox 验证
- [docs/phase2-status.md](./docs/phase2-status.md) — Phase 2 rootfs / bundle / restore 脚本
- [docs/phase3-status.md](./docs/phase3-status.md) — Phase 3 Go orchestrator MVP
- [docs/phase4-status.md](./docs/phase4-status.md) — Phase 4 前端开发历史
- [docs/phase4-frontend-prompt.md](./docs/phase4-frontend-prompt.md) — Phase 4 前端开发指南

### 敏感信息
- `docs/doris-connection.md` — Doris 连接凭据（**不入 git**）

## 当前状态

**Phase 3 已完成**：长生命周期容器架构已实现，支持：
- 容器在消息之间保持运行（热路径 ~10ms 延迟）
- Idle 30 分钟后自动 checkpoint 并释放资源
- 从 checkpoint 恢复会话上下文（~200ms）
- WebSocket 实时事件流
- SQLite 会话和消息持久化

**Phase 4 已完成**：前端 MVP 已实现：
- 三栏布局（会话列表 / 对话 / Artifacts）
- WebSocket 流式输出
- Real backend / Mock fallback 自动切换
- Artifact 预览和下载

**已知问题**：
- 多轮对话输出流路由问题（第二条消息后输出无法到达前端）
- 需要实现 per-container Event Hub 架构来解决

详见 [docs/architecture.md](./docs/architecture.md)。

## 仓库结构

```
harness-platform/
├── docs/               # 设计文档、决策记录、Doris schema
├── orchestrator/       # Go：sandbox 生命周期、warm pool、stdio 桥、会话 API（Phase 3）
├── frontend/           # Next.js：三栏 UI（Phase 4）
├── sandbox-image/      # rootfs 构建脚本、OCI config.json 模板（Phase 1–2）
├── bundle/             # runsc checkpoint/restore 脚本（Phase 2）
├── schema-pack/        # Doris 表 schema markdown（Phase 0）
└── infra/              # nftables、systemd unit、运行手册（Phase 6）
```
