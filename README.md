# harness-platform

每会话独立 **gVisor sandbox** 的数据分析 Agent 平台。Agent 运行在 sandbox 内，通过 Agent Bridge 与 Go orchestrator 通信，可查询远端 Apache Doris、生成文件 artifact，并由 Next.js 工作台展示会话、对话和产物。

当前底层运行时是 gVisor `runsc` 的 `systrap` 平台。宿主机不依赖 Docker daemon，直接消费 OCI bundle（`config.json` + `rootfs/`）。

## 当前状态

最新基线见 [docs/current-status.md](./docs/current-status.md)。

- Orchestrator 使用长生命周期 generation：会话在消息之间保持运行。自动空闲 checkpoint 由策略关闭，checkpoint-safe 控制面已落地。
- Phase 8 `sandbox-isolation-v1` 已完成：精确 `/workspace`/`/agent-home` 绑定、无父目录挂载、无 `/harness-secrets`、只读 rootfs、非 root agent、模型凭证留在宿主/proxy 侧。
- 多轮输出路由走 Agent Bridge 和 per-container `OutputHub`；每次用户 turn 都会订阅当前容器输出。
- 浏览器事件流走 frontend same-origin SSE：`GET /api/events/stream`，orchestrator 仍保留 WebSocket 兼容端点 `/api/events`。
- 前端会在发送消息后轮询 messages/session/artifacts，作为 SSE 断线时的状态补偿。
- Claude Code 是当前主路径；`Shell` 是 PTY-backed 的交互式命令会话，`Agent` 直接映射到 Claude Code。
- Artifact 浏览已完成 Phase 6：右栏是实时文件树，支持搜索、下载和 Markdown/code/text/image/JSON/CSV/TSV/PDF 预览；下载路径会拒绝 traversal、symlink escape 和非 regular file。
- `config/harness.yaml` 已迁到 Phase 7 `harness:` schema；现有热路径从每个 generation 的 network profile 派生 sandbox-visible proxy URL，宿主机 proxy 默认监听 `http://0.0.0.0:8082`，本地 key 为 `123`。

## 文档

### 当前设计

- [docs/current-status.md](./docs/current-status.md) - 当前基线、资格证据和约束
- [docs/architecture.md](./docs/architecture.md) - 当前架构、状态机、事件流和运行时边界
- [docs/phase7/](./docs/phase7/README.md) - checkpoint-safe 控制面与 Phase 7 重构目标（按主题拆分）
- [docs/PLAN.md](./docs/PLAN.md) - 总体路线图和后续阶段

### 技术决策与研究

- [docs/gvisor-decision.md](./docs/gvisor-decision.md) - 运行时选型决策记录
- [docs/runsc-warm-sentry-research.md](./docs/runsc-warm-sentry-research.md) - runsc 与 warm sentry 调研
- [docs/llm-harness-local-test.md](./docs/llm-harness-local-test.md) - LLM harness 连通性测试

### 阶段记录

- [docs/phase1-status.md](./docs/phase1-status.md) - Phase 1 手工 sandbox 验证
- [docs/phase2-status.md](./docs/phase2-status.md) - Phase 2 rootfs / bundle / restore 脚本
- [docs/phase3-status.md](./docs/phase3-status.md) - Phase 3 Go orchestrator MVP
- [docs/phase4-status.md](./docs/phase4-status.md) - Phase 4 前端 MVP

### 数据与敏感信息

- [schema-pack/doris-schema.md](./schema-pack/doris-schema.md) - Doris schema 整理稿
- `docs/doris-connection.md` - Doris 连接凭据，本地文件，不应提交到 git

## 快速启动

Orchestrator:

```bash
cd orchestrator
go run ./cmd/orchestrator
```

Frontend:

```bash
cd frontend
npm install
PORT=8000 npm run dev
```

默认访问：

- Frontend: <http://127.0.0.1:8000>
- Orchestrator: <http://127.0.0.1:8090>

## 仓库结构

```text
harness-platform/
├── config/             # 显式 lab runtime / Claude proxy 配置
├── docs/               # 当前设计、路线图、阶段记录和研究文档
├── orchestrator/       # Go：会话 API、runtime、事件流、artifact watcher
├── frontend/           # Next.js：三栏工作台和 same-origin proxy
├── sandbox-image/      # rootfs 构建脚本、OCI config.json 模板、entrypoint
├── bundle/             # runsc bundle/checkpoint 辅助脚本
└── schema-pack/        # Doris 表 schema markdown
```
