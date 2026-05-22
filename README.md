# harness-platform

每会话独立 **gVisor sandbox** 的数据分析 Agent 平台。Agent 运行在 sandbox 内，通过 stdio 与 Go orchestrator 通信，可查询远端 Apache Doris、生成文件 artifact，并由 Next.js 工作台展示会话、对话和产物。

当前底层运行时是 gVisor `runsc` 的 `systrap` 平台。宿主机不依赖 Docker daemon，直接消费 OCI bundle（`config.json` + `rootfs/`）。

## 当前状态

最新基线见 [docs/current-status.md](./docs/current-status.md)。

- Orchestrator 目前使用长生命周期容器：会话在消息之间保持运行。自动空闲 checkpoint 已暂时关闭，直到 Phase 7 的 checkpoint-safe 控制面落地。
- 多轮输出路由已通过 per-container `OutputHub` 修复；每次用户 turn 都会订阅当前容器输出。
- 浏览器事件流走 frontend same-origin SSE：`GET /api/events/stream`，orchestrator 仍保留 WebSocket 兼容端点 `/api/events`。
- 前端会在发送消息后轮询 messages/session/artifacts，作为 SSE 断线时的状态补偿。
- Claude Code 是当前主路径；`Shell` 是 PTY-backed 的交互式命令会话，`Agent` 直接映射到 Claude Code。
- 本地 Claude proxy 配置显式写在 `config/harness.yaml`：宿主机监听 `http://0.0.0.0:8082`，sandbox 访问 `http://10.200.1.1:8082`，key 为 `123`。

## 文档

### 当前设计

- [docs/current-status.md](./docs/current-status.md) - 当前可用能力、近期提交影响和约束
- [docs/architecture.md](./docs/architecture.md) - 当前架构、状态机、事件流和运行时设计
- [docs/checkpoint-safe-control-plane-architecture.md](./docs/checkpoint-safe-control-plane-architecture.md) - checkpoint-safe 控制面与 Phase 7 重构目标
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
