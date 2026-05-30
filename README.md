# Agent Runtime Platform

Control plane for long-lived, sandboxed AI agent sessions.

这个项目把每个 AI agent 会话运行在独立的 gVisor sandbox runtime 中，并由 Go orchestrator 管理会话、turn、runtime generation、事件流和 artifact。前端 workbench 负责会话操作、实时输出和文件产物浏览。

## 定位

本项目的核心定位是 **Agent Runtime Platform**：

- **Control Plane**: 管理 session/turn 生命周期、runtime generation、Agent Bridge、事件、artifact metadata、proxy correlation、quota 和 retention。
- **Sandbox Runtime**: 使用 gVisor `runsc` 启动隔离 sandbox，提供 rootfs、mount、network namespace、agent process、checkpoint/restore 等执行能力。
- **Workbench**: 使用 Next.js 提供浏览器界面，展示会话、实时输出和 artifact。

## 技术栈

- **Backend**: Go orchestrator, SQLite, HTTP API, SSE/WebSocket event endpoints
- **Frontend**: Next.js, TypeScript, same-origin API proxy, live artifact browser
- **Runtime**: gVisor `runsc`, OCI bundle/rootfs, per-generation sandbox resources
- **Agent paths**: Pi, Claude Code, and PTY-backed shell; deployment config selects the product `Agent` default
- **Control protocol**: Agent Bridge file-queue claim/ack protocol
- **Artifacts**: host-side metadata watcher with safe read-only downloads/previews

## 快速启动

仓库默认 lab 配置中，Workbench 的 `Agent` 模式解析到 Pi，`Shell` 也启用。启动前先让 active rootfs 的 `/etc/harness-image/agents.json` 覆盖这两个 product modes：

```bash
SANDBOX_AGENT_DRIVERS=pi,sh ./sandbox-image/build-rootfs.sh
```

如果已有 rootfs 缺少这些 CLI，用同一 driver set 加 `FORCE=1` 全量重建；非 `FORCE` 路径只同步 overlay 并重新生成 manifest。

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

健康检查：

```bash
curl -sS http://127.0.0.1:8090/healthz
curl -sS http://127.0.0.1:8000/api/healthz
curl -sS http://127.0.0.1:8090/api/deployment-capabilities
```

## 目录结构

```text
agent-runtime-platform/
├── config/             # runtime / proxy / lab 配置
├── docs/               # 架构、路线图、阶段记录和设计文档
├── orchestrator/       # Go control plane
├── frontend/           # Next.js workbench
├── sandbox-image/      # rootfs 构建脚本、OCI 模板和 sandbox entrypoint
├── bundle/             # runsc bundle/checkpoint 辅助脚本
└── schema-pack/        # sandbox 可挂载的 schema/documentation pack
```
