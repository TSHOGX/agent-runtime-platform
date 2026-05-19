# Phase 4 Frontend Status

> Last updated: 2026-05-20 02:12 Asia/Shanghai
> Scope guard: only frontend Phase 4 files and this status file were changed in this round.

## Round 1/8

### 本轮完成内容

- 创建 `frontend/` Next.js + TypeScript 项目脚手架。
- 添加三栏工作台静态 MVP：
  - 左栏：session 列表、创建 session、状态、agent 选择。
  - 中栏：一次任务输入、运行状态、`agent.output` 输出区。
  - 右栏：artifact 文件名、大小、更新时间和下载入口占位。
- 添加莫兰迪低饱和视觉样式，避免营销 hero 和重 UI 框架。
- 添加 `frontend/README.md`，记录安装、启动、检查命令和后端默认目标。
- 添加 `frontend/.gitignore`，避免 `.next/`、`node_modules/`、`*.tsbuildinfo` 进入提交。

### 测试结果

- `cd frontend && npm install`：通过。
- `npm run lint`：通过。
- `npm run typecheck`：通过。
- `npm run build`：通过。
- `PORT=8000 npm run dev`：通过，Next dev server 在 `http://127.0.0.1:8000` 返回 200，测试后已停止。
- `curl -I --max-time 5 http://127.0.0.1:8000`：返回 HTTP 200。
- `npm audit --omit=dev`：仍报告 Next 16.2.6 依赖 `postcss@8.4.31` 的 moderate advisory；`npm audit fix --force` 会降级到旧 Next，不在本轮执行。生产依赖已从最初 Next 14 的 high advisory 降到该上游链路残留。

### Real backend 是否可用

- 本机真实 orchestrator 可用：
  - `GET http://127.0.0.1:8090/healthz` 返回 200。
  - `GET http://127.0.0.1:8090/api/sessions` 返回真实 session 列表。
- 本轮只做前端脚手架，尚未实现 same-origin proxy、真实 HTTP client 或 WebSocket，因此前端尚未真正消费真实后端。

### mock fallback 是否验证

- 未验证。
- 本轮尚未实现 mock/dev fallback；下一轮应先补 same-origin proxy 和前端 API client，再实现健康检查失败时的 mock fallback。

### 已创建 commit hash

- `242972f` — `frontend: scaffold phase 4 app`

### 下一步

- 第 2 轮：实现 frontend same-origin route handler/proxy，支持 `HARNESS_API_BASE_URL` / `ORCHESTRATOR_URL` 指向默认 `http://127.0.0.1:8090`。
- 加入真实后端健康检查和 UI 模式状态，为后续 WebSocket 与 mock fallback 做接口层铺垫。

## 工作区注意事项

- 本轮开始前已有未跟踪项，未纳入提交：
  - `.codex-loop/phase4-frontend-prompt.md`
  - `orchestrator/orchestrator`
  - `scripts/phase4-frontend-ralph-loop.sh`
