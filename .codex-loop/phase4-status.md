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

## Round 2/8

> Last updated: 2026-05-20 02:56 Asia/Shanghai
> Scope guard: only `frontend/` and `.codex-loop/phase4-status.md` were changed in this round.

### 本轮完成内容

- 添加 frontend same-origin proxy：
  - `/api/healthz` 转发到 orchestrator `/healthz`。
  - `/api/*` 转发到 orchestrator `/api/*`。
  - `/artifacts/:session_id/:path` 转发到 orchestrator artifact download。
- 支持 `HARNESS_API_BASE_URL` / `ORCHESTRATOR_URL` 覆盖服务端 proxy 目标，默认 `http://127.0.0.1:8090`。
- 首页从静态示例切换为 client workbench：
  - 启动先请求 `/api/healthz`，成功后读取真实 `/api/sessions`。
  - 选择 session 后读取真实 `/api/sessions/:id/artifacts`。
  - UI 明确显示 `Real backend` / `Mock fallback` / `Checking`。
  - 提供 `Retry real` 手动重试真实后端连接。
- 加入 mock fallback 数据层：仅在健康检查或 proxied HTTP 请求失败、或真实后端 5xx/不可达时进入。
- 更新 `frontend/README.md`，记录 proxy routes 和环境变量。

### 测试结果

- `cd frontend && npm install`：通过；依赖未变化。
- `npm run lint`：通过。
- `npm run typecheck`：通过。
- `npm run build`：通过。
- `PORT=8000 npm run dev`：通过，测试后已停止。
- Real proxy smoke：
  - `curl -i --max-time 5 http://127.0.0.1:8000/api/healthz` 返回 HTTP 200 和 `{"status":"ok"}`。
  - `curl --max-time 5 http://127.0.0.1:8000/api/sessions` 返回真实 Phase 3 session 列表。
  - `curl --max-time 5 http://127.0.0.1:8000/api/sessions/sess_yOGYOjI06VzfrE78/artifacts` 返回真实 artifact 元数据。
  - `curl -I --max-time 5 http://127.0.0.1:8000/` 返回 HTTP 200。
- Mock fallback smoke：
  - 使用 `ORCHESTRATOR_URL=http://127.0.0.1:18090 PORT=8000 npm run dev` 启动前端。
  - `curl -i --max-time 5 http://127.0.0.1:8000/api/healthz` 返回 HTTP 503，body 包含 `{"error":"fetch failed","upstream":"http://127.0.0.1:18090"}`。
  - 页面入口仍返回 200，客户端会根据 health 失败进入 `Mock fallback`。
- 未做浏览器截图：当前环境没有 Chromium/Playwright 依赖，使用 HTTP smoke 代替实际截图。
- `npm install` 仍报告 2 个 moderate vulnerabilities；`npm audit fix --force` 需要破坏性升级/降级路径，本轮未执行。

### Real backend 是否可用

- 可用。
- `GET http://127.0.0.1:8090/healthz` 返回 200。
- 通过 frontend same-origin proxy 读取到了真实 `/api/sessions` 和 `/api/sessions/:id/artifacts`。

### mock fallback 是否验证

- 已验证 HTTP 不可达 fallback 条件。
- 本轮尚未实现 WebSocket 连接失败后的 mock stream fallback；留到 WebSocket 小阶段处理。

### 已创建 commit hash

- `d928655` — `frontend: add real backend client`

### 下一步

- 第 3 轮：实现创建 session 与发送首条 task 的真实 HTTP client，保持一次任务运行语义。
- 之后再接入 WebSocket `agent.output` 流和 WebSocket 失败后的 mock stream fallback。
