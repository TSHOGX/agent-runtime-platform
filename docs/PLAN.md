# gVisor Data Agent Harness — 技术方案

> 这份是项目主计划，后续在不同 session 中按 Phase 推进。
> 凭据等敏感信息见 [`doris-connection.md`](./doris-connection.md)（不要提交到 git）。
> 选型决策与放弃 Firecracker 的原因见 [`gvisor-decision.md`](./gvisor-decision.md)。

## 待办（Phases）

- [x] **Phase 0**：本机 LLM harness 连通性 + Doris 连通性 + `vhr_data` schema 整理；确认本机虚拟化能力（KVM 不可用，改走 gVisor `systrap` 平台）
- [x] **Phase 1**：手工建 rootfs 目录 + 单 sandbox（`runsc run`）跑通 Claude Code 查 Doris 出图的端到端 DEMO
- [x] **Phase 2**：写 `build-rootfs.sh` + `bake-bundle.sh` + `restore-sandbox.sh`，做到一键造 rootfs、一键起 sandbox（标准 checkpoint/restore 已跑通；官方 `runsc` 没有 `--warm-sentry`，但有 `-background` / `-direct` / `-fs-restore-direct` 可继续压测）
- [x] **Phase 3**：Go 写 Orchestrator MVP：长生命周期容器、checkpoint/restore、agent stdio 桥接、会话 API、WebSocket 事件流
- [x] **Phase 4**：Next.js 前端 MVP：三栏布局 + WebSocket 流式渲染 + Real/Mock fallback
- [ ] **Phase 5**：架构优化：per-container Event Hub 解决输出流路由问题
- [ ] **Phase 6**：Artifact 体验：host 端 inotify 实时事件 + 右栏文件树与预览
- [ ] **Phase 7**：多用户鉴权 + 出站白名单 + cgroup 资源限制 + 可观测 + 接入 OpenCode 作为第二 harness

---

## 1. 目标与范围

- **功能**：让用户在网页里跟一个会写 SQL、跑 Python、连 Doris、画图的 AI agent 对话；右侧实时展示产生的文件（CSV / 图表 / Markdown 报告）。
- **隔离**：每个会话 = 一个全新的 gVisor sandbox（`runsc` 进程组 + 用户态 syscall 拦截），从一个预热的 sentry pool 里取一个使用，用完即销毁。
- **底座**：单台华为云 ECS（Ubuntu 24.04，AMD CPU，`/dev/kvm` 不可用，依赖 gVisor `systrap` 平台）。
- **第一期 Harness**：Claude Code 先跑通，再加 OpenCode。Pi 暂不上。
- **数据源**：远端 Apache Doris（MySQL 协议，端口 9030，只读账号 `ro_user_batt`），默认只关注 `vhr_data` 库。

## 2. 整体架构

```mermaid
flowchart LR
  Browser["Browser (Next.js UI)"]
  Orchestrator["Orchestrator (Go, on ECS host)"]
  Pool["Warm Sentry Pool"]
  Sandbox["Per-session gVisor sandbox (runsc)"]
  Workspace["/var/lib/harness/sessions/<id> (host bind mount)"]
  Doris["Remote Doris (MySQL :9030)"]
  LLM["Anthropic-compatible proxy / OpenAI API"]

  Browser <-->|WebSocket| Orchestrator
  Orchestrator -->|os/exec runsc| Pool
  Pool -->|runsc restore| Sandbox
  Orchestrator <-->|stdio (runsc exec)| Sandbox
  Sandbox -->|netstack -> host veth| Doris
  Sandbox -->|netstack -> host veth| LLM
  Sandbox -->|bind mount| Workspace
  Workspace -->|host inotify| Orchestrator
```

Sandbox 内运行栈（无 init、无 daemon，直接以 agent 进程为 PID 1）：

- 1 个 agent CLI 进程（Claude Code 或 OpenCode），由 `runsc` 直接作为 sandbox 主进程拉起，stdin/stdout 桥接给 orchestrator
- 凭据 / `vhr_data` schema 路径 / session_id / 用户首条消息通过 OCI `process.env` 注入
- artifact 落在 bind-mount 进 sandbox 的 `/workspace`（host 端是 `/var/lib/harness/sessions/<id>`），文件事件由 host 上的 inotify 直接拿到，不需要 in-sandbox uploader

## 3. 关键技术决策

- **运行时选 gVisor `runsc` + `systrap` 平台**：本机是华为云 ECS，`/dev/kvm` 不可用，Firecracker 装不上；嵌套虚拟化即便申请开通也不被 Firecracker 官方支持。gVisor systrap 通过 `seccomp SECCOMP_RET_TRAP` 拦截 syscall，**不依赖虚拟化指令**，本机零阻塞可跑。详见 [`gvisor-decision.md`](./gvisor-decision.md)。
- **不装 Docker**：`runsc` 直接消费 OCI bundle（`config.json` + `rootfs/` 目录）。rootfs 用 `debootstrap` 或 `skopeo + umoci` 构造，host 上不跑任何容器 daemon，依赖最小。
- **通信通道：进程 stdio + bind mount，不开 SSH，不用 vsock**：gVisor 不支持 AF_VSOCK。
  - **Agent stdio**：orchestrator 直接当 `runsc run` 的父进程，读写其 stdin/stdout/stderr。
  - **Artifact 事件**：sandbox 写 `/workspace`（bind 自 host `/var/lib/harness/sessions/<id>`），host 端 `fanotify`/`inotify` 监听该目录把事件推给 orchestrator。**根本不需要 in-sandbox uploader daemon**。
- **网络：netstack + host nftables 严格出站白名单**。gVisor 默认 `--network=sandbox` 走自带用户态 netstack，出口经 host veth pair。nftables 规则挂在 veth 或 cgroup classid 上，只放行 Doris FE 的 `172.16.0.138:9030` 和本机 LLM proxy `127.0.0.1:8082`（或 LLM API 域名解析出的 ipset，由 sidecar 周期刷新），其余全 drop。
- **每会话注入凭据，绝不烤进 rootfs**：rootfs 是"光秃秃"的；启动时 orchestrator 生成的 `config.json` 把 `ANTHROPIC_BASE_URL` / `ANTHROPIC_API_KEY` / `DORIS_*` / `SESSION_ID` 写进 `process.env`，再把首条用户消息通过 stdin 推入。
- **预恢复 sandbox pool**：用 `runsc checkpoint`/`restore` 维持 3–5 个已完成 syscall filter 安装 / netstack 就绪、卡在等待 first message 状态的 sandbox。新会话从池里取一个，用户首字延迟可压到 <100ms，具体取决于 restore 参数和池命中率。
- **后端语言用 Go**：直接 `os/exec` 驱动 `runsc`，无需任何虚拟化 SDK。前端用 Next.js + Tailwind + shadcn/ui。
- **资源限制走 cgroup v2**：在 OCI `config.json` 的 `linux.resources` 里写 memory / cpu / pids 上限，runsc 会代劳设置 cgroup。

## 4. 前端布局（仿 Claude.ai 三栏）

- **左**：会话列表（按用户隔离），新建/重命名/删除/归档
- **中**：聊天流。气泡分三类：
  - 用户输入
  - Agent 思考（可折叠的 thinking block，灰色细字）
  - Agent 最终输出（含 inline 代码块、SQL、图片预览）
  - 工具调用事件行（"running SQL…", "writing report.md", "rendering chart.png"）
- **右**：当前会话的 artifact 树
  - 文件系统视图：`/workspace/` 下目录树
  - 文件预览：CSV → 简易表格、PNG → 图、Markdown → 渲染
  - 下载/打包按钮
- **顶**：sandbox 状态徽标（运行中 / 空闲 / 已停），手动 kill 按钮

## 5. 分阶段实施

> 本机 ECS 当前是个空壳，所以从 0 开始。按从底到顶推进，每一阶段都能独立验证。

### Phase 0 — 环境就绪与可行性确认（已完成）

- 已确认本机 `/dev/kvm` 不可用、AMD `svm` 标志被宿主屏蔽 → 排除 Firecracker，选定 gVisor。
- 已确认 `unprivileged_userns_clone=1` / cgroup v2 / seccomp 编译开启 / fuse 在 / `max_user_namespaces=247758`，gVisor `systrap` 运行条件满足。
- 已通过本机直接验证 Doris 9030 连通性 + `ro_user_batt` 只读权限，统一在 `vhr_data` 库内操作。
- 已拉 `vhr_data` 库的 schema，整成 markdown 放进 `schema-pack/`（启动时 bind mount 进 sandbox 当 system context）。
- 已在本机跑通 Claude Code Proxy（`0.0.0.0:8082`）+ Claude Code CLI + OpenCode CLI（详见 [`llm-harness-local-test.md`](./llm-harness-local-test.md)）。
- 起步资源画像：单 sandbox 1 vCPU / 1 GB RAM，warm pool 上限 5，session 上限 30。

### Phase 1 — 单 sandbox 端到端打通（1–2 天）

- 装 `runsc`（官方静态二进制 + `runsc install` 仅写自身 config，不动 dockerd）。
- 构造 Phase 1 rootfs（推荐 `debootstrap noble ./rootfs` + chroot 装 Python / `pymysql` / `pandas` / `matplotlib` / Node.js / Claude Code CLI；或 `skopeo copy docker://python:3.12-slim` + `umoci unpack`）。**rootfs 是普通目录，不打 ext4 image。**
- 写一份最小 `config.json`（命令、env、`/workspace` bind mount、`memory.limit=1G`），`runsc run claude-test` 启动。
- 在 sandbox 里手工跑 `claude --bare -p`，给它喂 schema + 一句话需求，让它写 SQL、出 CSV、画一张图。
- **门禁标准**：能从 Doris 取数 → 写 CSV → 出 PNG → 写 `report.md`，且这些文件直接出现在 host `/var/lib/harness/sessions/<id>/`。

当前进展见 [`phase1-status.md`](./phase1-status.md)。`runsc`、手工 rootfs、`/workspace` bind mount、gVisor sandbox 网络到 Doris、Claude Code 到本机 proxy 的最小验证已完成；最终业务表 SELECT 受 Doris compute group 权限阻塞，授权后重跑即可完成门禁。

### Phase 2 — Bundle 流水线 + Warm Pool（1–2 天）

- 写 `build-rootfs.sh`：固定 base 选择 + 装包清单 + agent 二进制 → 输出 `rootfs/` 目录（用 rsync/btrfs subvolume snapshot 做 per-session copy）。
- 写 `bake-bundle.sh`：生成模板 `config.json`，再 `runsc checkpoint` 一份 "刚启动等输入" 的状态。
- 写 `restore-sandbox.sh`：从 checkpoint `runsc restore` 一个新 sandbox，输出其 stdin/stdout 句柄路径。
- **门禁**：脚本化一键造 rootfs + 一键起 sandbox，restore 实测和稳定性可接受，低延迟另行靠池化和参数优化。

当前进展见 [`phase2-status.md`](./phase2-status.md)。`build-rootfs.sh`、`bake-bundle.sh`、`restore-sandbox.sh` 已完成并通过 smoke test；本机现装 `runsc release-20260511.0` 没有 `--warm-sentry`，但已提供 `-background` / `-direct` / `-fs-restore-direct` 等官方参数，标准 restore 实测约 124 ms，后续靠池化和参数优化继续压缩。

### Phase 3 — Orchestrator MVP（已完成）

**实现内容：**
- 长生命周期容器架构：容器在消息之间保持运行
- Checkpoint/Restore 机制：idle 30 分钟后自动 checkpoint
- `os/exec` 驱动 `runsc run`/`restore`/`checkpoint`/`delete`
- stdin/stdout/stderr pipe 管理
- 会话生命周期：create / send message / checkpoint / restore / destroy
- 简陋鉴权：lab 共享密码 + cookie
- SQLite 记录 sessions / messages / artifacts 元数据
- WebSocket 事件流：session 状态、agent 输出、artifact 更新
- Stream parser：解析 Claude stream-json 格式

**当前状态：**
- 基础架构已完成并通过测试
- 首条消息全链路正常
- **已知问题**：多轮对话输出流路由问题（goroutines 使用旧 callback）

详见 [`phase3-status.md`](./phase3-status.md) 和 [`architecture.md`](./architecture.md)。

### Phase 4 — 前端 MVP（已完成）

**实现内容：**
- 三栏布局：会话列表 / 对话 / Artifacts
- WebSocket 流式渲染：agent.delta 事件实时显示
- Real backend / Mock fallback 自动切换
- Same-origin proxy：避免 CORS 问题
- Artifact 预览：Markdown / 代码 / 图片 / 文本
- 莫兰迪色系 UI：低饱和、温暖、简约

**当前状态：**
- 前端基础功能已完成
- 可创建 session、发送消息、查看 artifacts
- 受后端输出流问题影响，多轮对话体验不完整

详见 [`phase4-status.md`](./phase4-status.md)。

### Phase 5 — 架构优化（规划中）

**目标：** 解决多轮对话输出流路由问题

**方案：** Per-container Event Hub 架构
- 每个 Container 有独立的 OutputHub
- goroutines 发布到 hub（解耦生产者）
- 每次 runSession() 订阅 hub（独立消费者）
- 支持多个订阅者（监控、日志等）

**预期效果：**
- 修复多轮对话输出问题
- 更清晰的架构边界
- 更好的可扩展性

详见 [`architecture.md`](./architecture.md) 的"推荐优化方案"章节。

## 6. 主要风险与缓解

- **gVisor syscall 兼容性**：少数 syscall（`io_uring` 全套、`statx` 某些 flag、罕用 `ioctl`）gVisor 实现不全。对策：Phase 1 用 `runsc --strace` 留一次完整 trace，发现 ENOSYS 提前处理；Python pandas / matplotlib / Node 18+ 用 epoll，不会踩 io_uring。
- **网络白名单维护**：LLM API 用域名而非固定 IP，需要 sidecar 定期解析并刷 ipset；起步可先放宽到"只允许 443 + 9030 + 本机 8082"。
- **API key 管理**：起步可用单一团队 key 注入；后续要做按用户 key 或代理审计层。
- **Doris 查询量**：用户写出失控 SQL 会拖累整个 Doris。对策：在 sandbox 内的连接器 wrapper 强制 `USE vhr_data;`、`SET query_timeout`、`LIMIT` 提示，host 侧再设 per-session QPS 上限。
- **rootfs 体积**：装满 Python / Node / Claude Code 容易上 GB；rootfs 是目录而非 image，per-session 用 btrfs/overlay 子卷做 copy-on-write，可避免每个会话物理复制一份。
- **OpenCode 与 Claude Code 行为差异**：先把抽象层定义成"标准 stdio 协议 + 事件 schema"，两个 harness 各自适配，UI 不感知。
- **隔离强度比 Firecracker 弱一档**：gVisor 攻击面是 ~200 个用户态实现的 syscall，比 Firecracker 的 ~50 syscall + KVM ioctl 大。对"内部同事 + 只读 DB + 严格出站白名单"的威胁模型够用；未来若要面向外部用户，可以把整套 orchestrator 平移到一台 BMS / 支持嵌套虚拟化的实例上换回 Firecracker（接口形态尽量保持兼容）。

## 7. 仓库结构

```
harness-platform/
├── docs/               # 设计文档、决策记录、Doris schema 整理稿
├── orchestrator/       # Go：sandbox 生命周期、warm pool、stdio 桥、会话 API
├── frontend/           # Next.js：三栏 UI
├── sandbox-image/      # rootfs 构建脚本、OCI config.json 模板
├── bundle/             # bake / restore 脚本（runsc checkpoint/restore）
├── schema-pack/        # Doris 表 schema markdown（启动时 bind mount 进 sandbox 给 agent 当 context）
└── infra/              # nftables、systemd unit、运行手册
```

## 8. 可行性结论

- **架构层面可行**。gVisor + bind mount + 远端 Doris 这条链路在 GKE Sandbox、Cloud Run gen1、Fly Machines（早期）都有大规模生产先例。
- **本机直接可跑**。systrap 平台不依赖 KVM，本机 ECS 缺失 `/dev/kvm` 不构成阻塞。
- **首字延迟达标且更快**。预恢复 pool 命中时可压到 <100 ms；冷启仍在 ~150 ms 量级，是否需要再压缩由后续参数和池化策略决定。
- **最大的工程量在 orchestrator 的 sandbox 生命周期管理 + 前端 artifact 实时同步**，建议 Phase 3 / 5 多预留 buffer。
- **未来可平滑迁回 Firecracker**：sandbox 抽象、stdio 协议、artifact 事件协议都与具体 runtime 解耦；如果之后拿到 BMS，仅需替换 orchestrator 的 runtime adapter。

下一步从 **Phase 1**（装 `runsc` + 手工 rootfs + 单 sandbox 端到端 DEMO）开始。
