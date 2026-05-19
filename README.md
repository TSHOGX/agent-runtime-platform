# harness-platform

每会话独立 **gVisor sandbox** 的数据分析 Agent 平台。Agent（Claude Code / OpenCode）跑在 sandbox 内，通过只读账号查询远端 Apache Doris，前端类 Claude.ai 三栏布局（会话列表 / 聊天 / artifact）。

底层运行时是 [gVisor `runsc`](https://gvisor.dev/) 的 `systrap` 平台（用户态 syscall 拦截，不依赖 KVM / 嵌套虚拟化），host 上**不跑 Docker**，直接消费 OCI bundle（`config.json` + `rootfs/`）。

## 文档

- [docs/PLAN.md](./docs/PLAN.md) — 总体技术方案 & 分阶段实施计划
- [docs/gvisor-decision.md](./docs/gvisor-decision.md) — 运行时选型决策记录（为什么放弃 Firecracker，gVisor 能力对照）
- [docs/llm-harness-local-test.md](./docs/llm-harness-local-test.md) — 本机 Claude Code / OpenCode 通过 proxy 的连通性测试
- [docs/phase1-status.md](./docs/phase1-status.md) — Phase 1 手工 sandbox 验证状态
- [docs/phase2-status.md](./docs/phase2-status.md) — Phase 2 rootfs / bundle / restore 脚本状态
- [docs/phase3-status.md](./docs/phase3-status.md) — Phase 3 Go orchestrator MVP 状态
- [docs/runsc-warm-sentry-research.md](./docs/runsc-warm-sentry-research.md) — 新版 `runsc` 与 warm sentry 调研结论
- `docs/doris-connection.md` — Doris 连接凭据（**不入 git**，本机本地查看）

## 当前状态

Phase 0 已完成：本机虚拟化能力已排查（`/dev/kvm` 缺失，AMD `svm` 被宿主屏蔽，Firecracker 不可用，改用 gVisor），LLM harness 本地连通性已通过 Claude Code Proxy 验证，Doris 只读账号已就绪。

Phase 1 已完成最小验证：手工 rootfs、`runsc` sandbox、workspace bind mount、Doris 元数据查询和 Claude Code 到本机 proxy 的连通性均已跑通；业务表 SELECT 仍等 Doris compute group 授权。

Phase 2 已补齐脚本：`sandbox-image/build-rootfs.sh`、`bundle/bake-bundle.sh`、`bundle/restore-sandbox.sh` 可一键生成 rootfs / bundle / checkpoint 并 restore sandbox。当前本机 `runsc release-20260511.0` 没有 `--warm-sentry`，但提供 `-background` / `-direct` / `-fs-restore-direct` 等官方 restore 选项；标准 restore smoke test 约 124 ms，后续转向参数压测和预恢复 sandbox pool。

Phase 3 已补齐 Go orchestrator MVP：HTTP 会话 API、WebSocket 生命周期 / artifact 事件流、共享密码登录、SQLite 元数据、artifact watcher，以及对 Phase 2 `restore-sandbox.sh` 的 runtime adapter。当前 MVP 支持每个 sandbox 的首条消息全链路；新版 `runsc restore` 已在长期运行 Go 服务场景下稳定，当前路径不再保留 cold `runsc run` fallback。

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
