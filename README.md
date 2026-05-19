# harness-platform

每会话独立 **gVisor sandbox** 的数据分析 Agent 平台。Agent（Claude Code / OpenCode）跑在 sandbox 内，通过只读账号查询远端 Apache Doris，前端类 Claude.ai 三栏布局（会话列表 / 聊天 / artifact）。

底层运行时是 [gVisor `runsc`](https://gvisor.dev/) 的 `systrap` 平台（用户态 syscall 拦截，不依赖 KVM / 嵌套虚拟化），host 上**不跑 Docker**，直接消费 OCI bundle（`config.json` + `rootfs/`）。

## 文档

- [docs/PLAN.md](./docs/PLAN.md) — 总体技术方案 & 分阶段实施计划
- [docs/gvisor-decision.md](./docs/gvisor-decision.md) — 运行时选型决策记录（为什么放弃 Firecracker，gVisor 能力对照）
- [docs/llm-harness-local-test.md](./docs/llm-harness-local-test.md) — 本机 Claude Code / OpenCode 通过 proxy 的连通性测试
- `docs/doris-connection.md` — Doris 连接凭据（**不入 git**，本机本地查看）

## 当前状态

Phase 0 已完成：本机虚拟化能力已排查（`/dev/kvm` 缺失，AMD `svm` 被宿主屏蔽，Firecracker 不可用，改用 gVisor），LLM harness 本地连通性已通过 Claude Code Proxy 验证，Doris 只读账号已就绪。下一步进入 **Phase 1**：在本机装 `runsc`、手工建 rootfs 目录、用单个 sandbox 跑通"Claude Code → Doris → CSV / PNG / report.md"端到端 DEMO。

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
