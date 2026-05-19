# runsc Warm Sentry 调研记录

> Date: 2026-05-20
> Scope: `runsc` 新版本下 warm sentry / checkpoint restore 能力确认。

## 背景

项目 Phase 2 / Phase 3 文档里曾假设：升级到新版 `runsc` 后，可以开启 `--warm-sentry`，从而把 checkpoint restore 延迟压到 100 ms 以下，并解决旧版 `runsc restore` 从 Go orchestrator 启动时的不稳定问题。

这次安装新版 `runsc` 后，需要重新确认该假设是否仍成立。

## 本机版本

当前主机安装版本：

```bash
runsc version release-20260511.0
spec: 1.2.1
```

实际命令能力检查结果：

- `runsc help restore` 没有 `--warm-sentry` 参数。
- `runsc help reset` 返回 `Subcommand reset not understood`。
- `runsc flags` 没有 `warm-sentry`。
- 本机 `runsc` 二进制字符串中也没有 `warm-sentry` / `warm_sentry`。

因此，官方 `release-20260511.0` 不能直接使用 warm sentry。

## 上游状态

相关上游资料：

- Proposal: <https://github.com/google/gvisor/issues/12809>
- PR: <https://github.com/google/gvisor/pull/12810>

结论：

- `--warm-sentry` 来自 2026-03 的上游提案和 PR。
- PR 标题为 `runsc: add warm sentry mode for faster restore cycles`。
- PR 状态是 closed，`merged_at` 为 `null`，没有合入官方 gVisor。
- PR 原始设计中包含：
  - `runsc --warm-sentry create`
  - `runsc --warm-sentry restore`
  - `runsc reset`
  - 复用同一个 sentry 进程、seccomp filters、platform、URPC server。
- PR 作者给出的实验数据是：
  - `--network=none`: cold restore 平均约 144 ms，warm restore 平均约 65 ms。
  - `--network=sandbox`: cold restore 平均约 160 ms，warm restore 平均约 79 ms。
- 但 gVisor maintainer 在 review 中指出安全问题：Sentry 运行过不可信代码后不应继续被信任；如果复用 Sentry 状态，后续执行的数据可能被已被利用的 Sentry 窃取。
- PR 作者随后关闭了 PR，表示这个方向不够 clean / secure。

所以，warm sentry 当前是未合入的实验方向，不是新版官方 `runsc` 的可用能力。

## 新版 runsc 可用的 restore 优化点

官方新版仍保留 checkpoint / restore，并暴露了一些可以继续评估的参数：

`runsc checkpoint`：

- `-compression none|flate-best-speed`
- `-exclude-committed-zero-pages`
- `-direct`
- `-leave-running`

`runsc restore`：

- `-background`
- `-direct`
- `-fs-restore-direct`

这些参数与 warm sentry 不是一回事。它们不能复用 Sentry 进程，但可以减少 checkpoint 镜像体积、I/O 或前台 restore 等待时间，需要结合本项目的 checkpoint 格式和 workload 单独压测。

## 对本项目的结论

1. 不再把 `--warm-sentry` 作为 Phase 2 / Phase 3 的升级目标。
2. 文档中“升级 runsc 后打开 warm sentry”的描述需要修正。
3. 近期优先目标应改为：
   - 用 `release-20260511.0` 重新 bake checkpoint。
   - 复测 `bundle/restore-sandbox.sh` 标准 restore。
   - 复测 Go orchestrator 从长进程启动 restore 时，旧版 `hostfd ... socket operation on non-socket` 问题是否消失。
   - 新版 `runsc release-20260511.0` 上已复测通过，cold `runsc run` fallback 可以移除。
   - 再评估 `restore -background`、`checkpoint -exclude-committed-zero-pages`、`restore -direct` 等官方参数。
4. 如果仍需要“首字延迟 <100 ms”或更低，应在 orchestrator 层做 pool：
   - 预先 restore 若干一次性 sandbox。
   - 请求到来时从 pool 中取已就绪 sandbox。
   - 每个 sandbox 用完即销毁，不复用执行过用户代码的 Sentry。

## 简要结论

官方新版 `runsc release-20260511.0` 没有 warm sentry。warm sentry 是未合入的上游实验 PR，并且因安全模型问题被关闭。本项目应继续基于官方 checkpoint / restore 做稳定性和性能复测；低延迟方向应转向 orchestrator 级别的预恢复 sandbox pool，而不是等待或依赖 `--warm-sentry`。当前 host 上的长进程 restore 已验证稳定。
