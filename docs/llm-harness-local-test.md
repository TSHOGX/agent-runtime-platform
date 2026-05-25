# 本机 LLM Harness 连通性测试

> 测试时间：2026-05-19
> 目的：确认本机 Claude Code Proxy、Claude Code、OpenCode 能否通过 Anthropic-compatible 方式完成最小请求。
> 当前实现注记：OpenCode 只保留为 Phase 0 候选验证记录；当前 agent registry 支持 Claude Code 和 PTY-backed Shell。

## 环境概览

- Claude Code Proxy 路径：`/root/claude-code-proxy`
- Proxy 运行方式：`uv run claude-code-proxy`
- 本地 Anthropic-compatible 地址：`http://127.0.0.1:8082`
- Harness sandbox-visible 地址：每个 generation 从分配到的 `host_gateway_ip` 派生，例如 `http://10.200.0.1:8082`
- Harness 当前本地 client key：`123`
- Claude Messages endpoint：`/v1/messages`
- 上游 OpenAI-compatible base URL：`https://api.modelarts-maas.com/openai/v1`
- 上游模型映射：`deepseek-v4-pro`
- Claude Code 版本：`2.1.144`
- OpenCode 版本：`1.15.5`

敏感信息不要写入文档。当前本机有两类 key：

- 上游 OpenAI-compatible API key：供 proxy 访问模型服务。
- 本地 Anthropic-compatible client key：供 Claude Code / OpenCode 访问 proxy 时通过校验。

## Proxy 直连测试

Claude Code Proxy 已在本机运行，监听 `0.0.0.0:8082`。直连测试使用 Anthropic Messages 格式请求：

```bash
python3 - <<'PY'
import json, urllib.request

payload = {
    "model": "claude-3-5-sonnet-20241022",
    "max_tokens": 32,
    "messages": [{"role": "user", "content": "Hi"}],
}

req = urllib.request.Request(
    "http://127.0.0.1:8082/v1/messages",
    data=json.dumps(payload).encode(),
    headers={
        "Content-Type": "application/json",
        "x-api-key": "<local-client-key>",
    },
    method="POST",
)

with urllib.request.urlopen(req, timeout=120) as resp:
    print(resp.status)
    print(resp.read().decode())
PY
```

结论：返回 `200`，能拿到 Claude 格式的 text response。首次测试上游响应较慢，约 100 秒；后续 CLI 请求约数秒级。

## Claude Code 测试

命令：

```bash
CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1 \
ANTHROPIC_BASE_URL=http://127.0.0.1:8082/v1 \
ANTHROPIC_API_KEY=<local-client-key> \
claude --bare -p --model sonnet --tools "" --output-format json \
  "Reply exactly: hi" < /dev/null
```

结论：返回成功，`result` 为 `hi`。

注意：

- 本机 `/root/.claude/settings.json` 已配置 `ANTHROPIC_BASE_URL=http://0.0.0.0:8082` 和本地 auth token。
- Claude Code 会从 settings 注入这些环境变量；命令行临时设置可能被 settings 中的值覆盖。
- Debug 日志能看到请求走 `/v1/messages`，且因 `ANTHROPIC_BASE_URL` 指向本地非一方 host，tool search 默认关闭。

## OpenCode 测试

命令：

```bash
ANTHROPIC_BASE_URL=http://127.0.0.1:8082/v1 \
ANTHROPIC_API_KEY=<local-client-key> \
opencode run --pure \
  -m anthropic/claude-3-5-sonnet-20241022 \
  --format json \
  "Reply exactly: hi" < /dev/null
```

结论：返回成功，text part 为 `hi`。

注意：

- OpenCode 本机没有 provider credentials，但可通过环境变量跑通。
- `ANTHROPIC_BASE_URL` 必须包含 `/v1`。如果只写 `http://127.0.0.1:8082`，OpenCode 会请求 `/messages`，proxy 返回 `404 Not Found`。
- 非交互测试建议显式加 `< /dev/null`，避免 CLI 等待 stdin 导致卡住。

## 对项目的影响

- Phase 0 的 LLM API / harness 连通性已验证：本机已有可用的 Anthropic-compatible proxy。
- 当前 orchestrator 不依赖宿主机 Claude settings。`config/harness.yaml` 已迁到 Phase 7 `harness:` schema；当前热路径把本地 proxy 约定写入每个 generation 的 control manifest：宿主机 bind URL `http://0.0.0.0:8082`、sandbox base URL 从分配到的 `host_gateway_ip` 派生、本地 key `123` 通过 Phase 7 secret materialization 注入。
- Phase 1 在 sandbox 内集成时，可以优先复用同一组约定：`ANTHROPIC_BASE_URL` 指向 proxy 的 `/v1` base URL，`ANTHROPIC_API_KEY` 使用本地 client key（通过 runtime control manifest 注入，不烤进 rootfs）。
- OpenCode 当时验证为可行候选，但当前实现没有注册 OpenCode agent；现有主路径是 Claude Code，Shell 作为交互式命令会话。
