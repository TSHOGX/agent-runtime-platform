# Phase 9a: Configurable Harness System Prompt

> Status: planned. Part of [Phase 9](./README.md).

## Goal

Allow the harness operator to inject a configurable system prompt into every new agent session. This prompt carries:

- Agent identity (e.g., "BatteryGPT") — visible to the agent so it can describe itself consistently.
- Capability bounds (e.g., the model deployed behind the proxy cannot read images, so `Read` on image artifacts should be discouraged).
- Sandbox resource constraints (1 GiB cgroup limit; do not `fetchall()` on wide rows; stream large exports — see the case study below).
- Any future operator-specific guidance that should apply to every session, separately from the versioned skills mount (Phase 9c).

The system prompt is operator-controlled and applies to every session. The skills mount (Phase 9c) is for procedural how-to content the agent reads on demand. These two are intentionally separate.

## Config Schema

Add a new section to `config/harness.yaml`:

```yaml
harness:
  system_prompt:
    enabled: true
    agent_identity: BatteryGPT
    text: |
      You are BatteryGPT, a data analysis agent.

      Capability bounds:
      - You cannot read images. If the user references an image artifact, ask them to describe it instead.

      Sandbox resource constraints:
      - Your runtime has a 1 GiB memory limit. The kernel OOM killer will terminate you if you exceed it.
      - For database exports, never use cursor.fetchall() on unknown or wide tables. Stream with fetchmany(N) and write CSV rows incrementally.
      - Count rows first; if a SELECT may return more than ~100k rows of a wide table, paginate or split by day/partition.

      ...
```

Go side:

```go
type SystemPromptConfig struct {
    Enabled        bool
    AgentIdentity  string  // optional, surfaced separately for UI
    Text           string  // canonical injected text
}
```

Validation:

- If `enabled: true` and `text` is empty: fail config load.
- `text` may be multi-line; preserve as-is.
- `agent_identity` is optional and is only used for orchestrator/UI labelling; the agent learns its identity from `text`.

## Control Manifest Fields

Add to the per-generation control manifest:

```json
{
  "system_prompt_enabled": true,
  "system_prompt_text": "...",
  "system_prompt_digest": "sha256:..."
}
```

`system_prompt_digest` is computed over the canonical UTF-8 bytes of `system_prompt_text`. Include it in the strict-field set used by the projected control-manifest digest (see `docs/phase7/checkpoint-restore.md`), so checkpoint/restore enforces prompt continuity: a session checkpointed with prompt version A cannot be restored under prompt version B.

## Entrypoint Integration

The sandbox entrypoint (`sandbox-image/files/usr/local/bin/harness-agent-entrypoint`) reads the system prompt fields from the control manifest before launching the agent.

Claude Code path:

```sh
if [ "$HARNESS_SYSTEM_PROMPT_ENABLED" = "true" ] && [ -s "$HARNESS_SYSTEM_PROMPT_FILE" ]; then
  CLAUDE_ARGS="$CLAUDE_ARGS --append-system-prompt @${HARNESS_SYSTEM_PROMPT_FILE}"
fi
exec claude $CLAUDE_ARGS
```

The text is written to a file inside the control mount (read-only from the agent's perspective) rather than passed as an env var, because env vars have OS-level length limits and the prompt is expected to grow.

Shell path (`harness-shell-agent`):

The shell shim doesn't talk to an LLM, so the system prompt does not apply directly. However, the shim should still expose the prompt to any sub-agent it spawns (`HARNESS_SYSTEM_PROMPT_FILE` is part of the env passed through). No injection at the shell shim layer in Phase 9a.

## Implementation Steps

1. Add `harness.system_prompt` config block and validation.
2. Add `SystemPromptConfig` to the orchestrator runtime config struct.
3. Extend control manifest generation to include the three new fields and update the projected digest projection.
4. Render the prompt text into the per-generation control directory as `system_prompt.txt`.
5. Add a read-only bind mount or include the file in the existing `/harness-control` mount (current entrypoint already reads `/harness-control/manifest.json`; add the prompt file alongside it).
6. Update the entrypoint to pass `--append-system-prompt @/harness-control/system_prompt.txt` to Claude Code when enabled.
7. Tests:
   - `config_test.go`: enabled=true with empty text → validation error.
   - `runtime` package: generated control manifest contains the three fields and the digest.
   - `runtime`: when enabled=false, no prompt file is written and no flag is passed.
   - `checkpoint-restore`: restoring with a changed prompt digest forces cold fallback.

## Case Study: Why This Matters

Recorded incident, session `sess_eyRQYZ1B67z9h9Vc`, 2026-05-25:

The agent was exporting vehicle charging-condition data from Doris to CSV. It successfully wrote two small aggregate tables (33 and 35 rows), then attempted the wide raw-signal table `ods_vhcl_sgnl_f2_gongkuang` (218,273 rows). The generated Python used `cur.fetchall()` after `SELECT *`, materializing the entire result set in memory. The cgroup `/phase3-sess_eyRQYZ1B67z9h9Vc` hit its 1 GiB limit, the kernel OOM killer fired, and the turn failed with `agent_exit_nonzero` / `claude exited with status -9`.

Root cause is prompt-level: the agent had no information about the sandbox memory limit and defaulted to the naïve `fetchall()` pattern. This class of failure cannot be fixed by Claude Code skills or by retry logic — it requires the agent to know about the resource constraint before it writes the export script.

Recommended baseline text (incorporate into operator's `harness.system_prompt.text`):

```text
For database exports:
- Do not use fetchall() for unknown, wide, or large result sets.
- Do not run unbounded SELECT * into memory before writing output.
- Count or estimate rows first, then choose a streaming or paginated export strategy.
- Use fetchmany(N), server-side cursors when available, or key/time-window pagination.
- Prefer exporting only required columns for wide signal tables.
- Print progress after each batch.
- For large tables, write to a temporary file and rename only after successful completion.
- If the expected export may exceed sandbox memory, split output by day, partition, or row range.
```

A safe pattern the agent should default to:

```python
with open(csv_path + ".tmp", "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    cur.execute(query, params)
    writer.writerow([desc[0] for desc in cur.description])
    while True:
        rows = cur.fetchmany(5000)
        if not rows:
            break
        writer.writerows(rows)
os.replace(csv_path + ".tmp", csv_path)
```

This guidance is also a candidate seed for a future Phase 9c skill (`doris-export.md`), but injecting it as a system prompt today is the lowest-cost mitigation.
