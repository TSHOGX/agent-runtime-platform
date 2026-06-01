# Phase 10a: Configurable Agent System Prompt

> Status: planned on top of the completed [Phase 8 runtime isolation hardening](../phase8/README.md).
> Part of [Phase 10](./README.md).

## Goal

Inject an operator-controlled system prompt into every model-backed agent
session through the selected driver adapter.

Use it for stable platform policy: agent identity, capability bounds, sandbox
resource limits, and small mandatory behavioral rules. Keep procedural how-to
content in Phase 10c skills instead.

Claude Code is the first renderer. Pi and later drivers must either implement a
`DriverSystemPromptAdapter` equivalent or explicitly declare system prompts
unsupported; enabling prompts with an unsupported driver must fail at
deployment/config validation.

## Cutover And Gates

Phase 10a uses the Phase 10 destructive cutover principle:

- Pre-10 sessions, generations, checkpoints, control files, bridge queues, and
  driver homes may be deleted instead of migrated.
- Live runtimes, bridge workers, network resources, and provider/isolation
  resources must be stopped, proven absent, or durably quarantined before their
  DB ownership rows are removed.
- Missing prompt fields after cutover are post-cutover corruption, not legacy
  compatibility input.

The 10a merge gate is strict post-cutover artifact validation before any Claude
launch. New and reused prepared artifacts must prove manifest fields, sidecar
state, digests, and session snapshot identity before `Runtime.Start`, including
the live-container reattach path.

## Config

```yaml
harness:
  system_prompt:
    enabled: true
    text: |
      You are BatteryGPT, an internal vehicle data analysis agent.
      You cannot inspect image pixels. For images, explain that limitation.
      Your runtime has a 1 GiB memory limit.
```

```go
type SystemPromptConfig struct {
    Enabled bool
    Text    string
}
```

Add `SystemPrompt SystemPromptConfig` with `yaml:"system_prompt"` to
`orchestrator/internal/config.HarnessConfig`. The loader decodes `harness:`
directly into `HarnessConfig` with `KnownFields(true)`, so the field must live
there.

Validation rules:

- `enabled=true` with `strings.TrimSpace(text) == ""` fails config load.
- Valid prompt text is persisted and injected byte-for-byte; do not trim it
  after validation.
- `enabled=false` stores an inert disabled snapshot even if `text` is present.
- Do not add `SystemPrompt` to `orchestrator/internal/runtime.Config`; runtime
  rendering must use only per-session snapshot input.
- Do not add separate `agent_identity` API/store/frontend fields in 10a. Put
  identity text inside `harness.system_prompt.text`.

## Session Snapshot

Compute the prompt snapshot in `createSession` and persist it in the same
transaction that creates the session row, before any generation is allocated.
Later config changes apply only to newly created sessions.

Persist:

```go
type Session struct {
    SystemPromptEnabled bool   `json:"-"`
    SystemPromptText    string `json:"-"`
    SystemPromptDigest  string `json:"-"`
}
```

Digest rules:

- Enabled: `system_prompt_text` is the exact configured text and
  `system_prompt_digest` is bare lowercase SHA-256 hex over those bytes.
- Disabled: store `false`, `""`, `""`.

The snapshot is internal orchestrator state. It must not appear in public
session DTOs, HTTP responses, SSE events, or frontend payloads. It is not a
secrecy boundary: the prompt is also written into `/harness-control` so the
agent can receive it. Operators must not put credentials, private customer
data, or hidden evaluation canaries in the prompt.

## Manifest And Sidecar

Every post-cutover control manifest emits these fields:

```json
{
  "system_prompt_enabled": true,
  "system_prompt_text": "...",
  "system_prompt_digest": "1f2d..."
}
```

Include all three fields in the strict projected control-manifest digest. The
manifest text is the source of truth; `system_prompt.txt` is only the Claude
delivery artifact.

Artifact rendering must:

- reject enabled snapshots with empty/whitespace text or missing digest using
  stable `system prompt snapshot missing` text;
- write `/harness-control/system_prompt.txt` only when enabled;
- verify sidecar bytes hash back to `system_prompt_digest`;
- unlink stale sidecars when disabled;
- never read process config while rendering artifacts.

## Prepared Artifact Validation

Add `ValidatePreparedGenerationArtifacts(ctx, req)` to the runtime driver and
call it for every non-new generation before `Runtime.Start`, including
live-container hot-path reattach.

The validator must:

- parse the manifest wrapper and payload as raw maps first;
- require raw presence and correct JSON types for all three prompt fields;
- verify wrapper and projected digests against prepared artifacts and stored
  `RuntimeGenerationDetails`;
- compare manifest prompt fields to the persisted session snapshot;
- verify enabled sidecar presence/hash and disabled sidecar absence.

Failure handling:

- Restore failures use the P0 restore-fallback helper: fail/reclaim the claimed
  restoring generation, clear restore metadata, and allow cold fallback N+1.
- Non-restore failures are post-cutover artifact corruption: stop the
  generation-fenced live runtime if present, fail/reclaim generation N, keep the
  session retryable when the P0 turn ledger permits it, and let
  `ensureActiveGeneration` allocate N+1.

Classify these messages as `manifest_digest_mismatch`:

```text
control manifest digest mismatch
projected control manifest digest mismatch
checkpoint metadata mismatch: checkpoint_control_manifest_digest
checkpoint metadata missing: checkpoint_control_manifest_digest
missing system prompt manifest field
system prompt manifest field type mismatch
system prompt snapshot missing
system prompt snapshot mismatch
missing system prompt sidecar
stale system prompt sidecar
system prompt digest mismatch
```

## Sandbox Launch

The entrypoint reads the JSON control manifest, exports only:

- `HARNESS_SYSTEM_PROMPT_ENABLED`
- `HARNESS_SYSTEM_PROMPT_DIGEST`
- `HARNESS_SYSTEM_PROMPT_FILE=/harness-control/system_prompt.txt`

It must verify the sidecar before the bridge claim loop or any retained smoke
tooling can launch Claude. Missing or type-mismatched manifest fields fail
closed; missing fields are never interpreted as disabled prompts.

The legacy `/harness-control/session.env` path is sunset for Phase 10 Claude
launches. If `bundle/restore-sandbox.sh` remains, mark it non-Claude /
Phase-2-only or make it fail closed before Claude launch.

Claude invocation rules:

- `harness-bridge-client` appends `--append-system-prompt-file
  /harness-control/system_prompt.txt` whenever prompts are enabled, before
  adding either `--session-id` or `--resume`.
- Any retained legacy stdin smoke loop uses the same argv rule.
- The shell shim performs no prompt injection in 10a; sub-agents only inherit
  the environment.

## Claude CLI Gate

Pin the Claude Code CLI version installed in the rootfs. The build/reuse path
must assert the installed version without requiring `chroot`.

Release gates:

- First-turn parse check proves `--append-system-prompt-file` is recognized by
  the pinned binary.
- Resume parse check is CI-safe only if a valid local Claude session record can
  be seeded offline; otherwise record it as manual/live evidence.
- A live resumed-turn smoke must prove the resumed turn observes the prompt, not
  merely that the command exits without error.

## Implementation Checklist

1. Add destructive cutover cleanup under the orchestrator owner lock.
2. Add `harness.system_prompt` config, validation, and defaults.
3. Add session snapshot columns/fields with `json:"-"` and an internal accessor.
4. Persist the normalized prompt snapshot at session creation.
5. Pass the stored snapshot through `RuntimeGenerationDetails` /
   `runtime.StartRequest`.
6. Emit prompt fields in the control manifest and projected digest.
7. Render and verify `system_prompt.txt` from manifest text.
8. Add prepared-artifact validation before `Runtime.Start`.
9. Route restore and non-restore validation failures through the correct
   generation lifecycle helpers.
10. Update the entrypoint and bridge client Claude argv construction.
11. Pin and gate the Claude Code CLI prompt-file behavior.
12. Keep Phase 7 manifest docs/fixtures synchronized, labeling prompt fields as
    Phase 10 additions.

## Acceptance Tests

- Cutover deletes or quarantines pre-10 DB/runtime state without leaving live
  provider/isolation resources owned by deleted rows.
- Config decoding rejects blank enabled prompts and preserves valid multi-line
  text.
- Session snapshots round-trip internally, are omitted from public JSON/SSE, and
  are not changed by later config mutation.
- Manifests always include the three prompt fields; changing prompt
  text/digest changes the projected digest.
- Runtime rendering writes, hashes, or removes the sidecar correctly and rejects
  invalid enabled snapshots.
- Prepared validation rejects missing fields, type mismatches, digest drift,
  snapshot mismatch, missing enabled sidecars, and stale disabled sidecars before
  `Runtime.Start`.
- Restore validation failures use cold fallback; non-restore prepared corruption
  tears down the generation-fenced runtime before replacement.
- `session.env` Claude paths fail closed.
- Bridge, entrypoint, rootfs, and failure-classification tests cover the pinned
  Claude prompt flag and stable classifier text.

## Case Study

Recorded incident `sess_eyRQYZ1B67z9h9Vc` on 2026-05-25: the agent exported a
wide Doris table with `SELECT *` plus `fetchall()` over 218,273 rows, exceeded
the 1 GiB sandbox limit, and was killed with `agent_exit_nonzero` / status `-9`.

The operator prompt should therefore include a compact mandatory rule such as:

```text
For database exports, never load unknown or wide result sets with fetchall().
Count or estimate rows first, stream with fetchmany(N), write incrementally to
a temp file, and split large exports by day, partition, or row range.
```

Longer Doris export procedure belongs in a Phase 10c skill.
