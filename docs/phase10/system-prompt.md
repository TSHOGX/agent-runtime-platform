# Phase 10a: Configurable Agent System Prompt

> Status: planned on top of the completed [Phase 8 runtime isolation hardening](../phase8/README.md).
> Part of [Phase 10](./README.md).

## Merge Gate

Phase 10a has completed baseline prerequisites and one remaining
Phase 10a-specific merge gate:

- Completed baseline: Phase 8 runtime isolation, because 10a adds
  control-manifest fields and a Claude-visible sidecar that must use the
  sandbox-visible projection and exact mount contract.
- Completed baseline: P0 session-lifetime separation, including retryable
  generation start failures, restore-fallback retirement, durable non-terminal
  runtime events, and failed/canceled bridge turn completions that do not fail
  the session.

The remaining Phase 10a-specific merge gate is prepared-artifact compatibility. Phase 10a introduces stricter manifest/sidecar validation for already prepared generations, so the implementation must add:

- Host-side prepared-artifact validation before `Runtime.Start`, including the existing live-container hot path.
- Retryable prepared-generation retirement for non-restore active/idle prepared artifacts, including generation-fenced live-runtime teardown when a container already exists.

Restore validation failures should use the completed P0 restore-fallback retirement path. Non-restore prepared-artifact failures need a Phase 10a helper that retires only the affected generation and then allocates replacement generation N+1 for the same session.

## Goal

Allow the platform operator to inject a configurable system prompt into every new agent session. This prompt carries:

- Agent identity inside the prompt text (e.g., "You are BatteryGPT") so the agent can describe itself consistently.
- Capability bounds (e.g., the model deployed behind the proxy cannot read images, so `Read` on image artifacts should be discouraged).
- Sandbox resource constraints (1 GiB cgroup limit; do not `fetchall()` on wide rows; stream large exports — see the case study below).
- Any future operator-specific guidance that should apply to every session, separately from the versioned skills mount (Phase 10c).

The system prompt is operator-controlled and applies to every session. The skills mount (Phase 10c) is for procedural how-to content the agent reads on demand. These two are intentionally separate.

Phase 10a should land through `DriverSystemPromptAdapter` after the Phase 9
driver contract. This document names Claude Code in the delivery sections
because Claude is the first renderer; Pi and later drivers must either implement
their own renderer or declare system-prompt support `unsupported`.

## Config Schema

Add a new section to `config/harness.yaml`:

```yaml
harness:
  system_prompt:
    enabled: true
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
    Enabled bool
    Text    string  // canonical injected text
}
```

Loader/runtime plumbing:

- Add the `SystemPrompt SystemPromptConfig` field with the `yaml:"system_prompt"` tag directly to `orchestrator/internal/config.Phase7Config` unless the loader is first refactored to decode `harness:` into a wrapper type. The current loader decodes `harness:` directly into `Phase7Config` with `KnownFields(true)`, so `harness.system_prompt` will be rejected unless that schema owns the field.
- Do **not** add `SystemPrompt` to `orchestrator/internal/runtime.Config`. The runtime must not read process config when rendering prompt fields; it accepts prompt fields only via per-generation `runtime.StartRequest` / `RuntimeGenerationDetails` input derived from the persisted session snapshot. Removing this plumbing eliminates the temptation for `renderGenerationArtifacts` to fall back to current process config when a session was created under a different config.
- The default is disabled with empty `text`.

Validation:

- If `enabled: true` and `strings.TrimSpace(text) == ""`: fail config load. Whitespace-only prompts are rejected as accidental blank configuration.
- `text` may be multi-line; after validation, preserve it byte-for-byte as configured. Do not trim the stored or injected prompt text.
- Phase 10a does not add separate `agent_identity` config, API, store, or frontend fields. If an operator wants an identity such as "BatteryGPT", it must be included in `text`; the agent and UI-visible transcript learn it only from the injected text.

## Control Manifest Fields

Add to the per-generation control manifest:

```json
{
  "system_prompt_enabled": true,
  "system_prompt_text": "...",
  "system_prompt_digest": "1f2d..."
}
```

`system_prompt_digest` uses the existing manifest digest style: bare lowercase SHA-256 hex from `digestHex`, with no `sha256:` prefix. It is computed over the canonical UTF-8 bytes of `system_prompt_text` exactly as stored in the manifest. When disabled, set `system_prompt_enabled=false`, `system_prompt_text=""`, `system_prompt_digest=""`, and do not write the sidecar prompt file.

The manifest text is the source of truth. `system_prompt.txt` is only a Claude delivery artifact rendered from the manifest; it must not become a second independent prompt source.

Prompt continuity is session-pinned in Phase 10a. `createSession` persists the session row before any generation is allocated, and the first generation may be prepared much later, on first message. The prompt snapshot must therefore be computed and persisted **at session creation time**, not at first-generation prepare. Later changes to `harness.system_prompt.text` apply only to newly created sessions. Existing sessions keep their original `system_prompt_text`, `system_prompt_digest`, and `ClaudeSessionUUID` across prepared-generation reuse, checkpoint restore, and cold fallback. This avoids resuming a Claude Code session created under prompt A while passing prompt B, and avoids relying on the CLI's behavior for a new append prompt during `--resume`.

Snapshot normalization is explicit:

- If `harness.system_prompt.enabled=false`, persist `system_prompt_enabled=false`, `system_prompt_text=""`, and `system_prompt_digest=""`, even when the config file contains non-empty `text`. Disabled configured text is inert operator configuration, not session state.
- If `harness.system_prompt.enabled=true`, persist the text byte-for-byte and persist `digestHex([]byte(text))`.

Implementation consequence: every `runtime.StartRequest` / `RuntimeGenerationDetails` for a session must carry that session's persisted prompt snapshot as input. The runtime layer must reject a request whose `system_prompt_enabled=true` but has `strings.TrimSpace(system_prompt_text) == ""` or missing `system_prompt_digest`, rather than silently falling back to current process config. Include all three prompt fields in the strict-field set used by the projected control-manifest digest (see `docs/phase7/checkpoint-restore.md`) so checkpoint/restore compares the stored prompt snapshot as part of the session's restore identity. If a future phase wants config changes to apply to existing sessions, it must instead rotate `ClaudeSessionUUID` and start a fresh Claude session whenever the prompt digest changes.

### Pre-10a Generation Compatibility

Generations prepared before 10a do not carry `system_prompt_*` manifest fields, and pre-10a session rows do not have the snapshot columns. This applies to checkpointed generations and to ordinary active/idle prepared generations whose control manifest was rendered before the upgrade. Phase 10a's policy:

- Session migration: existing session rows backfill to `system_prompt_enabled=false`, `system_prompt_text=""`, `system_prompt_digest=""`. These remain disabled for the life of the session even if the operator later enables a prompt — only sessions created after the operator enables the prompt receive non-empty values.
- Checkpoint restore: pre-10a checkpoint manifests lack the three prompt fields. Strict-projection digest comparison will mismatch against any post-10a manifest that includes the new keys. We accept this: pre-10a checkpoints **cold-fallback** on first restore after upgrade, which is consistent with the existing `runsc`-version invalidation behavior in `docs/phase7/checkpoint-restore.md`. In this plan, cold fallback means allocating a replacement generation for the **same existing session**; it does not create a new session and does not snapshot the current operator config. The replacement generation must use the migrated session's stored prompt snapshot, which is disabled/empty for pre-10a sessions. The plan does not introduce a legacy normalization path; operators should drain checkpointed sessions before the 10a upgrade if cold-start latency matters.
- Active/idle prepared generations: pre-10a non-checkpointed manifests also lack the three prompt fields. The prepared-artifact validator must treat that as an incompatible prepared artifact, fail/reclaim only the old generation through the Phase 10a prepared-generation retirement path, and let the normal replacement-generation flow allocate N+1 for the same migrated session using the stored disabled/empty prompt snapshot. This path must not use the generic newly allocated start-failure helper, because a prepared generation may already have a live runtime and must be torn down by generation before the old resources become reclaimable. Turn retry eligibility still follows the Phase 7/P0 turn ledger rules. This is an automatic one-time fallback, not a terminal session failure and not a new session. Operators may drain active/idle sessions before the 10a upgrade to avoid that first post-upgrade retry, but draining is not required for correctness.
- New manifests must always emit all three prompt fields (with empty/false defaults when disabled) so post-10a checkpoints have a stable, comparable shape from the start.

## Session Store Snapshot

The persisted snapshot is internal-only state for the orchestrator API boundary. Public session responses and `session.created` events are rendered through the `apiSession` DTO in `orchestrator/internal/server/session_dto.go`, not by exposing every `store.Session` field. The operator system prompt must not appear in that DTO or in any public HTTP/SSE payload.

Required handling:

- Add the three snapshot fields to `store.Session` with `json:"-"` tags as defense in depth, and do not add matching fields to `apiSession`:

```go
type Session struct {
    // ... existing fields ...
    SystemPromptEnabled bool   `json:"-"`
    SystemPromptText    string `json:"-"`
    SystemPromptDigest  string `json:"-"`
}
```

- The snapshot is read only by internal call sites (`renderGenerationArtifacts`, prepared-artifact validation, checkpoint-restore identity comparison) via `store.GetSession` / a dedicated `store.GetSessionPromptSnapshot(ctx, sessionID)` accessor. It must not be exposed through HTTP responses, SSE events, or any new API DTO.
- Add a server-level test that decodes `GET /sessions/{id}` and `session.created` event payloads and asserts none of the three snapshot keys appear, even when the session was created with a non-empty prompt. This guards against future code that accidentally re-exports the field (for example, by adding a struct-copy DTO or removing the JSON tag).

## Prompt Visibility Boundary

Operator prompts are policy, not secrets. The `json:"-"` tags and API tests prevent accidental HTTP/SSE exposure, but they are not a sandbox secrecy boundary. Phase 10a stores the prompt text in the session row and writes it into the per-generation control manifest (`/harness-control/session.json`) and sidecar (`/harness-control/system_prompt.txt`) so Claude can receive it. Agent tools and shell commands running in the sandbox can read those mounted files.

Operational rule: do not put credentials, secret tokens, private customer data, or hidden evaluation canaries in `harness.system_prompt.text`. If a future operator needs prompt content that is hidden from sandbox tools while still influencing the model, the delivery design must change; Phase 10a intentionally does not provide that boundary.

Because Phase 8 release gates scan host-rendered control artifacts under
`/harness-control`, prompts must also use sandbox-visible paths such as
`/workspace` and must not include host roots, proxy-internal paths, provider
paths, or other host-only runtime identities.

## Host-Side Artifact Rendering

Host-side validation is the primary correctness gate. Do not rely on entrypoint-only sidecar validation for server failure classification: in the default bridge path, `startEnsuredGeneration` calls `Runtime.Start` with `WaitForTurn=false`, and `Runtime.Start` returns success as soon as `runsc run` starts. An entrypoint exit after that point may not flow through `runtimeFailureClass`.

Implement sidecar handling in `orchestrator/internal/runtime.renderGenerationArtifacts`, which is shared by `PrepareGeneration` and the lazy render path:

- Build the manifest prompt fields **only** from the per-request `runtime.StartRequest` / `RuntimeGenerationDetails` snapshot input. Do not consult `runtime.Config` or any process-level config inside the render path; the runtime layer has no `SystemPrompt` config to read. Reject the request with a stable error (e.g. `system prompt snapshot missing`) if `system_prompt_enabled=true` but `strings.TrimSpace(system_prompt_text) == ""` or `system_prompt_digest` is empty, rather than silently falling back.
- Derive the sidecar path from the control manifest directory, e.g. `filepath.Join(filepath.Dir(details.ControlManifestPath), "system_prompt.txt")`.
- When enabled, write `system_prompt.txt` atomically with bytes exactly equal to `manifest.system_prompt_text`, then verify on the host before returning artifacts:

```text
system_prompt_enabled=true requires:
  system_prompt.txt exists and is non-empty
  sha256(system_prompt.txt bytes) == system_prompt_digest

system_prompt_enabled=false requires:
  system_prompt.txt does not exist
```

- If host verification fails, return an error before `RecordGenerationRuntimeArtifactDigests`, `MarkGenerationResourcesLive`, or `Runtime.Start`. Use stable error text such as `missing system prompt sidecar`, `stale system prompt sidecar`, or `system prompt digest mismatch` so the server can classify the failure as `manifest_digest_mismatch`.
- When disabled, set empty manifest fields and explicitly unlink the sidecar path, ignoring not-exist. This is required because `prepareGenerationDirs` only calls `MkdirAll`; reused control paths must not retain an old `system_prompt.txt`.

Prepared artifacts need the same host-side check, and the check must be visible at the server/runtime boundary. `Runtime.generationArtifacts` currently returns `req.PreparedArtifacts` without calling `renderGenerationArtifacts` when bundle/spec/manifest digests are already populated, and `Runtime.Start` checks the live-container hot path before it calls `generationArtifacts`. A validator hidden only inside `generationArtifacts` would therefore be bypassed for existing live containers. `startEnsuredGeneration` only calls `PrepareGeneration` for `ensured.IsNew`, so add an explicit validation path for prepared artifacts before `Runtime.Start`:

- Extend `runtimeDriver` with `ValidatePreparedGenerationArtifacts(ctx, req)` and implement it on `Runtime`. Do not rely on `Runtime.Start` or `Runtime.generationArtifacts` alone for this validation.
- The validator must read the already prepared control manifest wrapper before any sidecar checks. Parse the wrapper and `payload` as raw maps first, require raw presence of `system_prompt_enabled`, `system_prompt_text`, and `system_prompt_digest`, and verify their JSON types before any typed `controlManifest` decode or struct defaults can run. Do not unmarshal directly into a typed `controlManifest` first: pre-10a manifests that omit these keys must fail as `missing system prompt manifest field`, not silently default to `false`/empty values. After raw validation, canonicalize the manifest payload and verify the wrapper digest, compare that digest to both `req.PreparedArtifacts.ManifestDigest` and the stored `RuntimeGenerationDetails.ControlManifestDigest`, compute the projected control-manifest digest, and compare it to both `req.PreparedArtifacts.ProjectedManifestDigest` and the stored `RuntimeGenerationDetails.ProjectedControlManifestDigest`. A stale or tampered prepared manifest must fail before Claude launch even if its sidecar file happens to match itself.
- After the wrapper and projected digest checks pass, the validator must compare `system_prompt_enabled`, `system_prompt_text`, and `system_prompt_digest` in the manifest payload back to the persisted session snapshot carried on `RuntimeGenerationDetails`. Only then should it derive `system_prompt.txt` from the manifest directory and apply the same enabled/trimmed-empty/digest/SHA-256 sidecar checks. If the manifest snapshot is disabled, the validator must additionally require that `system_prompt.txt` is absent and fail with stable text such as `stale system prompt sidecar` when a leftover sidecar exists.
- `startEnsuredGeneration` must invoke this validation for every non-new prepared generation before constructing the final `Runtime.Start` request, including the case where the runtime already has a live container and `Runtime.Start` would otherwise return from the hot path.
- If validation fails for `ensured.RestoreFromCheckpoint`, the server must route the error through the P0 restore-fallback retirement helper, not the ordinary pre-start generation failure helper. `ClaimCheckpointedGenerationForRestore` has already moved the generation to `restoring` and allocation/resources to `recreating`; the restore-fallback helper is responsible for failing the claimed generation, moving resources to `reclaimable`, clearing checkpoint/restore session metadata, publishing the retryable non-terminal event, and allowing cold fallback N+1. The same transition must apply whether the restore error comes from prepared-artifact validation before `Runtime.Start`, from `Runtime.Start`, or from the post-restore `MarkGenerationResourcesLive` path after live runtime cleanup.
- If validation fails for a non-restore prepared generation, the server must use a Phase 10a retryable prepared-generation retirement helper, not the generic pre-start failure path. This is a runtime retirement path, not only a database transition: if generation N may already have a live runtime, the helper must stop it before or atomically with failing the generation so N cannot keep polling the bridge or emitting output after its resources become reclaimable. Use the existing runtime teardown surface only if it also evicts the in-memory live-container entry; otherwise extend the runtime driver with an explicit session/generation teardown method and call it from this helper. After teardown, call `store.FailGeneration` to mark generation N failed and resources reclaimable, leave the session status/input eligibility intact, avoid publishing a terminal `session.failed` event, publish a retryable non-terminal event, and return a sentinel that makes `sendMessage` re-run `ensureActiveGeneration` and start replacement generation N+1 for the same session. This is the explicit path used for pre-10a active/idle manifests, stale prepared sidecars, and other prepared-artifact validation failures discovered before `Runtime.Start`, including existing live containers that `Runtime.Start` would otherwise treat as the hot path.
- Classification and error text must match the render path, entrypoint, and manifest-digest paths (`control manifest digest mismatch`, `projected control manifest digest mismatch`, `checkpoint metadata mismatch: checkpoint_control_manifest_digest`, `checkpoint metadata missing: checkpoint_control_manifest_digest`, `missing system prompt manifest field`, `system prompt manifest field type mismatch`, `system prompt snapshot missing`, `system prompt snapshot mismatch`, `missing system prompt sidecar`, `stale system prompt sidecar`, `system prompt digest mismatch`).

The sandbox entrypoint must still verify the delivery artifact before launching Claude as defense in depth, but Phase 10a's normal failure path should be caught during host-side validation before `Runtime.Start`.

## Sandbox Launch Integration

The sandbox entrypoint (`sandbox-image/files/usr/local/bin/harness-agent-entrypoint`) reads the system prompt fields from the control manifest before launching the agent. It must export prompt file/digest metadata and verify the sidecar before dispatching into either the default bridge claim loop or the legacy stdin turn loop. This check protects against tampering or unexpected artifact drift after host rendering; it is not the primary server-side classification mechanism unless a future bridge-readiness wait is added before `Runtime.Start` returns success.

JSON manifest loading should export only file/digest metadata, not the prompt text itself:

```python
for key in ("system_prompt_enabled", "system_prompt_text", "system_prompt_digest"):
    if key not in data:
        die(f"missing system prompt manifest field: {key}", exit_code=65)

enabled = data["system_prompt_enabled"]
text = data["system_prompt_text"]
digest = data["system_prompt_digest"]
if not isinstance(enabled, bool) or not isinstance(text, str) or not isinstance(digest, str):
    die("system prompt manifest field type mismatch", exit_code=65)
if enabled and (not text.strip() or not digest):
    die("system prompt snapshot missing", exit_code=65)
if not enabled and (text != "" or digest != ""):
    die("system prompt snapshot mismatch", exit_code=65)

emit("HARNESS_SYSTEM_PROMPT_ENABLED", "1" if enabled else "0")
emit("HARNESS_SYSTEM_PROMPT_DIGEST", digest)
emit("HARNESS_SYSTEM_PROMPT_FILE", "/harness-control/system_prompt.txt")
```

The legacy `/harness-control/session.env` control path must not bypass the fail-closed prompt checks. Phase 10a may either deprecate env control files and reject `session.env` for Claude launches, or require an equivalent env contract with `HARNESS_SYSTEM_PROMPT_ENABLED`, `HARNESS_SYSTEM_PROMPT_DIGEST`, and `HARNESS_SYSTEM_PROMPT_FILE` plus the same sidecar verification before any bridge or legacy Claude branch. If env support remains, enabled prompts require a non-empty digest and digest-matching `/harness-control/system_prompt.txt`; disabled prompts require the file to be absent. Missing env prompt metadata is an incompatible control file, not an implicit disabled prompt.

Known legacy producer: `bundle/restore-sandbox.sh` currently writes `session.env` without prompt metadata. Current docs mark the Phase 2 bundle scripts as quarantined smoke tooling, not active runtime evidence. Phase 10a must preserve that boundary or update/deprecate the script explicitly. If the script remains a Claude smoke tool, it must emit the equivalent prompt env fields, disabled/empty defaults, and sidecar behavior described above. If the env control path is deprecated, the script must be marked non-Claude/Phase-2-only or fail closed before launching Claude so it cannot silently exercise a bypass path.

Common sidecar verification:

```sh
verify_system_prompt_file() {
  if [ "${HARNESS_SYSTEM_PROMPT_ENABLED:-0}" != "1" ]; then
    [ ! -e "${HARNESS_SYSTEM_PROMPT_FILE:-/harness-control/system_prompt.txt}" ] || {
      echo "[entrypoint] stale system prompt sidecar" >&2
      exit 65
    }
    return 0
  fi
  if [ -z "${HARNESS_SYSTEM_PROMPT_FILE:-}" ] || [ -z "${HARNESS_SYSTEM_PROMPT_DIGEST:-}" ]; then
    echo "[entrypoint] system prompt snapshot missing" >&2
    exit 65
  fi
  [ -s "$HARNESS_SYSTEM_PROMPT_FILE" ] || {
    echo "[entrypoint] missing system prompt sidecar" >&2
    exit 65
  }
  actual="$(sha256sum "$HARNESS_SYSTEM_PROMPT_FILE")"
  actual="${actual%% *}"
  [ "$actual" = "$HARNESS_SYSTEM_PROMPT_DIGEST" ] || {
    echo "[entrypoint] system prompt digest mismatch" >&2
    exit 65
  }
}

verify_system_prompt_file
```

Do not use shell parameter expansion errors such as `${VAR:?message}` for required prompt metadata. Those messages are shell-generated and do not provide stable classifier text. Entrypoint failures must echo one of the classifier substrings listed in "Failure Classification" before exiting.

The verifier must run after the entrypoint validates the expected manifest identity fields and before any branch that can launch Claude. This placement matters because Phase 7 defaults `HARNESS_BRIDGE_MODE=claim-loop` in `orchestrator/internal/runtime/runtime.go`, and the entrypoint normally execs the bridge client before reaching the legacy stdin loop:

```sh
if [ "${HARNESS_BRIDGE_MODE:-}" = "claim-loop" ]; then
  echo "[entrypoint] bridge claim loop starting" >&2
  exec /usr/local/bin/harness-bridge-client claim-loop
fi
```

### Default Claude Path: Bridge Claim Loop

The actual default Claude argv is built in `sandbox-image/files/usr/local/bin/harness-bridge-client`, inside `ClaudeTurnRunner.run_turn`. Add the prompt flag to the `claude_command` list there, for both first-turn and resumed turns:

```python
claude_command = [
    "/usr/local/bin/claude",
    "--bare",
    "--permission-mode",
    "bypassPermissions",
    "-p",
    "--model",
    os.environ.get("CLAUDE_MODEL", "sonnet"),
    "--input-format",
    "stream-json",
    "--output-format",
    os.environ.get("CLAUDE_OUTPUT_FORMAT", "stream-json"),
    "--include-partial-messages",
    "--replay-user-messages",
    "--verbose",
]
if os.environ.get("HARNESS_SYSTEM_PROMPT_ENABLED", "0") == "1":
    prompt_file = os.environ.get("HARNESS_SYSTEM_PROMPT_FILE", "")
    if not prompt_file:
        raise RuntimeError("HARNESS_SYSTEM_PROMPT_FILE is required for system prompt")
    claude_command.extend(["--append-system-prompt-file", prompt_file])
```

Keep `claude_command` as a list; do not build a string-valued argument list. Use `--append-system-prompt-file "$HARNESS_SYSTEM_PROMPT_FILE"` / `["--append-system-prompt-file", prompt_file]`, not `--append-system-prompt "@/harness-control/system_prompt.txt"`.

CLI evidence pinned against Claude Code `2.1.150`:

- **First-turn / file-flag parse**: `claude -p --append-system-prompt-file /tmp/does-not-exist ...` fails before any model request with `Error: Append system prompt file not found`; a fake flag fails as `unknown option`. Binary inspection shows `--append-system-prompt-file <file>` registered and read into `appendSystemPrompt`.
- **First-turn / live model**: a `--bare -p --no-session-persistence --tools "" --append-system-prompt-file <tmpfile>` invocation returned a random token present only in the tmpfile; the same prompt without the flag reported no token.
- **Resumed-turn / file-flag parse (REQUIRED before merge)**: rerun the file-not-found and `unknown option` checks with `--resume <existing-session-uuid>` instead of a fresh session. The UUID must name an existing local Claude session created by the same installed CLI; otherwise the command can fail on resume lookup before validating `--append-system-prompt-file`, which proves the wrong thing. This gate is CI-safe only if the rootfs test can seed or copy a valid local Claude session record offline, without credentials, network, or a live model call. If the installed CLI's session format cannot be seeded offline, this parse check is reclassified as a manual/live release gate and must not be advertised as CI-safe. Either way, record the setup, command line, and exit message.
- **Resumed-turn / live model (REQUIRED before merge)**: in a separate live smoke, start a session with prompt A injecting a token T1, take one turn, then `--resume` the same session passing prompt B injecting a different token T2; assert the resumed turn observes T2. Merely proving that the flag does not error is not sufficient, because the CLI could accept but ignore `--append-system-prompt-file` on the resume path. The pinned outcome is recorded in the rootfs build/release gate alongside the first-turn evidence.

The recorded smoke must cover both first-turn and `--resume` because the bridge claim loop and the legacy stdin loop both pass `--append-system-prompt-file` on every invocation, including resumed turns; we will not depend on undocumented CLI behavior for the resume path. For the pinned CLI version, observing the resumed prompt token is mandatory. First-turn parse-only file-not-found/unknown-option checks are CI-safe. Resume parse-only checks are CI-safe only with an offline seeded session record; otherwise they are manual/live release evidence and cannot substitute for the live resumed-turn token check. If a future Claude Code release changes resume-time flag handling, the rootfs gate must catch it before the version is accepted.

The same checks found no documented `@file` expansion syntax. If a future implementation wants `@file` expansion, it must include a pinned Claude Code smoke test proving that the local CLI expands `@file` to file contents instead of injecting the literal path.

The rootfs must not install or reuse an unpinned Claude Code CLI while the plan depends on a version-sensitive flag. Change `sandbox-image/build-rootfs.sh` from `npm install -g @anthropic-ai/claude-code` to a pinned install, initially `@anthropic-ai/claude-code@2.1.150` or a `CLAUDE_CODE_VERSION=2.1.150` build arg used in that install command.

The existing-rootfs reuse branch (currently short-circuits before `require_root` in `sandbox-image/build-rootfs.sh:45`) must also assert the installed version before exiting. Reuse runs as an unprivileged user today, so the version assertion must be **non-chroot** to preserve that property:

- Resolve `${ROOTFS_DIR}/usr/local/bin/claude` from the host and follow the symlink to the installed Claude Code package when possible; if the symlink target is absolute, rebase it under `${ROOTFS_DIR}` before reading. The current rootfs installs under `/opt/node-v24.15.0/lib/node_modules/@anthropic-ai/claude-code/...` and symlinks `/usr/local/bin/claude` there. Read that package's `package.json` `version` directly from the host. This requires only filesystem read access, not `chroot` or `unshare`.
- If symlink resolution does not identify the package root, search both `${ROOTFS_DIR}/opt/node-*/lib/node_modules/@anthropic-ai/claude-code/package.json` and `${ROOTFS_DIR}/usr/lib/node_modules/@anthropic-ai/claude-code/package.json`. Equivalent fallbacks may parse the discovered `package.json` with `python3 -c` / `jq`, or run the discovered CLI JS with the host's `node`, as long as no privileged operation is needed.
- Do **not** introduce `chroot "${ROOTFS_DIR}" claude --version` in the reuse branch — that would silently require root for what is currently an unprivileged code path. If a future change genuinely needs in-rootfs execution, gate it explicitly behind `require_root` and document the regression.
- On mismatch, fail with an instruction such as `existing rootfs has Claude Code <got>; rerun with FORCE=1 or upgrade the rootfs to CLAUDE_CODE_VERSION=<want>`.

Add a release/build gate that runs **inside the freshly built rootfs** (where root and `chroot` are already available) and records `claude --version`, then proves `--append-system-prompt-file` is recognized by the rootfs binary. The first-turn parse gate is CI-safe: a missing prompt file must fail as file-not-found rather than `unknown option`. The resumed-turn parse gate is CI-safe only when the test can seed or copy a valid local Claude session record offline; otherwise it is manual/live release evidence. Every CLI version bump must rerun the applicable rootfs gates and update the recorded evidence.

### Legacy stdin Turn Loop

The entrypoint still has a non-claim-loop Claude path that builds argv with `set --` inside the per-turn loop and invokes Claude through `setpriv`. Keep this path consistent with the default bridge path:

```sh

while IFS= read -r turn; do
  set -- /usr/local/bin/claude \
    --bare \
    --permission-mode bypassPermissions \
    -p \
    --model "${CLAUDE_MODEL:-sonnet}" \
    --input-format stream-json \
    --output-format "${CLAUDE_OUTPUT_FORMAT:-stream-json}" \
    --include-partial-messages \
    --replay-user-messages \
    --verbose

  if [ "${HARNESS_SYSTEM_PROMPT_ENABLED:-0}" = "1" ]; then
    set -- "$@" --append-system-prompt-file "$HARNESS_SYSTEM_PROMPT_FILE"
  fi

  if [ "$first_turn" = "1" ] && [ "${CLAUDE_RESUME:-0}" != "1" ]; then
    set -- "$@" --session-id "$CLAUDE_SESSION_UUID"
  else
    set -- "$@" --resume "$CLAUDE_SESSION_UUID"
  fi

  printf '%s\n' "$turn" | setpriv ... sh "$@"
done
```

The text is written to a file inside the control mount (read-only from the agent's perspective) rather than passed as an env var, because env vars have OS-level length limits and the prompt is expected to grow.

## Failure Classification

Prompt manifest and sidecar validation failures are control-manifest validation failures: the manifest binds the prompt snapshot and expected delivery-artifact state, while the prepared artifacts, checkpoint metadata, or entrypoint-visible files do not match. The primary classified paths are host-side `PrepareGeneration` / artifact-render errors for new generations and prepared-artifact validation errors for existing generations, both before `Runtime.Start`. Update `orchestrator/internal/server/server.go` so host-side, restore-validation, and entrypoint-side messages classify as `manifest_digest_mismatch`:

```text
missing system prompt sidecar
stale system prompt sidecar
system prompt digest mismatch
missing system prompt manifest field
system prompt manifest field type mismatch
system prompt snapshot missing
system prompt snapshot mismatch
projected control manifest digest mismatch
checkpoint metadata mismatch: checkpoint_control_manifest_digest
checkpoint metadata missing: checkpoint_control_manifest_digest
```

Add `runtimeFailureClass` tests alongside the existing expected-field and secret-mount manifest mismatch cases. Also add server-level tests proving both a host-side sidecar verification error from `PrepareGeneration` and a prepared-artifact sidecar validation error fail the generation before `Runtime.Start` and record `manifest_digest_mismatch`.

Shell path (`harness-shell-agent`):

The shell shim doesn't talk to an LLM, so the system prompt does not apply directly. However, the shim should still expose the prompt to any sub-agent it spawns (`HARNESS_SYSTEM_PROMPT_FILE` is part of the env passed through). No injection at the shell shim layer in Phase 10a.

## Implementation Steps

1. Add `SystemPromptConfig` and the `SystemPrompt SystemPromptConfig` field with the `yaml:"system_prompt"` tag to `orchestrator/internal/config.Phase7Config`, with disabled/empty defaults.
2. Extend `validatePhase7Config` for `enabled=true` with `strings.TrimSpace(text) == ""`, preserving valid multi-line text exactly after validation.
3. Do **not** add prompt fields to `orchestrator/internal/runtime.Config`. Process config feeds the snapshot only at session creation in the server; the runtime accepts prompt fields only as per-request input.
4. Add store-level session prompt snapshot fields — `system_prompt_enabled`, `system_prompt_text`, `system_prompt_digest` columns on `sessions`, plus matching `store.Session` fields tagged `json:"-"` so they are never serialized through `writeJSON` or the events hub. Migration backfills existing rows to disabled/empty. Add a `store.GetSessionPromptSnapshot(ctx, sessionID)` accessor (or equivalent) for internal callers that should not see the rest of the row.
5. At session creation in `orchestrator/internal/server/server.go` (`createSession`, around `:268`), compute a normalized snapshot from `s.cfg.Phase7.SystemPrompt` and persist it as part of the same `CreateSession` call that writes the session row, before the response is returned and before any generation is allocated. Disabled config always snapshots to `false`, `""`, `""`; enabled config snapshots the exact configured text and its bare SHA-256 digest. For existing sessions, every later start, replacement-generation prepare, and cold fallback must read the stored snapshot via the new accessor instead of current config.
6. Pass the stored session prompt snapshot through `runtime.StartRequest` / `RuntimeGenerationDetails` as the manifest prompt source. The runtime layer must reject a request whose `system_prompt_enabled=true` but has `strings.TrimSpace(system_prompt_text) == ""` or missing `system_prompt_digest`, with stable error text (e.g. `system prompt snapshot missing`).
7. Extend control manifest generation to include the three new fields, using bare `digestHex` format for `system_prompt_digest`. Always emit all three keys (with empty/false defaults when disabled) so post-10a manifests have a stable shape for digest projection.
8. Update the projected digest projection in `orchestrator/internal/runtime/runtime.go` to include `system_prompt_enabled`, `system_prompt_text`, and `system_prompt_digest` as strict fields. Pre-10a prepared generations will automatically fall back on first use after upgrade; checkpointed generations use restore cold fallback, and active/idle prepared generations use the Phase 10a prepared-generation retirement path (see "Pre-10a Generation Compatibility").
9. In `renderGenerationArtifacts`, render the prompt text into the per-generation control directory as `system_prompt.txt`, with bytes exactly equal to the manifest's `system_prompt_text`. Source those bytes from the request snapshot only — never from `runtime.Config`.
10. In the same host artifact path, verify enabled sidecar existence, non-empty bytes, and SHA-256 digest before returning artifacts; when disabled, unlink any existing `system_prompt.txt` so reused control paths cannot retain stale prompt content.
11. Extend `runtimeDriver` / `Runtime` with `ValidatePreparedGenerationArtifacts(ctx, req)` and call it from `startEnsuredGeneration` for every non-new generation before `Runtime.Start`. This validation must run even when a container already exists, because `Runtime.Start` returns from the hot path before `generationArtifacts`. The validator must verify the prepared manifest wrapper digest, compare wrapper/projected digests to the stored generation digests, compare prompt fields to the persisted session snapshot on `RuntimeGenerationDetails`, validate enabled sidecars, and require disabled sidecars to be absent.
12. Route prepared-artifact validation failures by lifecycle state instead of using the generic pre-start helper. If validation fails while `ensured.RestoreFromCheckpoint=true`, call the P0 restore-fallback retirement helper so the already-claimed `restoring` generation is failed/reclaimable and the session cold-falls back instead of remaining stuck in restore state. If validation fails for a non-restore prepared generation, call a new retryable prepared-generation retirement helper that first tears down any live runtime for generation N, then fails/reclaims only generation N, does not call `store.FailSession` or publish terminal session failure, and makes `sendMessage` re-run `ensureActiveGeneration` to allocate and start generation N+1 for the same session. Keep the normal retryable start-failure helper for newly allocated generations; use the prepared-retirement helper only when already prepared artifacts or live runtime state must be retired.
13. Add a read-only bind mount or include the file in the existing `/harness-control` mount (current entrypoint already reads `/harness-control/session.json`; add the prompt file alongside it).
14. Update the entrypoint JSON manifest export to require `system_prompt_enabled`, `system_prompt_text`, and `system_prompt_digest` keys before Claude launch. Missing keys are an incompatible manifest and must fail with stable text such as `missing system prompt manifest field`, not silently default to disabled. Export `HARNESS_SYSTEM_PROMPT_ENABLED`, `HARNESS_SYSTEM_PROMPT_DIGEST`, and `HARNESS_SYSTEM_PROMPT_FILE`; do not export the prompt text.
15. Close the legacy `session.env` path and its producers explicitly. Either remove/env-reject `session.env` for Claude control files in Phase 10a, or require equivalent prompt env fields and run the same enabled/disabled sidecar verification before any bridge or legacy Claude launch. Update or deprecate `bundle/restore-sandbox.sh`, which currently writes `session.env` without prompt metadata, and keep `docs/architecture.md`, `docs/current-status.md`, and `docs/phase2-status.md` aligned with the chosen support status.
16. Keep entrypoint sidecar verification before any Claude-launch branch as defense in depth. Enabled manifests require a present digest-matching sidecar; disabled manifests require no `system_prompt.txt` sidecar. Required prompt metadata failures must echo stable classifier text such as `system prompt snapshot missing`; do not rely on shell-generated `${VAR:?message}` errors.
17. Pin the Claude Code CLI installed by `sandbox-image/build-rootfs.sh` to the version validated for `--append-system-prompt-file`, or introduce an equivalent build arg defaulting to that pinned version.
18. Update the existing-rootfs reuse branch in `sandbox-image/build-rootfs.sh` to assert the installed Claude version **without `chroot`**: resolve `${ROOTFS_DIR}/usr/local/bin/claude` to the installed package when possible, rebasing absolute symlink targets under `${ROOTFS_DIR}`; otherwise search `${ROOTFS_DIR}/opt/node-*/lib/node_modules/@anthropic-ai/claude-code/package.json` and `${ROOTFS_DIR}/usr/lib/node_modules/@anthropic-ai/claude-code/package.json`, or run the discovered JS entry with the host's `node`. Keep the reuse branch before `require_root`. Fail with a `FORCE=1`/upgrade instruction instead of silently reusing an old CLI.
19. Add a rootfs build/release gate that runs inside the built rootfs (where root and `chroot` are already permitted) and proves `--append-system-prompt-file` is recognized by the installed Claude binary on first-turn invocations. Add the `--resume <existing-session-uuid>` parse check only as a CI-safe gate if the test can seed or copy a valid local Claude session record offline, without credentials, network, or live model access. If the only way to create that session record is a live Claude invocation, classify the resume parse check as a manual/live release gate. In all cases, keep the separate live resumed-turn smoke where the pinned CLI actually observes the resume prompt token.
20. Update the default bridge Claude path in `sandbox-image/files/usr/local/bin/harness-bridge-client`: `ClaudeTurnRunner.run_turn` must append `["--append-system-prompt-file", prompt_file]` to `claude_command` whenever `HARNESS_SYSTEM_PROMPT_ENABLED=1`, before adding either `--session-id` or `--resume`.
21. Update the legacy entrypoint Claude argv construction inside the per-turn `set --` block to pass `--append-system-prompt-file "$HARNESS_SYSTEM_PROMPT_FILE"` on every invocation when enabled, including resumed turns. Avoid string-built `CLAUDE_ARGS`.
22. Update `orchestrator/internal/server/server.go` failure classification so `missing system prompt sidecar`, `stale system prompt sidecar`, `system prompt digest mismatch`, `missing system prompt manifest field`, `system prompt manifest field type mismatch`, `system prompt snapshot missing`, `system prompt snapshot mismatch`, `projected control manifest digest mismatch`, and the existing restore error texts `checkpoint metadata mismatch: checkpoint_control_manifest_digest` and `checkpoint metadata missing: checkpoint_control_manifest_digest` map to `manifest_digest_mismatch` from host artifact-render, prepared-artifact validation, restore validation, and entrypoint messages.
23. Keep Phase 7 manifest documentation and fixtures synchronized in the implementation patch. Until the fixture patch lands, Phase 7 restore documentation that mentions the prompt fields must label them as Phase 10a additions rather than implying the current Phase 7 canonical manifest fixture already contains them:
   - `docs/phase7/runtime-resources.md`: add the three prompt fields to the control-manifest field list as Phase 10a additions.
   - `docs/phase7/checkpoint-restore.md`: keep `system_prompt_enabled`, `system_prompt_text`, and `system_prompt_digest` in the strict-field documentation as **Phase 10a additions**, and keep the pre-10a fallback note aligned with "Pre-10a Generation Compatibility".
   - `docs/phase7/fixtures/control-manifest-payload.json`: add fixture values for the prompt fields.
   - `orchestrator/internal/runtime/runtime_test.go`: regenerate the canonical fixture string and expected digest.
   - `sandbox-image/tests/test_harness_bridge_client.py`: update the fixture digest expectation if it consumes the same canonical manifest fixture.
24. Tests:
   - `config_test.go`: `harness.system_prompt` decodes under the current `KnownFields(true)` `Phase7Config` loader.
   - `config_test.go`: enabled=true with empty or whitespace-only text -> validation error.
   - `store`: session prompt snapshot columns migrate with disabled/empty defaults and round-trip through `CreateSession` / `GetSession` / the new `GetSessionPromptSnapshot` accessor.
   - `store` or `server`: when config has `enabled=false` with non-empty `text`, the persisted snapshot is exactly `false`, `""`, `""`.
   - `store`: `Session.SystemPromptText` is omitted from `json.Marshal(session)` output (regression guard for the `json:"-"` tag).
   - `server`: session creation snapshots the current prompt config and persists it before any generation is allocated; mutate `s.cfg.Phase7.SystemPrompt` between `createSession` and the first `sendMessage`/prepare and assert the manifest still uses the original snapshot.
   - `server` API leak guard: decode `GET /sessions/{id}`, `GET /sessions`, and the `session.created` event payload after creating a session with a non-empty prompt; assert none of `system_prompt_enabled`, `system_prompt_text`, `system_prompt_digest` appear in the JSON.
   - `runtime` snapshot rejection: a `runtime.StartRequest` with `system_prompt_enabled=true` and empty/whitespace-only text or empty digest fails before any sidecar is written, returning the stable `system prompt snapshot missing` error rather than reading any process config.
   - `runtime` package: generated control manifest contains the three fields and the digest uses bare lowercase hex.
   - `runtime` package: sidecar bytes exactly match `system_prompt_text`, and sidecar SHA-256 equals `system_prompt_digest`.
   - `runtime` package: pre-create `system_prompt.txt`, render with `enabled=false`, and assert the stale sidecar is removed.
   - `server`: for every non-new generation, `ValidatePreparedGenerationArtifacts` runs before `Runtime.Start`; this includes an existing live-container hot path where `Runtime.Start` would otherwise return before `generationArtifacts`.
   - `server`: when `ValidatePreparedGenerationArtifacts` fails for `ensured.RestoreFromCheckpoint=true`, the error uses the P0 restore-fallback retirement helper: the claimed `restoring` generation becomes failed/reclaimable, checkpoint/restore session metadata is cleared, the session is left non-checkpointed and retryable, and cold fallback N+1 can be allocated. The test must assert the session does not remain stuck in `checkpointed`/`restoring` state merely because validation failed before `Runtime.Start`.
   - `server`: when `ValidatePreparedGenerationArtifacts` fails for a non-restore active/idle prepared generation, the server tears down any live runtime for generation N, fails/reclaims only generation N, leaves the session non-failed and input-eligible, does not publish terminal session failure, re-runs `ensureActiveGeneration`, and starts replacement generation N+1 for the same session.
   - `server` / bridge control plane: after a non-restore prepared-generation retirement, stale bridge polling, turn claiming, and output emission from generation N are rejected, while replacement generation N+1 can poll and emit normally.
   - `runtime` or `server`: prepared artifacts fail before `Runtime.Start` when the manifest wrapper digest is stale, when wrapper/projected digests differ from stored `ControlManifestDigest` / `ProjectedControlManifestDigest`, or when manifest prompt fields differ from the persisted session snapshot.
   - `runtime` or `server`: prepared manifest validation parses the wrapper payload as a raw map, requires raw presence and correct JSON types for all three `system_prompt_*` fields before any typed struct decode/defaulting, and reports a pre-10a missing field as `missing system prompt manifest field`.
   - `runtime` or `server`: prepared artifacts with an enabled manifest but missing/stale `system_prompt.txt` fail before `Runtime.Start`.
   - `runtime` or `server`: prepared artifacts with a disabled manifest/snapshot and a leftover `system_prompt.txt` fail before `Runtime.Start` with `stale system prompt sidecar`.
   - `server`: a pre-10a active/idle prepared generation whose manifest lacks `system_prompt_*` fields fails once through the Phase 10a prepared-generation retirement path and the replacement generation uses the same migrated session's disabled/empty prompt snapshot. This must not publish a terminal session failure, create a new session, or snapshot current operator config.
   - `runtime` projected-digest test: changing prompt text/digest changes the strict projected digest.
   - `runtime` fixture test: updated cross-language canonical manifest fixture matches the regenerated digest.
   - `rootfs` build/release gate:
     - Installed Claude Code version is pinned/recorded.
     - Existing-rootfs reuse branch rejects a mismatched version using the **non-chroot** check, with a unit-style script test that runs as a non-root user (no `chroot`/`unshare` invocations), covers the `/usr/local/bin/claude` symlink-to-`/opt/node-*` layout, and still detects a mismatch.
     - Inside the freshly built rootfs, the binary recognizes `--append-system-prompt-file` on a **first-turn** invocation (`-p --append-system-prompt-file /tmp/missing` fails as `Append system prompt file not found`, not `unknown option`).
     - If a valid local Claude session record can be seeded or copied offline, inside the freshly built rootfs the binary recognizes `--append-system-prompt-file` on a **`--resume <existing-session-uuid>`** invocation with the same file-not-found vs. unknown-option distinction. The test must document the offline seed source and assert it does not use credentials, network, or live model access.
     - If no offline seed is available for the installed CLI session format, the resumed-turn parse check is recorded as a manual/live release gate instead of a CI-safe gate. A nonexistent/random UUID remains invalid because it can fail before option validation.
     - The pinned Claude Code version has a recorded live resumed-turn smoke where the resumed turn observes the prompt-B token T2. A no-error result alone fails this gate.
   - `sandbox-image/tests/test_harness_bridge_client.py`: `ClaudeTurnRunner` appends `--append-system-prompt-file /harness-control/system_prompt.txt` to both first-turn and `--resume` Claude invocations in bridge claim-loop mode.
   - `entrypoint`: sidecar verification runs before `HARNESS_BRIDGE_MODE=claim-loop` execs `harness-bridge-client`.
   - `entrypoint`: enabled prompt appends `--append-system-prompt-file /harness-control/system_prompt.txt` to both first-turn and `--resume` Claude invocations in the legacy stdin loop.
   - `entrypoint`: JSON manifests missing any of `system_prompt_enabled`, `system_prompt_text`, or `system_prompt_digest` fail before Claude launch rather than defaulting to disabled.
   - `entrypoint`: legacy `session.env` either fails closed as deprecated for Claude launches, or requires equivalent prompt env metadata and applies the same missing/stale/mismatch sidecar checks before bridge and legacy Claude launch.
   - `entrypoint`: enabled `session.env` with missing `HARNESS_SYSTEM_PROMPT_FILE` or `HARNESS_SYSTEM_PROMPT_DIGEST` exits with explicit `system prompt snapshot missing` text, not a shell-generated required-variable error.
   - `bundle/restore-sandbox.sh`: chosen legacy-env policy is enforced. If `session.env` remains supported for Claude smoke tests, the script writes the prompt env metadata with disabled/empty defaults and sidecar behavior; if deprecated, the script fails closed or is documented and guarded as non-Claude/Phase-2-only. Keep `docs/architecture.md`, `docs/current-status.md`, and `docs/phase2-status.md` aligned in the same implementation patch.
   - `entrypoint`: missing or digest-mismatched sidecar exits before Claude launch.
   - `entrypoint`: a disabled manifest with leftover `/harness-control/system_prompt.txt` exits before Claude launch.
   - `server`: sidecar missing/stale/mismatch, missing or type-mismatched manifest field, snapshot missing/mismatch, projected manifest mismatch, and both `checkpoint metadata mismatch: checkpoint_control_manifest_digest` and `checkpoint metadata missing: checkpoint_control_manifest_digest` messages classify as `manifest_digest_mismatch`.
   - `server`: host-side artifact-render and prepared-artifact sidecar verification errors fail the generation before `Runtime.Start` when `WaitForTurn=false`.
   - `runtime`: when enabled=false, no prompt file is written and no flag is passed.
   - `checkpoint-restore`: restoring a post-10a session uses the session-pinned prompt snapshot even after process config changes, and strict projected digest checks still reject restore if the stored prompt fields do not match the checkpoint metadata.
   - `checkpoint-restore` legacy compatibility: a fixture pre-10a checkpoint manifest (lacking the three prompt fields) cold-falls-back rather than restoring, and the test asserts the replacement generation is allocated for the same migrated session using that session's stored disabled/empty prompt snapshot. It must not create a fresh session or use the current operator snapshot.

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

This guidance is also a candidate seed for a future Phase 10c skill (`doris-export.md`), but injecting it as a system prompt today is the lowest-cost mitigation.
