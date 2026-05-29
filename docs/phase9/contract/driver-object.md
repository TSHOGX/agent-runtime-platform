# Driver Object

> Parent: [Sandbox contract schema v2](../sandbox-contract-v2.md).

The `driver` object replaces the v1 string `driver` field. It is the immutable
driver identity for the generation, not a UI label.

Required Phase 9a fields:

- `driver_id`
- `driver_version`
- `bridge_protocol`
- `output_schema`
- `command_argv_digest`
- `driver_config_digest`
- `required_runtime_capabilities_digest`
- `supports_interrupt`
- `supports_compaction`

`claude` is a retired legacy token. New v2 contracts must record the canonical
driver spec ID, `claude_code`, and validation rejects
`driver.driver_id: "claude"`.
`driver_config_digest` covers the canonical driver config input selected for
the generation. It is not the digest of a rendered `/harness-control` file.
`driver.bridge_protocol` names the driver-native runner/output protocol. It is
not the host/sandbox bridge queue protocol; 9d versions the bridge-queue
protocol separately through image-manifest evidence and `hello.payload`.

The 9a v2 validator recomputes and rejects mismatches for
`command_argv_digest`, `driver_config_digest`, and
`required_runtime_capabilities_digest` before a contract row is written. During
9a these checks use the hard-coded `claude_code`, `sh`, and `local_runsc`
fixtures; 9b moves the same facts into registries without changing bytes for
unchanged facts.

## Driver Digest Algorithms

`command_argv_digest` uses `driver_command_argv_digest_v1`. Despite the field
name, the digest covers the canonical sandbox command plan, not just one
unconditional argv. A driver with a stable launcher records one argv variant; a
driver whose invocation changes with driver state records every allowed argv
variant and the canonical state key that selects it. This is required for
Claude Code, whose per-turn command currently switches between a fresh-session
flag and a resume flag.

1. Build a canonical JSON object with sorted keys and no insignificant
   whitespace:

   ```json
   {
     "schema_version": 1,
     "driver_id": "<canonical-driver-id>",
     "driver_version": "<driver-version>",
     "command_plan": {
       "schema_version": 1,
       "variant_selector": "<canonical-driver-state-selector>",
       "argv_variants": [
         {
           "state_key": "<canonical-state-key>",
           "argv": ["<sandbox argv[0]>", "<arg1>"]
         }
       ]
     }
   }
   ```

2. Require the canonical `driver_id`. Driver aliases such as `claude` are
   rejected in the preimage.
3. Sort `argv_variants` by `state_key`. Each `state_key` is a canonical driver
   lifecycle state or selector output, not a UI label. If multiple runtime
   states intentionally execute the same argv, list the state keys explicitly
   or use a documented selector value that covers that set.
4. Preserve each `argv` order exactly as the runtime will execute it. Do not
   hash a shell-joined command string. Each argument is a UTF-8 JSON string
   after the host command builder has normalized sandbox paths and explicit
   mode flags.
5. Dynamic driver-state values in argv, such as a persisted Claude session ID,
   are represented by canonical placeholder strings that name their source,
   for example `${driver_state.session_uuid}`. The mutable value itself is
   covered by driver-state sidecar digests, not by this command-plan digest.
6. Do not include environment variables, rendered config file bytes, OCI spec
   bytes, host binary paths, or host roots. Those facts belong to driver config,
   runtime template, or sidecar artifact digests.
7. Any change to the launcher path, argv flags, argv ordering, placeholder
   source, allowed variant set, state key, or variant selector changes the
   digest. A runtime invocation is valid only if it matches one recorded
   variant for the selected driver state.
8. Prefix the canonical bytes with `driver_command_argv_digest_v1\n` and compute
   `sha256:<hex>`.

For 9a Claude Code, the command plan must include at least these two variants:
one fresh-session variant with `--session-id ${driver_state.session_uuid}` and
one resume variant with `--resume ${driver_state.session_uuid}`. The selector
must be derived from canonical driver state. In Phase 9 that means the
persisted Claude sidecar's `initialized` flag: `false` selects the
fresh-session variant, and `true` selects the resume variant. Same-process
turns, reconnects, and cold restarts must use the same sidecar-backed selector;
process-local `first_turn` state, `CLAUDE_RESUME`, and filesystem probing are
projection/runner compatibility only and must not override the persisted
selector. A change to that selector or to either flag is a command-plan digest
change.

`driver_config_digest` uses `driver_config_digest_v1`:

1. Build a canonical JSON object with sorted keys and no insignificant
   whitespace:

   ```json
   {
     "schema_version": 1,
     "driver_id": "<canonical-driver-id>",
     "driver_version": "<driver-version>",
     "config": {}
   }
   ```

2. `config` is the canonical driver config input selected for the generation
   before rendering `/harness-control/driver/<driver_id>/`. It includes
   driver-owned sandbox-visible settings such as selected model profile IDs,
   sandbox proxy provider IDs, native runner modes, and generated config
   descriptors. It excludes rendered file bytes, host paths, provider secrets,
   runtime resource identities, and mutable driver state.
3. Normalize aliases, enum values, and IDs to canonical strings. Object keys are
   deterministic. Arrays that are semantic sets are de-duplicated and sorted;
   arrays that are ordered driver inputs preserve schema-defined order.
4. Prefix the canonical bytes with `driver_config_digest_v1\n` and compute
   `sha256:<hex>`.

9a fixtures must cover the canonical `claude_code` command/config preimages,
including both the fresh-session and resume argv variants plus the
sidecar-backed initialized/uninitialized selector, and the canonical `sh`
command/config preimages. 9f adds Pi command/config fixtures before Pi is
selectable. Any changed launch argv, allowed state variant, variant selector,
or effective driver config must change the matching digest before a new
contract is written.

During 9a these fields come from a minimal hard-coded capability vocabulary for
`claude_code`, `sh`, and `local_runsc`. 9b promotes that vocabulary into the
driver and runtime-provider registries. The promotion must preserve digest
bytes for the same facts unless the capability vocabulary version intentionally
changes.
