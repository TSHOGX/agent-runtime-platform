# 9c: Generic Config and Frontend Product Surface

> Parent: [Phase 9 implementation slices](../implementation-slices.md).

Goal: move deployment choices to generic config, replace the temporary 9a/9b
legacy API adapter with product `mode`, and keep the product model as `Agent`
and `Shell`.

Deliverables:

1. Add `harness.agents.<id>`, `harness.model_profiles.<id>`, and
   `harness.runtime_providers.<id>`.
2. Replace legacy `claude:` config with `harness.agents.claude_code`.
3. Keep `harness.model_proxy.sandbox_base_url` as the only sandbox proxy alias;
   model profiles reference it by `proxy_ref`.
4. Replace `agent == "claude"` model-access and credential-posture checks with
   selected driver spec, model profile, runtime provider, and v2 grant facts.
5. Add durable `sessions.mode` as the public product surface. Keep
   `sessions.driver_id`, introduced in 9a, as the internal runtime selector.
   The 9c schema cutover may directly rebuild the sessions table and delete
   invalid pre-9c rows before public DTOs stop returning the legacy `agent`
   field. Valid rows are mapped deterministically:
   `driver_id = 'claude_code' -> mode: "agent"` and
   `driver_id = 'sh' -> mode: "shell"`. After 9f widens the driver slot,
   `driver_id = 'pi'` also maps to `mode: "agent"`. Rows with missing, legacy,
   or unknown `driver_id` values are deleted by the automatic cutover. If any
   deleted row owns active/checkpointed runtime resources or generation-owned
   filesystem paths, the 9c cutover must first run the same owner-lock
   cleanup/quarantine gate as 9a and preserve ownership rows until live
   resources are cleaned, proven absent, or durably quarantined. Rows
   whose canonical `driver_id` is valid but currently disabled in deployment
   config are not deleted by this cutover, are not reinterpreted against the
   current config/image, and keep validating active/checkpointed generations
   against their persisted allocation-time evidence. Purging valid rows for a
   disabled driver requires a separately named destructive reset gate with
   explicit provider cleanup/quarantine behavior.
   The 9c selection table is:

   | Public input | Persisted mode | Persisted driver_id |
   | --- | --- | --- |
   | omitted body, empty object, or missing `mode` | `agent` | `harness.default_agent` |
   | `mode: "agent"` | `agent` | `harness.default_agent` |
   | `mode: "shell"` | `shell` | `sh` |

   `harness.default_agent` must name an enabled non-shell agent driver, such as
   `claude_code` in 9c or `pi` after 9f. It must not be `sh`; a deployment that
   sets the Agent default to `sh`, an unknown driver, a disabled driver, or a
   driver without an Agent-capable spec fails config validation/startup.
6. Add a product-safe deployment capability API on the normal product server:
   `GET /api/deployment-capabilities`. It is separate from the 9b
   operator-only `/api/agents` catalog and must not expose raw driver IDs,
   host paths, image-manifest evidence, provider IDs, grant details, or
   driver-private state. The DTO shape is:

   ```json
   {
     "schema_version": 1,
     "default_mode": "agent",
     "session_modes": [
       {
         "mode": "agent",
         "label": "Agent",
         "visible": true,
         "create_enabled": true,
         "disabled_reason": null
       },
       {
         "mode": "shell",
         "label": "Shell",
         "visible": false,
         "create_enabled": false,
         "disabled_reason": "disabled"
       }
     ]
   }
   ```

   `mode` is limited to product values, currently `"agent"` and `"shell"`.
   `disabled_reason` is either `null` or one stable product-safe code such as
   `disabled`, `missing_from_image`, `provider_unsupported`,
   `default_unavailable`, or `operator_unavailable`. The backend computes this
   DTO through a single deployment-capability resolver fed by enabled driver
   specs, runtime-provider capabilities, the selected image manifest, and
   validated deployment defaults. `POST /api/sessions` must validate requested
   `mode` against the same resolver before inserting a session; allocation then
   re-validates against the same manifest/rootfs evidence before runtime
   creation. Unknown or unreadable current image capability state fails closed.
7. Shell is a deployment capability, not an unconditional product promise for
   every image. The product can expose `Shell` only when `sh` is enabled in
   config, present in the selected runtime provider/image manifest, and passes
   allocation-time manifest validation. If `sh` is absent or disabled, the
   frontend hides or disables Shell creation and the API rejects
   `mode: "shell"` with a capability/configuration error before persisting a
   session. Existing shell sessions validate against their persisted
   allocation-time manifest/rootfs evidence; they are not reinterpreted against
   the current image or deleted merely because `sh` is now disabled in current
   config. A deployable image does not have to include `sh`, but any deployment
   that advertises Shell must include and enable it.
8. Make `POST /api/sessions` input `mode: "agent" | "shell"`. Remove or reject
   the 9a/9b legacy `agent` compatibility input.
9. Make session DTOs and session-created events return `mode` and
   `mode_label`; do not return raw driver IDs, runtime generation IDs such as
   `active_generation_id`, restore IDs, or `agent` mirrors. Runtime generation
   IDs remain internal store/runtime identifiers, not product API concepts.
10. Add a public session-event DTO/sanitizer layer for persisted replay and
   hub delivery. It must remove top-level runtime `generation_id`, payload
   `generation_id`, payload `active_generation_id`, restore IDs, raw driver
   IDs, host paths, DataVolume evidence, and driver-private state from product
   events before JSON serialization. Store/runtime internals may keep
   generation IDs for CAS, dedupe, and recovery, but the public event shape must
   use product session fields such as `mode`, `mode_label`, `status`,
   `session_updated_at`, and `session_last_activity_at`. Events that currently
   use `active_generation_id` only to update the frontend's session state must
   either omit it or replace it with a product-safe session-status transition.
   The sanitizer is a shared boundary, not per-event ad hoc filtering.
11. Update the frontend to fetch `GET /api/deployment-capabilities`, create
   sessions by product mode, read product-mode DTO fields, and use the
   capability DTO to hide or disable Shell when the Shell mode is absent,
   invisible, or not create-enabled. The frontend must stop hard-coding the
   available mode list from local constants, stop storing public runtime
   generation IDs from session DTOs or events, and update reducers that consume
   `session.checkpoint_retired`, `session.restore_fallback_retired`, and similar
   lifecycle events to use the product-safe status/time fields instead of
   `active_generation_id`.
12. Parameterize sandbox image construction by deployed driver set for the
   current drivers before Pi-specific build support lands. `sh` remains the
   base runner path but must still have a manifest entry; Claude Code CLI is
   installed only when `claude_code` is selected. Unknown driver IDs and empty
   driver sets fail the build.
13. Generate `/etc/harness-image/agents.json` for the built Claude/shell image.
    Each entry records canonical `driver_id`, installed binary or runner path,
    pinned package/version facts when a driver CLI exists, file or package
    digest evidence, `bridge_protocol_version`, `turn_input_schema`, and the
    build input driver set. The manifest is generated from the same selected
    driver set used for installation; it is not hand edited after the rootfs is
    built.
14. Load `/etc/harness-image/agents.json` or its host-visible equivalent before
    allocation. Reject selected drivers absent from the image, including `sh`.
    From this gate onward, new v2 contracts write
    `contract_gate_version: "phase9c"`, carry a non-null
    `input_digests.agent_manifest_digest` and a non-null
    `input_digests.runtime_config_digest` computed from, and backed by, the
    allocation-time canonical source-config preimage described in
    [Input and Artifact Digests](../contract/input-and-artifact-digests.md).
    Existing `phase9a` generations with null image/runtime digests may remain
    valid only under the persisted `phase9a` validation profile. They are not
    manifest-backed generations and must not be advertised, reclassified, or
    silently upgraded as protocol-v2-capable evidence for later bridge gates.
    If the 9c rollout enables a product surface that depends on
    manifest-backed active/checkpointed continuity, it must either delete
    those missing-manifest `phase9a` generations during the automatic cutover
    under the owner-lock cleanup/quarantine gate, or leave them to the explicit
    9d missing-manifest reset gate before the v2 bridge requirement is enabled.

Gates:

- Deployment config expressed through `harness.agents.claude_code` maps to the
  same Claude Code behavior.
- Public DTOs expose product mode and hide raw driver IDs, host paths,
  DataVolume evidence, restore IDs, runtime generation IDs including
  `active_generation_id`, and driver-private state.
- Public event DTO/sanitizer tests prove live hub delivery and persisted replay
  have the same product-safe shape: no top-level `generation_id`, no payload
  `generation_id` or `active_generation_id`, no raw driver IDs, restore IDs,
  host paths, DataVolume evidence, or driver-private state. Tests must cover
  session-created, checkpoint-retired, restore-fallback-retired, terminal
  failure, and turn-completion event families, plus a regression fixture for
  any current store event that still persists runtime IDs internally.
- Frontend event reducer tests prove session lifecycle updates no longer depend
  on event `active_generation_id`; reducers use product-safe status/time fields
  and preserve existing public session state when runtime IDs are absent.
- Deployment capability API tests prove `GET /api/deployment-capabilities`
  returns only product-safe `mode`, label, visibility, create-enabled, and
  stable reason-code fields; never raw driver IDs, provider IDs, host paths,
  manifest evidence, grants, or driver-private state.
- Deployment capability behavior tests prove Shell is visible/creatable only
  when `sh` is enabled and present in the image manifest, hidden or disabled in
  the frontend when unavailable, and rejected by the API before persistence if
  `mode: "shell"` is requested without capability. The endpoint and
  `POST /api/sessions` must use the same backend resolver. These tests also
  prove Agent creation is unaffected by a deployment that intentionally omits
  `sh`.
- Runtime/allocation paths continue to select from `sessions.driver_id`, not
  the public `mode` label.
- Mode cutover tests start from 9a/9b-shaped rows and prove
  `claude_code -> agent`, `pi -> agent` after the 9f widening gate,
  `sh -> shell`, and missing/legacy/unknown driver IDs are deleted by the 9c
  cutover instead of receiving a guessed mode. When deleted rows own live
  runtime resources, tests prove cleanup inputs are captured and resources are
  cleaned, proven absent, or durably quarantined before ownership rows are
  removed. The tests also prove
  valid-but-disabled canonical driver rows are not deleted by the generic mode
  cutover and still validate active/checkpointed generations from persisted
  allocation evidence. Public DTOs and session-created events cannot switch to
  `mode` until every remaining session row has a non-null valid
  `sessions.mode`.
- Mode mapping tests cover omitted body, empty object, missing fields,
  canonical modes, and rejected `agent` inputs after the 9c cutover. They prove
  `mode: "shell"` always persists `driver_id: "sh"` regardless of
  `harness.default_agent` when Shell capability is available, requests for
  `mode: "shell"` fail before persistence when `sh` is disabled or missing from
  the image, `mode: "agent"` and omitted input select only an enabled non-shell
  Agent-capable driver, and `harness.default_agent: sh` fails config
  validation/startup.
- Non-Claude synthetic authorization tests prove model access no longer depends
  on `agent == "claude"`.
- Rootfs build tests cover `SANDBOX_AGENT_DRIVERS=claude_code`,
  `SANDBOX_AGENT_DRIVERS=sh`, and `SANDBOX_AGENT_DRIVERS=claude_code,sh`:
  Claude Code is absent from shell-only images, present and pinned in
  Claude-enabled images, unknown IDs fail, and the generated manifest entries
  match the installed driver facts.
- Image-manifest tests reject missing selected drivers, mismatched Claude CLI
  facts, and mismatched `sh` runner facts before allocation; existing
  generations validate against their persisted allocation-time manifest/rootfs
  evidence rather than the current image manifest.
- Runtime config digest tests fixture the exact canonical source-config
  preimage, prove the preimage/config generation is persisted as host-only
  allocation evidence, and prove the digest changes for new allocations when
  the selected agent config, model profile, runtime provider config, product
  mode, or deployment default selection changes. Existing active or
  checkpointed generations validate against their persisted allocation-time
  evidence, not current deployment config. Rendered control files, OCI specs,
  bundle paths, resource identities, and other rendered artifacts must not
  affect it. Fixture coverage must include `mode: "shell"` allocations where
  changing `harness.default_agent` changes
  `deployment_defaults.default_agent` and therefore the
  `runtime_config_digest`, while selected shell driver/runtime behavior remains
  `driver_id: "sh"`.
- Gate-version tests prove new writes are `contract_gate_version: "phase9c"`,
  `phase9c` contracts reject missing runtime-config and agent-manifest digests,
  reject missing rootfs digests when the provider exposes full rootfs/template
  content evidence, and existing `phase9a` rows are either validated under the
  pre-9c nullable-digest profile or deleted by an automatic destructive
  cutover. The loader must not silently upgrade a `phase9a` row to `phase9c`.
  Missing-manifest `phase9a` generations must be classified explicitly as
  non-manifest-backed; tests prove they are either deleted by a named 9c cutover
  before manifest-backed continuity is exposed, using the owner-lock
  cleanup/quarantine gate for active/checkpointed resources, or are left for
  the named 9d protocol-v2 reset gate, never treated as if protocol support
  were proven.
