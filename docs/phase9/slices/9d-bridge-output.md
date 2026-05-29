# 9d: Bridge and Output Refactor

> Parent: [Phase 9 implementation slices](../implementation-slices.md).

Goal: remove host-side driver-specific turn framing and parser branching.

9d owns the full bridge-runner replacement. The narrower 9a safety patch that
makes the current Claude runner honor the sidecar-derived fresh/resume selector
is a prerequisite, not the 9d registry refactor.

Deliverables:

1. Replace sandbox `make_turn_runner(agent)` with an `AgentRunner` registry.
2. Add versioned host/sandbox bridge protocol negotiation before changing turn
   input. `hello.payload.protocol_version` must be `2` for the `RunTurn`
   protocol and the payload must include canonical `driver_id` plus
   `turn_input_schema: "RunTurn"`. The host must validate the hello payload
   against the generation's persisted allocation-time image manifest evidence
   before it marks hello complete, grants work, or accepts resume.
   This is the point where the protocol-v1 control-manifest compatibility
   fields kept by 9a-9c (`agent`, Claude resume/session fields, and
   traffic-disable flags) can be removed from sandbox-visible projection.
3. Change host-to-sandbox input to driver-neutral `RunTurn`. `RunTurn` is
   available only to bridge protocol v2 images whose manifest evidence says
   they support the v2 turn-input schema.
4. Replace `stream_parser.go` branching with an `OutputNormalizer` registry.
5. Add a test-only `native_events_probe` runner to exercise
   `harness_native_events_v1` over `emit_output.payload`. The probe is not a
   product mode and is not returned by the deployment catalog.
6. Keep `ack_turn_completed` for terminal status and optional
   `driver_state_update`; native output events must be emitted before
   completion and deduped by `output_sequence`. The failure behavior in
   [driver-state.md](../driver-state.md) is normative: if validation or
   sidecar CAS fails during completion after output has already been emitted,
   retained output is followed by a host-authored generation/session failure,
   no `ack_turn_completed` is persisted, and the failed generation accepts no
   further output or completion envelopes.
7. Update image-manifest evidence and digest fixtures for bridge protocol v2.
   The manifest must record the bridge protocol version and turn-input schema
   supported by each installed driver entry. Existing active/checkpointed
   generations whose persisted manifest proves only protocol v1, plus any
   older `phase9a` active/checkpointed generation with missing/null
   image-manifest evidence, must be deleted by the automatic release cutover or
   explicitly failed before the host requires protocol v2. That reset uses the
   same owner-lock cleanup/quarantine rule as 9a: cleanup inputs are captured
   durably, live runsc/netns/veth/nft, bridge/control, bundle, checkpoint, log,
   workspace, and driver-home resources are cleaned, proven absent, or durably
   quarantined before DB ownership rows are removed, and a non-quarantined live
   cleanup failure leaves the reset marker in place and blocks the v2 host
   requirement. Missing manifest evidence is incompatible with the v2 bridge
   gate; it is not implicit proof of either v1 or v2 support. After that reset,
   reconnect/restore fail closed for any remaining protocol-v1 or
   missing-manifest evidence with an explicit incompatibility reason.

Gates:

- Release-cutover tests seed valid 9c generations whose persisted image
  manifests prove only bridge protocol v1 and prove the 9d rollout can delete
  them before enabling the protocol-v2 host requirement. The same tests seed
  active/checkpointed `phase9a` generations with null or missing manifest
  evidence and prove the 9d reset deletes or explicitly fails them before the
  v2 host requirement is enabled. They also prove cleanup inputs are captured
  before row deletion, live resources are cleaned, proven absent, or durably
  quarantined, and a non-quarantined live cleanup failure blocks rollout. The
  rollout must fail if any v1 or missing-manifest active/checkpointed
  generation remains after the automatic reset.
- Bridge hello tests reject missing protocol fields, unknown versions, selected
  driver mismatch, and protocol v1 clients after the v2 cutover; rejected
  clients receive no turn grants and cannot resume leased work.
- Restore tests fail before invoking runsc when the checkpointed generation's
  persisted image manifest does not prove the bridge protocol required by the
  current host, including the case where manifest evidence is absent because
  the generation was written under the `phase9a` nullable-digest profile.
- Claude/shell public normalized event output remains unchanged.
- Raw bridge queue records may change only if public normalized events stay
  unchanged.
- Unknown native event types fail closed.
- Probe tests validate schema tagging, `emit_output.payload`, and
  `output_sequence` dedupe anchors.
- Completion-failure tests emit native output, then force driver-state
  validation failure and sidecar CAS failure. They prove retained output
  replays once, mismatched duplicate `output_sequence` fails closed, no
  `ack_turn_completed` is persisted, driver-private state is absent from public
  events, and the generation is failed or retired before any further output is
  accepted.
