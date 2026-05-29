# Snapshot Policy

> Parent: [Sandbox contract schema v2](../sandbox-contract-v2.md).

`snapshot_policy` distinguishes current checkpoint/restore behavior from
future fanout:

- `generation_checkpoint_restore`: current local runsc behavior.
- `base_branch_fanout`: future base-to-N child sandbox behavior.
- `pause_resume_only`: no checkpoint/branch guarantee.

Phase 9 must not infer fanout readiness from `snapshot_disk: true`. Fanout
requires `provider_supports_branch: true` and
`snapshot_semantic: base_branch_fanout`.

Snapshot policy fields are derived from the selected `RuntimeProviderSpec` and
its capability digest, not from allocator request input and not from rendered
runtime artifacts. Validation must recompute this projection before accepting a
v2 contract:

```text
provider_supports_snapshot_disk   = provider.capabilities["snapshot_disk"]
provider_supports_snapshot_memory = provider.capabilities["snapshot_memory"]
provider_supports_branch          = provider.capabilities["branch"]
branch_count_limit                = provider.snapshot.branch_count_limit
must_quiesce_processes            = provider.snapshot.must_quiesce_processes
stream_disconnects_on_snapshot    = provider.snapshot.stream_disconnects_on_snapshot
snapshot_semantic                 = provider.snapshot.snapshot_semantic
```

For `local_runsc` in Phase 9, the derived values are disk snapshot support
enabled, memory snapshot support disabled, branch support disabled,
`branch_count_limit = 0`, and
`snapshot_semantic = "generation_checkpoint_restore"`, with quiesce required
and snapshot streams disconnected. A non-zero `branch_count_limit` requires
`provider_supports_branch: true` and
`snapshot_semantic: "base_branch_fanout"`. `base_branch_fanout` is invalid
unless the provider spec declares `branch: true`; `generation_checkpoint_restore`
is invalid unless the provider spec declares `snapshot_disk: true`.
