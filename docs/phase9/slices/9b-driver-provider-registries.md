# 9b: Driver and Provider Registries

> Parent: [Phase 9 implementation slices](../implementation-slices.md).

Goal: promote 9a hard-coded driver/provider facts into registries without
changing behavior.

Deliverables:

1. Add a `DriverSpec` registry with canonical ID, label, kind, bridge protocol,
   output schema, required runtime capabilities, model access, process facts,
   and Phase 10 support declarations.
2. Register canonical drivers `claude_code` and `sh`. `claude` is not a
   registered alias in the clean Phase 9 architecture.
3. Add `RuntimeProviderSpec(local_runsc)` with capability vocabulary v1.
4. Enforce driver-required capabilities as a subset of provider capabilities
   before DataVolume provisioning, MountPlan generation, or runtime creation.
5. Add an operator-only `GET /api/agents` catalog. It may expose raw canonical
   driver IDs only across an internal/operator boundary, not through the
   frontend product API or public session DTO/event surface. Until Phase 11 adds
   user auth, the route is default-off on the normal product server. It can be
   registered only on an explicit admin listener, explicit operator-only server
   mode, or deployment wiring that is unreachable from the product origin; a
   same-origin product server must return 404 or 403 for normal product HTTP
   clients. This catalog is not the product deployment-capability API; 9c adds
   a separate product-safe endpoint for Agent/Shell creation modes.

Gates:

- Unsupported driver/provider pairs fail before allocation.
- Registry-computed capability digests match 9a fixture bytes for unchanged
  facts.
- `claude` is rejected in v2 contracts, `sessions.driver_id`,
  agent-runtime-profile canonical IDs, image manifests, and grant allowlists.
- Snapshot policy fields in v2 contracts are derived from the selected
  `RuntimeProviderSpec`, not accepted as independent allocator input:
  `provider_supports_snapshot_disk`,
  `provider_supports_snapshot_memory`, `provider_supports_branch`,
  `branch_count_limit`, and `snapshot_semantic` must match the provider spec
  and capability digest.
- Operator catalog tests directly call the normal product HTTP server and prove
  `GET /api/agents` returns 404 or 403 unless the admin listener/operator mode
  is explicitly enabled. Separate tests prove the frontend/product API path
  does not call it and product DTOs/events cannot consume or expose raw driver
  IDs from the operator catalog. 9c product capability tests must use the
  separate product-safe endpoint, not this operator catalog.
