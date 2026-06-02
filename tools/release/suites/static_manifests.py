#!/usr/bin/env python3
"""Static-check manifests (pure data).

These are file-pattern scans that any release can run without a host. The
full list is the source of truth for the sandbox_isolation suite's
``--static-only`` payload; the agent_capability suite exposes the next-stage
capability subset as a focused view (not a replacement).

Each spec is ``{"name", "kind": "lacks"|"contains", "path", "patterns"}``
where ``patterns`` is a tuple of ``(label, pattern)`` pairs. The generic
runner is ``engine.run_static_checks``.
"""
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


def _next_stage_checks():
    return [
        {
            "name": "next_stage_skills_docs_use_exact_bind_prerequisite",
            "kind": "lacks",
            "path": REPO_ROOT / "docs" / "next-stage.md",
            "patterns": (
                ("workspace_symlink_to_sessions", "`/workspace` is a symlink to `/sessions/<session_id>`"),
                ("agent_home_parent_root", "`/agent-homes/<session_id>`"),
                ("removed_mount_centralization", "Runtime spec generation already centralizes mounts"),
            ),
        },
        {
            "name": "next_stage_skills_docs_pin_content_snapshots",
            "kind": "contains",
            "path": REPO_ROOT / "docs" / "next-stage.md",
            "patterns": (
                ("content_addressed_snapshot", "content-addressed"),
                ("no_mutable_repo_bind", "mutable"),
            ),
        },
        {
            "name": "next_stage_managed_settings_do_not_reference_live_secret_mount",
            "kind": "lacks",
            "path": REPO_ROOT / "docs" / "next-stage.md",
            "patterns": (("existing_model_provider_secret_mount", "existing model-provider `/harness-secrets` mount"),),
        },
        {
            "name": "next_stage_managed_settings_docs_pin_content_snapshots",
            "kind": "contains",
            "path": REPO_ROOT / "docs" / "next-stage.md",
            "patterns": (
                ("content_addressed_snapshot", "content-addressed"),
                ("no_credential_mcp_without_broker", "Credential-bearing MCP"),
            ),
        },
        {
            "name": "next_stage_generation_plan_store_persists_immutable_rows",
            "kind": "contains",
            "path": REPO_ROOT / "orchestrator" / "internal" / "store" / "generation_plan.go",
            "patterns": (
                ("store_generation_plan", "func (s *Store) StoreGenerationPlan"),
                ("store_generation_plan_projection", "func (s *Store) StoreGenerationPlanProjection"),
                ("immutable_plan_payload_error", "generation plan already exists with different immutable payload"),
                ("immutable_projection_payload_error", "generation plan projection already exists with different immutable payload"),
            ),
        },
        {
            "name": "next_stage_projection_kinds_have_store_metadata",
            "kind": "contains",
            "path": REPO_ROOT / "orchestrator" / "internal" / "store" / "generation_plan.go",
            "patterns": (
                ("projection_version_constant", "const GenerationPlanProjectionVersion = 1"),
                ("projection_kinds_helper", "func GenerationPlanProjectionKinds"),
                ("projection_version_helper", "func GenerationPlanProjectionVersionFor"),
                ("projected_manifest_kind", "GenerationPlanProjectionControlManifestProjected"),
            ),
        },
        {
            "name": "next_stage_content_snapshots_store_persists_immutable_rows",
            "kind": "contains",
            "path": REPO_ROOT / "orchestrator" / "internal" / "store" / "content_snapshots.go",
            "patterns": (
                ("store_content_snapshot", "func (s *Store) StoreContentSnapshot"),
                ("list_content_snapshots", "func (s *Store) ListContentSnapshots"),
                ("immutable_snapshot_payload_error", "content snapshot already exists with different immutable payload"),
                ("skills_snapshot_kind", "ContentSnapshotKindSkills"),
                ("managed_settings_snapshot_kind", "ContentSnapshotKindManagedSettings"),
            ),
        },
        {
            "name": "next_stage_generation_plan_server_persists_shadow_launch_rows",
            "kind": "contains",
            "path": REPO_ROOT / "orchestrator" / "internal" / "server" / "server.go",
            "patterns": (
                ("shadow_generation_plan_payload", "shadowGenerationPlanPayload"),
                ("store_shadow_generation_plan", "storeShadowGenerationPlan"),
                ("store_generation_plan_call", "StoreGenerationPlan(ctx"),
                ("store_generation_plan_projection_call", "StoreGenerationPlanProjection(ctx"),
            ),
        },
        {
            "name": "next_stage_generation_plan_server_verifies_stored_projections",
            "kind": "contains",
            "path": REPO_ROOT / "orchestrator" / "internal" / "server" / "server.go",
            "patterns": (
                ("verify_stored_generation_plan_projections", "verifyStoredGenerationPlanProjections"),
                ("projection_expectations", "generationPlanProjectionExpectations"),
                ("verify_generation_plan_projections_call", "VerifyGenerationPlanProjections(ctx"),
            ),
        },
        {
            "name": "next_stage_generation_plan_validates_shadow_payloads",
            "kind": "contains",
            "path": REPO_ROOT / "orchestrator" / "internal" / "generationplan" / "validate.go",
            "patterns": (
                ("generation_plan_validate", "func Validate"),
                ("feature_policy_validation", "ValidateFeaturePolicy"),
                ("content_snapshot_validation", "validateContentSnapshots"),
                ("projection_digest_validation", "validateProjectionDigests"),
            ),
        },
        {
            "name": "next_stage_generation_plan_verifies_frozen_evidence",
            "kind": "contains",
            "path": REPO_ROOT / "orchestrator" / "internal" / "server" / "server.go",
            "patterns": (
                ("verify_frozen_evidence_hook", "verifyGenerationPlanFrozenEvidence"),
                ("verify_frozen_evidence_call", "VerifyFrozenEvidence"),
                ("checkpoint_bundle_digest", "CheckpointBundleDigest"),
                ("restore_from_checkpoint", "RestoreFromCheckpoint"),
            ),
        },
        {
            "name": "next_stage_capability_plane_uses_typed_feature_policy",
            "kind": "contains",
            "path": REPO_ROOT / "orchestrator" / "internal" / "agents" / "agents.go",
            "patterns": (
                ("feature_id_type", "type FeatureID string"),
                ("sub_capability_id_type", "type SubCapabilityID string"),
                ("driver_capabilities_type", "type DriverCapabilities struct"),
                ("runtime_provider_capabilities_type", "type RuntimeProviderCapabilities struct"),
                ("feature_policy_type", "type FeaturePolicy map[FeatureID]FeaturePolicyState"),
                ("validate_feature_policy", "func ValidateFeaturePolicy"),
            ),
        },
        {
            "name": "next_stage_driver_config_uses_adapter_renderer",
            "kind": "contains",
            "path": REPO_ROOT / "orchestrator" / "internal" / "runtime" / "runtime.go",
            "patterns": (
                ("driver_adapter_import", "internal/driveradapter"),
                ("config_projection_renderer_lookup", "ConfigProjectionRendererFor"),
            ),
        },
        {
            "name": "next_stage_driver_runtime_layout_uses_adapter",
            "kind": "contains",
            "path": REPO_ROOT / "orchestrator" / "internal" / "runtime" / "runtime.go",
            "patterns": (
                ("driver_adapter_import", "internal/driveradapter"),
                ("runtime_layout_adapter_lookup", "RuntimeLayoutSpecFor"),
            ),
        },
    ]


def sandbox_isolation_checks():
    """All 8 static checks, in the order the runtime-isolation harness emitted them."""
    return [
        {
            "name": "current_docs_do_not_claim_parent_session_mounts",
            "kind": "lacks",
            "path": REPO_ROOT / "docs" / "PLAN.md",
            "patterns": (
                ("removed_parent_mount_boundary", "the sandbox reaches"),
                ("removed_parent_mount_target", "parent `/sessions` and `/agent-homes` mounts"),
            ),
        },
        {
            "name": "current_architecture_uses_state_db_default",
            "kind": "lacks",
            "path": REPO_ROOT / "docs" / "architecture.md",
            "patterns": (("removed_db_under_sessions", "/var/lib/harness/sessions/orchestrator.db"),),
        },
        {
            "name": "bridge_client_has_no_pre_turn_model_probe_config",
            "kind": "lacks",
            "path": REPO_ROOT / "sandbox-image" / "files" / "usr" / "local" / "bin" / "harness-bridge-client",
            "patterns": (("pre_turn_model_probe_status_env", "HARNESS_PROBE_MESSAGE_STATUSES"),),
        },
        {
            "name": "frontend_session_types_hide_removed_host_fields",
            "kind": "lacks",
            "path": REPO_ROOT / "frontend" / "lib" / "types.ts",
            "patterns": (
                ("agent_home_path", "agent_home_path"),
                ("restore_id", "restore_id"),
            ),
        },
        *_next_stage_checks(),
    ]


def agent_capability_checks():
    """Next-stage capability subset view (not a replacement for the full list)."""
    return _next_stage_checks()
