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
                ("legacy_mount_centralization", "Runtime spec generation already centralizes mounts"),
            ),
        },
        {
            "name": "next_stage_skills_docs_pin_content_snapshots",
            "kind": "contains",
            "path": REPO_ROOT / "docs" / "next-stage.md",
            "patterns": (
                ("content_addressed_snapshot", "content-addressed snapshot"),
                ("no_mutable_repo_bind", "no mutable repo path mounted directly"),
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
                ("content_addressed_snapshot", "content-addressed snapshot"),
                ("no_credential_mcp_without_broker", "Credential-bearing MCP needs a later broker/token design"),
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
                ("obsolete_parent_mount_boundary", "the sandbox reaches"),
                ("obsolete_parent_mount_target", "parent `/sessions` and `/agent-homes` mounts"),
            ),
        },
        {
            "name": "current_architecture_uses_state_db_default",
            "kind": "lacks",
            "path": REPO_ROOT / "docs" / "architecture.md",
            "patterns": (("obsolete_db_under_sessions", "/var/lib/harness/sessions/orchestrator.db"),),
        },
        {
            "name": "bridge_client_has_no_pre_turn_model_probe_config",
            "kind": "lacks",
            "path": REPO_ROOT / "sandbox-image" / "files" / "usr" / "local" / "bin" / "harness-bridge-client",
            "patterns": (("pre_turn_model_probe_status_env", "HARNESS_PROBE_MESSAGE_STATUSES"),),
        },
        {
            "name": "frontend_session_types_hide_legacy_host_fields",
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
