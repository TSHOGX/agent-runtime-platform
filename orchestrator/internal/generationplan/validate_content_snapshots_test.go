package generationplan

import (
	"strings"
	"testing"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/store"
)

func TestRenderContentSnapshotsPayloadFreezesImmutableRefs(t *testing.T) {
	payload, err := RenderContentSnapshotsPayload([]store.ContentSnapshotRecord{
		{
			Kind:                 store.ContentSnapshotKindSkills,
			Digest:               "sha256:skills",
			ImmutableHostPath:    "/var/lib/harness/content/skills/sha256-skills",
			MountDestination:     "/harness-skills",
			SourceEvidenceDigest: "sha256:skills-source",
			RetentionClass:       "generation_plan",
		},
	})
	if err != nil {
		t.Fatalf("render content snapshots: %v", err)
	}
	if payload[store.ContentSnapshotKindManagedSettings] != nil {
		t.Fatalf("managed settings snapshot should remain nil: %+v", payload)
	}
	skills := payload[store.ContentSnapshotKindSkills].(map[string]any)
	if skills["kind"] != store.ContentSnapshotKindSkills ||
		skills["digest"] != "sha256:skills" ||
		skills["immutable_host_path"] != "/var/lib/harness/content/skills/sha256-skills" ||
		skills["mount_destination"] != "/harness-skills" ||
		skills["source_evidence_digest"] != "sha256:skills-source" ||
		skills["retention_class"] != "generation_plan" {
		t.Fatalf("unexpected skills snapshot payload: %+v", skills)
	}
	plan := validPlanPayload()
	plan["content_snapshots"] = payload
	addSkillsSnapshotMount(plan, skills)
	if err := Validate(ValidateParams{Payload: plan}); err != nil {
		t.Fatalf("rendered content snapshots should validate: %v", err)
	}
}

func TestRenderContentSnapshotsPayloadRejectsInvalidSelection(t *testing.T) {
	if _, err := RenderContentSnapshotsPayload([]store.ContentSnapshotRecord{
		{Kind: store.ContentSnapshotKindSkills, Digest: "sha256:skills", ImmutableHostPath: "/var/lib/harness/content/skills/sha256-skills", MountDestination: "/harness-skills", SourceEvidenceDigest: "sha256:skills-source", RetentionClass: "generation_plan"},
		{Kind: store.ContentSnapshotKindSkills, Digest: "sha256:skills2", ImmutableHostPath: "/var/lib/harness/content/skills/sha256-skills2", MountDestination: "/harness-skills", SourceEvidenceDigest: "sha256:skills-source2", RetentionClass: "generation_plan"},
	}); err == nil || !strings.Contains(err.Error(), "duplicate content snapshot kind") {
		t.Fatalf("expected duplicate snapshot rejection, got %v", err)
	}
	if _, err := RenderContentSnapshotsPayload([]store.ContentSnapshotRecord{
		{Kind: "workspace", Digest: "sha256:workspace", ImmutableHostPath: "/var/lib/harness/content/workspace/sha256-workspace", MountDestination: "/workspace-content", SourceEvidenceDigest: "sha256:workspace-source", RetentionClass: "generation_plan"},
	}); err == nil || !strings.Contains(err.Error(), "unsupported content snapshot kind") {
		t.Fatalf("expected unsupported snapshot rejection, got %v", err)
	}
	for _, tt := range []struct {
		name string
		edit func(*store.ContentSnapshotRecord)
		want string
	}{
		{
			name: "unclean host path",
			edit: func(record *store.ContentSnapshotRecord) {
				record.ImmutableHostPath = "/var/lib/harness/content/skills/../skills-current"
			},
			want: "content snapshot immutable_host_path must be canonical absolute",
		},
		{
			name: "host path whitespace",
			edit: func(record *store.ContentSnapshotRecord) {
				record.ImmutableHostPath = "/var/lib/harness/content/skills/sha256-skills "
			},
			want: "content snapshot immutable_host_path must be canonical absolute",
		},
		{
			name: "unclean mount destination",
			edit: func(record *store.ContentSnapshotRecord) {
				record.MountDestination = "/harness-skills/.."
			},
			want: "content snapshot mount_destination must be canonical absolute",
		},
		{
			name: "mount destination whitespace",
			edit: func(record *store.ContentSnapshotRecord) {
				record.MountDestination = "/harness-skills "
			},
			want: "content snapshot mount_destination must be canonical absolute",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			record := store.ContentSnapshotRecord{
				Kind:                 store.ContentSnapshotKindSkills,
				Digest:               "sha256:skills",
				ImmutableHostPath:    "/var/lib/harness/content/skills/sha256-skills",
				MountDestination:     "/harness-skills",
				SourceEvidenceDigest: "sha256:skills-source",
				RetentionClass:       "generation_plan",
			}
			tt.edit(&record)
			if _, err := RenderContentSnapshotsPayload([]store.ContentSnapshotRecord{record}); err == nil ||
				!strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %q, got %v", tt.want, err)
			}
		})
	}
}

func TestValidateRejectsMutableContentSnapshotReference(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(map[string]any)
		want string
	}{
		{
			name: "relative host path",
			edit: func(snapshot map[string]any) { snapshot["immutable_host_path"] = "relative/path" },
			want: "content_snapshots.skills.immutable_host_path must be canonical absolute",
		},
		{
			name: "unclean host path",
			edit: func(snapshot map[string]any) {
				snapshot["immutable_host_path"] = "/var/lib/harness/content/skills/../skills-current"
			},
			want: "content_snapshots.skills.immutable_host_path must be canonical absolute",
		},
		{
			name: "relative mount destination",
			edit: func(snapshot map[string]any) { snapshot["mount_destination"] = "harness-skills" },
			want: "content_snapshots.skills.mount_destination must be canonical absolute",
		},
		{
			name: "unclean mount destination",
			edit: func(snapshot map[string]any) { snapshot["mount_destination"] = "/harness-skills/.." },
			want: "content_snapshots.skills.mount_destination must be canonical absolute",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := validPlanPayload()
			contentSnapshots := payload["content_snapshots"].(map[string]any)
			snapshot := map[string]any{
				"kind":                   "skills",
				"digest":                 "sha256:skills",
				"immutable_host_path":    "/var/lib/harness/content/skills/sha256-skills",
				"mount_destination":      "/harness-skills",
				"source_evidence_digest": "sha256:source",
				"retention_class":        "active",
			}
			tc.edit(snapshot)
			contentSnapshots["skills"] = snapshot

			err := Validate(ValidateParams{Payload: payload})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q, got %v", tc.want, err)
			}
		})
	}
}

func TestValidateRejectsContentSnapshotKindMismatch(t *testing.T) {
	payload := validPlanPayload()
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	contentSnapshots["managed_settings"] = map[string]any{
		"kind":                   "skills",
		"digest":                 "sha256:settings",
		"immutable_host_path":    "/var/lib/harness/content/managed-settings/sha256-settings",
		"mount_destination":      "/harness-managed-settings",
		"source_evidence_digest": "sha256:source",
		"retention_class":        "active",
	}

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "content_snapshots.managed_settings.kind must be managed_settings") {
		t.Fatalf("expected content snapshot kind error, got %v", err)
	}
}

func TestValidateRejectsSkillsSnapshotMountDrift(t *testing.T) {
	payload := validPlanPayload()
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	contentSnapshots["skills"] = map[string]any{
		"kind":                   "skills",
		"digest":                 "sha256:skills",
		"immutable_host_path":    "/var/lib/harness/content/skills/sha256-skills",
		"mount_destination":      "/other-skills",
		"source_evidence_digest": "sha256:source",
		"retention_class":        "active",
	}

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "content_snapshots.skills.mount_destination must be /harness-skills") {
		t.Fatalf("expected skills mount destination error, got %v", err)
	}
}

func TestValidateRejectsManagedSettingsSnapshotMountDrift(t *testing.T) {
	payload := validPlanPayload()
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	contentSnapshots["managed_settings"] = map[string]any{
		"kind":                   "managed_settings",
		"digest":                 "sha256:settings",
		"immutable_host_path":    "/var/lib/harness/content/managed-settings/sha256-settings",
		"mount_destination":      "/other-managed-settings",
		"source_evidence_digest": "sha256:source",
		"retention_class":        "active",
	}

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "content_snapshots.managed_settings.mount_destination must be /harness-managed-settings") {
		t.Fatalf("expected managed settings mount destination error, got %v", err)
	}
}

func TestValidateRejectsUnsupportedContentSnapshotKey(t *testing.T) {
	payload := validPlanPayload()
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	contentSnapshots["workspace"] = nil

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "content_snapshots.workspace is unsupported") {
		t.Fatalf("expected content snapshot key error, got %v", err)
	}
}

func TestValidateRequiresSkillsSnapshotMountEvidence(t *testing.T) {
	payload := validPlanPayload()
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	skills := validSkillsSnapshotPayload()
	contentSnapshots["skills"] = skills

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "mounts.content_snapshots is required for skills content snapshot") {
		t.Fatalf("expected missing skills mount evidence error, got %v", err)
	}

	addSkillsSnapshotMount(payload, skills)
	mounts := payload["mounts"].(map[string]any)
	snapshotMounts := mounts["content_snapshots"].(map[string]any)
	snapshotMounts["skills"].(map[string]any)["digest"] = "sha256:changed"
	err = Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "mounts.content_snapshots.skills.digest mismatch") {
		t.Fatalf("expected skills mount digest mismatch, got %v", err)
	}
}

func TestValidateRequiresContentSnapshotMountScope(t *testing.T) {
	payload := validPlanPayload()
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	skills := validSkillsSnapshotPayload()
	contentSnapshots["skills"] = skills
	addSkillsSnapshotMount(payload, skills)
	workspace := payload["data_volumes"].(map[string]any)["workspace"].(map[string]any)
	workspace["platform_content_mount_scope"] = "none"

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "platform_content_mount_scope must be immutable_content_snapshots") {
		t.Fatalf("expected content snapshot mount scope error, got %v", err)
	}
}

func TestValidateRequiresManagedSettingsSnapshotMountEvidence(t *testing.T) {
	payload := validPlanPayload()
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	settings := validManagedSettingsSnapshotPayload()
	contentSnapshots["managed_settings"] = settings

	err := Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "mounts.content_snapshots is required for managed_settings content snapshot") {
		t.Fatalf("expected missing managed settings mount evidence error, got %v", err)
	}

	addManagedSettingsSnapshotMount(payload, settings)
	mounts := payload["mounts"].(map[string]any)
	snapshotMounts := mounts["content_snapshots"].(map[string]any)
	snapshotMounts["managed_settings"].(map[string]any)["mount_name"] = "settings_snapshot"
	err = Validate(ValidateParams{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "mounts.content_snapshots.managed_settings.mount_name mismatch") {
		t.Fatalf("expected managed settings mount name mismatch, got %v", err)
	}
}

func TestValidateRequiredContentSnapshotSelections(t *testing.T) {
	policy := map[string]any{
		string(agents.FeatureSkillsSnapshot):  string(agents.FeaturePolicyRequired),
		string(agents.FeatureManagedSettings): string(agents.FeaturePolicyDisabled),
	}
	snapshots := map[string]any{
		store.ContentSnapshotKindSkills:          nil,
		store.ContentSnapshotKindManagedSettings: nil,
	}

	err := validateRequiredContentSnapshotSelections(policy, snapshots)
	if err == nil || !strings.Contains(err.Error(), "content_snapshots.skills is required by feature_policy.skills_snapshot") {
		t.Fatalf("expected required skills snapshot error, got %v", err)
	}

	snapshots[store.ContentSnapshotKindSkills] = validSkillsSnapshotPayload()
	if err := validateRequiredContentSnapshotSelections(policy, snapshots); err != nil {
		t.Fatalf("required skills snapshot should validate: %v", err)
	}

	policy[string(agents.FeatureManagedSettings)] = string(agents.FeaturePolicyRequired)
	err = validateRequiredContentSnapshotSelections(policy, snapshots)
	if err == nil || !strings.Contains(err.Error(), "content_snapshots.managed_settings is required by feature_policy.managed_settings") {
		t.Fatalf("expected required managed settings snapshot error, got %v", err)
	}
}
