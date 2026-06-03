package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestRecordGenerationRuntimeArtifactDigestsRequiresCompleteMetadata(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_artifact_metadata")
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_artifact_metadata",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    testAllocatorConfig(t),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	valid := GenerationRuntimeArtifactDigests{
		ControlManifestDigest:          "manifest_digest",
		ProjectedControlManifestDigest: "projected_manifest_digest",
		BundleDigest:                   "bundle_digest",
		RuntimeConfigDigest:            "runtime_config_digest",
		SpecDigest:                     "spec_digest",
		RunscVersion:                   "runsc test",
		RunscBinaryPath:                "/usr/local/bin/runsc-test",
		RunscBinaryDigest:              "sha256:runsc-test",
	}
	tests := []struct {
		name string
		want string
		edit func(*GenerationRuntimeArtifactDigests)
	}{
		{name: "control manifest", want: "control manifest digest", edit: func(d *GenerationRuntimeArtifactDigests) { d.ControlManifestDigest = "" }},
		{name: "projected manifest", want: "projected control manifest digest", edit: func(d *GenerationRuntimeArtifactDigests) { d.ProjectedControlManifestDigest = "" }},
		{name: "bundle", want: "bundle digest", edit: func(d *GenerationRuntimeArtifactDigests) { d.BundleDigest = "" }},
		{name: "runtime config", want: "runtime config digest", edit: func(d *GenerationRuntimeArtifactDigests) { d.RuntimeConfigDigest = "" }},
		{name: "spec", want: "spec digest", edit: func(d *GenerationRuntimeArtifactDigests) { d.SpecDigest = "" }},
		{name: "runsc version", want: "runsc version", edit: func(d *GenerationRuntimeArtifactDigests) { d.RunscVersion = "" }},
		{name: "runsc binary path", want: "runsc binary path", edit: func(d *GenerationRuntimeArtifactDigests) { d.RunscBinaryPath = "" }},
		{name: "relative runsc binary path", want: "runsc binary path must be canonical absolute", edit: func(d *GenerationRuntimeArtifactDigests) { d.RunscBinaryPath = "runsc" }},
		{name: "unclean runsc binary path", want: "runsc binary path must be canonical absolute", edit: func(d *GenerationRuntimeArtifactDigests) { d.RunscBinaryPath = "/usr/local/bin/../bin/runsc-test" }},
		{name: "runsc binary digest", want: "runsc binary digest", edit: func(d *GenerationRuntimeArtifactDigests) { d.RunscBinaryDigest = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			digests := valid
			tt.edit(&digests)
			err := st.RecordGenerationRuntimeArtifactDigests(ctx, allocation.GenerationID, digests)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("RecordGenerationRuntimeArtifactDigests error=%v want field %q", err, tt.want)
			}
		})
	}

	if err := st.RecordGenerationRuntimeArtifactDigests(ctx, allocation.GenerationID, valid); err != nil {
		t.Fatalf("record complete artifacts: %v", err)
	}
	partial := valid
	partial.ControlManifestDigest = "new_manifest_digest"
	partial.BundleDigest = ""
	if err := st.RecordGenerationRuntimeArtifactDigests(ctx, allocation.GenerationID, partial); err == nil {
		t.Fatalf("partial artifact update succeeded")
	}
	details, err := st.GetRuntimeGenerationDetails(ctx, "sess_artifact_metadata", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get runtime generation details: %v", err)
	}
	if details.ControlManifestDigest != valid.ControlManifestDigest ||
		details.BundleDigest != valid.BundleDigest ||
		details.RunscVersion != valid.RunscVersion {
		t.Fatalf("partial artifact update changed stored metadata: %+v", details)
	}
}

func TestGetRuntimeGenerationDetailsRejectsCorruptPathEvidence(t *testing.T) {
	tests := []struct {
		name      string
		updateSQL string
		value     string
		want      string
	}{
		{
			name:      "missing checkpoint path",
			updateSQL: `UPDATE runtime_generation_resources SET checkpoint_path = ? WHERE generation_id = ?`,
			value:     "",
			want:      "runtime generation checkpoint path is required",
		},
		{
			name:      "relative control dir",
			updateSQL: `UPDATE runtime_generation_resources SET control_dir_path = ? WHERE generation_id = ?`,
			value:     "control/gen-1",
			want:      "runtime generation control dir path must be canonical absolute",
		},
		{
			name:      "unclean control manifest",
			updateSQL: `UPDATE runtime_generation_resources SET control_manifest_path = ? WHERE generation_id = ?`,
			value:     "/var/lib/harness/run/control/../control/gen-1/session.json",
			want:      "runtime generation control manifest path must be canonical absolute",
		},
		{
			name:      "relative bundle dir",
			updateSQL: `UPDATE runtime_generation_resources SET bundle_dir_path = ? WHERE generation_id = ?`,
			value:     "runtime/gen-1",
			want:      "runtime generation bundle dir path must be canonical absolute",
		},
		{
			name:      "unclean spec path",
			updateSQL: `UPDATE runtime_generation_resources SET spec_path = ? WHERE generation_id = ?`,
			value:     "/var/lib/harness/run/runtime/gen-1/../gen-1/config.json",
			want:      "runtime generation spec path must be canonical absolute",
		},
		{
			name:      "relative bridge dir",
			updateSQL: `UPDATE runtime_generation_resources SET bridge_dir_path = ? WHERE generation_id = ?`,
			value:     "bridge/gen-1",
			want:      "runtime generation bridge dir path must be canonical absolute",
		},
		{
			name:      "unclean log dir",
			updateSQL: `UPDATE runtime_generation_resources SET log_dir_path = ? WHERE generation_id = ?`,
			value:     "/var/lib/harness/run/logs/../logs/gen-1",
			want:      "runtime generation log dir path must be canonical absolute",
		},
		{
			name:      "relative secrets dir",
			updateSQL: `UPDATE runtime_generation_resources SET secrets_dir_path = ? WHERE generation_id = ?`,
			value:     "control/gen-1/secrets",
			want:      "runtime generation secrets dir path must be canonical absolute",
		},
		{
			name:      "whitespace network hosts",
			updateSQL: `UPDATE runtime_generation_resources SET network_hosts_path = ? WHERE generation_id = ?`,
			value:     " /var/lib/harness/run/network/gen-1/hosts",
			want:      "runtime generation network hosts path must be canonical absolute",
		},
		{
			name:      "relative runsc binary",
			updateSQL: `UPDATE runtime_generation_resources SET runsc_binary_path = ? WHERE generation_id = ?`,
			value:     "runsc",
			want:      "runtime generation runsc binary path must be canonical absolute",
		},
		{
			name:      "unclean checkpoint runsc binary",
			updateSQL: `UPDATE runtime_generations SET checkpoint_runsc_binary_path = ? WHERE generation_id = ?`,
			value:     "/usr/local/bin/../bin/runsc-test",
			want:      "runtime generation checkpoint runsc binary path must be canonical absolute",
		},
		{
			name:      "relative netns path",
			updateSQL: `UPDATE network_profiles SET netns_path = ? WHERE generation_id = ?`,
			value:     "netns/harness-gen-1",
			want:      "runtime generation netns path must be canonical absolute",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, owner := openOwnedStore(t, ctx)
			sessionID := "sess_corrupt_paths_" + strings.ReplaceAll(tt.name, " ", "_")
			createStoreSession(t, ctx, st, sessionID)
			allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
				SessionID: sessionID,
				Owner:     GenerationLeaseOwner(owner.UUID),
				LeaseTTL:  time.Minute,
				Now:       time.Now().UTC(),
				Config:    testAllocatorConfig(t),
			})
			if err != nil {
				t.Fatalf("allocate generation: %v", err)
			}
			if _, err := st.GetRuntimeGenerationDetails(ctx, sessionID, allocation.GenerationID); err != nil {
				t.Fatalf("get clean runtime generation details: %v", err)
			}
			if _, err := st.db.ExecContext(ctx, tt.updateSQL, tt.value, allocation.GenerationID); err != nil {
				t.Fatalf("corrupt path evidence: %v", err)
			}
			_, err = st.GetRuntimeGenerationDetails(ctx, sessionID, allocation.GenerationID)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("GetRuntimeGenerationDetails error=%v want %q", err, tt.want)
			}
		})
	}
}
