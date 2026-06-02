package runtime

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/store"
)

func TestBuildSandboxMountPlanUsesExactSandboxSurface(t *testing.T) {
	dir := t.TempDir()
	plan, err := BuildSandboxMountPlan(SandboxMountPlanInputs{
		Generation: store.RuntimeGenerationDetails{
			ControlDirPath: filepath.Join(dir, "run", "control", "gen-1"),
			BridgeDirPath:  filepath.Join(dir, "run", "bridge", "gen-1"),
		},
		WorkspaceHostPath: filepath.Join(dir, "sessions", "sess-1"),
		AgentHomeHostPath: filepath.Join(dir, "agent-homes", "sess-1", "sh"),
		NetworkHostsPath:  filepath.Join(dir, "run", "network", "gen-1", "hosts"),
		ContentSnapshots: []store.ContentSnapshotRecord{
			{
				Kind:                 store.ContentSnapshotKindSkills,
				Digest:               "sha256:skills",
				ImmutableHostPath:    filepath.Join(dir, "content", "skills", "sha256-skills"),
				MountDestination:     store.ContentSnapshotSkillsMount,
				SourceEvidenceDigest: "sha256:skills-source",
				RetentionClass:       "generation_plan",
			},
			{
				Kind:                 store.ContentSnapshotKindManagedSettings,
				Digest:               "sha256:settings",
				ImmutableHostPath:    filepath.Join(dir, "content", "managed-settings", "sha256-settings"),
				MountDestination:     store.ContentSnapshotManagedSettingsMount,
				SourceEvidenceDigest: "sha256:settings-source",
				RetentionClass:       "generation_plan",
			},
		},
	})
	if err != nil {
		t.Fatalf("build mount plan: %v", err)
	}

	mounts := plan.SpecMounts()
	assertMount(t, mounts, "/workspace", filepath.Join(dir, "sessions", "sess-1"), "rw", false)
	assertMount(t, mounts, "/agent-home", filepath.Join(dir, "agent-homes", "sess-1", "sh"), "rw", false)
	assertMount(t, mounts, "/harness-control", filepath.Join(dir, "run", "control", "gen-1"), "ro", true)
	assertMount(t, mounts, store.ContentSnapshotSkillsMount, filepath.Join(dir, "content", "skills", "sha256-skills"), "ro", true)
	assertMount(t, mounts, store.ContentSnapshotManagedSettingsMount, filepath.Join(dir, "content", "managed-settings", "sha256-settings"), "ro", true)
	bridgeMount := assertMount(t, mounts, bridge.BridgeMountDestination, filepath.Join(dir, "run", "bridge", "gen-1"), "rw", true)
	if bridgeMount.Annotations["dev.gvisor.spec.mount./harness-control/bridge.share"] != "exclusive" {
		t.Fatalf("bridge mount missing exclusive annotation: %+v", bridgeMount.Annotations)
	}
	assertMount(t, mounts, filepath.Join(bridge.BridgeMountDestination, bridge.InboxDir), bridge.HostOwnedPath(filepath.Join(dir, "run", "bridge", "gen-1"), bridge.InboxDir), "ro", true)
	assertMount(t, mounts, filepath.Join(bridge.BridgeMountDestination, bridge.HostTmpDir), bridge.HostOwnedPath(filepath.Join(dir, "run", "bridge", "gen-1"), bridge.HostTmpDir), "ro", true)
	if mountByDestination(mounts, filepath.Join(bridge.BridgeMountDestination, bridge.HostHeartbeatDir)) != nil {
		t.Fatalf("host heartbeat must remain host-only, not mounted into sandbox: %+v", mounts)
	}
	assertMount(t, mounts, "/etc/hosts", filepath.Join(dir, "run", "network", "gen-1", "hosts"), "ro", true)
	if mountByDestination(mounts, "/schema-pack") != nil {
		t.Fatalf("sandbox v2 mount plan must not auto-mount schema-pack: %+v", mounts)
	}

	forbidden := []string{"/sessions", "/agent-homes", "/harness-secrets"}
	for _, destination := range forbidden {
		if mountByDestination(mounts, destination) != nil {
			t.Fatalf("mount plan must not include forbidden destination %s: %+v", destination, mounts)
		}
	}
	for _, mount := range mounts {
		if mount.Type == "bind" && slices.Contains(mount.Options, "rbind") {
			t.Fatalf("mount plan bind must be exact, got recursive options for %+v", mount)
		}
	}
}

func TestBuildSandboxMountPlanRejectsUnsupportedContentSnapshotKind(t *testing.T) {
	dir := t.TempDir()
	_, err := BuildSandboxMountPlan(SandboxMountPlanInputs{
		Generation: store.RuntimeGenerationDetails{
			ControlDirPath: filepath.Join(dir, "run", "control", "gen-1"),
			BridgeDirPath:  filepath.Join(dir, "run", "bridge", "gen-1"),
		},
		WorkspaceHostPath: filepath.Join(dir, "sessions", "sess-1"),
		AgentHomeHostPath: filepath.Join(dir, "agent-homes", "sess-1", "sh"),
		ContentSnapshots: []store.ContentSnapshotRecord{
			{
				Kind:                 "workspace",
				Digest:               "sha256:workspace",
				ImmutableHostPath:    filepath.Join(dir, "content", "workspace", "sha256-workspace"),
				MountDestination:     "/workspace-content",
				SourceEvidenceDigest: "sha256:workspace-source",
				RetentionClass:       "generation_plan",
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported content snapshot kind") {
		t.Fatalf("expected unsupported content snapshot kind error, got %v", err)
	}
}

func TestBuildSandboxMountPlanRejectsContentSnapshotMountDestinationDrift(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct {
		name     string
		snapshot store.ContentSnapshotRecord
		want     string
	}{
		{
			name: "skills",
			snapshot: store.ContentSnapshotRecord{
				Kind:                 store.ContentSnapshotKindSkills,
				Digest:               "sha256:skills",
				ImmutableHostPath:    filepath.Join(dir, "content", "skills", "sha256-skills"),
				MountDestination:     "/other-skills",
				SourceEvidenceDigest: "sha256:skills-source",
				RetentionClass:       "generation_plan",
			},
			want: "content snapshot skills mount destination must be /harness-skills",
		},
		{
			name: "managed settings",
			snapshot: store.ContentSnapshotRecord{
				Kind:                 store.ContentSnapshotKindManagedSettings,
				Digest:               "sha256:settings",
				ImmutableHostPath:    filepath.Join(dir, "content", "managed-settings", "sha256-settings"),
				MountDestination:     "/other-managed-settings",
				SourceEvidenceDigest: "sha256:settings-source",
				RetentionClass:       "generation_plan",
			},
			want: "content snapshot managed_settings mount destination must be /harness-managed-settings",
		},
		{
			name: "skills whitespace",
			snapshot: store.ContentSnapshotRecord{
				Kind:                 store.ContentSnapshotKindSkills,
				Digest:               "sha256:skills",
				ImmutableHostPath:    filepath.Join(dir, "content", "skills", "sha256-skills"),
				MountDestination:     store.ContentSnapshotSkillsMount + " ",
				SourceEvidenceDigest: "sha256:skills-source",
				RetentionClass:       "generation_plan",
			},
			want: "content snapshot skills mount destination must be /harness-skills",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := BuildSandboxMountPlan(SandboxMountPlanInputs{
				Generation: store.RuntimeGenerationDetails{
					ControlDirPath: filepath.Join(dir, "run", "control", "gen-1"),
					BridgeDirPath:  filepath.Join(dir, "run", "bridge", "gen-1"),
				},
				WorkspaceHostPath: filepath.Join(dir, "sessions", "sess-1"),
				AgentHomeHostPath: filepath.Join(dir, "agent-homes", "sess-1", "sh"),
				ContentSnapshots:  []store.ContentSnapshotRecord{tc.snapshot},
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q, got %v", tc.want, err)
			}
		})
	}
}

func TestMountPlanRejectsForbiddenAndRecursiveBinds(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		name  string
		plan  MountPlan
		error string
	}{
		{
			name: "parent sessions destination",
			plan: MountPlan{Content: []MountPlanMount{{
				Name:        "bad",
				Destination: "/sessions",
				Type:        "bind",
				Source:      filepath.Join(dir, "sessions"),
				Mode:        "rw",
				Options:     []string{"bind", "rw"},
			}}},
			error: "forbidden",
		},
		{
			name: "recursive bind",
			plan: MountPlan{Content: []MountPlanMount{{
				Name:        "bad",
				Destination: "/workspace",
				Type:        "bind",
				Source:      filepath.Join(dir, "sessions", "sess-1"),
				Mode:        "rw",
				Options:     []string{"rbind", "rw"},
			}}},
			error: "not recursive",
		},
		{
			name: "unlisted content mount",
			plan: MountPlan{Content: []MountPlanMount{{
				Name:        "claude_home",
				Destination: "/root/.claude",
				Type:        "bind",
				Source:      filepath.Join(dir, "agent-homes", "sess-1", "claude_code"),
				Mode:        "ro",
				Options:     []string{"bind", "ro"},
			}}},
			error: "allow-list",
		},
		{
			name: "allow-listed name wrong destination",
			plan: MountPlan{Content: []MountPlanMount{{
				Name:        "workspace",
				Destination: "/workspace/private",
				Type:        "bind",
				Source:      filepath.Join(dir, "sessions", "sess-1"),
				Mode:        "rw",
				Options:     []string{"bind", "rw"},
			}}},
			error: "allow-list",
		},
		{
			name: "unlisted scratch mount",
			plan: MountPlan{Scratch: []MountPlanMount{{
				Name:        "cache",
				Destination: "/root/.cache",
				Type:        "tmpfs",
				Source:      "tmpfs",
				Mode:        "rw",
				Options:     []string{"nosuid", "nodev"},
			}}},
			error: "allow-list",
		},
		{
			name: "destination whitespace",
			plan: MountPlan{Content: []MountPlanMount{{
				Name:        "workspace",
				Destination: "/workspace ",
				Type:        "bind",
				Source:      filepath.Join(dir, "sessions", "sess-1"),
				Mode:        "rw",
				Options:     []string{"bind", "rw"},
			}}},
			error: "canonical absolute",
		},
		{
			name: "source whitespace",
			plan: MountPlan{Content: []MountPlanMount{{
				Name:        "workspace",
				Destination: "/workspace",
				Type:        "bind",
				Source:      filepath.Join(dir, "sessions", "sess-1") + " ",
				Mode:        "rw",
				Options:     []string{"bind", "rw"},
			}}},
			error: "canonical absolute",
		},
		{
			name: "duplicate destination",
			plan: MountPlan{Content: []MountPlanMount{
				{
					Name:        "workspace",
					Destination: "/workspace",
					Type:        "bind",
					Source:      filepath.Join(dir, "sessions", "sess-1"),
					Mode:        "rw",
					Options:     []string{"bind", "rw"},
				},
				{
					Name:        "workspace_duplicate",
					Destination: "/workspace",
					Type:        "bind",
					Source:      filepath.Join(dir, "sessions", "sess-2"),
					Mode:        "rw",
					Options:     []string{"bind", "rw"},
				},
			}},
			error: "duplicated",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.plan.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.error) {
				t.Fatalf("expected %q error, got %v", tt.error, err)
			}
		})
	}
}

func TestRuntimeAdapterPseudoMountsAreSeparateAllowList(t *testing.T) {
	mounts := RuntimeAdapterPseudoMounts()
	if err := ValidateRuntimeAdapterPseudoMounts(mounts); err != nil {
		t.Fatalf("validate runtime adapter pseudo mounts: %v", err)
	}
	want := []string{"/proc", "/dev", "/dev/pts", "/dev/shm", "/dev/mqueue", "/sys"}
	if len(mounts) != len(want) {
		t.Fatalf("pseudo mounts len=%d want %d: %+v", len(mounts), len(want), mounts)
	}
	for _, destination := range want {
		if mountByDestination(mounts, destination) == nil {
			t.Fatalf("missing pseudo mount %s: %+v", destination, mounts)
		}
	}
	for _, mount := range mounts {
		if mount.Type == "bind" {
			t.Fatalf("runtime adapter pseudo mounts must not bind host product data: %+v", mount)
		}
		if mount.Destination == "/workspace" ||
			mount.Destination == "/agent-home" ||
			mount.Destination == "/harness-control" ||
			mount.Destination == bridge.BridgeMountDestination {
			t.Fatalf("pseudo mount leaked product destination: %+v", mount)
		}
	}
}

func TestRuntimeAdapterPseudoMountValidationRejectsDrift(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]specMount) []specMount
		want   string
	}{
		{
			name: "tun device",
			mutate: func(mounts []specMount) []specMount {
				return append(mounts, specMount{
					Destination: "/dev/net/tun",
					Type:        "bind",
					Source:      "/dev/net/tun",
					Options:     []string{"bind", "rw"},
				})
			},
			want: "allow-list",
		},
		{
			name: "option drift",
			mutate: func(mounts []specMount) []specMount {
				for i := range mounts {
					if mounts[i].Destination == "/sys" {
						mounts[i].Options = []string{"nosuid", "noexec", "nodev"}
					}
				}
				return mounts
			},
			want: "options drift",
		},
		{
			name: "missing",
			mutate: func(mounts []specMount) []specMount {
				return mounts[:len(mounts)-1]
			},
			want: "missing",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mounts := tt.mutate(RuntimeAdapterPseudoMounts())
			err := ValidateRuntimeAdapterPseudoMounts(mounts)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %q error, got %v", tt.want, err)
			}
		})
	}
}

func assertMount(t *testing.T, mounts []specMount, destination, source, mode string, wantNoexec bool) *specMount {
	t.Helper()
	mount := mountByDestination(mounts, destination)
	if mount == nil {
		t.Fatalf("missing mount %s in %+v", destination, mounts)
	}
	if mount.Source != source {
		t.Fatalf("%s source=%q want %q", destination, mount.Source, source)
	}
	if !slices.Contains(mount.Options, "bind") || !slices.Contains(mount.Options, mode) {
		t.Fatalf("%s options missing exact bind/mode %s: %+v", destination, mode, mount.Options)
	}
	if !slices.Contains(mount.Options, "nosuid") || !slices.Contains(mount.Options, "nodev") {
		t.Fatalf("%s options missing nosuid/nodev: %+v", destination, mount.Options)
	}
	if wantNoexec && !slices.Contains(mount.Options, "noexec") {
		t.Fatalf("%s options missing noexec: %+v", destination, mount.Options)
	}
	return mount
}
