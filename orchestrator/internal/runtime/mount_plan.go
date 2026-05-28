package runtime

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/store"
)

type MountPlan struct {
	Content []MountPlanMount
	Scratch []MountPlanMount
}

type MountPlanMount struct {
	Name        string
	Destination string
	Type        string
	Source      string
	Mode        string
	Options     []string
	Annotations map[string]string
}

type SandboxMountPlanInputs struct {
	Generation        store.RuntimeGenerationDetails
	WorkspaceHostPath string
	AgentHomeHostPath string
	NetworkHostsPath  string
	SchemaPackPath    string
}

type allowedMountPlanSurface struct {
	Destination string
	Type        string
	Mode        string
}

var contentMountPlanAllowList = map[string]allowedMountPlanSurface{
	"workspace":       {Destination: "/workspace", Type: "bind", Mode: "rw"},
	"agent_home":      {Destination: "/agent-home", Type: "bind", Mode: "rw"},
	"control":         {Destination: "/harness-control", Type: "bind", Mode: "ro"},
	"bridge":          {Destination: bridge.BridgeMountDestination, Type: "bind", Mode: "rw"},
	"bridge_inbox":    {Destination: filepath.Join(bridge.BridgeMountDestination, bridge.InboxDir), Type: "bind", Mode: "ro"},
	"bridge_host_tmp": {Destination: filepath.Join(bridge.BridgeMountDestination, bridge.HostTmpDir), Type: "bind", Mode: "ro"},
	"network_hosts":   {Destination: "/etc/hosts", Type: "bind", Mode: "ro"},
	"schema_pack":     {Destination: "/schema-pack", Type: "bind", Mode: "ro"},
}

var scratchMountPlanAllowList = map[string]allowedMountPlanSurface{
	"tmp":     {Destination: "/tmp", Type: "tmpfs", Mode: "rw"},
	"var_tmp": {Destination: "/var/tmp", Type: "tmpfs", Mode: "rw"},
}

func BuildSandboxMountPlan(input SandboxMountPlanInputs) (MountPlan, error) {
	details := input.Generation
	plan := MountPlan{
		Content: []MountPlanMount{
			exactBindMount("workspace", input.WorkspaceHostPath, "/workspace", "rw", []string{"bind", "rw", "nosuid", "nodev"}, nil),
			exactBindMount("agent_home", input.AgentHomeHostPath, "/agent-home", "rw", []string{"bind", "rw", "nosuid", "nodev"}, nil),
			exactBindMount("control", details.ControlDirPath, "/harness-control", "ro", []string{"bind", "ro", "nosuid", "nodev", "noexec"}, nil),
			exactBindMount("bridge", details.BridgeDirPath, bridge.BridgeMountDestination, "rw", []string{"bind", "rw", "nosuid", "nodev", "noexec"}, map[string]string{
				"dev.gvisor.spec.mount./harness-control/bridge.type":  "bind",
				"dev.gvisor.spec.mount./harness-control/bridge.share": "exclusive",
			}),
			exactBindMount("bridge_inbox", bridge.HostOwnedPath(details.BridgeDirPath, bridge.InboxDir), filepath.Join(bridge.BridgeMountDestination, bridge.InboxDir), "ro", []string{"bind", "ro", "nosuid", "nodev", "noexec"}, nil),
			exactBindMount("bridge_host_tmp", bridge.HostOwnedPath(details.BridgeDirPath, bridge.HostTmpDir), filepath.Join(bridge.BridgeMountDestination, bridge.HostTmpDir), "ro", []string{"bind", "ro", "nosuid", "nodev", "noexec"}, nil),
		},
		Scratch: []MountPlanMount{
			tmpfsMount("tmp", "/tmp"),
			tmpfsMount("var_tmp", "/var/tmp"),
		},
	}
	if strings.TrimSpace(input.NetworkHostsPath) != "" {
		plan.Content = append(plan.Content, exactBindMount("network_hosts", input.NetworkHostsPath, "/etc/hosts", "ro", []string{"bind", "ro", "nosuid", "nodev", "noexec"}, nil))
	}
	if strings.TrimSpace(input.SchemaPackPath) != "" {
		plan.Content = append(plan.Content, exactBindMount("schema_pack", input.SchemaPackPath, "/schema-pack", "ro", []string{"bind", "ro", "nosuid", "nodev", "noexec"}, nil))
	}
	if err := plan.Validate(); err != nil {
		return MountPlan{}, err
	}
	return plan, nil
}

func (p MountPlan) Validate() error {
	seenDestinations := map[string]string{}
	sections := []struct {
		name   string
		mounts []MountPlanMount
		allow  map[string]allowedMountPlanSurface
	}{
		{name: "content", mounts: p.Content, allow: contentMountPlanAllowList},
		{name: "scratch", mounts: p.Scratch, allow: scratchMountPlanAllowList},
	}
	for _, section := range sections {
		for _, mount := range section.mounts {
			if err := validateMountPlanMount(mount, seenDestinations); err != nil {
				return err
			}
			if err := validateMountPlanSurface(section.name, mount, section.allow); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateMountPlanSurface(section string, mount MountPlanMount, allow map[string]allowedMountPlanSurface) error {
	allowed, ok := allow[mount.Name]
	if !ok {
		return fmt.Errorf("mount plan %s mount %q is not in sandbox-isolation-v1 allow-list", section, mount.Name)
	}
	if mount.Destination != allowed.Destination {
		return fmt.Errorf("mount plan %s mount %q destination %q does not match allow-list destination %q", section, mount.Name, mount.Destination, allowed.Destination)
	}
	if mount.Type != allowed.Type {
		return fmt.Errorf("mount plan %s mount %q type %q does not match allow-list type %q", section, mount.Name, mount.Type, allowed.Type)
	}
	if mount.Mode != allowed.Mode {
		return fmt.Errorf("mount plan %s mount %q mode %q does not match allow-list mode %q", section, mount.Name, mount.Mode, allowed.Mode)
	}
	return nil
}

func validateMountPlanMount(mount MountPlanMount, seenDestinations map[string]string) error {
	if strings.TrimSpace(mount.Name) == "" {
		return fmt.Errorf("mount plan mount name is required")
	}
	if !filepath.IsAbs(mount.Destination) || filepath.Clean(mount.Destination) != mount.Destination {
		return fmt.Errorf("mount plan destination %q must be canonical absolute path", mount.Destination)
	}
	if forbiddenMountPlanDestination(mount.Destination) {
		return fmt.Errorf("mount plan destination %q is forbidden in sandbox-isolation-v1", mount.Destination)
	}
	if existing := seenDestinations[mount.Destination]; existing != "" {
		return fmt.Errorf("mount plan destination %q duplicated by %s and %s", mount.Destination, existing, mount.Name)
	}
	seenDestinations[mount.Destination] = mount.Name
	switch mount.Type {
	case "bind":
		if strings.TrimSpace(mount.Source) == "" || !filepath.IsAbs(mount.Source) || filepath.Clean(mount.Source) != mount.Source {
			return fmt.Errorf("mount plan bind source for %s must be canonical absolute path", mount.Name)
		}
		if mount.Source == string(filepath.Separator) {
			return fmt.Errorf("mount plan bind source for %s must not be filesystem root", mount.Name)
		}
		if slices.Contains(mount.Options, "rbind") {
			return fmt.Errorf("mount plan bind %s must be exact, not recursive", mount.Name)
		}
		if !slices.Contains(mount.Options, mount.Mode) {
			return fmt.Errorf("mount plan bind %s options must include mode %s", mount.Name, mount.Mode)
		}
	case "tmpfs":
		if mount.Source != "tmpfs" {
			return fmt.Errorf("mount plan tmpfs %s source must be tmpfs", mount.Name)
		}
	default:
		return fmt.Errorf("mount plan mount %s has unsupported type %q", mount.Name, mount.Type)
	}
	return nil
}

func (p MountPlan) SpecMounts() []specMount {
	mounts := make([]specMount, 0, len(p.Content)+len(p.Scratch))
	for _, mount := range append(append([]MountPlanMount{}, p.Content...), p.Scratch...) {
		mounts = append(mounts, specMount{
			Destination: mount.Destination,
			Type:        mount.Type,
			Source:      mount.Source,
			Options:     append([]string(nil), mount.Options...),
			Annotations: copyStringMap(mount.Annotations),
		})
	}
	return mounts
}

var runtimeAdapterPseudoMountAllowList = []specMount{
	{Destination: "/proc", Type: "proc", Source: "proc"},
	{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
	{Destination: "/dev/pts", Type: "devpts", Source: "devpts", Options: []string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"}},
	{Destination: "/dev/shm", Type: "tmpfs", Source: "shm", Options: []string{"nosuid", "noexec", "nodev", "mode=1777", "size=65536k"}},
	{Destination: "/dev/mqueue", Type: "mqueue", Source: "mqueue", Options: []string{"nosuid", "noexec", "nodev"}},
	{Destination: "/sys", Type: "sysfs", Source: "sysfs", Options: []string{"nosuid", "noexec", "nodev", "ro"}},
}

func RuntimeAdapterPseudoMounts() []specMount {
	mounts := make([]specMount, 0, len(runtimeAdapterPseudoMountAllowList))
	for _, mount := range runtimeAdapterPseudoMountAllowList {
		mounts = append(mounts, specMount{
			Destination: mount.Destination,
			Type:        mount.Type,
			Source:      mount.Source,
			Options:     append([]string(nil), mount.Options...),
			Annotations: copyStringMap(mount.Annotations),
		})
	}
	return mounts
}

func ValidateRuntimeAdapterPseudoMounts(mounts []specMount) error {
	expected := map[string]specMount{}
	for _, mount := range runtimeAdapterPseudoMountAllowList {
		expected[mount.Destination] = mount
	}
	seen := map[string]struct{}{}
	for _, mount := range mounts {
		want, ok := expected[mount.Destination]
		if !ok {
			return fmt.Errorf("runtime adapter pseudo mount %q is not in sandbox-isolation-v1 allow-list", mount.Destination)
		}
		if _, ok := seen[mount.Destination]; ok {
			return fmt.Errorf("runtime adapter pseudo mount %q is duplicated", mount.Destination)
		}
		seen[mount.Destination] = struct{}{}
		if mount.Type != want.Type || mount.Source != want.Source {
			return fmt.Errorf("runtime adapter pseudo mount %q type/source drift: got %s/%s want %s/%s", mount.Destination, mount.Type, mount.Source, want.Type, want.Source)
		}
		if !slices.Equal(mount.Options, want.Options) {
			return fmt.Errorf("runtime adapter pseudo mount %q options drift: got %v want %v", mount.Destination, mount.Options, want.Options)
		}
		if len(mount.Annotations) != 0 {
			return fmt.Errorf("runtime adapter pseudo mount %q must not carry annotations", mount.Destination)
		}
	}
	if len(seen) != len(expected) {
		for destination := range expected {
			if _, ok := seen[destination]; !ok {
				return fmt.Errorf("runtime adapter pseudo mount %q is missing", destination)
			}
		}
	}
	return nil
}

func exactBindMount(name, source, destination, mode string, options []string, annotations map[string]string) MountPlanMount {
	return MountPlanMount{
		Name:        name,
		Destination: destination,
		Type:        "bind",
		Source:      filepath.Clean(strings.TrimSpace(source)),
		Mode:        mode,
		Options:     append([]string(nil), options...),
		Annotations: copyStringMap(annotations),
	}
}

func tmpfsMount(name, destination string) MountPlanMount {
	return MountPlanMount{
		Name:        name,
		Destination: destination,
		Type:        "tmpfs",
		Source:      "tmpfs",
		Mode:        "rw",
		Options:     []string{"nosuid", "nodev", "mode=1777", "size=65536k"},
	}
}

func forbiddenMountPlanDestination(destination string) bool {
	switch destination {
	case "/sessions", "/agent-homes", "/harness-secrets":
		return true
	default:
		return false
	}
}

func copyStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
