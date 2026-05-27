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

type Phase8MountPlanInputs struct {
	Generation        store.RuntimeGenerationDetails
	WorkspaceHostPath string
	AgentHomeHostPath string
	NetworkHostsPath  string
	SchemaPackPath    string
}

func BuildPhase8MountPlan(input Phase8MountPlanInputs) (MountPlan, error) {
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
	for _, mount := range append(append([]MountPlanMount{}, p.Content...), p.Scratch...) {
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

func RuntimeAdapterPseudoMounts() []specMount {
	return []specMount{
		{Destination: "/proc", Type: "proc", Source: "proc"},
		{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
		{Destination: "/dev/pts", Type: "devpts", Source: "devpts", Options: []string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"}},
		{Destination: "/dev/shm", Type: "tmpfs", Source: "shm", Options: []string{"nosuid", "noexec", "nodev", "mode=1777", "size=65536k"}},
		{Destination: "/dev/mqueue", Type: "mqueue", Source: "mqueue", Options: []string{"nosuid", "noexec", "nodev"}},
		{Destination: "/sys", Type: "sysfs", Source: "sysfs", Options: []string{"nosuid", "noexec", "nodev", "ro"}},
	}
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
