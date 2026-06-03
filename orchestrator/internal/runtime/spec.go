package runtime

import (
	"encoding/json"
	"fmt"
	"strings"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/driveradapter"
)

type runtimeSpec struct {
	OCIVersion string `json:"ociVersion"`
	Process    struct {
		Terminal        bool     `json:"terminal"`
		User            specUser `json:"user"`
		Args            []string `json:"args"`
		Env             []string `json:"env"`
		Cwd             string   `json:"cwd"`
		Capabilities    any      `json:"capabilities,omitempty"`
		Rlimits         any      `json:"rlimits,omitempty"`
		NoNewPrivileges bool     `json:"noNewPrivileges"`
	} `json:"process"`
	Root     specRoot        `json:"root"`
	Hostname string          `json:"hostname"`
	Mounts   []specMount     `json:"mounts"`
	Linux    json.RawMessage `json:"linux"`
}

type specUser struct {
	UID            int   `json:"uid"`
	GID            int   `json:"gid"`
	AdditionalGIDs []int `json:"additionalGids,omitempty"`
}

type specRoot struct {
	Path     string `json:"path"`
	Readonly bool   `json:"readonly"`
}

type specMount struct {
	Destination string            `json:"destination"`
	Type        string            `json:"type"`
	Source      string            `json:"source"`
	Options     []string          `json:"options,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

func (r *Runtime) renderRuntimeSpec(req StartRequest) (runtimeSpec, string, error) {
	driverSpec, err := runtimeDriverSpec(req)
	if err != nil {
		return runtimeSpec{}, "", err
	}
	return r.renderRuntimeSpecWithDriverSpec(req, driverSpec)
}

func runtimeDriverSpec(req StartRequest) (agents.DriverSpec, error) {
	selectedDriver := driverID(req)
	if selectedDriver == "" {
		return agents.DriverSpec{}, fmt.Errorf("driver id is required")
	}
	driverSpec, ok := agents.DriverSpecFor(selectedDriver)
	if !ok || !isSandboxIsolatedDriverSpec(driverSpec) {
		return agents.DriverSpec{}, fmt.Errorf("unsupported driver %q", selectedDriver)
	}
	return driverSpec, nil
}

func isSandboxIsolatedDriverSpec(spec agents.DriverSpec) bool {
	return spec.ID == agents.ClaudeCode || spec.ID == agents.Pi || spec.ID == agents.Shell
}

func (r *Runtime) renderRuntimeSpecWithDriverSpec(req StartRequest, driverSpec agents.DriverSpec) (runtimeSpec, string, error) {
	selectedDriver := driverID(req)
	if selectedDriver == "" {
		return runtimeSpec{}, "", fmt.Errorf("driver id is required")
	}
	if string(driverSpec.ID) != selectedDriver {
		return runtimeSpec{}, "", fmt.Errorf("generation driver mismatch")
	}
	if !isSandboxIsolatedDriverSpec(driverSpec) {
		return runtimeSpec{}, "", fmt.Errorf("unsupported driver %q", driverSpec.ID)
	}
	return r.renderSandboxIsolatedRuntimeSpec(req, driverSpec)
}

func (r *Runtime) renderSandboxIsolatedRuntimeSpec(req StartRequest, driverSpec agents.DriverSpec) (runtimeSpec, string, error) {
	var spec runtimeSpec
	details := req.Generation
	selectedDriver := string(driverSpec.ID)
	identity, err := r.requiredSandboxIdentity(details)
	if err != nil {
		return runtimeSpec{}, "", err
	}
	workspaceHostPath, agentHomeHostPath, err := r.sandboxIsolationDataPaths(req)
	if err != nil {
		return runtimeSpec{}, "", err
	}
	plan, err := BuildSandboxMountPlan(SandboxMountPlanInputs{
		Generation:        details,
		WorkspaceHostPath: workspaceHostPath,
		AgentHomeHostPath: agentHomeHostPath,
		NetworkHostsPath:  details.NetworkHostsPath,
		ContentSnapshots:  req.ContentSnapshots,
	})
	if err != nil {
		return runtimeSpec{}, "", err
	}
	bridgeProbeConfig, err := r.requiredBridgeProbeConfig()
	if err != nil {
		return runtimeSpec{}, "", err
	}
	spec.OCIVersion = "1.0.2"
	spec.Process.Terminal = false
	spec.Process.User = specUser{UID: identity.UID, GID: identity.GID, AdditionalGIDs: identity.SupplementalGIDs}
	spec.Process.Args = []string{"/usr/local/bin/harness-agent-entrypoint"}
	spec.Process.Env = []string{
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"LANG=C.UTF-8",
		"MPLCONFIGDIR=/tmp/matplotlib",
		"TMPDIR=/tmp",
		"HOME=/agent-home",
		"USER=harness",
		"LOGNAME=harness",
		"SESSION_WORKSPACE=/workspace",
		"HARNESS_AGENT_HOME=/agent-home",
		"HARNESS_DRIVER_ID=" + selectedDriver,
		"HARNESS_TURN_INPUT_SCHEMA=" + driverSpec.TurnInputSchema,
		fmt.Sprintf("HARNESS_BRIDGE_PROTOCOL_VERSION=%d", driverSpec.BridgeProtocolVersion),
		"HARNESS_EXPECTED_SESSION_ID=" + req.SessionID,
		"HARNESS_EXPECTED_GENERATION_ID=" + details.GenerationID,
		"HARNESS_EXPECTED_NETWORK_PROFILE_ID=" + details.NetworkProfileID,
		"HARNESS_EXPECTED_AGENT_RUNTIME_PROFILE_ID=" + details.AgentRuntimeProfileID,
		"HARNESS_EXPECTED_MANIFEST_VERSION=1",
		fmt.Sprintf("HARNESS_AGENT_UID=%d", identity.UID),
		fmt.Sprintf("HARNESS_AGENT_GID=%d", identity.GID),
		"HARNESS_BRIDGE_DIR=" + bridge.BridgeMountDestination,
		"HARNESS_BRIDGE_MODE=" + bridgeProbeConfig.bridgeMode,
		"HARNESS_BRIDGE_HEARTBEAT_INTERVAL=" + formatSeconds(bridgeProbeConfig.heartbeat),
		"HARNESS_BRIDGE_POLL_INTERVAL=" + formatSeconds(bridgeProbeConfig.pollInterval),
		"HARNESS_BRIDGE_IDLE_INTERVAL=" + formatSeconds(bridgeProbeConfig.pollInterval),
		"HARNESS_PROBE_HEALTHZ_STATUSES=" + joinInts(bridgeProbeConfig.healthzStatuses),
	}
	if layout, ok := driveradapter.RuntimeLayoutSpecFor(agents.ID(selectedDriver)); ok {
		spec.Process.Env = append(spec.Process.Env, driverRuntimeEnv(layout.Env)...)
	}
	spec.Process.Cwd = "/"
	spec.Process.Capabilities = emptyCapabilities()
	spec.Process.Rlimits = []map[string]any{{"type": "RLIMIT_NOFILE", "hard": 1024, "soft": 1024}}
	spec.Process.NoNewPrivileges = true
	spec.Root = specRoot{Path: r.rootFSPath(), Readonly: true}
	shortGenerationID, err := shortID(details.GenerationID)
	if err != nil {
		return runtimeSpec{}, "", err
	}
	spec.Hostname = "harness-gen-" + shortGenerationID
	pseudoMounts := RuntimeAdapterPseudoMounts()
	if err := ValidateRuntimeAdapterPseudoMounts(pseudoMounts); err != nil {
		return runtimeSpec{}, "", err
	}
	spec.Mounts = append(pseudoMounts, plan.SpecMounts()...)
	linux := map[string]any{
		"resources": map[string]any{
			"memory": map[string]any{"limit": 1073741824},
			"cpu":    map[string]any{"shares": 1024},
			"pids":   map[string]any{"limit": 256},
		},
		"namespaces": []map[string]any{
			{"type": "pid"},
			{"type": "ipc"},
			{"type": "uts"},
			{"type": "mount"},
		},
	}
	runscNetwork, err := r.runscNetwork(details)
	if err != nil {
		return runtimeSpec{}, "", err
	}
	if strings.EqualFold(runscNetwork, "sandbox") {
		if strings.TrimSpace(details.NetnsPath) == "" {
			return runtimeSpec{}, "", fmt.Errorf("sandbox generation requires netns path")
		}
		linux["namespaces"] = append(linux["namespaces"].([]map[string]any), map[string]any{"type": "network", "path": details.NetnsPath})
	}
	linuxBytes, err := canonicalJSON(linux)
	if err != nil {
		return runtimeSpec{}, "", err
	}
	spec.Linux = linuxBytes
	payload, err := canonicalJSON(spec)
	if err != nil {
		return runtimeSpec{}, "", err
	}
	return spec, digestHex(payload), nil
}

func emptyCapabilities() map[string]any {
	return map[string]any{
		"bounding":    []string{},
		"effective":   []string{},
		"inheritable": []string{},
		"permitted":   []string{},
		"ambient":     []string{},
	}
}

func driverRuntimeEnv(vars []driveradapter.RuntimeEnvVarSpec) []string {
	env := make([]string, 0, len(vars))
	for _, item := range vars {
		env = append(env, strings.TrimSpace(item.Name)+"="+item.Value)
	}
	return env
}
