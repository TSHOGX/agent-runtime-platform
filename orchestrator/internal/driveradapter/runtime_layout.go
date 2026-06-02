package driveradapter

import (
	"io/fs"
	"strings"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/store"
)

type RuntimeEnvVarSpec struct {
	Name  string
	Value string
}

type RuntimeHomeDirSpec struct {
	Label                 string
	AgentHomeRelativePath string
	Mode                  fs.FileMode
}

type RuntimeControlManifestSpec struct {
	Fields map[string]any
}

type RuntimeControlManifestRenderer func(store.RuntimeGenerationDetails) (map[string]any, error)

type RuntimeLayoutSpec struct {
	Env             []RuntimeEnvVarSpec
	HomeDirs        []RuntimeHomeDirSpec
	ControlManifest RuntimeControlManifestSpec
}

var runtimeControlManifestRenderers = map[agents.ID]RuntimeControlManifestRenderer{
	agents.ClaudeCode: renderClaudeCodeControlManifestFields,
}

var runtimeLayoutSpecs = map[agents.ID]RuntimeLayoutSpec{
	agents.Pi: {
		Env: []RuntimeEnvVarSpec{
			{Name: "PI_CODING_AGENT_DIR", Value: agents.PiCodingAgentDir},
			{Name: "PI_CODING_AGENT_SESSION_DIR", Value: agents.PiSessionDir},
			{Name: "PI_OFFLINE", Value: "1"},
			{Name: "PI_SKIP_VERSION_CHECK", Value: "1"},
			{Name: "PI_TELEMETRY", Value: "0"},
		},
		HomeDirs: []RuntimeHomeDirSpec{
			{Label: "pi root dir", AgentHomeRelativePath: ".pi", Mode: 0o750},
			{Label: "pi agent dir", AgentHomeRelativePath: ".pi/agent", Mode: 0o750},
			{Label: "pi session dir", AgentHomeRelativePath: ".pi/agent/sessions", Mode: 0o750},
		},
		ControlManifest: RuntimeControlManifestSpec{
			Fields: map[string]any{
				"pi_coding_agent_dir":         agents.PiCodingAgentDir,
				"pi_coding_agent_session_dir": agents.PiSessionDir,
				"pi_offline":                  true,
				"pi_skip_version_check":       true,
				"pi_telemetry_disabled":       true,
			},
		},
	},
}

func RuntimeLayoutSpecFor(driver agents.ID) (RuntimeLayoutSpec, bool) {
	spec, ok := runtimeLayoutSpecs[agents.ID(strings.TrimSpace(string(driver)))]
	if !ok {
		return RuntimeLayoutSpec{}, false
	}
	return cloneRuntimeLayoutSpec(spec), true
}

func RuntimeControlManifestFieldsFor(driver agents.ID, details store.RuntimeGenerationDetails) (map[string]any, error) {
	driver = agents.ID(strings.TrimSpace(string(driver)))
	fields := map[string]any{}
	if spec, ok := runtimeLayoutSpecs[driver]; ok {
		mergeRuntimeControlManifestFields(fields, spec.ControlManifest.Fields)
	}
	if renderer, ok := runtimeControlManifestRenderers[driver]; ok {
		rendered, err := renderer(details)
		if err != nil {
			return nil, err
		}
		mergeRuntimeControlManifestFields(fields, rendered)
	}
	if len(fields) == 0 {
		return nil, nil
	}
	return fields, nil
}

func renderClaudeCodeControlManifestFields(details store.RuntimeGenerationDetails) (map[string]any, error) {
	return map[string]any{
		"claude_code_disable_nonessential_traffic": details.DisableNonessentialTraffic,
	}, nil
}

func cloneRuntimeLayoutSpec(spec RuntimeLayoutSpec) RuntimeLayoutSpec {
	spec.Env = append([]RuntimeEnvVarSpec(nil), spec.Env...)
	spec.HomeDirs = append([]RuntimeHomeDirSpec(nil), spec.HomeDirs...)
	spec.ControlManifest.Fields = cloneRuntimeControlManifestFields(spec.ControlManifest.Fields)
	return spec
}

func mergeRuntimeControlManifestFields(dst, src map[string]any) {
	for key, value := range src {
		dst[key] = cloneRuntimeControlManifestValue(value)
	}
}

func cloneRuntimeControlManifestFields(fields map[string]any) map[string]any {
	if len(fields) == 0 {
		return nil
	}
	out := make(map[string]any, len(fields))
	mergeRuntimeControlManifestFields(out, fields)
	return out
}

func cloneRuntimeControlManifestValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneRuntimeControlManifestFields(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = cloneRuntimeControlManifestValue(item)
		}
		return out
	default:
		return value
	}
}
