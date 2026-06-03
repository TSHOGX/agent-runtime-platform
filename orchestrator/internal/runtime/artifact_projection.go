package runtime

import (
	"fmt"
	"net/netip"
	"net/url"
	"path/filepath"
	"strings"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/driveradapter"
	"harness-platform/orchestrator/internal/store"
)

type networkHostsProjection struct {
	Path    string
	Payload []byte
}

type driverConfigProjection struct {
	Entries  []DriverConfigMaterialization
	Payloads map[string][]byte
}

func (r *Runtime) writeNetworkHostsProjection(details store.RuntimeGenerationDetails) error {
	rendered, err := renderOptionalNetworkHostsProjection(details)
	if err != nil {
		return err
	}
	if strings.TrimSpace(rendered.Path) == "" {
		return nil
	}
	if err := writeFileAtomic(rendered.Path, rendered.Payload, 0o644); err != nil {
		return fmt.Errorf("write network hosts projection: %w", err)
	}
	return nil
}

func renderOptionalNetworkHostsProjection(details store.RuntimeGenerationDetails) (networkHostsProjection, error) {
	path := strings.TrimSpace(details.NetworkHostsPath)
	if path == "" {
		return networkHostsProjection{}, nil
	}
	if details.NetworkHostsPath != path || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return networkHostsProjection{}, fmt.Errorf("network hosts path %q must be canonical absolute path", details.NetworkHostsPath)
	}
	payload, err := renderNetworkHostsProjection(details)
	if err != nil {
		return networkHostsProjection{}, err
	}
	return networkHostsProjection{Path: path, Payload: payload}, nil
}

func (r *Runtime) renderDriverConfigProjection(req StartRequest) (driverConfigProjection, error) {
	driver := agents.ID(strings.TrimSpace(driverID(req)))
	specs := agents.DriverConfigMaterializationSpecsFor(driver)
	renderer, ok := driveradapter.ConfigProjectionRendererFor(driver)
	if len(specs) == 0 {
		if ok {
			return driverConfigProjection{}, fmt.Errorf("%s driver config materialization specs are missing", driver)
		}
		return driverConfigProjection{}, nil
	}
	if !ok {
		return driverConfigProjection{}, fmt.Errorf("%s driver config projection renderer is missing", driver)
	}
	details := req.Generation
	if err := validateRuntimeArtifactPathEvidence("driver config", "control dir path", details.ControlDirPath); err != nil {
		return driverConfigProjection{}, err
	}
	payloads, err := renderer(details)
	if err != nil {
		return driverConfigProjection{}, err
	}
	entries := make([]DriverConfigMaterialization, 0, len(specs))
	for _, spec := range specs {
		if _, ok := payloads[spec.Name]; !ok {
			return driverConfigProjection{}, fmt.Errorf("%s %s config renderer is missing", driver, spec.Name)
		}
		entries = append(entries, DriverConfigMaterialization{
			Name:                        spec.Name,
			SourceProjectionPath:        spec.SourceProjectionPath,
			HostSourcePath:              spec.HostSourcePath(details.ControlDirPath),
			SandboxDestination:          spec.SandboxDestination,
			DestinationMutableBySandbox: spec.DestinationMutableBySandbox,
		})
	}
	renderedPayloads := make(map[string][]byte, len(entries))
	for i := range entries {
		payload, err := canonicalJSON(payloads[entries[i].Name])
		if err != nil {
			return driverConfigProjection{}, fmt.Errorf("render %s %s config: %w", driver, entries[i].Name, err)
		}
		entries[i].SourceDigest = prefixedSHA256(payload)
		renderedPayloads[entries[i].Name] = payload
	}
	return driverConfigProjection{Entries: entries, Payloads: renderedPayloads}, nil
}

func (r *Runtime) writeDriverConfigProjection(req StartRequest) ([]DriverConfigMaterialization, error) {
	rendered, err := r.renderDriverConfigProjection(req)
	if err != nil {
		return nil, err
	}
	for _, entry := range rendered.Entries {
		payload := rendered.Payloads[entry.Name]
		if err := writeFileAtomic(entry.HostSourcePath, payload, 0o644); err != nil {
			return nil, fmt.Errorf("write %s %s config: %w", driverID(req), entry.Name, err)
		}
	}
	return rendered.Entries, nil
}

func renderNetworkHostsProjection(details store.RuntimeGenerationDetails) ([]byte, error) {
	host, err := modelProxyBaseURLHost(details.ManifestAnthropicBaseURL)
	if err != nil {
		return nil, err
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return nil, fmt.Errorf("network hosts projection requires a hostname alias, got %q", host)
	}
	gateway, err := netip.ParseAddr(strings.TrimSpace(details.HostGatewayIP))
	if err != nil {
		return nil, fmt.Errorf("network hosts projection requires host gateway ip: %w", err)
	}
	lines := []string{
		"127.0.0.1 localhost",
		"::1 localhost ip6-localhost ip6-loopback",
		fmt.Sprintf("%s %s", gateway.String(), host),
	}
	return []byte(strings.Join(lines, "\n") + "\n"), nil
}

func modelProxyBaseURLHost(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("invalid model proxy base url: %w", err)
	}
	if parsed.Scheme != "http" {
		return "", fmt.Errorf("model proxy base url must use the local http proxy scheme")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("model proxy base url must not include userinfo, query, or fragment")
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return "", fmt.Errorf("model proxy base url must not include a path")
	}
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	if host == "" || strings.ContainsAny(host, " \t\r\n/") {
		return "", fmt.Errorf("model proxy base url must include a hostname")
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return "", fmt.Errorf("model proxy base url must use a stable hostname alias, not an IP literal")
	}
	if modelProxyHostIsHostLocal(host) {
		return "", fmt.Errorf("model proxy base url must not use a host-local name")
	}
	if modelProxyHostIsProviderUpstream(host) {
		return "", fmt.Errorf("model proxy base url must not point at a provider upstream")
	}
	return host, nil
}

func modelProxyHostIsHostLocal(host string) bool {
	return host == "localhost" ||
		host == "host.docker.internal" ||
		strings.HasSuffix(host, ".localhost") ||
		strings.HasSuffix(host, ".local")
}

func modelProxyHostIsProviderUpstream(host string) bool {
	switch host {
	case "api.anthropic.com", "anthropic.com", "api.openai.com", "openai.com":
		return true
	default:
		return strings.HasSuffix(host, ".anthropic.com") || strings.HasSuffix(host, ".openai.com")
	}
}
