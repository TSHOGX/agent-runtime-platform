package store

import (
	"fmt"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

func (c ResourceAllocatorConfig) driverID() string {
	return strings.TrimSpace(c.DriverID)
}

func (c ResourceAllocatorConfig) outputFormat() string {
	return strings.TrimSpace(c.OutputFormat)
}

func (c ResourceAllocatorConfig) sandboxUID() int {
	return c.SandboxUID
}

func (c ResourceAllocatorConfig) sandboxGID() int {
	return c.SandboxGID
}

func (c ResourceAllocatorConfig) sandboxSupplementalGIDs() []int {
	if len(c.SandboxSupplementalGIDs) == 0 {
		return []int{}
	}
	out := append([]int(nil), c.SandboxSupplementalGIDs...)
	sort.Ints(out)
	return out
}

func (c ResourceAllocatorConfig) modelAccessAllowed() bool {
	return c.ModelAccessAllowed != nil && *c.ModelAccessAllowed
}

func (c ResourceAllocatorConfig) providerCredentialsHostOnly() bool {
	return c.ProviderCredentialsHostOnly
}

func (c ResourceAllocatorConfig) requiresNetworkHostsProjection() bool {
	if !c.providerCredentialsHostOnly() || !c.modelAccessAllowed() {
		return false
	}
	host := modelProxyBaseURLHost(c.sandboxModelProxyBaseURL())
	if host == "" {
		return false
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return false
	}
	return true
}

func (c ResourceAllocatorConfig) manifestAnthropicBaseURL(baseURL string) string {
	if !c.modelAccessAllowed() {
		return ""
	}
	if c.providerCredentialsHostOnly() {
		return c.sandboxModelProxyBaseURL()
	}
	return strings.TrimSpace(baseURL)
}

func (c ResourceAllocatorConfig) sandboxModelProxyBaseURL() string {
	return strings.TrimSpace(c.SandboxModelProxyBaseURL)
}

func (c ResourceAllocatorConfig) validateAllocationBoundary() error {
	if c.driverID() == "" {
		return fmt.Errorf("driver id is required")
	}
	if c.outputFormat() == "" {
		return fmt.Errorf("output format is required")
	}
	if c.sandboxUID() <= 0 {
		return fmt.Errorf("sandbox uid must be > 0")
	}
	if c.sandboxGID() <= 0 {
		return fmt.Errorf("sandbox gid must be > 0")
	}
	if err := c.validateSandboxSupplementalGIDs(); err != nil {
		return err
	}
	if c.hostProxyBindURL() == "" {
		return fmt.Errorf("host proxy bind url is required")
	}
	if c.proxyPort() <= 0 {
		return fmt.Errorf("proxy port must be > 0")
	}
	if c.ModelAccessAllowed == nil {
		return fmt.Errorf("model access allowed must be explicitly set")
	}
	if c.modelAccessAllowed() && strings.TrimSpace(c.Model) == "" {
		return fmt.Errorf("model is required when model access is enabled")
	}
	if err := c.validateSandboxModelProxyBaseURL(); err != nil {
		return err
	}
	return nil
}

func (c ResourceAllocatorConfig) validateSandboxSupplementalGIDs() error {
	seen := map[int]struct{}{}
	for _, gid := range c.SandboxSupplementalGIDs {
		if gid <= 0 {
			return fmt.Errorf("sandbox supplemental gids must contain only positive gids")
		}
		if _, ok := seen[gid]; ok {
			return fmt.Errorf("sandbox supplemental gids contains duplicate gid %d", gid)
		}
		seen[gid] = struct{}{}
	}
	return nil
}

func (c ResourceAllocatorConfig) validateSandboxModelProxyBaseURL() error {
	if !c.providerCredentialsHostOnly() || !c.modelAccessAllowed() {
		return nil
	}
	raw := c.sandboxModelProxyBaseURL()
	if raw == "" {
		return fmt.Errorf("sandbox model proxy base url is required when host-only model access is enabled")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("sandbox model proxy base url %q is invalid: %w", raw, err)
	}
	if parsed.Scheme != "http" {
		return fmt.Errorf("sandbox model proxy base url %q must use the local http proxy scheme", raw)
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("sandbox model proxy base url %q must not include userinfo, query, or fragment", raw)
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return fmt.Errorf("sandbox model proxy base url %q must not include a path", raw)
	}
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	if host == "" {
		return fmt.Errorf("sandbox model proxy base url %q must include a hostname alias", raw)
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return fmt.Errorf("sandbox model proxy base url %q must use a stable hostname alias, not an IP literal", raw)
	}
	if modelProxyHostIsHostLocal(host) {
		return fmt.Errorf("sandbox model proxy base url %q must not use a host-local name", raw)
	}
	if modelProxyHostIsProviderUpstream(host) {
		return fmt.Errorf("sandbox model proxy base url %q must not point at a provider upstream", raw)
	}
	portRaw := parsed.Port()
	if portRaw == "" {
		return fmt.Errorf("sandbox model proxy base url %q must include an explicit port matching proxy port %d", raw, c.proxyPort())
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil || port <= 0 || port > 65535 {
		return fmt.Errorf("sandbox model proxy base url %q contains invalid port %q", raw, portRaw)
	}
	if port != c.proxyPort() {
		return fmt.Errorf("sandbox model proxy base url %q port must match proxy port %d", raw, c.proxyPort())
	}
	return nil
}

func modelProxyBaseURLHost(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(parsed.Hostname()))
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

func (c ResourceAllocatorConfig) hostProxyBindURL() string {
	return strings.TrimSpace(c.HostProxyBindURL)
}

func (c ResourceAllocatorConfig) proxyPort() int {
	return c.ProxyPort
}
