package config

import (
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
)

func normalizeModelProxyConfig(cfg ModelProxyConfig) ModelProxyConfig {
	cfg.BindURL = strings.TrimSpace(cfg.BindURL)
	if port, err := parseModelProxyBindPort(cfg.BindURL); err == nil {
		cfg.BindPort = port
	} else {
		cfg.BindPort = 0
	}
	cfg.SandboxBaseURL = strings.TrimSpace(cfg.SandboxBaseURL)
	return cfg
}

func validateModelProxyConfig(cfg ModelProxyConfig) error {
	bindPort, err := parseModelProxyBindPort(cfg.BindURL)
	if err != nil {
		return err
	}
	return validateModelProxySandboxBaseURL(cfg.SandboxBaseURL, bindPort)
}

func validateModelProxySandboxBaseURL(raw string, bindPort int) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fmt.Errorf("harness.model_proxy.sandbox_base_url is required")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("harness.model_proxy.sandbox_base_url is invalid: %w", err)
	}
	if parsed.Scheme != "http" {
		return fmt.Errorf("harness.model_proxy.sandbox_base_url must use http scheme")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("harness.model_proxy.sandbox_base_url must not include userinfo, query, or fragment")
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return fmt.Errorf("harness.model_proxy.sandbox_base_url must not include a path")
	}
	if strings.TrimSpace(parsed.Hostname()) == "" {
		return fmt.Errorf("harness.model_proxy.sandbox_base_url must include a host")
	}
	portRaw := parsed.Port()
	if portRaw == "" {
		return fmt.Errorf("harness.model_proxy.sandbox_base_url must include an explicit port matching bind_url")
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil || port <= 0 || port > 65535 {
		return fmt.Errorf("harness.model_proxy.sandbox_base_url contains invalid port %q", portRaw)
	}
	if port != bindPort {
		return fmt.Errorf("harness.model_proxy.sandbox_base_url port %d must match bind_url port %d", port, bindPort)
	}
	return nil
}

func parseModelProxyBindPort(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("harness.model_proxy.bind_url is required")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return 0, fmt.Errorf("harness.model_proxy.bind_url is invalid: %w", err)
	}
	if parsed.Scheme != "http" {
		return 0, fmt.Errorf("harness.model_proxy.bind_url must use http scheme")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return 0, fmt.Errorf("harness.model_proxy.bind_url must not include userinfo, query, or fragment")
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return 0, fmt.Errorf("harness.model_proxy.bind_url must not include a path")
	}
	host, portRaw, err := net.SplitHostPort(parsed.Host)
	if err != nil || strings.TrimSpace(portRaw) == "" {
		return 0, fmt.Errorf("harness.model_proxy.bind_url must include an explicit port")
	}
	if strings.Trim(strings.TrimSpace(host), "[]") == "" {
		return 0, fmt.Errorf("harness.model_proxy.bind_url must include a host")
	}
	if !isUnspecifiedModelProxyBindHost(host) {
		return 0, fmt.Errorf("harness.model_proxy.bind_url host must be an unspecified address")
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil || port <= 0 || port > 65535 {
		return 0, fmt.Errorf("harness.model_proxy.bind_url contains invalid port %q", portRaw)
	}
	return port, nil
}

func isUnspecifiedModelProxyBindHost(host string) bool {
	host = strings.Trim(strings.ToLower(strings.TrimSpace(host)), "[]")
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	return addr.IsUnspecified()
}
