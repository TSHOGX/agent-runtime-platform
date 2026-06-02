package store

import (
	"fmt"
	"net/netip"
	"strings"
)

func allowedEgressRules(hostGatewayIP string, cfg ResourceAllocatorConfig) []string {
	rules := []string{fmt.Sprintf("tcp:%s:%d", hostGatewayIP, cfg.proxyPort())}
	for _, host := range cfg.EgressDorisFEHosts {
		for _, port := range cfg.EgressDorisPorts {
			rules = append(rules, fmt.Sprintf("tcp:%s:%d", host, port))
		}
	}
	for _, host := range cfg.EgressDorisBEHosts {
		for _, port := range cfg.EgressDorisPorts {
			rules = append(rules, fmt.Sprintf("tcp:%s:%d", host, port))
		}
	}
	if egressAllowsDNS(cfg) {
		rules = append(rules, "udp:53", "tcp:53")
	}
	return rules
}

func egressAllowsDNS(cfg ResourceAllocatorConfig) bool {
	switch cfg.EgressDNSPolicy {
	case "always":
		return true
	case "hostnames_only":
		return egressUsesHostnames(cfg)
	default:
		return false
	}
}

func egressUsesHostnames(cfg ResourceAllocatorConfig) bool {
	return containsHostname(cfg.EgressDorisFEHosts) || containsHostname(cfg.EgressDorisBEHosts)
}

func containsHostname(hosts []string) bool {
	for _, host := range hosts {
		trimmed := strings.TrimSpace(host)
		if trimmed == "" {
			return true
		}
		if _, err := netip.ParseAddr(trimmed); err != nil {
			return true
		}
	}
	return false
}

func egressPolicyID(cfg ResourceAllocatorConfig) string {
	payload := fmt.Sprintf("proxy_port=%d", cfg.proxyPort()) + "|" +
		strings.Join(cfg.EgressDorisFEHosts, ",") + "|" +
		strings.Join(cfg.EgressDorisBEHosts, ",") + "|" +
		fmt.Sprint(cfg.EgressDorisPorts) + "|" + cfg.EgressDNSPolicy + "|" +
		fmt.Sprintf("dns_allowed=%t", egressAllowsDNS(cfg))
	return "egress_" + strings.ReplaceAll(payload, " ", "_")
}
