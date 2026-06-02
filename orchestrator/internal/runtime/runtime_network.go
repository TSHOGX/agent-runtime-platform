package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/store"
)

func (r *Runtime) ensureSandboxNetwork(ctx context.Context, details store.RuntimeGenerationDetails) error {
	runscNetwork, err := r.runscNetwork(details)
	if err != nil {
		return err
	}
	if !strings.EqualFold(runscNetwork, "sandbox") {
		return nil
	}
	if strings.TrimSpace(details.NetnsName) == "" ||
		strings.TrimSpace(details.HostVeth) == "" ||
		strings.TrimSpace(details.SandboxVeth) == "" ||
		strings.TrimSpace(details.HostSideCIDR) == "" ||
		strings.TrimSpace(details.SandboxIPCIDR) == "" ||
		strings.TrimSpace(details.HostGatewayIP) == "" ||
		strings.TrimSpace(details.ProbeURL) == "" {
		return fmt.Errorf("sandbox network allocation is incomplete")
	}
	hostGatewayCIDR, err := hostGatewayCIDR(details)
	if err != nil {
		return err
	}

	commands := [][]string{
		{"ip", "netns", "add", details.NetnsName},
		{"ip", "link", "delete", details.HostVeth},
		{"ip", "netns", "exec", details.NetnsName, "ip", "link", "delete", details.SandboxVeth},
		{"ip", "link", "add", details.HostVeth, "type", "veth", "peer", "name", details.SandboxVeth},
		{"ip", "link", "set", details.SandboxVeth, "netns", details.NetnsName},
		{"ip", "addr", "replace", hostGatewayCIDR, "dev", details.HostVeth},
		{"ip", "link", "set", details.HostVeth, "up"},
		{"ip", "netns", "exec", details.NetnsName, "ip", "addr", "replace", details.SandboxIPCIDR, "dev", details.SandboxVeth},
		{"ip", "netns", "exec", details.NetnsName, "ip", "link", "set", "lo", "up"},
		{"ip", "netns", "exec", details.NetnsName, "ip", "link", "set", details.SandboxVeth, "up"},
		{"ip", "netns", "exec", details.NetnsName, "ip", "route", "replace", "default", "via", details.HostGatewayIP, "dev", details.SandboxVeth},
	}
	for _, args := range commands {
		output, err := r.runner.CombinedOutput(ctx, args[0], args[1:]...)
		if err != nil {
			if ignoreSandboxNetworkCommandError(args, string(output), err) {
				continue
			}
			return fmt.Errorf("configure sandbox network %q: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
		}
	}
	if err := r.applySandboxEgressPolicy(ctx, details); err != nil {
		return err
	}
	if err := r.applyHostEgressPolicy(ctx, details); err != nil {
		return err
	}
	if err := r.probeSandboxNetwork(ctx, details); err != nil {
		return err
	}
	return nil
}

func hostGatewayCIDR(details store.RuntimeGenerationDetails) (string, error) {
	_, suffix, ok := strings.Cut(strings.TrimSpace(details.HostSideCIDR), "/")
	if !ok || strings.TrimSpace(suffix) == "" {
		return "", fmt.Errorf("invalid host side cidr %q", details.HostSideCIDR)
	}
	return strings.TrimSpace(details.HostGatewayIP) + "/" + strings.TrimSpace(suffix), nil
}

func ignoreSandboxNetworkCommandError(args []string, output string, err error) bool {
	if len(args) >= 4 && args[0] == "ip" && args[1] == "netns" && args[2] == "add" {
		return commandOutputContains(output, "file exists", "already exists")
	}
	if len(args) >= 4 && args[0] == "ip" && args[1] == "link" && args[2] == "delete" {
		return commandOutputContains(output, "cannot find device", "does not exist", "not found")
	}
	if len(args) >= 8 && args[0] == "ip" && args[1] == "netns" && args[2] == "exec" && args[4] == "ip" && args[5] == "link" && args[6] == "delete" {
		return commandOutputContains(output, "cannot find device", "does not exist", "not found")
	}
	return false
}

func generationNftTableName(details store.RuntimeGenerationDetails) (string, error) {
	if tableName := strings.TrimSpace(details.NftTableName); tableName != "" {
		return tableName, nil
	}
	return hostEgressTableName(details.GenerationID)
}

func (r *Runtime) applySandboxEgressPolicy(ctx context.Context, details store.RuntimeGenerationDetails) error {
	rules, err := parseAllowedEgressRules(details.AllowedEgressRules)
	if err != nil {
		return err
	}
	const tableName = "harness_egress"
	base := []string{"netns", "exec", details.NetnsName, "nft"}
	if _, err := r.runner.CombinedOutput(ctx, "ip", append(base, "list", "table", "inet", tableName)...); err == nil {
		if err := r.runNetworkCommand(ctx, "ip", append(base, "delete", "table", "inet", tableName)...); err != nil {
			return err
		}
	}
	if err := r.runNetworkCommand(ctx, "ip", append(base, "add", "table", "inet", tableName)...); err != nil {
		return err
	}
	if err := r.runNetworkCommand(ctx, "ip", append(base, "add", "chain", "inet", tableName, "output", "{", "type", "filter", "hook", "output", "priority", "0", ";", "policy", "drop", ";", "}")...); err != nil {
		return err
	}
	if err := r.runNetworkCommand(ctx, "ip", append(base, "add", "rule", "inet", tableName, "output", "oifname", "lo", "accept")...); err != nil {
		return err
	}
	for _, rule := range rules {
		args := append([]string{}, base...)
		args = append(args, "add", "rule", "inet", tableName, "output")
		if rule.Host != "" {
			args = append(args, "ip", "daddr", rule.Host)
		}
		args = append(args, rule.Proto, "dport", strconv.Itoa(rule.Port), "accept")
		if err := r.runNetworkCommand(ctx, "ip", args...); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) applyHostEgressPolicy(ctx context.Context, details store.RuntimeGenerationDetails) error {
	rules, err := parseAllowedEgressRules(details.AllowedEgressRules)
	if err != nil {
		return err
	}
	if err := r.runNetworkCommand(ctx, "sysctl", "-w", "net.ipv4.ip_forward=1"); err != nil {
		return err
	}
	tableName, err := hostEgressTableName(details.GenerationID)
	if err != nil {
		return err
	}
	if _, err := r.runner.CombinedOutput(ctx, "nft", "list", "table", "inet", tableName); err == nil {
		if err := r.runNetworkCommand(ctx, "nft", "delete", "table", "inet", tableName); err != nil {
			return err
		}
	}
	if err := r.runNetworkCommand(ctx, "nft", "add", "table", "inet", tableName); err != nil {
		return err
	}
	if err := r.runNetworkCommand(ctx, "nft", "add", "chain", "inet", tableName, "forward", "{", "type", "filter", "hook", "forward", "priority", "0", ";", "policy", "accept", ";", "}"); err != nil {
		return err
	}
	if err := r.runNetworkCommand(ctx, "nft", "add", "chain", "inet", tableName, "postrouting", "{", "type", "nat", "hook", "postrouting", "priority", "100", ";", "policy", "accept", ";", "}"); err != nil {
		return err
	}
	for _, rule := range rules {
		if rule.Host == details.HostGatewayIP {
			continue
		}
		args := []string{"add", "rule", "inet", tableName, "forward", "iifname", details.HostVeth}
		if rule.Host != "" {
			args = append(args, "ip", "daddr", rule.Host)
		}
		args = append(args, rule.Proto, "dport", strconv.Itoa(rule.Port), "accept")
		if err := r.runNetworkCommand(ctx, "nft", args...); err != nil {
			return err
		}
	}
	if err := r.runNetworkCommand(ctx, "nft", "add", "rule", "inet", tableName, "forward", "oifname", details.HostVeth, "ct", "state", "established,related", "accept"); err != nil {
		return err
	}
	if err := r.runNetworkCommand(ctx, "nft", "add", "rule", "inet", tableName, "forward", "iifname", details.HostVeth, "drop"); err != nil {
		return err
	}
	if err := r.runNetworkCommand(ctx, "nft", "add", "rule", "inet", tableName, "postrouting", "ip", "saddr", details.HostSideCIDR, "masquerade"); err != nil {
		return err
	}
	return nil
}

func nftIdentifier(value string) (string, error) {
	value = strings.TrimSpace(value)
	var b strings.Builder
	hasToken := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
			hasToken = true
		case r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	out := b.String()
	if out == "" || !hasToken {
		return "", fmt.Errorf("nft identifier is required")
	}
	return out, nil
}

func hostEgressTableName(generationID string) (string, error) {
	identifier, err := nftIdentifier(generationID)
	if err != nil {
		return "", err
	}
	return "harness_gen_" + identifier, nil
}

func (r *Runtime) runNetworkCommand(ctx context.Context, name string, args ...string) error {
	output, err := r.runner.CombinedOutput(ctx, name, args...)
	if err != nil {
		return fmt.Errorf("configure sandbox network %q: %w: %s", strings.Join(append([]string{name}, args...), " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (r *Runtime) probeSandboxNetwork(ctx context.Context, details store.RuntimeGenerationDetails) error {
	attempts := r.cfg.PreStartProbeAttempts
	if attempts <= 0 {
		attempts = 3
	}
	interval := r.cfg.PreStartProbeInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := r.runSandboxNetworkProbeOnce(ctx, details); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt == attempts {
			break
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func (r *Runtime) runSandboxNetworkProbeOnce(ctx context.Context, details store.RuntimeGenerationDetails) error {
	baseURL := strings.TrimRight(details.ProbeURL, "/")
	bridgeProbeConfig, err := r.requiredBridgeProbeConfig()
	if err != nil {
		return err
	}
	probes := []struct {
		args           []string
		acceptStatuses []int
	}{
		{
			args:           []string{"netns", "exec", details.NetnsName, "curl", "-sS", "--max-time", "2", "-o", "/dev/null", "-w", "%{http_code}", baseURL + "/healthz"},
			acceptStatuses: bridgeProbeConfig.healthzStatuses,
		},
	}
	for _, probe := range probes {
		output, err := r.runner.CombinedOutput(ctx, "ip", probe.args...)
		if err != nil {
			return fmt.Errorf("pre-start sandbox network probe %q: %w: %s", strings.Join(append([]string{"ip"}, probe.args...), " "), err, strings.TrimSpace(string(output)))
		}
		status, err := strconv.Atoi(strings.TrimSpace(string(output)))
		if err != nil {
			return fmt.Errorf("pre-start sandbox network probe %q: invalid status %s", strings.Join(append([]string{"ip"}, probe.args...), " "), strings.TrimSpace(string(output)))
		}
		if !statusAccepted(status, probe.acceptStatuses) {
			return fmt.Errorf("pre-start sandbox network probe %q: unexpected status %d", strings.Join(append([]string{"ip"}, probe.args...), " "), status)
		}
	}
	return nil
}

func statusAccepted(status int, accepted []int) bool {
	for _, value := range accepted {
		if status == value {
			return true
		}
	}
	return false
}

type egressRule struct {
	Proto string
	Host  string
	Port  int
}

func parseAllowedEgressRules(raw string) ([]egressRule, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var values []string
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return nil, fmt.Errorf("parse egress rules: %w", err)
	}
	rules := make([]egressRule, 0, len(values))
	for _, value := range values {
		rule, err := parseAllowedEgressRule(value)
		if err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}
	return rules, nil
}

func parseAllowedEgressRule(value string) (egressRule, error) {
	value = strings.TrimSpace(value)
	parts := strings.Split(value, ":")
	if len(parts) == 2 {
		port, err := strconv.Atoi(parts[1])
		if err != nil || port <= 0 || port > 65535 {
			return egressRule{}, fmt.Errorf("invalid egress rule %q", value)
		}
		if parts[0] != "tcp" && parts[0] != "udp" {
			return egressRule{}, fmt.Errorf("invalid egress rule %q", value)
		}
		return egressRule{Proto: parts[0], Port: port}, nil
	}
	if len(parts) != 3 {
		return egressRule{}, fmt.Errorf("invalid egress rule %q", value)
	}
	port, err := strconv.Atoi(parts[2])
	if err != nil || port <= 0 || port > 65535 {
		return egressRule{}, fmt.Errorf("invalid egress rule %q", value)
	}
	if parts[0] != "tcp" && parts[0] != "udp" {
		return egressRule{}, fmt.Errorf("invalid egress rule %q", value)
	}
	if strings.TrimSpace(parts[1]) == "" {
		return egressRule{}, fmt.Errorf("invalid egress rule %q", value)
	}
	return egressRule{Proto: parts[0], Host: parts[1], Port: port}, nil
}
