package runtime

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestEnsureSandboxNetworkUsesGenerationAllocationAndProbes(t *testing.T) {
	runner := &recordingCommandRunner{
		outputs: map[string][]byte{
			"runsc --version": []byte("runsc test"),
			"ip netns exec harness-gen-a nft list table inet harness_egress":                                                []byte("table exists"),
			"nft list table inet harness_gen_gen_a":                                                                         []byte("table exists"),
			"ip netns exec harness-gen-a curl -sS --max-time 2 -o /dev/null -w %{http_code} http://10.250.0.1:8082/healthz": []byte("200"),
		},
	}
	rt := New(Config{
		RunscNetwork:         "sandbox",
		RunscOverlay2:        "none",
		CommandRunner:        runner,
		BridgeMode:           "claim-loop",
		BridgeHeartbeat:      20 * time.Second,
		BridgePollInterval:   5 * time.Millisecond,
		ProbeHealthzStatuses: []int{200},
	})
	details := testGenerationDetails(t.TempDir(), "gen_a")
	details.RunscNetwork = "sandbox"
	details.RunscOverlay2 = "none"
	details.NetnsName = "harness-gen-a"
	details.NetnsPath = "/var/run/netns/harness-gen-a"
	details.HostVeth = "hgenah"
	details.SandboxVeth = "hgenas"
	details.HostSideCIDR = "10.250.0.0/30"
	details.SandboxIPCIDR = "10.250.0.2/30"
	details.HostGatewayIP = "10.250.0.1"
	details.ProbeURL = "http://10.250.0.1:8082"
	details.AllowedEgressRules = `["tcp:10.250.0.1:8082","tcp:172.16.0.138:9030","udp:53"]`

	if err := rt.ensureSandboxNetwork(context.Background(), details); err != nil {
		t.Fatalf("ensure sandbox network: %v", err)
	}

	want := []string{
		"ip netns add harness-gen-a",
		"ip link delete hgenah",
		"ip netns exec harness-gen-a ip link delete hgenas",
		"ip link add hgenah type veth peer name hgenas",
		"ip link set hgenas netns harness-gen-a",
		"ip addr replace 10.250.0.1/30 dev hgenah",
		"ip link set hgenah up",
		"ip netns exec harness-gen-a ip addr replace 10.250.0.2/30 dev hgenas",
		"ip netns exec harness-gen-a ip link set lo up",
		"ip netns exec harness-gen-a ip link set hgenas up",
		"ip netns exec harness-gen-a ip route replace default via 10.250.0.1 dev hgenas",
		"ip netns exec harness-gen-a nft list table inet harness_egress",
		"ip netns exec harness-gen-a nft delete table inet harness_egress",
		"ip netns exec harness-gen-a nft add table inet harness_egress",
		"ip netns exec harness-gen-a nft add chain inet harness_egress output { type filter hook output priority 0 ; policy drop ; }",
		"ip netns exec harness-gen-a nft add rule inet harness_egress output oifname lo accept",
		"ip netns exec harness-gen-a nft add rule inet harness_egress output ip daddr 10.250.0.1 tcp dport 8082 accept",
		"ip netns exec harness-gen-a nft add rule inet harness_egress output ip daddr 172.16.0.138 tcp dport 9030 accept",
		"ip netns exec harness-gen-a nft add rule inet harness_egress output udp dport 53 accept",
		"sysctl -w net.ipv4.ip_forward=1",
		"nft list table inet harness_gen_gen_a",
		"nft delete table inet harness_gen_gen_a",
		"nft add table inet harness_gen_gen_a",
		"nft add chain inet harness_gen_gen_a forward { type filter hook forward priority 0 ; policy accept ; }",
		"nft add chain inet harness_gen_gen_a postrouting { type nat hook postrouting priority 100 ; policy accept ; }",
		"nft add rule inet harness_gen_gen_a forward iifname hgenah ip daddr 172.16.0.138 tcp dport 9030 accept",
		"nft add rule inet harness_gen_gen_a forward iifname hgenah udp dport 53 accept",
		"nft add rule inet harness_gen_gen_a forward oifname hgenah ct state established,related accept",
		"nft add rule inet harness_gen_gen_a forward iifname hgenah drop",
		"nft add rule inet harness_gen_gen_a postrouting ip saddr 10.250.0.0/30 masquerade",
		"ip netns exec harness-gen-a curl -sS --max-time 2 -o /dev/null -w %{http_code} http://10.250.0.1:8082/healthz",
	}
	if got := runner.Commands(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("unexpected commands:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	for _, command := range runner.Commands() {
		if strings.Contains(command, "/v1/messages") || strings.Contains(command, "x-api-key") {
			t.Fatalf("pre-start sandbox network probe must not call model endpoints or pass API keys: %s", command)
		}
	}
}

func TestProbeSandboxNetworkRetriesAndUsesConfiguredHealthzStatuses(t *testing.T) {
	healthz := "ip netns exec harness-gen-a curl -sS --max-time 2 -o /dev/null -w %{http_code} http://10.250.0.1:8082/healthz"
	runner := &recordingCommandRunner{
		sequence: map[string][]commandResult{
			healthz: {
				{out: []byte("503")},
				{out: []byte("204")},
			},
		},
	}
	rt := New(Config{
		CommandRunner:         runner,
		PreStartProbeAttempts: 2,
		PreStartProbeInterval: time.Nanosecond,
		ProbeHealthzStatuses:  []int{204},
		BridgeMode:            "claim-loop",
		BridgeHeartbeat:       20 * time.Second,
		BridgePollInterval:    5 * time.Millisecond,
	})
	details := testGenerationDetails(t.TempDir(), "gen_a")
	details.NetnsName = "harness-gen-a"
	details.ProbeURL = "http://10.250.0.1:8082"

	if err := rt.probeSandboxNetwork(context.Background(), details); err != nil {
		t.Fatalf("probe sandbox network: %v", err)
	}
	want := []string{healthz, healthz}
	if got := runner.Commands(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("unexpected commands:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}
