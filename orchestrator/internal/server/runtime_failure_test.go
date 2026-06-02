package server

import "testing"

func TestRuntimeFailureClassDetectsPostStartProbeFailure(t *testing.T) {
	cases := []string{
		"harness-bridge-client probe exited with status 1",
		"bridge probe starting failed",
		"bridge startup probe did not complete: missing probe_network",
		"probe GET /healthz returned 503, want one of [200]",
		"probe POST /v1/messages returned 502, want one of [400]",
	}
	for _, message := range cases {
		if got := runtimeFailureClass(message); got != "probe_failed_post_start" {
			t.Fatalf("runtimeFailureClass(%q)=%s want probe_failed_post_start", message, got)
		}
	}
}

func TestRuntimeFailureClassDetectsManifestFailures(t *testing.T) {
	cases := []struct {
		message string
		want    string
	}{
		{"sandbox_secret_disallowed", "sandbox_secret_disallowed"},
		{"shell_secret_disallowed", "shell_secret_disallowed"},
		{"runsc run: exit status 1: control manifest digest mismatch", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected session_id=sess_a got sess_b", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected generation_id=gen_a got gen_b", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected network_profile_id=net_a got net_b", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected agent_runtime_profile_id=arp_a got arp_b", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected anthropic_api_key_secret_id=anthropic_api_key got other", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected anthropic_auth_token_secret_id=anthropic_auth_token got other", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected manifest_version=1 got 2", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: expected secret_version=local got rotated", "manifest_digest_mismatch"},
		{"runsc run: exit status 1: secret mount /harness-secrets missing", "manifest_digest_mismatch"},
	}
	for _, tc := range cases {
		if got := runtimeFailureClass(tc.message); got != tc.want {
			t.Fatalf("runtimeFailureClass(%q)=%s want %s", tc.message, got, tc.want)
		}
	}
}
