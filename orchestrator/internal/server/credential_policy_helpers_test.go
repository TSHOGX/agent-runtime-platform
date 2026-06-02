package server

import (
	"testing"

	"harness-platform/orchestrator/internal/store"
)

func serverCredentialPolicyForTest(t *testing.T, driverID string) map[string]any {
	t.Helper()
	return serverCredentialPolicyPayloadForTest(driverID)
}

func serverCredentialPolicyPayloadForTest(driverID string) map[string]any {
	secretGrants := []map[string]any{}
	if driverID == "claude_code" {
		secretGrants = append(secretGrants, map[string]any{
			"grant_id":                  "model_provider:anthropic_proxy",
			"domain":                    "model_provider",
			"scope":                     "anthropic_messages",
			"exposure_mode":             "proxy_only",
			"ttl_seconds":               nil,
			"allowed_drivers":           []string{driverID},
			"allowed_runtime_providers": []string{"local_runsc"},
		})
	}
	policy := map[string]any{
		"provider_credentials": "host-only",
		"sandbox_secret_mount": "absent",
		"proxy_token":          "absent",
		"secret_grants":        secretGrants,
	}
	digest, err := store.CredentialPolicyDigest(policy)
	if err != nil {
		panic(err)
	}
	policy["digest"] = digest
	return policy
}
