package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"harness-platform/orchestrator/internal/agents"
)

const credentialPolicyDigestPrefix = "credential_policy_digest_v1\n"

type normalizedCredentialGrant struct {
	GrantID                 string   `json:"grant_id"`
	Domain                  string   `json:"domain"`
	Scope                   string   `json:"scope"`
	ExposureMode            string   `json:"exposure_mode"`
	TTLSeconds              any      `json:"ttl_seconds"`
	AllowedDrivers          []string `json:"allowed_drivers"`
	AllowedRuntimeProviders []string `json:"allowed_runtime_providers"`
}

type normalizedCredentialPolicy struct {
	ProviderCredentials string                      `json:"provider_credentials"`
	SandboxSecretMount  string                      `json:"sandbox_secret_mount"`
	ProxyToken          string                      `json:"proxy_token"`
	SecretGrants        []normalizedCredentialGrant `json:"secret_grants"`
}

func CredentialPolicyDigest(value any) (string, error) {
	policy, err := normalizeCredentialPolicy(value)
	if err != nil {
		return "", err
	}
	canonical, err := canonicalDataVolumeJSON(policy)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(append([]byte(credentialPolicyDigestPrefix), canonical...))
	return "sha256:" + fmt.Sprintf("%x", sum[:]), nil
}

func validateCredentialPolicyGrantSemantics(policy normalizedCredentialPolicy, driverSpec agents.DriverSpec, providerSpec agents.RuntimeProviderSpec, modelAccessEnabled bool) error {
	if modelAccessEnabled && !driverSpec.ModelAccess {
		return fmt.Errorf("driver %s does not support model access grants", driverSpec.ID)
	}
	modelGrant := false
	for _, grant := range policy.SecretGrants {
		spec, ok := agents.SecretGrantSpecFor(grant.Domain, grant.Scope)
		if !ok {
			if grant.Domain != "model_provider" {
				return fmt.Errorf("unsupported credential grant domain %q", grant.Domain)
			}
			return fmt.Errorf("unsupported model provider grant scope %q", grant.Scope)
		}
		if grant.GrantID != spec.GrantID {
			return fmt.Errorf("credential grant_id %q does not match registry grant %q", grant.GrantID, spec.GrantID)
		}
		if grant.ExposureMode != spec.ExposureMode {
			return fmt.Errorf("unsupported credential exposure mode %q", grant.ExposureMode)
		}
		if ttl, ok := credentialGrantTTLSeconds(grant.TTLSeconds); ok && spec.TTLMaxSeconds > 0 && ttl > spec.TTLMaxSeconds {
			return fmt.Errorf("credential grant ttl_seconds %d exceeds maximum %d", ttl, spec.TTLMaxSeconds)
		}
		switch spec.Domain {
		case "model_provider":
			modelGrant = true
			if !stringSliceContains(grant.AllowedDrivers, string(driverSpec.ID)) {
				return fmt.Errorf("model provider grant does not allow driver %s", driverSpec.ID)
			}
			if !stringSliceContains(grant.AllowedRuntimeProviders, providerSpec.ID) {
				return fmt.Errorf("model provider grant does not allow runtime provider %s", providerSpec.ID)
			}
		default:
			return fmt.Errorf("unsupported credential grant domain %q", grant.Domain)
		}
	}
	if modelAccessEnabled && !modelGrant {
		return fmt.Errorf("model access contract requires model_provider grant")
	}
	if !modelAccessEnabled && len(policy.SecretGrants) != 0 {
		return fmt.Errorf("non-model contract must not carry model provider grants")
	}
	return nil
}

func normalizeCredentialPolicy(value any) (normalizedCredentialPolicy, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return normalizedCredentialPolicy{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil {
		return normalizedCredentialPolicy{}, err
	}
	delete(object, "digest")
	policy := normalizedCredentialPolicy{
		ProviderCredentials: strings.ToLower(strings.TrimSpace(stringValue(object["provider_credentials"]))),
		SandboxSecretMount:  strings.ToLower(strings.TrimSpace(stringValue(object["sandbox_secret_mount"]))),
		ProxyToken:          strings.ToLower(strings.TrimSpace(stringValue(object["proxy_token"]))),
	}
	if policy.ProviderCredentials != "host-only" ||
		policy.SandboxSecretMount != "absent" ||
		policy.ProxyToken != "absent" {
		return normalizedCredentialPolicy{}, fmt.Errorf("credential policy posture mismatch")
	}
	grantsRaw, _ := object["secret_grants"].([]any)
	for _, grantRaw := range grantsRaw {
		grantObject, ok := grantRaw.(map[string]any)
		if !ok {
			return normalizedCredentialPolicy{}, fmt.Errorf("credential grant must be an object")
		}
		grant := normalizedCredentialGrant{
			GrantID:                 strings.TrimSpace(stringValue(grantObject["grant_id"])),
			Domain:                  strings.ToLower(strings.TrimSpace(stringValue(grantObject["domain"]))),
			Scope:                   strings.TrimSpace(stringValue(grantObject["scope"])),
			ExposureMode:            strings.ToLower(strings.TrimSpace(stringValue(grantObject["exposure_mode"]))),
			TTLSeconds:              normalizedTTLSeconds(grantObject["ttl_seconds"]),
			AllowedDrivers:          normalizedStringList(grantObject["allowed_drivers"]),
			AllowedRuntimeProviders: normalizedStringList(grantObject["allowed_runtime_providers"]),
		}
		if grant.GrantID == "" || grant.Scope == "" {
			return normalizedCredentialPolicy{}, fmt.Errorf("credential grant id and scope are required")
		}
		if grant.TTLSeconds == "invalid" {
			return normalizedCredentialPolicy{}, fmt.Errorf("credential grant ttl_seconds must be null or positive integer")
		}
		if grant.Domain == "model_provider" && (len(grant.AllowedDrivers) == 0 || len(grant.AllowedRuntimeProviders) == 0) {
			return normalizedCredentialPolicy{}, fmt.Errorf("model provider grant allowlists are required")
		}
		for _, driverID := range grant.AllowedDrivers {
			if _, ok := agents.Lookup(driverID); !ok {
				return normalizedCredentialPolicy{}, fmt.Errorf("unsupported credential grant driver %q", driverID)
			}
		}
		for _, providerID := range grant.AllowedRuntimeProviders {
			if _, ok := agents.RuntimeProviderSpecFor(providerID); !ok {
				return normalizedCredentialPolicy{}, fmt.Errorf("unsupported credential grant runtime provider %q", providerID)
			}
		}
		policy.SecretGrants = append(policy.SecretGrants, grant)
	}
	sort.Slice(policy.SecretGrants, func(i, j int) bool {
		left := credentialGrantSortKey(policy.SecretGrants[i])
		right := credentialGrantSortKey(policy.SecretGrants[j])
		return left < right
	})
	return policy, nil
}

func credentialGrantTTLSeconds(value any) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case json.Number:
		i, err := v.Int64()
		return i, err == nil
	case float64:
		i := int64(v)
		if float64(i) == v {
			return i, true
		}
	}
	return 0, false
}

func normalizedTTLSeconds(value any) any {
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case json.Number:
		i, err := v.Int64()
		if err != nil || i <= 0 || v.String() != fmt.Sprint(i) {
			return "invalid"
		}
		return i
	case float64:
		i := int64(v)
		if v != float64(i) || i <= 0 {
			return "invalid"
		}
		return i
	default:
		return "invalid"
	}
}

func normalizedStringList(value any) []string {
	raw, _ := value.([]any)
	seen := map[string]struct{}{}
	values := make([]string, 0, len(raw))
	for _, item := range raw {
		text := strings.TrimSpace(stringValue(item))
		if text == "" {
			continue
		}
		if _, ok := seen[text]; ok {
			continue
		}
		seen[text] = struct{}{}
		values = append(values, text)
	}
	sort.Strings(values)
	return values
}

func credentialGrantSortKey(grant normalizedCredentialGrant) string {
	return strings.Join([]string{
		grant.Domain,
		grant.GrantID,
		grant.Scope,
		grant.ExposureMode,
		fmt.Sprint(grant.TTLSeconds),
		strings.Join(grant.AllowedDrivers, ","),
		strings.Join(grant.AllowedRuntimeProviders, ","),
	}, "\x00")
}

func stringSliceContains(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}
