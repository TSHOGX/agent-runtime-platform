package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStoreSandboxContractPersistsCanonicalDigestAndMirrors(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_contract")
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_contract",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    testAllocatorConfig(t),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	payload := testSandboxContractPayload(t, "sess_contract", allocation)

	record, err := st.StoreSandboxContract(ctx, StoreSandboxContractParams{
		ContractID:             "contract_" + allocation.GenerationID,
		SessionID:              "sess_contract",
		GenerationID:           allocation.GenerationID,
		SandboxContractVersion: SandboxContractVersion,
		Payload:                payload,
		Now:                    now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("store sandbox contract: %v", err)
	}
	wantPayload, err := CanonicalSandboxContractPayload(payload)
	if err != nil {
		t.Fatalf("canonical payload: %v", err)
	}
	if !bytes.Equal(record.CanonicalPayload, wantPayload) {
		t.Fatalf("canonical payload mismatch:\n got %s\nwant %s", record.CanonicalPayload, wantPayload)
	}
	sum := sha256.Sum256(wantPayload)
	wantDigest := fmt.Sprintf("sha256:%x", sum[:])
	if record.SandboxContractDigest != wantDigest {
		t.Fatalf("digest = %s, want %s", record.SandboxContractDigest, wantDigest)
	}
	if !strings.HasPrefix(record.SandboxContractDigest, "sha256:") {
		t.Fatalf("digest should carry sha256 prefix: %s", record.SandboxContractDigest)
	}

	got, err := st.GetSandboxContractForGeneration(ctx, "sess_contract", allocation.GenerationID)
	if err != nil {
		t.Fatalf("get sandbox contract: %v", err)
	}
	if got.ContractID != record.ContractID ||
		got.SandboxContractDigest != record.SandboxContractDigest ||
		!bytes.Equal(got.CanonicalPayload, record.CanonicalPayload) {
		t.Fatalf("loaded contract mismatch: got %+v want %+v", got, record)
	}

	var generationContractID, generationVersion, resourceContractID, resourceVersion string
	if err := st.db.QueryRowContext(ctx, `
SELECT COALESCE(g.sandbox_contract_id, ''), COALESCE(g.sandbox_contract_version, ''),
       COALESCE(r.contract_id, ''), COALESCE(r.sandbox_contract_version, '')
FROM runtime_generations g
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(
		&generationContractID, &generationVersion, &resourceContractID, &resourceVersion,
	); err != nil {
		t.Fatalf("query contract mirrors: %v", err)
	}
	if generationContractID != record.ContractID ||
		generationVersion != SandboxContractVersion ||
		resourceContractID != record.ContractID ||
		resourceVersion != SandboxContractVersion {
		t.Fatalf("unexpected contract mirrors: generation=%s/%s resource=%s/%s",
			generationContractID, generationVersion, resourceContractID, resourceVersion)
	}
}

func TestCredentialPolicyDigestV1StableForModelProviderGrant(t *testing.T) {
	policy := map[string]any{
		"provider_credentials": "host-only",
		"sandbox_secret_mount": "absent",
		"proxy_token":          "absent",
		"secret_grants": []map[string]any{{
			"grant_id":                  "model_provider:anthropic_proxy",
			"domain":                    "model_provider",
			"scope":                     "anthropic_messages",
			"exposure_mode":             "proxy_only",
			"ttl_seconds":               nil,
			"allowed_drivers":           []string{"claude_code"},
			"allowed_runtime_providers": []string{"local_runsc"},
		}},
	}
	digest, err := CredentialPolicyDigest(policy)
	if err != nil {
		t.Fatalf("credential policy digest: %v", err)
	}
	if digest != "sha256:d016de1bb099d7b6c778c1e0328c0ce69c093b022dd1251f65d3db53cb526529" {
		t.Fatalf("credential_policy_digest_v1 changed: %s", digest)
	}
}

func TestStoreSandboxContractRejectsInvalidSecretGrantSemantics(t *testing.T) {
	for _, tc := range []struct {
		name   string
		tamper func(map[string]any)
		want   string
	}{
		{
			name: "reserved non model domain",
			tamper: func(payload map[string]any) {
				grant := firstSecretGrantForTest(payload)
				grant["grant_id"] = "git:repo"
				grant["domain"] = "git"
				grant["scope"] = "repo"
			},
			want: `unsupported credential grant domain "git"`,
		},
		{
			name: "wrong model grant id",
			tamper: func(payload map[string]any) {
				firstSecretGrantForTest(payload)["grant_id"] = "model_provider:other"
			},
			want: `credential grant_id "model_provider:other" does not match registry grant "model_provider:anthropic_proxy"`,
		},
		{
			name: "unsupported model scope",
			tamper: func(payload map[string]any) {
				firstSecretGrantForTest(payload)["scope"] = "openai_responses"
			},
			want: `unsupported model provider grant scope "openai_responses"`,
		},
		{
			name: "unsupported exposure mode",
			tamper: func(payload map[string]any) {
				firstSecretGrantForTest(payload)["exposure_mode"] = "gateway_url"
			},
			want: `unsupported credential exposure mode "gateway_url"`,
		},
		{
			name: "ttl exceeds registry bound",
			tamper: func(payload map[string]any) {
				firstSecretGrantForTest(payload)["ttl_seconds"] = 86401
			},
			want: "credential grant ttl_seconds 86401 exceeds maximum 86400",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st, owner := openOwnedStore(t, ctx)
			sessionID := "sess_secret_grant_" + strings.ReplaceAll(tc.name, " ", "_")
			createStoreSession(t, ctx, st, sessionID)
			now := time.Now().UTC()
			allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
				SessionID: sessionID,
				Owner:     GenerationLeaseOwner(owner.UUID),
				LeaseTTL:  time.Minute,
				Now:       now,
				Config:    testAllocatorConfig(t),
			})
			if err != nil {
				t.Fatalf("allocate generation: %v", err)
			}
			payload := testSandboxContractPayload(t, sessionID, allocation)
			tc.tamper(payload)
			refreshCredentialPolicyDigestForTest(t, payload)
			_, err = st.StoreSandboxContract(ctx, StoreSandboxContractParams{
				ContractID:   "contract_" + allocation.GenerationID,
				SessionID:    sessionID,
				GenerationID: allocation.GenerationID,
				Payload:      payload,
				Now:          now.Add(time.Second),
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("StoreSandboxContract err=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestCredentialPolicyDigestRejectsUnknownGrantRegistryReferences(t *testing.T) {
	policy := map[string]any{
		"provider_credentials": "host-only",
		"sandbox_secret_mount": "absent",
		"proxy_token":          "absent",
		"secret_grants": []map[string]any{{
			"grant_id":                  "model_provider:anthropic_proxy",
			"domain":                    "model_provider",
			"scope":                     "anthropic_messages",
			"exposure_mode":             "proxy_only",
			"ttl_seconds":               nil,
			"allowed_drivers":           []string{"claude_code"},
			"allowed_runtime_providers": []string{"unknown_runsc"},
		}},
	}
	_, err := CredentialPolicyDigest(policy)
	if err == nil || !strings.Contains(err.Error(), `unsupported credential grant runtime provider "unknown_runsc"`) {
		t.Fatalf("CredentialPolicyDigest err=%v, want unknown runtime provider", err)
	}
}

func TestGetSandboxContractForGenerationRejectsPayloadDigestMismatch(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_contract_corrupt")
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_contract_corrupt",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    testAllocatorConfig(t),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if _, err := st.StoreSandboxContract(ctx, StoreSandboxContractParams{
		ContractID:   "contract_" + allocation.GenerationID,
		SessionID:    "sess_contract_corrupt",
		GenerationID: allocation.GenerationID,
		Payload:      testSandboxContractPayload(t, "sess_contract_corrupt", allocation),
		Now:          now.Add(time.Second),
	}); err != nil {
		t.Fatalf("store sandbox contract: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE sandbox_contracts
SET canonical_payload = '{}'
WHERE generation_id = ?`, allocation.GenerationID); err != nil {
		t.Fatalf("corrupt sandbox contract: %v", err)
	}

	_, err = st.GetSandboxContractForGeneration(ctx, "sess_contract_corrupt", allocation.GenerationID)
	if err == nil || !strings.Contains(err.Error(), "sandbox contract digest mismatch") {
		t.Fatalf("expected digest mismatch, got %v", err)
	}
}

func TestCorruptSandboxContractBlocksLiveRuntimeUse(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()
	sessionID := "sess_contract_live_corrupt"
	createStoreSession(t, ctx, st, sessionID)
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    testAllocatorConfig(t),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	instance := createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, sessionID, allocation, owner.UUID, "host-contract-corrupt", now.Add(2*time.Second))
	turnID, err := st.EnqueueTurn(ctx, sessionID, "before corruption", now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	if grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		RequestID:    "claim_before_corruption",
		LeaseTTL:     time.Minute,
		Now:          now.Add(4 * time.Second),
	}); err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim setup: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	if _, err := st.AckTurnStarted(ctx, AckStartedParams{
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		TurnID:       turnID,
		Owner:        allocation.Owner,
		LeaseTTL:     time.Minute,
		Now:          now.Add(5 * time.Second),
	}); err != nil {
		t.Fatalf("ack setup: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE sandbox_contracts
SET canonical_payload = '{}'
WHERE generation_id = ?`, allocation.GenerationID); err != nil {
		t.Fatalf("corrupt sandbox contract: %v", err)
	}
	if _, _, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
		SessionID:    sessionID,
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		RequestID:    "claim_before_corruption",
		LeaseTTL:     time.Minute,
		Now:          now.Add(6 * time.Second),
	}); err == nil || !strings.Contains(err.Error(), "sandbox contract digest mismatch") {
		t.Fatalf("expected claim contract corruption error, got %v", err)
	}
	if _, err := st.ListBridgePollGenerations(ctx, allocation.Owner, now.Add(6*time.Second), 0); err == nil || !strings.Contains(err.Error(), "sandbox contract digest mismatch") {
		t.Fatalf("expected bridge poll contract corruption error, got %v", err)
	}
	if _, err := st.StartProxyRequest(ctx, StartProxyRequestParams{
		SandboxSourceIP: instance.SandboxIP,
		ProxyRequestID:  "proxy_after_corruption",
		Now:             now.Add(6 * time.Second),
	}); err == nil || !strings.Contains(err.Error(), "sandbox contract digest mismatch") {
		t.Fatalf("expected proxy contract corruption error, got %v", err)
	}
}

func TestStoreSandboxContractRejectsDigestInPayload(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_contract_digest_field")
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_contract_digest_field",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    testAllocatorConfig(t),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	payload := testSandboxContractPayload(t, "sess_contract_digest_field", allocation)
	payload["sandbox_contract_digest"] = "sha256:bad"

	_, err = st.StoreSandboxContract(ctx, StoreSandboxContractParams{
		ContractID:   "contract_" + allocation.GenerationID,
		SessionID:    "sess_contract_digest_field",
		GenerationID: allocation.GenerationID,
		Payload:      payload,
		Now:          now.Add(time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "must not contain sandbox_contract_digest") {
		t.Fatalf("expected digest-field rejection, got %v", err)
	}
}

func TestCanonicalSandboxContractPayloadRejectsFloatingPointNumbers(t *testing.T) {
	_, err := CanonicalSandboxContractPayload(map[string]any{
		"contract_id":              "contract_gen_float",
		"contract_schema_version":  1.5,
		"generation_id":            "gen_float",
		"sandbox_contract_version": SandboxContractVersion,
		"session_id":               "sess_float",
	})
	if err == nil || !strings.Contains(err.Error(), "numbers must be integers") {
		t.Fatalf("expected floating-point number rejection, got %v", err)
	}
}

func TestRecordSandboxContractArtifactsVerifiesContractDigest(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_contract_artifacts")
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_contract_artifacts",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    testAllocatorConfig(t),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	contract, err := st.StoreSandboxContract(ctx, StoreSandboxContractParams{
		ContractID:   "contract_" + allocation.GenerationID,
		SessionID:    "sess_contract_artifacts",
		GenerationID: allocation.GenerationID,
		Payload:      testSandboxContractPayload(t, "sess_contract_artifacts", allocation),
		Now:          now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("store sandbox contract: %v", err)
	}
	_, err = st.RecordSandboxContractArtifacts(ctx, RecordSandboxContractArtifactsParams{
		ContractID:               contract.ContractID,
		SandboxContractDigest:    "sha256:wrong",
		ControlManifestDigest:    "sha256:manifest",
		OCISpecDigest:            "sha256:spec",
		BundleDigest:             "sha256:bundle",
		NetworkHostsDigest:       "sha256:hosts",
		CheckpointMetadataDigest: "sha256:checkpoint",
		Now:                      now.Add(2 * time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "artifact digest mismatch") {
		t.Fatalf("expected artifact digest mismatch, got %v", err)
	}

	artifacts, err := st.RecordSandboxContractArtifacts(ctx, RecordSandboxContractArtifactsParams{
		ContractID:               contract.ContractID,
		ControlManifestDigest:    "sha256:manifest",
		OCISpecDigest:            "sha256:spec",
		BundleDigest:             "sha256:bundle",
		NetworkHostsDigest:       "sha256:hosts",
		CheckpointMetadataDigest: "sha256:checkpoint",
		Now:                      now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatalf("record artifacts: %v", err)
	}
	if artifacts.SandboxContractDigest != contract.SandboxContractDigest ||
		artifacts.NetworkHostsDigest != "sha256:hosts" ||
		artifacts.ControlManifestDigest != "sha256:manifest" ||
		artifacts.OCISpecDigest != "sha256:spec" ||
		artifacts.BundleDigest != "sha256:bundle" ||
		artifacts.CheckpointMetadataDigest != "sha256:checkpoint" {
		t.Fatalf("unexpected artifacts: %+v", artifacts)
	}
}

func testSandboxContractPayload(t *testing.T, sessionID string, allocation GenerationAllocation) map[string]any {
	t.Helper()
	contractID := "contract_" + allocation.GenerationID
	driverID := allocation.DriverState.DriverID
	if driverID == "" {
		driverID = "claude_code"
	}
	stateDigest := allocation.DriverState.StateDigest
	if stateDigest == "" {
		stateDigest = "sha256:test-driver-state"
	}
	secretGrants := []map[string]any{{
		"grant_id":                  "model_provider:anthropic_proxy",
		"domain":                    "model_provider",
		"scope":                     "anthropic_messages",
		"exposure_mode":             "proxy_only",
		"ttl_seconds":               nil,
		"allowed_drivers":           []string{driverID},
		"allowed_runtime_providers": []string{"local_runsc"},
	}}
	credentialPolicy := map[string]any{
		"provider_credentials": "host-only",
		"sandbox_secret_mount": "absent",
		"proxy_token":          "absent",
		"secret_grants":        secretGrants,
	}
	credentialDigest, err := CredentialPolicyDigest(credentialPolicy)
	if err != nil {
		t.Fatalf("credential digest: %v", err)
	}
	credentialPolicy["digest"] = credentialDigest
	return map[string]any{
		"runtime_profile_id":       allocation.AgentRuntimeProfileID,
		"session_id":               sessionID,
		"network_profile_id":       allocation.NetworkProfileID,
		"contract_schema_version":  SandboxContractSchemaVersion,
		"contract_gate_version":    SandboxContractGatePhase9A,
		"generation_id":            allocation.GenerationID,
		"sandbox_contract_version": SandboxContractVersion,
		"contract_id":              contractID,
		"identity": map[string]any{
			"sandbox_uid":               65534,
			"sandbox_gid":               65534,
			"sandbox_supplemental_gids": []int{},
			"model_access_allowed":      true,
		},
		"driver": map[string]any{
			"driver_id":                            driverID,
			"driver_version":                       "test",
			"bridge_protocol":                      "claude_stream_json_per_turn",
			"output_schema":                        "claude_stream_json_v1",
			"command_argv_digest":                  "sha256:command",
			"driver_config_digest":                 "sha256:driver-config",
			"required_runtime_capabilities_digest": "sha256:driver-capabilities",
			"supports_interrupt":                   false,
			"supports_compaction":                  true,
		},
		"runtime_provider": map[string]any{
			"provider_id":              "local_runsc",
			"provider_profile_id":      "local_runsc_default",
			"isolation_kind":           "gvisor",
			"template_ref":             "default",
			"template_digest":          "sha256:template",
			"capability_vocab_version": "1",
			"capability_digest":        "sha256:provider-capabilities",
			"provider_specific": map[string]any{
				"runsc_container_id": "harness-gen-" + allocation.GenerationID,
				"runsc_platform":     "systrap",
				"runsc_version":      "runsc test",
			},
		},
		"network_identity": map[string]any{
			"runsc_network": "sandbox",
			"sandbox_ip":    "10.240.0.2",
		},
		"mount_plan": map[string]any{
			"workspace":  map[string]any{"destination": "/workspace", "mode": "rw"},
			"agent_home": map[string]any{"destination": "/agent-home", "mode": "rw"},
			"control":    map[string]any{"destination": "/harness-control", "mode": "ro"},
			"bridge":     map[string]any{"destination": "/harness-control/bridge", "mode": "rw"},
		},
		"runtime_adapter": map[string]any{
			"kind":                "runsc",
			"runsc_platform":      "systrap",
			"runsc_version":       "runsc test",
			"runsc_binary_path":   filepath.Join(t.TempDir(), "runsc"),
			"runsc_binary_digest": "sha256:runsc",
			"runsc_container_id":  "harness-gen-" + allocation.GenerationID,
		},
		"credential_policy": credentialPolicy,
		"model_access": map[string]any{
			"model_access_allowed":         true,
			"active_turn_required":         true,
			"sandbox_model_proxy_base_url": "http://harness-model-proxy.internal:8082",
		},
		"driver_runtime": map[string]any{
			"driver_home_mount":             "/agent-home",
			"generated_driver_config_mount": "/harness-control/driver/" + driverID,
			"materialized_driver_config":    map[string]any{},
			"initial_driver_state_digest":   stateDigest,
		},
		"input_digests": map[string]any{
			"runtime_config_digest": nil,
			"rootfs_image_digest":   nil,
			"agent_manifest_digest": nil,
		},
	}
}

func firstSecretGrantForTest(payload map[string]any) map[string]any {
	policy := payload["credential_policy"].(map[string]any)
	switch grants := policy["secret_grants"].(type) {
	case []map[string]any:
		return grants[0]
	case []any:
		return grants[0].(map[string]any)
	default:
		panic("unexpected secret_grants test shape")
	}
}

func refreshCredentialPolicyDigestForTest(t *testing.T, payload map[string]any) {
	t.Helper()
	policy := payload["credential_policy"].(map[string]any)
	delete(policy, "digest")
	digest, err := CredentialPolicyDigest(policy)
	if err != nil {
		t.Fatalf("credential digest: %v", err)
	}
	policy["digest"] = digest
}
