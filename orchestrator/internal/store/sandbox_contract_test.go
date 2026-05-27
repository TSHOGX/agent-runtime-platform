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
	return map[string]any{
		"runtime_profile_id":       allocation.AgentRuntimeProfileID,
		"session_id":               sessionID,
		"network_profile_id":       allocation.NetworkProfileID,
		"contract_schema_version":  1,
		"generation_id":            allocation.GenerationID,
		"sandbox_contract_version": SandboxContractVersion,
		"contract_id":              contractID,
		"identity": map[string]any{
			"sandbox_uid":               65534,
			"sandbox_gid":               65534,
			"sandbox_supplemental_gids": []int{},
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
		"credential_policy": map[string]any{
			"provider_credentials": "host-only",
			"sandbox_secret_mount": "absent",
			"proxy_token":          "absent",
		},
		"model_access": map[string]any{
			"model_access_allowed":         true,
			"active_turn_required":         true,
			"sandbox_model_proxy_base_url": "http://harness-model-proxy.internal:8082",
		},
		"input_digests": map[string]any{
			"runtime_config_digest": "sha256:runtime",
			"rootfs_image_digest":   "sha256:rootfs",
			"schema_pack_digest":    nil,
		},
	}
}
