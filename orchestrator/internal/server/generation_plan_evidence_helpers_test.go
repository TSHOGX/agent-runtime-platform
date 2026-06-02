package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/store"
)

type serverGenerationPlanSourceDigestsForTest struct {
	RuntimeConfigDigest string
	AgentManifestDigest string
}

func storeServerSyntheticSandboxContractParentForPlan(t *testing.T, ctx context.Context, st *store.Store, plan store.GenerationPlanRecord) {
	t.Helper()
	sessionID := serverGenerationPlanSessionID(t, plan.CanonicalPayload)
	contractID := sandboxContractID(plan.GenerationID)
	canonicalPayload, err := store.CanonicalSandboxContractPayload(serverFrozenEvidenceSandboxContractPayloadForTest(
		sessionID,
		plan.GenerationID,
		contractID,
		"claude_code",
		"sha256:driver-state",
	))
	if err != nil {
		t.Fatalf("canonical synthetic sandbox contract parent: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
INSERT INTO sandbox_contracts (
  contract_id, generation_id, session_id, sandbox_contract_version,
  contract_schema_version, contract_gate_version, canonical_payload,
  sandbox_contract_digest, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(contract_id) DO NOTHING`,
		contractID, plan.GenerationID, sessionID, store.SandboxContractVersion,
		store.SandboxContractSchemaVersion, store.SandboxContractGateDriverManifest,
		string(canonicalPayload), store.SandboxContractDigest(canonicalPayload),
		time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("store synthetic sandbox contract parent: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET sandbox_contract_id = ?,
    sandbox_contract_version = ?
WHERE generation_id = ?
  AND session_id = ?`, contractID, store.SandboxContractVersion, plan.GenerationID, sessionID); err != nil {
		t.Fatalf("store synthetic sandbox contract generation mirror: %v", err)
	}
}

func storeServerSandboxContractInputEvidenceFromGenerationPlanIfPresent(t *testing.T, ctx context.Context, st *store.Store, generationID string) {
	t.Helper()
	plan, err := st.GetGenerationPlan(ctx, generationID)
	if errors.Is(err, sql.ErrNoRows) {
		return
	}
	if err != nil {
		t.Fatalf("get generation plan for input evidence: %v", err)
	}
	storeServerSandboxContractInputEvidenceFromPlan(t, ctx, st, plan)
}

func storeServerSandboxContractInputEvidenceFromPlan(t *testing.T, ctx context.Context, st *store.Store, plan store.GenerationPlanRecord) {
	t.Helper()
	digests := serverGenerationPlanSourceDigests(t, plan.CanonicalPayload)
	contractID := sandboxContractID(plan.GenerationID)
	if _, err := st.DBForTest().ExecContext(ctx, `
INSERT INTO sandbox_contract_input_evidence (
  contract_id, runtime_config_digest, runtime_config_preimage,
  agent_manifest_digest, agent_manifest_payload, created_at
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(contract_id) DO NOTHING`,
		contractID, digests.RuntimeConfigDigest, "{}",
		digests.AgentManifestDigest, "{}", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("store sandbox contract input evidence: %v", err)
	}
	evidence, err := st.GetSandboxContractInputEvidence(ctx, contractID)
	if err != nil {
		t.Fatalf("get sandbox contract input evidence: %v", err)
	}
	if evidence.RuntimeConfigDigest != digests.RuntimeConfigDigest ||
		evidence.AgentManifestDigest != digests.AgentManifestDigest {
		t.Fatalf("sandbox contract input evidence mismatch: evidence=%+v want=%+v", evidence, digests)
	}
}

func serverGenerationPlanSessionID(t *testing.T, canonicalPayload []byte) string {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(canonicalPayload, &payload); err != nil {
		t.Fatalf("decode generation plan payload: %v", err)
	}
	identity, ok := payload["identity"].(map[string]any)
	if !ok {
		t.Fatalf("generation plan missing identity: %s", canonicalPayload)
	}
	sessionID, _ := identity["session_id"].(string)
	if strings.TrimSpace(sessionID) == "" {
		t.Fatalf("generation plan missing identity.session_id: %s", canonicalPayload)
	}
	return sessionID
}

func serverGenerationPlanSourceDigests(t *testing.T, canonicalPayload []byte) serverGenerationPlanSourceDigestsForTest {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(canonicalPayload, &payload); err != nil {
		t.Fatalf("decode generation plan payload: %v", err)
	}
	sourceDigests, ok := payload["source_digests"].(map[string]any)
	if !ok {
		t.Fatalf("generation plan missing source_digests: %s", canonicalPayload)
	}
	digests := serverGenerationPlanSourceDigestsForTest{}
	digests.RuntimeConfigDigest, _ = sourceDigests["runtime_config_digest"].(string)
	digests.AgentManifestDigest, _ = sourceDigests["agent_manifest_digest"].(string)
	if strings.TrimSpace(digests.RuntimeConfigDigest) == "" ||
		strings.TrimSpace(digests.AgentManifestDigest) == "" {
		t.Fatalf("generation plan missing source digests: %s", canonicalPayload)
	}
	return digests
}
