package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/agents"
)

const SandboxContractVersion = "sandbox-isolation-v1"
const SandboxContractSchemaVersion = 2
const SandboxContractGateDriverManifest = "driver_manifest_v1"
const runtimeConfigDigestPrefix = "runtime_config_digest_v1\n"

type SandboxContractRecord struct {
	ContractID             string
	SessionID              string
	GenerationID           string
	SandboxContractVersion string
	ContractSchemaVersion  int
	ContractGateVersion    string
	CanonicalPayload       []byte
	SandboxContractDigest  string
	CreatedAt              time.Time
}

type StoreSandboxContractParams struct {
	ContractID             string
	SessionID              string
	GenerationID           string
	Owner                  string
	SandboxContractVersion string
	ContractSchemaVersion  int
	ContractGateVersion    string
	DriverState            DriverStateToken
	Payload                any
	RuntimeConfigDigest    string
	RuntimeConfigPreimage  any
	AgentManifestDigest    string
	AgentManifestPayload   any
	Now                    time.Time
}

type SandboxContractArtifacts struct {
	ContractID               string
	SandboxContractDigest    string
	NetworkHostsDigest       string
	ControlManifestDigest    string
	OCISpecDigest            string
	BundleDigest             string
	CheckpointMetadataDigest string
	CreatedAt                time.Time
}

type SandboxContractInputEvidence struct {
	ContractID            string
	RuntimeConfigDigest   string
	RuntimeConfigPreimage []byte
	AgentManifestDigest   string
	AgentManifestPayload  []byte
	CreatedAt             time.Time
}

type RecordSandboxContractArtifactsParams struct {
	ContractID               string
	SandboxContractDigest    string
	NetworkHostsDigest       string
	ControlManifestDigest    string
	OCISpecDigest            string
	BundleDigest             string
	CheckpointMetadataDigest string
	Now                      time.Time
}

func CanonicalSandboxContractPayload(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return canonicalizeSandboxContractJSON(data)
}

func SandboxContractDigest(canonicalPayload []byte) string {
	sum := sha256.Sum256(canonicalPayload)
	return "sha256:" + fmt.Sprintf("%x", sum[:])
}

// RuntimeConfigInputDigest is the single source of truth for the runtime
// config digest: sha256 over the versioned prefix plus the canonical preimage
// bytes. Callers outside the store (e.g. the deployment preimage builder) must
// route through this so the digest stays byte-for-byte identical to what is
// persisted and re-validated here.
func RuntimeConfigInputDigest(canonicalPreimage []byte) string {
	sum := sha256.Sum256(append([]byte(runtimeConfigDigestPrefix), canonicalPreimage...))
	return "sha256:" + fmt.Sprintf("%x", sum[:])
}

func (s *Store) ensureSandboxContractInputEvidenceSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS sandbox_contract_input_evidence (
  contract_id TEXT PRIMARY KEY,
  runtime_config_digest TEXT NOT NULL,
  runtime_config_preimage TEXT NOT NULL,
  agent_manifest_digest TEXT NOT NULL,
  agent_manifest_payload TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(contract_id) REFERENCES sandbox_contracts(contract_id) ON DELETE CASCADE
);
`)
	return err
}

func VerifySandboxContractDigest(canonicalPayload []byte, digest string) error {
	canonical, err := canonicalizeSandboxContractJSON(canonicalPayload)
	if err != nil {
		return err
	}
	if !bytes.Equal(canonicalPayload, canonical) {
		return fmt.Errorf("sandbox contract payload is not canonical")
	}
	if got := SandboxContractDigest(canonicalPayload); got != strings.TrimSpace(digest) {
		return fmt.Errorf("sandbox contract digest mismatch: got %s want %s", got, strings.TrimSpace(digest))
	}
	return nil
}

func (s *Store) StoreSandboxContract(ctx context.Context, p StoreSandboxContractParams) (SandboxContractRecord, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	p.ContractID = strings.TrimSpace(p.ContractID)
	p.SessionID = strings.TrimSpace(p.SessionID)
	p.GenerationID = strings.TrimSpace(p.GenerationID)
	p.SandboxContractVersion = strings.TrimSpace(p.SandboxContractVersion)
	p.ContractGateVersion = strings.TrimSpace(p.ContractGateVersion)
	if p.ContractID == "" || p.SessionID == "" || p.GenerationID == "" {
		return SandboxContractRecord{}, fmt.Errorf("contract id, session id, and generation id are required")
	}
	if p.SandboxContractVersion == "" {
		return SandboxContractRecord{}, fmt.Errorf("sandbox contract version is required")
	}
	if p.SandboxContractVersion != SandboxContractVersion {
		return SandboxContractRecord{}, fmt.Errorf("unsupported sandbox contract version %q", p.SandboxContractVersion)
	}
	if p.ContractSchemaVersion == 0 {
		return SandboxContractRecord{}, fmt.Errorf("sandbox contract schema version is required")
	}
	if p.ContractGateVersion == "" {
		return SandboxContractRecord{}, fmt.Errorf("sandbox contract gate version is required")
	}
	canonicalPayload, err := CanonicalSandboxContractPayload(p.Payload)
	if err != nil {
		return SandboxContractRecord{}, err
	}
	if err := validateSandboxContractPayload(canonicalPayload, p); err != nil {
		return SandboxContractRecord{}, err
	}
	digest := SandboxContractDigest(canonicalPayload)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SandboxContractRecord{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := validateSandboxContractDriverStateTx(ctx, tx, p, canonicalPayload); err != nil {
		return SandboxContractRecord{}, err
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO sandbox_contracts (
  contract_id, generation_id, session_id, sandbox_contract_version,
  contract_schema_version, contract_gate_version,
  canonical_payload, sandbox_contract_digest, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(generation_id) DO NOTHING`,
		p.ContractID, p.GenerationID, p.SessionID, p.SandboxContractVersion,
		p.ContractSchemaVersion, p.ContractGateVersion,
		string(canonicalPayload), digest, formatTime(p.Now)); err != nil {
		return SandboxContractRecord{}, err
	}
	record, err := getSandboxContractForGenerationTx(ctx, tx, p.SessionID, p.GenerationID)
	if err != nil {
		return SandboxContractRecord{}, err
	}
	if record.ContractID != p.ContractID ||
		record.SandboxContractVersion != p.SandboxContractVersion ||
		record.ContractSchemaVersion != p.ContractSchemaVersion ||
		record.ContractGateVersion != p.ContractGateVersion ||
		record.SandboxContractDigest != digest ||
		!bytes.Equal(record.CanonicalPayload, canonicalPayload) {
		return SandboxContractRecord{}, fmt.Errorf("sandbox contract already exists with different immutable payload")
	}
	if err := updateSandboxContractMirrorsTx(ctx, tx, record); err != nil {
		return SandboxContractRecord{}, err
	}
	if err := recordSandboxContractInputEvidenceTx(ctx, tx, p, record.ContractID); err != nil {
		return SandboxContractRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return SandboxContractRecord{}, err
	}
	return record, nil
}

func (s *Store) GetSandboxContractForGeneration(ctx context.Context, sessionID, generationID string) (SandboxContractRecord, error) {
	return getSandboxContractForGenerationWithGenerationMirror(ctx, s.db, sessionID, generationID)
}

func recordSandboxContractInputEvidenceTx(ctx context.Context, tx *sql.Tx, p StoreSandboxContractParams, contractID string) error {
	hasEvidence := p.RuntimeConfigPreimage != nil || p.AgentManifestPayload != nil ||
		strings.TrimSpace(p.RuntimeConfigDigest) != "" || strings.TrimSpace(p.AgentManifestDigest) != ""
	if !hasEvidence {
		return nil
	}
	if p.RuntimeConfigPreimage == nil || p.AgentManifestPayload == nil {
		return fmt.Errorf("sandbox contract input evidence requires runtime config preimage and agent manifest payload")
	}
	runtimePreimage, err := CanonicalSandboxContractPayload(p.RuntimeConfigPreimage)
	if err != nil {
		return fmt.Errorf("canonicalize runtime config input evidence: %w", err)
	}
	runtimeDigest := strings.TrimSpace(p.RuntimeConfigDigest)
	if runtimeDigest == "" {
		runtimeDigest = RuntimeConfigInputDigest(runtimePreimage)
	}
	if want := RuntimeConfigInputDigest(runtimePreimage); runtimeDigest != want {
		return fmt.Errorf("runtime config input evidence digest mismatch: got %s want %s", runtimeDigest, want)
	}
	agentManifestPayload, err := CanonicalSandboxContractPayload(p.AgentManifestPayload)
	if err != nil {
		return fmt.Errorf("canonicalize agent manifest input evidence: %w", err)
	}
	agentManifestDigest := strings.TrimSpace(p.AgentManifestDigest)
	if agentManifestDigest == "" {
		agentManifestDigest = SandboxContractDigest(agentManifestPayload)
	}
	if want := SandboxContractDigest(agentManifestPayload); agentManifestDigest != want {
		return fmt.Errorf("agent manifest input evidence digest mismatch: got %s want %s", agentManifestDigest, want)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO sandbox_contract_input_evidence (
  contract_id, runtime_config_digest, runtime_config_preimage,
  agent_manifest_digest, agent_manifest_payload, created_at
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(contract_id) DO NOTHING`,
		contractID, runtimeDigest, string(runtimePreimage),
		agentManifestDigest, string(agentManifestPayload), formatTime(p.Now)); err != nil {
		return err
	}
	evidence, err := getSandboxContractInputEvidenceTx(ctx, tx, contractID)
	if err != nil {
		return err
	}
	if evidence.RuntimeConfigDigest != runtimeDigest ||
		!bytes.Equal(evidence.RuntimeConfigPreimage, runtimePreimage) ||
		evidence.AgentManifestDigest != agentManifestDigest ||
		!bytes.Equal(evidence.AgentManifestPayload, agentManifestPayload) {
		return fmt.Errorf("sandbox contract input evidence already exists with different immutable payload")
	}
	return nil
}

func (s *Store) GetSandboxContractInputEvidence(ctx context.Context, contractID string) (SandboxContractInputEvidence, error) {
	return getSandboxContractInputEvidenceTx(ctx, s.db, strings.TrimSpace(contractID))
}

func getSandboxContractForGenerationWithGenerationMirror(ctx context.Context, db dbRunner, sessionID, generationID string) (SandboxContractRecord, error) {
	row := db.QueryRowContext(ctx, `
SELECT
  sc.contract_id,
  sc.session_id,
  sc.generation_id,
  sc.sandbox_contract_version,
  sc.contract_schema_version,
  sc.contract_gate_version,
  sc.canonical_payload,
  sc.sandbox_contract_digest,
  sc.created_at,
  COALESCE(g.sandbox_contract_id, ''),
  COALESCE(g.sandbox_contract_version, '')
FROM sandbox_contracts sc
JOIN runtime_generations g ON g.generation_id = sc.generation_id
WHERE sc.session_id = ?
  AND sc.generation_id = ?`, strings.TrimSpace(sessionID), strings.TrimSpace(generationID))
	return scanSandboxContractWithGenerationMirror(row)
}

func (s *Store) RecordSandboxContractArtifacts(ctx context.Context, p RecordSandboxContractArtifactsParams) (SandboxContractArtifacts, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	p.ContractID = strings.TrimSpace(p.ContractID)
	if p.ContractID == "" {
		return SandboxContractArtifacts{}, fmt.Errorf("contract id is required")
	}
	if strings.TrimSpace(p.ControlManifestDigest) == "" ||
		strings.TrimSpace(p.OCISpecDigest) == "" ||
		strings.TrimSpace(p.BundleDigest) == "" {
		return SandboxContractArtifacts{}, fmt.Errorf("control manifest, OCI spec, and bundle digests are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SandboxContractArtifacts{}, err
	}
	defer func() { _ = tx.Rollback() }()

	storedDigest, err := sandboxContractDigestForIDTx(ctx, tx, p.ContractID)
	if err != nil {
		return SandboxContractArtifacts{}, err
	}
	if p.SandboxContractDigest = strings.TrimSpace(p.SandboxContractDigest); p.SandboxContractDigest == "" {
		p.SandboxContractDigest = storedDigest
	}
	if p.SandboxContractDigest != storedDigest {
		return SandboxContractArtifacts{}, fmt.Errorf("sandbox contract artifact digest mismatch: got %s want %s", p.SandboxContractDigest, storedDigest)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO sandbox_contract_artifacts (
  contract_id, sandbox_contract_digest, network_hosts_digest,
  control_manifest_digest, oci_spec_digest, bundle_digest,
  checkpoint_metadata_digest, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(contract_id) DO NOTHING`,
		p.ContractID, p.SandboxContractDigest, nullableString(p.NetworkHostsDigest),
		p.ControlManifestDigest, p.OCISpecDigest, p.BundleDigest,
		nullableString(p.CheckpointMetadataDigest), formatTime(p.Now)); err != nil {
		return SandboxContractArtifacts{}, err
	}
	artifacts, err := getSandboxContractArtifactsTx(ctx, tx, p.ContractID)
	if err != nil {
		return SandboxContractArtifacts{}, err
	}
	if artifacts.SandboxContractDigest != p.SandboxContractDigest ||
		artifacts.NetworkHostsDigest != strings.TrimSpace(p.NetworkHostsDigest) ||
		artifacts.ControlManifestDigest != p.ControlManifestDigest ||
		artifacts.OCISpecDigest != p.OCISpecDigest ||
		artifacts.BundleDigest != p.BundleDigest ||
		artifacts.CheckpointMetadataDigest != strings.TrimSpace(p.CheckpointMetadataDigest) {
		return SandboxContractArtifacts{}, fmt.Errorf("sandbox contract artifacts already exist with different immutable digests")
	}
	if err := tx.Commit(); err != nil {
		return SandboxContractArtifacts{}, err
	}
	return artifacts, nil
}

func canonicalizeSandboxContractJSON(data []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var normalized any
	if err := decoder.Decode(&normalized); err != nil {
		return nil, fmt.Errorf("decode sandbox contract payload: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("sandbox contract payload contains trailing JSON")
	}
	object, ok := normalized.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("sandbox contract payload must be a JSON object")
	}
	if _, ok := object["sandbox_contract_digest"]; ok {
		return nil, fmt.Errorf("sandbox contract payload must not contain sandbox_contract_digest")
	}
	if err := rejectFloatingPointJSONNumbers(object); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(object); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func rejectFloatingPointJSONNumbers(value any) error {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if err := rejectFloatingPointJSONNumbers(child); err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
		}
	case []any:
		for i, child := range v {
			if err := rejectFloatingPointJSONNumbers(child); err != nil {
				return fmt.Errorf("[%d]: %w", i, err)
			}
		}
	case json.Number:
		if strings.ContainsAny(v.String(), ".eE") {
			return fmt.Errorf("sandbox contract payload numbers must be integers, got %s", v.String())
		}
	}
	return nil
}

func validateSandboxContractPayload(canonicalPayload []byte, p StoreSandboxContractParams) error {
	object, err := decodeSandboxContractObject(canonicalPayload)
	if err != nil {
		return err
	}
	if got, _ := object["sandbox_contract_version"].(string); got != p.SandboxContractVersion {
		return fmt.Errorf("sandbox contract payload version = %q, want %q", got, p.SandboxContractVersion)
	}
	if got, _ := object["contract_id"].(string); got != p.ContractID {
		return fmt.Errorf("sandbox contract payload contract_id = %q, want %q", got, p.ContractID)
	}
	if got, _ := object["session_id"].(string); got != p.SessionID {
		return fmt.Errorf("sandbox contract payload session_id = %q, want %q", got, p.SessionID)
	}
	if got, _ := object["generation_id"].(string); got != p.GenerationID {
		return fmt.Errorf("sandbox contract payload generation_id = %q, want %q", got, p.GenerationID)
	}
	schemaVersion, ok := object["contract_schema_version"].(json.Number)
	if !ok || schemaVersion.String() != fmt.Sprint(p.ContractSchemaVersion) {
		return fmt.Errorf("sandbox contract payload contract_schema_version = %v, want %d", object["contract_schema_version"], p.ContractSchemaVersion)
	}
	if p.ContractSchemaVersion != SandboxContractSchemaVersion {
		return fmt.Errorf("unsupported sandbox contract schema version %d", p.ContractSchemaVersion)
	}
	if got, _ := object["contract_gate_version"].(string); got != p.ContractGateVersion {
		return fmt.Errorf("sandbox contract payload contract_gate_version = %q, want %q", got, p.ContractGateVersion)
	}
	if p.ContractGateVersion != SandboxContractGateDriverManifest {
		return fmt.Errorf("unsupported sandbox contract gate version %q", p.ContractGateVersion)
	}
	if err := validateSandboxContractV2Semantics(object, p.ContractGateVersion); err != nil {
		return err
	}
	return nil
}

func decodeSandboxContractObject(canonicalPayload []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(canonicalPayload))
	decoder.UseNumber()
	var normalized any
	if err := decoder.Decode(&normalized); err != nil {
		return nil, err
	}
	object, ok := normalized.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("sandbox contract payload must be a JSON object")
	}
	return object, nil
}

func getSandboxContractForGenerationTx(ctx context.Context, tx *sql.Tx, sessionID, generationID string) (SandboxContractRecord, error) {
	row := tx.QueryRowContext(ctx, `
SELECT
  sc.contract_id,
  sc.session_id,
  sc.generation_id,
  sc.sandbox_contract_version,
  sc.contract_schema_version,
  sc.contract_gate_version,
  sc.canonical_payload,
  sc.sandbox_contract_digest,
  sc.created_at
FROM sandbox_contracts sc
WHERE sc.session_id = ?
  AND sc.generation_id = ?`, strings.TrimSpace(sessionID), strings.TrimSpace(generationID))
	return scanSandboxContract(row)
}

func updateSandboxContractMirrorsTx(ctx context.Context, tx *sql.Tx, record SandboxContractRecord) error {
	res, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET sandbox_contract_id = ?,
    sandbox_contract_version = ?
WHERE generation_id = ?
  AND session_id = ?
  AND (sandbox_contract_id IS NULL OR sandbox_contract_id = ?)
  AND (sandbox_contract_version IS NULL OR sandbox_contract_version = ?)`,
		record.ContractID, record.SandboxContractVersion, record.GenerationID, record.SessionID,
		record.ContractID, record.SandboxContractVersion)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("runtime generation sandbox contract mirror CAS failed")
	}
	res, err = tx.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET contract_id = ?,
    sandbox_contract_version = ?
WHERE generation_id = ?
  AND (contract_id IS NULL OR contract_id = ?)
  AND (sandbox_contract_version IS NULL OR sandbox_contract_version = ?)`,
		record.ContractID, record.SandboxContractVersion, record.GenerationID,
		record.ContractID, record.SandboxContractVersion)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("runtime resource sandbox contract mirror CAS failed")
	}
	return nil
}

type sandboxContractScanner interface {
	Scan(dest ...any) error
}

func scanSandboxContract(row sandboxContractScanner) (SandboxContractRecord, error) {
	var record SandboxContractRecord
	var payload, createdAt string
	if err := row.Scan(
		&record.ContractID,
		&record.SessionID,
		&record.GenerationID,
		&record.SandboxContractVersion,
		&record.ContractSchemaVersion,
		&record.ContractGateVersion,
		&payload,
		&record.SandboxContractDigest,
		&createdAt,
	); err != nil {
		return SandboxContractRecord{}, err
	}
	record.CanonicalPayload = []byte(payload)
	record.CreatedAt = parseTime(createdAt)
	if err := VerifySandboxContractDigest(record.CanonicalPayload, record.SandboxContractDigest); err != nil {
		return SandboxContractRecord{}, err
	}
	if err := validateLoadedSandboxContract(record); err != nil {
		return SandboxContractRecord{}, err
	}
	return record, nil
}

func scanSandboxContractWithGenerationMirror(row sandboxContractScanner) (SandboxContractRecord, error) {
	var record SandboxContractRecord
	var payload, createdAt, generationContractID, generationContractVersion string
	if err := row.Scan(
		&record.ContractID,
		&record.SessionID,
		&record.GenerationID,
		&record.SandboxContractVersion,
		&record.ContractSchemaVersion,
		&record.ContractGateVersion,
		&payload,
		&record.SandboxContractDigest,
		&createdAt,
		&generationContractID,
		&generationContractVersion,
	); err != nil {
		return SandboxContractRecord{}, err
	}
	record.CanonicalPayload = []byte(payload)
	record.CreatedAt = parseTime(createdAt)
	if err := VerifySandboxContractDigest(record.CanonicalPayload, record.SandboxContractDigest); err != nil {
		return SandboxContractRecord{}, err
	}
	if err := validateLoadedSandboxContract(record); err != nil {
		return SandboxContractRecord{}, err
	}
	if generationContractID != record.ContractID ||
		generationContractVersion != record.SandboxContractVersion {
		return SandboxContractRecord{}, fmt.Errorf("sandbox contract generation mirror does not match contract row")
	}
	return record, nil
}

func validateLoadedSandboxContract(record SandboxContractRecord) error {
	if record.ContractSchemaVersion != SandboxContractSchemaVersion {
		return fmt.Errorf("sandbox contract row schema version = %d, want %d", record.ContractSchemaVersion, SandboxContractSchemaVersion)
	}
	if record.ContractGateVersion != SandboxContractGateDriverManifest {
		return fmt.Errorf("unsupported sandbox contract row gate version %q", record.ContractGateVersion)
	}
	object, err := decodeSandboxContractObject(record.CanonicalPayload)
	if err != nil {
		return err
	}
	schemaVersion, ok := object["contract_schema_version"].(json.Number)
	if !ok || schemaVersion.String() != fmt.Sprint(record.ContractSchemaVersion) {
		return fmt.Errorf("sandbox contract row schema version does not match payload")
	}
	if gateVersion, _ := object["contract_gate_version"].(string); gateVersion != record.ContractGateVersion {
		return fmt.Errorf("sandbox contract row gate version does not match payload")
	}
	if err := validateSandboxContractV2Semantics(object, record.ContractGateVersion); err != nil {
		return err
	}
	return nil
}

func validateSandboxContractV2Semantics(object map[string]any, gateVersion string) error {
	driver, ok := object["driver"].(map[string]any)
	if !ok {
		return fmt.Errorf("sandbox contract missing driver object")
	}
	driverID, _ := driver["driver_id"].(string)
	driverSpec, ok := agents.DriverSpecFor(driverID)
	if !ok {
		return fmt.Errorf("unsupported sandbox contract driver_id %q", driverID)
	}
	runtimeProvider, ok := object["runtime_provider"].(map[string]any)
	if !ok {
		return fmt.Errorf("sandbox contract missing runtime_provider object")
	}
	providerID, _ := runtimeProvider["provider_id"].(string)
	providerSpec, ok := agents.RuntimeProviderSpecFor(providerID)
	if !ok {
		return fmt.Errorf("unsupported runtime provider %q", providerID)
	}
	driverRuntime, ok := object["driver_runtime"].(map[string]any)
	if !ok {
		return fmt.Errorf("sandbox contract missing driver_runtime object")
	}
	if digest, _ := driverRuntime["initial_driver_state_digest"].(string); !strings.HasPrefix(digest, "sha256:") {
		return fmt.Errorf("driver runtime initial_driver_state_digest is required")
	}
	if err := validateDriverConfigMaterialization(object, driverRuntime, driverSpec.ID); err != nil {
		return err
	}
	inputDigests, ok := object["input_digests"].(map[string]any)
	if !ok {
		return fmt.Errorf("sandbox contract missing input_digests object")
	}
	if gateVersion != SandboxContractGateDriverManifest {
		return fmt.Errorf("unsupported sandbox contract gate version %q", gateVersion)
	}
	for _, key := range []string{"runtime_config_digest", "agent_manifest_digest"} {
		value, _ := inputDigests[key].(string)
		if !strings.HasPrefix(value, "sha256:") {
			return fmt.Errorf("driver manifest input digest %s is required", key)
		}
	}
	credentialPolicyRaw, ok := object["credential_policy"].(map[string]any)
	if !ok {
		return fmt.Errorf("sandbox contract missing credential_policy object")
	}
	declaredDigest, _ := credentialPolicyRaw["digest"].(string)
	computedDigest, err := CredentialPolicyDigest(credentialPolicyRaw)
	if err != nil {
		return err
	}
	if declaredDigest != computedDigest {
		return fmt.Errorf("credential policy digest mismatch: got %s want %s", declaredDigest, computedDigest)
	}
	policy, err := normalizeCredentialPolicy(credentialPolicyRaw)
	if err != nil {
		return err
	}
	identityModelAccess := boolFromObjectPath(object, "identity", "model_access_allowed")
	modelAccess := boolFromObjectPath(object, "model_access", "model_access_allowed")
	modelAccessEnabled := identityModelAccess && modelAccess
	return validateCredentialPolicyGrantSemantics(policy, driverSpec, providerSpec, modelAccessEnabled)
}

func validateDriverConfigMaterialization(contract map[string]any, driverRuntime map[string]any, driverID agents.ID) error {
	specs := agents.DriverConfigMaterializationSpecsFor(driverID)
	materialized, ok := driverRuntime["materialized_driver_config"].(map[string]any)
	if !ok {
		if len(specs) == 0 {
			return nil
		}
		return fmt.Errorf("%s driver runtime missing materialized_driver_config", driverID)
	}
	if len(specs) == 0 {
		if len(materialized) != 0 {
			return fmt.Errorf("driver %s does not support driver config materialization", driverID)
		}
		if mountPlan, ok := contract["mount_plan"].(map[string]any); ok {
			if mountMaterialized, ok := mountPlan["driver_config_materializations"].(map[string]any); ok && len(mountMaterialized) != 0 {
				return fmt.Errorf("driver %s does not support driver config materialization", driverID)
			}
		}
		return nil
	}
	mountPlan, ok := contract["mount_plan"].(map[string]any)
	if !ok {
		return fmt.Errorf("%s sandbox contract missing mount_plan", driverID)
	}
	mountMaterialized, ok := mountPlan["driver_config_materializations"].(map[string]any)
	if !ok {
		return fmt.Errorf("%s mount plan missing driver_config_materializations", driverID)
	}
	expected := map[string]agents.DriverConfigMaterializationSpec{}
	for _, spec := range specs {
		expected[spec.Name] = spec
	}
	if len(materialized) != len(expected) || len(mountMaterialized) != len(expected) {
		return fmt.Errorf("%s driver config materialization must contain exactly %d projections", driverID, len(expected))
	}
	for name, want := range expected {
		runtimeEntry, ok := materialized[name].(map[string]any)
		if !ok {
			return fmt.Errorf("%s driver runtime missing %s materialization", driverID, name)
		}
		mountEntry, ok := mountMaterialized[name].(map[string]any)
		if !ok {
			return fmt.Errorf("%s mount plan missing %s materialization", driverID, name)
		}
		if source, _ := runtimeEntry["source_projection_path"].(string); source != want.SourceProjectionPath {
			return fmt.Errorf("%s runtime %s source_projection_path = %q", driverID, name, source)
		}
		if digest, _ := runtimeEntry["source_digest"].(string); !strings.HasPrefix(digest, "sha256:") {
			return fmt.Errorf("%s runtime %s source_digest is required", driverID, name)
		}
		if destination, _ := runtimeEntry["sandbox_destination"].(string); destination != want.SandboxDestination {
			return fmt.Errorf("%s runtime %s sandbox_destination = %q", driverID, name, destination)
		}
		if mutable, _ := runtimeEntry["destination_mutable_by_sandbox"].(bool); mutable != want.DestinationMutableBySandbox {
			return fmt.Errorf("%s runtime %s destination mutability mismatch", driverID, name)
		}
		if typ, _ := mountEntry["type"].(string); typ != want.MountType {
			return fmt.Errorf("%s mount %s type = %q", driverID, name, typ)
		}
		if mode, _ := mountEntry["mode"].(string); mode != want.MountMode {
			return fmt.Errorf("%s mount %s mode = %q", driverID, name, mode)
		}
		if exact, _ := mountEntry["exact"].(bool); exact != want.MountExact {
			return fmt.Errorf("%s mount %s exactness mismatch", driverID, name)
		}
		if source, _ := mountEntry["source_projection_path"].(string); source != want.SourceProjectionPath {
			return fmt.Errorf("%s mount %s source_projection_path = %q", driverID, name, source)
		}
		if destination, _ := mountEntry["sandbox_destination"].(string); destination != want.SandboxDestination {
			return fmt.Errorf("%s mount %s sandbox_destination = %q", driverID, name, destination)
		}
		if mutable, _ := mountEntry["destination_mutable_by_sandbox"].(bool); mutable != want.DestinationMutableBySandbox {
			return fmt.Errorf("%s mount %s destination mutability mismatch", driverID, name)
		}
	}
	return nil
}

func boolFromObjectPath(object map[string]any, parent, child string) bool {
	nested, ok := object[parent].(map[string]any)
	if !ok {
		return false
	}
	value, _ := nested[child].(bool)
	return value
}

func validateSandboxContractDriverStateTx(ctx context.Context, tx *sql.Tx, p StoreSandboxContractParams, canonicalPayload []byte) error {
	if strings.TrimSpace(p.DriverState.DriverID) == "" {
		return nil
	}
	if p.DriverState.StateVersion <= 0 || !strings.HasPrefix(strings.TrimSpace(p.DriverState.StateDigest), "sha256:") {
		return fmt.Errorf("sandbox contract driver state token is invalid")
	}
	object, err := decodeSandboxContractObject(canonicalPayload)
	if err != nil {
		return err
	}
	driver, ok := object["driver"].(map[string]any)
	if !ok {
		return fmt.Errorf("sandbox contract missing driver object")
	}
	if got, _ := driver["driver_id"].(string); got != p.DriverState.DriverID {
		return fmt.Errorf("sandbox contract driver_id = %q, want %q", got, p.DriverState.DriverID)
	}
	driverRuntime, ok := object["driver_runtime"].(map[string]any)
	if !ok {
		return fmt.Errorf("sandbox contract missing driver_runtime object")
	}
	if got, _ := driverRuntime["initial_driver_state_digest"].(string); got != p.DriverState.StateDigest {
		return fmt.Errorf("sandbox contract initial driver state digest = %q, want %q", got, p.DriverState.StateDigest)
	}
	current, err := getDriverStateTx(ctx, tx, p.SessionID, p.DriverState.DriverID)
	if err != nil {
		return err
	}
	if current.StateDigest != p.DriverState.StateDigest || current.StateVersion != p.DriverState.StateVersion {
		return fmt.Errorf("sandbox contract driver state token is stale")
	}
	query := `
SELECT COUNT(*)
FROM runtime_generations g
JOIN sessions s ON s.id = g.session_id
JOIN agent_runtime_profiles a ON a.agent_runtime_profile_id = g.agent_runtime_profile_id
WHERE g.session_id = ?
  AND g.generation_id = ?
  AND a.driver_id = ?
  AND s.active_generation_id = ?`
	args := []any{p.SessionID, p.GenerationID, p.DriverState.DriverID, p.GenerationID}
	if strings.TrimSpace(p.Owner) != "" {
		query += `
  AND g.lease_owner = ?
  AND g.lease_expires_at > ?`
		args = append(args, p.Owner, formatTime(p.Now))
	}
	var matches int
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&matches); err != nil {
		return err
	}
	if matches != 1 {
		return fmt.Errorf("sandbox contract generation driver state lease check failed")
	}
	return nil
}

func sandboxContractDigestForIDTx(ctx context.Context, tx *sql.Tx, contractID string) (string, error) {
	var digest string
	err := tx.QueryRowContext(ctx, `
SELECT sandbox_contract_digest
FROM sandbox_contracts
WHERE contract_id = ?`, contractID).Scan(&digest)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("sandbox contract %s not found", contractID)
	}
	return digest, err
}

func getSandboxContractArtifactsTx(ctx context.Context, tx *sql.Tx, contractID string) (SandboxContractArtifacts, error) {
	row := tx.QueryRowContext(ctx, `
SELECT
  contract_id,
  sandbox_contract_digest,
  COALESCE(network_hosts_digest, ''),
  control_manifest_digest,
  oci_spec_digest,
  bundle_digest,
  COALESCE(checkpoint_metadata_digest, ''),
  created_at
FROM sandbox_contract_artifacts
WHERE contract_id = ?`, contractID)
	var artifacts SandboxContractArtifacts
	var createdAt string
	if err := row.Scan(
		&artifacts.ContractID,
		&artifacts.SandboxContractDigest,
		&artifacts.NetworkHostsDigest,
		&artifacts.ControlManifestDigest,
		&artifacts.OCISpecDigest,
		&artifacts.BundleDigest,
		&artifacts.CheckpointMetadataDigest,
		&createdAt,
	); err != nil {
		return SandboxContractArtifacts{}, err
	}
	artifacts.CreatedAt = parseTime(createdAt)
	return artifacts, nil
}

func getSandboxContractInputEvidenceTx(ctx context.Context, db dbRunner, contractID string) (SandboxContractInputEvidence, error) {
	row := db.QueryRowContext(ctx, `
SELECT
  contract_id,
  runtime_config_digest,
  runtime_config_preimage,
  agent_manifest_digest,
  agent_manifest_payload,
  created_at
FROM sandbox_contract_input_evidence
WHERE contract_id = ?`, strings.TrimSpace(contractID))
	var evidence SandboxContractInputEvidence
	var runtimePreimage, agentManifestPayload, createdAt string
	if err := row.Scan(
		&evidence.ContractID,
		&evidence.RuntimeConfigDigest,
		&runtimePreimage,
		&evidence.AgentManifestDigest,
		&agentManifestPayload,
		&createdAt,
	); err != nil {
		return SandboxContractInputEvidence{}, err
	}
	evidence.RuntimeConfigPreimage = []byte(runtimePreimage)
	evidence.AgentManifestPayload = []byte(agentManifestPayload)
	evidence.CreatedAt = parseTime(createdAt)
	return evidence, nil
}
