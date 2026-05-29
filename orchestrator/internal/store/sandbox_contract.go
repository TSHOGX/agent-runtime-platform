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
	"sort"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/agents"
)

const SandboxContractVersion = "sandbox-isolation-v1"
const SandboxContractSchemaVersion = 2
const SandboxContractGatePhase9A = "phase9a"
const SandboxContractGatePhase9C = "phase9c"
const credentialPolicyDigestPrefix = "credential_policy_digest_v1\n"

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
	if strings.TrimSpace(p.SandboxContractVersion) == "" {
		p.SandboxContractVersion = SandboxContractVersion
	} else {
		p.SandboxContractVersion = strings.TrimSpace(p.SandboxContractVersion)
	}
	if p.ContractID == "" {
		p.ContractID = "contract_" + p.GenerationID
	}
	if p.ContractID == "contract_" || p.SessionID == "" || p.GenerationID == "" {
		return SandboxContractRecord{}, fmt.Errorf("contract id, session id, and generation id are required")
	}
	if p.SandboxContractVersion != SandboxContractVersion {
		return SandboxContractRecord{}, fmt.Errorf("unsupported sandbox contract version %q", p.SandboxContractVersion)
	}
	if p.ContractSchemaVersion == 0 {
		p.ContractSchemaVersion = SandboxContractSchemaVersion
	}
	if strings.TrimSpace(p.ContractGateVersion) == "" {
		p.ContractGateVersion = SandboxContractGatePhase9A
	} else {
		p.ContractGateVersion = strings.TrimSpace(p.ContractGateVersion)
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
	if err := tx.Commit(); err != nil {
		return SandboxContractRecord{}, err
	}
	return record, nil
}

func (s *Store) GetSandboxContractForGeneration(ctx context.Context, sessionID, generationID string) (SandboxContractRecord, error) {
	return getSandboxContractForGenerationWithMirrors(ctx, s.db, sessionID, generationID)
}

func getSandboxContractForGenerationWithMirrors(ctx context.Context, db dbRunner, sessionID, generationID string) (SandboxContractRecord, error) {
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
  COALESCE(g.sandbox_contract_version, ''),
  COALESCE(r.contract_id, ''),
  COALESCE(r.sandbox_contract_version, '')
FROM sandbox_contracts sc
JOIN runtime_generations g ON g.generation_id = sc.generation_id
JOIN runtime_generation_resources r ON r.generation_id = sc.generation_id
WHERE sc.session_id = ?
  AND sc.generation_id = ?`, strings.TrimSpace(sessionID), strings.TrimSpace(generationID))
	return scanSandboxContractWithMirrors(row)
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
	switch p.ContractGateVersion {
	case SandboxContractGatePhase9A, SandboxContractGatePhase9C:
	default:
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

func scanSandboxContractWithMirrors(row sandboxContractScanner) (SandboxContractRecord, error) {
	var record SandboxContractRecord
	var payload, createdAt, generationContractID, generationContractVersion, resourceContractID, resourceContractVersion string
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
		&resourceContractID,
		&resourceContractVersion,
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
		generationContractVersion != record.SandboxContractVersion ||
		resourceContractID != record.ContractID ||
		resourceContractVersion != record.SandboxContractVersion {
		return SandboxContractRecord{}, fmt.Errorf("sandbox contract mirrors do not match contract row")
	}
	return record, nil
}

func validateLoadedSandboxContract(record SandboxContractRecord) error {
	if record.ContractSchemaVersion != SandboxContractSchemaVersion {
		return fmt.Errorf("sandbox contract row schema version = %d, want %d", record.ContractSchemaVersion, SandboxContractSchemaVersion)
	}
	if record.ContractGateVersion != SandboxContractGatePhase9A && record.ContractGateVersion != SandboxContractGatePhase9C {
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
	if driverSpec.ID == agents.Pi {
		if err := validatePiDriverConfigMaterialization(object, driverRuntime); err != nil {
			return err
		}
	}
	inputDigests, ok := object["input_digests"].(map[string]any)
	if !ok {
		return fmt.Errorf("sandbox contract missing input_digests object")
	}
	switch gateVersion {
	case SandboxContractGatePhase9A:
		for _, key := range []string{"runtime_config_digest", "rootfs_image_digest", "agent_manifest_digest"} {
			if value, ok := inputDigests[key]; !ok || value != nil {
				return fmt.Errorf("phase9a input digest %s must be null", key)
			}
		}
	case SandboxContractGatePhase9C:
		for _, key := range []string{"runtime_config_digest", "agent_manifest_digest"} {
			value, _ := inputDigests[key].(string)
			if !strings.HasPrefix(value, "sha256:") {
				return fmt.Errorf("phase9c input digest %s is required", key)
			}
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

func validatePiDriverConfigMaterialization(contract map[string]any, driverRuntime map[string]any) error {
	materialized, ok := driverRuntime["materialized_driver_config"].(map[string]any)
	if !ok {
		return fmt.Errorf("pi driver runtime missing materialized_driver_config")
	}
	mountPlan, ok := contract["mount_plan"].(map[string]any)
	if !ok {
		return fmt.Errorf("pi sandbox contract missing mount_plan")
	}
	mountMaterialized, ok := mountPlan["driver_config_materializations"].(map[string]any)
	if !ok {
		return fmt.Errorf("pi mount plan missing driver_config_materializations")
	}
	expected := map[string]struct {
		source      string
		destination string
	}{
		"models":   {source: agents.PiModelsConfigPath, destination: agents.PiModelsSandboxPath},
		"settings": {source: agents.PiSettingsConfigPath, destination: agents.PiSettingsSandboxPath},
	}
	if len(materialized) != len(expected) || len(mountMaterialized) != len(expected) {
		return fmt.Errorf("pi driver config materialization must contain exactly models and settings")
	}
	for name, want := range expected {
		runtimeEntry, ok := materialized[name].(map[string]any)
		if !ok {
			return fmt.Errorf("pi driver runtime missing %s materialization", name)
		}
		mountEntry, ok := mountMaterialized[name].(map[string]any)
		if !ok {
			return fmt.Errorf("pi mount plan missing %s materialization", name)
		}
		if source, _ := runtimeEntry["source_projection_path"].(string); source != want.source {
			return fmt.Errorf("pi runtime %s source_projection_path = %q", name, source)
		}
		if digest, _ := runtimeEntry["source_digest"].(string); !strings.HasPrefix(digest, "sha256:") {
			return fmt.Errorf("pi runtime %s source_digest is required", name)
		}
		if destination, _ := runtimeEntry["sandbox_destination"].(string); destination != want.destination {
			return fmt.Errorf("pi runtime %s sandbox_destination = %q", name, destination)
		}
		if mutable, _ := runtimeEntry["destination_mutable_by_sandbox"].(bool); mutable {
			return fmt.Errorf("pi runtime %s destination must be immutable", name)
		}
		if typ, _ := mountEntry["type"].(string); typ != "bind" {
			return fmt.Errorf("pi mount %s type = %q", name, typ)
		}
		if mode, _ := mountEntry["mode"].(string); mode != "ro" {
			return fmt.Errorf("pi mount %s mode = %q", name, mode)
		}
		if exact, _ := mountEntry["exact"].(bool); !exact {
			return fmt.Errorf("pi mount %s must be exact", name)
		}
		if source, _ := mountEntry["source_projection_path"].(string); source != want.source {
			return fmt.Errorf("pi mount %s source_projection_path = %q", name, source)
		}
		if destination, _ := mountEntry["sandbox_destination"].(string); destination != want.destination {
			return fmt.Errorf("pi mount %s sandbox_destination = %q", name, destination)
		}
		if mutable, _ := mountEntry["destination_mutable_by_sandbox"].(bool); mutable {
			return fmt.Errorf("pi mount %s destination must be immutable", name)
		}
	}
	return nil
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

func boolFromObjectPath(object map[string]any, parent, child string) bool {
	nested, ok := object[parent].(map[string]any)
	if !ok {
		return false
	}
	value, _ := nested[child].(bool)
	return value
}

func stringSliceContains(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
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
