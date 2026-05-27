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
)

const SandboxContractVersion = "sandbox-isolation-v1"

type SandboxContractRecord struct {
	ContractID             string
	SessionID              string
	GenerationID           string
	SandboxContractVersion string
	CanonicalPayload       []byte
	SandboxContractDigest  string
	CreatedAt              time.Time
}

type StoreSandboxContractParams struct {
	ContractID             string
	SessionID              string
	GenerationID           string
	SandboxContractVersion string
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

	if _, err := tx.ExecContext(ctx, `
INSERT INTO sandbox_contracts (
  contract_id, generation_id, session_id, sandbox_contract_version,
  canonical_payload, sandbox_contract_digest, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(generation_id) DO NOTHING`,
		p.ContractID, p.GenerationID, p.SessionID, p.SandboxContractVersion,
		string(canonicalPayload), digest, formatTime(p.Now)); err != nil {
		return SandboxContractRecord{}, err
	}
	record, err := getSandboxContractForGenerationTx(ctx, tx, p.SessionID, p.GenerationID)
	if err != nil {
		return SandboxContractRecord{}, err
	}
	if record.ContractID != p.ContractID ||
		record.SandboxContractVersion != p.SandboxContractVersion ||
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
	if !ok || schemaVersion.String() != "1" {
		return fmt.Errorf("sandbox contract payload contract_schema_version = %v, want 1", object["contract_schema_version"])
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
	if generationContractID != record.ContractID ||
		generationContractVersion != record.SandboxContractVersion ||
		resourceContractID != record.ContractID ||
		resourceContractVersion != record.SandboxContractVersion {
		return SandboxContractRecord{}, fmt.Errorf("sandbox contract mirrors do not match contract row")
	}
	return record, nil
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
