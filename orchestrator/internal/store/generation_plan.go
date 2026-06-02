package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

const GenerationPlanVersion = 1
const GenerationPlanProjectionVersion = 1

const (
	GenerationPlanProjectionSandboxContract          = "sandbox_contract"
	GenerationPlanProjectionControlManifest          = "control_manifest"
	GenerationPlanProjectionControlManifestProjected = "control_manifest_projected"
	GenerationPlanProjectionOCISpec                  = "oci_spec"
	GenerationPlanProjectionBundle                   = "bundle"
	GenerationPlanProjectionRuntimeConfig            = "runtime_config"
)

var generationPlanProjectionKinds = []string{
	GenerationPlanProjectionSandboxContract,
	GenerationPlanProjectionControlManifest,
	GenerationPlanProjectionControlManifestProjected,
	GenerationPlanProjectionOCISpec,
	GenerationPlanProjectionBundle,
	GenerationPlanProjectionRuntimeConfig,
}

type GenerationPlanRecord struct {
	GenerationID     string
	PlanVersion      int
	CanonicalPayload []byte
	PlanDigest       string
	CreatedAt        time.Time
}

type StoreGenerationPlanParams struct {
	GenerationID string
	PlanVersion  int
	Payload      any
	PlanDigest   string
	Now          time.Time
}

type GenerationPlanProjectionRecord struct {
	GenerationID      string
	PlanDigest        string
	ProjectionKind    string
	ProjectionVersion int
	PayloadDigest     string
	MaterializedPath  string
	CreatedAt         time.Time
}

type StoreGenerationPlanProjectionParams struct {
	GenerationID      string
	PlanDigest        string
	ProjectionKind    string
	ProjectionVersion int
	PayloadDigest     string
	MaterializedPath  string
	Now               time.Time
}

type VerifyGenerationPlanProjectionsParams struct {
	GenerationID string
	PlanDigest   string
	Expected     []GenerationPlanProjectionExpectation
	RequirePlan  bool
}

type GenerationPlanProjectionExpectation struct {
	ProjectionKind    string
	ProjectionVersion int
	PayloadDigest     string
	MaterializedPath  string
}

func GenerationPlanProjectionKinds() []string {
	return append([]string(nil), generationPlanProjectionKinds...)
}

func GenerationPlanProjectionVersionFor(kind string) (int, bool) {
	switch strings.TrimSpace(kind) {
	case GenerationPlanProjectionSandboxContract,
		GenerationPlanProjectionControlManifest,
		GenerationPlanProjectionControlManifestProjected,
		GenerationPlanProjectionOCISpec,
		GenerationPlanProjectionBundle,
		GenerationPlanProjectionRuntimeConfig:
		return GenerationPlanProjectionVersion, true
	default:
		return 0, false
	}
}

func GenerationPlanDigest(canonicalPayload []byte) string {
	return SandboxContractDigest(canonicalPayload)
}

func CanonicalGenerationPlanPayload(value any) ([]byte, error) {
	var (
		data []byte
		err  error
	)
	switch v := value.(type) {
	case []byte:
		data, err = canonicalDataVolumeJSONBytes(v)
	default:
		data, err = canonicalDataVolumeJSON(value)
	}
	if err != nil {
		return nil, fmt.Errorf("canonicalize generation plan payload: %w", err)
	}
	if len(data) == 0 || data[0] != '{' {
		return nil, fmt.Errorf("generation plan payload must be a JSON object")
	}
	return data, nil
}

func VerifyGenerationPlanDigest(canonicalPayload []byte, digest string) error {
	canonical, err := CanonicalGenerationPlanPayload(canonicalPayload)
	if err != nil {
		return err
	}
	if !bytes.Equal(canonicalPayload, canonical) {
		return fmt.Errorf("generation plan payload is not canonical")
	}
	if got := GenerationPlanDigest(canonicalPayload); got != strings.TrimSpace(digest) {
		return fmt.Errorf("generation plan digest mismatch: got %s want %s", got, strings.TrimSpace(digest))
	}
	return nil
}

func (s *Store) StoreGenerationPlan(ctx context.Context, p StoreGenerationPlanParams) (GenerationPlanRecord, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	p.GenerationID = strings.TrimSpace(p.GenerationID)
	p.PlanDigest = strings.TrimSpace(p.PlanDigest)
	if p.GenerationID == "" {
		return GenerationPlanRecord{}, fmt.Errorf("generation id is required")
	}
	if p.PlanVersion == 0 {
		p.PlanVersion = GenerationPlanVersion
	}
	if p.PlanVersion != GenerationPlanVersion {
		return GenerationPlanRecord{}, fmt.Errorf("unsupported generation plan version %d", p.PlanVersion)
	}
	canonicalPayload, err := CanonicalGenerationPlanPayload(p.Payload)
	if err != nil {
		return GenerationPlanRecord{}, err
	}
	digest := GenerationPlanDigest(canonicalPayload)
	if p.PlanDigest != "" && p.PlanDigest != digest {
		return GenerationPlanRecord{}, fmt.Errorf("generation plan digest mismatch: got %s want %s", p.PlanDigest, digest)
	}
	if p.PlanDigest == "" {
		p.PlanDigest = digest
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GenerationPlanRecord{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO generation_plans (
  generation_id, plan_version, canonical_payload, plan_digest, created_at
) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(generation_id) DO NOTHING`,
		p.GenerationID, p.PlanVersion, string(canonicalPayload), p.PlanDigest, formatTime(p.Now)); err != nil {
		return GenerationPlanRecord{}, err
	}
	record, err := getGenerationPlanTx(ctx, tx, p.GenerationID)
	if err != nil {
		return GenerationPlanRecord{}, err
	}
	if record.PlanVersion != p.PlanVersion ||
		record.PlanDigest != p.PlanDigest ||
		!bytes.Equal(record.CanonicalPayload, canonicalPayload) {
		return GenerationPlanRecord{}, fmt.Errorf("generation plan already exists with different immutable payload")
	}
	if err := tx.Commit(); err != nil {
		return GenerationPlanRecord{}, err
	}
	return record, nil
}

func (s *Store) GetGenerationPlan(ctx context.Context, generationID string) (GenerationPlanRecord, error) {
	return getGenerationPlanTx(ctx, s.db, strings.TrimSpace(generationID))
}

func (s *Store) RequireGenerationPlanForLaunch(ctx context.Context, generationID string) (GenerationPlanRecord, error) {
	generationID = strings.TrimSpace(generationID)
	if generationID == "" {
		return GenerationPlanRecord{}, fmt.Errorf("generation id is required")
	}
	record, err := s.GetGenerationPlan(ctx, generationID)
	if errors.Is(err, sql.ErrNoRows) {
		var status string
		statusErr := s.db.QueryRowContext(ctx, `
SELECT status FROM runtime_generations WHERE generation_id = ?`, generationID).Scan(&status)
		if errors.Is(statusErr, sql.ErrNoRows) {
			return GenerationPlanRecord{}, err
		}
		if statusErr != nil {
			return GenerationPlanRecord{}, statusErr
		}
		if runtimeGenerationStatusTerminal(status) {
			return GenerationPlanRecord{}, err
		}
		return GenerationPlanRecord{}, fmt.Errorf("generation plan is required for non-terminal generation %s", generationID)
	}
	return record, err
}

func (s *Store) StoreGenerationPlanProjection(ctx context.Context, p StoreGenerationPlanProjectionParams) (GenerationPlanProjectionRecord, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	p.GenerationID = strings.TrimSpace(p.GenerationID)
	p.PlanDigest = strings.TrimSpace(p.PlanDigest)
	p.ProjectionKind = strings.TrimSpace(p.ProjectionKind)
	p.PayloadDigest = strings.TrimSpace(p.PayloadDigest)
	p.MaterializedPath = strings.TrimSpace(p.MaterializedPath)
	if p.GenerationID == "" {
		return GenerationPlanProjectionRecord{}, fmt.Errorf("generation id is required")
	}
	if p.PlanDigest == "" || !strings.HasPrefix(p.PlanDigest, "sha256:") {
		return GenerationPlanProjectionRecord{}, fmt.Errorf("generation plan projection plan digest is required")
	}
	if p.ProjectionKind == "" {
		return GenerationPlanProjectionRecord{}, fmt.Errorf("generation plan projection kind is required")
	}
	projectionVersion, ok := GenerationPlanProjectionVersionFor(p.ProjectionKind)
	if !ok {
		return GenerationPlanProjectionRecord{}, fmt.Errorf("unsupported generation plan projection kind %q", p.ProjectionKind)
	}
	if p.ProjectionVersion <= 0 {
		return GenerationPlanProjectionRecord{}, fmt.Errorf("generation plan projection version is required")
	}
	if p.ProjectionVersion != projectionVersion {
		return GenerationPlanProjectionRecord{}, fmt.Errorf("generation plan projection %s version = %d, want %d", p.ProjectionKind, p.ProjectionVersion, projectionVersion)
	}
	if p.PayloadDigest == "" || !strings.HasPrefix(p.PayloadDigest, "sha256:") {
		return GenerationPlanProjectionRecord{}, fmt.Errorf("generation plan projection payload digest is required")
	}
	if p.MaterializedPath != "" && !filepath.IsAbs(p.MaterializedPath) {
		return GenerationPlanProjectionRecord{}, fmt.Errorf("generation plan projection materialized path must be absolute")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GenerationPlanProjectionRecord{}, err
	}
	defer func() { _ = tx.Rollback() }()
	plan, err := getGenerationPlanTx(ctx, tx, p.GenerationID)
	if err != nil {
		return GenerationPlanProjectionRecord{}, err
	}
	if plan.PlanDigest != p.PlanDigest {
		return GenerationPlanProjectionRecord{}, fmt.Errorf("generation plan projection plan digest mismatch: got %s want %s", p.PlanDigest, plan.PlanDigest)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO generation_plan_projections (
  generation_id, plan_digest, projection_kind, projection_version,
  payload_digest, materialized_path, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(generation_id, projection_kind) DO NOTHING`,
		p.GenerationID, p.PlanDigest, p.ProjectionKind, p.ProjectionVersion,
		p.PayloadDigest, nullableString(p.MaterializedPath), formatTime(p.Now)); err != nil {
		return GenerationPlanProjectionRecord{}, err
	}
	record, err := getGenerationPlanProjectionTx(ctx, tx, p.GenerationID, p.ProjectionKind)
	if err != nil {
		return GenerationPlanProjectionRecord{}, err
	}
	if record.PlanDigest != p.PlanDigest ||
		record.ProjectionVersion != p.ProjectionVersion ||
		record.PayloadDigest != p.PayloadDigest ||
		record.MaterializedPath != p.MaterializedPath {
		return GenerationPlanProjectionRecord{}, fmt.Errorf("generation plan projection already exists with different immutable payload")
	}
	if err := tx.Commit(); err != nil {
		return GenerationPlanProjectionRecord{}, err
	}
	return record, nil
}

func (s *Store) ListGenerationPlanProjections(ctx context.Context, generationID string) ([]GenerationPlanProjectionRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT generation_id, plan_digest, projection_kind, projection_version, payload_digest, materialized_path, created_at
FROM generation_plan_projections
WHERE generation_id = ?
ORDER BY projection_kind`, strings.TrimSpace(generationID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := []GenerationPlanProjectionRecord{}
	for rows.Next() {
		record, err := scanGenerationPlanProjection(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (s *Store) VerifyGenerationPlanProjections(ctx context.Context, p VerifyGenerationPlanProjectionsParams) (bool, error) {
	generationID := strings.TrimSpace(p.GenerationID)
	if generationID == "" {
		return false, fmt.Errorf("generation id is required")
	}
	plan, err := s.GetGenerationPlan(ctx, generationID)
	if errors.Is(err, sql.ErrNoRows) {
		if p.RequirePlan {
			return false, fmt.Errorf("generation plan is required for generation %s", generationID)
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}
	expectedPlanDigest := strings.TrimSpace(p.PlanDigest)
	if expectedPlanDigest != "" && expectedPlanDigest != plan.PlanDigest {
		return true, fmt.Errorf("generation plan digest mismatch: got %s want %s", expectedPlanDigest, plan.PlanDigest)
	}
	records, err := s.ListGenerationPlanProjections(ctx, generationID)
	if err != nil {
		return true, err
	}
	byKind := map[string]GenerationPlanProjectionRecord{}
	for _, record := range records {
		if record.PlanDigest != plan.PlanDigest {
			return true, fmt.Errorf("generation plan projection %s plan digest mismatch: got %s want %s", record.ProjectionKind, record.PlanDigest, plan.PlanDigest)
		}
		byKind[record.ProjectionKind] = record
	}
	for _, expectation := range p.Expected {
		kind := strings.TrimSpace(expectation.ProjectionKind)
		digest := strings.TrimSpace(expectation.PayloadDigest)
		if kind == "" {
			return true, fmt.Errorf("generation plan projection kind is required")
		}
		if digest == "" || !strings.HasPrefix(digest, "sha256:") {
			return true, fmt.Errorf("generation plan projection %s payload digest is required", kind)
		}
		record, ok := byKind[kind]
		if !ok {
			return true, fmt.Errorf("generation plan projection %s is required", kind)
		}
		version := expectation.ProjectionVersion
		if version == 0 {
			defaultVersion, ok := GenerationPlanProjectionVersionFor(kind)
			if ok {
				version = defaultVersion
			}
		}
		if version <= 0 {
			return true, fmt.Errorf("generation plan projection %s version is required", kind)
		}
		if record.ProjectionVersion != version {
			return true, fmt.Errorf("generation plan projection %s version mismatch: got %d want %d", kind, version, record.ProjectionVersion)
		}
		if record.PayloadDigest != digest {
			return true, fmt.Errorf("generation plan projection %s digest mismatch: got %s want %s", kind, digest, record.PayloadDigest)
		}
		materializedPath := strings.TrimSpace(expectation.MaterializedPath)
		if materializedPath != "" && record.MaterializedPath != materializedPath {
			return true, fmt.Errorf("generation plan projection %s materialized path mismatch: got %s want %s", kind, record.MaterializedPath, materializedPath)
		}
	}
	return true, nil
}

func getGenerationPlanTx(ctx context.Context, db dbRunner, generationID string) (GenerationPlanRecord, error) {
	row := db.QueryRowContext(ctx, `
SELECT generation_id, plan_version, canonical_payload, plan_digest, created_at
FROM generation_plans
WHERE generation_id = ?`, strings.TrimSpace(generationID))
	return scanGenerationPlan(row)
}

func getGenerationPlanProjectionTx(ctx context.Context, db dbRunner, generationID, projectionKind string) (GenerationPlanProjectionRecord, error) {
	row := db.QueryRowContext(ctx, `
SELECT generation_id, plan_digest, projection_kind, projection_version, payload_digest, materialized_path, created_at
FROM generation_plan_projections
WHERE generation_id = ?
  AND projection_kind = ?`, strings.TrimSpace(generationID), strings.TrimSpace(projectionKind))
	return scanGenerationPlanProjection(row)
}

func scanGenerationPlan(row scanner) (GenerationPlanRecord, error) {
	var record GenerationPlanRecord
	var canonicalPayload, createdAt string
	err := row.Scan(&record.GenerationID, &record.PlanVersion, &canonicalPayload, &record.PlanDigest, &createdAt)
	if err != nil {
		return GenerationPlanRecord{}, err
	}
	record.CanonicalPayload = []byte(canonicalPayload)
	record.CreatedAt = parseTime(createdAt)
	if err := VerifyGenerationPlanDigest(record.CanonicalPayload, record.PlanDigest); err != nil {
		return GenerationPlanRecord{}, err
	}
	return record, nil
}

func scanGenerationPlanProjection(row scanner) (GenerationPlanProjectionRecord, error) {
	var record GenerationPlanProjectionRecord
	var materializedPath sql.NullString
	var createdAt string
	err := row.Scan(
		&record.GenerationID, &record.PlanDigest, &record.ProjectionKind,
		&record.ProjectionVersion, &record.PayloadDigest, &materializedPath, &createdAt,
	)
	if err != nil {
		return GenerationPlanProjectionRecord{}, err
	}
	if materializedPath.Valid {
		record.MaterializedPath = materializedPath.String
	}
	record.CreatedAt = parseTime(createdAt)
	if record.PlanDigest == "" || !strings.HasPrefix(record.PlanDigest, "sha256:") {
		return GenerationPlanProjectionRecord{}, fmt.Errorf("generation plan projection plan digest is invalid")
	}
	if record.PayloadDigest == "" || !strings.HasPrefix(record.PayloadDigest, "sha256:") {
		return GenerationPlanProjectionRecord{}, fmt.Errorf("generation plan projection payload digest is invalid")
	}
	projectionVersion, ok := GenerationPlanProjectionVersionFor(record.ProjectionKind)
	if !ok {
		return GenerationPlanProjectionRecord{}, fmt.Errorf("unsupported generation plan projection kind %q", record.ProjectionKind)
	}
	if record.ProjectionVersion != projectionVersion {
		return GenerationPlanProjectionRecord{}, fmt.Errorf("generation plan projection %s version = %d, want %d", record.ProjectionKind, record.ProjectionVersion, projectionVersion)
	}
	return record, nil
}

func runtimeGenerationStatusTerminal(status string) bool {
	switch strings.TrimSpace(status) {
	case "failed", "destroyed":
		return true
	default:
		return false
	}
}
