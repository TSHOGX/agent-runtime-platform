package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

const (
	ContentSnapshotKindSkills          = "skills"
	ContentSnapshotKindManagedSettings = "managed_settings"
)

type ContentSnapshotRecord struct {
	Kind                 string
	Digest               string
	ImmutableHostPath    string
	MountDestination     string
	SourceEvidenceDigest string
	RetentionClass       string
	CreatedAt            time.Time
}

type StoreContentSnapshotParams struct {
	Kind                 string
	Digest               string
	ImmutableHostPath    string
	MountDestination     string
	SourceEvidenceDigest string
	RetentionClass       string
	Now                  time.Time
}

func (s *Store) StoreContentSnapshot(ctx context.Context, p StoreContentSnapshotParams) (ContentSnapshotRecord, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	p.Kind = strings.TrimSpace(p.Kind)
	p.Digest = strings.TrimSpace(p.Digest)
	p.ImmutableHostPath = strings.TrimSpace(p.ImmutableHostPath)
	p.MountDestination = strings.TrimSpace(p.MountDestination)
	p.SourceEvidenceDigest = strings.TrimSpace(p.SourceEvidenceDigest)
	p.RetentionClass = strings.TrimSpace(p.RetentionClass)
	if err := validateContentSnapshotParams(p); err != nil {
		return ContentSnapshotRecord{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ContentSnapshotRecord{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO content_snapshots (
  snapshot_kind, snapshot_digest, immutable_host_path, mount_destination,
  source_evidence_digest, retention_class, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(snapshot_kind, snapshot_digest) DO NOTHING`,
		p.Kind, p.Digest, p.ImmutableHostPath, p.MountDestination,
		p.SourceEvidenceDigest, p.RetentionClass, formatTime(p.Now)); err != nil {
		return ContentSnapshotRecord{}, err
	}
	record, err := getContentSnapshotTx(ctx, tx, p.Kind, p.Digest)
	if err != nil {
		return ContentSnapshotRecord{}, err
	}
	if record.ImmutableHostPath != p.ImmutableHostPath ||
		record.MountDestination != p.MountDestination ||
		record.SourceEvidenceDigest != p.SourceEvidenceDigest ||
		record.RetentionClass != p.RetentionClass {
		return ContentSnapshotRecord{}, fmt.Errorf("content snapshot already exists with different immutable payload")
	}
	if err := tx.Commit(); err != nil {
		return ContentSnapshotRecord{}, err
	}
	return record, nil
}

func (s *Store) GetContentSnapshot(ctx context.Context, kind, digest string) (ContentSnapshotRecord, error) {
	return getContentSnapshotTx(ctx, s.db, strings.TrimSpace(kind), strings.TrimSpace(digest))
}

func (s *Store) ListContentSnapshots(ctx context.Context, kind string) ([]ContentSnapshotRecord, error) {
	kind = strings.TrimSpace(kind)
	if kind != "" && !knownContentSnapshotKind(kind) {
		return nil, fmt.Errorf("unsupported content snapshot kind %q", kind)
	}
	query := `
SELECT snapshot_kind, snapshot_digest, immutable_host_path, mount_destination,
       source_evidence_digest, retention_class, created_at
FROM content_snapshots`
	args := []any{}
	if kind != "" {
		query += `
WHERE snapshot_kind = ?`
		args = append(args, kind)
	}
	query += `
ORDER BY snapshot_kind, snapshot_digest`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := []ContentSnapshotRecord{}
	for rows.Next() {
		record, err := scanContentSnapshot(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func validateContentSnapshotParams(p StoreContentSnapshotParams) error {
	if !knownContentSnapshotKind(p.Kind) {
		return fmt.Errorf("unsupported content snapshot kind %q", p.Kind)
	}
	if p.Digest == "" || !strings.HasPrefix(p.Digest, "sha256:") {
		return fmt.Errorf("content snapshot digest is required")
	}
	if p.ImmutableHostPath == "" {
		return fmt.Errorf("content snapshot immutable host path is required")
	}
	if !filepath.IsAbs(p.ImmutableHostPath) {
		return fmt.Errorf("content snapshot immutable host path must be absolute")
	}
	if p.MountDestination == "" {
		return fmt.Errorf("content snapshot mount destination is required")
	}
	if !filepath.IsAbs(p.MountDestination) {
		return fmt.Errorf("content snapshot mount destination must be absolute")
	}
	if p.SourceEvidenceDigest == "" || !strings.HasPrefix(p.SourceEvidenceDigest, "sha256:") {
		return fmt.Errorf("content snapshot source evidence digest is required")
	}
	if p.RetentionClass == "" {
		return fmt.Errorf("content snapshot retention class is required")
	}
	return nil
}

func knownContentSnapshotKind(kind string) bool {
	switch strings.TrimSpace(kind) {
	case ContentSnapshotKindSkills, ContentSnapshotKindManagedSettings:
		return true
	default:
		return false
	}
}

func getContentSnapshotTx(ctx context.Context, db dbRunner, kind, digest string) (ContentSnapshotRecord, error) {
	row := db.QueryRowContext(ctx, `
SELECT snapshot_kind, snapshot_digest, immutable_host_path, mount_destination,
       source_evidence_digest, retention_class, created_at
FROM content_snapshots
WHERE snapshot_kind = ?
  AND snapshot_digest = ?`, strings.TrimSpace(kind), strings.TrimSpace(digest))
	return scanContentSnapshot(row)
}

func scanContentSnapshot(row scanner) (ContentSnapshotRecord, error) {
	var record ContentSnapshotRecord
	var createdAt string
	err := row.Scan(
		&record.Kind, &record.Digest, &record.ImmutableHostPath,
		&record.MountDestination, &record.SourceEvidenceDigest,
		&record.RetentionClass, &createdAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return ContentSnapshotRecord{}, err
		}
		return ContentSnapshotRecord{}, err
	}
	record.CreatedAt = parseTime(createdAt)
	if !knownContentSnapshotKind(record.Kind) {
		return ContentSnapshotRecord{}, fmt.Errorf("unsupported content snapshot kind %q", record.Kind)
	}
	if record.Digest == "" || !strings.HasPrefix(record.Digest, "sha256:") {
		return ContentSnapshotRecord{}, fmt.Errorf("content snapshot digest is invalid")
	}
	if record.SourceEvidenceDigest == "" || !strings.HasPrefix(record.SourceEvidenceDigest, "sha256:") {
		return ContentSnapshotRecord{}, fmt.Errorf("content snapshot source evidence digest is invalid")
	}
	if record.ImmutableHostPath == "" || !filepath.IsAbs(record.ImmutableHostPath) {
		return ContentSnapshotRecord{}, fmt.Errorf("content snapshot immutable host path is invalid")
	}
	if record.MountDestination == "" || !filepath.IsAbs(record.MountDestination) {
		return ContentSnapshotRecord{}, fmt.Errorf("content snapshot mount destination is invalid")
	}
	if record.RetentionClass == "" {
		return ContentSnapshotRecord{}, fmt.Errorf("content snapshot retention class is invalid")
	}
	return record, nil
}
