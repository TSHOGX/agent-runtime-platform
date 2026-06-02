package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

type ReaperParams struct {
	OwnerUUID       string
	FailedRetention time.Duration
	Now             time.Time
}

type ReaperResult struct {
	FailedMarkedReclaimable int64
	DestroyedAllocations    int64
}

type ReclaimableGeneration struct {
	SessionID    string
	GenerationID string
}

type DestroyGenerationResourcesParams struct {
	SessionID    string
	GenerationID string
	Now          time.Time
}

func (s *Store) ReapResources(ctx context.Context, p ReaperParams) (ReaperResult, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ReaperResult{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := assertOwnerTx(ctx, tx, p.OwnerUUID); err != nil {
		return ReaperResult{}, err
	}

	cutoff := p.Now.Add(-p.FailedRetention)
	res, err := tx.ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reclaimable'
WHERE allocation_state IN ('allocating','ready','live','reserved_checkpointed','recreating')
  AND generation_id IN (
    SELECT generation_id FROM runtime_generations
    WHERE status = 'failed'
      AND ended_at IS NOT NULL
      AND ended_at <= ?
  )`, formatTime(cutoff))
	if err != nil {
		return ReaperResult{}, err
	}
	failedMarked, err := res.RowsAffected()
	if err != nil {
		return ReaperResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'reclaimable'
WHERE resource_state IN ('allocating','ready','live','reserved_checkpointed','recreating')
  AND generation_id IN (
    SELECT generation_id FROM runtime_generations
    WHERE status = 'failed'
      AND ended_at IS NOT NULL
      AND ended_at <= ?
  )`, formatTime(cutoff)); err != nil {
		return ReaperResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return ReaperResult{}, err
	}
	return ReaperResult{FailedMarkedReclaimable: failedMarked}, nil
}

func (s *Store) ListDestroyableReclaimableGenerations(ctx context.Context, now time.Time, failedRetention time.Duration) ([]ReclaimableGeneration, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	cutoff := now.Add(-failedRetention)
	rows, err := s.db.QueryContext(ctx, `
SELECT n.session_id, n.generation_id
FROM network_profiles n
JOIN runtime_generation_resources r ON r.generation_id = n.generation_id
JOIN runtime_generations g ON g.generation_id = n.generation_id
WHERE n.allocation_state = 'reclaimable'
  AND r.resource_state = 'reclaimable'
  AND (
    g.status != 'failed'
    OR COALESCE(g.error_class, '') = 'checkpoint_retired'
    OR (g.ended_at IS NOT NULL AND g.ended_at <= ?)
  )
ORDER BY n.created_at, n.generation_id`, formatTime(cutoff))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var generations []ReclaimableGeneration
	for rows.Next() {
		var generation ReclaimableGeneration
		if err := rows.Scan(&generation.SessionID, &generation.GenerationID); err != nil {
			return nil, err
		}
		generations = append(generations, generation)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return generations, nil
}

func (s *Store) MarkGenerationResourcesDestroyed(ctx context.Context, p DestroyGenerationResourcesParams) error {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if strings.TrimSpace(p.SessionID) == "" {
		return fmt.Errorf("session id is required")
	}
	if strings.TrimSpace(p.GenerationID) == "" {
		return fmt.Errorf("generation id is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var networkState, resourceState string
	if err := tx.QueryRowContext(ctx, `
SELECT n.allocation_state, r.resource_state
FROM network_profiles n
JOIN runtime_generation_resources r ON r.generation_id = n.generation_id
WHERE n.session_id = ?
  AND n.generation_id = ?`, p.SessionID, p.GenerationID).Scan(&networkState, &resourceState); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("generation resources not found for session %q generation %q", p.SessionID, p.GenerationID)
		}
		return err
	}
	if networkState == "destroyed" && resourceState == "destroyed" {
		return tx.Commit()
	}
	if networkState != "reclaimable" {
		return fmt.Errorf("network allocation destroyed CAS failed: state=%q", networkState)
	}
	if resourceState != "reclaimable" {
		return fmt.Errorf("generation resource destroyed CAS failed: state=%q", resourceState)
	}
	res, err := tx.ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'destroyed',
    destroyed_at = COALESCE(destroyed_at, ?)
WHERE session_id = ?
  AND generation_id = ?
  AND allocation_state = 'reclaimable'`,
		formatTime(p.Now), p.SessionID, p.GenerationID)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("network allocation destroyed CAS failed")
	}
	res, err = tx.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'destroyed',
    destroyed_at = COALESCE(destroyed_at, ?)
WHERE generation_id = ?
  AND resource_state = 'reclaimable'`, formatTime(p.Now), p.GenerationID)
	if err != nil {
		return err
	}
	affected, err = res.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("generation resource destroyed CAS failed")
	}
	return tx.Commit()
}
