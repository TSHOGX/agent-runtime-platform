package store

import (
	"context"
	"fmt"
	"time"
)

type RenewHeartbeatParams struct {
	SessionID    string
	GenerationID string
	Owner        string
	LeaseTTL     time.Duration
	Now          time.Time
}

type RenewGenerationStartLeaseParams struct {
	SessionID    string
	GenerationID string
	Owner        string
	LeaseTTL     time.Duration
	Now          time.Time
}

type FailGenerationParams struct {
	SessionID    string
	GenerationID string
	TurnID       int64
	Owner        string
	ErrorClass   string
	Reason       string
	Now          time.Time
}

type FailGenerationStartParams struct {
	SessionID      string
	GenerationID   string
	Owner          string
	SessionStatus  string
	ErrorClass     string
	Reason         string
	EventType      string
	EventDedupeKey string
	Now            time.Time
}

func (s *Store) RenewGenerationHeartbeat(ctx context.Context, p RenewHeartbeatParams) error {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	expiresAt := p.Now.Add(p.LeaseTTL)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET lease_expires_at = ?,
    last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?
  AND status IN ('starting','probing','active','idle','checkpointing','restoring')
  AND lease_owner = ?
  AND lease_expires_at > ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = ?
      AND active_generation_id = ?
  )`, formatTime(expiresAt), formatTime(p.Now), p.GenerationID, p.SessionID, p.Owner, formatTime(p.Now), p.SessionID, p.GenerationID)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("generation heartbeat CAS failed")
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE turns
SET lease_expires_at = ?
WHERE session_id = ?
  AND generation_id = ?
  AND status IN ('leased', 'running')
  AND lease_owner = ?
  AND lease_expires_at > ?`, formatTime(expiresAt), p.SessionID, p.GenerationID, p.Owner, formatTime(p.Now)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE active_model_request_contexts
SET expires_at = ?,
    lease_owner = ?,
    updated_at = ?
WHERE session_id = ?
  AND generation_id = ?
  AND turn_id IN (
    SELECT id FROM turns
    WHERE session_id = ?
      AND generation_id = ?
      AND status = 'running'
  )`, formatTime(expiresAt), p.Owner, formatTime(p.Now), p.SessionID, p.GenerationID, p.SessionID, p.GenerationID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) RenewGenerationStartLease(ctx context.Context, p RenewGenerationStartLeaseParams) error {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if p.LeaseTTL <= 0 {
		return fmt.Errorf("lease ttl must be > 0")
	}
	expiresAt := p.Now.Add(p.LeaseTTL)
	res, err := s.db.ExecContext(ctx, `
UPDATE runtime_generations
SET lease_expires_at = ?,
    last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?
  AND status IN ('allocating','starting','probing','idle','active','restoring')
  AND lease_owner = ?
  AND lease_expires_at > ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = ?
      AND active_generation_id = ?
      AND status NOT IN ('failed', 'destroyed')
  )`, formatTime(expiresAt), formatTime(p.Now), p.GenerationID, p.SessionID, p.Owner, formatTime(p.Now), p.SessionID, p.GenerationID)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("generation start lease renewal CAS failed")
	}
	return nil
}

func (s *Store) FailGeneration(ctx context.Context, p FailGenerationParams) error {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if p.TurnID != 0 {
		res, err := tx.ExecContext(ctx, `
UPDATE turns
SET status = 'failed',
    completed_at = ?,
    error_class = ?,
    error = ?
WHERE id = ?
  AND status IN ('leased', 'running')
  AND session_id = ?
  AND generation_id = ?
  AND lease_owner = ?
  AND lease_expires_at > ?`,
			formatTime(p.Now), nullableString(p.ErrorClass), nullableString(p.Reason), p.TurnID,
			p.SessionID, p.GenerationID, p.Owner, formatTime(p.Now))
		if err != nil {
			return err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return err
		} else if affected != 1 {
			return fmt.Errorf("turn failure CAS failed")
		}
	}
	res, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'failed',
    error_class = ?,
    failure_reason = ?,
    ended_at = ?,
    lease_owner = NULL
WHERE generation_id = ?
  AND session_id = ?
  AND status IN ('allocating','starting','probing','active','idle','checkpointing','restoring')
  AND lease_owner = ?
  AND lease_expires_at > ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = ?
      AND active_generation_id = ?
  )`,
		nullableString(p.ErrorClass), nullableString(p.Reason), formatTime(p.Now), p.GenerationID, p.SessionID,
		p.Owner, formatTime(p.Now), p.SessionID, p.GenerationID)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("generation failure CAS failed")
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reclaimable'
WHERE generation_id = ?
  AND session_id = ?
  AND allocation_state IN ('allocating','ready','live','reserved_checkpointed','recreating')`, p.GenerationID, p.SessionID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'reclaimable'
WHERE generation_id = ?
  AND resource_state IN ('allocating','ready','live','reserved_checkpointed','recreating')`, p.GenerationID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM active_model_request_contexts
WHERE session_id = ?
  AND generation_id = ?`, p.SessionID, p.GenerationID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) FailGenerationStart(ctx context.Context, p FailGenerationStartParams) (int64, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if p.SessionStatus == "" {
		p.SessionStatus = "running_idle"
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'failed',
    error_class = ?,
    failure_reason = ?,
    ended_at = ?,
    lease_owner = NULL
WHERE generation_id = ?
  AND session_id = ?
  AND status IN ('allocating','starting','probing','idle','restoring')
  AND lease_owner = ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = ?
      AND active_generation_id = ?
      AND status NOT IN ('failed', 'destroyed')
  )`,
		nullableString(p.ErrorClass), nullableString(p.Reason), formatTime(p.Now), p.GenerationID, p.SessionID,
		p.Owner, p.SessionID, p.GenerationID)
	if err != nil {
		return 0, err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return 0, err
	} else if affected != 1 {
		return 0, fmt.Errorf("generation start failure CAS failed")
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reclaimable'
WHERE generation_id = ?
  AND session_id = ?
  AND allocation_state IN ('allocating','ready','live','reserved_checkpointed','recreating')`, p.GenerationID, p.SessionID); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'reclaimable'
WHERE generation_id = ?
  AND resource_state IN ('allocating','ready','live','reserved_checkpointed','recreating')`, p.GenerationID); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM active_model_request_contexts
WHERE session_id = ?
  AND generation_id = ?`, p.SessionID, p.GenerationID); err != nil {
		return 0, err
	}
	res, err = tx.ExecContext(ctx, `
	UPDATE sessions
	SET status = ?,
	    updated_at = ?,
	    checkpoint_path = NULL,
	    restore_ms = NULL,
	    error_class = NULL,
	    failure_reason = NULL
WHERE id = ?
  AND active_generation_id = ?
  AND status NOT IN ('failed', 'destroyed')`,
		p.SessionStatus, formatTime(p.Now), p.SessionID, p.GenerationID)
	if err != nil {
		return 0, err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return 0, err
	} else if affected != 1 {
		return 0, fmt.Errorf("session start failure CAS failed")
	}
	var eventID int64
	if p.EventType != "" {
		eventID, err = appendEventTx(ctx, tx, AppendEventParams{
			SessionID:    p.SessionID,
			GenerationID: p.GenerationID,
			DedupeKey:    p.EventDedupeKey,
			Type:         p.EventType,
			Payload: map[string]any{
				"terminal":             false,
				"error_class":          p.ErrorClass,
				"error":                p.Reason,
				"generation_id":        p.GenerationID,
				"session_status":       p.SessionStatus,
				"session_updated_at":   formatTime(p.Now),
				"active_generation_id": p.GenerationID,
			},
			Now: p.Now,
		})
		if err != nil {
			return 0, err
		}
	}
	return eventID, tx.Commit()
}
