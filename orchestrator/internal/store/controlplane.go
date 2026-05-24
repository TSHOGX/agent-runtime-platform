package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type ClaimNextTurnParams struct {
	SessionID    string
	GenerationID string
	Owner        string
	RequestID    string
	LeaseTTL     time.Duration
	Now          time.Time
}

type TurnGrant struct {
	TurnID    int64
	Sequence  int64
	Content   string
	Attempt   int
	Replayed  bool
	ExpiresAt time.Time
}

type AckStartedParams struct {
	SessionID       string
	GenerationID    string
	TurnID          int64
	Owner           string
	SandboxSourceIP string
	LeaseTTL        time.Duration
	Now             time.Time
}

type CompleteTurnParams struct {
	SessionID      string
	GenerationID   string
	TurnID         int64
	Owner          string
	TerminalStatus string
	ErrorClass     string
	Error          string
	Now            time.Time
}

type RenewHeartbeatParams struct {
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

func (s *Store) EnqueueTurn(ctx context.Context, sessionID, content string, now time.Time) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	next, err := nextTurnSequence(ctx, tx, sessionID)
	if err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx, `
INSERT INTO turns (session_id, sequence, role, content, status, attempt, created_at)
VALUES (?, ?, 'user', ?, 'queued', 0, ?)`, sessionID, next, content, formatTime(now))
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, tx.Commit()
}

func (s *Store) ClaimNextTurn(ctx context.Context, p ClaimNextTurnParams) (TurnGrant, bool, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return TurnGrant{}, false, err
	}
	defer func() { _ = tx.Rollback() }()

	replay, ok, err := claimReplay(ctx, tx, p)
	if err != nil {
		return TurnGrant{}, false, err
	}
	if ok {
		replay.Replayed = true
		return replay, true, tx.Commit()
	}

	var grant TurnGrant
	err = tx.QueryRowContext(ctx, `
SELECT id, sequence, content, attempt
FROM turns
WHERE session_id = ?
  AND status = 'queued'
  AND lease_owner IS NULL
ORDER BY sequence ASC
LIMIT 1`, p.SessionID).Scan(&grant.TurnID, &grant.Sequence, &grant.Content, &grant.Attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return TurnGrant{}, false, tx.Commit()
	}
	if err != nil {
		return TurnGrant{}, false, err
	}
	expiresAt := p.Now.Add(p.LeaseTTL)

	res, err := tx.ExecContext(ctx, `
UPDATE turns
SET status = 'leased',
    generation_id = ?,
    lease_owner = ?,
    lease_expires_at = ?,
    claim_request_id = ?,
    claim_granted_at = ?
WHERE id = ?
  AND session_id = ?
  AND status = 'queued'
  AND lease_owner IS NULL
  AND NOT EXISTS (
    SELECT 1 FROM turns
    WHERE generation_id = ?
      AND status IN ('leased', 'running')
  )
  AND EXISTS (
    SELECT 1 FROM runtime_generations g
    JOIN sessions s ON s.id = g.session_id
    WHERE g.generation_id = ?
      AND g.session_id = ?
      AND g.status IN ('idle', 'active')
      AND g.lease_owner = ?
      AND g.lease_expires_at > ?
      AND s.active_generation_id = ?
	)`,
		p.GenerationID, p.Owner, formatTime(expiresAt), p.RequestID, formatTime(p.Now), grant.TurnID,
		p.SessionID, p.GenerationID, p.GenerationID, p.SessionID, p.Owner, formatTime(p.Now), p.GenerationID)
	if err != nil {
		return TurnGrant{}, false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return TurnGrant{}, false, err
	}
	if affected != 1 {
		return TurnGrant{}, false, tx.Commit()
	}

	res, err = tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'active'
WHERE generation_id = ?
  AND session_id = ?
  AND status IN ('idle', 'active')
  AND lease_owner = ?
  AND lease_expires_at > ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = ?
      AND active_generation_id = ?
  )`, p.GenerationID, p.SessionID, p.Owner, formatTime(p.Now), p.SessionID, p.GenerationID)
	if err != nil {
		return TurnGrant{}, false, err
	}
	affected, err = res.RowsAffected()
	if err != nil {
		return TurnGrant{}, false, err
	}
	if affected != 1 {
		return TurnGrant{}, false, fmt.Errorf("generation CAS failed after turn claim")
	}

	grant.ExpiresAt = expiresAt
	return grant, true, tx.Commit()
}

func (s *Store) AckTurnStarted(ctx context.Context, p AckStartedParams) error {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	expiresAt := p.Now.Add(p.LeaseTTL)
	res, err := tx.ExecContext(ctx, `
UPDATE turns
SET status = 'running',
    started_at = ?,
    ack_started_at = ?
WHERE id = ?
  AND status = 'leased'
  AND session_id = ?
  AND generation_id = ?
  AND lease_owner = ?
  AND lease_expires_at > ?`,
		formatTime(p.Now), formatTime(p.Now), p.TurnID, p.SessionID, p.GenerationID, p.Owner, formatTime(p.Now))
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("turn ack_started CAS failed")
	}
	res, err = tx.ExecContext(ctx, `
UPDATE runtime_generations
SET last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?
  AND status = 'active'
  AND lease_owner = ?
  AND lease_expires_at > ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = ?
      AND active_generation_id = ?
  )`, formatTime(p.Now), p.GenerationID, p.SessionID, p.Owner, formatTime(p.Now), p.SessionID, p.GenerationID)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("generation ack_started CAS failed")
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO active_model_request_contexts (
  sandbox_source_ip, session_id, generation_id, turn_id,
  lease_owner, expires_at, next_request_sequence, registered_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?) ON CONFLICT(sandbox_source_ip) DO UPDATE SET
  session_id = excluded.session_id,
  generation_id = excluded.generation_id,
  turn_id = excluded.turn_id,
  lease_owner = excluded.lease_owner,
  expires_at = excluded.expires_at,
  next_request_sequence = excluded.next_request_sequence,
  registered_at = excluded.registered_at,
  updated_at = excluded.updated_at`,
		p.SandboxSourceIP, p.SessionID, p.GenerationID, p.TurnID, p.Owner, formatTime(expiresAt), formatTime(p.Now), formatTime(p.Now))
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) CompleteTurn(ctx context.Context, p CompleteTurnParams) error {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	switch p.TerminalStatus {
	case "completed", "failed", "canceled":
	default:
		return fmt.Errorf("invalid terminal turn status %q", p.TerminalStatus)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx, `
UPDATE turns
SET status = ?,
    completed_at = ?,
    completed_by_generation = ?,
    error_class = ?,
    error = ?
WHERE id = ?
  AND status IN ('leased', 'running')
  AND session_id = ?
  AND generation_id = ?
  AND lease_owner = ?
  AND lease_expires_at > ?`,
		p.TerminalStatus, formatTime(p.Now), p.GenerationID, nullableString(p.ErrorClass), nullableString(p.Error),
		p.TurnID, p.SessionID, p.GenerationID, p.Owner, formatTime(p.Now))
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("turn completion CAS failed")
	}
	if err := markGenerationIdleIfNoInflight(ctx, tx, p.SessionID, p.GenerationID, p.Owner, p.Now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM active_model_request_contexts
WHERE session_id = ?
  AND generation_id = ?
  AND turn_id = ?`, p.SessionID, p.GenerationID, p.TurnID); err != nil {
		return err
	}
	return tx.Commit()
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

func claimReplay(ctx context.Context, tx *sql.Tx, p ClaimNextTurnParams) (TurnGrant, bool, error) {
	row := tx.QueryRowContext(ctx, `
SELECT id, sequence, content, attempt, lease_expires_at
FROM turns
WHERE session_id = ?
  AND generation_id = ?
  AND claim_request_id = ?
  AND status IN ('leased', 'running')
  AND lease_owner = ?
  AND lease_expires_at > ?`,
		p.SessionID, p.GenerationID, p.RequestID, p.Owner, formatTime(p.Now))
	var grant TurnGrant
	var leaseExpires string
	err := row.Scan(&grant.TurnID, &grant.Sequence, &grant.Content, &grant.Attempt, &leaseExpires)
	if errors.Is(err, sql.ErrNoRows) {
		return TurnGrant{}, false, nil
	}
	if err != nil {
		return TurnGrant{}, false, err
	}
	grant.ExpiresAt = parseTime(leaseExpires)
	return grant, true, nil
}

func nextTurnSequence(ctx context.Context, tx *sql.Tx, sessionID string) (int64, error) {
	var next sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
SELECT MAX(sequence) + 1 FROM turns WHERE session_id = ?`, sessionID).Scan(&next); err != nil {
		return 0, err
	}
	if !next.Valid {
		return 1, nil
	}
	return next.Int64, nil
}

func markGenerationIdleIfNoInflight(ctx context.Context, tx *sql.Tx, sessionID, generationID, owner string, now time.Time) error {
	res, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'idle'
WHERE generation_id = ?
  AND session_id = ?
  AND status = 'active'
  AND lease_owner = ?
  AND lease_expires_at > ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = ?
      AND active_generation_id = ?
  )
  AND NOT EXISTS (
    SELECT 1 FROM turns
    WHERE generation_id = ?
      AND status IN ('leased', 'running')
  )`, generationID, sessionID, owner, formatTime(now), sessionID, generationID, generationID)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("generation idle CAS failed")
	}
	return nil
}
