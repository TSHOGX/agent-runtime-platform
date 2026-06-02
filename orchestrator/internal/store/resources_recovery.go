package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type StartupRecoveryParams struct {
	OwnerUUID       string
	Now             time.Time
	LeaseTTL        time.Duration
	ReconnectGrace  time.Duration
	AckStartedGrace time.Duration
}

type StartupRecoveryResult struct {
	ExpiredLifecycleFailed int64
	ReconnectGraceFailed   int64
	ExpiredLeasedRequeued  int64
	UnknownAfterAckStarted int64
	RuntimeCleanupSkipped  int64
	EventIDs               []int64
}

type ExpiredRuntimeRecoveryCandidate struct {
	SessionID    string
	GenerationID string
	RuntimeID    string
	Status       string
	ErrorClass   string
}

func (s *Store) ListExpiredRuntimeRecoveryCandidates(ctx context.Context, p StartupRecoveryParams) ([]ExpiredRuntimeRecoveryCandidate, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if p.ReconnectGrace <= 0 {
		return nil, fmt.Errorf("reconnect grace must be > 0")
	}
	if p.AckStartedGrace <= 0 {
		return nil, fmt.Errorf("ack-started grace must be > 0")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := assertOwnerTx(ctx, tx, p.OwnerUUID); err != nil {
		return nil, err
	}

	var candidates []ExpiredRuntimeRecoveryCandidate
	rows, err := tx.QueryContext(ctx, `
SELECT s.id, g.generation_id, ri.runsc_container_id, g.status
FROM runtime_generations g
JOIN sessions s ON s.id = g.session_id
JOIN runtime_resource_instances ri ON ri.generation_id = g.generation_id
  AND ri.session_id = s.id
  AND ri.contract_id = g.sandbox_contract_id
  AND ri.sandbox_contract_version = g.sandbox_contract_version
  AND ri.sandbox_contract_version = 'sandbox-isolation-v1'
WHERE g.status IN ('allocating','starting','probing','restoring','checkpointing')
  AND g.lease_expires_at IS NOT NULL
  AND g.lease_expires_at <= ?
  AND s.active_generation_id = g.generation_id
  AND s.status NOT IN ('failed', 'destroyed')
ORDER BY s.id, g.generation_id`, formatTime(p.Now))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var c ExpiredRuntimeRecoveryCandidate
		if err := rows.Scan(&c.SessionID, &c.GenerationID, &c.RuntimeID, &c.Status); err != nil {
			_ = rows.Close()
			return nil, err
		}
		c.ErrorClass = "orchestrator_restart_during_" + c.Status
		candidates = append(candidates, c)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	ackStartedCutoff := p.Now.Add(-p.AckStartedGrace)
	rows, err = tx.QueryContext(ctx, `
SELECT s.id, g.generation_id, ri.runsc_container_id, g.status
FROM runtime_generations g
JOIN sessions s ON s.id = g.session_id
JOIN runtime_resource_instances ri ON ri.generation_id = g.generation_id
  AND ri.session_id = s.id
  AND ri.contract_id = g.sandbox_contract_id
  AND ri.sandbox_contract_version = g.sandbox_contract_version
  AND ri.sandbox_contract_version = 'sandbox-isolation-v1'
WHERE g.status IN ('active','idle')
  AND g.lease_expires_at IS NOT NULL
  AND g.lease_expires_at <= ?
  AND s.active_generation_id = g.generation_id
  AND s.status NOT IN ('failed', 'destroyed')
  AND EXISTS (
    SELECT 1 FROM turns
    WHERE turns.generation_id = g.generation_id
      AND turns.status = 'running'
      AND turns.ack_started_at IS NOT NULL
      AND turns.lease_expires_at IS NOT NULL
      AND turns.lease_expires_at <= ?
  )
ORDER BY s.id, g.generation_id`, formatTime(ackStartedCutoff), formatTime(ackStartedCutoff))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var c ExpiredRuntimeRecoveryCandidate
		if err := rows.Scan(&c.SessionID, &c.GenerationID, &c.RuntimeID, &c.Status); err != nil {
			_ = rows.Close()
			return nil, err
		}
		c.ErrorClass = "unknown_after_ack_started"
		candidates = append(candidates, c)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	cutoff := p.Now.Add(-p.ReconnectGrace)
	rows, err = tx.QueryContext(ctx, `
SELECT s.id, g.generation_id, ri.runsc_container_id, g.status
FROM runtime_generations g
JOIN sessions s ON s.id = g.session_id
JOIN runtime_resource_instances ri ON ri.generation_id = g.generation_id
  AND ri.session_id = s.id
  AND ri.contract_id = g.sandbox_contract_id
  AND ri.sandbox_contract_version = g.sandbox_contract_version
  AND ri.sandbox_contract_version = 'sandbox-isolation-v1'
WHERE g.status IN ('active','idle')
  AND g.lease_expires_at IS NOT NULL
  AND g.lease_expires_at <= ?
  AND s.active_generation_id = g.generation_id
  AND s.status NOT IN ('failed', 'destroyed')
  AND NOT EXISTS (
    SELECT 1 FROM turns
    WHERE turns.generation_id = g.generation_id
      AND turns.status = 'running'
      AND turns.ack_started_at IS NOT NULL
  )
ORDER BY s.id, g.generation_id`, formatTime(cutoff))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var c ExpiredRuntimeRecoveryCandidate
		if err := rows.Scan(&c.SessionID, &c.GenerationID, &c.RuntimeID, &c.Status); err != nil {
			_ = rows.Close()
			return nil, err
		}
		c.ErrorClass = "orchestrator_restart_reconnect_grace_expired"
		candidates = append(candidates, c)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return candidates, nil
}

func (s *Store) RepairExpiredRuntimeRecovery(ctx context.Context, p StartupRecoveryParams, candidates []ExpiredRuntimeRecoveryCandidate) (StartupRecoveryResult, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if p.ReconnectGrace <= 0 {
		return StartupRecoveryResult{}, fmt.Errorf("reconnect grace must be > 0")
	}
	if p.AckStartedGrace <= 0 {
		return StartupRecoveryResult{}, fmt.Errorf("ack-started grace must be > 0")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return StartupRecoveryResult{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := assertOwnerTx(ctx, tx, p.OwnerUUID); err != nil {
		return StartupRecoveryResult{}, err
	}
	result := StartupRecoveryResult{}
	now := formatTime(p.Now)
	lifecycleIDs, unknownIDs, reconnectIDs := recoveryCandidateIDs(candidates)

	lifecycleIDs, err = filterLifecycleRecoveryIDsTx(ctx, tx, lifecycleIDs, p.Now)
	if err != nil {
		return StartupRecoveryResult{}, err
	}
	if len(lifecycleIDs) > 0 {
		requeued, err := requeueExpiredLeasedTurnsTx(ctx, tx, lifecycleIDs, p.Now)
		if err != nil {
			return StartupRecoveryResult{}, err
		}
		result.ExpiredLeasedRequeued += requeued
		args := []any{now}
		args = appendStringIDs(args, lifecycleIDs)
		if _, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'failed',
    error_class = 'orchestrator_restart_during_' || status,
    failure_reason = 'orchestrator_restart_during_' || status,
    ended_at = ?,
    lease_owner = NULL
WHERE generation_id IN (`+sqlPlaceholders(len(lifecycleIDs))+`)`, args...); err != nil {
			return StartupRecoveryResult{}, err
		}
		if err := markAllocationsReclaimableTx(ctx, tx, lifecycleIDs); err != nil {
			return StartupRecoveryResult{}, err
		}
		if err := deleteActiveContextsForGenerationsTx(ctx, tx, lifecycleIDs); err != nil {
			return StartupRecoveryResult{}, err
		}
		result.ExpiredLifecycleFailed = int64(len(lifecycleIDs))
	}

	unknownIDs, err = filterUnknownRecoveryIDsTx(ctx, tx, unknownIDs, p.Now.Add(-p.AckStartedGrace))
	if err != nil {
		return StartupRecoveryResult{}, err
	}
	if len(unknownIDs) > 0 {
		args := []any{now}
		args = appendStringIDs(args, unknownIDs)
		res, err := tx.ExecContext(ctx, `
UPDATE turns
SET status = 'failed',
    completed_at = ?,
    completed_by_generation = generation_id,
    error_class = 'unknown_after_ack_started',
    error = 'unknown_after_ack_started',
    lease_owner = NULL,
    lease_expires_at = NULL
WHERE generation_id IN (`+sqlPlaceholders(len(unknownIDs))+`)
  AND status = 'running'
  AND ack_started_at IS NOT NULL`, args...)
		if err != nil {
			return StartupRecoveryResult{}, err
		}
		unknownTurns, err := res.RowsAffected()
		if err != nil {
			return StartupRecoveryResult{}, err
		}
		args = []any{now}
		args = appendStringIDs(args, unknownIDs)
		if _, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'failed',
    error_class = 'unknown_after_ack_started',
    failure_reason = 'unknown_after_ack_started',
    ended_at = ?,
    lease_owner = NULL
WHERE generation_id IN (`+sqlPlaceholders(len(unknownIDs))+`)`, args...); err != nil {
			return StartupRecoveryResult{}, err
		}
		if err := markAllocationsReclaimableTx(ctx, tx, unknownIDs); err != nil {
			return StartupRecoveryResult{}, err
		}
		if err := deleteActiveContextsForGenerationsTx(ctx, tx, unknownIDs); err != nil {
			return StartupRecoveryResult{}, err
		}
		result.UnknownAfterAckStarted += unknownTurns
	}

	reconnectIDs, err = filterReconnectRecoveryIDsTx(ctx, tx, reconnectIDs, p.Now.Add(-p.ReconnectGrace))
	if err != nil {
		return StartupRecoveryResult{}, err
	}
	if len(reconnectIDs) > 0 {
		requeued, err := requeueExpiredLeasedTurnsTx(ctx, tx, reconnectIDs, p.Now)
		if err != nil {
			return StartupRecoveryResult{}, err
		}
		result.ExpiredLeasedRequeued += requeued
		args := []any{now}
		args = appendStringIDs(args, reconnectIDs)
		if _, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'failed',
    error_class = 'orchestrator_restart_reconnect_grace_expired',
    failure_reason = 'orchestrator_restart_reconnect_grace_expired',
    ended_at = ?,
    lease_owner = NULL
WHERE generation_id IN (`+sqlPlaceholders(len(reconnectIDs))+`)`, args...); err != nil {
			return StartupRecoveryResult{}, err
		}
		if err := markAllocationsReclaimableTx(ctx, tx, reconnectIDs); err != nil {
			return StartupRecoveryResult{}, err
		}
		if err := deleteActiveContextsForGenerationsTx(ctx, tx, reconnectIDs); err != nil {
			return StartupRecoveryResult{}, err
		}
		result.ReconnectGraceFailed = int64(len(reconnectIDs))
	}
	repairedIDs := append(append([]string{}, lifecycleIDs...), unknownIDs...)
	repairedIDs = append(repairedIDs, reconnectIDs...)
	for _, generationID := range repairedIDs {
		eventID, err := repairRecoveredSessionTx(ctx, tx, generationID, p.Now)
		if err != nil {
			return StartupRecoveryResult{}, err
		}
		if eventID != 0 {
			result.EventIDs = append(result.EventIDs, eventID)
		}
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM active_model_request_contexts
WHERE lease_owner NOT LIKE ?`, p.OwnerUUID+":%"); err != nil {
		return StartupRecoveryResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return StartupRecoveryResult{}, err
	}
	return result, nil
}

func recoveryCandidateIDs(candidates []ExpiredRuntimeRecoveryCandidate) (lifecycleIDs, unknownIDs, reconnectIDs []string) {
	for _, candidate := range candidates {
		switch candidate.ErrorClass {
		case "unknown_after_ack_started":
			unknownIDs = append(unknownIDs, candidate.GenerationID)
		case "orchestrator_restart_reconnect_grace_expired":
			reconnectIDs = append(reconnectIDs, candidate.GenerationID)
		default:
			if strings.HasPrefix(candidate.ErrorClass, "orchestrator_restart_during_") {
				lifecycleIDs = append(lifecycleIDs, candidate.GenerationID)
			}
		}
	}
	return lifecycleIDs, unknownIDs, reconnectIDs
}

func filterLifecycleRecoveryIDsTx(ctx context.Context, tx *sql.Tx, ids []string, now time.Time) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := appendStringIDs([]any{formatTime(now)}, ids)
	return queryStringColumnTx(ctx, tx, `
SELECT generation_id
FROM runtime_generations
WHERE status IN ('allocating','starting','probing','restoring','checkpointing')
  AND lease_expires_at IS NOT NULL
  AND lease_expires_at <= ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE sessions.id = runtime_generations.session_id
      AND sessions.active_generation_id = runtime_generations.generation_id
      AND sessions.status NOT IN ('failed', 'destroyed')
  )
  AND generation_id IN (`+sqlPlaceholders(len(ids))+`)`, args...)
}

func filterUnknownRecoveryIDsTx(ctx context.Context, tx *sql.Tx, ids []string, cutoff time.Time) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := appendStringIDs([]any{formatTime(cutoff), formatTime(cutoff)}, ids)
	return queryStringColumnTx(ctx, tx, `
SELECT generation_id
FROM runtime_generations
WHERE status IN ('active','idle')
  AND lease_expires_at IS NOT NULL
  AND lease_expires_at <= ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE sessions.id = runtime_generations.session_id
      AND sessions.active_generation_id = runtime_generations.generation_id
      AND sessions.status NOT IN ('failed', 'destroyed')
  )
  AND EXISTS (
    SELECT 1 FROM turns
    WHERE turns.generation_id = runtime_generations.generation_id
      AND turns.status = 'running'
      AND turns.ack_started_at IS NOT NULL
      AND turns.lease_expires_at IS NOT NULL
      AND turns.lease_expires_at <= ?
  )
  AND generation_id IN (`+sqlPlaceholders(len(ids))+`)`, args...)
}

func filterReconnectRecoveryIDsTx(ctx context.Context, tx *sql.Tx, ids []string, cutoff time.Time) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	args := appendStringIDs([]any{formatTime(cutoff)}, ids)
	return queryStringColumnTx(ctx, tx, `
SELECT generation_id
FROM runtime_generations
WHERE status IN ('active','idle')
  AND lease_expires_at IS NOT NULL
  AND lease_expires_at <= ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE sessions.id = runtime_generations.session_id
      AND sessions.active_generation_id = runtime_generations.generation_id
      AND sessions.status NOT IN ('failed', 'destroyed')
  )
  AND NOT EXISTS (
    SELECT 1 FROM turns
    WHERE turns.generation_id = runtime_generations.generation_id
      AND turns.status = 'running'
      AND turns.ack_started_at IS NOT NULL
  )
  AND generation_id IN (`+sqlPlaceholders(len(ids))+`)`, args...)
}

func repairRecoveredSessionTx(ctx context.Context, tx *sql.Tx, generationID string, now time.Time) (int64, error) {
	nowString := formatTime(now)
	res, err := tx.ExecContext(ctx, `
UPDATE sessions
SET status = CASE
      WHEN EXISTS (
        SELECT 1 FROM turns
        WHERE turns.session_id = sessions.id
          AND turns.status IN ('leased','running')
      ) THEN 'running_active'
      ELSE 'running_idle'
    END,
    checkpoint_path = NULL,
    restore_ms = NULL,
    error_class = NULL,
    failure_reason = NULL,
    updated_at = ?
WHERE active_generation_id = ?
  AND status NOT IN ('failed', 'destroyed')`, nowString, generationID)
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if affected == 0 {
		return 0, nil
	}
	var sessionID, sessionStatus, errorClass, reason string
	if err := tx.QueryRowContext(ctx, `
SELECT s.id, s.status, COALESCE(g.error_class, ''), COALESCE(g.failure_reason, '')
FROM sessions s
JOIN runtime_generations g ON g.generation_id = s.active_generation_id
WHERE s.active_generation_id = ?`, generationID).Scan(&sessionID, &sessionStatus, &errorClass, &reason); err != nil {
		return 0, err
	}
	return appendEventTx(ctx, tx, AppendEventParams{
		SessionID:    sessionID,
		GenerationID: generationID,
		DedupeKey:    "runtime_recovery:" + generationID + ":" + errorClass,
		Type:         "generation.error",
		Payload: map[string]any{
			"terminal":             false,
			"error_class":          errorClass,
			"error":                reason,
			"generation_id":        generationID,
			"session_status":       sessionStatus,
			"session_updated_at":   nowString,
			"active_generation_id": generationID,
			"restore_ms":           nil,
		},
		Now: now,
	})
}
