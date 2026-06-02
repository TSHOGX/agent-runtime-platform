package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

var ErrStaleCheckpointRestore = errors.New("stale checkpoint restore")

type ClaimCheckpointedGenerationParams struct {
	SessionID    string
	GenerationID string
	Owner        string
	LeaseTTL     time.Duration
	Now          time.Time
}

type CheckpointCandidate struct {
	SessionID     string
	GenerationID  string
	BridgeDirPath string
}

type CompleteCheckpointParams struct {
	SessionID                       string
	GenerationID                    string
	Owner                           string
	CheckpointPath                  string
	RunscPlatform                   string
	RunscVersion                    string
	RunscBinaryPath                 string
	RunscBinaryDigest               string
	CheckpointBundleDigest          string
	CheckpointRuntimeConfigDigest   string
	CheckpointControlManifestDigest string
	CheckpointPlanDigest            string
	CheckpointImageManifestDigest   string
	Now                             time.Time
}

type RetireExpiredCheckpointsParams struct {
	OwnerUUID                string
	Now                      time.Time
	CheckpointImageRetention time.Duration
}

type RetiredCheckpoint struct {
	SessionID    string
	GenerationID string
	EventID      int64
}

func (s *Store) ClaimCheckpointedGenerationForRestore(ctx context.Context, p ClaimCheckpointedGenerationParams) (GenerationAllocation, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if strings.TrimSpace(p.SessionID) == "" {
		return GenerationAllocation{}, fmt.Errorf("session id is required")
	}
	if strings.TrimSpace(p.GenerationID) == "" {
		return GenerationAllocation{}, fmt.Errorf("generation id is required")
	}
	if strings.TrimSpace(p.Owner) == "" {
		return GenerationAllocation{}, fmt.Errorf("owner is required")
	}
	if p.LeaseTTL <= 0 {
		return GenerationAllocation{}, fmt.Errorf("lease ttl must be > 0")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GenerationAllocation{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := assertOwnerTx(ctx, tx, ownerUUIDFromLeaseOwner(p.Owner)); err != nil {
		return GenerationAllocation{}, err
	}
	currentFence, err := checkpointDriverStatesDigestTx(ctx, tx, p.SessionID, p.GenerationID)
	if err != nil {
		return GenerationAllocation{}, err
	}
	var storedFence string
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(checkpoint_driver_states_digest, '')
FROM runtime_generations
WHERE generation_id = ?
  AND session_id = ?`, p.GenerationID, p.SessionID).Scan(&storedFence); err != nil {
		return GenerationAllocation{}, err
	}
	if storedFence == "" {
		return GenerationAllocation{}, fmt.Errorf("%w: checkpoint driver state fence is missing", ErrStaleCheckpointRestore)
	}
	if storedFence != currentFence {
		return GenerationAllocation{}, fmt.Errorf("%w: checkpoint driver state fence mismatch", ErrStaleCheckpointRestore)
	}
	plan, err := getGenerationPlanTx(ctx, tx, p.GenerationID)
	if err != nil {
		return GenerationAllocation{}, err
	}
	var checkpointPlanDigest string
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(checkpoint_plan_digest, '')
FROM runtime_generations
WHERE generation_id = ?
  AND session_id = ?`, p.GenerationID, p.SessionID).Scan(&checkpointPlanDigest); err != nil {
		return GenerationAllocation{}, err
	}
	if checkpointPlanDigest == "" {
		return GenerationAllocation{}, fmt.Errorf("%w: checkpoint plan digest is missing", ErrStaleCheckpointRestore)
	}
	if checkpointPlanDigest != plan.PlanDigest {
		return GenerationAllocation{}, fmt.Errorf("%w: checkpoint plan digest mismatch", ErrStaleCheckpointRestore)
	}
	var checkpointBundleDigest, checkpointRuntimeConfigDigest, checkpointControlManifestDigest string
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(checkpoint_bundle_digest, ''),
       COALESCE(checkpoint_runtime_config_digest, ''),
       COALESCE(checkpoint_control_manifest_digest, '')
FROM runtime_generations
WHERE generation_id = ?
  AND session_id = ?`, p.GenerationID, p.SessionID).Scan(
		&checkpointBundleDigest, &checkpointRuntimeConfigDigest, &checkpointControlManifestDigest,
	); err != nil {
		return GenerationAllocation{}, err
	}
	if err := verifyCheckpointProjectionDigestChecksTx(ctx, tx, p.GenerationID, checkpointPlanDigest, checkpointProjectionDigestChecks(
		checkpointBundleDigest,
		checkpointRuntimeConfigDigest,
		checkpointControlManifestDigest,
	)); err != nil {
		return GenerationAllocation{}, fmt.Errorf("%w: %w", ErrStaleCheckpointRestore, err)
	}
	var checkpointImageManifestDigest string
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(checkpoint_image_manifest_digest, '')
FROM runtime_generations
WHERE generation_id = ?
  AND session_id = ?`, p.GenerationID, p.SessionID).Scan(&checkpointImageManifestDigest); err != nil {
		return GenerationAllocation{}, err
	}
	if checkpointImageManifestDigest == "" {
		return GenerationAllocation{}, fmt.Errorf("%w: checkpoint image manifest digest is missing", ErrStaleCheckpointRestore)
	}
	if strings.TrimSpace(checkpointImageManifestDigest) != checkpointImageManifestDigest ||
		!strings.HasPrefix(checkpointImageManifestDigest, "sha256:") {
		return GenerationAllocation{}, fmt.Errorf("%w: checkpoint image manifest digest is invalid", ErrStaleCheckpointRestore)
	}

	expiresAt := p.Now.Add(p.LeaseTTL)
	res, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'restoring',
    lease_owner = ?,
    lease_expires_at = ?,
    last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?
  AND status = 'checkpointed'
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = ?
      AND active_generation_id = ?
      AND status = 'checkpointed'
  )`, p.Owner, formatTime(expiresAt), formatTime(p.Now), p.GenerationID, p.SessionID, p.SessionID, p.GenerationID)
	if err != nil {
		return GenerationAllocation{}, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return GenerationAllocation{}, err
	}
	if affected != 1 {
		stale, err := staleCheckpointRestoreTx(ctx, tx, p.SessionID, p.GenerationID)
		if err != nil {
			return GenerationAllocation{}, err
		}
		if stale {
			return GenerationAllocation{}, fmt.Errorf("%w: checkpointed generation restore CAS failed", ErrStaleCheckpointRestore)
		}
		return GenerationAllocation{}, fmt.Errorf("checkpointed generation restore CAS failed")
	}

	res, err = tx.ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'recreating'
WHERE generation_id = ?
  AND session_id = ?
  AND allocation_state = 'reserved_checkpointed'`, p.GenerationID, p.SessionID)
	if err != nil {
		return GenerationAllocation{}, err
	}
	affected, err = res.RowsAffected()
	if err != nil {
		return GenerationAllocation{}, err
	}
	if affected != 1 {
		return GenerationAllocation{}, fmt.Errorf("checkpointed network restore CAS failed")
	}

	res, err = tx.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'recreating'
WHERE generation_id = ?
  AND resource_state = 'reserved_checkpointed'`, p.GenerationID)
	if err != nil {
		return GenerationAllocation{}, err
	}
	affected, err = res.RowsAffected()
	if err != nil {
		return GenerationAllocation{}, err
	}
	if affected != 1 {
		return GenerationAllocation{}, fmt.Errorf("checkpointed resource restore CAS failed")
	}

	allocation := GenerationAllocation{
		GenerationID:   p.GenerationID,
		Owner:          p.Owner,
		LeaseExpiresAt: expiresAt,
	}
	if err := tx.QueryRowContext(ctx, `
SELECT network_profile_id, agent_runtime_profile_id
FROM runtime_generations
WHERE generation_id = ?
  AND session_id = ?`, p.GenerationID, p.SessionID).Scan(&allocation.NetworkProfileID, &allocation.AgentRuntimeProfileID); err != nil {
		return GenerationAllocation{}, err
	}
	driverState, err := currentDriverStateTokenTx(ctx, tx, p.SessionID, p.GenerationID)
	if err != nil {
		return GenerationAllocation{}, err
	}
	allocation.DriverState = driverState
	if err := tx.Commit(); err != nil {
		return GenerationAllocation{}, err
	}
	return allocation, nil
}

func staleCheckpointRestoreTx(ctx context.Context, tx *sql.Tx, sessionID, generationID string) (bool, error) {
	var sessionStatus, activeGenerationID, generationStatus string
	if err := tx.QueryRowContext(ctx, `
SELECT s.status, COALESCE(s.active_generation_id, ''), COALESCE(g.status, '')
FROM sessions s
LEFT JOIN runtime_generations g ON g.session_id = s.id
  AND g.generation_id = ?
WHERE s.id = ?`, generationID, sessionID).Scan(&sessionStatus, &activeGenerationID, &generationStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return activeGenerationID != generationID ||
		sessionStatus != "checkpointed" ||
		generationStatus != "checkpointed", nil
}

func (s *Store) RetireExpiredCheckpoints(ctx context.Context, p RetireExpiredCheckpointsParams) ([]RetiredCheckpoint, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if p.CheckpointImageRetention < 0 {
		return nil, fmt.Errorf("checkpoint image retention must be >= 0")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := assertOwnerTx(ctx, tx, p.OwnerUUID); err != nil {
		return nil, err
	}

	cutoff := p.Now.Add(-p.CheckpointImageRetention)
	rows, err := tx.QueryContext(ctx, `
SELECT s.id, g.generation_id, s.last_activity_at
FROM sessions s
JOIN runtime_generations g ON g.generation_id = s.active_generation_id
  AND g.session_id = s.id
JOIN network_profiles n ON n.generation_id = g.generation_id
  AND n.session_id = s.id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE s.status = 'checkpointed'
  AND g.status = 'checkpointed'
  AND n.allocation_state = 'reserved_checkpointed'
  AND r.resource_state = 'reserved_checkpointed'
  AND COALESCE(s.last_activity_at, g.checkpoint_created_at, s.updated_at, s.created_at) < ?
ORDER BY COALESCE(s.last_activity_at, g.checkpoint_created_at, s.updated_at, s.created_at), s.id`, formatTime(cutoff))
	if err != nil {
		return nil, err
	}
	type candidate struct {
		sessionID      string
		generationID   string
		lastActivityAt sql.NullString
	}
	var candidates []candidate
	for rows.Next() {
		var candidate candidate
		if err := rows.Scan(&candidate.sessionID, &candidate.generationID, &candidate.lastActivityAt); err != nil {
			_ = rows.Close()
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	retired := make([]RetiredCheckpoint, 0, len(candidates))
	nowString := formatTime(p.Now)
	for _, candidate := range candidates {
		res, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'failed',
    error_class = 'checkpoint_retired',
    failure_reason = 'checkpoint image retired after retention window',
    ended_at = ?,
    lease_owner = NULL,
    lease_expires_at = NULL
WHERE generation_id = ?
  AND session_id = ?
  AND status = 'checkpointed'
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = ?
      AND active_generation_id = ?
      AND status = 'checkpointed'
  )`, nowString, candidate.generationID, candidate.sessionID, candidate.sessionID, candidate.generationID)
		if err != nil {
			return nil, err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if affected != 1 {
			return nil, fmt.Errorf("checkpoint retirement generation CAS failed")
		}
		res, err = tx.ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reclaimable'
WHERE generation_id = ?
  AND session_id = ?
  AND allocation_state = 'reserved_checkpointed'`, candidate.generationID, candidate.sessionID)
		if err != nil {
			return nil, err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if affected != 1 {
			return nil, fmt.Errorf("checkpoint retirement network CAS failed")
		}
		res, err = tx.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'reclaimable'
WHERE generation_id = ?
  AND resource_state = 'reserved_checkpointed'`, candidate.generationID)
		if err != nil {
			return nil, err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if affected != 1 {
			return nil, fmt.Errorf("checkpoint retirement resource CAS failed")
		}
		res, err = tx.ExecContext(ctx, `
UPDATE sessions
SET status = 'running_idle',
    checkpoint_path = NULL,
    restore_ms = NULL,
    updated_at = ?
WHERE id = ?
  AND status = 'checkpointed'
  AND active_generation_id = ?`, nowString, candidate.sessionID, candidate.generationID)
		if err != nil {
			return nil, err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if affected != 1 {
			return nil, fmt.Errorf("checkpoint retirement session CAS failed")
		}
		var lastActivity any
		if candidate.lastActivityAt.Valid {
			lastActivity = candidate.lastActivityAt.String
		}
		eventID, err := appendEventTx(ctx, tx, AppendEventParams{
			SessionID:    candidate.sessionID,
			GenerationID: candidate.generationID,
			DedupeKey:    "checkpoint_retired:" + candidate.generationID,
			Type:         "session.checkpoint_retired",
			Payload: map[string]any{
				"terminal":                 false,
				"generation_id":            candidate.generationID,
				"session_status":           "running_idle",
				"status":                   "running_idle",
				"session_updated_at":       nowString,
				"updated_at":               nowString,
				"session_last_activity_at": lastActivity,
				"active_generation_id":     candidate.generationID,
				"restore_ms":               nil,
			},
			Now: p.Now,
		})
		if err != nil {
			return nil, err
		}
		retired = append(retired, RetiredCheckpoint{SessionID: candidate.sessionID, GenerationID: candidate.generationID, EventID: eventID})
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return retired, nil
}

func (s *Store) ListAutoCheckpointCandidates(ctx context.Context, owner string, now time.Time, idleThreshold time.Duration) ([]CheckpointCandidate, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if strings.TrimSpace(owner) == "" {
		return nil, fmt.Errorf("owner is required")
	}
	if idleThreshold < 0 {
		return nil, fmt.Errorf("idle threshold must be >= 0")
	}
	cutoff := now.Add(-idleThreshold)
	rows, err := s.db.QueryContext(ctx, `
SELECT s.id, g.generation_id, ri.bridge_dir_path
FROM sessions s
JOIN runtime_generations g ON g.generation_id = s.active_generation_id
  AND g.session_id = s.id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
JOIN runtime_resource_instances ri ON ri.generation_id = g.generation_id
  AND ri.session_id = g.session_id
WHERE s.status = 'running_idle'
  AND s.auto_checkpoint_enabled = 1
  AND s.last_activity_at IS NOT NULL
  AND s.last_activity_at <= ?
  AND g.status = 'idle'
  AND g.auto_checkpoint_enabled = 1
  AND g.lease_owner = ?
  AND g.lease_expires_at > ?
  AND g.runsc_version IS NOT NULL
  AND g.runsc_platform IS NOT NULL
  AND ri.state = 'live'
  AND r.checkpoint_path IS NOT NULL
  AND r.control_manifest_digest IS NOT NULL
  AND r.projected_control_manifest_digest IS NOT NULL
  AND r.bundle_digest IS NOT NULL
  AND r.runtime_config_digest IS NOT NULL
  AND r.spec_digest IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM turns t
    WHERE t.session_id = s.id
      AND t.status IN ('queued', 'leased', 'running')
  )
ORDER BY s.last_activity_at ASC`, formatTime(cutoff), owner, formatTime(now))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var candidates []CheckpointCandidate
	for rows.Next() {
		var candidate CheckpointCandidate
		if err := rows.Scan(&candidate.SessionID, &candidate.GenerationID, &candidate.BridgeDirPath); err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, candidate := range candidates {
		if _, err := s.GetSandboxContractForGeneration(ctx, candidate.SessionID, candidate.GenerationID); err != nil {
			return nil, err
		}
	}
	return candidates, nil
}

func (s *Store) BeginGenerationCheckpoint(ctx context.Context, sessionID, generationID, owner string, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := getSandboxContractForGenerationWithGenerationMirror(ctx, tx, sessionID, generationID); err != nil {
		return err
	}
	checkpointDriverStatesDigest, err := checkpointDriverStatesDigestTx(ctx, tx, sessionID, generationID)
	if err != nil {
		return err
	}
	plan, err := getGenerationPlanTx(ctx, tx, generationID)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'checkpointing',
    last_seen_at = ?,
    checkpoint_driver_states_digest = ?,
    checkpoint_plan_digest = ?
WHERE generation_id = ?
  AND session_id = ?
  AND status = 'idle'
  AND auto_checkpoint_enabled = 1
  AND lease_owner = ?
  AND lease_expires_at > ?
  AND runsc_version IS NOT NULL
  AND runsc_platform IS NOT NULL
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = ?
      AND active_generation_id = ?
      AND status = 'running_idle'
      AND auto_checkpoint_enabled = 1
  )
  AND EXISTS (
    SELECT 1 FROM runtime_generation_resources r
    JOIN runtime_resource_instances ri ON ri.generation_id = r.generation_id
    WHERE r.generation_id = runtime_generations.generation_id
      AND ri.session_id = runtime_generations.session_id
      AND ri.state = 'live'
      AND r.checkpoint_path IS NOT NULL
      AND r.control_manifest_digest IS NOT NULL
      AND r.projected_control_manifest_digest IS NOT NULL
      AND r.bundle_digest IS NOT NULL
      AND r.runtime_config_digest IS NOT NULL
      AND r.spec_digest IS NOT NULL
  )
  AND NOT EXISTS (
    SELECT 1 FROM turns
    WHERE session_id = ?
      AND status IN ('queued', 'leased', 'running')
  )`, formatTime(now), checkpointDriverStatesDigest, plan.PlanDigest, generationID, sessionID, owner, formatTime(now), sessionID, generationID, sessionID)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("generation checkpoint begin CAS failed")
	}
	res, err = tx.ExecContext(ctx, `
UPDATE sessions
SET status = 'checkpointing',
    updated_at = ?
WHERE id = ?
  AND status = 'running_idle'
  AND active_generation_id = ?
  AND auto_checkpoint_enabled = 1`, formatTime(now), sessionID, generationID)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("session checkpoint begin CAS failed")
	}
	return tx.Commit()
}

func (s *Store) AbortGenerationCheckpoint(ctx context.Context, sessionID, generationID, owner string, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'idle',
    last_seen_at = ?,
    checkpoint_driver_states_digest = NULL,
    checkpoint_plan_digest = NULL
WHERE generation_id = ?
  AND session_id = ?
  AND status = 'checkpointing'
  AND lease_owner = ?
  AND lease_expires_at > ?`, formatTime(now), generationID, sessionID, owner, formatTime(now))
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("generation checkpoint abort CAS failed")
	}
	res, err = tx.ExecContext(ctx, `
UPDATE sessions
SET status = 'running_idle',
    updated_at = ?
WHERE id = ?
  AND status = 'checkpointing'
  AND active_generation_id = ?`, formatTime(now), sessionID, generationID)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("session checkpoint abort CAS failed")
	}
	return tx.Commit()
}

func (s *Store) CompleteGenerationCheckpoint(ctx context.Context, p CompleteCheckpointParams) error {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if strings.TrimSpace(p.CheckpointPath) == "" {
		return fmt.Errorf("checkpoint path is required")
	}
	if strings.TrimSpace(p.CheckpointPath) != p.CheckpointPath ||
		!filepath.IsAbs(p.CheckpointPath) ||
		filepath.Clean(p.CheckpointPath) != p.CheckpointPath {
		return fmt.Errorf("checkpoint path must be canonical absolute")
	}
	if strings.TrimSpace(p.CheckpointBundleDigest) == "" {
		return fmt.Errorf("checkpoint bundle digest is required")
	}
	if strings.TrimSpace(p.CheckpointRuntimeConfigDigest) == "" {
		return fmt.Errorf("checkpoint runtime config digest is required")
	}
	if strings.TrimSpace(p.CheckpointControlManifestDigest) == "" {
		return fmt.Errorf("checkpoint control manifest digest is required")
	}
	if strings.TrimSpace(p.CheckpointPlanDigest) == "" {
		return fmt.Errorf("checkpoint plan digest is required")
	}
	if strings.TrimSpace(p.CheckpointImageManifestDigest) == "" {
		return fmt.Errorf("checkpoint image manifest digest is required")
	}
	if strings.TrimSpace(p.CheckpointImageManifestDigest) != p.CheckpointImageManifestDigest ||
		!strings.HasPrefix(p.CheckpointImageManifestDigest, "sha256:") {
		return fmt.Errorf("checkpoint image manifest digest is invalid")
	}
	if strings.TrimSpace(p.RunscVersion) == "" {
		return fmt.Errorf("checkpoint runsc version is required")
	}
	if strings.TrimSpace(p.RunscPlatform) == "" {
		return fmt.Errorf("checkpoint runsc platform is required")
	}
	if strings.TrimSpace(p.RunscBinaryPath) == "" {
		return fmt.Errorf("checkpoint runsc binary path is required")
	}
	if strings.TrimSpace(p.RunscBinaryPath) != p.RunscBinaryPath ||
		!filepath.IsAbs(p.RunscBinaryPath) ||
		filepath.Clean(p.RunscBinaryPath) != p.RunscBinaryPath {
		return fmt.Errorf("checkpoint runsc binary path must be canonical absolute")
	}
	if strings.TrimSpace(p.RunscBinaryDigest) == "" {
		return fmt.Errorf("checkpoint runsc binary digest is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	currentFence, err := checkpointDriverStatesDigestTx(ctx, tx, p.SessionID, p.GenerationID)
	if err != nil {
		return err
	}
	var storedFence string
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(checkpoint_driver_states_digest, '')
FROM runtime_generations
WHERE generation_id = ?
  AND session_id = ?`, p.GenerationID, p.SessionID).Scan(&storedFence); err != nil {
		return err
	}
	if storedFence == "" {
		return fmt.Errorf("checkpoint driver state fence is missing")
	}
	if storedFence != currentFence {
		return fmt.Errorf("checkpoint driver state fence mismatch")
	}
	var storedPlanDigest string
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(checkpoint_plan_digest, '')
FROM runtime_generations
WHERE generation_id = ?
  AND session_id = ?`, p.GenerationID, p.SessionID).Scan(&storedPlanDigest); err != nil {
		return err
	}
	if storedPlanDigest == "" {
		return fmt.Errorf("checkpoint plan digest is missing")
	}
	if storedPlanDigest != strings.TrimSpace(p.CheckpointPlanDigest) {
		return fmt.Errorf("checkpoint plan digest mismatch")
	}
	plan, err := getGenerationPlanTx(ctx, tx, p.GenerationID)
	if err != nil {
		return err
	}
	if plan.PlanDigest != storedPlanDigest {
		return fmt.Errorf("checkpoint stored plan digest mismatch")
	}
	if err := verifyCheckpointProjectionDigestsTx(ctx, tx, p); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'checkpointed',
    checkpoint_created_at = ?,
    checkpoint_network_profile_id = network_profile_id,
    checkpoint_agent_runtime_profile_id = agent_runtime_profile_id,
    checkpoint_runsc_version = ?,
    checkpoint_runsc_platform = ?,
    checkpoint_runsc_binary_path = ?,
    checkpoint_runsc_binary_digest = ?,
    checkpoint_bundle_digest = ?,
    checkpoint_runtime_config_digest = ?,
    checkpoint_control_manifest_digest = ?,
    checkpoint_plan_digest = ?,
    checkpoint_image_manifest_digest = ?,
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?
  AND status = 'checkpointing'
  AND lease_owner = ?
  AND lease_expires_at > ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = ?
      AND active_generation_id = ?
      AND status = 'checkpointing'
  )`, formatTime(p.Now), p.RunscVersion, p.RunscPlatform, p.RunscBinaryPath, p.RunscBinaryDigest,
		p.CheckpointBundleDigest, p.CheckpointRuntimeConfigDigest, p.CheckpointControlManifestDigest,
		p.CheckpointPlanDigest, p.CheckpointImageManifestDigest, formatTime(p.Now), p.GenerationID, p.SessionID, p.Owner, formatTime(p.Now), p.SessionID, p.GenerationID)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("generation checkpoint complete CAS failed")
	}
	res, err = tx.ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reserved_checkpointed'
WHERE generation_id = ?
  AND session_id = ?
  AND allocation_state = 'live'`, p.GenerationID, p.SessionID)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("network checkpoint complete CAS failed")
	}
	res, err = tx.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'reserved_checkpointed',
    checkpoint_path = ?
WHERE generation_id = ?
  AND resource_state IN ('allocating','ready','live','recreating')
  AND EXISTS (
    SELECT 1 FROM runtime_resource_instances ri
    WHERE ri.generation_id = runtime_generation_resources.generation_id
      AND ri.session_id = ?
      AND ri.state = 'live'
  )`, p.CheckpointPath, p.GenerationID, p.SessionID)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("resource checkpoint complete CAS failed")
	}
	res, err = tx.ExecContext(ctx, `
UPDATE sessions
SET status = 'checkpointed',
    checkpoint_path = ?,
    updated_at = ?
WHERE id = ?
  AND status = 'checkpointing'
  AND active_generation_id = ?`, p.CheckpointPath, formatTime(p.Now), p.SessionID, p.GenerationID)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected != 1 {
		return fmt.Errorf("session checkpoint complete CAS failed")
	}
	return tx.Commit()
}

type checkpointProjectionDigestCheck struct {
	kind   string
	digest string
}

func checkpointProjectionDigestChecks(bundleDigest, runtimeConfigDigest, controlManifestDigest string) []checkpointProjectionDigestCheck {
	return []checkpointProjectionDigestCheck{
		{kind: GenerationPlanProjectionBundle, digest: bundleDigest},
		{kind: GenerationPlanProjectionRuntimeConfig, digest: runtimeConfigDigest},
		{kind: GenerationPlanProjectionControlManifestProjected, digest: controlManifestDigest},
	}
}

func verifyCheckpointProjectionDigestsTx(ctx context.Context, tx *sql.Tx, p CompleteCheckpointParams) error {
	return verifyCheckpointProjectionDigestChecksTx(ctx, tx, p.GenerationID, p.CheckpointPlanDigest, checkpointProjectionDigestChecks(
		p.CheckpointBundleDigest,
		p.CheckpointRuntimeConfigDigest,
		p.CheckpointControlManifestDigest,
	))
}

func verifyCheckpointProjectionDigestChecksTx(ctx context.Context, tx *sql.Tx, generationID, planDigest string, checks []checkpointProjectionDigestCheck) error {
	for _, check := range checks {
		if strings.TrimSpace(check.digest) == "" {
			return fmt.Errorf("checkpoint projection %s digest is missing", check.kind)
		}
		projection, err := getGenerationPlanProjectionTx(ctx, tx, generationID, check.kind)
		if err != nil {
			return fmt.Errorf("checkpoint projection %s: %w", check.kind, err)
		}
		if projection.PlanDigest != strings.TrimSpace(planDigest) {
			return fmt.Errorf("checkpoint projection %s plan digest mismatch", check.kind)
		}
		expected := generationPlanProjectionPayloadDigest(check.kind, check.digest)
		if projection.PayloadDigest != expected {
			return fmt.Errorf("checkpoint projection %s digest mismatch", check.kind)
		}
	}
	return nil
}
