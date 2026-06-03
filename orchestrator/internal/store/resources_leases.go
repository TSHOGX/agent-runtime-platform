package store

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type BridgePollGeneration struct {
	SessionID     string
	GenerationID  string
	BridgeDirPath string
}

type RenewLiveGenerationsParams struct {
	Owner    string
	LeaseTTL time.Duration
	Now      time.Time
}

func GenerationLeaseOwner(ownerUUID string) string {
	return strings.TrimSpace(ownerUUID) + ":" + RuntimeManagerRoleTag
}

func (s *Store) RenewLiveGenerationLeases(ctx context.Context, p RenewLiveGenerationsParams) (int64, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if strings.TrimSpace(p.Owner) == "" {
		return 0, fmt.Errorf("owner is required")
	}
	if p.LeaseTTL <= 0 {
		return 0, fmt.Errorf("lease ttl must be > 0")
	}
	expiresAt := p.Now.Add(p.LeaseTTL)
	res, err := s.db.ExecContext(ctx, `
UPDATE runtime_generations
SET lease_expires_at = ?,
    last_seen_at = ?
WHERE status IN ('starting','probing','active','idle','checkpointing','restoring')
  AND lease_owner = ?
  AND lease_expires_at > ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = runtime_generations.session_id
      AND active_generation_id = runtime_generations.generation_id
      AND status NOT IN ('failed', 'destroyed')
  )`, formatTime(expiresAt), formatTime(p.Now), p.Owner, formatTime(p.Now))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) ListBridgePollGenerations(ctx context.Context, owner string, now time.Time, ackStartedGrace time.Duration) ([]BridgePollGeneration, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if strings.TrimSpace(owner) == "" {
		return nil, fmt.Errorf("owner is required")
	}
	args := []any{owner, formatTime(now)}
	recoverableWhere := ""
	if ackStartedGrace > 0 {
		cutoff := now.Add(-ackStartedGrace)
		recoverableWhere = `
  OR (
    g.status IN ('active','idle')
    AND g.lease_expires_at IS NOT NULL
    AND g.lease_expires_at <= ?
    AND g.lease_expires_at > ?
    AND EXISTS (
      SELECT 1 FROM turns t
      WHERE t.session_id = g.session_id
        AND t.generation_id = g.generation_id
        AND t.status = 'running'
        AND t.ack_started_at IS NOT NULL
        AND t.lease_expires_at IS NOT NULL
        AND t.lease_expires_at <= ?
        AND t.lease_expires_at > ?
    )
  )`
		args = append(args, formatTime(now), formatTime(cutoff), formatTime(now), formatTime(cutoff))
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT g.session_id, g.generation_id, ri.bridge_dir_path
FROM runtime_generations g
JOIN runtime_resource_instances ri ON ri.generation_id = g.generation_id
  AND ri.session_id = g.session_id
JOIN sessions s ON s.id = g.session_id
WHERE g.status IN ('active','idle','probing','restoring','starting')
  AND (
    (g.lease_owner = ? AND g.lease_expires_at > ?)
`+recoverableWhere+`
  )
  AND s.active_generation_id = g.generation_id
  AND s.status NOT IN ('failed', 'destroyed')
  AND ri.state = 'live'
ORDER BY g.session_id, g.generation_id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var generations []BridgePollGeneration
	for rows.Next() {
		var generation BridgePollGeneration
		if err := rows.Scan(&generation.SessionID, &generation.GenerationID, &generation.BridgeDirPath); err != nil {
			return nil, err
		}
		generations = append(generations, generation)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, generation := range generations {
		if _, err := s.GetSandboxContractForGeneration(ctx, generation.SessionID, generation.GenerationID); err != nil {
			return nil, err
		}
	}
	return generations, nil
}

func (s *Store) GetRuntimeGenerationStatus(ctx context.Context, sessionID, generationID string) (string, error) {
	var status string
	err := s.db.QueryRowContext(ctx, `
SELECT status
FROM runtime_generations
WHERE session_id = ?
  AND generation_id = ?`, sessionID, generationID).Scan(&status)
	return status, err
}
