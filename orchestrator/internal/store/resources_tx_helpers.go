package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

func assertOwnerTx(ctx context.Context, tx *sql.Tx, ownerUUID string) error {
	if strings.TrimSpace(ownerUUID) == "" {
		return fmt.Errorf("owner uuid is required")
	}
	var got string
	if err := tx.QueryRowContext(ctx, `SELECT uuid FROM orchestrator_owner WHERE singleton = 1`).Scan(&got); err != nil {
		return err
	}
	if got != ownerUUID {
		return fmt.Errorf("orchestrator owner uuid mismatch: db=%s process=%s", got, ownerUUID)
	}
	return nil
}

func queryStringColumnTx(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]string, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func appendStringIDs(args []any, ids []string) []any {
	for _, id := range ids {
		args = append(args, id)
	}
	return args
}

func markAllocationsReclaimableTx(ctx context.Context, tx *sql.Tx, generationIDs []string) error {
	if len(generationIDs) == 0 {
		return nil
	}
	args := appendStringIDs(nil, generationIDs)
	if _, err := tx.ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reclaimable'
WHERE allocation_state IN ('allocating','ready','live','reserved_checkpointed','recreating')
  AND generation_id IN (`+sqlPlaceholders(len(generationIDs))+`)`, args...); err != nil {
		return err
	}
	args = appendStringIDs(nil, generationIDs)
	if _, err := tx.ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'reclaimable'
WHERE resource_state IN ('allocating','ready','live','reserved_checkpointed','recreating')
  AND generation_id IN (`+sqlPlaceholders(len(generationIDs))+`)`, args...); err != nil {
		return err
	}
	return nil
}

func requeueExpiredLeasedTurnsTx(ctx context.Context, tx *sql.Tx, generationIDs []string, now time.Time) (int64, error) {
	if len(generationIDs) == 0 {
		return 0, nil
	}
	args := appendStringIDs([]any{formatTime(now)}, generationIDs)
	res, err := tx.ExecContext(ctx, `
UPDATE turns
SET status = 'queued',
    generation_id = NULL,
    lease_owner = NULL,
    lease_expires_at = NULL,
    claim_request_id = NULL,
    claim_granted_at = NULL,
    started_at = NULL,
    ack_started_at = NULL,
    completed_by_generation = NULL,
    completed_at = NULL,
    error_class = NULL,
    error = NULL,
    attempt = attempt + 1
WHERE lease_expires_at IS NOT NULL
  AND lease_expires_at <= ?
  AND generation_id IN (`+sqlPlaceholders(len(generationIDs))+`)
  AND status = 'leased'
  AND ack_started_at IS NULL`, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func deleteActiveContextsForGenerationsTx(ctx context.Context, tx *sql.Tx, generationIDs []string) error {
	if len(generationIDs) == 0 {
		return nil
	}
	args := appendStringIDs(nil, generationIDs)
	_, err := tx.ExecContext(ctx, `
DELETE FROM active_model_request_contexts
WHERE generation_id IN (`+sqlPlaceholders(len(generationIDs))+`)`, args...)
	return err
}

func updateSessionActiveGenerationTx(ctx context.Context, tx *sql.Tx, p SessionActiveGenerationCASParams) error {
	args := []any{p.NextGenerationID, p.SessionID}
	where := "active_generation_id IS NULL"
	if p.ExpectedGenerationID.Valid {
		where = "active_generation_id = ?"
		args = append(args, p.ExpectedGenerationID.String)
	}
	res, err := tx.ExecContext(ctx, `
UPDATE sessions
SET active_generation_id = ?
WHERE id = ?
  AND `+where, args...)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("session active generation CAS failed")
	}
	return nil
}
