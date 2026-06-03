package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var errDuplicateOutputSequenceMismatch = errors.New("duplicate output_sequence mismatch")

func IsDuplicateOutputSequenceMismatch(err error) bool {
	return errors.Is(err, errDuplicateOutputSequenceMismatch)
}

type AppendEventParams struct {
	SessionID      string
	TurnID         *int64
	GenerationID   string
	Owner          string
	OutputSequence *int64
	DedupeKey      string
	ProxyRequestID string
	Stream         string
	Severity       string
	Type           string
	Payload        any
	Now            time.Time
}

func (s *Store) AppendEvent(ctx context.Context, p AppendEventParams) (int64, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if p.Type == "" {
		return 0, fmt.Errorf("event type is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	if p.OutputSequence != nil {
		if err := assertOutputEventTurnTx(ctx, tx, p); err != nil {
			return 0, err
		}
	}
	eventID, err := appendEventTx(ctx, tx, p)
	if err != nil {
		return 0, err
	}
	return eventID, tx.Commit()
}

func appendEventTx(ctx context.Context, tx *sql.Tx, p AppendEventParams) (int64, error) {
	payload, err := json.Marshal(p.Payload)
	if err != nil {
		return 0, err
	}
	res, err := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO events (
  session_id, turn_id, generation_id, output_sequence, dedupe_key,
  proxy_request_id, stream, severity, type, payload, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		nullableString(p.SessionID), nullableInt64Ptr(p.TurnID), nullableString(p.GenerationID),
		nullableInt64Ptr(p.OutputSequence), nullableString(p.DedupeKey), nullableString(p.ProxyRequestID),
		nullableString(p.Stream), nullableString(p.Severity), p.Type, string(payload), formatEventTime(p.Now))
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if affected == 0 {
		if err := assertDuplicateEventMatches(ctx, tx, p, string(payload)); err != nil {
			return 0, err
		}
		return 0, nil
	}
	return res.LastInsertId()
}

func assertDuplicateEventMatches(ctx context.Context, tx *sql.Tx, p AppendEventParams, payload string) error {
	if p.OutputSequence == nil || p.TurnID == nil {
		return nil
	}
	var existingType, existingPayload string
	var existingStream sql.NullString
	err := tx.QueryRowContext(ctx, `
SELECT type, stream, payload
FROM events
WHERE turn_id = ?
  AND generation_id = ?
  AND output_sequence = ?`,
		*p.TurnID, p.GenerationID, *p.OutputSequence).Scan(&existingType, &existingStream, &existingPayload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	stream := ""
	if existingStream.Valid {
		stream = existingStream.String
	}
	if existingType == p.Type && stream == p.Stream && existingPayload == payload {
		return nil
	}
	return fmt.Errorf("%w for turn %d generation %s sequence %d", errDuplicateOutputSequenceMismatch, *p.TurnID, p.GenerationID, *p.OutputSequence)
}

func assertOutputEventTurnTx(ctx context.Context, tx *sql.Tx, p AppendEventParams) error {
	if p.TurnID == nil {
		return fmt.Errorf("output event requires turn id")
	}
	var exists int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns t
JOIN runtime_generations g ON g.generation_id = t.generation_id
JOIN sessions s ON s.id = t.session_id
WHERE t.id = ?
  AND t.session_id = ?
  AND t.generation_id = ?
  AND t.status = 'running'
  AND t.lease_owner = ?
  AND t.lease_expires_at > ?
  AND g.generation_id = ?
  AND g.session_id = ?
  AND g.lease_owner = ?
  AND g.lease_expires_at > ?
  AND s.active_generation_id = ?`,
		*p.TurnID, p.SessionID, p.GenerationID, p.Owner, formatTime(p.Now),
		p.GenerationID, p.SessionID, p.Owner, formatTime(p.Now), p.GenerationID).Scan(&exists); err != nil {
		return err
	}
	if exists != 1 {
		return fmt.Errorf("output event turn CAS failed")
	}
	return nil
}
