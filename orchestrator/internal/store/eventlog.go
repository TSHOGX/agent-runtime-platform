package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type EventRecord struct {
	EventID        int64
	SessionID      string
	TurnID         *int64
	GenerationID   string
	OutputSequence *int64
	ProxyRequestID string
	Stream         string
	Severity       string
	Type           string
	Payload        json.RawMessage
	CreatedAt      time.Time
}

type ListEventsParams struct {
	AfterEventID int64
	SessionID    string
	Limit        int
}

type PruneEventsParams struct {
	RetentionWindow time.Duration
	RetentionRows   int64
	Now             time.Time
}

func (s *Store) GetEvent(ctx context.Context, eventID int64) (EventRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT event_id, session_id, turn_id, generation_id, output_sequence,
  proxy_request_id, stream, severity, type, payload, created_at
FROM events
WHERE event_id = ?`, eventID)
	record, err := scanEventRecord(row)
	if err == nil {
		return record, true, nil
	}
	if err == sql.ErrNoRows {
		return EventRecord{}, false, nil
	}
	return EventRecord{}, false, err
}

func (s *Store) ListEvents(ctx context.Context, p ListEventsParams) ([]EventRecord, error) {
	query := `
SELECT event_id, session_id, turn_id, generation_id, output_sequence,
  proxy_request_id, stream, severity, type, payload, created_at
FROM events
WHERE event_id > ?`
	args := []any{p.AfterEventID}
	if p.SessionID != "" {
		query += ` AND session_id = ?`
		args = append(args, p.SessionID)
	}
	query += ` ORDER BY event_id`
	if p.Limit > 0 {
		query += ` LIMIT ?`
		args = append(args, p.Limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []EventRecord
	for rows.Next() {
		record, err := scanEventRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (s *Store) OldestEventID(ctx context.Context, sessionID string) (int64, bool, error) {
	query := `SELECT MIN(event_id) FROM events`
	args := []any{}
	if sessionID != "" {
		query += ` WHERE session_id = ?`
		args = append(args, sessionID)
	}
	var oldest sql.NullInt64
	if err := s.db.QueryRowContext(ctx, query, args...).Scan(&oldest); err != nil {
		return 0, false, err
	}
	if !oldest.Valid {
		return 0, false, nil
	}
	return oldest.Int64, true, nil
}

func (s *Store) PruneEvents(ctx context.Context, p PruneEventsParams) (int64, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	if p.RetentionWindow < 0 {
		return 0, fmt.Errorf("event retention window must be >= 0")
	}
	if p.RetentionRows < 0 {
		return 0, fmt.Errorf("event retention rows must be >= 0")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	var deleted int64
	if p.RetentionWindow > 0 {
		res, err := tx.ExecContext(ctx, `
DELETE FROM events
WHERE created_at < ?`, formatEventTime(p.Now.Add(-p.RetentionWindow)))
		if err != nil {
			return 0, err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}
		deleted += affected
	}
	if p.RetentionRows > 0 {
		res, err := tx.ExecContext(ctx, `
DELETE FROM events
WHERE event_id NOT IN (
  SELECT event_id
  FROM events
  ORDER BY event_id DESC
  LIMIT ?
)`, p.RetentionRows)
		if err != nil {
			return 0, err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}
		deleted += affected
	}
	return deleted, tx.Commit()
}

type eventScanner interface {
	Scan(dest ...any) error
}

func scanEventRecord(scanner eventScanner) (EventRecord, error) {
	var record EventRecord
	var sessionID, generationID, proxyRequestID, stream, severity sql.NullString
	var turnID, outputSequence sql.NullInt64
	var payload, createdAt string
	if err := scanner.Scan(
		&record.EventID, &sessionID, &turnID, &generationID, &outputSequence,
		&proxyRequestID, &stream, &severity, &record.Type, &payload, &createdAt,
	); err != nil {
		return EventRecord{}, err
	}
	record.SessionID = sessionID.String
	if turnID.Valid {
		id := turnID.Int64
		record.TurnID = &id
	}
	record.GenerationID = generationID.String
	if outputSequence.Valid {
		sequence := outputSequence.Int64
		record.OutputSequence = &sequence
	}
	record.ProxyRequestID = proxyRequestID.String
	record.Stream = stream.String
	record.Severity = severity.String
	record.Payload = json.RawMessage(payload)
	record.CreatedAt = parseTime(createdAt)
	return record, nil
}
