package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"time"
)

var errDuplicateOutputSequenceMismatch = errors.New("duplicate output_sequence mismatch")

func IsDuplicateOutputSequenceMismatch(err error) bool {
	return errors.Is(err, errDuplicateOutputSequenceMismatch)
}

// errPermanentTurnCompletion marks a CompleteTurn error as a definitive
// rejection: a CAS conflict (the turn/generation lease no longer holds) or a
// driver-state validation failure (the bridge supplied semantically invalid
// data). Such errors can never succeed on retry, so the caller may retire the
// generation. Transient/infra errors (e.g. "database is locked", BeginTx /
// Commit failures, the driver-state lease-check query error) are deliberately
// left unmarked so the caller retries them instead of failing the generation.
var errPermanentTurnCompletion = errors.New("permanent turn completion failure")

// IsPermanentTurnCompletion reports whether a CompleteTurn error is a definitive
// rejection that must not be retried.
func IsPermanentTurnCompletion(err error) bool {
	return errors.Is(err, errPermanentTurnCompletion)
}

// permanentTurnCompletionf builds a new permanent turn-completion error.
func permanentTurnCompletionf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{errPermanentTurnCompletion}, args...)...)
}

// permanentTurnCompletion marks an existing error as a permanent
// turn-completion rejection while preserving it for errors.Is/As inspection.
func permanentTurnCompletion(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", errPermanentTurnCompletion, err)
}

// permanentRequireOneRow behaves like requireOneRow but classifies the
// "affected != 1" CAS conflict as a permanent turn-completion failure. A
// RowsAffected() transport error is left unmarked so it is retried.
func permanentRequireOneRow(result sql.Result, message string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return permanentTurnCompletionf(message)
	}
	return nil
}

type ClaimNextTurnParams struct {
	SessionID    string
	GenerationID string
	Owner        string
	RequestID    string
	LeaseTTL     time.Duration
	Now          time.Time
}

type BridgeProtocolEvidence struct {
	DriverID              string
	ProtocolVersion       int
	TurnInputSchema       string
	AgentManifestDigest   string
	RuntimeConfigDigest   string
	ContractGateVersion   string
	SandboxContractDigest string
}

type ResumeTurnParams struct {
	SessionID       string
	GenerationID    string
	TurnID          int64
	Owner           string
	LeaseTTL        time.Duration
	AckStartedGrace time.Duration
	Now             time.Time
}

type TurnGrant struct {
	TurnID             int64
	Sequence           int64
	Content            string
	Attempt            int
	Replayed           bool
	ExpiresAt          time.Time
	DriverState        DriverStateToken
	DriverStatePayload []byte
}

type BridgeHelloAck struct {
	LastOutputSequenceByTurn map[int64]int64
	LeasedTurnID             *int64
	ServerTime               time.Time
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

type AckStartedParams struct {
	SessionID       string
	GenerationID    string
	TurnID          int64
	Owner           string
	SandboxSourceIP string
	LeaseTTL        time.Duration
	EventType       string
	EventDedupeKey  string
	EventPayload    any
	Now             time.Time
}

type CompleteTurnParams struct {
	SessionID         string
	GenerationID      string
	TurnID            int64
	Owner             string
	TerminalStatus    string
	ErrorClass        string
	Error             string
	DriverStateUpdate *DriverStateUpdate
	EventType         string
	EventDedupeKey    string
	EventPayload      any
	Now               time.Time
}

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

func (s *Store) BridgeHelloAck(ctx context.Context, sessionID, generationID, owner string, now time.Time, ackStartedGrace time.Duration) (BridgeHelloAck, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return BridgeHelloAck{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var exists int
	activeLeaseQuery := `
SELECT COUNT(*)
FROM runtime_generations g
JOIN sessions s ON s.id = g.session_id
WHERE g.session_id = ?
  AND g.generation_id = ?
  AND g.lease_owner = ?
  AND g.status IN ('idle','active','probing','restoring','starting')
  AND g.lease_expires_at > ?
  AND s.active_generation_id = ?`
	if err := tx.QueryRowContext(ctx, activeLeaseQuery, sessionID, generationID, owner, formatTime(now), generationID).Scan(&exists); err != nil {
		return BridgeHelloAck{}, err
	}
	if exists != 1 {
		if ackStartedGrace <= 0 {
			return BridgeHelloAck{}, fmt.Errorf("bridge hello generation CAS failed")
		}
		cutoff := now.Add(-ackStartedGrace)
		if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM runtime_generations g
JOIN sessions s ON s.id = g.session_id
WHERE g.session_id = ?
  AND g.generation_id = ?
  AND g.status IN ('active', 'idle')
  AND g.lease_expires_at IS NOT NULL
  AND g.lease_expires_at <= ?
  AND g.lease_expires_at > ?
  AND s.active_generation_id = ?
  AND s.status NOT IN ('failed', 'destroyed')
  AND EXISTS (
    SELECT 1 FROM turns t
    WHERE t.session_id = g.session_id
      AND t.generation_id = g.generation_id
      AND t.status = 'running'
      AND t.ack_started_at IS NOT NULL
      AND t.lease_expires_at IS NOT NULL
      AND t.lease_expires_at <= ?
      AND t.lease_expires_at > ?
  )`, sessionID, generationID, formatTime(now), formatTime(cutoff), generationID, formatTime(now), formatTime(cutoff)).Scan(&exists); err != nil {
			return BridgeHelloAck{}, err
		}
		if exists != 1 {
			return BridgeHelloAck{}, fmt.Errorf("bridge hello generation CAS failed")
		}
	}
	ack := BridgeHelloAck{
		LastOutputSequenceByTurn: map[int64]int64{},
		ServerTime:               now,
	}
	rows, err := tx.QueryContext(ctx, `
SELECT t.id, COALESCE(MAX(e.output_sequence), 0)
FROM turns t
LEFT JOIN events e ON e.turn_id = t.id
  AND e.generation_id = t.generation_id
  AND e.output_sequence IS NOT NULL
WHERE t.session_id = ?
  AND t.generation_id = ?
  AND t.status IN ('leased','running')
GROUP BY t.id
ORDER BY t.id`, sessionID, generationID)
	if err != nil {
		return BridgeHelloAck{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var turnID int64
		var lastSequence int64
		if err := rows.Scan(&turnID, &lastSequence); err != nil {
			return BridgeHelloAck{}, err
		}
		ack.LastOutputSequenceByTurn[turnID] = lastSequence
		if ack.LeasedTurnID == nil {
			id := turnID
			ack.LeasedTurnID = &id
		}
	}
	if err := rows.Err(); err != nil {
		return BridgeHelloAck{}, err
	}
	return ack, tx.Commit()
}

func (s *Store) BridgeProtocolEvidence(ctx context.Context, sessionID, generationID string) (BridgeProtocolEvidence, error) {
	record, err := getSandboxContractForGenerationWithGenerationMirror(ctx, s.db, sessionID, generationID)
	if err != nil {
		return BridgeProtocolEvidence{}, err
	}
	if record.ContractGateVersion != SandboxContractGateDriverManifest {
		return BridgeProtocolEvidence{}, fmt.Errorf("bridge protocol v2 requires driver manifest evidence, got %s", record.ContractGateVersion)
	}
	object, err := decodeSandboxContractObject(record.CanonicalPayload)
	if err != nil {
		return BridgeProtocolEvidence{}, err
	}
	driver, ok := object["driver"].(map[string]any)
	if !ok {
		return BridgeProtocolEvidence{}, fmt.Errorf("sandbox contract missing driver object")
	}
	driverID := strings.TrimSpace(stringValue(driver["driver_id"]))
	if driverID == "" {
		return BridgeProtocolEvidence{}, fmt.Errorf("sandbox contract missing driver_id")
	}
	protocolVersion := 0
	switch value := driver["bridge_protocol_version"].(type) {
	case json.Number:
		parsed, err := value.Int64()
		if err == nil {
			protocolVersion = int(parsed)
		}
	case float64:
		protocolVersion = int(value)
	}
	turnInputSchema := strings.TrimSpace(stringValue(driver["turn_input_schema"]))
	inputDigests, ok := object["input_digests"].(map[string]any)
	if !ok {
		return BridgeProtocolEvidence{}, fmt.Errorf("sandbox contract missing input_digests")
	}
	agentManifestDigest := strings.TrimSpace(stringValue(inputDigests["agent_manifest_digest"]))
	runtimeConfigDigest := strings.TrimSpace(stringValue(inputDigests["runtime_config_digest"]))
	if !strings.HasPrefix(agentManifestDigest, "sha256:") || !strings.HasPrefix(runtimeConfigDigest, "sha256:") {
		return BridgeProtocolEvidence{}, fmt.Errorf("bridge protocol v2 requires manifest-backed input digests")
	}
	return BridgeProtocolEvidence{
		DriverID:              driverID,
		ProtocolVersion:       protocolVersion,
		TurnInputSchema:       turnInputSchema,
		AgentManifestDigest:   agentManifestDigest,
		RuntimeConfigDigest:   runtimeConfigDigest,
		ContractGateVersion:   record.ContractGateVersion,
		SandboxContractDigest: record.SandboxContractDigest,
	}, nil
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
    JOIN runtime_resource_instances ri ON ri.generation_id = g.generation_id
    WHERE g.generation_id = ?
      AND g.session_id = ?
      AND g.sandbox_contract_version = 'sandbox-isolation-v1'
      AND g.status IN ('idle', 'active')
      AND g.lease_owner = ?
      AND g.lease_expires_at > ?
      AND s.active_generation_id = ?
      AND ri.state = 'live'
      AND ri.sandbox_contract_version = 'sandbox-isolation-v1'
      AND ri.contract_id = g.sandbox_contract_id
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
	if _, err := getSandboxContractForGenerationWithGenerationMirror(ctx, tx, p.SessionID, p.GenerationID); err != nil {
		return TurnGrant{}, false, err
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

	grant, err = turnGrantByID(ctx, tx, p.SessionID, p.GenerationID, grant.TurnID, p.Owner, p.Now)
	if err != nil {
		return TurnGrant{}, false, err
	}
	grant.ExpiresAt = expiresAt
	return grant, true, tx.Commit()
}

func (s *Store) ResumeTurn(ctx context.Context, p ResumeTurnParams) (TurnGrant, bool, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return TurnGrant{}, false, err
	}
	defer func() { _ = tx.Rollback() }()
	expiresAt := p.Now.Add(p.LeaseTTL)
	res, err := tx.ExecContext(ctx, `
UPDATE turns
SET lease_expires_at = ?
WHERE id = ?
  AND session_id = ?
  AND generation_id = ?
  AND status IN ('leased', 'running')
  AND lease_owner = ?
  AND EXISTS (
    SELECT 1 FROM runtime_generations g
    JOIN sessions s ON s.id = g.session_id
    WHERE g.generation_id = ?
      AND g.session_id = ?
      AND g.status IN ('active', 'idle')
      AND g.lease_owner = ?
      AND g.lease_expires_at > ?
      AND s.active_generation_id = ?
  )`, formatTime(expiresAt), p.TurnID, p.SessionID, p.GenerationID, p.Owner,
		p.GenerationID, p.SessionID, p.Owner, formatTime(p.Now), p.GenerationID)
	if err != nil {
		return TurnGrant{}, false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return TurnGrant{}, false, err
	}
	recoveringExpired := false
	if affected != 1 {
		if p.AckStartedGrace <= 0 {
			return TurnGrant{}, false, tx.Commit()
		}
		if err := assertOwnerTx(ctx, tx, ownerUUIDFromLeaseOwner(p.Owner)); err != nil {
			return TurnGrant{}, false, err
		}
		cutoff := p.Now.Add(-p.AckStartedGrace)
		res, err = tx.ExecContext(ctx, `
UPDATE turns
SET lease_owner = ?,
    lease_expires_at = ?
WHERE id = ?
  AND session_id = ?
  AND generation_id = ?
  AND status = 'running'
  AND ack_started_at IS NOT NULL
  AND lease_expires_at IS NOT NULL
  AND lease_expires_at <= ?
  AND lease_expires_at > ?
  AND EXISTS (
    SELECT 1 FROM runtime_generations g
    JOIN sessions s ON s.id = g.session_id
    WHERE g.generation_id = ?
      AND g.session_id = ?
      AND g.status IN ('active', 'idle')
      AND g.lease_expires_at IS NOT NULL
      AND g.lease_expires_at <= ?
      AND g.lease_expires_at > ?
      AND s.active_generation_id = ?
      AND s.status NOT IN ('failed', 'destroyed')
  )`, p.Owner, formatTime(expiresAt), p.TurnID, p.SessionID, p.GenerationID,
			formatTime(p.Now), formatTime(cutoff), p.GenerationID, p.SessionID,
			formatTime(p.Now), formatTime(cutoff), p.GenerationID)
		if err != nil {
			return TurnGrant{}, false, err
		}
		affected, err = res.RowsAffected()
		if err != nil {
			return TurnGrant{}, false, err
		}
		if affected != 1 {
			return TurnGrant{}, false, tx.Commit()
		}
		recoveringExpired = true
	}
	var generationUpdate string
	var generationArgs []any
	if recoveringExpired {
		cutoff := p.Now.Add(-p.AckStartedGrace)
		generationUpdate = `
UPDATE runtime_generations
SET status = 'active',
    lease_owner = ?,
    lease_expires_at = ?,
    last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?
  AND status IN ('active', 'idle')
  AND lease_expires_at IS NOT NULL
  AND lease_expires_at <= ?
  AND lease_expires_at > ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = ?
      AND active_generation_id = ?
      AND status NOT IN ('failed', 'destroyed')
  )`
		generationArgs = []any{
			p.Owner, formatTime(expiresAt), formatTime(p.Now), p.GenerationID, p.SessionID,
			formatTime(p.Now), formatTime(cutoff), p.SessionID, p.GenerationID,
		}
	} else {
		generationUpdate = `
UPDATE runtime_generations
SET status = 'active',
    lease_expires_at = ?,
    last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?
  AND status IN ('active', 'idle')
  AND lease_owner = ?
  AND lease_expires_at > ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = ?
      AND active_generation_id = ?
  )`
		generationArgs = []any{
			formatTime(expiresAt), formatTime(p.Now), p.GenerationID, p.SessionID,
			p.Owner, formatTime(p.Now), p.SessionID, p.GenerationID,
		}
	}
	res, err = tx.ExecContext(ctx, generationUpdate, generationArgs...)
	if err != nil {
		return TurnGrant{}, false, err
	}
	affected, err = res.RowsAffected()
	if err != nil {
		return TurnGrant{}, false, err
	}
	if affected != 1 {
		return TurnGrant{}, false, fmt.Errorf("generation CAS failed after turn resume")
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE active_model_request_contexts
SET expires_at = ?,
    lease_owner = ?,
    updated_at = ?
WHERE session_id = ?
  AND generation_id = ?
  AND turn_id = ?
  AND turn_id IN (
    SELECT id FROM turns
    WHERE session_id = ?
      AND generation_id = ?
      AND lease_owner = ?
      AND status = 'running'
  )`, formatTime(expiresAt), p.Owner, formatTime(p.Now), p.SessionID, p.GenerationID, p.TurnID, p.SessionID, p.GenerationID, p.Owner); err != nil {
		return TurnGrant{}, false, err
	}
	grant, err := turnGrantByID(ctx, tx, p.SessionID, p.GenerationID, p.TurnID, p.Owner, p.Now)
	if err != nil {
		return TurnGrant{}, false, err
	}
	grant.ExpiresAt = expiresAt
	return grant, true, tx.Commit()
}

func (s *Store) AckTurnStarted(ctx context.Context, p AckStartedParams) (int64, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := getSandboxContractForGenerationWithGenerationMirror(ctx, tx, p.SessionID, p.GenerationID); err != nil {
		return 0, err
	}
	expiresAt := p.Now.Add(p.LeaseTTL)
	var alreadyRunning int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns
WHERE id = ?
  AND status = 'running'
  AND ack_started_at IS NOT NULL
  AND session_id = ?
  AND generation_id = ?
  AND lease_owner = ?
  AND lease_expires_at > ?`,
		p.TurnID, p.SessionID, p.GenerationID, p.Owner, formatTime(p.Now)).Scan(&alreadyRunning); err != nil {
		return 0, err
	}
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
		return 0, err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return 0, err
	} else if affected != 1 && alreadyRunning != 1 {
		return 0, fmt.Errorf("turn ack_started CAS failed")
	}
	startedNow := alreadyRunning != 1
	res, err = tx.ExecContext(ctx, `
UPDATE runtime_generations
SET last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?
  AND sandbox_contract_version = 'sandbox-isolation-v1'
  AND status = 'active'
  AND lease_owner = ?
  AND lease_expires_at > ?
  AND EXISTS (
    SELECT 1 FROM sessions
    WHERE id = ?
      AND active_generation_id = ?
  )
  AND EXISTS (
    SELECT 1 FROM runtime_resource_instances ri
    WHERE ri.generation_id = ?
      AND ri.state = 'live'
      AND ri.sandbox_contract_version = 'sandbox-isolation-v1'
      AND ri.contract_id = runtime_generations.sandbox_contract_id
  )`, formatTime(p.Now), p.GenerationID, p.SessionID, p.Owner, formatTime(p.Now), p.SessionID, p.GenerationID, p.GenerationID)
	if err != nil {
		return 0, err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return 0, err
	} else if affected != 1 {
		return 0, fmt.Errorf("generation ack_started CAS failed")
	}
	sandboxSourceIP, err := sandboxSourceIPForGenerationTx(ctx, tx, p.SessionID, p.GenerationID)
	if err != nil {
		return 0, err
	}
	var modelAccessAllowed int
	if err := tx.QueryRowContext(ctx, `
SELECT a.model_access_allowed
FROM runtime_generations g
JOIN agent_runtime_profiles a ON a.agent_runtime_profile_id = g.agent_runtime_profile_id
WHERE g.session_id = ?
  AND g.generation_id = ?`, p.SessionID, p.GenerationID).Scan(&modelAccessAllowed); err != nil {
		return 0, err
	}
	if startedNow {
		_, err = tx.ExecContext(ctx, `
INSERT INTO active_model_request_contexts (
  sandbox_source_ip, session_id, generation_id, turn_id,
  lease_owner, expires_at, model_access_allowed, next_request_sequence, registered_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?) ON CONFLICT(sandbox_source_ip) DO UPDATE SET
  session_id = excluded.session_id,
  generation_id = excluded.generation_id,
  turn_id = excluded.turn_id,
  lease_owner = excluded.lease_owner,
  expires_at = excluded.expires_at,
  model_access_allowed = excluded.model_access_allowed,
  next_request_sequence = excluded.next_request_sequence,
  registered_at = excluded.registered_at,
  updated_at = excluded.updated_at`,
			sandboxSourceIP, p.SessionID, p.GenerationID, p.TurnID, p.Owner, formatTime(expiresAt), modelAccessAllowed, formatTime(p.Now), formatTime(p.Now))
		if err != nil {
			return 0, err
		}
	}
	var eventID int64
	if p.EventType != "" {
		eventID, err = appendEventTx(ctx, tx, AppendEventParams{
			SessionID:    p.SessionID,
			TurnID:       &p.TurnID,
			GenerationID: p.GenerationID,
			DedupeKey:    p.EventDedupeKey,
			Type:         p.EventType,
			Payload:      p.EventPayload,
			Now:          p.Now,
		})
		if err != nil {
			return 0, err
		}
	}
	return eventID, tx.Commit()
}

func sandboxSourceIPForGenerationTx(ctx context.Context, tx *sql.Tx, sessionID, generationID string) (string, error) {
	var sandboxIP string
	if err := tx.QueryRowContext(ctx, `
SELECT ri.sandbox_ip
FROM runtime_resource_instances ri
JOIN runtime_generations g ON g.generation_id = ri.generation_id
WHERE g.session_id = ?
  AND g.generation_id = ?
  AND g.sandbox_contract_version = 'sandbox-isolation-v1'
  AND ri.state = 'live'
  AND ri.sandbox_contract_version = 'sandbox-isolation-v1'
  AND ri.contract_id = g.sandbox_contract_id`, sessionID, generationID).Scan(&sandboxIP); err != nil {
		return "", err
	}
	addr, err := netip.ParseAddr(sandboxIP)
	if err != nil {
		return "", fmt.Errorf("invalid runtime resource sandbox_ip %q: %w", sandboxIP, err)
	}
	return addr.String(), nil
}

func (s *Store) CompleteTurn(ctx context.Context, p CompleteTurnParams) (int64, error) {
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	switch p.TerminalStatus {
	case "completed", "failed", "canceled":
	default:
		return 0, permanentTurnCompletionf("invalid terminal turn status %q", p.TerminalStatus)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := applyDriverStateUpdateTx(ctx, tx, p); err != nil {
		return 0, err
	}
	var alreadyTerminal int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns
WHERE id = ?
  AND status = ?
  AND session_id = ?
  AND generation_id = ?
  AND completed_by_generation = ?`,
		p.TurnID, p.TerminalStatus, p.SessionID, p.GenerationID, p.GenerationID).Scan(&alreadyTerminal); err != nil {
		return 0, err
	}
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
		return 0, err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return 0, err
	} else if affected != 1 && alreadyTerminal != 1 {
		return 0, permanentTurnCompletionf("turn completion CAS failed")
	}
	sessionMarkedIdle := false
	if alreadyTerminal != 1 {
		sessionMarkedIdle, err = markGenerationAndSessionIdleIfNoInflight(ctx, tx, p.SessionID, p.GenerationID, p.TurnID, p.Owner, p.Now)
		if err != nil {
			return 0, err
		}
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM active_model_request_contexts
WHERE session_id = ?
  AND generation_id = ?
  AND turn_id = ?`, p.SessionID, p.GenerationID, p.TurnID); err != nil {
		return 0, err
	}
	var eventID int64
	if p.EventType != "" {
		eventPayload, err := completeTurnEventPayload(p.EventPayload, p, sessionMarkedIdle)
		if err != nil {
			return 0, err
		}
		eventID, err = appendEventTx(ctx, tx, AppendEventParams{
			SessionID:    p.SessionID,
			TurnID:       &p.TurnID,
			GenerationID: p.GenerationID,
			DedupeKey:    p.EventDedupeKey,
			Type:         p.EventType,
			Payload:      eventPayload,
			Now:          p.Now,
		})
		if err != nil {
			return 0, err
		}
	}
	return eventID, tx.Commit()
}

func completeTurnEventPayload(base any, p CompleteTurnParams, sessionMarkedIdle bool) (map[string]any, error) {
	payload := map[string]any{}
	if base != nil {
		raw, err := json.Marshal(base)
		if err != nil {
			return nil, err
		}
		if string(raw) != "null" {
			if err := json.Unmarshal(raw, &payload); err != nil {
				return nil, err
			}
		}
	}
	delete(payload, "driver_state_update")
	payload["session_marked_idle"] = sessionMarkedIdle
	payload["session_terminal"] = false
	if sessionMarkedIdle {
		payload["session_status"] = "running_idle"
		payload["session_updated_at"] = formatTime(p.Now)
		payload["session_last_activity_at"] = formatTime(p.Now)
		payload["active_generation_id"] = p.GenerationID
	}
	return payload, nil
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

func claimReplay(ctx context.Context, tx *sql.Tx, p ClaimNextTurnParams) (TurnGrant, bool, error) {
	row := tx.QueryRowContext(ctx, `
SELECT id
FROM turns
WHERE session_id = ?
  AND generation_id = ?
  AND claim_request_id = ?
  AND status IN ('leased', 'running')
  AND lease_owner = ?
  AND lease_expires_at > ?
  AND EXISTS (
    SELECT 1 FROM runtime_resource_instances ri
    JOIN runtime_generations g ON g.generation_id = ri.generation_id
    WHERE ri.generation_id = ?
      AND ri.state = 'live'
      AND ri.sandbox_contract_version = 'sandbox-isolation-v1'
      AND g.sandbox_contract_version = 'sandbox-isolation-v1'
      AND ri.contract_id = g.sandbox_contract_id
  )`,
		p.SessionID, p.GenerationID, p.RequestID, p.Owner, formatTime(p.Now), p.GenerationID)
	var turnID int64
	err := row.Scan(&turnID)
	if errors.Is(err, sql.ErrNoRows) {
		return TurnGrant{}, false, nil
	}
	if err != nil {
		return TurnGrant{}, false, err
	}
	if _, err := getSandboxContractForGenerationWithGenerationMirror(ctx, tx, p.SessionID, p.GenerationID); err != nil {
		return TurnGrant{}, false, err
	}
	grant, err := turnGrantByID(ctx, tx, p.SessionID, p.GenerationID, turnID, p.Owner, p.Now)
	if err != nil {
		return TurnGrant{}, false, err
	}
	return grant, true, nil
}

func turnGrantByID(ctx context.Context, tx *sql.Tx, sessionID, generationID string, turnID int64, owner string, now time.Time) (TurnGrant, error) {
	row := tx.QueryRowContext(ctx, `
SELECT t.id, t.sequence, t.content, t.attempt, t.lease_expires_at,
       ds.driver_id, ds.state_digest, ds.state_version, ds.state_payload
FROM turns t
JOIN runtime_generations g
  ON g.session_id = t.session_id
 AND g.generation_id = t.generation_id
JOIN agent_runtime_profiles a
  ON a.agent_runtime_profile_id = g.agent_runtime_profile_id
JOIN session_driver_states ds
  ON ds.session_id = t.session_id
 AND ds.driver_id = a.driver_id
WHERE t.id = ?
  AND t.session_id = ?
  AND t.generation_id = ?
  AND t.status IN ('leased', 'running')
  AND t.lease_owner = ?
  AND t.lease_expires_at > ?
  AND EXISTS (
    SELECT 1 FROM runtime_resource_instances ri
    WHERE ri.generation_id = ?
      AND ri.state = 'live'
      AND ri.sandbox_contract_version = 'sandbox-isolation-v1'
      AND g.sandbox_contract_version = 'sandbox-isolation-v1'
      AND ri.contract_id = g.sandbox_contract_id
  )`,
		turnID, sessionID, generationID, owner, formatTime(now), generationID)
	var grant TurnGrant
	var leaseExpires string
	err := row.Scan(
		&grant.TurnID,
		&grant.Sequence,
		&grant.Content,
		&grant.Attempt,
		&leaseExpires,
		&grant.DriverState.DriverID,
		&grant.DriverState.StateDigest,
		&grant.DriverState.StateVersion,
		&grant.DriverStatePayload,
	)
	if err != nil {
		return TurnGrant{}, err
	}
	grant.ExpiresAt = parseTime(leaseExpires)
	return grant, nil
}

func nullableInt64Ptr(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
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

func markGenerationAndSessionIdleIfNoInflight(ctx context.Context, tx *sql.Tx, sessionID, generationID string, currentTurnID int64, owner string, now time.Time) (bool, error) {
	var ownsActiveGeneration int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM runtime_generations g
JOIN sessions s ON s.id = g.session_id
WHERE g.generation_id = ?
  AND g.session_id = ?
  AND g.status = 'active'
  AND g.lease_owner = ?
  AND g.lease_expires_at > ?
  AND s.id = ?
  AND s.active_generation_id = ?`,
		generationID, sessionID, owner, formatTime(now), sessionID, generationID).Scan(&ownsActiveGeneration); err != nil {
		return false, err
	}
	if ownsActiveGeneration != 1 {
		return false, permanentTurnCompletionf("generation idle CAS failed")
	}

	var pendingTurns int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns
WHERE session_id = ?
  AND id <> ?
  AND status IN ('queued', 'leased', 'running')`, sessionID, currentTurnID).Scan(&pendingTurns); err != nil {
		return false, err
	}
	if pendingTurns != 0 {
		return false, nil
	}

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
    WHERE session_id = ?
      AND id <> ?
      AND status IN ('queued', 'leased', 'running')
  )`, generationID, sessionID, owner, formatTime(now), sessionID, generationID, sessionID, currentTurnID)
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected != 1 {
		return false, permanentTurnCompletionf("generation idle CAS failed")
	}
	res, err = tx.ExecContext(ctx, `
UPDATE sessions
SET status = 'running_idle',
    updated_at = ?,
    last_activity_at = ?
WHERE id = ?
  AND status NOT IN ('failed', 'destroyed')
  AND active_generation_id = ?
  AND NOT EXISTS (
    SELECT 1 FROM turns
    WHERE session_id = ?
      AND id <> ?
      AND status IN ('queued', 'leased', 'running')
  )`, formatTime(now), formatTime(now), sessionID, generationID, sessionID, currentTurnID)
	if err != nil {
		return false, err
	}
	affected, err = res.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected != 1 {
		return false, permanentTurnCompletionf("session idle CAS failed")
	}
	return true, nil
}
