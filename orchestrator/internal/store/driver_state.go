package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/agents"
)

const (
	driverStateDigestPrefix            = "driver_state_digest_v1\n"
	checkpointDriverStatesDigestPrefix = "checkpoint_driver_states_digest_v1\n"
	claudeDriverStateKind              = "claude_session"
	emptyDriverStateKind               = "empty"
)

type DriverStateToken struct {
	DriverID     string `json:"driver_id"`
	StateDigest  string `json:"state_digest"`
	StateVersion int    `json:"state_version"`
}

type DriverStateUpdate struct {
	DriverID            string          `json:"driver_id"`
	PreviousStateDigest string          `json:"previous_state_digest"`
	StatePayload        json.RawMessage `json:"state_payload"`
	StateDigest         string          `json:"state_digest"`
	StateVersion        int             `json:"state_version"`
}

type driverStateRow struct {
	SessionID           string
	DriverID            string
	StatePayload        []byte
	StateDigest         string
	StateVersion        int
	UpdatedGenerationID string
	UpdatedTurnID       sql.NullInt64
	UpdatedAt           time.Time
}

type driverStateBootstrapContext struct {
	SessionID string
}

type driverStateCodec struct {
	driverID                             agents.ID
	bootstrap                            func(driverStateBootstrapContext) (any, error)
	validatePayload                      func(map[string]any) error
	validateHostPayload                  func(canonicalPayload []byte, agentHomeHostPath, expectedCompletedTurnID string) error
	validateCompletedUpdateAgainstHostTx func(context.Context, *sql.Tx, CompleteTurnParams, []byte) error
	synthesizeCompletedUpdateTx          func(context.Context, *sql.Tx, CompleteTurnParams, driverStateRow) error
}

func driverStateCodecRegistry() map[agents.ID]driverStateCodec {
	return map[agents.ID]driverStateCodec{
		agents.ClaudeCode: {
			driverID: agents.ClaudeCode,
			bootstrap: func(ctx driverStateBootstrapContext) (any, error) {
				sessionID := strings.TrimSpace(ctx.SessionID)
				if sessionID == "" {
					return nil, fmt.Errorf("session id is required")
				}
				return map[string]any{
					"schema_version": 1,
					"driver_id":      string(agents.ClaudeCode),
					"state_kind":     claudeDriverStateKind,
					// Claude Code private resume state; not a session column or runtime manifest field.
					"claude_session_uuid":    "bootstrap-" + sessionID,
					"initialized":            false,
					"last_completed_turn_id": nil,
				}, nil
			},
			validatePayload:             validateClaudeDriverStatePayload,
			synthesizeCompletedUpdateTx: synthesizeClaudeCompletedDriverStateUpdateTx,
		},
		agents.Shell: {
			driverID: agents.Shell,
			bootstrap: func(driverStateBootstrapContext) (any, error) {
				return map[string]any{
					"schema_version": 1,
					"driver_id":      string(agents.Shell),
					"state_kind":     emptyDriverStateKind,
				}, nil
			},
			validatePayload: validateEmptyDriverStatePayload,
		},
		agents.Pi: {
			driverID:                             agents.Pi,
			bootstrap:                            bootstrapPiDriverState,
			validatePayload:                      validatePiDriverStatePayload,
			validateHostPayload:                  validatePiDriverStatePayloadForHost,
			validateCompletedUpdateAgainstHostTx: validatePiDriverStateUpdateAgainstHostTx,
		},
	}
}

func driverStateCodecFor(driverID string) (driverStateCodec, bool) {
	codec, ok := driverStateCodecRegistry()[agents.ID(strings.TrimSpace(driverID))]
	return codec, ok
}

func DriverStateDigest(canonicalPayload []byte) string {
	sum := sha256.Sum256(append([]byte(driverStateDigestPrefix), canonicalPayload...))
	return "sha256:" + fmt.Sprintf("%x", sum[:])
}

func CheckpointDriverStatesDigest(generationID string, states []DriverStateToken) (string, error) {
	if strings.TrimSpace(generationID) == "" {
		return "", fmt.Errorf("generation id is required")
	}
	if len(states) == 0 {
		return "", fmt.Errorf("checkpoint driver state set is required")
	}
	type driver struct {
		DriverID     string `json:"driver_id"`
		StateDigest  string `json:"state_digest"`
		StateVersion int    `json:"state_version"`
	}
	drivers := make([]driver, 0, len(states))
	for _, state := range states {
		if _, ok := agents.Lookup(state.DriverID); !ok {
			return "", fmt.Errorf("unsupported driver %q", state.DriverID)
		}
		if !strings.HasPrefix(strings.TrimSpace(state.StateDigest), "sha256:") {
			return "", fmt.Errorf("driver state digest is required")
		}
		if state.StateVersion <= 0 {
			return "", fmt.Errorf("driver state version must be positive")
		}
		drivers = append(drivers, driver{
			DriverID:     strings.TrimSpace(state.DriverID),
			StateDigest:  strings.TrimSpace(state.StateDigest),
			StateVersion: state.StateVersion,
		})
	}
	sort.Slice(drivers, func(i, j int) bool { return drivers[i].DriverID < drivers[j].DriverID })
	payload := map[string]any{
		"schema_version": 1,
		"generation_id":  strings.TrimSpace(generationID),
		"drivers":        drivers,
	}
	canonical, err := canonicalDataVolumeJSON(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(append([]byte(checkpointDriverStatesDigestPrefix), canonical...))
	return "sha256:" + fmt.Sprintf("%x", sum[:]), nil
}

func canonicalBootstrapDriverState(driverID, sessionID string) ([]byte, string, error) {
	codec, ok := driverStateCodecFor(driverID)
	if !ok {
		return nil, "", fmt.Errorf("unsupported driver %q", driverID)
	}
	payload, err := codec.bootstrap(driverStateBootstrapContext{SessionID: sessionID})
	if err != nil {
		return nil, "", err
	}
	return canonicalDriverStatePayload(payload, string(codec.driverID))
}

func canonicalDriverStatePayload(value any, driverID string) ([]byte, string, error) {
	var canonical []byte
	var err error
	switch v := value.(type) {
	case []byte:
		canonical, err = canonicalDataVolumeJSONBytes(v)
	case json.RawMessage:
		canonical, err = canonicalDataVolumeJSONBytes(v)
	default:
		canonical, err = canonicalDataVolumeJSON(value)
	}
	if err != nil {
		return nil, "", err
	}
	if err := validateDriverStatePayload(canonical, driverID); err != nil {
		return nil, "", err
	}
	return canonical, DriverStateDigest(canonical), nil
}

func validateDriverStatePayload(canonicalPayload []byte, driverID string) error {
	object, err := decodeSandboxContractObject(canonicalPayload)
	if err != nil {
		return err
	}
	schemaVersion, ok := object["schema_version"].(json.Number)
	if !ok || schemaVersion.String() != "1" {
		return fmt.Errorf("driver state schema_version must be 1")
	}
	if got, _ := object["driver_id"].(string); got != strings.TrimSpace(driverID) {
		return fmt.Errorf("driver state driver_id = %q, want %q", got, strings.TrimSpace(driverID))
	}
	codec, ok := driverStateCodecFor(driverID)
	if !ok {
		return fmt.Errorf("unsupported driver %q", driverID)
	}
	return codec.validatePayload(object)
}

func validateClaudeDriverStatePayload(object map[string]any) error {
	if got, _ := object["state_kind"].(string); got != claudeDriverStateKind {
		return fmt.Errorf("claude driver state_kind = %q", got)
	}
	if strings.TrimSpace(stringValue(object["claude_session_uuid"])) == "" {
		return fmt.Errorf("claude driver state uuid is required")
	}
	if _, ok := object["initialized"].(bool); !ok {
		return fmt.Errorf("claude driver state initialized is required")
	}
	if _, ok := object["last_completed_turn_id"]; !ok {
		return fmt.Errorf("claude driver state last_completed_turn_id is required")
	}
	return nil
}

func validateEmptyDriverStatePayload(object map[string]any) error {
	if got, _ := object["state_kind"].(string); got != emptyDriverStateKind {
		return fmt.Errorf("shell driver state_kind = %q", got)
	}
	return nil
}

func stringValue(value any) string {
	if value == nil {
		return ""
	}
	if s, ok := value.(string); ok {
		return s
	}
	return ""
}

func ensureAllocationDriverStateTx(ctx context.Context, tx *sql.Tx, sessionID, generationID, driverID string, now time.Time) (DriverStateToken, error) {
	row, err := getDriverStateTx(ctx, tx, sessionID, driverID)
	if err == nil {
		return DriverStateToken{DriverID: row.DriverID, StateDigest: row.StateDigest, StateVersion: row.StateVersion}, nil
	}
	if err != nil && err != sql.ErrNoRows {
		return DriverStateToken{}, err
	}
	var priorGenerations int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM runtime_generations g
JOIN agent_runtime_profiles a ON a.agent_runtime_profile_id = g.agent_runtime_profile_id
WHERE g.session_id = ?
  AND a.driver_id = ?
  AND g.generation_id <> ?`, sessionID, driverID, generationID).Scan(&priorGenerations); err != nil {
		return DriverStateToken{}, err
	}
	if priorGenerations != 0 {
		return DriverStateToken{}, fmt.Errorf("missing driver state for existing %s session", driverID)
	}
	payload, digest, err := canonicalBootstrapDriverState(driverID, sessionID)
	if err != nil {
		return DriverStateToken{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO session_driver_states (
  session_id, driver_id, state_payload, state_digest, state_version,
  updated_generation_id, updated_turn_id, updated_at
) VALUES (?, ?, ?, ?, 1, ?, NULL, ?)`,
		sessionID, driverID, string(payload), digest, generationID, formatTime(now)); err != nil {
		return DriverStateToken{}, err
	}
	return DriverStateToken{DriverID: driverID, StateDigest: digest, StateVersion: 1}, nil
}

func getDriverStateTx(ctx context.Context, tx *sql.Tx, sessionID, driverID string) (driverStateRow, error) {
	row := tx.QueryRowContext(ctx, `
SELECT
  session_id, driver_id, state_payload, state_digest, state_version,
  updated_generation_id, updated_turn_id, updated_at
FROM session_driver_states
WHERE session_id = ?
  AND driver_id = ?`, sessionID, driverID)
	var state driverStateRow
	var payload, updatedAt string
	if err := row.Scan(
		&state.SessionID,
		&state.DriverID,
		&payload,
		&state.StateDigest,
		&state.StateVersion,
		&state.UpdatedGenerationID,
		&state.UpdatedTurnID,
		&updatedAt,
	); err != nil {
		return driverStateRow{}, err
	}
	state.StatePayload = []byte(payload)
	state.UpdatedAt = parseTime(updatedAt)
	if err := validateDriverStateRow(state); err != nil {
		return driverStateRow{}, err
	}
	return state, nil
}

func validateDriverStateRow(state driverStateRow) error {
	canonical, digest, err := canonicalDriverStatePayload(state.StatePayload, state.DriverID)
	if err != nil {
		return err
	}
	if !bytes.Equal(canonical, state.StatePayload) {
		return fmt.Errorf("driver state payload is not canonical")
	}
	if digest != state.StateDigest {
		return fmt.Errorf("driver state digest mismatch: got %s want %s", digest, state.StateDigest)
	}
	if state.StateVersion <= 0 {
		return fmt.Errorf("driver state version must be positive")
	}
	return nil
}

func currentDriverStateTokenTx(ctx context.Context, tx *sql.Tx, sessionID, generationID string) (DriverStateToken, error) {
	var driverID string
	if err := tx.QueryRowContext(ctx, `
SELECT a.driver_id
FROM runtime_generations g
JOIN agent_runtime_profiles a ON a.agent_runtime_profile_id = g.agent_runtime_profile_id
WHERE g.session_id = ?
  AND g.generation_id = ?`, sessionID, generationID).Scan(&driverID); err != nil {
		return DriverStateToken{}, err
	}
	state, err := getDriverStateTx(ctx, tx, sessionID, driverID)
	if err != nil {
		return DriverStateToken{}, err
	}
	return DriverStateToken{DriverID: driverID, StateDigest: state.StateDigest, StateVersion: state.StateVersion}, nil
}

func checkpointDriverStatesDigestTx(ctx context.Context, tx *sql.Tx, sessionID, generationID string) (string, error) {
	token, err := currentDriverStateTokenTx(ctx, tx, sessionID, generationID)
	if err != nil {
		return "", err
	}
	return CheckpointDriverStatesDigest(generationID, []DriverStateToken{token})
}

func applyDriverStateUpdateTx(ctx context.Context, tx *sql.Tx, p CompleteTurnParams) error {
	if p.DriverStateUpdate == nil {
		return synthesizeCompletedDriverStateUpdateTx(ctx, tx, p)
	}
	update := *p.DriverStateUpdate
	driverID, err := agents.CanonicalDriverID(update.DriverID)
	if err != nil {
		return permanentTurnCompletion(err)
	}
	if p.TerminalStatus != "completed" {
		return permanentTurnCompletionf("driver state update is only valid for completed turns")
	}
	canonical, nextDigest, err := canonicalDriverStatePayload(update.StatePayload, string(driverID))
	if err != nil {
		return permanentTurnCompletion(err)
	}
	if update.StateDigest != nextDigest {
		return permanentTurnCompletionf("driver state digest mismatch: got %s want %s", update.StateDigest, nextDigest)
	}

	var generationDriver string
	if err := tx.QueryRowContext(ctx, `
SELECT a.driver_id
FROM runtime_generations g
JOIN sessions s ON s.id = g.session_id
JOIN agent_runtime_profiles a ON a.agent_runtime_profile_id = g.agent_runtime_profile_id
WHERE g.session_id = ?
  AND g.generation_id = ?
  AND g.lease_owner = ?
  AND g.lease_expires_at > ?
  AND s.active_generation_id = ?`,
		p.SessionID, p.GenerationID, p.Owner, formatTime(p.Now), p.GenerationID).Scan(&generationDriver); err != nil {
		return fmt.Errorf("driver state generation lease check: %w", err)
	}
	if generationDriver != string(driverID) {
		return permanentTurnCompletionf("driver state update driver mismatch: got %s want %s", driverID, generationDriver)
	}
	var turnCount int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns
WHERE id = ?
  AND session_id = ?
  AND generation_id = ?
  AND lease_owner = ?`,
		p.TurnID, p.SessionID, p.GenerationID, p.Owner).Scan(&turnCount); err != nil {
		return err
	}
	if turnCount != 1 {
		return permanentTurnCompletionf("driver state update turn does not belong to generation")
	}
	if codec, ok := driverStateCodecFor(string(driverID)); ok && codec.validateCompletedUpdateAgainstHostTx != nil {
		if err := codec.validateCompletedUpdateAgainstHostTx(ctx, tx, p, canonical); err != nil {
			return err
		}
	}

	current, err := getDriverStateTx(ctx, tx, p.SessionID, string(driverID))
	if err != nil {
		return err
	}
	if current.StateDigest != strings.TrimSpace(update.PreviousStateDigest) {
		return driverStateReplayOrStale(ctx, tx, p, current, canonical, update)
	}
	if update.StateVersion != current.StateVersion+1 {
		return permanentTurnCompletionf("driver state version = %d, want %d", update.StateVersion, current.StateVersion+1)
	}
	res, err := tx.ExecContext(ctx, `
UPDATE session_driver_states
SET state_payload = ?,
    state_digest = ?,
    state_version = ?,
    updated_generation_id = ?,
    updated_turn_id = ?,
    updated_at = ?
WHERE session_id = ?
  AND driver_id = ?
  AND state_digest = ?
  AND state_version = ?`,
		string(canonical), update.StateDigest, update.StateVersion, p.GenerationID, p.TurnID, formatTime(p.Now),
		p.SessionID, string(driverID), update.PreviousStateDigest, current.StateVersion)
	if err != nil {
		return err
	}
	return permanentRequireOneRow(res, "driver state CAS failed")
}

func ValidateDriverStatePayloadForRuntimeLaunch(driverID string, canonicalPayload []byte, agentHomeHostPath string) error {
	codec, ok := driverStateCodecFor(driverID)
	if !ok {
		return fmt.Errorf("unsupported driver %q", driverID)
	}
	if codec.validateHostPayload == nil {
		return nil
	}
	if len(canonicalPayload) == 0 {
		return fmt.Errorf("%s runtime launch requires driver state payload", codec.driverID)
	}
	if err := codec.validateHostPayload(canonicalPayload, agentHomeHostPath, ""); err != nil {
		return fmt.Errorf("%s runtime launch driver state validation: %w", codec.driverID, err)
	}
	return nil
}

func ValidateDriverStatePayloadForHost(driverID string, canonicalPayload []byte, agentHomeHostPath, expectedCompletedTurnID string) error {
	codec, ok := driverStateCodecFor(driverID)
	if !ok {
		return fmt.Errorf("unsupported driver %q", driverID)
	}
	if codec.validateHostPayload == nil {
		return nil
	}
	return codec.validateHostPayload(canonicalPayload, agentHomeHostPath, expectedCompletedTurnID)
}

func synthesizeCompletedDriverStateUpdateTx(ctx context.Context, tx *sql.Tx, p CompleteTurnParams) error {
	if p.TerminalStatus != "completed" {
		return nil
	}
	var driverID string
	if err := tx.QueryRowContext(ctx, `
SELECT a.driver_id
FROM runtime_generations g
JOIN sessions s ON s.id = g.session_id
JOIN agent_runtime_profiles a ON a.agent_runtime_profile_id = g.agent_runtime_profile_id
WHERE g.session_id = ?
  AND g.generation_id = ?
  AND g.lease_owner = ?
  AND g.lease_expires_at > ?
  AND s.active_generation_id = ?`,
		p.SessionID, p.GenerationID, p.Owner, formatTime(p.Now), p.GenerationID).Scan(&driverID); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return fmt.Errorf("driver state generation lease check: %w", err)
	}
	codec, ok := driverStateCodecFor(driverID)
	if !ok || codec.synthesizeCompletedUpdateTx == nil {
		return nil
	}
	current, err := getDriverStateTx(ctx, tx, p.SessionID, driverID)
	if err != nil {
		return err
	}
	return codec.synthesizeCompletedUpdateTx(ctx, tx, p, current)
}

func synthesizeClaudeCompletedDriverStateUpdateTx(ctx context.Context, tx *sql.Tx, p CompleteTurnParams, current driverStateRow) error {
	var payload struct {
		SchemaVersion       int     `json:"schema_version"`
		DriverID            string  `json:"driver_id"`
		StateKind           string  `json:"state_kind"`
		ClaudeSessionUUID   string  `json:"claude_session_uuid"`
		Initialized         bool    `json:"initialized"`
		LastCompletedTurnID *string `json:"last_completed_turn_id"`
	}
	if err := json.Unmarshal(current.StatePayload, &payload); err != nil {
		return err
	}
	if payload.DriverID != string(agents.ClaudeCode) || payload.StateKind != claudeDriverStateKind {
		return permanentTurnCompletionf("cannot synthesize claude driver state from %s/%s", payload.DriverID, payload.StateKind)
	}
	turnID := fmt.Sprint(p.TurnID)
	if payload.Initialized && payload.LastCompletedTurnID != nil && *payload.LastCompletedTurnID == turnID {
		return nil
	}
	nextPayload, nextDigest, err := canonicalDriverStatePayload(map[string]any{
		"schema_version":         1,
		"driver_id":              string(agents.ClaudeCode),
		"state_kind":             claudeDriverStateKind,
		"claude_session_uuid":    payload.ClaudeSessionUUID,
		"initialized":            true,
		"last_completed_turn_id": turnID,
	}, string(agents.ClaudeCode))
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
UPDATE session_driver_states
SET state_payload = ?,
    state_digest = ?,
    state_version = ?,
    updated_generation_id = ?,
    updated_turn_id = ?,
    updated_at = ?
WHERE session_id = ?
  AND driver_id = ?
  AND state_digest = ?
  AND state_version = ?`,
		string(nextPayload), nextDigest, current.StateVersion+1, p.GenerationID, p.TurnID, formatTime(p.Now),
		p.SessionID, current.DriverID, current.StateDigest, current.StateVersion)
	if err != nil {
		return err
	}
	return permanentRequireOneRow(res, "driver state CAS failed")
}

func driverStateReplayOrStale(ctx context.Context, tx *sql.Tx, p CompleteTurnParams, current driverStateRow, canonical []byte, update DriverStateUpdate) error {
	if current.StateDigest == update.StateDigest &&
		current.StateVersion == update.StateVersion &&
		current.UpdatedGenerationID == p.GenerationID &&
		current.UpdatedTurnID.Valid &&
		current.UpdatedTurnID.Int64 == p.TurnID &&
		bytes.Equal(current.StatePayload, canonical) {
		var alreadyTerminal int
		if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns
WHERE id = ?
  AND session_id = ?
  AND generation_id = ?
  AND status = ?
  AND completed_by_generation = ?`,
			p.TurnID, p.SessionID, p.GenerationID, p.TerminalStatus, p.GenerationID).Scan(&alreadyTerminal); err != nil {
			return err
		}
		if alreadyTerminal == 1 {
			return nil
		}
	}
	return permanentTurnCompletionf("driver state stale digest: got %s want %s", update.PreviousStateDigest, current.StateDigest)
}
