package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
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
	piDriverStateKindUninitialized     = "pi_uninitialized"
	piDriverStateKindSession           = "pi_session"
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

func canonicalBootstrapDriverState(driverID, claudeSessionUUID string) ([]byte, string, error) {
	switch agents.ID(strings.TrimSpace(driverID)) {
	case agents.ClaudeCode:
		if strings.TrimSpace(claudeSessionUUID) == "" {
			return nil, "", fmt.Errorf("claude session uuid is required")
		}
		return canonicalDriverStatePayload(map[string]any{
			"schema_version":         1,
			"driver_id":              string(agents.ClaudeCode),
			"state_kind":             claudeDriverStateKind,
			"claude_session_uuid":    strings.TrimSpace(claudeSessionUUID),
			"initialized":            false,
			"last_completed_turn_id": nil,
		}, string(agents.ClaudeCode))
	case agents.Shell:
		return canonicalDriverStatePayload(map[string]any{
			"schema_version": 1,
			"driver_id":      string(agents.Shell),
			"state_kind":     emptyDriverStateKind,
		}, string(agents.Shell))
	case agents.Pi:
		return canonicalDriverStatePayload(map[string]any{
			"schema_version": 1,
			"driver_id":      string(agents.Pi),
			"state_kind":     piDriverStateKindUninitialized,
			"session_dir":    agents.PiSessionDir,
		}, string(agents.Pi))
	default:
		return nil, "", fmt.Errorf("unsupported driver %q", driverID)
	}
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
	switch agents.ID(strings.TrimSpace(driverID)) {
	case agents.ClaudeCode:
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
	case agents.Shell:
		if got, _ := object["state_kind"].(string); got != emptyDriverStateKind {
			return fmt.Errorf("shell driver state_kind = %q", got)
		}
	case agents.Pi:
		if err := validatePiDriverStatePayload(object); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported driver %q", driverID)
	}
	return nil
}

func validatePiDriverStatePayload(object map[string]any) error {
	if sessionDir, _ := object["session_dir"].(string); sessionDir != agents.PiSessionDir {
		return fmt.Errorf("pi driver state session_dir = %q", sessionDir)
	}
	switch got, _ := object["state_kind"].(string); got {
	case piDriverStateKindUninitialized:
		return nil
	case piDriverStateKindSession:
		rel := strings.TrimSpace(stringValue(object["selected_session_relpath"]))
		if rel == "" {
			return fmt.Errorf("pi driver state selected_session_relpath is required")
		}
		if strings.HasPrefix(rel, "/") || strings.Contains(rel, "\\") {
			return fmt.Errorf("pi driver state selected_session_relpath must be relative")
		}
		if rel != strings.TrimPrefix(rel, "./") || rel != strings.TrimSpace(rel) {
			return fmt.Errorf("pi driver state selected_session_relpath must be clean")
		}
		parts := strings.Split(rel, "/")
		for _, part := range parts {
			if part == "" || part == "." || part == ".." {
				return fmt.Errorf("pi driver state selected_session_relpath must stay under session_dir")
			}
		}
		selectedFile, _ := object["selected_session_file"].(string)
		if selectedFile != agents.PiSessionDir+"/"+rel {
			return fmt.Errorf("pi driver state selected_session_file = %q, want %q", selectedFile, agents.PiSessionDir+"/"+rel)
		}
		if strings.TrimSpace(stringValue(object["selected_session_id"])) == "" {
			return fmt.Errorf("pi driver state selected_session_id is required")
		}
		if strings.TrimSpace(stringValue(object["last_completed_turn_id"])) == "" {
			return fmt.Errorf("pi driver state last_completed_turn_id is required")
		}
		return nil
	default:
		return fmt.Errorf("pi driver state_kind = %q", got)
	}
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

func ensureAllocationDriverStateTx(ctx context.Context, tx *sql.Tx, sessionID, generationID, driverID, claudeSessionUUID string, now time.Time) (DriverStateToken, error) {
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
	if agents.ID(driverID) == agents.ClaudeCode && strings.TrimSpace(claudeSessionUUID) == "" {
		claudeSessionUUID = "phase9a-" + strings.TrimSpace(sessionID)
	}
	payload, digest, err := canonicalBootstrapDriverState(driverID, claudeSessionUUID)
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
		return err
	}
	if p.TerminalStatus != "completed" {
		return fmt.Errorf("driver state update is only valid for completed turns")
	}
	canonical, nextDigest, err := canonicalDriverStatePayload(update.StatePayload, string(driverID))
	if err != nil {
		return err
	}
	if update.StateDigest != nextDigest {
		return fmt.Errorf("driver state digest mismatch: got %s want %s", update.StateDigest, nextDigest)
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
		return fmt.Errorf("driver state update driver mismatch: got %s want %s", driverID, generationDriver)
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
		return fmt.Errorf("driver state update turn does not belong to generation")
	}
	if driverID == agents.Pi {
		if err := validatePiDriverStateUpdateAgainstHostTx(ctx, tx, p, canonical); err != nil {
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
		return fmt.Errorf("driver state version = %d, want %d", update.StateVersion, current.StateVersion+1)
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
	return requireOneRow(res, "driver state CAS failed")
}

func validatePiDriverStateUpdateAgainstHostTx(ctx context.Context, tx *sql.Tx, p CompleteTurnParams, canonicalPayload []byte) error {
	record, err := getSandboxContractForGenerationWithMirrors(ctx, tx, p.SessionID, p.GenerationID)
	if err != nil {
		return fmt.Errorf("pi driver state contract validation: %w", err)
	}
	contract, err := decodeSandboxContractObject(record.CanonicalPayload)
	if err != nil {
		return err
	}
	agentHomeHostPath, err := piAgentHomeHostPathFromContract(contract)
	if err != nil {
		return err
	}
	return ValidatePiDriverStatePayloadForHost(canonicalPayload, agentHomeHostPath, fmt.Sprint(p.TurnID))
}

func piAgentHomeHostPathFromContract(contract map[string]any) (string, error) {
	dataVolumes, ok := contract["data_volumes"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("pi driver state validation requires data_volumes")
	}
	agentHome, ok := dataVolumes["agent_home"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("pi driver state validation requires agent_home data volume")
	}
	if driver, _ := agentHome["driver"].(string); driver != string(agents.Pi) {
		return "", fmt.Errorf("pi agent_home data volume driver = %q", driver)
	}
	if key, _ := agentHome["driver_home_key"].(string); key != string(agents.Pi) {
		return "", fmt.Errorf("pi agent_home data volume key = %q", key)
	}
	if destination, _ := agentHome["sandbox_destination"].(string); destination != "/agent-home" {
		return "", fmt.Errorf("pi agent_home sandbox destination = %q", destination)
	}
	hostPath := strings.TrimSpace(stringValue(agentHome["host_path"]))
	if hostPath == "" || !filepath.IsAbs(hostPath) {
		return "", fmt.Errorf("pi agent_home host_path is required")
	}
	return hostPath, nil
}

func ValidatePiDriverStatePayloadForHost(canonicalPayload []byte, agentHomeHostPath, expectedCompletedTurnID string) error {
	canonical, _, err := canonicalDriverStatePayload(canonicalPayload, string(agents.Pi))
	if err != nil {
		return err
	}
	if !bytes.Equal(canonical, canonicalPayload) {
		return fmt.Errorf("pi driver state payload is not canonical")
	}
	object, err := decodeSandboxContractObject(canonicalPayload)
	if err != nil {
		return err
	}
	stateKind, _ := object["state_kind"].(string)
	agentHomeHostPath = strings.TrimSpace(agentHomeHostPath)
	if agentHomeHostPath == "" || !filepath.IsAbs(agentHomeHostPath) {
		return fmt.Errorf("pi agent_home host path is required")
	}
	switch stateKind {
	case piDriverStateKindUninitialized:
		if strings.TrimSpace(expectedCompletedTurnID) != "" {
			return fmt.Errorf("pi completed turn must advance to pi_session state")
		}
		return nil
	case piDriverStateKindSession:
		if expected := strings.TrimSpace(expectedCompletedTurnID); expected != "" && strings.TrimSpace(stringValue(object["last_completed_turn_id"])) != expected {
			return fmt.Errorf("pi driver state last_completed_turn_id mismatch")
		}
		return validatePiSessionFileAgainstHost(agentHomeHostPath, object)
	default:
		return fmt.Errorf("pi driver state_kind = %q", stateKind)
	}
}

func validatePiSessionFileAgainstHost(agentHomeHostPath string, object map[string]any) error {
	rel := strings.TrimSpace(stringValue(object["selected_session_relpath"]))
	if rel == "" || strings.HasPrefix(rel, "/") || strings.Contains(rel, "\\") || path.Clean(rel) != rel {
		return fmt.Errorf("pi selected session relpath is invalid")
	}
	if strings.HasPrefix(rel, "../") || rel == ".." {
		return fmt.Errorf("pi selected session relpath escapes session dir")
	}
	selectedFile := strings.TrimSpace(stringValue(object["selected_session_file"]))
	if selectedFile != agents.PiSessionDir+"/"+rel {
		return fmt.Errorf("pi selected session file = %q, want %q", selectedFile, agents.PiSessionDir+"/"+rel)
	}
	hostSessionRoot := filepath.Join(agentHomeHostPath, ".pi", "agent", "sessions")
	hostCandidate := filepath.Join(hostSessionRoot, filepath.FromSlash(rel))
	info, err := os.Lstat(hostCandidate)
	if err != nil {
		return fmt.Errorf("pi selected session host file missing: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("pi selected session host file must not be a symlink")
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("pi selected session host file is not regular")
	}
	realAgentHome, err := filepath.EvalSymlinks(agentHomeHostPath)
	if err != nil {
		return fmt.Errorf("pi agent_home realpath failed: %w", err)
	}
	realRoot, err := filepath.EvalSymlinks(hostSessionRoot)
	if err != nil {
		return fmt.Errorf("pi session root realpath failed: %w", err)
	}
	rootRel, err := filepath.Rel(realAgentHome, realRoot)
	if err != nil {
		return fmt.Errorf("pi session root relative path failed: %w", err)
	}
	if filepath.ToSlash(rootRel) != ".pi/agent/sessions" {
		return fmt.Errorf("pi session root realpath escapes agent_home")
	}
	realCandidate, err := filepath.EvalSymlinks(hostCandidate)
	if err != nil {
		return fmt.Errorf("pi selected session realpath failed: %w", err)
	}
	realRel, err := filepath.Rel(realRoot, realCandidate)
	if err != nil {
		return fmt.Errorf("pi selected session relative path failed: %w", err)
	}
	if realRel == "." || strings.HasPrefix(realRel, ".."+string(filepath.Separator)) || realRel == ".." || filepath.IsAbs(realRel) {
		return fmt.Errorf("pi selected session realpath escapes session dir")
	}
	if filepath.ToSlash(realRel) != rel {
		return fmt.Errorf("pi selected session realpath = %q, want %q", filepath.ToSlash(realRel), rel)
	}
	return nil
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
	if driverID != string(agents.ClaudeCode) {
		return nil
	}
	current, err := getDriverStateTx(ctx, tx, p.SessionID, driverID)
	if err != nil {
		return err
	}
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
		return fmt.Errorf("cannot synthesize claude driver state from %s/%s", payload.DriverID, payload.StateKind)
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
		p.SessionID, driverID, current.StateDigest, current.StateVersion)
	if err != nil {
		return err
	}
	return requireOneRow(res, "driver state CAS failed")
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
	return fmt.Errorf("driver state stale digest: got %s want %s", update.PreviousStateDigest, current.StateDigest)
}
