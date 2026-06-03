package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type BridgeProtocolEvidence struct {
	DriverID              string
	ProtocolVersion       int
	TurnInputSchema       string
	AgentManifestDigest   string
	RuntimeConfigDigest   string
	ContractGateVersion   string
	SandboxContractDigest string
}

type BridgeHelloAck struct {
	LastOutputSequenceByTurn map[int64]int64
	LeasedTurnID             *int64
	ServerTime               time.Time
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
