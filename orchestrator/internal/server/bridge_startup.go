package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/store"
)

type bridgeStartupProbeState struct {
	heartbeatSeen bool
	helloSeen     bool
	probeSeen     bool
	heartbeatSeq  uint64
	helloSeq      uint64
	probeSeq      uint64
}

type bridgeHelloAckPayload struct {
	LastOutputSequenceByTurn map[string]int64 `json:"last_output_sequence_by_turn"`
	LeasedTurnID             *int64           `json:"leased_turn_id,omitempty"`
	ServerTime               time.Time        `json:"server_time"`
}

func (s *Server) waitForBridgeStartupReadiness(ctx context.Context, allocation store.GenerationAllocation, instance store.RuntimeResourceInstance) (string, error) {
	attempts := s.cfg.Harness.Probe.PostStartAttempts
	if attempts <= 0 {
		attempts = 5
	}
	interval := s.cfg.Harness.Probe.PostStartInterval.Duration
	if interval <= 0 {
		interval = time.Second
	}
	inbox, err := bridge.OpenQueue(instance.BridgeDirPath, bridge.InboxDir)
	if err != nil {
		return "", fmt.Errorf("bridge startup probe open inbox: %w", err)
	}
	outbox, err := bridge.OpenQueue(instance.BridgeDirPath, bridge.OutboxDir)
	if err != nil {
		return "", fmt.Errorf("bridge startup probe open outbox: %w", err)
	}
	state := bridgeStartupProbeState{}
	for attempt := 1; attempt <= attempts; attempt++ {
		ready, err := s.processBridgeStartupBatch(ctx, inbox, outbox, allocation.Owner, instance, &state)
		if err != nil {
			return "", err
		}
		if ready {
			return state.evidence(), nil
		}
		if attempt == attempts {
			break
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", ctx.Err()
		case <-timer.C:
		}
	}
	return "", fmt.Errorf("bridge startup probe did not complete: missing %s", state.missing())
}

func (s *Server) processBridgeStartupBatch(ctx context.Context, inbox, outbox bridge.Queue, owner string, instance store.RuntimeResourceInstance, state *bridgeStartupProbeState) (bool, error) {
	files, err := outbox.ReadAll()
	if err != nil {
		return false, fmt.Errorf("bridge startup probe read outbox: %w", err)
	}
	for _, file := range files {
		if state.ready() {
			return true, nil
		}
		envelope := file.Envelope
		if err := validateBridgeStartupEnvelope(envelope, instance); err != nil {
			return false, err
		}
		switch envelope.Type {
		case bridge.TypeHeartbeat:
			state.heartbeatSeen = true
			if state.heartbeatSeq == 0 {
				state.heartbeatSeq = file.Seq
			}
		case bridge.TypeHello:
			if _, _, err := bridge.ValidateHelloPayload(ctx, bridgeStore(s.store), envelope, bridge.RequiredProtocolVersionV2, bridge.RequiredTurnInputRunTurn); err != nil {
				return false, fmt.Errorf("bridge startup probe hello validation failed: %w", err)
			}
			ack, err := s.store.BridgeHelloAck(ctx, envelope.SessionID, envelope.GenerationID, owner, time.Now().UTC(), 0)
			if err != nil {
				return false, fmt.Errorf("bridge startup probe hello failed: %w", err)
			}
			if err := writeBridgeStartupResponse(ctx, inbox, envelope, bridge.TypeHelloAck, bridgeHelloAckPayload{
				LastOutputSequenceByTurn: bridgeHelloLastSequences(ack.LastOutputSequenceByTurn),
				LeasedTurnID:             ack.LeasedTurnID,
				ServerTime:               ack.ServerTime,
			}); err != nil {
				return false, fmt.Errorf("bridge startup probe hello response: %w", err)
			}
			state.helloSeen = true
			if state.helloSeq == 0 {
				state.helloSeq = file.Seq
			}
		case bridge.TypeProbeNetwork:
			if !state.helloSeen {
				return false, fmt.Errorf("bridge startup probe received probe_network before hello")
			}
			if err := writeBridgeStartupResponse(ctx, inbox, envelope, bridge.TypeNoWork, map[string]string{"status": "probe_ok"}); err != nil {
				return false, fmt.Errorf("bridge startup probe response: %w", err)
			}
			state.probeSeen = true
			if state.probeSeq == 0 {
				state.probeSeq = file.Seq
			}
		case bridge.TypeClaimNextTurn, bridge.TypeResumeTurn, bridge.TypeAckTurnStarted, bridge.TypeEmitOutput, bridge.TypeAckTurnCompleted:
			return false, fmt.Errorf("bridge startup probe received %s before ready -> live", envelope.Type)
		default:
			return false, fmt.Errorf("bridge startup probe received unsupported message type %q", envelope.Type)
		}
		if err := file.Unlink(); err != nil {
			return false, fmt.Errorf("bridge startup probe unlink %s: %w", envelope.Type, err)
		}
		if state.ready() {
			return true, nil
		}
	}
	return state.ready(), nil
}

func validateBridgeStartupEnvelope(envelope bridge.Envelope, instance store.RuntimeResourceInstance) error {
	if strings.TrimSpace(envelope.MessageID) == "" {
		return fmt.Errorf("bridge startup probe envelope missing message_id")
	}
	if envelope.SessionID != instance.SessionID || envelope.GenerationID != instance.GenerationID {
		return fmt.Errorf("bridge startup probe identity mismatch: session=%s generation=%s, want session=%s generation=%s",
			envelope.SessionID, envelope.GenerationID, instance.SessionID, instance.GenerationID)
	}
	return nil
}

func writeBridgeStartupResponse(ctx context.Context, inbox bridge.Queue, request bridge.Envelope, responseType string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = inbox.Write(ctx, bridge.Envelope{
		RequestID:    bridgeRequestID(request),
		Type:         responseType,
		SessionID:    request.SessionID,
		GenerationID: request.GenerationID,
		TurnID:       request.TurnID,
		Payload:      raw,
	})
	return err
}

func bridgeRequestID(envelope bridge.Envelope) string {
	if strings.TrimSpace(envelope.RequestID) != "" {
		return envelope.RequestID
	}
	return envelope.MessageID
}

func bridgeHelloLastSequences(values map[int64]int64) map[string]int64 {
	out := make(map[string]int64, len(values))
	for turnID, sequence := range values {
		out[fmt.Sprint(turnID)] = sequence
	}
	return out
}

func (s bridgeStartupProbeState) ready() bool {
	return s.heartbeatSeen && s.helloSeen && s.probeSeen
}

func (s bridgeStartupProbeState) missing() string {
	missing := []string{}
	if !s.heartbeatSeen {
		missing = append(missing, "heartbeat")
	}
	if !s.helloSeen {
		missing = append(missing, "hello")
	}
	if !s.probeSeen {
		missing = append(missing, "probe_network")
	}
	return strings.Join(missing, ",")
}

func (s bridgeStartupProbeState) evidence() string {
	return fmt.Sprintf("bridge_startup_probe:passed; check=bridge_bootstrap; heartbeat_seq=%d; hello_seq=%d; probe_network_seq=%d",
		s.heartbeatSeq, s.helloSeq, s.probeSeq)
}
