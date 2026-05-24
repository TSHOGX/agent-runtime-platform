package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/store"
)

type Store interface {
	BridgeHelloAck(context.Context, string, string, string, time.Time) (store.BridgeHelloAck, error)
	RenewGenerationHeartbeat(context.Context, store.RenewHeartbeatParams) error
	ClaimNextTurn(context.Context, store.ClaimNextTurnParams) (store.TurnGrant, bool, error)
	ResumeTurn(context.Context, store.ResumeTurnParams) (store.TurnGrant, bool, error)
	AckTurnStarted(context.Context, store.AckStartedParams) error
	CompleteTurn(context.Context, store.CompleteTurnParams) error
	AppendEvent(context.Context, store.AppendEventParams) (int64, error)
}

type Processor struct {
	Store        Store
	Owner        string
	LeaseTTL     time.Duration
	Now          func() time.Time
	ProbeHandler func(context.Context, Envelope) error
	connected    map[string]bridgeState
}

var errProtocol = errors.New("bridge protocol error")

type bridgeState struct {
	helloSeen bool
	probed    bool
}

type grantPayload struct {
	TurnID    int64     `json:"turn_id"`
	Sequence  int64     `json:"sequence"`
	Content   string    `json:"content"`
	Attempt   int       `json:"attempt"`
	Replayed  bool      `json:"replayed"`
	ExpiresAt time.Time `json:"expires_at"`
}

type helloAckPayload struct {
	LastOutputSequenceByTurn map[string]int64 `json:"last_output_sequence_by_turn"`
	LeasedTurnID             *int64           `json:"leased_turn_id,omitempty"`
	ServerTime               time.Time        `json:"server_time"`
}

type errorPayload struct {
	ErrorClass string `json:"error_class"`
	Error      string `json:"error"`
}

type emitOutputPayload struct {
	OutputSequence int64           `json:"output_sequence"`
	Stream         string          `json:"stream,omitempty"`
	Payload        json.RawMessage `json:"payload,omitempty"`
}

type ackCompletedPayload struct {
	Status     string `json:"status"`
	ErrorClass string `json:"error_class,omitempty"`
	Error      string `json:"error,omitempty"`
}

func (p *Processor) ProcessOnce(ctx context.Context, root string) error {
	if p.Store == nil {
		return fmt.Errorf("bridge processor store is required")
	}
	inbox, err := OpenQueue(root, InboxDir)
	if err != nil {
		return err
	}
	outbox, err := OpenQueue(root, OutboxDir)
	if err != nil {
		return err
	}
	files, err := outbox.ReadAll()
	if err != nil {
		return err
	}
	for _, file := range files {
		if err := p.handle(ctx, inbox, file.Envelope); err != nil {
			if !errors.Is(err, errProtocol) {
				return err
			}
			if writeErr := p.writeResponse(ctx, inbox, file.Envelope, TypeError, errorPayload{ErrorClass: "bridge_protocol_error", Error: err.Error()}); writeErr != nil {
				return writeErr
			}
		}
		if err := file.Unlink(); err != nil {
			return err
		}
	}
	return nil
}

func (p *Processor) handle(ctx context.Context, inbox Queue, envelope Envelope) error {
	if err := validateEnvelope(envelope); err != nil {
		return protocolError(err)
	}
	now := p.now()
	key := stateKey(envelope.SessionID, envelope.GenerationID)
	switch envelope.Type {
	case TypeHello:
		ack, err := p.Store.BridgeHelloAck(ctx, envelope.SessionID, envelope.GenerationID, p.Owner, now)
		if err != nil {
			return err
		}
		p.setState(key, func(state bridgeState) bridgeState {
			state.helloSeen = true
			return state
		})
		lastSequences := make(map[string]int64, len(ack.LastOutputSequenceByTurn))
		for turnID, sequence := range ack.LastOutputSequenceByTurn {
			lastSequences[fmt.Sprint(turnID)] = sequence
		}
		return p.writeResponse(ctx, inbox, envelope, TypeHelloAck, helloAckPayload{
			LastOutputSequenceByTurn: lastSequences,
			LeasedTurnID:             ack.LeasedTurnID,
			ServerTime:               ack.ServerTime,
		})
	case TypeHeartbeat:
		return p.Store.RenewGenerationHeartbeat(ctx, store.RenewHeartbeatParams{
			SessionID:    envelope.SessionID,
			GenerationID: envelope.GenerationID,
			Owner:        p.Owner,
			LeaseTTL:     p.leaseTTL(),
			Now:          now,
		})
	case TypeProbeNetwork:
		if !p.state(key).helloSeen {
			return protocolErrorf("bridge must send hello before probe_network")
		}
		if p.ProbeHandler != nil {
			if err := p.ProbeHandler(ctx, envelope); err != nil {
				return err
			}
		}
		p.setState(key, func(state bridgeState) bridgeState {
			state.probed = true
			return state
		})
		return p.writeResponse(ctx, inbox, envelope, TypeNoWork, map[string]string{"status": "probe_ok"})
	case TypeClaimNextTurn:
		state := p.state(key)
		if !state.helloSeen || !state.probed {
			return p.writeResponse(ctx, inbox, envelope, TypeNoWork, map[string]string{"reason": "bridge_not_ready"})
		}
		grant, ok, err := p.Store.ClaimNextTurn(ctx, store.ClaimNextTurnParams{
			SessionID:    envelope.SessionID,
			GenerationID: envelope.GenerationID,
			Owner:        p.Owner,
			RequestID:    requestID(envelope),
			LeaseTTL:     p.leaseTTL(),
			Now:          now,
		})
		if err != nil {
			return err
		}
		if !ok {
			return p.writeResponse(ctx, inbox, envelope, TypeNoWork, map[string]string{"reason": "no_eligible_turn"})
		}
		return p.writeResponse(ctx, inbox, envelope, TypeGrant, grantPayload{
			TurnID:    grant.TurnID,
			Sequence:  grant.Sequence,
			Content:   grant.Content,
			Attempt:   grant.Attempt,
			Replayed:  grant.Replayed,
			ExpiresAt: grant.ExpiresAt,
		})
	case TypeResumeTurn:
		state := p.state(key)
		if !state.helloSeen || !state.probed {
			return p.writeResponse(ctx, inbox, envelope, TypeNoWork, map[string]string{"reason": "bridge_not_ready"})
		}
		if envelope.TurnID == nil {
			return protocolErrorf("resume_turn requires turn_id")
		}
		grant, ok, err := p.Store.ResumeTurn(ctx, store.ResumeTurnParams{
			SessionID:    envelope.SessionID,
			GenerationID: envelope.GenerationID,
			TurnID:       *envelope.TurnID,
			Owner:        p.Owner,
			LeaseTTL:     p.leaseTTL(),
			Now:          now,
		})
		if err != nil {
			return err
		}
		if !ok {
			return p.writeResponse(ctx, inbox, envelope, TypeNoWork, map[string]string{"reason": "no_resumable_turn"})
		}
		grant.Replayed = true
		return p.writeResponse(ctx, inbox, envelope, TypeGrant, grantPayload{
			TurnID:    grant.TurnID,
			Sequence:  grant.Sequence,
			Content:   grant.Content,
			Attempt:   grant.Attempt,
			Replayed:  grant.Replayed,
			ExpiresAt: grant.ExpiresAt,
		})
	case TypeAckTurnStarted:
		if envelope.TurnID == nil {
			return protocolErrorf("ack_turn_started requires turn_id")
		}
		sandboxSourceIP := ""
		if len(envelope.Payload) > 0 {
			var payload struct {
				SandboxSourceIP string `json:"sandbox_source_ip"`
			}
			if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
				return protocolError(err)
			}
			sandboxSourceIP = payload.SandboxSourceIP
		}
		if strings.TrimSpace(sandboxSourceIP) == "" {
			return protocolErrorf("ack_turn_started requires sandbox_source_ip")
		}
		if err := p.Store.AckTurnStarted(ctx, store.AckStartedParams{
			SessionID:       envelope.SessionID,
			GenerationID:    envelope.GenerationID,
			TurnID:          *envelope.TurnID,
			Owner:           p.Owner,
			SandboxSourceIP: sandboxSourceIP,
			LeaseTTL:        p.leaseTTL(),
			EventType:       TypeAckTurnStarted,
			EventDedupeKey:  fmt.Sprintf("ack_started:%s:%d", envelope.GenerationID, *envelope.TurnID),
			EventPayload:    jsonPayload(envelope.Payload),
			Now:             now,
		}); err != nil {
			return err
		}
		return nil
	case TypeEmitOutput:
		if envelope.TurnID == nil {
			return protocolErrorf("emit_output requires turn_id")
		}
		var payload emitOutputPayload
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return protocolError(err)
		}
		if payload.OutputSequence <= 0 {
			return protocolErrorf("emit_output requires positive output_sequence")
		}
		_, err := p.Store.AppendEvent(ctx, store.AppendEventParams{
			SessionID:      envelope.SessionID,
			TurnID:         envelope.TurnID,
			GenerationID:   envelope.GenerationID,
			Owner:          p.Owner,
			OutputSequence: &payload.OutputSequence,
			Stream:         payload.Stream,
			Type:           TypeEmitOutput,
			Payload:        jsonPayload(payload.Payload),
			Now:            now,
		})
		return err
	case TypeAckTurnCompleted:
		if envelope.TurnID == nil {
			return protocolErrorf("ack_turn_completed requires turn_id")
		}
		var payload ackCompletedPayload
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return protocolError(err)
		}
		if payload.Status == "" {
			payload.Status = "completed"
		}
		if err := p.Store.CompleteTurn(ctx, store.CompleteTurnParams{
			SessionID:      envelope.SessionID,
			GenerationID:   envelope.GenerationID,
			TurnID:         *envelope.TurnID,
			Owner:          p.Owner,
			TerminalStatus: payload.Status,
			ErrorClass:     payload.ErrorClass,
			Error:          payload.Error,
			EventType:      TypeAckTurnCompleted,
			EventDedupeKey: fmt.Sprintf("ack_completed:%s:%d", envelope.GenerationID, *envelope.TurnID),
			EventPayload:   payload,
			Now:            now,
		}); err != nil {
			return err
		}
		return nil
	default:
		return protocolErrorf("unsupported bridge message type %q", envelope.Type)
	}
}

func (p *Processor) writeResponse(ctx context.Context, inbox Queue, request Envelope, responseType string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = inbox.Write(ctx, Envelope{
		RequestID:    requestID(request),
		Type:         responseType,
		SessionID:    request.SessionID,
		GenerationID: request.GenerationID,
		TurnID:       request.TurnID,
		Payload:      raw,
	})
	return err
}

func validateEnvelope(envelope Envelope) error {
	if strings.TrimSpace(envelope.MessageID) == "" {
		return fmt.Errorf("bridge envelope missing message_id")
	}
	if strings.TrimSpace(envelope.Type) == "" ||
		strings.TrimSpace(envelope.SessionID) == "" ||
		strings.TrimSpace(envelope.GenerationID) == "" {
		return fmt.Errorf("bridge envelope missing required identity")
	}
	return nil
}

func protocolError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", errProtocol, err)
}

func protocolErrorf(format string, args ...any) error {
	return protocolError(fmt.Errorf(format, args...))
}

func (p *Processor) now() time.Time {
	if p.Now != nil {
		return p.Now().UTC()
	}
	return time.Now().UTC()
}

func (p *Processor) leaseTTL() time.Duration {
	if p.LeaseTTL > 0 {
		return p.LeaseTTL
	}
	return time.Minute
}

func (p *Processor) state(key string) bridgeState {
	if p.connected == nil {
		return bridgeState{}
	}
	return p.connected[key]
}

func (p *Processor) setState(key string, update func(bridgeState) bridgeState) {
	if p.connected == nil {
		p.connected = map[string]bridgeState{}
	}
	p.connected[key] = update(p.connected[key])
}

func stateKey(sessionID, generationID string) string {
	return sessionID + "\x00" + generationID
}

func requestID(envelope Envelope) string {
	if strings.TrimSpace(envelope.RequestID) != "" {
		return envelope.RequestID
	}
	return envelope.MessageID
}

func jsonPayload(raw json.RawMessage) any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return map[string]string{"raw": string(raw)}
	}
	return value
}
