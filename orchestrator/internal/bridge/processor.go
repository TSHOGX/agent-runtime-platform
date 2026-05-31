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
	BridgeProtocolEvidence(context.Context, string, string) (store.BridgeProtocolEvidence, error)
	BridgeHelloAck(context.Context, string, string, string, time.Time, time.Duration) (store.BridgeHelloAck, error)
	RenewGenerationHeartbeat(context.Context, store.RenewHeartbeatParams) error
	ClaimNextTurn(context.Context, store.ClaimNextTurnParams) (store.TurnGrant, bool, error)
	ResumeTurn(context.Context, store.ResumeTurnParams) (store.TurnGrant, bool, error)
	AckTurnStarted(context.Context, store.AckStartedParams) (int64, error)
	CompleteTurn(context.Context, store.CompleteTurnParams) (int64, error)
	FailGeneration(context.Context, store.FailGenerationParams) error
	AppendEvent(context.Context, store.AppendEventParams) (int64, error)
}

type Processor struct {
	Store                   Store
	Owner                   string
	LeaseTTL                time.Duration
	AckStartedGrace         time.Duration
	RequiredProtocolVersion int
	RequiredTurnInputSchema string
	Now                     func() time.Time
	ProbeHandler            func(context.Context, Envelope) error
	AfterCommit             func(context.Context, Envelope, int64)
	connected               map[string]bridgeState
}

var errProtocol = errors.New("bridge protocol error")

type bridgeState struct {
	helloSeen bool
	probed    bool
}

func (p *Processor) MarkReady(sessionID, generationID string) {
	key := stateKey(sessionID, generationID)
	p.setState(key, func(state bridgeState) bridgeState {
		state.helloSeen = true
		state.probed = true
		return state
	})
}

type grantPayload struct {
	TurnID          int64             `json:"turn_id"`
	Sequence        int64             `json:"sequence"`
	TurnInputSchema string            `json:"turn_input_schema"`
	Input           runTurnInput      `json:"input"`
	Attempt         int               `json:"attempt"`
	Replayed        bool              `json:"replayed"`
	ExpiresAt       time.Time         `json:"expires_at"`
	DriverState     *grantDriverState `json:"driver_state,omitempty"`
}

type grantDriverState struct {
	DriverID     string          `json:"driver_id"`
	StateDigest  string          `json:"state_digest"`
	StateVersion int             `json:"state_version"`
	StatePayload json.RawMessage `json:"state_payload,omitempty"`
}

type runTurnInput struct {
	Content string `json:"content"`
}

func (p *Processor) buildGrantPayload(grant store.TurnGrant) grantPayload {
	return grantPayload{
		TurnID:          grant.TurnID,
		Sequence:        grant.Sequence,
		TurnInputSchema: p.turnInputSchema(),
		Input:           runTurnInput{Content: grant.Content},
		Attempt:         grant.Attempt,
		Replayed:        grant.Replayed,
		ExpiresAt:       grant.ExpiresAt,
		DriverState:     grantDriverStatePayload(grant),
	}
}

func grantDriverStatePayload(grant store.TurnGrant) *grantDriverState {
	if strings.TrimSpace(grant.DriverState.DriverID) == "" {
		return nil
	}
	payload := &grantDriverState{
		DriverID:     grant.DriverState.DriverID,
		StateDigest:  grant.DriverState.StateDigest,
		StateVersion: grant.DriverState.StateVersion,
	}
	if len(grant.DriverStatePayload) > 0 {
		payload.StatePayload = append(json.RawMessage(nil), grant.DriverStatePayload...)
	}
	return payload
}

type helloPayload struct {
	ProtocolVersion int    `json:"protocol_version"`
	DriverID        string `json:"driver_id"`
	TurnInputSchema string `json:"turn_input_schema"`
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

type nativeEventPayload struct {
	Schema string `json:"schema"`
	Event  struct {
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload,omitempty"`
	} `json:"event"`
}

type ackCompletedPayload struct {
	Status            string                   `json:"status"`
	ErrorClass        string                   `json:"error_class,omitempty"`
	Error             string                   `json:"error,omitempty"`
	DriverStateUpdate *store.DriverStateUpdate `json:"driver_state_update,omitempty"`
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
		if _, _, err := p.validateHello(ctx, envelope); err != nil {
			return err
		}
		ack, err := p.Store.BridgeHelloAck(ctx, envelope.SessionID, envelope.GenerationID, p.Owner, now, p.AckStartedGrace)
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
		return p.writeResponse(ctx, inbox, envelope, TypeGrant, p.buildGrantPayload(grant))
	case TypeResumeTurn:
		state := p.state(key)
		if !state.helloSeen || !state.probed {
			return p.writeResponse(ctx, inbox, envelope, TypeNoWork, map[string]string{"reason": "bridge_not_ready"})
		}
		if envelope.TurnID == nil {
			return protocolErrorf("resume_turn requires turn_id")
		}
		grant, ok, err := p.Store.ResumeTurn(ctx, store.ResumeTurnParams{
			SessionID:       envelope.SessionID,
			GenerationID:    envelope.GenerationID,
			TurnID:          *envelope.TurnID,
			Owner:           p.Owner,
			LeaseTTL:        p.leaseTTL(),
			AckStartedGrace: p.AckStartedGrace,
			Now:             now,
		})
		if err != nil {
			return err
		}
		if !ok {
			return p.writeResponse(ctx, inbox, envelope, TypeNoWork, map[string]string{"reason": "no_resumable_turn"})
		}
		grant.Replayed = true
		return p.writeResponse(ctx, inbox, envelope, TypeGrant, p.buildGrantPayload(grant))
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
		eventID, err := p.Store.AckTurnStarted(ctx, store.AckStartedParams{
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
		})
		if err != nil {
			return err
		}
		if eventID != 0 {
			p.afterCommit(ctx, envelope, eventID)
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
		if err := validateEmitOutputPayload(payload); err != nil {
			return err
		}
		eventID, err := p.Store.AppendEvent(ctx, store.AppendEventParams{
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
		if err != nil && store.IsDuplicateOutputSequenceMismatch(err) {
			if failErr := p.failGeneration(ctx, envelope, "bridge_output_sequence_mismatch", err.Error(), now); failErr != nil {
				return fmt.Errorf("%w; additionally failed to retire generation: %v", err, failErr)
			}
			return protocolError(err)
		}
		if err == nil && eventID != 0 {
			p.afterCommit(ctx, envelope, eventID)
		}
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
		eventID, err := p.Store.CompleteTurn(ctx, store.CompleteTurnParams{
			SessionID:         envelope.SessionID,
			GenerationID:      envelope.GenerationID,
			TurnID:            *envelope.TurnID,
			Owner:             p.Owner,
			TerminalStatus:    payload.Status,
			ErrorClass:        payload.ErrorClass,
			Error:             payload.Error,
			DriverStateUpdate: payload.DriverStateUpdate,
			EventType:         TypeAckTurnCompleted,
			EventDedupeKey:    fmt.Sprintf("ack_completed:%s:%d", envelope.GenerationID, *envelope.TurnID),
			EventPayload:      payload,
			Now:               now,
		})
		if err != nil {
			if !store.IsPermanentTurnCompletion(err) {
				// Transient/infra failure (e.g. database locked, BeginTx /
				// Commit failure, lease-check query error). Propagate so
				// ProcessOnce retains the outbox envelope and retries instead
				// of permanently failing the generation.
				return err
			}
			if failErr := p.failGeneration(ctx, envelope, completionFailureClass(err), err.Error(), now); failErr != nil {
				return fmt.Errorf("%w; additionally failed to retire generation: %v", err, failErr)
			}
			return nil
		}
		if eventID != 0 {
			p.afterCommit(ctx, envelope, eventID)
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

func (p *Processor) validateHello(ctx context.Context, envelope Envelope) (helloPayload, store.BridgeProtocolEvidence, error) {
	return ValidateHelloPayload(ctx, p.Store, envelope, p.protocolVersion(), p.turnInputSchema())
}

type ProtocolEvidenceStore interface {
	BridgeProtocolEvidence(context.Context, string, string) (store.BridgeProtocolEvidence, error)
}

func ValidateHelloPayload(ctx context.Context, st ProtocolEvidenceStore, envelope Envelope, requiredProtocolVersion int, requiredTurnInputSchema string) (helloPayload, store.BridgeProtocolEvidence, error) {
	if requiredProtocolVersion <= 0 {
		requiredProtocolVersion = 2
	}
	requiredTurnInputSchema = strings.TrimSpace(requiredTurnInputSchema)
	if requiredTurnInputSchema == "" {
		requiredTurnInputSchema = "RunTurn"
	}
	var payload helloPayload
	if len(envelope.Payload) == 0 {
		return helloPayload{}, store.BridgeProtocolEvidence{}, protocolErrorf("hello requires protocol_version, driver_id, and turn_input_schema")
	}
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		return helloPayload{}, store.BridgeProtocolEvidence{}, protocolError(err)
	}
	payload.DriverID = strings.TrimSpace(payload.DriverID)
	payload.TurnInputSchema = strings.TrimSpace(payload.TurnInputSchema)
	if payload.ProtocolVersion != requiredProtocolVersion {
		return helloPayload{}, store.BridgeProtocolEvidence{}, protocolErrorf("unsupported bridge protocol_version %d", payload.ProtocolVersion)
	}
	if payload.DriverID == "" {
		return helloPayload{}, store.BridgeProtocolEvidence{}, protocolErrorf("hello requires driver_id")
	}
	if payload.TurnInputSchema != requiredTurnInputSchema {
		return helloPayload{}, store.BridgeProtocolEvidence{}, protocolErrorf("unsupported turn_input_schema %q", payload.TurnInputSchema)
	}
	evidence, err := st.BridgeProtocolEvidence(ctx, envelope.SessionID, envelope.GenerationID)
	if err != nil {
		return helloPayload{}, store.BridgeProtocolEvidence{}, err
	}
	if payload.DriverID != evidence.DriverID {
		return helloPayload{}, store.BridgeProtocolEvidence{}, protocolErrorf("hello driver_id %q does not match generation driver %q", payload.DriverID, evidence.DriverID)
	}
	if evidence.ProtocolVersion != requiredProtocolVersion {
		return helloPayload{}, store.BridgeProtocolEvidence{}, protocolErrorf("generation manifest bridge protocol_version %d is incompatible", evidence.ProtocolVersion)
	}
	if evidence.TurnInputSchema != requiredTurnInputSchema {
		return helloPayload{}, store.BridgeProtocolEvidence{}, protocolErrorf("generation manifest turn_input_schema %q is incompatible", evidence.TurnInputSchema)
	}
	return payload, evidence, nil
}

func (p *Processor) afterCommit(ctx context.Context, envelope Envelope, eventID int64) {
	if p.AfterCommit != nil {
		p.AfterCommit(ctx, envelope, eventID)
	}
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

func validateEmitOutputPayload(payload emitOutputPayload) error {
	if payload.OutputSequence <= 0 {
		return protocolErrorf("emit_output requires positive output_sequence")
	}
	if len(payload.Payload) == 0 {
		return nil
	}
	var probe struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(payload.Payload, &probe); err != nil {
		return protocolError(err)
	}
	if strings.TrimSpace(probe.Schema) == "" {
		return nil
	}
	if probe.Schema != "harness_native_events_v1" {
		return protocolErrorf("unsupported native event schema %q", probe.Schema)
	}
	var native nativeEventPayload
	if err := json.Unmarshal(payload.Payload, &native); err != nil {
		return protocolError(err)
	}
	switch native.Event.Type {
	case "agent.delta", "agent.message", "agent.output", "system.status":
		return nil
	case "":
		return protocolErrorf("native event payload requires event.type")
	default:
		return protocolErrorf("unsupported native event type %q", native.Event.Type)
	}
}

func (p *Processor) failGeneration(ctx context.Context, envelope Envelope, errorClass, reason string, now time.Time) error {
	turnID := int64(0)
	if envelope.TurnID != nil {
		turnID = *envelope.TurnID
	}
	return p.Store.FailGeneration(ctx, store.FailGenerationParams{
		SessionID:    envelope.SessionID,
		GenerationID: envelope.GenerationID,
		TurnID:       turnID,
		Owner:        p.Owner,
		ErrorClass:   errorClass,
		Reason:       reason,
		Now:          now,
	})
}

func completionFailureClass(err error) string {
	if err == nil {
		return "turn_completion_commit_failed"
	}
	message := err.Error()
	switch {
	case strings.Contains(message, "CAS"):
		return "driver_state_cas_failed"
	case strings.Contains(message, "driver state"):
		return "driver_state_validation_failed"
	default:
		return "turn_completion_commit_failed"
	}
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

func (p *Processor) protocolVersion() int {
	if p.RequiredProtocolVersion > 0 {
		return p.RequiredProtocolVersion
	}
	return 2
}

func (p *Processor) turnInputSchema() string {
	if strings.TrimSpace(p.RequiredTurnInputSchema) != "" {
		return strings.TrimSpace(p.RequiredTurnInputSchema)
	}
	return "RunTurn"
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
