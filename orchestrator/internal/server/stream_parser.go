package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/runtime"
)

// streamParser owns per-turn normalizer state. Driver-specific parsing is
// dispatched through outputNormalizers so adding a driver does not add another
// branch to the bridge output path.
type streamParser struct {
	srv        *Server
	sessionID  string
	driverID   string
	turnID     int64
	normalizer outputNormalizer
	// pending text chunks per assistant message id, flushed when we see the
	// matching "assistant" full message (or when the runtime exits).
	pending map[string]*strings.Builder
	done    chan struct{}
	once    sync.Once
	err     error
	last    string
}

type outputNormalizer interface {
	Handle(*streamParser, normalizerBridgeOutput)
}

type outputNormalizerFactory func() outputNormalizer

type normalizerBridgeOutput struct {
	Stream  string
	Payload json.RawMessage
}

type lineBridgePayload struct {
	Line string `json:"line"`
}

type nativeBridgeEventPayload struct {
	Schema string `json:"schema"`
	Event  struct {
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload,omitempty"`
	} `json:"event"`
}

type claudeOutputNormalizer struct{}
type shellOutputNormalizer struct{}
type nativeEventsOutputNormalizer struct{}

type claudeStreamDelta struct {
	Type        string `json:"type"`
	Text        string `json:"text,omitempty"`
	Thinking    string `json:"thinking,omitempty"`
	PartialJSON string `json:"partial_json,omitempty"`
}

type claudeStreamEvent struct {
	Type    string            `json:"type"`
	Index   int               `json:"index"`
	Delta   claudeStreamDelta `json:"delta"`
	Message struct {
		ID string `json:"id"`
	} `json:"message"`
}

func newStreamParser(srv *Server, sessionID, agent string) *streamParser {
	return &streamParser{
		srv:        srv,
		sessionID:  sessionID,
		driverID:   strings.TrimSpace(agent),
		normalizer: outputNormalizerForDriver(agent),
		pending:    map[string]*strings.Builder{},
		done:       make(chan struct{}),
	}
}

func (p *streamParser) Done() <-chan struct{} {
	return p.done
}

func (p *streamParser) Err() error {
	return p.err
}

func (p *streamParser) complete() {
	p.once.Do(func() { close(p.done) })
}

func (p *streamParser) handle(output runtime.Output) {
	payload, _ := json.Marshal(lineBridgePayload{Line: output.Line})
	p.handleBridgeOutput(normalizerBridgeOutput{Stream: output.Stream, Payload: payload})
}

func (p *streamParser) handleBridgeOutput(output normalizerBridgeOutput) {
	if output.Stream == "" {
		output.Stream = "stdout"
	}
	line, hasLine := output.line()
	if hasLine && strings.TrimSpace(line) == "" {
		return
	}
	if output.Stream == "runtime" {
		if hasLine {
			p.publish("system.status", runtime.Output{Stream: output.Stream, Line: line})
		}
		return
	}
	if output.Stream == "stderr" {
		if hasLine {
			p.publish("agent.output", runtime.Output{Stream: output.Stream, Line: line})
		}
		return
	}
	p.normalizer.Handle(p, output)
}

func (output normalizerBridgeOutput) line() (string, bool) {
	if len(output.Payload) == 0 {
		return "", false
	}
	var payload lineBridgePayload
	if err := json.Unmarshal(output.Payload, &payload); err != nil || payload.Line == "" {
		return "", false
	}
	return payload.Line, true
}

func outputNormalizerForDriver(driverID string) outputNormalizer {
	driverID = strings.TrimSpace(driverID)
	if driverID == agents.LegacyClaudeToken {
		driverID = string(agents.ClaudeCode)
	}
	factory := outputNormalizers[driverID]
	if factory == nil {
		factory = outputNormalizers[string(agents.ClaudeCode)]
	}
	return factory()
}

var outputNormalizers = map[string]outputNormalizerFactory{
	string(agents.ClaudeCode): func() outputNormalizer { return claudeOutputNormalizer{} },
	string(agents.Shell):      func() outputNormalizer { return shellOutputNormalizer{} },
	"native_events_probe":     func() outputNormalizer { return nativeEventsOutputNormalizer{} },
}

func (claudeOutputNormalizer) Handle(p *streamParser, output normalizerBridgeOutput) {
	rawLine, ok := output.line()
	if !ok {
		return
	}
	line := strings.TrimSpace(rawLine)
	if strings.HasPrefix(line, "{") {
		var event struct {
			Type     string          `json:"type"`
			Subtype  string          `json:"subtype,omitempty"`
			Event    json.RawMessage `json:"event,omitempty"`
			Message  json.RawMessage `json:"message,omitempty"`
			Result   string          `json:"result,omitempty"`
			Stream   string          `json:"stream,omitempty"`
			Text     string          `json:"text,omitempty"`
			Line     string          `json:"line,omitempty"`
			ExitCode int             `json:"exit_code,omitempty"`
		}
		if err := json.Unmarshal([]byte(line), &event); err == nil {
			switch event.Type {
			case "stream_event":
				p.handleStreamEvent(event.Event)
				return
			case "assistant":
				p.handleAssistantMessage(event.Message)
				return
			case "result":
				p.handleResult(event.Subtype, event.Result)
				return
			case "error":
				p.err = fmt.Errorf("claude stream error")
				p.publish("agent.output", runtime.Output{Stream: output.Stream, Line: rawLine})
				p.complete()
				return
			default:
				// system/init/user/error/etc.
				p.publish("agent.output", runtime.Output{Stream: output.Stream, Line: rawLine})
				return
			}
		}
	}
	// stdout non-JSON: treat as assistant message for raw-text agents.
	p.persistAssistant(line)
}

func (shellOutputNormalizer) Handle(p *streamParser, output normalizerBridgeOutput) {
	rawLine, ok := output.line()
	if !ok {
		return
	}
	line := strings.TrimSpace(rawLine)
	if strings.HasPrefix(line, "{") {
		var event struct {
			Type     string `json:"type"`
			Stream   string `json:"stream,omitempty"`
			Text     string `json:"text,omitempty"`
			Line     string `json:"line,omitempty"`
			ExitCode int    `json:"exit_code,omitempty"`
		}
		if err := json.Unmarshal([]byte(line), &event); err == nil {
			switch event.Type {
			case "harness.shell_output":
				text := event.Text
				if text == "" {
					text = event.Line
				}
				p.handleShellOutput(event.Stream, text)
				return
			case "harness.turn_done":
				p.handleShellTurnDone(event.ExitCode)
				return
			default:
				p.publish("agent.output", runtime.Output{Stream: output.Stream, Line: rawLine})
				return
			}
		}
	}
	p.persistAssistant(rawLine)
}

func (nativeEventsOutputNormalizer) Handle(p *streamParser, output normalizerBridgeOutput) {
	var native nativeBridgeEventPayload
	if err := json.Unmarshal(output.Payload, &native); err != nil {
		p.err = fmt.Errorf("native event decode failed: %w", err)
		p.complete()
		return
	}
	if native.Schema != "harness_native_events_v1" {
		p.err = fmt.Errorf("unsupported native event schema %q", native.Schema)
		p.complete()
		return
	}
	switch native.Event.Type {
	case "agent.message":
		var payload struct {
			Content string `json:"content"`
		}
		if err := json.Unmarshal(native.Event.Payload, &payload); err != nil {
			p.err = fmt.Errorf("native agent.message decode failed: %w", err)
			p.complete()
			return
		}
		p.persistAssistant(payload.Content)
	case "agent.delta", "agent.output", "system.status":
		p.publish(native.Event.Type, nativeEventPublicPayload(native.Event.Payload))
	default:
		p.err = fmt.Errorf("unsupported native event type %q", native.Event.Type)
		p.complete()
	}
}

func nativeEventPublicPayload(raw json.RawMessage) any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var payload any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return map[string]string{"raw": string(raw)}
	}
	return payload
}

func (p *streamParser) handleStreamEvent(raw json.RawMessage) {
	if len(raw) == 0 {
		return
	}
	var inner claudeStreamEvent
	if err := json.Unmarshal(raw, &inner); err != nil {
		return
	}
	text := streamDeltaText(inner.Delta)
	if inner.Type != "content_block_delta" || text == "" {
		return
	}
	if inner.Delta.Type != "text_delta" {
		return
	}
	// Claude stream_event doesn't always nest the message id; use a stable
	// fallback so the UI can still group deltas.
	id := inner.Message.ID
	if id == "" {
		id = "assistant_pending"
	}
	if _, ok := p.pending[id]; !ok {
		p.pending[id] = &strings.Builder{}
	}
	p.pending[id].WriteString(text)
	p.srv.hub.Publish(events.Event{
		Type:      "agent.delta",
		SessionID: p.sessionID,
		Payload: map[string]any{
			"message_id": id,
			"text":       text,
			"delta_type": inner.Delta.Type,
			"index":      inner.Index,
		},
	})
}

func streamDeltaText(delta claudeStreamDelta) string {
	switch delta.Type {
	case "text_delta":
		return delta.Text
	case "thinking_delta":
		return delta.Thinking
	case "input_json_delta":
		return delta.PartialJSON
	default:
		return ""
	}
}

func (p *streamParser) handleAssistantMessage(raw json.RawMessage) {
	if len(raw) == 0 {
		return
	}
	var msg struct {
		ID      string `json:"id"`
		Role    string `json:"role"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	var b strings.Builder
	for _, part := range msg.Content {
		if part.Type == "text" {
			b.WriteString(part.Text)
		}
	}
	text := b.String()
	if text == "" {
		return
	}
	delete(p.pending, msg.ID)
	delete(p.pending, "assistant_pending")
	p.persistAssistant(text)
}

func (p *streamParser) handleResult(subtype, result string) {
	if subtype != "" && subtype != "success" {
		p.publish("system.status", runtime.Output{Stream: "runtime", Line: fmt.Sprintf("claude result subtype %s", subtype)})
		if strings.TrimSpace(result) != "" {
			p.persistAssistant(result)
		} else if len(p.pending) > 0 {
			p.flush()
		} else {
			p.persistAssistant(fmt.Sprintf("Claude turn ended with %s.", subtype))
		}
		p.complete()
		return
	}
	if result != "" {
		p.persistAssistant(result)
	} else {
		p.flush()
	}
	p.complete()
}

func (p *streamParser) handleShellOutput(stream, text string) {
	if stream == "" {
		stream = "stdout"
	}
	if text == "" {
		return
	}
	out := runtime.Output{Stream: stream, Line: text}
	p.publish("agent.output", out)
	p.persistShellOutput(text)
}

func (p *streamParser) handleShellTurnDone(exitCode int) {
	if exitCode != 0 {
		p.srv.log.Info("shell turn completed", "session_id", p.sessionID, "exit_code", exitCode)
	}
	p.complete()
}

func (p *streamParser) persistAssistant(text string) {
	if strings.TrimSpace(text) == "" || text == p.last {
		return
	}
	stored, err := p.srv.store.AddMessage(context.Background(), p.sessionID, "assistant", text)
	if err != nil {
		p.srv.log.Warn("failed to store assistant message", "session_id", p.sessionID, "error", err)
		return
	}
	p.last = text
	p.srv.hub.Publish(events.Event{Type: "agent.message", SessionID: p.sessionID, Payload: stored})
}

func (p *streamParser) persistShellOutput(text string) {
	stored, err := p.srv.store.AddMessage(context.Background(), p.sessionID, "assistant", text)
	if err != nil {
		p.srv.log.Warn("failed to store shell output", "session_id", p.sessionID, "error", err)
		return
	}
	p.srv.hub.Publish(events.Event{Type: "agent.message", SessionID: p.sessionID, Payload: stored})
}

func (p *streamParser) flush() {
	// If the runtime exited mid-stream without ever delivering an "assistant"
	// or "result" event, salvage whatever we buffered into a final message.
	if len(p.pending) == 0 {
		return
	}
	var b strings.Builder
	for _, sb := range p.pending {
		b.WriteString(sb.String())
	}
	p.pending = map[string]*strings.Builder{}
	if text := strings.TrimSpace(b.String()); text != "" {
		p.persistAssistant(text)
	}
}

func (p *streamParser) publish(eventType string, payload any) {
	p.srv.hub.Publish(events.Event{Type: eventType, SessionID: p.sessionID, Payload: payload})
}
