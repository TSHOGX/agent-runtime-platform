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
	// pendingOrder records the order ids were first buffered so salvaged text
	// is reassembled chronologically rather than in randomized map order.
	pending      map[string]*strings.Builder
	pendingOrder []string
	done         chan struct{}
	once         sync.Once
	err          error
	last         string
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
type piOutputNormalizer struct{}
type shellOutputNormalizer struct{}
type nativeEventsOutputNormalizer struct{}
type unsupportedOutputNormalizer struct {
	driverID string
}

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

func newStreamParser(srv *Server, sessionID, driverID string) *streamParser {
	return &streamParser{
		srv:        srv,
		sessionID:  sessionID,
		driverID:   strings.TrimSpace(driverID),
		normalizer: outputNormalizerForDriver(driverID),
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

// bufferPending returns the builder for id, creating it (and recording its
// insertion order) the first time the id is seen.
func (p *streamParser) bufferPending(id string) *strings.Builder {
	sb, ok := p.pending[id]
	if !ok {
		sb = &strings.Builder{}
		p.pending[id] = sb
		p.pendingOrder = append(p.pendingOrder, id)
	}
	return sb
}

// dropPending removes id from both the pending map and the order slice so a
// finalized message is never re-flushed.
func (p *streamParser) dropPending(id string) {
	if _, ok := p.pending[id]; !ok {
		return
	}
	delete(p.pending, id)
	for i, key := range p.pendingOrder {
		if key == id {
			p.pendingOrder = append(p.pendingOrder[:i], p.pendingOrder[i+1:]...)
			break
		}
	}
}

// drainPending concatenates the builders for the given ids in the order they
// were first buffered and removes them, returning the salvaged text. It lets a
// message_end finalize text that may have landed under a real id and/or the
// empty-id pending key.
func (p *streamParser) drainPending(ids ...string) string {
	want := make(map[string]bool, len(ids))
	for _, id := range ids {
		if _, ok := p.pending[id]; ok {
			want[id] = true
		}
	}
	if len(want) == 0 {
		return ""
	}
	var b strings.Builder
	for _, key := range p.pendingOrder {
		if want[key] {
			b.WriteString(p.pending[key].String())
		}
	}
	for id := range want {
		p.dropPending(id)
	}
	return b.String()
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
	factory := outputNormalizers[driverID]
	if factory == nil {
		return unsupportedOutputNormalizer{driverID: driverID}
	}
	return factory()
}

var outputNormalizers = map[string]outputNormalizerFactory{
	string(agents.ClaudeCode): func() outputNormalizer { return claudeOutputNormalizer{} },
	string(agents.Pi):         func() outputNormalizer { return piOutputNormalizer{} },
	string(agents.Shell):      func() outputNormalizer { return shellOutputNormalizer{} },
	"native_events_probe":     func() outputNormalizer { return nativeEventsOutputNormalizer{} },
}

func (n unsupportedOutputNormalizer) Handle(p *streamParser, output normalizerBridgeOutput) {
	if n.driverID == "" {
		p.err = fmt.Errorf("driver id is required")
	} else {
		p.err = fmt.Errorf("unsupported driver %q", n.driverID)
	}
	p.complete()
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
			case "system", "init", "user":
				p.publish("agent.output", runtime.Output{Stream: output.Stream, Line: rawLine})
				return
			default:
				p.err = fmt.Errorf("unsupported claude event type %q", event.Type)
				p.complete()
				return
			}
		} else {
			p.err = fmt.Errorf("claude event decode failed: %w", err)
			p.complete()
			return
		}
	}
	p.err = fmt.Errorf("claude stdout line is not a recognized JSON event")
	p.complete()
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

func (piOutputNormalizer) Handle(p *streamParser, output normalizerBridgeOutput) {
	rawLine, ok := output.line()
	if !ok {
		return
	}
	line := strings.TrimSpace(rawLine)
	if line == "" {
		return
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		p.err = fmt.Errorf("pi event decode failed: %w", err)
		p.complete()
		return
	}
	eventType, _ := event["type"].(string)
	switch eventType {
	case "response":
		if success, ok := event["success"].(bool); ok && !success {
			p.err = fmt.Errorf("pi command %q failed", stringFromMap(event, "command"))
			p.publish("agent.output", event)
			p.complete()
			return
		}
		p.publish("system.status", event)
	case "agent_start", "turn_start", "message_start", "queue_update", "compaction_start", "compaction_end", "auto_retry", "auto_retry_start", "auto_retry_end":
		p.publish("system.status", event)
	case "message_update":
		p.handlePiMessageUpdate(event)
	case "message_end":
		p.handlePiMessageEnd(event)
	case "tool_execution_start", "tool_execution_update", "tool_execution_end":
		p.publish("agent.output", event)
	case "turn_end", "agent_end":
		p.publish("system.status", event)
		p.flush()
		p.complete()
	case "error":
		p.err = fmt.Errorf("pi event error")
		p.publish("agent.output", event)
		p.complete()
	default:
		if eventType == "" {
			// A well-formed pi event always carries a type; an empty type is
			// malformed input and stays fatal, matching the decode-error path.
			p.err = fmt.Errorf("pi event missing type")
			p.complete()
			return
		}
		p.err = fmt.Errorf("unsupported pi event type %q", eventType)
		p.complete()
	}
}

func (p *streamParser) handlePiMessageUpdate(event map[string]any) {
	assistantEvent, _ := event["assistantMessageEvent"].(map[string]any)
	assistantEventType, _ := assistantEvent["type"].(string)
	switch assistantEventType {
	case "text_start", "text_end":
		p.publish("agent.output", event)
	case "text_delta":
		text, _ := assistantEvent["delta"].(string)
		if text == "" {
			return
		}
		id := stringFromMap(event, "messageId")
		if id == "" {
			id = "pi_assistant_pending"
		}
		p.bufferPending(id).WriteString(text)
		p.publish("agent.delta", map[string]any{
			"message_id": id,
			"text":       text,
			"delta_type": assistantEventType,
		})
	default:
		p.err = fmt.Errorf("unsupported pi assistant message event type %q", assistantEventType)
		p.complete()
	}
}

func (p *streamParser) handlePiMessageEnd(event map[string]any) {
	if messageRole(event["message"]) != "assistant" {
		p.publish("system.status", event)
		return
	}
	id := stringFromMap(event, "messageId")
	text := piMessageText(event["message"])
	if text == "" {
		// No authoritative inline text: salvage whatever streamed in under the
		// real id and the empty-id pending key, emitting it as this message's
		// assistant.message rather than deferring to the turn_end bulk flush.
		if salvaged := p.drainPending(id, "pi_assistant_pending"); salvaged != "" {
			p.persistAssistant(salvaged)
		}
		return
	}
	// Inline text is authoritative; drop the buffered deltas for both the real
	// id and the empty-id pending key (mirrors the claude path) so they are neither
	// re-emitted here nor salvaged again at turn_end.
	p.dropPending(id)
	p.dropPending("pi_assistant_pending")
	p.persistAssistant(text)
}

func messageRole(value any) string {
	message, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	role, _ := message["role"].(string)
	return role
}

func piMessageText(value any) string {
	message, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	content, ok := message["content"].([]any)
	if !ok {
		return ""
	}
	var b strings.Builder
	for _, item := range content {
		part, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if partType, _ := part["type"].(string); partType != "text" {
			continue
		}
		text, _ := part["text"].(string)
		b.WriteString(text)
	}
	return b.String()
}

func stringFromMap(value map[string]any, key string) string {
	got, _ := value[key].(string)
	return got
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
		payload, err := nativeEventPublicPayload(native.Event.Payload)
		if err != nil {
			p.err = fmt.Errorf("native %s payload decode failed: %w", native.Event.Type, err)
			p.complete()
			return
		}
		p.publish(native.Event.Type, payload)
	default:
		p.err = fmt.Errorf("unsupported native event type %q", native.Event.Type)
		p.complete()
	}
}

func nativeEventPublicPayload(raw json.RawMessage) (any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	var payload any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	return payload, nil
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
	// pending key so the UI can still group deltas.
	id := inner.Message.ID
	if id == "" {
		id = "assistant_pending"
	}
	p.bufferPending(id).WriteString(text)
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
	p.dropPending(msg.ID)
	p.dropPending("assistant_pending")
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
	// Iterate in insertion order so multi-builder text (multiple messageIds or
	// a real id plus the empty-id pending key) is reassembled chronologically rather
	// than in randomized map order.
	if len(p.pending) == 0 {
		return
	}
	var b strings.Builder
	for _, id := range p.pendingOrder {
		if sb, ok := p.pending[id]; ok {
			b.WriteString(sb.String())
		}
	}
	p.pending = map[string]*strings.Builder{}
	p.pendingOrder = nil
	if text := strings.TrimSpace(b.String()); text != "" {
		p.persistAssistant(text)
	}
}

func (p *streamParser) publish(eventType string, payload any) {
	p.srv.hub.Publish(events.Event{Type: eventType, SessionID: p.sessionID, Payload: payload})
}
