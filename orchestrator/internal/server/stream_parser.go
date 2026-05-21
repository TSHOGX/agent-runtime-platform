package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/runtime"
)

// streamParser converts a runtime stdout/stderr stream into chat-friendly
// hub events. Lines that successfully decode as Claude Code stream-json get
// translated to agent.delta / agent.message; everything else is forwarded as
// agent.output so the UI stays compatible with raw-text agents.
type streamParser struct {
	srv       *Server
	sessionID string
	agent     string
	// pending text chunks per assistant message id, flushed when we see the
	// matching "assistant" full message (or when the runtime exits).
	pending map[string]*strings.Builder
	done    chan struct{}
	once    sync.Once
	err     error
	last    string
}

func newStreamParser(srv *Server, sessionID, agent string) *streamParser {
	return &streamParser{
		srv:       srv,
		sessionID: sessionID,
		agent:     agent,
		pending:   map[string]*strings.Builder{},
		done:      make(chan struct{}),
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
	rawLine := output.Line
	line := strings.TrimSpace(rawLine)
	if line == "" {
		return
	}

	// runtime stream → system.status event (system status messages)
	if output.Stream == "runtime" {
		p.publish("system.status", output)
		return
	}

	// stderr always goes to agent.output (debug/logs)
	if output.Stream == "stderr" {
		p.publish("agent.output", output)
		return
	}
	// stdout: try to parse as stream-json first
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
			case "harness.shell_output":
				if p.agent == "sh" {
					text := event.Text
					if text == "" {
						text = event.Line
					}
					p.handleShellOutput(event.Stream, text)
					return
				}
			case "harness.turn_done":
				if p.agent == "sh" {
					p.handleShellTurnDone(event.ExitCode)
					return
				}
			case "error":
				p.err = fmt.Errorf("claude stream error")
				p.publish("agent.output", output)
				p.complete()
				return
			default:
				// system/init/user/error/etc.
				p.publish("agent.output", output)
				return
			}
		}
	}
	// stdout non-JSON: treat as assistant message for raw-text agents.
	if p.agent == "sh" {
		p.persistAssistant(rawLine)
		return
	}
	p.persistAssistant(line)
}

func (p *streamParser) handleStreamEvent(raw json.RawMessage) {
	if len(raw) == 0 {
		return
	}
	var inner struct {
		Type  string `json:"type"`
		Index int    `json:"index"`
		Delta struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"delta"`
		Message struct {
			ID string `json:"id"`
		} `json:"message"`
	}
	if err := json.Unmarshal(raw, &inner); err != nil {
		return
	}
	if inner.Type != "content_block_delta" || inner.Delta.Type != "text_delta" || inner.Delta.Text == "" {
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
	p.pending[id].WriteString(inner.Delta.Text)
	p.srv.hub.Publish(events.Event{
		Type:      "agent.delta",
		SessionID: p.sessionID,
		Payload:   map[string]any{"message_id": id, "text": inner.Delta.Text},
	})
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

func (p *streamParser) publish(eventType string, output runtime.Output) {
	p.srv.hub.Publish(events.Event{Type: eventType, SessionID: p.sessionID, Payload: output})
}
