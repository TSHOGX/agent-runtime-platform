package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func newParserTestServer(t *testing.T) (*Server, *store.Store) {
	t.Helper()

	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	if err := st.CreateSession(ctx, store.Session{
		ID:        "sess_1",
		UserID:    "lab",
		Status:    string(sessionstate.RunningActive),
		Agent:     "claude",
		Workspace: t.TempDir(),
		RestoreID: "phase3-sess_1",
		CreatedAt: now,
		UpdatedAt: now,
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	return &Server{
		store: st,
		hub:   events.NewHub(),
		log:   slog.Default(),
	}, st
}

func TestStreamParserCompletesOnClaudeResultWithoutDuplicate(t *testing.T) {
	srv, st := newParserTestServer(t)
	parser := newStreamParser(srv, "sess_1", "claude")

	parser.handle(runtime.Output{Stream: "stdout", Line: `{"type":"assistant","message":{"id":"msg_1","role":"assistant","content":[{"type":"text","text":"hi"}]}}`})
	parser.handle(runtime.Output{Stream: "stdout", Line: `{"type":"result","subtype":"success","result":"hi"}`})

	select {
	case <-parser.Done():
	case <-time.After(time.Second):
		t.Fatal("parser did not complete after result event")
	}

	messages, err := st.ListMessages(context.Background(), "sess_1")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected one assistant message, got %d: %+v", len(messages), messages)
	}
	if messages[0].Role != "assistant" || messages[0].Content != "hi" {
		t.Fatalf("unexpected assistant message: %+v", messages[0])
	}
}

func TestStreamParserPersistsResultWhenAssistantMessageIsMissing(t *testing.T) {
	srv, st := newParserTestServer(t)
	parser := newStreamParser(srv, "sess_1", "claude")

	parser.handle(runtime.Output{Stream: "stdout", Line: `{"type":"result","subtype":"success","result":"hi"}`})

	select {
	case <-parser.Done():
	case <-time.After(time.Second):
		t.Fatal("parser did not complete after result event")
	}

	messages, err := st.ListMessages(context.Background(), "sess_1")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 || messages[0].Content != "hi" {
		t.Fatalf("expected result text to be persisted, got %+v", messages)
	}
}

func TestBridgeOutputReusesParserAcrossTurn(t *testing.T) {
	srv, st := newParserTestServer(t)
	turnID := int64(7)
	generationID := "gen_1"
	assistantLine := `{"type":"assistant","message":{"id":"msg_1","role":"assistant","content":[{"type":"text","text":"hi"}]}}`
	resultLine := `{"type":"result","subtype":"success","result":"hi"}`

	srv.handleBridgeOutput(context.Background(), bridge.Envelope{
		Type:         bridge.TypeEmitOutput,
		SessionID:    "sess_1",
		GenerationID: generationID,
		TurnID:       &turnID,
		Payload:      bridgeOutputPayload(t, 1, assistantLine),
	})
	srv.handleBridgeOutput(context.Background(), bridge.Envelope{
		Type:         bridge.TypeEmitOutput,
		SessionID:    "sess_1",
		GenerationID: generationID,
		TurnID:       &turnID,
		Payload:      bridgeOutputPayload(t, 2, resultLine),
	})

	messages, err := st.ListMessages(context.Background(), "sess_1")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 || messages[0].Content != "hi" {
		t.Fatalf("expected one assistant message, got %+v", messages)
	}

	srv.completeBridgeStreamParser(bridge.Envelope{
		Type:         bridge.TypeAckTurnCompleted,
		SessionID:    "sess_1",
		GenerationID: generationID,
		TurnID:       &turnID,
		Payload:      json.RawMessage(`{"status":"completed"}`),
	})

	srv.bridgeParserMu.Lock()
	defer srv.bridgeParserMu.Unlock()
	if len(srv.bridgeParsers) != 0 {
		t.Fatalf("expected bridge parser cache to be empty, got %d", len(srv.bridgeParsers))
	}
}

func TestStreamParserIgnoresClaudeThinkingAndToolDeltas(t *testing.T) {
	srv, _ := newParserTestServer(t)
	parser := newStreamParser(srv, "sess_1", "claude")
	ch, cancel := srv.hub.Subscribe("sess_1")
	defer cancel()

	parser.handle(runtime.Output{Stream: "stdout", Line: `{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"checking files"},"message":{"id":"msg_1"}}}`})
	assertNoParserEvent(t, ch)

	parser.handle(runtime.Output{Stream: "stdout", Line: `{"type":"stream_event","event":{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"path\""},"message":{"id":"msg_1"}}}`})
	assertNoParserEvent(t, ch)
}

func TestStreamParserDoesNotFailSessionOnClaudeExecutionError(t *testing.T) {
	srv, st := newParserTestServer(t)
	parser := newStreamParser(srv, "sess_1", "claude")

	parser.handle(runtime.Output{Stream: "stdout", Line: `{"type":"result","subtype":"error_during_execution"}`})

	select {
	case <-parser.Done():
	case <-time.After(time.Second):
		t.Fatal("parser did not complete after error result event")
	}
	if err := parser.Err(); err != nil {
		t.Fatalf("claude turn execution error should not fail the session: %v", err)
	}

	messages, err := st.ListMessages(context.Background(), "sess_1")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 || messages[0].Content != "Claude turn ended with error_during_execution." {
		t.Fatalf("expected turn error message to be persisted, got %+v", messages)
	}
}

func bridgeOutputPayload(t *testing.T, sequence int64, line string) json.RawMessage {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"output_sequence": sequence,
		"stream":          "stdout",
		"payload": map[string]string{
			"line": line,
		},
	})
	if err != nil {
		t.Fatalf("marshal bridge output: %v", err)
	}
	return payload
}

func TestStreamParserPersistsNonSuccessResultText(t *testing.T) {
	srv, st := newParserTestServer(t)
	parser := newStreamParser(srv, "sess_1", "claude")

	parser.handle(runtime.Output{Stream: "stdout", Line: `{"type":"result","subtype":"error_during_execution","result":"API Error: 400 {\"detail\":\"Erro\"}"}`})

	select {
	case <-parser.Done():
	case <-time.After(time.Second):
		t.Fatal("parser did not complete after error result event")
	}
	if err := parser.Err(); err != nil {
		t.Fatalf("claude turn execution error should not fail the session: %v", err)
	}

	messages, err := st.ListMessages(context.Background(), "sess_1")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 || messages[0].Content != `API Error: 400 {"detail":"Erro"}` {
		t.Fatalf("expected result error text to be persisted, got %+v", messages)
	}
}

func TestStreamParserPersistsShellOutputAndCompletesOnTurnDone(t *testing.T) {
	srv, st := newParserTestServer(t)
	parser := newStreamParser(srv, "sess_1", "sh")

	parser.handle(runtime.Output{Stream: "stdout", Line: `{"type":"harness.shell_output","stream":"stdout","text":"hello from shell\n"}`})
	parser.handle(runtime.Output{Stream: "stdout", Line: `{"type":"harness.turn_done","exit_code":0}`})

	select {
	case <-parser.Done():
	case <-time.After(time.Second):
		t.Fatal("parser did not complete after shell turn_done event")
	}

	messages, err := st.ListMessages(context.Background(), "sess_1")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected one shell output message, got %d: %+v", len(messages), messages)
	}
	if messages[0].Role != "assistant" || messages[0].Content != "hello from shell\n" {
		t.Fatalf("unexpected shell output message: %+v", messages[0])
	}
}

func assertNoParserEvent(t *testing.T, ch <-chan events.Event) {
	t.Helper()
	select {
	case event := <-ch:
		t.Fatalf("expected no parser event, got %+v", event)
	case <-time.After(20 * time.Millisecond):
	}
}
