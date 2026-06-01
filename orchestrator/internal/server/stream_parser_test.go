package server

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"os"
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
		DriverID:  "claude_code",
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
	parser := newStreamParser(srv, "sess_1", "claude_code")

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
	parser := newStreamParser(srv, "sess_1", "claude_code")

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
	parser := newStreamParser(srv, "sess_1", "claude_code")
	ch, cancel := srv.hub.Subscribe("sess_1")
	defer cancel()

	parser.handle(runtime.Output{Stream: "stdout", Line: `{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"checking files"},"message":{"id":"msg_1"}}}`})
	assertNoParserEvent(t, ch)

	parser.handle(runtime.Output{Stream: "stdout", Line: `{"type":"stream_event","event":{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"path\""},"message":{"id":"msg_1"}}}`})
	assertNoParserEvent(t, ch)
}

func TestStreamParserDoesNotFailSessionOnClaudeExecutionError(t *testing.T) {
	srv, st := newParserTestServer(t)
	parser := newStreamParser(srv, "sess_1", "claude_code")

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
	parser := newStreamParser(srv, "sess_1", "claude_code")

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

func TestPiOutputNormalizerConsumesPinnedCorpus(t *testing.T) {
	srv, st := newParserTestServer(t)
	parser := newStreamParser(srv, "sess_1", "pi")
	path := filepath.Join("testdata", "pi", "0.77.0", "event-normalizer-corpus.jsonl")
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open pi corpus: %v", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		parser.handle(runtime.Output{Stream: "stdout", Line: scanner.Text()})
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan pi corpus: %v", err)
	}
	select {
	case <-parser.Done():
	case <-time.After(time.Second):
		t.Fatal("parser did not complete after pi corpus")
	}
	if err := parser.Err(); err != nil {
		t.Fatalf("pi corpus should not fail parser: %v", err)
	}
	messages, err := st.ListMessages(context.Background(), "sess_1")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 || messages[0].Role != "assistant" || messages[0].Content != "Hello world" {
		t.Fatalf("unexpected pi message: %+v", messages)
	}
}

func TestPiOutputNormalizerIgnoresUserMessageEndAndPersistsAssistantOnly(t *testing.T) {
	srv, st := newParserTestServer(t)
	parser := newStreamParser(srv, "sess_1", "pi")

	lines := []string{
		`{"type":"message_start","message":{"role":"user","content":[{"type":"text","text":"hi"}]}}`,
		`{"type":"message_end","message":{"role":"user","content":[{"type":"text","text":"hi"}]}}`,
		`{"type":"message_start","message":{"role":"assistant","content":[]}}`,
		`{"type":"message_update","messageId":"msg_1","assistantMessageEvent":{"type":"text_start","contentIndex":0}}`,
		`{"type":"message_update","messageId":"msg_1","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"hello"}}`,
		`{"type":"message_update","messageId":"msg_1","assistantMessageEvent":{"type":"text_end","contentIndex":0,"content":"hello"}}`,
		`{"type":"message_end","messageId":"msg_1","message":{"role":"assistant","content":[{"type":"text","text":"hello"}]}}`,
		`{"type":"agent_end"}`,
	}
	for _, line := range lines {
		parser.handle(runtime.Output{Stream: "stdout", Line: line})
	}

	messages, err := st.ListMessages(context.Background(), "sess_1")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 || messages[0].Role != "assistant" || messages[0].Content != "hello" {
		t.Fatalf("unexpected pi messages: %+v", messages)
	}
	if err := parser.Err(); err != nil {
		t.Fatalf("pi normalizer should accept standard message events: %v", err)
	}
}

func TestPiOutputNormalizerAcceptsRetryLifecycleEvents(t *testing.T) {
	srv, _ := newParserTestServer(t)
	parser := newStreamParser(srv, "sess_1", "pi")
	ch, cancel := srv.hub.Subscribe("sess_1")
	defer cancel()

	for _, line := range []string{
		`{"type":"auto_retry_start","attempt":1}`,
		`{"type":"auto_retry_end","attempt":1}`,
	} {
		parser.handle(runtime.Output{Stream: "stdout", Line: line})
	}

	for _, wantType := range []string{"auto_retry_start", "auto_retry_end"} {
		select {
		case event := <-ch:
			if event.Type != "system.status" {
				t.Fatalf("expected retry lifecycle event to publish system.status, got %+v", event)
			}
			payload, ok := event.Payload.(map[string]any)
			if !ok || payload["type"] != wantType {
				t.Fatalf("unexpected retry lifecycle payload: %+v", event.Payload)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %s event", wantType)
		}
	}
	if err := parser.Err(); err != nil {
		t.Fatalf("pi normalizer should accept retry lifecycle events: %v", err)
	}
}

func TestPiOutputNormalizerToleratesUnknownType(t *testing.T) {
	srv, st := newParserTestServer(t)
	parser := newStreamParser(srv, "sess_1", "pi")

	// The producer forwards every pi stdout line, so an unknown but well-formed
	// event (e.g. a future usage/token event) must not abort the turn. A real
	// assistant message before/after it should still be persisted normally.
	lines := []string{
		`{"type":"message_update","messageId":"msg_1","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"hello"}}`,
		`{"type":"future_event","tokens":42}`,
		`{"type":"message_end","messageId":"msg_1","message":{"role":"assistant","content":[{"type":"text","text":"hello"}]}}`,
		`{"type":"turn_end","status":"completed"}`,
	}
	for _, line := range lines {
		parser.handle(runtime.Output{Stream: "stdout", Line: line})
	}

	select {
	case <-parser.Done():
	case <-time.After(time.Second):
		t.Fatal("parser did not complete after turn_end")
	}
	if err := parser.Err(); err != nil {
		t.Fatalf("unknown pi event type should be tolerated, got error: %v", err)
	}

	messages, err := st.ListMessages(context.Background(), "sess_1")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 || messages[0].Role != "assistant" || messages[0].Content != "hello" {
		t.Fatalf("expected the surrounding assistant message to persist, got %+v", messages)
	}
}

func TestPiOutputNormalizerRejectsMissingType(t *testing.T) {
	srv, _ := newParserTestServer(t)
	parser := newStreamParser(srv, "sess_1", "pi")

	// A well-formed pi event always carries a type; an empty/missing type is
	// malformed input and stays fatal.
	parser.handle(runtime.Output{Stream: "stdout", Line: `{"foo":"bar"}`})

	select {
	case <-parser.Done():
	case <-time.After(time.Second):
		t.Fatal("parser did not complete after malformed pi event")
	}
	if err := parser.Err(); err == nil || err.Error() != "pi event missing type" {
		t.Fatalf("unexpected pi parser error: %v", err)
	}
}

func TestPiOutputNormalizerRejectsUnknownAssistantEventType(t *testing.T) {
	srv, _ := newParserTestServer(t)
	parser := newStreamParser(srv, "sess_1", "pi")

	parser.handle(runtime.Output{Stream: "stdout", Line: `{"type":"message_update","assistantMessageEvent":{"type":"future_delta"}}`})

	select {
	case <-parser.Done():
	case <-time.After(time.Second):
		t.Fatal("parser did not complete after unknown pi assistant event")
	}
	if err := parser.Err(); err == nil || err.Error() != `unsupported pi assistant message event type "future_delta"` {
		t.Fatalf("unexpected pi parser error: %v", err)
	}
}

func TestPiOutputNormalizerFlushPreservesInsertionOrder(t *testing.T) {
	srv, st := newParserTestServer(t)
	parser := newStreamParser(srv, "sess_1", "pi")

	// Two builders buffered under ids whose insertion order ("zzz" then "aaa")
	// is the reverse of their lexical order. turn_end salvages them via flush();
	// the result must follow chronological insertion order, not map/lexical
	// order, so randomized map iteration cannot scramble the assembled text.
	lines := []string{
		`{"type":"message_update","messageId":"msg_zzz","assistantMessageEvent":{"type":"text_delta","delta":"first"}}`,
		`{"type":"message_update","messageId":"msg_aaa","assistantMessageEvent":{"type":"text_delta","delta":"second"}}`,
		`{"type":"turn_end","status":"completed"}`,
	}
	for _, line := range lines {
		parser.handle(runtime.Output{Stream: "stdout", Line: line})
	}

	select {
	case <-parser.Done():
	case <-time.After(time.Second):
		t.Fatal("parser did not complete after turn_end")
	}

	messages, err := st.ListMessages(context.Background(), "sess_1")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 || messages[0].Content != "firstsecond" {
		t.Fatalf("expected insertion-order salvage \"firstsecond\", got %+v", messages)
	}
}

func TestPiOutputNormalizerDrainsEmptyIDPendingKeyAtMessageEnd(t *testing.T) {
	srv, st := newParserTestServer(t)
	parser := newStreamParser(srv, "sess_1", "pi")

	// Deltas arrive without a messageId so they buffer under the
	// "pi_assistant_pending" empty-id pending key. message_end then arrives with a real
	// id but no inline text. The pending text must be emitted as this
	// message's assistant.message at message_end rather than only being
	// salvaged in bulk at turn_end.
	lines := []string{
		`{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"buffered text"}}`,
		`{"type":"message_end","messageId":"msg_1","message":{"role":"assistant","content":[]}}`,
	}
	for _, line := range lines {
		parser.handle(runtime.Output{Stream: "stdout", Line: line})
	}

	messages, err := st.ListMessages(context.Background(), "sess_1")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 || messages[0].Role != "assistant" || messages[0].Content != "buffered text" {
		t.Fatalf("expected empty-id pending text persisted at message_end, got %+v", messages)
	}

	// A following turn_end must not re-emit the already-drained pending text.
	parser.handle(runtime.Output{Stream: "stdout", Line: `{"type":"turn_end","status":"completed"}`})
	messages, err = st.ListMessages(context.Background(), "sess_1")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected no duplicate message after turn_end, got %+v", messages)
	}
}

func TestNativeEventsOutputNormalizerPersistsMessage(t *testing.T) {
	srv, st := newParserTestServer(t)
	parser := newStreamParser(srv, "sess_1", "native_events_probe")

	parser.handleBridgeOutput(normalizerBridgeOutput{
		Stream: "stdout",
		Payload: json.RawMessage(`{
			"schema":"harness_native_events_v1",
			"event":{"type":"agent.message","payload":{"content":"native hello"}}
		}`),
	})

	messages, err := st.ListMessages(context.Background(), "sess_1")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 || messages[0].Role != "assistant" || messages[0].Content != "native hello" {
		t.Fatalf("unexpected native message: %+v", messages)
	}
}

func TestNativeEventsOutputNormalizerRejectsUnknownType(t *testing.T) {
	srv, _ := newParserTestServer(t)
	parser := newStreamParser(srv, "sess_1", "native_events_probe")

	parser.handleBridgeOutput(normalizerBridgeOutput{
		Stream: "stdout",
		Payload: json.RawMessage(`{
			"schema":"harness_native_events_v1",
			"event":{"type":"agent.future","payload":{}}
		}`),
	})

	select {
	case <-parser.Done():
	case <-time.After(time.Second):
		t.Fatal("parser did not complete after unknown native event")
	}
	if err := parser.Err(); err == nil || err.Error() != `unsupported native event type "agent.future"` {
		t.Fatalf("unexpected native parser error: %v", err)
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
