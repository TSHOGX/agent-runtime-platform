package server

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/runtime"
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
		Status:    "running_active",
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
