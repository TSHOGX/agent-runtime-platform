package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func TestPublicSessionDoesNotInferMissingMode(t *testing.T) {
	now := time.Now().UTC()
	got := publicSession(store.Session{
		ID:        "sess_missing_mode",
		UserID:    labUserID,
		Status:    string(sessionstate.Created),
		DriverID:  "claude_code",
		CreatedAt: now,
		UpdatedAt: now,
	})

	if got.Mode != "" || got.ModeLabel != "" {
		t.Fatalf("public session inferred missing mode as %q/%q", got.Mode, got.ModeLabel)
	}
}

func TestCreateSessionUsesPublicSessionDTO(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	hub := events.NewHub()
	eventsCh, cancelEvents := hub.Subscribe("")
	defer cancelEvents()
	cfg := testServerConfig(dir)
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: runtime.New(runtime.Config{}),
		watcher: newServerTestWatcher(t, cfg.SessionsRoot, st, events.NewHub()),
		hub:     hub,
		log:     slog.Default(),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/sessions", strings.NewReader(`{"mode":"agent"}`))
	rec := httptest.NewRecorder()
	srv.createSession(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d body %s", rec.Code, rec.Body.String())
	}
	assertPublicSessionJSONOmitsHostFields(t, rec.Body.Bytes())
	var created map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if created["id"] == "" || created["mode"] != "agent" || created["mode_label"] != "Agent" {
		t.Fatalf("unexpected create response: %v", created)
	}

	select {
	case event := <-eventsCh:
		if event.Type != "session.created" {
			t.Fatalf("event type=%s want session.created", event.Type)
		}
		payload, err := json.Marshal(event.Payload)
		if err != nil {
			t.Fatalf("marshal event payload: %v", err)
		}
		assertPublicSessionJSONOmitsHostFields(t, payload)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for session.created event")
	}
}

func TestSessionReadResponsesUsePublicSessionDTO(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	session := createServerTestSession(t, ctx, st, dir, "sess_public", string(sessionstate.RunningIdle), now, nil)
	restoreMS := int64(123)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE sessions
SET checkpoint_path = ?,
    restore_ms = ?
WHERE id = ?`, filepath.Join(dir, "checkpoints", session.ID), restoreMS, session.ID); err != nil {
		t.Fatalf("seed host-only fields: %v", err)
	}

	srv := &Server{
		store: st,
		hub:   events.NewHub(),
		log:   slog.Default(),
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/sessions/"+session.ID, nil)
	getRec := httptest.NewRecorder()
	srv.getSession(getRec, getReq, session.ID)
	if getRec.Code != http.StatusOK {
		t.Fatalf("expected get status 200, got %d body %s", getRec.Code, getRec.Body.String())
	}
	assertPublicSessionJSONOmitsHostFields(t, getRec.Body.Bytes())
	assertContains(t, getRec.Body.String(), `"restore_ms":123`)

	listReq := httptest.NewRequest(http.MethodGet, "/api/sessions", nil)
	listRec := httptest.NewRecorder()
	srv.listSessions(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("expected list status 200, got %d body %s", listRec.Code, listRec.Body.String())
	}
	assertPublicSessionJSONOmitsHostFields(t, listRec.Body.Bytes())
	assertContains(t, listRec.Body.String(), `"id":"sess_public"`)
}

func TestPublicEventSanitizerStripsRuntimePrivateFields(t *testing.T) {
	event := publicEvent(events.Event{
		EventID:      12,
		Type:         "session.checkpoint_retired",
		SessionID:    "sess_public_event",
		GenerationID: "gen_private",
		Payload: map[string]any{
			"session_status":           "running_idle",
			"session_updated_at":       "2026-05-26T01:02:00Z",
			"session_last_activity_at": "2026-05-26T00:30:00Z",
			"generation_id":            "gen_private",
			"active_generation_id":     "gen_private",
			"driver_id":                "claude_code",
			"agent":                    "claude",
			"restore_id":               "restore_private",
			"host_path":                filepath.Join(t.TempDir(), "private"),
			"driver_state": map[string]any{
				"state_digest": "sha256:private",
			},
			"data_volumes": map[string]any{
				"workspace": map[string]any{"host_path": "/host/workspace"},
			},
		},
	})
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal public event: %v", err)
	}
	body := string(data)
	for _, forbidden := range []string{
		`"generation_id"`,
		`"active_generation_id"`,
		`"driver_id"`,
		`"agent"`,
		`"restore_id"`,
		`"host_path"`,
		`"driver_state"`,
		`"data_volumes"`,
		"gen_private",
		"claude_code",
		"restore_private",
	} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("public event exposed %q: %s", forbidden, body)
		}
	}
	assertContains(t, body, `"event_id":12`)
	assertContains(t, body, `"session_id":"sess_public_event"`)
	assertContains(t, body, `"session_status":"running_idle"`)
	assertContains(t, body, `"session_updated_at":"2026-05-26T01:02:00Z"`)
}
