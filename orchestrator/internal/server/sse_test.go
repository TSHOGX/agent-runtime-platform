package server

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func TestEventsStreamReplaysDurableEventsAfterLastEventID(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	createServerTestSession(t, ctx, st, dir, "sess_a", string(sessionstate.RunningActive), now, nil)
	createServerTestSession(t, ctx, st, dir, "sess_b", string(sessionstate.RunningActive), now, nil)

	firstID, err := st.AppendEvent(ctx, store.AppendEventParams{
		SessionID: "sess_a",
		Type:      bridge.TypeAckTurnStarted,
		Payload:   map[string]string{"step": "first"},
		Now:       now,
	})
	if err != nil {
		t.Fatalf("append first event: %v", err)
	}
	secondID, err := st.AppendEvent(ctx, store.AppendEventParams{
		SessionID: "sess_b",
		Type:      bridge.TypeEmitOutput,
		Payload:   map[string]string{"line": "second"},
		Now:       now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("append second event: %v", err)
	}
	thirdID, err := st.AppendEvent(ctx, store.AppendEventParams{
		SessionID: "sess_a",
		Type:      bridge.TypeAckTurnCompleted,
		Payload:   map[string]string{"status": "completed"},
		Now:       now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("append third event: %v", err)
	}

	srv := &Server{store: st, hub: events.NewHub(), log: slog.Default()}
	req := httptest.NewRequest(http.MethodGet, "/api/events/stream?last_event_id="+strconv.FormatInt(firstID, 10), nil)
	lastEventID, ok, err := parseLastEventID(req)
	if err != nil || !ok || lastEventID != firstID {
		t.Fatalf("parse last_event_id: id=%d ok=%v err=%v", lastEventID, ok, err)
	}
	rec := httptest.NewRecorder()
	replayedThrough, err := srv.writeSSEReplay(req.Context(), rec, rec, "", lastEventID)
	if err != nil {
		t.Fatalf("write replay: %v", err)
	}
	if replayedThrough != thirdID {
		t.Fatalf("replayed through=%d want %d", replayedThrough, thirdID)
	}
	body := rec.Body.String()
	if strings.Contains(body, "id: "+strconv.FormatInt(firstID, 10)+"\n") {
		t.Fatalf("replay included already-seen event: %s", body)
	}
	assertContains(t, body, "id: "+strconv.FormatInt(secondID, 10)+"\n")
	assertContains(t, body, "event: "+bridge.TypeEmitOutput+"\n")
	assertContains(t, body, `"event_id":`+strconv.FormatInt(secondID, 10))
	assertContains(t, body, `"session_id":"sess_b"`)
	assertContains(t, body, `"payload":{"line":"second"}`)
	assertContains(t, body, "id: "+strconv.FormatInt(thirdID, 10)+"\n")
	assertContains(t, body, "event: "+bridge.TypeAckTurnCompleted+"\n")
	if strings.Index(body, "id: "+strconv.FormatInt(secondID, 10)+"\n") >
		strings.Index(body, "id: "+strconv.FormatInt(thirdID, 10)+"\n") {
		t.Fatalf("replayed events out of order: %s", body)
	}

	filtered := httptest.NewRecorder()
	replayedThrough, err = srv.writeSSEReplay(req.Context(), filtered, filtered, "sess_a", firstID)
	if err != nil {
		t.Fatalf("write filtered replay: %v", err)
	}
	if replayedThrough != thirdID {
		t.Fatalf("filtered replayed through=%d want %d", replayedThrough, thirdID)
	}
	filteredBody := filtered.Body.String()
	if strings.Contains(filteredBody, "id: "+strconv.FormatInt(secondID, 10)+"\n") {
		t.Fatalf("filtered replay included another session: %s", filteredBody)
	}
	assertContains(t, filteredBody, "id: "+strconv.FormatInt(thirdID, 10)+"\n")

	headerReq := httptest.NewRequest(http.MethodGet, "/api/events/stream?last_event_id=1", nil)
	headerReq.Header.Set("Last-Event-ID", strconv.FormatInt(thirdID, 10))
	lastEventID, ok, err = parseLastEventID(headerReq)
	if err != nil || !ok || lastEventID != thirdID {
		t.Fatalf("header Last-Event-ID should win: id=%d ok=%v err=%v", lastEventID, ok, err)
	}

	deleted, err := st.PruneEvents(ctx, store.PruneEventsParams{
		RetentionRows: 2,
		Now:           now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatalf("prune replay events: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted=%d want 1", deleted)
	}

	gap := httptest.NewRecorder()
	replayedThrough, err = srv.writeSSEReplay(req.Context(), gap, gap, "", 0)
	if err != nil {
		t.Fatalf("write gap replay: %v", err)
	}
	if replayedThrough != thirdID {
		t.Fatalf("gap replayed through=%d want %d", replayedThrough, thirdID)
	}
	gapBody := gap.Body.String()
	assertContains(t, gapBody, "id: "+strconv.FormatInt(secondID-1, 10)+"\n")
	assertContains(t, gapBody, "event: replay_gap\n")
	assertContains(t, gapBody, `"requested_last_event_id":0`)
	assertContains(t, gapBody, `"oldest_available":`+strconv.FormatInt(secondID, 10))
	assertContains(t, gapBody, `"session_id_filter":null`)
	assertContains(t, gapBody, `"reason":"retention_window_exceeded"`)
	assertContains(t, gapBody, "id: "+strconv.FormatInt(secondID, 10)+"\n")
	assertContains(t, gapBody, "id: "+strconv.FormatInt(thirdID, 10)+"\n")
	if strings.Contains(gapBody, `"payload":{"step":"first"}`) {
		t.Fatalf("gap replay included pruned event: %s", gapBody)
	}

	filteredGap := httptest.NewRecorder()
	replayedThrough, err = srv.writeSSEReplay(req.Context(), filteredGap, filteredGap, "sess_a", 0)
	if err != nil {
		t.Fatalf("write filtered gap replay: %v", err)
	}
	if replayedThrough != thirdID {
		t.Fatalf("filtered gap replayed through=%d want %d", replayedThrough, thirdID)
	}
	filteredGapBody := filteredGap.Body.String()
	assertContains(t, filteredGapBody, "id: "+strconv.FormatInt(thirdID-1, 10)+"\n")
	assertContains(t, filteredGapBody, "event: replay_gap\n")
	assertContains(t, filteredGapBody, `"oldest_available":`+strconv.FormatInt(thirdID, 10))
	assertContains(t, filteredGapBody, `"session_id_filter":"sess_a"`)
	assertContains(t, filteredGapBody, "id: "+strconv.FormatInt(thirdID, 10)+"\n")
	if strings.Contains(filteredGapBody, `"payload":{"line":"second"}`) {
		t.Fatalf("filtered gap replay included another session: %s", filteredGapBody)
	}
}
