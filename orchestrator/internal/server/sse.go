package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/store"
)

func (s *Server) eventsStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	header := w.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache, no-transform")
	header.Set("Connection", "keep-alive")
	header.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	sessionID := r.URL.Query().Get("session_id")
	lastEventID, cursorProvided, err := parseLastEventID(r)
	if err != nil {
		writeSSEError(w, flusher, "invalid_last_event_id", err.Error())
		return
	}
	ch, cancel := s.hub.Subscribe(sessionID)
	defer cancel()

	if _, err := w.Write([]byte(": connected\n\n")); err != nil {
		return
	}
	flusher.Flush()

	replayedThrough := lastEventID
	if cursorProvided {
		if nextAfter, err := s.writeSSEReplay(r.Context(), w, flusher, sessionID, lastEventID); err != nil {
			s.log.Warn("failed to replay stream events", "session_id", sessionID, "last_event_id", lastEventID, "error", err)
			return
		} else if nextAfter > replayedThrough {
			replayedThrough = nextAfter
		}
	}

	heartbeat := time.NewTicker(20 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			if _, err := w.Write([]byte(": ping\n\n")); err != nil {
				return
			}
			flusher.Flush()
		case event, ok := <-ch:
			if !ok {
				return
			}
			if event.EventID != 0 && event.EventID <= replayedThrough {
				continue
			}
			if err := writeSSEEvent(w, event); err != nil {
				return
			}
			if event.EventID > replayedThrough {
				replayedThrough = event.EventID
			}
			flusher.Flush()
		}
	}
}

func (s *Server) writeSSEReplay(ctx context.Context, w http.ResponseWriter, flusher http.Flusher, sessionID string, lastEventID int64) (int64, error) {
	replayAfter := lastEventID
	if oldest, ok, err := s.store.OldestEventID(ctx, sessionID); err != nil {
		return replayAfter, err
	} else if ok && lastEventID < oldest-1 {
		gapID := oldest - 1
		payloadSessionID := any(nil)
		if sessionID != "" {
			payloadSessionID = sessionID
		}
		if err := writeSSEEvent(w, events.Event{
			EventID: gapID,
			Type:    "replay_gap",
			Payload: map[string]any{
				"requested_last_event_id": lastEventID,
				"oldest_available":        oldest,
				"session_id_filter":       payloadSessionID,
				"reason":                  "retention_window_exceeded",
			},
		}); err != nil {
			return replayAfter, err
		}
		flusher.Flush()
		replayAfter = 0
	}
	records, err := s.store.ListEvents(ctx, store.ListEventsParams{
		AfterEventID: replayAfter,
		SessionID:    sessionID,
	})
	if err != nil {
		return replayAfter, err
	}
	replayedThrough := replayAfter
	for _, record := range records {
		event := eventFromRecord(record)
		if err := writeSSEEvent(w, event); err != nil {
			return replayedThrough, err
		}
		replayedThrough = record.EventID
	}
	if len(records) > 0 {
		flusher.Flush()
	}
	return replayedThrough, nil
}

func eventFromRecord(record store.EventRecord) events.Event {
	return events.Event{
		EventID:        record.EventID,
		Type:           record.Type,
		SessionID:      record.SessionID,
		TurnID:         record.TurnID,
		GenerationID:   record.GenerationID,
		OutputSequence: record.OutputSequence,
		ProxyRequestID: record.ProxyRequestID,
		Stream:         record.Stream,
		Severity:       record.Severity,
		Time:           record.CreatedAt,
		Payload:        record.Payload,
	}
}

func parseLastEventID(r *http.Request) (int64, bool, error) {
	raw := strings.TrimSpace(r.Header.Get("Last-Event-ID"))
	if raw == "" {
		raw = strings.TrimSpace(r.URL.Query().Get("last_event_id"))
	}
	if raw == "" {
		return 0, false, nil
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id < 0 {
		return 0, true, fmt.Errorf("last_event_id must be a non-negative integer")
	}
	return id, true, nil
}

func writeSSEEvent(w http.ResponseWriter, event events.Event) error {
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	event = publicEvent(event)
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if event.EventID > 0 {
		if _, err := fmt.Fprintf(w, "id: %d\n", event.EventID); err != nil {
			return err
		}
	}
	if event.Type != "" {
		if _, err := fmt.Fprintf(w, "event: %s\n", event.Type); err != nil {
			return err
		}
	}
	_, err = fmt.Fprintf(w, "data: %s\n\n", payload)
	return err
}

func writeSSEError(w http.ResponseWriter, flusher http.Flusher, errorClass, message string) {
	_ = writeSSEEvent(w, events.Event{
		Type: "error",
		Payload: map[string]string{
			"error_class": errorClass,
			"error":       message,
		},
	})
	flusher.Flush()
}
