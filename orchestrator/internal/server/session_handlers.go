package server

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func (s *Server) listSessions(w http.ResponseWriter, r *http.Request) {
	sessions, err := s.store.ListSessions(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessions": publicSessions(sessions)})
}

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	var raw map[string]json.RawMessage
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "invalid json")
			return
		}
	}
	if _, ok := raw["agent"]; ok {
		writeError(w, http.StatusBadRequest, "agent input is no longer supported")
		return
	}
	value, ok := raw["mode"]
	if !ok {
		writeError(w, http.StatusBadRequest, "mode is required")
		return
	}
	var mode string
	if err := json.Unmarshal(value, &mode); err != nil {
		writeError(w, http.StatusBadRequest, "invalid mode")
		return
	}
	mode = strings.TrimSpace(mode)
	if mode == "" {
		writeError(w, http.StatusBadRequest, "mode is required")
		return
	}
	driverID, err := s.driverForMode(mode)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	count, err := s.store.CountActiveSessions(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if count >= s.cfg.MaxSessions {
		writeErrorClass(w, http.StatusServiceUnavailable, "pool_exhausted", "active session limit reached")
		return
	}

	id := newID("sess")
	now := time.Now().UTC()
	var expiresAt *time.Time
	if s.cfg.SessionRetention > 0 {
		value := now.Add(s.cfg.SessionRetention)
		expiresAt = &value
	}
	session := store.Session{
		ID:                    id,
		UserID:                labUserID,
		Status:                string(sessionstate.Created),
		DriverID:              string(driverID),
		Mode:                  mode,
		AutoCheckpointEnabled: s.cfg.Harness.Checkpoint.AutoEnabled,
		CreatedAt:             now,
		UpdatedAt:             now,
		ExpiresAt:             expiresAt,
	}
	if err := s.store.CreateSession(r.Context(), session); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.hub.Publish(events.Event{Type: "session.created", SessionID: id, Payload: publicSession(session)})
	writeJSON(w, http.StatusCreated, publicSession(session))
}

func (s *Server) driverForMode(mode string) (agents.ID, error) {
	resolution, capabilityErr := s.resolveModeDeployment(mode)
	if capabilityErr != nil {
		return "", fmt.Errorf("%s", capabilityErr.message)
	}
	return resolution.DriverID, nil
}

func (s *Server) getSession(w http.ResponseWriter, r *http.Request, sessionID string) {
	session, err := s.store.GetSession(r.Context(), sessionID)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "session not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, publicSession(session))
}

func (s *Server) destroySession(w http.ResponseWriter, r *http.Request, sessionID string) {
	session, err := s.store.GetSession(r.Context(), sessionID)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "session not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if session.ActiveGenerationID != "" && session.Status != string(sessionstate.Checkpointed) {
		details, err := s.runtimeGenerationDetails(r.Context(), session.ID, session.ActiveGenerationID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if err := s.destroyGenerationRuntime(r.Context(), details); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	now := time.Now().UTC()
	result, err := s.store.DestroySession(r.Context(), sessionID, now)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for _, generationID := range result.GenerationIDs {
		if err := s.cleanupGenerationResources(r.Context(), sessionID, generationID, now); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	status := string(sessionstate.Destroyed)
	s.publishDurableEvent(r.Context(), result.EventID)
	writeJSON(w, http.StatusOK, map[string]string{"status": status})
}
