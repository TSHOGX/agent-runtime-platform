package server

import (
	"database/sql"
	"errors"
	"net/http"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/sessionstate"
)

func (s *Server) interruptSession(w http.ResponseWriter, r *http.Request, sessionID string) {
	session, err := s.store.GetSession(r.Context(), sessionID)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "session not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if session.Status != string(sessionstate.RunningActive) {
		writeError(w, http.StatusConflict, "session is not running")
		return
	}
	interruptRequired, err := s.generationPlanFeatureRequired(r.Context(), session.ActiveGenerationID, agents.FeatureInterrupt)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !interruptRequired {
		writeError(w, http.StatusConflict, "interrupt is not supported for this session")
		return
	}
	if err := s.runtime.Interrupt(sessionID); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "interrupting"})
}
