package server

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"harness-platform/orchestrator/internal/artifacts"
	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

const labUserID = "lab"

type Server struct {
	cfg      config.Config
	store    *store.Store
	runtime  *runtime.Runtime
	watcher  *artifacts.Watcher
	hub      *events.Hub
	log      *slog.Logger
	upgrader websocket.Upgrader
}

func New(
	cfg config.Config,
	store *store.Store,
	runtime *runtime.Runtime,
	watcher *artifacts.Watcher,
	hub *events.Hub,
	log *slog.Logger,
) *Server {
	return &Server{
		cfg:     cfg,
		store:   store,
		runtime: runtime,
		watcher: watcher,
		hub:     hub,
		log:     log,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("POST /api/login", s.login)
	mux.Handle("/api/", s.requireAuth(http.HandlerFunc(s.api)))
	mux.Handle("/artifacts/", s.requireAuth(http.HandlerFunc(s.downloadArtifact)))
	return mux
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if s.cfg.SharedSecret != "" && !hmac.Equal([]byte(req.Password), []byte(s.cfg.SharedSecret)) {
		writeError(w, http.StatusUnauthorized, "invalid password")
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     s.cfg.CookieName,
		Value:    s.signCookie(labUserID),
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Expires:  time.Now().Add(24 * time.Hour),
	})
	writeJSON(w, http.StatusOK, map[string]string{"user_id": labUserID})
}

func (s *Server) api(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/api/sessions" && r.Method == http.MethodGet:
		s.listSessions(w, r)
	case r.URL.Path == "/api/sessions" && r.Method == http.MethodPost:
		s.createSession(w, r)
	case r.URL.Path == "/api/events" && r.Method == http.MethodGet:
		s.events(w, r)
	case r.URL.Path == "/api/events/stream" && r.Method == http.MethodGet:
		s.eventsStream(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/sessions/"):
		s.sessionRoute(w, r)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (s *Server) sessionRoute(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/sessions/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	sessionID := parts[0]
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			s.getSession(w, r, sessionID)
		case http.MethodDelete:
			s.destroySession(w, r, sessionID)
		default:
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
		return
	}

	switch {
	case len(parts) == 2 && parts[1] == "messages" && r.Method == http.MethodGet:
		s.listMessages(w, r, sessionID)
	case len(parts) == 2 && parts[1] == "messages" && r.Method == http.MethodPost:
		s.sendMessage(w, r, sessionID)
	case len(parts) == 2 && parts[1] == "artifacts" && r.Method == http.MethodGet:
		s.listArtifacts(w, r, sessionID)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (s *Server) listSessions(w http.ResponseWriter, r *http.Request) {
	sessions, err := s.store.ListSessions(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessions": sessions})
}

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Agent string `json:"agent"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	if req.Agent == "" {
		req.Agent = s.cfg.DefaultAgent
	}
	count, err := s.store.CountActiveSessions(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if count >= s.cfg.MaxSessions {
		writeError(w, http.StatusTooManyRequests, "active session limit reached")
		return
	}

	id := newID("sess")
	now := time.Now().UTC()
	expiresAt := now.Add(s.cfg.SessionTTL)
	session := store.Session{
		ID:                id,
		UserID:            labUserID,
		Status:            string(sessionstate.Created),
		Agent:             req.Agent,
		Workspace:         filepath.Join(s.cfg.SessionsRoot, id),
		RestoreID:         "phase3-" + id,
		ClaudeSessionUUID: uuid.NewString(),
		CreatedAt:         now,
		UpdatedAt:         now,
		ExpiresAt:         &expiresAt,
	}
	if err := os.MkdirAll(session.Workspace, 0o755); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := s.store.CreateSession(r.Context(), session); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.hub.Publish(events.Event{Type: "session.created", SessionID: id, Payload: session})
	writeJSON(w, http.StatusCreated, session)
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
	writeJSON(w, http.StatusOK, session)
}

func (s *Server) sendMessage(w http.ResponseWriter, r *http.Request, sessionID string) {
	var req struct {
		Content string `json:"content"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Content) == "" {
		writeError(w, http.StatusBadRequest, "content is required")
		return
	}
	session, err := s.store.GetSession(r.Context(), sessionID)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "session not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !sessionstate.CanAcceptInput(session.Status) {
		if sessionstate.IsBusy(session.Status) {
			writeError(w, http.StatusConflict, "session is busy")
			return
		}
		writeError(w, http.StatusConflict, "session is "+session.Status)
		return
	}

	msg, err := s.store.AddMessage(r.Context(), sessionID, "user", req.Content)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	runningStatus := string(sessionstate.RunningActive)
	if err := s.store.UpdateSessionStatus(r.Context(), sessionID, runningStatus, nil); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.hub.Publish(events.Event{Type: "message.created", SessionID: sessionID, Payload: msg})
	s.hub.Publish(events.Event{Type: "session." + runningStatus, SessionID: sessionID})
	go s.runSession(context.Background(), session, req.Content)
	writeJSON(w, http.StatusAccepted, map[string]any{"status": runningStatus, "session_id": sessionID, "message": msg})
}

func (s *Server) listMessages(w http.ResponseWriter, r *http.Request, sessionID string) {
	if _, err := s.store.GetSession(r.Context(), sessionID); errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "session not found")
		return
	} else if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	messages, err := s.store.ListMessages(r.Context(), sessionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": messages})
}

func (s *Server) runSession(ctx context.Context, session store.Session, message string) {
	parser := newStreamParser(s, session.ID, session.Agent)
	turnCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()

	result := s.runtime.Start(turnCtx, runtime.StartRequest{
		SessionID:         session.ID,
		RestoreID:         session.RestoreID,
		Agent:             session.Agent,
		FirstMessage:      message,
		ClaudeSessionUUID: session.ClaudeSessionUUID,
		ResumeClaude:      session.Status != string(sessionstate.Created),
		Done:              parser.Done(),
	}, func(output runtime.Output) {
		s.log.Debug("runtime output", "session_id", session.ID, "stream", output.Stream, "line", output.Line)
		parser.handle(output)
	})
	parser.flush()

	now := time.Now()
	status := string(sessionstate.RunningIdle)
	if result.Err != nil {
		status = string(sessionstate.Failed)
		s.log.Warn("runtime failed", "session_id", session.ID, "error", result.Err)
		s.hub.Publish(events.Event{Type: "session.error", SessionID: session.ID, Payload: map[string]string{"error": result.Err.Error()}})
	} else if err := parser.Err(); err != nil {
		status = string(sessionstate.Failed)
		s.log.Warn("runtime stream failed", "session_id", session.ID, "error", err)
		s.hub.Publish(events.Event{Type: "session.error", SessionID: session.ID, Payload: map[string]string{"error": err.Error()}})
	}
	if err := s.store.UpdateSessionStatusAndActivity(ctx, session.ID, status, result.RestoreMS, now); err != nil {
		s.log.Warn("failed to update session status", "session_id", session.ID, "error", err)
	}
	if err := s.watcher.ScanSession(ctx, session.ID); err != nil {
		s.log.Warn("failed to scan session artifacts", "session_id", session.ID, "error", err)
	}
	s.hub.Publish(events.Event{Type: "session." + status, SessionID: session.ID, Payload: map[string]any{"restore_ms": result.RestoreMS}})
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
	if err := s.runtime.Destroy(r.Context(), session.RestoreID); err != nil {
		s.log.Warn("runtime destroy returned error", "session_id", sessionID, "error", err)
	}
	if err := s.store.UpdateSessionStatus(r.Context(), sessionID, string(sessionstate.Destroyed), nil); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	status := string(sessionstate.Destroyed)
	s.hub.Publish(events.Event{Type: "session." + status, SessionID: sessionID})
	writeJSON(w, http.StatusOK, map[string]string{"status": status})
}

func (s *Server) listArtifacts(w http.ResponseWriter, r *http.Request, sessionID string) {
	items, err := s.store.ListArtifacts(r.Context(), sessionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"artifacts": items})
}

func (s *Server) downloadArtifact(w http.ResponseWriter, r *http.Request) {
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/artifacts/"), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" || strings.Contains(parts[1], "..") {
		writeError(w, http.StatusBadRequest, "invalid artifact path")
		return
	}
	http.ServeFile(w, r, filepath.Join(s.cfg.SessionsRoot, parts[0], filepath.FromSlash(parts[1])))
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	sessionID := r.URL.Query().Get("session_id")
	ch, cancel := s.hub.Subscribe(sessionID)
	defer cancel()

	for event := range ch {
		if err := conn.WriteJSON(event); err != nil {
			return
		}
	}
}

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
	ch, cancel := s.hub.Subscribe(sessionID)
	defer cancel()

	if _, err := w.Write([]byte(": connected\n\n")); err != nil {
		return
	}
	flusher.Flush()

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
			payload, err := json.Marshal(event)
			if err != nil {
				s.log.Warn("failed to marshal stream event", "error", err)
				continue
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *Server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.SharedSecret == "" {
			next.ServeHTTP(w, r)
			return
		}
		cookie, err := r.Cookie(s.cfg.CookieName)
		if err != nil || !s.verifyCookie(cookie.Value) {
			writeError(w, http.StatusUnauthorized, "login required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) signCookie(userID string) string {
	mac := hmac.New(sha256.New, []byte(s.cfg.SharedSecret))
	mac.Write([]byte(userID))
	return userID + "." + hex.EncodeToString(mac.Sum(nil))
}

func (s *Server) verifyCookie(value string) bool {
	parts := strings.SplitN(value, ".", 2)
	if len(parts) != 2 {
		return false
	}
	return hmac.Equal([]byte(value), []byte(s.signCookie(parts[0])))
}

func newID(prefix string) string {
	var buf [12]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + base64.RawURLEncoding.EncodeToString(buf[:])
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func (s *Server) MonitorIdleSessions(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sessions, err := s.store.ListSessionsByStatus(ctx, string(sessionstate.RunningIdle))
			if err != nil {
				s.log.Warn("failed to list idle sessions", "error", err)
				continue
			}
			for _, session := range sessions {
				if session.LastActivityAt != nil && time.Since(*session.LastActivityAt) > 30*time.Minute {
					go s.checkpointSession(ctx, session)
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Server) checkpointSession(ctx context.Context, session store.Session) {
	s.log.Info("checkpointing idle session", "session_id", session.ID)

	if err := s.store.UpdateSessionStatus(ctx, session.ID, string(sessionstate.Checkpointing), nil); err != nil {
		s.log.Warn("failed to update session status to checkpointing", "session_id", session.ID, "error", err)
		return
	}

	if err := s.runtime.Checkpoint(ctx, session.ID); err != nil {
		s.log.Warn("checkpoint failed", "session_id", session.ID, "error", err)
		_ = s.store.UpdateSessionStatus(ctx, session.ID, string(sessionstate.Failed), nil)
		return
	}

	if err := s.store.UpdateSessionStatus(ctx, session.ID, string(sessionstate.Checkpointed), nil); err != nil {
		s.log.Warn("failed to update session status to checkpointed", "session_id", session.ID, "error", err)
		return
	}

	s.hub.Publish(events.Event{Type: "session." + string(sessionstate.Checkpointed), SessionID: session.ID})
}
