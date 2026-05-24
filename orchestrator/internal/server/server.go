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
	"net/netip"
	"os"
	pathpkg "path"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/artifacts"
	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

const labUserID = "lab"

const (
	idleCheckpointInterval  = 5 * time.Minute
	idleCheckpointThreshold = 30 * time.Minute
	checkpointTimeout       = 2 * time.Minute
	autoCheckpointEnabled   = false
)

type Server struct {
	cfg       config.Config
	store     *store.Store
	runtime   runtimeDriver
	watcher   *artifacts.Watcher
	hub       *events.Hub
	log       *slog.Logger
	upgrader  websocket.Upgrader
	ownerUUID string
}

type runtimeDriver interface {
	Start(context.Context, runtime.StartRequest, func(runtime.Output)) runtime.Result
	PrepareGeneration(context.Context, runtime.StartRequest) (runtime.GenerationArtifacts, error)
	Destroy(context.Context, string) error
	Interrupt(string) error
	Checkpoint(context.Context, string) error
}

type bridgeStore interface {
	bridge.Store
	ListBridgePollGenerations(context.Context, string, time.Time) ([]store.BridgePollGeneration, error)
}

func New(
	cfg config.Config,
	store *store.Store,
	runtime runtimeDriver,
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

func (s *Server) SetOwnerUUID(ownerUUID string) {
	s.ownerUUID = strings.TrimSpace(ownerUUID)
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
	case r.URL.Path == "/api/quota" && r.Method == http.MethodGet:
		s.getQuota(w, r)
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
	case len(parts) == 2 && parts[1] == "interrupt" && r.Method == http.MethodPost:
		s.interruptSession(w, r, sessionID)
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

func (s *Server) getQuota(w http.ResponseWriter, r *http.Request) {
	activeSessions, err := s.store.CountActiveSessions(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	resourceQuota, err := s.store.GetResourceQuota(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	poolCeiling := cidrPool30Capacity(s.cfg.Phase7.Network.CIDRPool.Prefix)
	remainingPoolSlots := poolCeiling - resourceQuota.AllocatedPoolSlots
	if remainingPoolSlots < 0 {
		remainingPoolSlots = 0
	}
	effectiveCeiling := s.cfg.MaxSessions
	if poolCeiling < effectiveCeiling {
		effectiveCeiling = poolCeiling
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"soft_session_ceiling": s.cfg.MaxSessions,
		"active_sessions":      activeSessions,
		"live_pool_ceiling":    poolCeiling,
		"allocated_pool_slots": resourceQuota.AllocatedPoolSlots,
		"remaining_pool_slots": remainingPoolSlots,
		"effective_ceiling":    effectiveCeiling,
	})
}

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Agent string `json:"agent"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	req.Agent = strings.TrimSpace(req.Agent)
	if req.Agent == "" {
		req.Agent = strings.TrimSpace(s.cfg.DefaultAgent)
	}
	if _, ok := agents.Lookup(req.Agent); !ok {
		writeError(w, http.StatusBadRequest, "unsupported agent")
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
	if _, err := s.store.SweepExpiredSessions(r.Context(), time.Now().UTC()); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
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
	leaseOwner := store.GenerationLeaseOwner(s.ownerUUID)
	allocation, err := s.ensureActiveGeneration(r.Context(), session, leaseOwner)
	if errors.Is(err, store.ErrPoolExhausted) {
		writeErrorClass(w, http.StatusServiceUnavailable, "pool_exhausted", "resource pool exhausted")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	generationDetails, err := s.runtimeGenerationDetails(r.Context(), session.ID, allocation.GenerationID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	isNewGeneration := strings.TrimSpace(session.ActiveGenerationID) == ""
	preparedArtifacts := runtimeArtifactsFromDetails(generationDetails)
	if isNewGeneration {
		preparedArtifacts, err = s.runtime.PrepareGeneration(r.Context(), s.runtimeStartRequest(session, allocation.GenerationID, generationDetails, runtime.GenerationArtifacts{}))
		if err != nil {
			s.failGenerationBeforeTurn(session.ID, allocation.GenerationID, allocation.Owner, err)
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if err := s.store.RecordGenerationRuntimeArtifacts(r.Context(), allocation.GenerationID, preparedArtifacts.ManifestDigest, preparedArtifacts.RunscVersion); err != nil {
			s.failGenerationBeforeTurn(session.ID, allocation.GenerationID, allocation.Owner, err)
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if err := s.store.MarkGenerationResourcesLive(r.Context(), session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
			s.failGenerationBeforeTurn(session.ID, allocation.GenerationID, allocation.Owner, err)
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	startReq := s.runtimeStartRequest(session, allocation.GenerationID, generationDetails, preparedArtifacts)
	result := s.runtime.Start(r.Context(), startReq, nil)
	if result.Err != nil {
		s.failGenerationBeforeTurn(session.ID, allocation.GenerationID, allocation.Owner, result.Err)
		writeError(w, http.StatusInternalServerError, result.Err.Error())
		return
	}
	runningStatus := string(sessionstate.RunningActive)
	enqueue, err := s.store.EnqueueTurnMessage(r.Context(), store.EnqueueTurnMessageParams{
		SessionID: sessionID,
		Content:   req.Content,
		Now:       time.Now().UTC(),
	})
	if err != nil {
		if strings.Contains(err.Error(), "session cannot accept input") {
			writeError(w, http.StatusConflict, "session is busy")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.hub.Publish(events.Event{Type: "message.created", SessionID: sessionID, Payload: enqueue.Message})
	s.hub.Publish(events.Event{Type: "session." + runningStatus, SessionID: sessionID})
	writeJSON(w, http.StatusAccepted, map[string]any{"status": runningStatus, "session_id": sessionID, "message": enqueue.Message})
}

func (s *Server) failGenerationBeforeTurn(sessionID, generationID, owner string, failure error) {
	reason := ""
	if failure != nil {
		reason = failure.Error()
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.store.FailGeneration(ctx, store.FailGenerationParams{
		SessionID:    sessionID,
		GenerationID: generationID,
		Owner:        owner,
		ErrorClass:   runtimeFailureClass(reason),
		Reason:       reason,
		Now:          now,
	}); err != nil {
		s.log.Warn("failed to fail generation before turn start", "session_id", sessionID, "generation_id", generationID, "error", err)
	}
	if err := s.store.UpdateSessionStatusAndActivity(ctx, sessionID, string(sessionstate.Failed), nil, now); err != nil {
		s.log.Warn("failed to mark session failed before turn start", "session_id", sessionID, "generation_id", generationID, "error", err)
	}
	s.hub.Publish(events.Event{Type: "session.error", SessionID: sessionID, Payload: map[string]string{"error": reason}})
	s.hub.Publish(events.Event{Type: "session." + string(sessionstate.Failed), SessionID: sessionID})
}

func (s *Server) ensureActiveGeneration(ctx context.Context, session store.Session, owner string) (store.GenerationAllocation, error) {
	if strings.TrimSpace(session.ActiveGenerationID) != "" {
		return store.GenerationAllocation{
			GenerationID: session.ActiveGenerationID,
			Owner:        owner,
		}, nil
	}
	allocation, err := s.store.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     owner,
		LeaseTTL:  s.cfg.Phase7.Bridge.LeaseTTL.Duration,
		Now:       time.Now().UTC(),
		Config:    s.resourceAllocatorConfig(session.Agent),
	})
	if err != nil {
		return store.GenerationAllocation{}, err
	}
	return allocation, nil
}

func (s *Server) resourceAllocatorConfig(agent string) store.ResourceAllocatorConfig {
	outputFormat := s.cfg.Claude.OutputFormat
	if agent == string(agents.Shell) {
		outputFormat = "shell_pty"
	}
	return store.ResourceAllocatorConfig{
		RunDir:                     s.cfg.Phase7.RunDir,
		CIDRPool:                   s.cfg.Phase7.Network.CIDRPool.Prefix,
		EgressDorisFEHosts:         s.cfg.Phase7.Network.Egress.DorisFEHosts,
		EgressDorisBEHosts:         s.cfg.Phase7.Network.Egress.DorisBEHosts,
		EgressDorisPorts:           s.cfg.Phase7.Network.Egress.DorisPorts,
		EgressDNSPolicy:            string(s.cfg.Phase7.Network.Egress.DNSPolicy),
		HostProxyBindURL:           s.cfg.Claude.ProxyBindURL,
		ProxyPort:                  8082,
		Agent:                      agent,
		AgentModel:                 s.cfg.Claude.Model,
		AgentOutputFormat:          outputFormat,
		DisableNonessentialTraffic: s.cfg.Claude.DisableNonessentialTraffic,
	}
}

func cidrPool30Capacity(prefix netip.Prefix) int {
	if !prefix.IsValid() || !prefix.Addr().Is4() || prefix.Bits() > 30 {
		return 0
	}
	return 1 << uint(30-prefix.Bits())
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

func (s *Server) runtimeStartRequest(session store.Session, generationID string, details store.RuntimeGenerationDetails, artifacts runtime.GenerationArtifacts) runtime.StartRequest {
	return runtime.StartRequest{
		SessionID:         session.ID,
		RestoreID:         session.RestoreID,
		GenerationID:      generationID,
		Agent:             session.Agent,
		WaitForTurn:       false,
		ClaudeSessionUUID: session.ClaudeSessionUUID,
		ResumeClaude:      session.Status != string(sessionstate.Created),
		Generation:        details,
		PreparedArtifacts: artifacts,
	}
}

func runtimeArtifactsFromDetails(details store.RuntimeGenerationDetails) runtime.GenerationArtifacts {
	return runtime.GenerationArtifacts{
		BundleDir:      details.BundleDirPath,
		SpecPath:       details.SpecPath,
		ManifestPath:   details.ControlManifestPath,
		ManifestDigest: details.ControlManifestDigest,
		RunscVersion:   details.RunscVersion,
	}
}

func runtimeFailureClass(message string) string {
	if strings.Contains(message, "pre-start sandbox network probe") {
		return "probe_failed_pre_start"
	}
	if strings.Contains(message, "harness-bridge-client probe") ||
		strings.Contains(message, "bridge probe") ||
		strings.Contains(message, "probe GET /healthz") ||
		strings.Contains(message, "probe POST /v1/messages") {
		return "probe_failed_post_start"
	}
	if strings.Contains(message, "configure sandbox network") {
		return "network_setup_failed"
	}
	return "runtime_failed"
}

func (s *Server) runtimeGenerationDetails(ctx context.Context, sessionID, generationID string) (store.RuntimeGenerationDetails, error) {
	if strings.TrimSpace(generationID) == "" {
		return store.RuntimeGenerationDetails{}, fmt.Errorf("generation id is required")
	}
	details, err := s.store.GetRuntimeGenerationDetails(ctx, sessionID, generationID)
	if err != nil {
		return store.RuntimeGenerationDetails{}, err
	}
	return details, nil
}

func (s *Server) RunPhase7Maintenance(ctx context.Context) error {
	if strings.TrimSpace(s.ownerUUID) == "" {
		return fmt.Errorf("phase7 maintenance requires owner uuid")
	}
	heartbeatInterval := s.cfg.Phase7.Bridge.HeartbeatInterval.Duration
	if heartbeatInterval <= 0 {
		heartbeatInterval = 30 * time.Second
	}
	pollInterval := s.cfg.Phase7.Bridge.PollInterval.Duration
	if pollInterval <= 0 {
		pollInterval = 10 * time.Millisecond
	}
	owner := store.GenerationLeaseOwner(s.ownerUUID)
	processor := &bridge.Processor{
		Store:       bridgeStore(s.store),
		Owner:       owner,
		LeaseTTL:    s.cfg.Phase7.Bridge.LeaseTTL.Duration,
		AfterCommit: s.handleBridgeCommittedEnvelope,
	}
	touchHostHeartbeat := func(generation store.BridgePollGeneration, now time.Time) {
		if err := bridge.TouchHeartbeat(generation.BridgeDirPath, bridge.HostHeartbeatFile, now); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("phase7 bridge host heartbeat failed", "session_id", generation.SessionID, "generation_id", generation.GenerationID, "error", err)
		}
	}

	runMaintenance := func(now time.Time) {
		if _, err := s.store.SweepExpiredSessions(ctx, now); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("phase7 expired-session sweep failed", "error", err)
		}
		if _, err := s.store.RenewLiveGenerationLeases(ctx, store.RenewLiveGenerationsParams{
			Owner:    owner,
			LeaseTTL: s.cfg.Phase7.Bridge.LeaseTTL.Duration,
			Now:      now,
		}); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("phase7 generation lease renewal failed", "error", err)
		}
		generations, err := s.store.ListBridgePollGenerations(ctx, owner, now)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				s.log.Warn("phase7 bridge heartbeat generation list failed", "error", err)
			}
		} else {
			for _, generation := range generations {
				touchHostHeartbeat(generation, now)
			}
		}
		if _, err := s.store.ReapResources(ctx, store.ReaperParams{
			OwnerUUID:       s.ownerUUID,
			FailedRetention: s.cfg.Phase7.Reaper.FailedRetention.Duration,
			Now:             now,
		}); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("phase7 resource reaper failed", "error", err)
		}
	}
	pollBridge := func(now time.Time) {
		generations, err := s.store.ListBridgePollGenerations(ctx, owner, now)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				s.log.Warn("phase7 bridge generation list failed", "error", err)
			}
			return
		}
		for _, generation := range generations {
			if err := processor.ProcessOnce(ctx, generation.BridgeDirPath); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				s.log.Warn("phase7 bridge poll failed", "session_id", generation.SessionID, "generation_id", generation.GenerationID, "error", err)
			}
		}
	}

	runMaintenance(time.Now().UTC())
	pollBridge(time.Now().UTC())
	maintenanceTicker := time.NewTicker(heartbeatInterval)
	defer maintenanceTicker.Stop()
	bridgeTicker := time.NewTicker(pollInterval)
	defer bridgeTicker.Stop()
	for {
		select {
		case now := <-maintenanceTicker.C:
			runMaintenance(now.UTC())
		case now := <-bridgeTicker.C:
			pollBridge(now.UTC())
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Server) handleBridgeCommittedEnvelope(ctx context.Context, envelope bridge.Envelope) {
	switch envelope.Type {
	case bridge.TypeEmitOutput:
		s.handleBridgeOutput(ctx, envelope)
	case bridge.TypeAckTurnCompleted:
		s.handleBridgeCompletion(ctx, envelope)
	}
}

func (s *Server) handleBridgeOutput(ctx context.Context, envelope bridge.Envelope) {
	var payload struct {
		Stream  string `json:"stream"`
		Payload struct {
			Line string `json:"line"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		s.log.Warn("failed to decode bridge output payload", "session_id", envelope.SessionID, "generation_id", envelope.GenerationID, "error", err)
		return
	}
	stream := payload.Stream
	if stream == "" {
		stream = "stdout"
	}
	line := payload.Payload.Line
	if line == "" {
		return
	}
	agent := ""
	if session, err := s.store.GetSession(ctx, envelope.SessionID); err == nil {
		agent = session.Agent
	} else {
		s.log.Warn("failed to load session for bridge output", "session_id", envelope.SessionID, "error", err)
	}
	parser := newStreamParser(s, envelope.SessionID, agent)
	if envelope.TurnID != nil {
		parser.turnID = *envelope.TurnID
	}
	parser.handle(runtime.Output{Stream: stream, Line: line})
}

func (s *Server) handleBridgeCompletion(ctx context.Context, envelope bridge.Envelope) {
	var payload struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		s.log.Warn("failed to decode bridge completion payload", "session_id", envelope.SessionID, "generation_id", envelope.GenerationID, "error", err)
		return
	}
	status := string(sessionstate.RunningIdle)
	if payload.Status == "failed" || payload.Status == "canceled" {
		status = string(sessionstate.Failed)
		if payload.Error != "" {
			s.hub.Publish(events.Event{Type: "session.error", SessionID: envelope.SessionID, Payload: map[string]string{"error": payload.Error}})
		}
	}
	if err := s.store.UpdateSessionStatusAndActivity(ctx, envelope.SessionID, status, nil, time.Now().UTC()); err != nil {
		if !errors.Is(err, context.Canceled) {
			s.log.Warn("failed to update bridge-completed session status", "session_id", envelope.SessionID, "generation_id", envelope.GenerationID, "error", err)
		}
		return
	}
	if err := s.watcher.ScanSession(ctx, envelope.SessionID); err != nil && !errors.Is(err, context.Canceled) {
		s.log.Warn("failed to scan bridge-completed session artifacts", "session_id", envelope.SessionID, "error", err)
	}
	s.hub.Publish(events.Event{Type: "session." + status, SessionID: envelope.SessionID})
}

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
	if session.Agent != string(agents.Shell) {
		writeError(w, http.StatusConflict, "interrupt is only supported for shell sessions")
		return
	}
	if err := s.runtime.Interrupt(sessionID); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "interrupting"})
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
	if len(parts) != 2 {
		writeError(w, http.StatusBadRequest, "invalid artifact path")
		return
	}
	file, info, status, message := s.openArtifactFile(parts[0], parts[1])
	if file == nil {
		writeError(w, status, message)
		return
	}
	defer file.Close()

	w.Header().Set("X-Content-Type-Options", "nosniff")
	http.ServeContent(w, r, info.Name(), info.ModTime(), file)
}

func (s *Server) openArtifactFile(sessionID, artifactPath string) (*os.File, os.FileInfo, int, string) {
	if !safePathSegment(sessionID) || artifactPath == "" || strings.Contains(artifactPath, `\`) {
		return nil, nil, http.StatusBadRequest, "invalid artifact path"
	}
	for _, segment := range strings.Split(artifactPath, "/") {
		if !safePathSegment(segment) {
			return nil, nil, http.StatusBadRequest, "invalid artifact path"
		}
	}

	cleanPath := pathpkg.Clean(artifactPath)
	if cleanPath == "." || strings.HasPrefix(cleanPath, "../") || cleanPath == ".." {
		return nil, nil, http.StatusBadRequest, "invalid artifact path"
	}

	sessionRoot := filepath.Join(s.cfg.SessionsRoot, sessionID)
	fullPath := filepath.Join(sessionRoot, filepath.FromSlash(cleanPath))
	if !isPathInside(sessionRoot, fullPath) {
		return nil, nil, http.StatusBadRequest, "invalid artifact path"
	}

	realSessionRoot, err := filepath.EvalSymlinks(sessionRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, http.StatusNotFound, "artifact not found"
		}
		return nil, nil, http.StatusInternalServerError, err.Error()
	}
	if status, message := rejectSymlinkComponents(sessionRoot, cleanPath); status != 0 {
		return nil, nil, status, message
	}
	realPath, err := filepath.EvalSymlinks(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, http.StatusNotFound, "artifact not found"
		}
		return nil, nil, http.StatusInternalServerError, err.Error()
	}
	if !isPathInside(realSessionRoot, realPath) {
		return nil, nil, http.StatusForbidden, "artifact path escapes session workspace"
	}

	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, http.StatusNotFound, "artifact not found"
		}
		return nil, nil, http.StatusInternalServerError, err.Error()
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, http.StatusInternalServerError, err.Error()
	}
	if info.IsDir() || !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, nil, http.StatusForbidden, "artifact is not a regular file"
	}
	return file, info, 0, ""
}

func safePathSegment(segment string) bool {
	return segment != "" && segment != "." && segment != ".." && !strings.Contains(segment, "/")
}

func rejectSymlinkComponents(root, slashPath string) (int, string) {
	current := root
	for _, segment := range strings.Split(slashPath, "/") {
		current = filepath.Join(current, segment)
		info, err := os.Lstat(current)
		if err != nil {
			if os.IsNotExist(err) {
				return http.StatusNotFound, "artifact not found"
			}
			return http.StatusInternalServerError, err.Error()
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return http.StatusForbidden, "artifact path uses a symlink"
		}
	}
	return 0, ""
}

func isPathInside(root, candidate string) bool {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(candidate))
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
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

func writeErrorClass(w http.ResponseWriter, status int, class, message string) {
	writeJSON(w, status, map[string]string{"error_class": class, "error": message})
}

func (s *Server) MonitorIdleSessions(ctx context.Context) error {
	if strings.EqualFold(strings.TrimSpace(s.cfg.RunscNetwork), "host") {
		s.log.Info("idle checkpoint monitor disabled because runsc host network is not checkpointable")
		return nil
	}

	if err := s.reconcileCheckpointingSessions(ctx); err != nil {
		s.log.Warn("failed to reconcile checkpointing sessions", "error", err)
	}
	if err := s.reconcileCheckpointedSessions(ctx); err != nil {
		s.log.Warn("failed to reconcile checkpointed sessions", "error", err)
	}
	if !autoCheckpointEnabled {
		s.log.Info("idle checkpoint monitor disabled because runsc restore cannot reconnect agent stdin")
		return nil
	}

	ticker := time.NewTicker(idleCheckpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.reconcileCheckpointingSessions(ctx); err != nil {
				s.log.Warn("failed to reconcile checkpointing sessions", "error", err)
			}
			if err := s.reconcileCheckpointedSessions(ctx); err != nil {
				s.log.Warn("failed to reconcile checkpointed sessions", "error", err)
			}
			sessions, err := s.store.ListSessionsByStatus(ctx, string(sessionstate.RunningIdle))
			if err != nil {
				s.log.Warn("failed to list idle sessions", "error", err)
				continue
			}
			for _, session := range sessions {
				if session.LastActivityAt != nil && time.Since(*session.LastActivityAt) > idleCheckpointThreshold {
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

	checkpointCtx, cancel := context.WithTimeout(ctx, checkpointTimeout)
	defer cancel()
	if err := s.runtime.Checkpoint(checkpointCtx, session.ID); err != nil {
		s.log.Warn("checkpoint failed", "session_id", session.ID, "error", err)
		if updateErr := s.store.UpdateSessionStatus(ctx, session.ID, string(sessionstate.RunningIdle), nil); updateErr != nil {
			s.log.Warn("failed to revert session status after checkpoint error", "session_id", session.ID, "error", updateErr)
		} else {
			s.hub.Publish(events.Event{Type: "session." + string(sessionstate.RunningIdle), SessionID: session.ID, Payload: map[string]string{"checkpoint_error": err.Error()}})
		}
		return
	}

	if err := s.store.UpdateSessionStatus(ctx, session.ID, string(sessionstate.Checkpointed), nil); err != nil {
		s.log.Warn("failed to update session status to checkpointed", "session_id", session.ID, "error", err)
		return
	}

	s.hub.Publish(events.Event{Type: "session." + string(sessionstate.Checkpointed), SessionID: session.ID})
}

func (s *Server) reconcileCheckpointingSessions(ctx context.Context) error {
	sessions, err := s.store.ListSessionsByStatus(ctx, string(sessionstate.Checkpointing))
	if err != nil {
		return err
	}
	for _, session := range sessions {
		checkpointPath := filepath.Join(s.cfg.CheckpointsRoot, session.ID)
		if hasCheckpointImage(checkpointPath) {
			if err := s.store.UpdateSessionStatus(ctx, session.ID, string(sessionstate.Checkpointed), nil); err != nil {
				s.log.Warn("failed to mark recovered checkpointed session", "session_id", session.ID, "error", err)
				continue
			}
			s.hub.Publish(events.Event{Type: "session." + string(sessionstate.Checkpointed), SessionID: session.ID, Payload: map[string]string{"recovered": "true"}})
			continue
		}
		if time.Since(session.UpdatedAt) < checkpointTimeout {
			continue
		}
		if err := s.store.UpdateSessionStatus(ctx, session.ID, string(sessionstate.RunningIdle), nil); err != nil {
			s.log.Warn("failed to revert stale checkpointing session", "session_id", session.ID, "error", err)
			continue
		}
		s.hub.Publish(events.Event{Type: "session." + string(sessionstate.RunningIdle), SessionID: session.ID, Payload: map[string]string{"checkpoint_recovered": "false"}})
	}
	return nil
}

func (s *Server) reconcileCheckpointedSessions(ctx context.Context) error {
	sessions, err := s.store.ListSessionsByStatus(ctx, string(sessionstate.Checkpointed))
	if err != nil {
		return err
	}
	for _, session := range sessions {
		if err := s.store.UpdateSessionStatus(ctx, session.ID, string(sessionstate.RunningIdle), nil); err != nil {
			s.log.Warn("failed to re-enable checkpointed session", "session_id", session.ID, "error", err)
			continue
		}
		s.hub.Publish(events.Event{Type: "session." + string(sessionstate.RunningIdle), SessionID: session.ID, Payload: map[string]string{"checkpoint_recovered": "disabled"}})
	}
	return nil
}

func hasCheckpointImage(path string) bool {
	required := []string{"checkpoint.img", "pages.img", "pages_meta.img"}
	for _, name := range required {
		info, err := os.Stat(filepath.Join(path, name))
		if err != nil || info.IsDir() || info.Size() == 0 {
			return false
		}
	}
	return true
}
