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
	"net"
	"net/http"
	"net/netip"
	"os"
	pathpkg "path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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
)

var errGenerationBusy = errors.New("generation lifecycle is busy")

type Server struct {
	cfg       config.Config
	store     *store.Store
	runtime   runtimeDriver
	watcher   *artifacts.Watcher
	hub       *events.Hub
	log       *slog.Logger
	upgrader  websocket.Upgrader
	ownerUUID string

	bridgeParserMu sync.Mutex
	bridgeParsers  map[bridgeStreamParserKey]*streamParser
}

type runtimeDriver interface {
	Start(context.Context, runtime.StartRequest, func(runtime.Output)) runtime.Result
	PrepareGeneration(context.Context, runtime.StartRequest) (runtime.GenerationArtifacts, error)
	Destroy(context.Context, string) error
	DestroyGenerationResources(context.Context, store.RuntimeGenerationDetails) (runtime.GenerationResourceCleanup, error)
	Interrupt(string) error
	Checkpoint(context.Context, runtime.CheckpointRequest) error
}

type bridgeStore interface {
	bridge.Store
	ListBridgePollGenerations(context.Context, string, time.Time, time.Duration) ([]store.BridgePollGeneration, error)
}

type ensuredGeneration struct {
	Allocation            store.GenerationAllocation
	IsNew                 bool
	RestoreFromCheckpoint bool
}

type bridgeStreamParserKey struct {
	SessionID    string
	GenerationID string
	TurnID       int64
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
	mux.HandleFunc("POST /internal/proxy/requests/start", s.internalProxyRequestStart)
	mux.HandleFunc("POST /internal/proxy/requests/finish", s.internalProxyRequestFinish)
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
		ID:                    id,
		UserID:                labUserID,
		Status:                string(sessionstate.Created),
		Agent:                 req.Agent,
		Workspace:             filepath.Join(s.cfg.SessionsRoot, id),
		RestoreID:             "phase3-" + id,
		ClaudeSessionUUID:     uuid.NewString(),
		AutoCheckpointEnabled: s.cfg.Phase7.Checkpoint.AutoEnabled,
		CreatedAt:             now,
		UpdatedAt:             now,
		ExpiresAt:             &expiresAt,
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
	ensured, err := s.ensureActiveGeneration(r.Context(), session, leaseOwner)
	if errors.Is(err, store.ErrPoolExhausted) {
		writeErrorClass(w, http.StatusServiceUnavailable, "pool_exhausted", "resource pool exhausted")
		return
	}
	if errors.Is(err, errGenerationBusy) {
		writeError(w, http.StatusConflict, "session is busy")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := s.startEnsuredGeneration(r.Context(), session, ensured); err != nil {
		if !ensured.RestoreFromCheckpoint {
			writeRuntimeStartError(w, err)
			return
		}
		ensured, err = s.ensureActiveGeneration(r.Context(), session, leaseOwner)
		if errors.Is(err, store.ErrPoolExhausted) {
			writeErrorClass(w, http.StatusServiceUnavailable, "pool_exhausted", "resource pool exhausted")
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if !ensured.IsNew {
			writeError(w, http.StatusInternalServerError, "restore fallback did not allocate a replacement generation")
			return
		}
		if err := s.startEnsuredGeneration(r.Context(), session, ensured); err != nil {
			writeRuntimeStartError(w, err)
			return
		}
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

func (s *Server) startEnsuredGeneration(ctx context.Context, session store.Session, ensured ensuredGeneration) error {
	allocation := ensured.Allocation
	generationDetails, err := s.runtimeGenerationDetails(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		return err
	}
	preparedArtifacts := runtimeArtifactsFromDetails(generationDetails)
	if ensured.IsNew {
		preparedArtifacts, err = s.runtime.PrepareGeneration(ctx, s.runtimeStartRequest(session, allocation.GenerationID, generationDetails, runtime.GenerationArtifacts{}))
		if err != nil {
			s.failGenerationBeforeTurn(session.ID, allocation.GenerationID, allocation.Owner, err)
			return err
		}
		if err := s.store.RecordGenerationRuntimeArtifactDigests(ctx, allocation.GenerationID, runtimeArtifactDigests(preparedArtifacts)); err != nil {
			s.failGenerationBeforeTurn(session.ID, allocation.GenerationID, allocation.Owner, err)
			return err
		}
		if err := s.store.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
			s.failGenerationBeforeTurn(session.ID, allocation.GenerationID, allocation.Owner, err)
			return err
		}
	}
	startReq := s.runtimeStartRequest(session, allocation.GenerationID, generationDetails, preparedArtifacts)
	startReq.RestoreFromCheckpoint = ensured.RestoreFromCheckpoint
	result := s.runtime.Start(ctx, startReq, nil)
	if result.Err != nil {
		if ensured.RestoreFromCheckpoint {
			s.failGenerationForRestoreFallback(session.ID, allocation.GenerationID, allocation.Owner, result.Err)
			return result.Err
		}
		s.failGenerationBeforeTurn(session.ID, allocation.GenerationID, allocation.Owner, result.Err)
		return result.Err
	}
	if ensured.RestoreFromCheckpoint {
		if err := s.store.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
			if destroyErr := s.runtime.Destroy(ctx, session.ID); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after restore live CAS failure", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
			}
			s.failGenerationForRestoreFallback(session.ID, allocation.GenerationID, allocation.Owner, err)
			return err
		}
	}
	return nil
}

func (s *Server) failGenerationBeforeTurn(sessionID, generationID, owner string, failure error) {
	reason := ""
	if failure != nil {
		reason = failure.Error()
	}
	errorClass := runtimeFailureClass(reason)
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.store.FailGeneration(ctx, store.FailGenerationParams{
		SessionID:    sessionID,
		GenerationID: generationID,
		Owner:        owner,
		ErrorClass:   errorClass,
		Reason:       reason,
		Now:          now,
	}); err != nil {
		s.log.Warn("failed to fail generation before turn start", "session_id", sessionID, "generation_id", generationID, "error", err)
	}
	if err := s.store.FailSession(ctx, store.FailSessionParams{
		SessionID:    sessionID,
		ErrorClass:   errorClass,
		Reason:       reason,
		LastActivity: now,
		Now:          now,
	}); err != nil {
		s.log.Warn("failed to mark session failed before turn start", "session_id", sessionID, "generation_id", generationID, "error", err)
	}
	s.hub.Publish(events.Event{Type: "session.error", SessionID: sessionID, Payload: map[string]string{
		"error_class": errorClass,
		"error":       runtimeFailureMessage(errorClass, reason),
	}})
	s.hub.Publish(events.Event{Type: "session." + string(sessionstate.Failed), SessionID: sessionID})
}

func (s *Server) failGenerationForRestoreFallback(sessionID, generationID, owner string, failure error) {
	reason := ""
	if failure != nil {
		reason = failure.Error()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.store.FailGeneration(ctx, store.FailGenerationParams{
		SessionID:    sessionID,
		GenerationID: generationID,
		Owner:        owner,
		ErrorClass:   runtimeFailureClass(reason),
		Reason:       reason,
		Now:          time.Now().UTC(),
	}); err != nil {
		s.log.Warn("failed to fail generation before restore fallback", "session_id", sessionID, "generation_id", generationID, "error", err)
	}
	s.hub.Publish(events.Event{Type: "session.error", SessionID: sessionID, Payload: map[string]string{"error": reason, "fallback": "cold"}})
}

func (s *Server) ensureActiveGeneration(ctx context.Context, session store.Session, owner string) (ensuredGeneration, error) {
	activeGenerationID := strings.TrimSpace(session.ActiveGenerationID)
	if activeGenerationID != "" {
		status, err := s.store.GetRuntimeGenerationStatus(ctx, session.ID, activeGenerationID)
		if err != nil {
			return ensuredGeneration{}, err
		}
		if status == "checkpointed" {
			allocation, err := s.store.ClaimCheckpointedGenerationForRestore(ctx, store.ClaimCheckpointedGenerationParams{
				SessionID:    session.ID,
				GenerationID: activeGenerationID,
				Owner:        owner,
				LeaseTTL:     s.cfg.Phase7.Bridge.LeaseTTL.Duration,
				Now:          time.Now().UTC(),
			})
			if err != nil {
				return ensuredGeneration{}, err
			}
			return ensuredGeneration{
				Allocation:            allocation,
				RestoreFromCheckpoint: true,
			}, nil
		}
		if generationLifecycleBusy(status) {
			return ensuredGeneration{}, errGenerationBusy
		}
		if status != "failed" {
			return ensuredGeneration{
				Allocation: store.GenerationAllocation{
					GenerationID: activeGenerationID,
					Owner:        owner,
				},
				IsNew: false,
			}, nil
		}
		allocation, err := s.store.AllocateGeneration(ctx, store.AllocateGenerationParams{
			SessionID:            session.ID,
			ExpectedGenerationID: sql.NullString{String: activeGenerationID, Valid: true},
			Owner:                owner,
			LeaseTTL:             s.cfg.Phase7.Bridge.LeaseTTL.Duration,
			Now:                  time.Now().UTC(),
			Config:               s.resourceAllocatorConfig(session.Agent),
		})
		if err != nil {
			return ensuredGeneration{}, err
		}
		return ensuredGeneration{Allocation: allocation, IsNew: true}, nil
	}
	allocation, err := s.store.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     owner,
		LeaseTTL:  s.cfg.Phase7.Bridge.LeaseTTL.Duration,
		Now:       time.Now().UTC(),
		Config:    s.resourceAllocatorConfig(session.Agent),
	})
	if err != nil {
		return ensuredGeneration{}, err
	}
	return ensuredGeneration{Allocation: allocation, IsNew: true}, nil
}

func generationLifecycleBusy(status string) bool {
	switch status {
	case "allocating", "starting", "probing", "checkpointing", "restoring":
		return true
	default:
		return false
	}
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
		BundleDir:               details.BundleDirPath,
		SpecPath:                details.SpecPath,
		ManifestPath:            details.ControlManifestPath,
		ManifestDigest:          details.ControlManifestDigest,
		ProjectedManifestDigest: details.ProjectedControlManifestDigest,
		BundleDigest:            details.BundleDigest,
		RuntimeConfigDigest:     details.RuntimeConfigDigest,
		SpecDigest:              details.SpecDigest,
		RunscVersion:            details.RunscVersion,
	}
}

func runtimeArtifactDigests(artifacts runtime.GenerationArtifacts) store.GenerationRuntimeArtifactDigests {
	return store.GenerationRuntimeArtifactDigests{
		ControlManifestDigest:          artifacts.ManifestDigest,
		ProjectedControlManifestDigest: artifacts.ProjectedManifestDigest,
		BundleDigest:                   artifacts.BundleDigest,
		RuntimeConfigDigest:            artifacts.RuntimeConfigDigest,
		SpecDigest:                     artifacts.SpecDigest,
		RunscVersion:                   artifacts.RunscVersion,
	}
}

func runtimeFailureClass(message string) string {
	if strings.Contains(message, "shell_secret_disallowed") {
		return "shell_secret_disallowed"
	}
	if strings.Contains(message, "control manifest digest mismatch") ||
		strings.Contains(message, "expected manifest_") ||
		strings.Contains(message, "expected session_id") ||
		strings.Contains(message, "expected generation_id") ||
		strings.Contains(message, "expected network_profile_id") ||
		strings.Contains(message, "expected agent_runtime_profile_id") ||
		strings.Contains(message, "expected anthropic_api_key_secret_id") ||
		strings.Contains(message, "expected anthropic_auth_token_secret_id") ||
		strings.Contains(message, "expected secret_version") ||
		strings.Contains(message, "secret mount") {
		return "manifest_digest_mismatch"
	}
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

func runtimeFailureMessage(errorClass, reason string) string {
	switch errorClass {
	case "probe_failed_pre_start":
		return "sandbox network probe failed before start"
	case "probe_failed_post_start":
		return "sandbox network probe failed after start"
	case "manifest_digest_mismatch":
		return "runtime manifest validation failed"
	case "network_setup_failed":
		return "sandbox network setup failed"
	case "shell_secret_disallowed":
		return "shell agent cannot mount model secrets"
	default:
		if strings.TrimSpace(reason) != "" {
			return reason
		}
		return "runtime failed"
	}
}

func writeRuntimeStartError(w http.ResponseWriter, err error) {
	reason := ""
	if err != nil {
		reason = err.Error()
	}
	errorClass := runtimeFailureClass(reason)
	writeErrorClass(w, http.StatusInternalServerError, errorClass, runtimeFailureMessage(errorClass, reason))
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

func (s *Server) startColdFallbackSessions(ctx context.Context, owner string) {
	fallbacks, err := s.store.ListColdFallbackSessions(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			s.log.Warn("phase7 cold fallback session list failed", "error", err)
		}
		return
	}
	for _, fallback := range fallbacks {
		ensured, err := s.ensureActiveGeneration(ctx, fallback.Session, owner)
		if err != nil {
			if errors.Is(err, store.ErrPoolExhausted) {
				s.log.Warn("phase7 cold fallback pool exhausted", "session_id", fallback.Session.ID, "old_generation_id", fallback.OldGeneration, "queued_turns", fallback.QueuedTurns)
				return
			}
			if !errors.Is(err, context.Canceled) {
				s.log.Warn("phase7 cold fallback allocation failed", "session_id", fallback.Session.ID, "old_generation_id", fallback.OldGeneration, "error", err)
			}
			continue
		}
		if !ensured.IsNew {
			continue
		}
		if err := s.startEnsuredGeneration(ctx, fallback.Session, ensured); err != nil {
			if !errors.Is(err, context.Canceled) {
				s.log.Warn("phase7 cold fallback start failed", "session_id", fallback.Session.ID, "old_generation_id", fallback.OldGeneration, "new_generation_id", ensured.Allocation.GenerationID, "error", err)
			}
			continue
		}
		if err := s.store.UpdateSessionStatusAndActivity(ctx, fallback.Session.ID, string(sessionstate.RunningActive), nil, time.Now().UTC()); err != nil {
			if !errors.Is(err, context.Canceled) {
				s.log.Warn("phase7 cold fallback status update failed", "session_id", fallback.Session.ID, "new_generation_id", ensured.Allocation.GenerationID, "error", err)
			}
			continue
		}
		s.hub.Publish(events.Event{Type: "session." + string(sessionstate.RunningActive), SessionID: fallback.Session.ID})
	}
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
		Store:           bridgeStore(s.store),
		Owner:           owner,
		LeaseTTL:        s.cfg.Phase7.Bridge.LeaseTTL.Duration,
		AckStartedGrace: s.cfg.Phase7.Bridge.AckStartedGrace.Duration,
		AfterCommit:     s.handleBridgeCommittedEnvelope,
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
		if _, err := s.store.CancelTerminalSessionPendingTurns(ctx, now); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("phase7 terminal-session turn cleanup failed", "error", err)
		}
		if _, err := s.store.RecoverAllocations(ctx, store.StartupRecoveryParams{
			OwnerUUID:       s.ownerUUID,
			Now:             now,
			LeaseTTL:        s.cfg.Phase7.Bridge.LeaseTTL.Duration,
			ReconnectGrace:  s.cfg.Phase7.Bridge.ReconnectGrace.Duration,
			AckStartedGrace: s.cfg.Phase7.Bridge.AckStartedGrace.Duration,
		}); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("phase7 allocation recovery failed", "error", err)
		}
		if _, err := s.store.RenewLiveGenerationLeases(ctx, store.RenewLiveGenerationsParams{
			Owner:    owner,
			LeaseTTL: s.cfg.Phase7.Bridge.LeaseTTL.Duration,
			Now:      now,
		}); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("phase7 generation lease renewal failed", "error", err)
		}
		s.startColdFallbackSessions(ctx, owner)
		generations, err := s.store.ListBridgePollGenerations(ctx, owner, now, s.cfg.Phase7.Bridge.AckStartedGrace.Duration)
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
		s.destroyReclaimableGenerationResources(ctx, now)
		if _, err := s.store.PruneEvents(ctx, store.PruneEventsParams{
			RetentionWindow: s.cfg.Phase7.Events.RetentionWindow.Duration,
			RetentionRows:   s.cfg.Phase7.Events.RetentionRows,
			Now:             now,
		}); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("phase7 event retention prune failed", "error", err)
		}
	}
	pollBridge := func(now time.Time) {
		generations, err := s.store.ListBridgePollGenerations(ctx, owner, now, s.cfg.Phase7.Bridge.AckStartedGrace.Duration)
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

func (s *Server) destroyReclaimableGenerationResources(ctx context.Context, now time.Time) {
	candidates, err := s.store.ListDestroyableReclaimableGenerations(ctx, now, s.cfg.Phase7.Reaper.FailedRetention.Duration)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			s.log.Warn("phase7 destroyable resource list failed", "error", err)
		}
		return
	}
	for _, candidate := range candidates {
		details, err := s.store.GetRuntimeGenerationDetails(ctx, candidate.SessionID, candidate.GenerationID)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				s.log.Warn("phase7 destroyable resource lookup failed", "session_id", candidate.SessionID, "generation_id", candidate.GenerationID, "error", err)
			}
			continue
		}
		if _, err := s.runtime.DestroyGenerationResources(ctx, details); err != nil {
			if !errors.Is(err, context.Canceled) {
				s.log.Warn("phase7 generation resource cleanup failed", "session_id", candidate.SessionID, "generation_id", candidate.GenerationID, "error", err)
			}
			continue
		}
		if err := s.store.MarkGenerationResourcesDestroyed(ctx, store.DestroyGenerationResourcesParams{
			SessionID:    candidate.SessionID,
			GenerationID: candidate.GenerationID,
			Now:          now,
		}); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("phase7 generation resource destroy mark failed", "session_id", candidate.SessionID, "generation_id", candidate.GenerationID, "error", err)
		}
	}
}

func (s *Server) handleBridgeCommittedEnvelope(ctx context.Context, envelope bridge.Envelope, eventID int64) {
	s.publishDurableEvent(ctx, eventID)
	switch envelope.Type {
	case bridge.TypeEmitOutput:
		s.handleBridgeOutput(ctx, envelope)
	case bridge.TypeAckTurnCompleted:
		s.handleBridgeCompletion(ctx, envelope)
	}
}

func (s *Server) publishDurableEvent(ctx context.Context, eventID int64) {
	if eventID == 0 {
		return
	}
	record, ok, err := s.store.GetEvent(ctx, eventID)
	if err != nil {
		s.log.Warn("failed to load durable event", "event_id", eventID, "error", err)
		return
	}
	if !ok {
		return
	}
	s.hub.Publish(eventFromRecord(record))
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
	parser := s.bridgeStreamParser(envelope, agent)
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
	s.completeBridgeStreamParser(envelope)
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

func (s *Server) bridgeStreamParser(envelope bridge.Envelope, agent string) *streamParser {
	key, ok := bridgeParserKey(envelope)
	if !ok {
		return newStreamParser(s, envelope.SessionID, agent)
	}
	s.bridgeParserMu.Lock()
	defer s.bridgeParserMu.Unlock()
	if s.bridgeParsers == nil {
		s.bridgeParsers = make(map[bridgeStreamParserKey]*streamParser)
	}
	parser := s.bridgeParsers[key]
	if parser == nil {
		parser = newStreamParser(s, envelope.SessionID, agent)
		parser.turnID = key.TurnID
		s.bridgeParsers[key] = parser
	}
	return parser
}

func (s *Server) completeBridgeStreamParser(envelope bridge.Envelope) {
	key, ok := bridgeParserKey(envelope)
	if !ok {
		return
	}
	s.bridgeParserMu.Lock()
	parser := s.bridgeParsers[key]
	delete(s.bridgeParsers, key)
	s.bridgeParserMu.Unlock()
	if parser == nil {
		return
	}
	parser.flush()
	parser.complete()
}

func bridgeParserKey(envelope bridge.Envelope) (bridgeStreamParserKey, bool) {
	if envelope.TurnID == nil {
		return bridgeStreamParserKey{}, false
	}
	return bridgeStreamParserKey{
		SessionID:    envelope.SessionID,
		GenerationID: envelope.GenerationID,
		TurnID:       *envelope.TurnID,
	}, true
}

func (s *Server) internalProxyRequestStart(w http.ResponseWriter, r *http.Request) {
	if !isLoopbackRemoteAddr(r.RemoteAddr) {
		writeError(w, http.StatusForbidden, "internal proxy endpoint is localhost-only")
		return
	}
	var req struct {
		SandboxSourceIP string `json:"sandbox_source_ip"`
		ProxyRequestID  string `json:"proxy_request_id"`
		UpstreamModel   string `json:"upstream_model"`
		UpstreamBaseURL string `json:"upstream_base_url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	result, err := s.store.StartProxyRequest(r.Context(), store.StartProxyRequestParams{
		SandboxSourceIP: req.SandboxSourceIP,
		ProxyRequestID:  req.ProxyRequestID,
		UpstreamModel:   req.UpstreamModel,
		UpstreamBaseURL: req.UpstreamBaseURL,
		Now:             time.Now().UTC(),
	})
	if errors.Is(err, store.ErrProxyContextUnavailable) {
		writeErrorClass(w, http.StatusNotFound, "active_context_unavailable", "proxy active context unavailable")
		return
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !result.Replayed {
		s.publishDurableEvent(r.Context(), result.EventID)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"session_id":       result.SessionID,
		"turn_id":          result.TurnID,
		"generation_id":    result.GenerationID,
		"request_sequence": result.RequestSequence,
		"event_id":         result.EventID,
		"replayed":         result.Replayed,
	})
}

func (s *Server) internalProxyRequestFinish(w http.ResponseWriter, r *http.Request) {
	if !isLoopbackRemoteAddr(r.RemoteAddr) {
		writeError(w, http.StatusForbidden, "internal proxy endpoint is localhost-only")
		return
	}
	var req struct {
		ProxyRequestID             string `json:"proxy_request_id"`
		ProxyConnectLatencyMS      *int64 `json:"proxy_connect_latency_ms"`
		UpstreamFirstByteLatencyMS *int64 `json:"upstream_first_byte_latency_ms"`
		UpstreamTotalLatencyMS     *int64 `json:"upstream_total_latency_ms"`
		RetryCount                 *int64 `json:"retry_count"`
		TimeoutKind                string `json:"timeout_kind"`
		HTTPStatus                 *int64 `json:"http_status"`
		ErrorClass                 string `json:"error_class"`
		Error                      string `json:"error"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	result, err := s.store.FinishProxyRequest(r.Context(), store.FinishProxyRequestParams{
		ProxyRequestID:             req.ProxyRequestID,
		ProxyConnectLatencyMS:      req.ProxyConnectLatencyMS,
		UpstreamFirstByteLatencyMS: req.UpstreamFirstByteLatencyMS,
		UpstreamTotalLatencyMS:     req.UpstreamTotalLatencyMS,
		RetryCount:                 req.RetryCount,
		TimeoutKind:                req.TimeoutKind,
		HTTPStatus:                 req.HTTPStatus,
		ErrorClass:                 req.ErrorClass,
		Error:                      req.Error,
		Now:                        time.Now().UTC(),
	})
	if errors.Is(err, store.ErrProxyRequestUnknown) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "stale_unknown_request"})
		return
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !result.Replayed {
		s.publishDurableEvent(r.Context(), result.EventID)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":        "accepted",
		"event_id":      result.EventID,
		"event_type":    result.EventType,
		"session_id":    result.SessionID,
		"turn_id":       result.TurnID,
		"generation_id": result.GenerationID,
		"replayed":      result.Replayed,
	})
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
	if err := s.store.DestroySession(r.Context(), sessionID, time.Now().UTC()); err != nil {
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

func isLoopbackRemoteAddr(remoteAddr string) bool {
	if addrPort, err := netip.ParseAddrPort(remoteAddr); err == nil {
		return addrPort.Addr().IsLoopback()
	}
	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil {
		remoteAddr = host
	}
	addr, err := netip.ParseAddr(remoteAddr)
	return err == nil && addr.IsLoopback()
}

func (s *Server) MonitorIdleSessions(ctx context.Context) error {
	if strings.EqualFold(strings.TrimSpace(s.cfg.RunscNetwork), "host") {
		s.log.Info("idle checkpoint monitor disabled because runsc host network is not checkpointable")
		return nil
	}
	if !s.cfg.Phase7.Checkpoint.AutoEnabled {
		s.log.Info("idle checkpoint monitor disabled by policy")
		return nil
	}
	if strings.TrimSpace(s.ownerUUID) == "" {
		return fmt.Errorf("idle checkpoint monitor requires owner uuid")
	}

	owner := store.GenerationLeaseOwner(s.ownerUUID)
	interval := s.cfg.Phase7.Checkpoint.MonitorInterval.Duration
	if interval <= 0 {
		interval = idleCheckpointInterval
	}
	idleThreshold := s.cfg.Phase7.Checkpoint.IdleThreshold.Duration
	if idleThreshold < 0 {
		idleThreshold = idleCheckpointThreshold
	}
	heartbeatInterval := s.cfg.Phase7.Bridge.HeartbeatInterval.Duration
	if heartbeatInterval <= 0 {
		heartbeatInterval = 30 * time.Second
	}
	tick := func(now time.Time) {
		candidates, err := s.store.ListAutoCheckpointCandidates(ctx, owner, now, idleThreshold)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				s.log.Warn("failed to list auto checkpoint candidates", "error", err)
			}
			return
		}
		for _, candidate := range candidates {
			if !bridgeCheckpointReady(candidate.BridgeDirPath, now, heartbeatInterval) {
				continue
			}
			if err := s.checkpointGeneration(ctx, candidate, owner, now); err != nil && !errors.Is(err, context.Canceled) {
				s.log.Warn("auto checkpoint failed", "session_id", candidate.SessionID, "generation_id", candidate.GenerationID, "error", err)
			}
		}
	}

	tick(time.Now().UTC())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			tick(now.UTC())
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Server) checkpointGeneration(ctx context.Context, candidate store.CheckpointCandidate, owner string, now time.Time) error {
	if err := s.store.BeginGenerationCheckpoint(ctx, candidate.SessionID, candidate.GenerationID, owner, now); err != nil {
		return err
	}
	details, err := s.store.GetRuntimeGenerationDetails(ctx, candidate.SessionID, candidate.GenerationID)
	if err != nil {
		abortNow := time.Now().UTC()
		if abortErr := s.store.AbortGenerationCheckpoint(ctx, candidate.SessionID, candidate.GenerationID, owner, abortNow); abortErr != nil {
			s.log.Warn("failed to abort generation checkpoint after metadata load failure", "session_id", candidate.SessionID, "generation_id", candidate.GenerationID, "error", abortErr)
		}
		return err
	}
	checkpointCtx, cancel := context.WithTimeout(ctx, checkpointTimeout)
	defer cancel()
	err = s.runtime.Checkpoint(checkpointCtx, runtime.CheckpointRequest{
		SessionID:      candidate.SessionID,
		GenerationID:   candidate.GenerationID,
		CheckpointPath: details.CheckpointPath,
	})
	if err != nil {
		abortNow := time.Now().UTC()
		if abortErr := s.store.AbortGenerationCheckpoint(ctx, candidate.SessionID, candidate.GenerationID, owner, abortNow); abortErr != nil {
			s.log.Warn("failed to abort generation checkpoint", "session_id", candidate.SessionID, "generation_id", candidate.GenerationID, "error", abortErr)
		} else {
			s.hub.Publish(events.Event{Type: "session." + string(sessionstate.RunningIdle), SessionID: candidate.SessionID, GenerationID: candidate.GenerationID, Payload: map[string]string{"checkpoint_error": err.Error()}})
		}
		return err
	}
	completeNow := time.Now().UTC()
	if err := s.store.CompleteGenerationCheckpoint(ctx, store.CompleteCheckpointParams{
		SessionID:                       candidate.SessionID,
		GenerationID:                    candidate.GenerationID,
		Owner:                           owner,
		CheckpointPath:                  details.CheckpointPath,
		RunscPlatform:                   details.RunscPlatform,
		RunscVersion:                    details.RunscVersion,
		CheckpointBundleDigest:          details.BundleDigest,
		CheckpointRuntimeConfigDigest:   details.RuntimeConfigDigest,
		CheckpointControlManifestDigest: details.ProjectedControlManifestDigest,
		Now:                             completeNow,
	}); err != nil {
		return err
	}
	s.hub.Publish(events.Event{Type: "session." + string(sessionstate.Checkpointed), SessionID: candidate.SessionID, GenerationID: candidate.GenerationID})
	return nil
}

func bridgeCheckpointReady(root string, now time.Time, heartbeatInterval time.Duration) bool {
	if strings.TrimSpace(root) == "" {
		return false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if heartbeatInterval <= 0 {
		heartbeatInterval = 30 * time.Second
	}
	maxAge := heartbeatInterval * 2
	if maxAge < heartbeatInterval+5*time.Second {
		maxAge = heartbeatInterval + 5*time.Second
	}
	heartbeatPath := filepath.Join(root, bridge.HeartbeatDir, bridge.BridgeHeartbeatFile)
	readyPath := filepath.Join(root, bridge.HeartbeatDir, bridge.CheckpointReadyFile)
	return controlFileFresh(heartbeatPath, now, maxAge) && controlFileFresh(readyPath, now, maxAge)
}

func controlFileFresh(path string, now time.Time, maxAge time.Duration) bool {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	timestamp, ok := parseBridgeControlTimestamp(strings.TrimSpace(string(data)))
	if !ok {
		timestamp = info.ModTime()
	}
	if timestamp.After(now.Add(5 * time.Second)) {
		return false
	}
	return now.Sub(timestamp) <= maxAge
}

func parseBridgeControlTimestamp(raw string) (time.Time, bool) {
	if raw == "" {
		return time.Time{}, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return parsed.UTC(), true
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return time.Time{}, false
	}
	return time.Unix(0, value).UTC(), true
}
