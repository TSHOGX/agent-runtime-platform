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
	"io"
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
	"syscall"
	"time"

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
	proxyCorrelationSocket  = "proxy-correlation.sock"
)

var (
	errGenerationBusy           = errors.New("generation lifecycle is busy")
	errGenerationStartLeaseLost = errors.New("generation start lease lost")
)

type generationStartFailureMode int

const (
	startFailureInputAcceptable generationStartFailureMode = iota
	startFailureInputBlocking
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

type proxyPeerCredentials struct {
	UID int
	GID int
	PID int
}

type proxyPeerCredentialsResult struct {
	Credentials proxyPeerCredentials
	Err         error
}

type proxyPeerCredentialsContextKey struct{}

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

func (s *Server) OperatorRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/agents", s.operatorAgentsCatalog)
	return mux
}

func (s *Server) ProxyCorrelationRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /internal/proxy/requests/start", s.requireProxyPeerCredentials(http.HandlerFunc(s.internalProxyRequestStart)))
	mux.Handle("POST /internal/proxy/requests/finish", s.requireProxyPeerCredentials(http.HandlerFunc(s.internalProxyRequestFinish)))
	return mux
}

func (s *Server) operatorAgentsCatalog(w http.ResponseWriter, r *http.Request) {
	type driverDTO struct {
		DriverID                    string   `json:"driver_id"`
		Label                       string   `json:"label"`
		Kind                        string   `json:"kind"`
		BridgeProtocol              string   `json:"bridge_protocol"`
		OutputSchema                string   `json:"output_schema"`
		RequiredRuntimeCapabilities []string `json:"required_runtime_capabilities"`
		ModelAccess                 bool     `json:"model_access"`
		SupportsInterrupt           bool     `json:"supports_interrupt"`
		SupportsCompaction          bool     `json:"supports_compaction"`
	}
	drivers := []driverDTO{}
	for _, spec := range agents.AllDriverSpecs() {
		drivers = append(drivers, driverDTO{
			DriverID:                    string(spec.ID),
			Label:                       spec.Label,
			Kind:                        string(spec.Kind),
			BridgeProtocol:              spec.BridgeProtocol,
			OutputSchema:                spec.OutputSchema,
			RequiredRuntimeCapabilities: append([]string(nil), spec.RequiredRuntimeCapabilities...),
			ModelAccess:                 spec.ModelAccess,
			SupportsInterrupt:           spec.SupportsInterrupt,
			SupportsCompaction:          spec.SupportsCompaction,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"schema_version": 1,
		"drivers":        drivers,
	})
}

func (s *Server) ProxyCorrelationServer() *http.Server {
	return &http.Server{
		Handler:           s.ProxyCorrelationRoutes(),
		ReadHeaderTimeout: 5 * time.Second,
		ConnContext: func(ctx context.Context, conn net.Conn) context.Context {
			credentials, err := unixPeerCredentials(conn)
			return context.WithValue(ctx, proxyPeerCredentialsContextKey{}, proxyPeerCredentialsResult{
				Credentials: credentials,
				Err:         err,
			})
		},
	}
}

func (s *Server) ListenProxyCorrelation() (net.Listener, string, error) {
	roots, err := config.ValidateIsolationRoots(s.cfg.IsolationRoots())
	if err != nil {
		return nil, "", err
	}
	socketPath := filepath.Join(roots.ProxyInternalRoot, proxyCorrelationSocket)
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o750); err != nil {
		return nil, "", fmt.Errorf("create proxy correlation socket root: %w", err)
	}
	if err := chownProxyCorrelationPath(filepath.Dir(socketPath), s.cfg.Harness.ProxyServiceIdentity.GID); err != nil {
		return nil, "", fmt.Errorf("chown proxy correlation socket root: %w", err)
	}
	if err := os.Chmod(filepath.Dir(socketPath), 0o750); err != nil {
		return nil, "", fmt.Errorf("chmod proxy correlation socket root: %w", err)
	}
	if info, err := os.Lstat(socketPath); err == nil {
		if info.Mode().Type()&os.ModeSocket == 0 {
			return nil, "", fmt.Errorf("proxy correlation socket path %q exists and is not a socket", socketPath)
		}
		if err := os.Remove(socketPath); err != nil {
			return nil, "", fmt.Errorf("remove stale proxy correlation socket: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, "", fmt.Errorf("stat proxy correlation socket: %w", err)
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, "", fmt.Errorf("listen proxy correlation socket: %w", err)
	}
	if err := os.Chmod(socketPath, 0o660); err != nil {
		_ = listener.Close()
		return nil, "", fmt.Errorf("chmod proxy correlation socket: %w", err)
	}
	if err := chownProxyCorrelationPath(socketPath, s.cfg.Harness.ProxyServiceIdentity.GID); err != nil {
		_ = listener.Close()
		return nil, "", fmt.Errorf("chown proxy correlation socket: %w", err)
	}
	return listener, socketPath, nil
}

func chownProxyCorrelationPath(path string, proxyServiceGID int) error {
	if os.Geteuid() != 0 {
		return nil
	}
	if proxyServiceGID < 0 {
		return fmt.Errorf("proxy service gid must be >= 0")
	}
	return os.Chown(path, 0, proxyServiceGID)
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
	case r.URL.Path == "/api/deployment-capabilities" && r.Method == http.MethodGet:
		s.deploymentCapabilities(w, r)
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
	writeJSON(w, http.StatusOK, map[string]any{"sessions": publicSessions(sessions)})
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
	poolCeiling := cidrPool30Capacity(s.cfg.Harness.Network.CIDRPool.Prefix)
	remainingPoolSlots := poolCeiling - resourceQuota.AllocatedPoolSlots
	if remainingPoolSlots < 0 {
		remainingPoolSlots = 0
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"soft_session_ceiling": s.cfg.MaxSessions,
		"active_sessions":      activeSessions,
		"live_pool_ceiling":    poolCeiling,
		"allocated_pool_slots": resourceQuota.AllocatedPoolSlots,
		"remaining_pool_slots": remainingPoolSlots,
	})
}

func (s *Server) deploymentCapabilities(w http.ResponseWriter, r *http.Request) {
	type modeDTO struct {
		Mode           string  `json:"mode"`
		Label          string  `json:"label"`
		Visible        bool    `json:"visible"`
		CreateEnabled  bool    `json:"create_enabled"`
		DisabledReason *string `json:"disabled_reason"`
	}
	disabled := func(reason string) *string { return &reason }
	agentReason := (*string)(nil)
	agentEnabled := true
	if _, err := s.resolveModeDeployment("agent"); err != nil {
		agentEnabled = false
		agentReason = disabled(err.code)
	}
	shellReason := (*string)(nil)
	shellEnabled := true
	if _, err := s.resolveModeDeployment("shell"); err != nil {
		shellEnabled = false
		shellReason = disabled(err.code)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"schema_version": 1,
		"default_mode":   "agent",
		"session_modes": []modeDTO{
			{Mode: "agent", Label: "Agent", Visible: true, CreateEnabled: agentEnabled, DisabledReason: agentReason},
			{Mode: "shell", Label: "Shell", Visible: shellEnabled, CreateEnabled: shellEnabled, DisabledReason: shellReason},
		},
	})
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
	mode := "agent"
	if value, ok := raw["mode"]; ok {
		if err := json.Unmarshal(value, &mode); err != nil {
			writeError(w, http.StatusBadRequest, "invalid mode")
			return
		}
	}
	mode = strings.TrimSpace(mode)
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
		Agent:                 string(driverID),
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
	if err := s.startEnsuredGeneration(r.Context(), session, ensured, startFailureInputAcceptable); err != nil {
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
		if err := s.startEnsuredGeneration(r.Context(), session, ensured, startFailureInputAcceptable); err != nil {
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

func (s *Server) startEnsuredGeneration(ctx context.Context, session store.Session, ensured ensuredGeneration, failureMode generationStartFailureMode) error {
	allocation := ensured.Allocation
	startCtx := ctx
	leaseKeeper := noopStartLeaseKeeper()
	if ensured.IsNew || ensured.RestoreFromCheckpoint {
		var err error
		startCtx, leaseKeeper, err = s.beginGenerationStartLease(ctx, session.ID, allocation.GenerationID, allocation.Owner)
		if err != nil {
			return err
		}
	}
	defer leaseKeeper.stop()
	generationDetails, err := s.runtimeGenerationDetails(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		return err
	}
	dataVolumes, err := s.ensureSessionRuntimeDataVolumes(ctx, session)
	if err != nil {
		if leaseErr := leaseKeeper.err(); leaseErr != nil {
			return leaseErr
		}
		if ensured.RestoreFromCheckpoint {
			if retireErr := s.retireGenerationForRestoreFallback(session.ID, allocation.GenerationID, allocation.Owner, err); retireErr != nil {
				return retireErr
			}
			return err
		}
		if ensured.IsNew {
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
		}
		return err
	}
	preparedArtifacts := runtimeArtifactsFromDetails(generationDetails)
	resourceWorkerID := runtimeResourceWorkerID(s.ownerUUID, allocation.Owner)
	resourceHostID := runtimeResourceHostID()
	var runtimeResourceCreated bool
	var runtimeResourceInstance store.RuntimeResourceInstance
	retireRuntimeResource := func() {
		if !runtimeResourceCreated {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.store.ClaimRuntimeResourceRetiring(cleanupCtx, store.RuntimeResourceRetireParams{
			GenerationID: allocation.GenerationID,
			WorkerID:     resourceWorkerID,
			HostID:       resourceHostID,
			Now:          time.Now().UTC(),
		}); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("failed to retire runtime resource after start failure", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", err)
		}
	}
	if ensured.IsNew {
		preparedArtifacts, err = s.runtime.PrepareGeneration(startCtx, s.runtimeStartRequest(session, allocation.GenerationID, generationDetails, runtime.GenerationArtifacts{}, dataVolumes))
		if err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			if leaseErr := leaseKeeper.ensureOwned(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			return err
		}
		runtimeResourceParams, err := s.runtimeResourceInstanceParams(generationDetails, preparedArtifacts, resourceHostID)
		if err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		_, resourceIdentityDigest, err := store.RuntimeResourceIdentityForParams(runtimeResourceParams)
		if err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		contractPayload, err := s.sandboxContractPayload(session, generationDetails, preparedArtifacts, resourceIdentityDigest, dataVolumes)
		if err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		inputEvidence, err := s.sandboxContractInputEvidenceFor(session, generationDetails.Agent)
		if err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if _, err := s.store.StoreSandboxContract(ctx, store.StoreSandboxContractParams{
			ContractID:             sandboxContractID(allocation.GenerationID),
			SessionID:              session.ID,
			GenerationID:           allocation.GenerationID,
			Owner:                  allocation.Owner,
			SandboxContractVersion: store.SandboxContractVersion,
			ContractGateVersion:    store.SandboxContractGateDriverManifest,
			DriverState:            allocation.DriverState,
			Payload:                contractPayload,
			RuntimeConfigDigest:    inputEvidence.RuntimeConfigDigest,
			RuntimeConfigPreimage:  inputEvidence.RuntimeConfigPreimage,
			AgentManifestDigest:    inputEvidence.AgentManifestDigest,
			AgentManifestPayload:   inputEvidence.AgentManifestPayload,
			Now:                    time.Now().UTC(),
		}); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			return err
		}
		if err := s.store.RecordGenerationRuntimeArtifactDigests(ctx, allocation.GenerationID, runtimeArtifactDigests(preparedArtifacts)); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			return err
		}
		if _, err := s.store.RecordSandboxContractArtifacts(ctx, store.RecordSandboxContractArtifactsParams{
			ContractID:            sandboxContractID(allocation.GenerationID),
			ControlManifestDigest: preparedArtifacts.ManifestDigest,
			OCISpecDigest:         preparedArtifacts.SpecDigest,
			BundleDigest:          preparedArtifacts.BundleDigest,
			Now:                   time.Now().UTC(),
		}); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			return err
		}
		instance, err := s.createRuntimeResourceInstance(ctx, runtimeResourceParams)
		if err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		runtimeResourceInstance = instance
		runtimeResourceCreated = true
		materializeNow := time.Now().UTC()
		if err := s.store.ClaimRuntimeResourceMaterialization(ctx, store.RuntimeResourceMaterializationClaimParams{
			GenerationID:     allocation.GenerationID,
			WorkerID:         resourceWorkerID,
			HostID:           resourceHostID,
			LeaseExpiresAt:   materializeNow.Add(s.cfg.Harness.Bridge.LeaseTTL.Duration),
			IdempotencyToken: "start:" + allocation.GenerationID,
			Now:              materializeNow,
		}); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			retireRuntimeResource()
			return err
		}
		if err := s.store.MarkRuntimeResourceReady(ctx, store.RuntimeResourceWorkerTransitionParams{
			GenerationID: allocation.GenerationID,
			WorkerID:     resourceWorkerID,
			HostID:       resourceHostID,
			Now:          time.Now().UTC(),
		}); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			retireRuntimeResource()
			return err
		}
		if err := s.store.MarkGenerationStarting(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			retireRuntimeResource()
			return err
		}
	}
	if ensured.RestoreFromCheckpoint {
		instance, resourceTracked, err := s.prepareRuntimeResourceRestore(ctx, allocation.GenerationID, resourceWorkerID, resourceHostID, s.cfg.Harness.Bridge.LeaseTTL.Duration)
		if err != nil {
			if retireErr := s.retireGenerationForRestoreFallback(session.ID, allocation.GenerationID, allocation.Owner, err); retireErr != nil {
				return retireErr
			}
			return err
		}
		runtimeResourceCreated = resourceTracked
		runtimeResourceInstance = instance
	}
	if runtimeResourceCreated {
		generationDetails = runtimeDetailsWithResourceInstance(generationDetails, runtimeResourceInstance)
	}
	if err := validateDriverStateForRuntimeLaunch(generationDetails, dataVolumes); err != nil {
		if ensured.RestoreFromCheckpoint {
			retireRuntimeResource()
			if retireErr := s.retireGenerationForRestoreFallback(session.ID, allocation.GenerationID, allocation.Owner, err); retireErr != nil {
				return retireErr
			}
			return err
		}
		retireRuntimeResource()
		s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
		return err
	}
	startReq := s.runtimeStartRequest(session, allocation.GenerationID, generationDetails, preparedArtifacts, dataVolumes)
	startReq.RestoreFromCheckpoint = ensured.RestoreFromCheckpoint
	result := s.runtime.Start(startCtx, startReq, nil)
	if result.Err != nil {
		if leaseErr := leaseKeeper.err(); leaseErr != nil {
			return leaseErr
		}
		if leaseErr := leaseKeeper.ensureOwned(); leaseErr != nil {
			return leaseErr
		}
		if ensured.RestoreFromCheckpoint {
			retireRuntimeResource()
			if err := s.retireGenerationForRestoreFallback(session.ID, allocation.GenerationID, allocation.Owner, result.Err); err != nil {
				return err
			}
			return result.Err
		}
		retireRuntimeResource()
		s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, result.Err, failureMode)
		return result.Err
	}
	if err := leaseKeeper.ensureOwned(); err != nil {
		if destroyErr := s.destroyGenerationRuntime(context.Background(), generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
			s.log.Warn("failed to destroy runtime after start lease loss", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
			return destroyErr
		}
		retireRuntimeResource()
		return err
	}
	bridgeStartupEvidence := ""
	if runtimeResourceCreated {
		renewRuntimeResourceWorkerLease := func() error {
			now := time.Now().UTC()
			return s.store.RenewRuntimeResourceWorkerLease(ctx, store.RuntimeResourceWorkerLeaseRenewalParams{
				GenerationID:   allocation.GenerationID,
				WorkerID:       resourceWorkerID,
				HostID:         resourceHostID,
				LeaseExpiresAt: now.Add(s.cfg.Harness.Bridge.LeaseTTL.Duration),
				Now:            now,
			})
		}
		if err := renewRuntimeResourceWorkerLease(); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			if destroyErr := s.destroyGenerationRuntime(ctx, generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after resource lease renewal failure", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			retireRuntimeResource()
			if ensured.RestoreFromCheckpoint {
				if retireErr := s.retireGenerationForRestoreFallback(session.ID, allocation.GenerationID, allocation.Owner, err); retireErr != nil {
					return retireErr
				}
			} else {
				s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			}
			return err
		}
		bridgeStartupEvidence, err = s.waitForBridgeStartupReadiness(startCtx, allocation, runtimeResourceInstance)
		if err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				if destroyErr := s.destroyGenerationRuntime(context.Background(), generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
					s.log.Warn("failed to destroy runtime after bridge startup lease loss", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
					return destroyErr
				}
				retireRuntimeResource()
				return leaseErr
			}
			if leaseErr := leaseKeeper.ensureOwned(); leaseErr != nil {
				if destroyErr := s.destroyGenerationRuntime(context.Background(), generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
					s.log.Warn("failed to destroy runtime after bridge startup lease loss", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
					return destroyErr
				}
				retireRuntimeResource()
				return leaseErr
			}
			if destroyErr := s.destroyGenerationRuntime(ctx, generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after bridge startup probe failure", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			retireRuntimeResource()
			if ensured.RestoreFromCheckpoint {
				if retireErr := s.retireGenerationForRestoreFallback(session.ID, allocation.GenerationID, allocation.Owner, err); retireErr != nil {
					return retireErr
				}
			} else {
				s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			}
			return err
		}
		if err := renewRuntimeResourceWorkerLease(); err != nil {
			if leaseErr := leaseKeeper.err(); leaseErr != nil {
				return leaseErr
			}
			if destroyErr := s.destroyGenerationRuntime(ctx, generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after resource lease renewal failure", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			retireRuntimeResource()
			if ensured.RestoreFromCheckpoint {
				if retireErr := s.retireGenerationForRestoreFallback(session.ID, allocation.GenerationID, allocation.Owner, err); retireErr != nil {
					return retireErr
				}
			} else {
				s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			}
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			if destroyErr := s.destroyGenerationRuntime(context.Background(), generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after bridge startup lease loss", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			retireRuntimeResource()
			return err
		}
	}
	if ensured.IsNew {
		postStartProof, err := runtimeResourcePostStartProof(runtimeResourceInstance, result, bridgeStartupEvidence)
		if err != nil {
			if destroyErr := s.destroyGenerationRuntime(ctx, generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after missing post-start proof", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := s.store.MarkRuntimeResourceLive(ctx, store.RuntimeResourceWorkerTransitionParams{
			GenerationID: allocation.GenerationID,
			WorkerID:     resourceWorkerID,
			HostID:       resourceHostID,
			PostStart:    &postStartProof,
			Now:          time.Now().UTC(),
		}); err != nil {
			if destroyErr := s.destroyGenerationRuntime(ctx, generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after resource live CAS failure", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			if destroyErr := s.destroyGenerationRuntime(context.Background(), generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after resource live lease loss", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			retireRuntimeResource()
			return err
		}
		if err := s.store.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
			if destroyErr := s.destroyGenerationRuntime(ctx, generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after generation live CAS failure", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			retireRuntimeResource()
			s.failGenerationBeforeTurn(session, allocation.GenerationID, allocation.Owner, err, failureMode)
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			if destroyErr := s.destroyGenerationRuntime(context.Background(), generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after generation live lease loss", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			retireRuntimeResource()
			return err
		}
	}
	if ensured.RestoreFromCheckpoint {
		if runtimeResourceCreated {
			postStartProof, err := runtimeResourcePostStartProof(runtimeResourceInstance, result, bridgeStartupEvidence)
			if err != nil {
				if destroyErr := s.destroyGenerationRuntime(ctx, generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
					s.log.Warn("failed to destroy runtime after restore missing post-start proof", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
					return destroyErr
				}
				retireRuntimeResource()
				if leaseErr := leaseKeeper.ensureOwned(); leaseErr != nil {
					return leaseErr
				}
				if retireErr := s.retireGenerationForRestoreFallback(session.ID, allocation.GenerationID, allocation.Owner, err); retireErr != nil {
					return retireErr
				}
				return err
			}
			if err := s.store.MarkRuntimeResourceLive(ctx, store.RuntimeResourceWorkerTransitionParams{
				GenerationID: allocation.GenerationID,
				WorkerID:     resourceWorkerID,
				HostID:       resourceHostID,
				PostStart:    &postStartProof,
				Now:          time.Now().UTC(),
			}); err != nil {
				if destroyErr := s.destroyGenerationRuntime(ctx, generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
					s.log.Warn("failed to destroy runtime after restore resource live CAS failure", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
					return destroyErr
				}
				retireRuntimeResource()
				if leaseErr := leaseKeeper.ensureOwned(); leaseErr != nil {
					return leaseErr
				}
				if retireErr := s.retireGenerationForRestoreFallback(session.ID, allocation.GenerationID, allocation.Owner, err); retireErr != nil {
					return retireErr
				}
				return err
			}
		}
		if err := s.store.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
			if destroyErr := s.destroyGenerationRuntime(ctx, generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after restore live CAS failure", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			retireRuntimeResource()
			if leaseErr := leaseKeeper.ensureOwned(); leaseErr != nil {
				return leaseErr
			}
			if retireErr := s.retireGenerationForRestoreFallback(session.ID, allocation.GenerationID, allocation.Owner, err); retireErr != nil {
				return retireErr
			}
			return err
		}
		if err := leaseKeeper.ensureOwned(); err != nil {
			if destroyErr := s.destroyGenerationRuntime(context.Background(), generationDetails); destroyErr != nil && !errors.Is(destroyErr, context.Canceled) {
				s.log.Warn("failed to destroy runtime after restore start lease loss", "session_id", session.ID, "generation_id", allocation.GenerationID, "error", destroyErr)
				return destroyErr
			}
			return err
		}
	}
	return nil
}

type startLeaseKeeper struct {
	cancel context.CancelFunc
	done   chan struct{}
	renew  func() error

	mu      sync.Mutex
	failure error
}

func noopStartLeaseKeeper() *startLeaseKeeper {
	done := make(chan struct{})
	close(done)
	return &startLeaseKeeper{
		cancel: func() {},
		done:   done,
		renew:  func() error { return nil },
	}
}

func (s *Server) beginGenerationStartLease(ctx context.Context, sessionID, generationID, owner string) (context.Context, *startLeaseKeeper, error) {
	startCtx, cancel := context.WithCancel(ctx)
	ttl := s.cfg.Harness.Bridge.LeaseTTL.Duration
	keeper := &startLeaseKeeper{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	keeper.renew = func() error {
		renewCtx, renewCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer renewCancel()
		return s.store.RenewGenerationStartLease(renewCtx, store.RenewGenerationStartLeaseParams{
			SessionID:    sessionID,
			GenerationID: generationID,
			Owner:        owner,
			LeaseTTL:     ttl,
			Now:          time.Now().UTC(),
		})
	}
	if err := keeper.ensureOwned(); err != nil {
		cancel()
		close(keeper.done)
		return startCtx, keeper, err
	}
	go func() {
		defer close(keeper.done)
		ticker := time.NewTicker(startLeaseRenewalInterval(ttl))
		defer ticker.Stop()
		for {
			select {
			case <-startCtx.Done():
				return
			case <-ticker.C:
				if err := keeper.ensureOwned(); err != nil {
					return
				}
			}
		}
	}()
	return startCtx, keeper, nil
}

func startLeaseRenewalInterval(ttl time.Duration) time.Duration {
	interval := ttl / 2
	if interval < time.Millisecond {
		return time.Millisecond
	}
	return interval
}

func (k *startLeaseKeeper) ensureOwned() error {
	if err := k.getErr(); err != nil {
		return err
	}
	if k.renew == nil {
		return nil
	}
	if err := k.renew(); err != nil {
		wrapped := fmt.Errorf("%w: %v", errGenerationStartLeaseLost, err)
		k.setErr(wrapped)
		return wrapped
	}
	return k.getErr()
}

func (k *startLeaseKeeper) err() error {
	return k.getErr()
}

func (k *startLeaseKeeper) getErr() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.failure
}

func (k *startLeaseKeeper) setErr(err error) {
	if err == nil {
		return
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.failure != nil {
		return
	}
	k.failure = err
	k.cancel()
}

func (k *startLeaseKeeper) stop() {
	k.cancel()
	<-k.done
}

func (s *Server) failGenerationBeforeTurn(session store.Session, generationID, owner string, failure error, failureMode generationStartFailureMode) {
	reason := ""
	if failure != nil {
		reason = failure.Error()
	}
	errorClass := runtimeFailureClass(reason)
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	retryableStatus := string(sessionstate.RunningIdle)
	if failureMode == startFailureInputBlocking {
		retryableStatus = string(sessionstate.RunningActive)
	} else if session.Status == string(sessionstate.Created) {
		retryableStatus = string(sessionstate.Created)
	}
	eventID, err := s.store.FailGenerationStart(ctx, store.FailGenerationStartParams{
		SessionID:      session.ID,
		GenerationID:   generationID,
		Owner:          owner,
		SessionStatus:  retryableStatus,
		ErrorClass:     errorClass,
		Reason:         reason,
		EventType:      "generation.error",
		EventDedupeKey: "generation_error:" + generationID,
		Now:            now,
	})
	if err != nil {
		s.log.Warn("failed to fail generation before turn start", "session_id", session.ID, "generation_id", generationID, "error", err)
		return
	}
	s.publishDurableEvent(ctx, eventID)
}

func (s *Server) retireGenerationForRestoreFallback(sessionID, generationID, owner string, failure error) error {
	reason := ""
	if failure != nil {
		reason = failure.Error()
	}
	errorClass := runtimeFailureClass(reason)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	retired, err := s.store.RetireRestoreFallback(ctx, store.RetireRestoreFallbackParams{
		SessionID:    sessionID,
		GenerationID: generationID,
		Owner:        owner,
		ErrorClass:   errorClass,
		Reason:       reason,
		Now:          time.Now().UTC(),
	})
	if err != nil {
		s.log.Warn("failed to retire generation before restore fallback", "session_id", sessionID, "generation_id", generationID, "error", err)
		return err
	}
	for _, eventID := range retired.EventIDs {
		s.publishDurableEvent(ctx, eventID)
	}
	return nil
}

func (s *Server) ensureActiveGeneration(ctx context.Context, session store.Session, owner string) (ensuredGeneration, error) {
	return s.ensureActiveGenerationWithRestoreRefetch(ctx, session, owner, true)
}

func (s *Server) ensureActiveGenerationWithRestoreRefetch(ctx context.Context, session store.Session, owner string, allowRestoreRefetch bool) (ensuredGeneration, error) {
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
				LeaseTTL:     s.cfg.Harness.Bridge.LeaseTTL.Duration,
				Now:          time.Now().UTC(),
			})
			if err != nil {
				if allowRestoreRefetch && errors.Is(err, store.ErrStaleCheckpointRestore) {
					refreshed, refreshErr := s.store.GetSession(ctx, session.ID)
					if refreshErr != nil {
						return ensuredGeneration{}, refreshErr
					}
					return s.ensureActiveGenerationWithRestoreRefetch(ctx, refreshed, owner, false)
				}
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
		if _, capabilityErr := s.resolveDriverDeployment(store.ModeForDriver(session.Agent), agents.ID(session.Agent)); capabilityErr != nil {
			return ensuredGeneration{}, capabilityErr
		}
		allocation, err := s.store.AllocateGeneration(ctx, store.AllocateGenerationParams{
			SessionID:            session.ID,
			ExpectedGenerationID: sql.NullString{String: activeGenerationID, Valid: true},
			Owner:                owner,
			LeaseTTL:             s.cfg.Harness.Bridge.LeaseTTL.Duration,
			Now:                  time.Now().UTC(),
			Config:               s.resourceAllocatorConfig(session.Agent),
		})
		if err != nil {
			return ensuredGeneration{}, err
		}
		return ensuredGeneration{Allocation: allocation, IsNew: true}, nil
	}
	if _, capabilityErr := s.resolveDriverDeployment(store.ModeForDriver(session.Agent), agents.ID(session.Agent)); capabilityErr != nil {
		return ensuredGeneration{}, capabilityErr
	}
	allocation, err := s.store.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     owner,
		LeaseTTL:  s.cfg.Harness.Bridge.LeaseTTL.Duration,
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
	if driverID, err := agents.CanonicalDriverID(agent); err == nil {
		agent = string(driverID)
	}
	outputFormat := ""
	providerCredentialsHostOnly := false
	if driverSpec, ok := agents.DriverSpecFor(agent); ok {
		outputFormat = driverSpec.OutputFormat
		providerCredentialsHostOnly = driverSpec.ModelAccess
	}
	var model string
	var disableNonessentialTraffic bool
	if _, agentCfg, ok := s.enabledAgentConfigForDriver(agents.ID(agent)); ok {
		if agentCfg.DisableNonessentialTraffic != nil {
			disableNonessentialTraffic = *agentCfg.DisableNonessentialTraffic
		}
		if strings.TrimSpace(agentCfg.ModelProfile) != "" {
			if profile, ok := s.cfg.DeploymentModelProfiles()[agentCfg.ModelProfile]; ok && strings.TrimSpace(profile.Model) != "" {
				model = strings.TrimSpace(profile.Model)
			}
		}
	}
	return store.ResourceAllocatorConfig{
		RunDir:                      s.cfg.Harness.RunDir,
		CIDRPool:                    s.cfg.Harness.Network.CIDRPool.Prefix,
		EgressDorisFEHosts:          s.cfg.Harness.Network.Egress.DorisFEHosts,
		EgressDorisBEHosts:          s.cfg.Harness.Network.Egress.DorisBEHosts,
		EgressDorisPorts:            s.cfg.Harness.Network.Egress.DorisPorts,
		EgressDNSPolicy:             string(s.cfg.Harness.Network.Egress.DNSPolicy),
		HostProxyBindURL:            s.cfg.ModelProxy.BindURL,
		ProxyPort:                   s.cfg.ModelProxy.BindPort,
		Agent:                       agent,
		AgentModel:                  model,
		AgentOutputFormat:           outputFormat,
		DisableNonessentialTraffic:  disableNonessentialTraffic,
		SandboxUID:                  s.cfg.Harness.SandboxIdentity.UID,
		SandboxGID:                  s.cfg.Harness.SandboxIdentity.GID,
		SandboxSupplementalGIDs:     s.cfg.Harness.SandboxIdentity.SupplementalGIDs,
		ProviderCredentialsHostOnly: providerCredentialsHostOnly,
		SandboxModelProxyBaseURL:    s.cfg.ModelProxy.SandboxBaseURL,
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

type sessionRuntimeDataVolumes struct {
	Workspace  store.SessionWorkspaceVolume
	DriverHome store.SessionDriverHomeVolume
}

func (s *Server) runtimeStartRequest(session store.Session, generationID string, details store.RuntimeGenerationDetails, artifacts runtime.GenerationArtifacts, volumes sessionRuntimeDataVolumes) runtime.StartRequest {
	return runtime.StartRequest{
		SessionID:         session.ID,
		GenerationID:      generationID,
		Agent:             session.Agent,
		Generation:        details,
		PreparedArtifacts: artifacts,
		WorkspaceHostPath: volumes.Workspace.HostPath,
		AgentHomeHostPath: volumes.DriverHome.HostPath,
	}
}

func validateDriverStateForRuntimeLaunch(details store.RuntimeGenerationDetails, volumes sessionRuntimeDataVolumes) error {
	return store.ValidateDriverStatePayloadForRuntimeLaunch(details.Agent, details.DriverStatePayload, volumes.DriverHome.HostPath)
}

func (s *Server) ensureSessionRuntimeDataVolumes(ctx context.Context, session store.Session) (sessionRuntimeDataVolumes, error) {
	volumeConfig, err := s.dataVolumeProvisionerConfig()
	if err != nil {
		return sessionRuntimeDataVolumes{}, err
	}
	now := time.Now().UTC()
	workspace, err := s.store.ProvisionSessionWorkspace(ctx, store.ProvisionSessionWorkspaceParams{
		SessionID: session.ID,
		Config:    volumeConfig,
		Now:       now,
	})
	if err != nil {
		return sessionRuntimeDataVolumes{}, fmt.Errorf("provision session workspace volume: %w", err)
	}
	driverHome, err := s.store.ProvisionSessionDriverHome(ctx, store.ProvisionSessionDriverHomeParams{
		SessionID: session.ID,
		Driver:    session.Agent,
		Config:    volumeConfig,
		Now:       now,
	})
	if err != nil {
		return sessionRuntimeDataVolumes{}, fmt.Errorf("provision session driver home volume: %w", err)
	}
	return sessionRuntimeDataVolumes{Workspace: workspace, DriverHome: driverHome}, nil
}

func (s *Server) destroyGenerationRuntime(ctx context.Context, details store.RuntimeGenerationDetails) error {
	runtimeID := strings.TrimSpace(details.RunscContainerID)
	if runtimeID == "" {
		return fmt.Errorf("generation %s has no runsc container id", details.GenerationID)
	}
	return s.runtime.Destroy(ctx, runtimeID)
}

func sandboxContractID(generationID string) string {
	return "contract_" + strings.TrimSpace(generationID)
}

func driverConfigMaterializationPayload(driverID string, entries []runtime.DriverConfigMaterialization) (map[string]any, map[string]any, error) {
	driverID = strings.TrimSpace(driverID)
	specs := agents.DriverConfigMaterializationSpecsFor(agents.ID(driverID))
	if len(specs) == 0 {
		if len(entries) != 0 {
			return nil, nil, fmt.Errorf("driver %s does not support driver config materialization", driverID)
		}
		return map[string]any{}, nil, nil
	}
	if len(entries) != len(specs) {
		return nil, nil, fmt.Errorf("driver %s config materialization requires %d projections", driverID, len(specs))
	}
	runtimePayload := map[string]any{}
	mountPayload := map[string]any{}
	expected := map[string]agents.DriverConfigMaterializationSpec{}
	for _, spec := range specs {
		expected[spec.Name] = spec
	}
	seen := map[string]struct{}{}
	for _, entry := range entries {
		name := strings.TrimSpace(entry.Name)
		want, ok := expected[name]
		if !ok {
			return nil, nil, fmt.Errorf("unsupported %s driver config materialization %q", driverID, entry.Name)
		}
		if _, ok := seen[name]; ok {
			return nil, nil, fmt.Errorf("duplicate %s driver config materialization %q", driverID, name)
		}
		seen[name] = struct{}{}
		if entry.SourceProjectionPath != want.SourceProjectionPath || entry.SandboxDestination != want.SandboxDestination {
			return nil, nil, fmt.Errorf("%s driver config materialization %s path mismatch", driverID, name)
		}
		if !strings.HasPrefix(strings.TrimSpace(entry.SourceDigest), "sha256:") {
			return nil, nil, fmt.Errorf("%s driver config materialization %s digest is required", driverID, name)
		}
		if entry.DestinationMutableBySandbox != want.DestinationMutableBySandbox {
			return nil, nil, fmt.Errorf("%s driver config materialization %s mutability mismatch", driverID, name)
		}
		runtimePayload[name] = map[string]any{
			"source_projection_path":         entry.SourceProjectionPath,
			"source_digest":                  entry.SourceDigest,
			"sandbox_destination":            entry.SandboxDestination,
			"destination_mutable_by_sandbox": entry.DestinationMutableBySandbox,
		}
		mountPayload[name] = map[string]any{
			"type":                           want.MountType,
			"mode":                           want.MountMode,
			"exact":                          want.MountExact,
			"source_projection_path":         entry.SourceProjectionPath,
			"sandbox_destination":            entry.SandboxDestination,
			"destination_mutable_by_sandbox": entry.DestinationMutableBySandbox,
		}
	}
	if len(seen) != len(expected) {
		return nil, nil, fmt.Errorf("%s driver config materialization missing required projections", driverID)
	}
	return runtimePayload, mountPayload, nil
}

func (s *Server) sandboxContractPayload(session store.Session, details store.RuntimeGenerationDetails, artifacts runtime.GenerationArtifacts, resourceIdentityDigest string, volumes sessionRuntimeDataVolumes) (map[string]any, error) {
	runscPlatform := strings.TrimSpace(details.RunscPlatform)
	if runscPlatform == "" {
		runscPlatform = "systrap"
	}
	driverID := strings.TrimSpace(details.Agent)
	driverSpec, ok := agents.DriverSpecFor(driverID)
	if !ok {
		return nil, fmt.Errorf("unsupported driver %q", driverID)
	}
	mode := strings.TrimSpace(session.Mode)
	if mode == "" {
		mode = store.ModeForDriver(driverID)
	}
	deployment, capabilityErr := s.resolveDriverDeployment(mode, agents.ID(driverID))
	if capabilityErr != nil {
		return nil, capabilityErr
	}
	driverSpec = deployment.DriverSpec
	providerSpec := deployment.ProviderSpec
	initialDriverStateDigest := strings.TrimSpace(details.DriverStateDigest)
	if initialDriverStateDigest == "" {
		initialDriverStateDigest = sandboxContractDigestForPayload(map[string]any{
			"schema_version": 1,
			"driver_id":      driverID,
			"state_kind":     "missing_driver_state",
		})
	}
	sandboxIP, err := runtimeResourceSandboxIP(details.SandboxIPCIDR)
	if err != nil {
		return nil, err
	}
	var sandboxModelProxyBaseURL any
	if value := strings.TrimSpace(details.ManifestAnthropicBaseURL); value != "" {
		sandboxModelProxyBaseURL = value
	}
	mountPlan := map[string]any{
		"workspace":  map[string]any{"source": volumes.Workspace.HostPath, "destination": "/workspace", "mode": "rw"},
		"agent_home": map[string]any{"source": volumes.DriverHome.HostPath, "destination": "/agent-home", "mode": "rw"},
		"control":    map[string]any{"source": details.ControlDirPath, "destination": "/harness-control", "mode": "ro"},
		"bridge":     map[string]any{"source": details.BridgeDirPath, "destination": "/harness-control/bridge", "mode": "rw"},
	}
	if strings.TrimSpace(details.NetworkHostsPath) != "" {
		mountPlan["network_hosts"] = map[string]any{"source": details.NetworkHostsPath, "destination": "/etc/hosts", "mode": "ro"}
	}
	materializedDriverConfig, mountMaterializations, err := driverConfigMaterializationPayload(driverID, artifacts.MaterializedDriverConfig)
	if err != nil {
		return nil, err
	}
	if len(mountMaterializations) > 0 {
		mountPlan["driver_config_materializations"] = mountMaterializations
	}
	driverConfigPreimage := map[string]any{
		"driver_id":     driverID,
		"model":         details.Model,
		"output_format": details.OutputFormat,
	}
	if len(materializedDriverConfig) > 0 {
		driverConfigPreimage["materialized_driver_config"] = materializedDriverConfig
	}
	driverConfigDigest := sandboxContractDigestForPayload(driverConfigPreimage)
	commandDigest := sandboxContractDigestForPayload(map[string]any{
		"driver_id":    driverID,
		"protocol":     details.OutputFormat,
		"resume_field": "driver_state",
	})
	driverCapabilitiesDigest := sandboxContractDigestForPayload(map[string]any{
		"driver_id":     driverID,
		"capabilities":  driverSpec.RequiredRuntimeCapabilities,
		"registry_kind": string(driverSpec.Kind),
	})
	providerCapabilitiesDigest := agents.CapabilityDigest(providerSpec)
	runtimeTemplateDigest := sandboxContractDigestForPayload(map[string]any{
		"provider_id":          providerSpec.ID,
		"runsc_platform":       runscPlatform,
		"runsc_overlay2":       details.RunscOverlay2,
		"no_new_privileges":    true,
		"ambient_capabilities": []string{},
	})
	secretGrants := []map[string]any{}
	if details.ModelAccessAllowed {
		secretGrants = append(secretGrants, map[string]any{
			"grant_id":                  "model_provider:anthropic_proxy",
			"domain":                    "model_provider",
			"scope":                     "anthropic_messages",
			"exposure_mode":             "proxy_only",
			"ttl_seconds":               nil,
			"allowed_drivers":           []string{driverID},
			"allowed_runtime_providers": []string{providerSpec.ID},
		})
	}
	credentialPreimage := map[string]any{
		"provider_credentials": "host-only",
		"sandbox_secret_mount": "absent",
		"proxy_token":          "absent",
		"secret_grants":        secretGrants,
	}
	credentialDigest, err := store.CredentialPolicyDigest(credentialPreimage)
	if err != nil {
		return nil, err
	}
	inputDigests := s.driverManifestInputDigests(deployment)
	payload := map[string]any{
		"sandbox_contract_version": store.SandboxContractVersion,
		"contract_schema_version":  store.SandboxContractSchemaVersion,
		"contract_gate_version":    store.SandboxContractGateDriverManifest,
		"contract_id":              sandboxContractID(details.GenerationID),
		"session_id":               details.SessionID,
		"generation_id":            details.GenerationID,
		"driver": map[string]any{
			"driver_id":                            driverID,
			"driver_version":                       "bundled",
			"bridge_protocol":                      driverSpec.BridgeProtocol,
			"bridge_protocol_version":              driverSpec.BridgeProtocolVersion,
			"turn_input_schema":                    driverSpec.TurnInputSchema,
			"output_schema":                        driverSpec.OutputSchema,
			"command_argv_digest":                  commandDigest,
			"driver_config_digest":                 driverConfigDigest,
			"required_runtime_capabilities_digest": driverCapabilitiesDigest,
			"supports_interrupt":                   driverSpec.SupportsInterrupt,
			"supports_compaction":                  driverSpec.SupportsCompaction,
		},
		"runtime_provider": map[string]any{
			"provider_id":              providerSpec.ID,
			"provider_profile_id":      providerSpec.ProviderProfileID,
			"isolation_kind":           providerSpec.IsolationKind,
			"template_ref":             providerSpec.TemplateRef,
			"template_digest":          runtimeTemplateDigest,
			"capability_vocab_version": providerSpec.CapabilityVocabulary,
			"capability_digest":        providerCapabilitiesDigest,
			"provider_specific": map[string]any{
				"runsc_container_id":   details.RunscContainerID,
				"runsc_platform":       runscPlatform,
				"runsc_version":        artifacts.RunscVersion,
				"runsc_binary_path":    artifacts.RunscBinaryPath,
				"runsc_binary_digest":  artifacts.RunscBinaryDigest,
				"runsc_overlay2":       details.RunscOverlay2,
				"no_new_privileges":    true,
				"ambient_capabilities": []string{},
				"required_annotations": map[string]any{
					bridge.BridgeMountDestination: map[string]string{
						"dev.gvisor.spec.mount./harness-control/bridge.type":  "bind",
						"dev.gvisor.spec.mount./harness-control/bridge.share": "exclusive",
					},
				},
			},
		},
		"runtime_profile_id": details.AgentRuntimeProfileID,
		"network_profile_id": details.NetworkProfileID,
		"identity": map[string]any{
			"sandbox_uid":               details.SandboxUID,
			"sandbox_gid":               details.SandboxGID,
			"sandbox_supplemental_gids": append([]int(nil), details.SandboxSupplementalGIDs...),
			"model_access_allowed":      details.ModelAccessAllowed,
		},
		"mount_plan": mountPlan,
		"network_identity": map[string]any{
			"runsc_network":    details.RunscNetwork,
			"sandbox_ip":       sandboxIP,
			"sandbox_ip_cidr":  details.SandboxIPCIDR,
			"host_gateway_ip":  details.HostGatewayIP,
			"netns_name":       details.NetnsName,
			"netns_path":       details.NetnsPath,
			"host_veth":        details.HostVeth,
			"sandbox_veth":     details.SandboxVeth,
			"host_side_cidr":   details.HostSideCIDR,
			"nft_table_name":   runtimeResourceNftTableName(details.GenerationID),
			"egress_policy_id": details.EgressPolicyID,
		},
		"runtime_adapter": map[string]any{
			"kind":                 "runsc",
			"runsc_platform":       runscPlatform,
			"runsc_version":        artifacts.RunscVersion,
			"runsc_binary_path":    artifacts.RunscBinaryPath,
			"runsc_binary_digest":  artifacts.RunscBinaryDigest,
			"runsc_container_id":   details.RunscContainerID,
			"runsc_network":        details.RunscNetwork,
			"runsc_overlay2":       details.RunscOverlay2,
			"no_new_privileges":    true,
			"ambient_capabilities": []string{},
			"forbidden_capabilities": []string{
				"CAP_NET_ADMIN",
				"CAP_NET_RAW",
				"CAP_SYS_ADMIN",
			},
			"required_annotations": map[string]any{
				bridge.BridgeMountDestination: map[string]string{
					"dev.gvisor.spec.mount./harness-control/bridge.type":  "bind",
					"dev.gvisor.spec.mount./harness-control/bridge.share": "exclusive",
				},
			},
		},
		"resource_identity": map[string]any{
			"resource_identity_digest": resourceIdentityDigest,
		},
		"data_volumes": map[string]any{
			"workspace": map[string]any{
				"table":                      "session_workspaces",
				"session_id":                 volumes.Workspace.SessionID,
				"host_path":                  volumes.Workspace.HostPath,
				"layout_version":             volumes.Workspace.LayoutVersion,
				"runtime_identity_digest":    volumes.Workspace.RuntimeIdentityDigest,
				"provisioning_marker_path":   volumes.Workspace.ProvisioningMarkerPath,
				"provisioning_marker_digest": volumes.Workspace.ProvisioningMarkerDigest,
				"sandbox_destination":        "/workspace",
				"provisioning_evidence_root": filepath.Dir(filepath.Dir(volumes.Workspace.ProvisioningMarkerPath)),
			},
			"agent_home": map[string]any{
				"table":                      "session_driver_homes",
				"session_id":                 volumes.DriverHome.SessionID,
				"driver":                     volumes.DriverHome.Driver,
				"driver_home_key":            volumes.DriverHome.Driver,
				"host_path":                  volumes.DriverHome.HostPath,
				"layout_version":             volumes.DriverHome.LayoutVersion,
				"runtime_identity_digest":    volumes.DriverHome.RuntimeIdentityDigest,
				"provisioning_marker_path":   volumes.DriverHome.ProvisioningMarkerPath,
				"provisioning_marker_digest": volumes.DriverHome.ProvisioningMarkerDigest,
				"sandbox_destination":        "/agent-home",
				"provisioning_evidence_root": filepath.Dir(filepath.Dir(filepath.Dir(volumes.DriverHome.ProvisioningMarkerPath))),
			},
		},
		"credential_policy": map[string]any{
			"provider_credentials": "host-only",
			"sandbox_secret_mount": "absent",
			"proxy_token":          "absent",
			"digest":               credentialDigest,
			"secret_grants":        secretGrants,
		},
		"model_access": map[string]any{
			"model_access_allowed":         details.ModelAccessAllowed,
			"active_turn_required":         true,
			"provider_protocol":            "anthropic_messages",
			"sandbox_model_proxy_base_url": sandboxModelProxyBaseURL,
		},
		"snapshot_policy": map[string]any{
			"provider_supports_snapshot_disk":   providerSpec.SnapshotPolicy.ProviderSupportsSnapshotDisk,
			"provider_supports_snapshot_memory": providerSpec.SnapshotPolicy.ProviderSupportsSnapshotMemory,
			"provider_supports_branch":          providerSpec.SnapshotPolicy.ProviderSupportsBranch,
			"branch_count_limit":                providerSpec.SnapshotPolicy.BranchCountLimit,
			"must_quiesce_processes":            providerSpec.SnapshotPolicy.MustQuiesceProcesses,
			"stream_disconnects_on_snapshot":    providerSpec.SnapshotPolicy.StreamDisconnectsOnSnapshot,
			"snapshot_semantic":                 providerSpec.SnapshotPolicy.SnapshotSemantic,
		},
		"driver_runtime": map[string]any{
			"driver_home_mount":             "/agent-home",
			"generated_driver_config_mount": "/harness-control/driver/" + driverID,
			"materialized_driver_config":    materializedDriverConfig,
			"initial_driver_state_digest":   initialDriverStateDigest,
		},
		"input_digests": map[string]any{
			"runtime_config_digest": inputDigests.RuntimeConfigDigest,
			"rootfs_image_digest":   nil,
			"agent_manifest_digest": inputDigests.AgentManifestDigest,
		},
	}
	return payload, nil
}

type driverManifestInputDigests struct {
	RuntimeConfigDigest string
	AgentManifestDigest string
}

func (s *Server) driverManifestInputDigests(deployment deploymentResolution) driverManifestInputDigests {
	defaultAgent := strings.TrimSpace(s.cfg.DefaultAgent)
	if defaultAgent == "" {
		defaultAgent = string(agents.ClaudeCode)
	}
	if canonical, err := agents.CanonicalDriverID(defaultAgent); err == nil {
		defaultAgent = string(canonical)
	}
	runtimeConfigDigest := runtimeConfigDigest(deployment.runtimeConfigPreimage(defaultAgent))
	return driverManifestInputDigests{
		RuntimeConfigDigest: runtimeConfigDigest,
		AgentManifestDigest: deployment.AgentManifest.Digest,
	}
}

func effectiveString(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func driverInstallDigest(driverID agents.ID) (string, error) {
	spec, ok := agents.DriverSpecFor(string(driverID))
	if !ok {
		return "", fmt.Errorf("unsupported driver %q", driverID)
	}
	binaryPath, err := expectedDriverBinaryPath(driverID)
	if err != nil {
		return "", err
	}
	payload := map[string]any{
		"driver_id": string(driverID),
		"path":      binaryPath,
	}
	facts := spec.PackageFacts
	if strings.TrimSpace(facts.Name) != "" {
		payload["package"] = facts.Name
	}
	if strings.TrimSpace(facts.Version) != "" {
		payload["package_version"] = facts.Version
	}
	if strings.TrimSpace(facts.Shasum) != "" {
		payload["package_shasum"] = facts.Shasum
	}
	if strings.TrimSpace(facts.Integrity) != "" {
		payload["package_integrity"] = facts.Integrity
	}
	if strings.TrimSpace(facts.EventSchemaVersion) != "" {
		payload["event_schema"] = facts.EventSchemaVersion
	}
	return sandboxContractDigestForPayload(payload), nil
}

func sandboxContractDigestForPayload(value any) string {
	payload, err := store.CanonicalSandboxContractPayload(value)
	if err != nil {
		return "sha256:invalid"
	}
	return store.SandboxContractDigest(payload)
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
		RunscBinaryPath:         details.RunscBinaryPath,
		RunscBinaryDigest:       details.RunscBinaryDigest,
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
		RunscBinaryPath:                artifacts.RunscBinaryPath,
		RunscBinaryDigest:              artifacts.RunscBinaryDigest,
	}
}

func (s *Server) runtimeResourceInstanceParams(details store.RuntimeGenerationDetails, artifacts runtime.GenerationArtifacts, hostID string) (store.RuntimeResourceInstanceParams, error) {
	runscPlatform := strings.TrimSpace(details.RunscPlatform)
	if runscPlatform == "" {
		runscPlatform = "systrap"
	}
	sandboxIP, err := runtimeResourceSandboxIP(details.SandboxIPCIDR)
	if err != nil {
		return store.RuntimeResourceInstanceParams{}, err
	}
	return store.RuntimeResourceInstanceParams{
		GenerationID:           details.GenerationID,
		SessionID:              details.SessionID,
		ContractID:             sandboxContractID(details.GenerationID),
		SandboxContractVersion: store.SandboxContractVersion,
		HostID:                 hostID,
		RunscContainerID:       details.RunscContainerID,
		RunscPlatform:          runscPlatform,
		RunscVersion:           artifacts.RunscVersion,
		RunscBinaryPath:        artifacts.RunscBinaryPath,
		RunscBinaryDigest:      artifacts.RunscBinaryDigest,
		NetworkProfileID:       details.NetworkProfileID,
		NetnsName:              details.NetnsName,
		NetnsPath:              details.NetnsPath,
		HostVeth:               details.HostVeth,
		SandboxVeth:            details.SandboxVeth,
		HostGatewayIP:          details.HostGatewayIP,
		SandboxIP:              sandboxIP,
		SandboxIPCIDR:          details.SandboxIPCIDR,
		HostSideCIDR:           details.HostSideCIDR,
		NftTableName:           runtimeResourceNftTableName(details.GenerationID),
		ControlDirPath:         details.ControlDirPath,
		ControlManifestPath:    details.ControlManifestPath,
		BundleDirPath:          details.BundleDirPath,
		SpecPath:               details.SpecPath,
		CheckpointPath:         details.CheckpointPath,
		BridgeDirPath:          details.BridgeDirPath,
		NetworkHostsPath:       details.NetworkHostsPath,
		LogDirPath:             details.LogDirPath,
		RootPrefixes:           s.runtimeResourceRootPrefixes(),
		Now:                    time.Now().UTC(),
	}, nil
}

func (s *Server) createRuntimeResourceInstance(ctx context.Context, params store.RuntimeResourceInstanceParams) (store.RuntimeResourceInstance, error) {
	return s.store.CreateRuntimeResourceInstance(ctx, params)
}

func (s *Server) prepareRuntimeResourceRestore(ctx context.Context, generationID, workerID, hostID string, leaseTTL time.Duration) (store.RuntimeResourceInstance, bool, error) {
	_, ok, err := s.runtimeResourceInstanceIfExists(ctx, generationID)
	if err != nil || !ok {
		return store.RuntimeResourceInstance{}, false, err
	}
	now := time.Now().UTC()
	if err := s.store.ClaimRuntimeResourceCheckpointRestore(ctx, store.RuntimeResourceMaterializationClaimParams{
		GenerationID:     generationID,
		WorkerID:         workerID,
		HostID:           hostID,
		LeaseExpiresAt:   now.Add(leaseTTL),
		IdempotencyToken: "restore:" + generationID,
		Now:              now,
	}); err != nil {
		return store.RuntimeResourceInstance{}, true, err
	}
	if err := s.store.MarkRuntimeResourceReady(ctx, store.RuntimeResourceWorkerTransitionParams{
		GenerationID: generationID,
		WorkerID:     workerID,
		HostID:       hostID,
		Now:          time.Now().UTC(),
	}); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.store.ClaimRuntimeResourceRetiring(cleanupCtx, store.RuntimeResourceRetireParams{
			GenerationID: generationID,
			WorkerID:     workerID,
			HostID:       hostID,
			Now:          time.Now().UTC(),
		})
		return store.RuntimeResourceInstance{}, true, err
	}
	instance, err := s.store.GetRuntimeResourceInstance(ctx, generationID)
	if err != nil {
		return store.RuntimeResourceInstance{}, true, err
	}
	return instance, true, nil
}

func (s *Server) reserveRuntimeResourceCheckpoint(ctx context.Context, generationID string) error {
	instance, ok, err := s.runtimeResourceInstanceIfExists(ctx, generationID)
	if err != nil || !ok {
		return err
	}
	return s.store.ReserveRuntimeResourceCheckpoint(ctx, store.RuntimeResourceWorkerTransitionParams{
		GenerationID: generationID,
		WorkerID:     instance.WorkerID,
		HostID:       instance.HostID,
		Now:          time.Now().UTC(),
	})
}

func (s *Server) runtimeResourceInstanceIfExists(ctx context.Context, generationID string) (store.RuntimeResourceInstance, bool, error) {
	instance, err := s.store.GetRuntimeResourceInstance(ctx, generationID)
	if errors.Is(err, sql.ErrNoRows) {
		return store.RuntimeResourceInstance{}, false, nil
	}
	if err != nil {
		return store.RuntimeResourceInstance{}, false, err
	}
	return instance, true, nil
}

func (s *Server) runtimeResourceCleanupIdentityIfExists(ctx context.Context, generationID string) (store.RuntimeResourceInstance, bool, error) {
	instance, err := s.store.GetRuntimeResourceCleanupIdentity(ctx, generationID)
	if errors.Is(err, sql.ErrNoRows) {
		return store.RuntimeResourceInstance{}, false, nil
	}
	if err != nil {
		return store.RuntimeResourceInstance{}, false, err
	}
	return instance, true, nil
}

func (s *Server) claimRuntimeResourceCleanup(ctx context.Context, instance store.RuntimeResourceInstance, now time.Time) error {
	switch instance.State {
	case store.RuntimeResourceRetiring, store.RuntimeResourceReconciling, store.RuntimeResourceAbsentVerified, store.RuntimeResourceDestroyed:
		return nil
	}
	if err := s.store.ClaimRuntimeResourceRetiring(ctx, store.RuntimeResourceRetireParams{
		GenerationID: instance.GenerationID,
		WorkerID:     instance.WorkerID,
		HostID:       instance.HostID,
		Now:          now,
	}); err != nil {
		return fmt.Errorf("claim runtime resource retiring: %w", err)
	}
	return nil
}

func (s *Server) completeRuntimeResourceCleanup(ctx context.Context, instance store.RuntimeResourceInstance, cleanup runtime.GenerationResourceCleanup, now time.Time) error {
	evidence := runtimeResourceCleanupEvidence(instance, cleanup)
	current, err := s.store.GetRuntimeResourceCleanupIdentity(ctx, instance.GenerationID)
	if err != nil {
		return err
	}
	switch current.State {
	case store.RuntimeResourceDestroyed:
		return nil
	case store.RuntimeResourceAbsentVerified:
	case store.RuntimeResourceReconciling:
	default:
		if err := s.store.MarkRuntimeResourceReconciling(ctx, store.RuntimeResourceEvidenceParams{
			GenerationID: instance.GenerationID,
			WorkerID:     current.WorkerID,
			HostID:       instance.HostID,
			Evidence:     evidence,
			Now:          now,
		}); err != nil {
			return err
		}
	}
	current, err = s.store.GetRuntimeResourceCleanupIdentity(ctx, instance.GenerationID)
	if err != nil {
		return err
	}
	if current.State == store.RuntimeResourceReconciling {
		if err := s.store.MarkRuntimeResourceAbsentVerified(ctx, store.RuntimeResourceEvidenceParams{
			GenerationID: instance.GenerationID,
			WorkerID:     current.WorkerID,
			HostID:       instance.HostID,
			Evidence:     evidence,
			Now:          now,
		}); err != nil {
			return err
		}
	}
	if err := s.store.MarkRuntimeResourceDestroyed(ctx, store.RuntimeResourceRetireParams{
		GenerationID: instance.GenerationID,
		WorkerID:     current.WorkerID,
		HostID:       instance.HostID,
		Now:          now,
	}); err != nil {
		return err
	}
	return nil
}

func runtimeDetailsWithResourceInstance(details store.RuntimeGenerationDetails, instance store.RuntimeResourceInstance) store.RuntimeGenerationDetails {
	details.RunscContainerID = instance.RunscContainerID
	details.RunscPlatform = instance.RunscPlatform
	details.RunscVersion = instance.RunscVersion
	details.RunscBinaryPath = instance.RunscBinaryPath
	details.RunscBinaryDigest = instance.RunscBinaryDigest
	details.NetworkProfileID = instance.NetworkProfileID
	details.NetnsName = instance.NetnsName
	details.NetnsPath = instance.NetnsPath
	details.HostVeth = instance.HostVeth
	details.SandboxVeth = instance.SandboxVeth
	details.HostGatewayIP = instance.HostGatewayIP
	details.SandboxIPCIDR = instance.SandboxIPCIDR
	details.HostSideCIDR = instance.HostSideCIDR
	details.NftTableName = instance.NftTableName
	details.ControlDirPath = instance.ControlDirPath
	details.ControlManifestPath = instance.ControlManifestPath
	details.BundleDirPath = instance.BundleDirPath
	details.SpecPath = instance.SpecPath
	details.CheckpointPath = instance.CheckpointPath
	details.BridgeDirPath = instance.BridgeDirPath
	details.NetworkHostsPath = instance.NetworkHostsPath
	details.LogDirPath = instance.LogDirPath
	return details
}

type bridgeStartupProbeState struct {
	heartbeatSeen bool
	helloSeen     bool
	probeSeen     bool
	heartbeatSeq  uint64
	helloSeq      uint64
	probeSeq      uint64
}

type bridgeHelloAckPayload struct {
	LastOutputSequenceByTurn map[string]int64 `json:"last_output_sequence_by_turn"`
	LeasedTurnID             *int64           `json:"leased_turn_id,omitempty"`
	ServerTime               time.Time        `json:"server_time"`
}

func (s *Server) waitForBridgeStartupReadiness(ctx context.Context, allocation store.GenerationAllocation, instance store.RuntimeResourceInstance) (string, error) {
	attempts := s.cfg.Harness.Probe.PostStartAttempts
	if attempts <= 0 {
		attempts = 5
	}
	interval := s.cfg.Harness.Probe.PostStartInterval.Duration
	if interval <= 0 {
		interval = time.Second
	}
	inbox, err := bridge.OpenQueue(instance.BridgeDirPath, bridge.InboxDir)
	if err != nil {
		return "", fmt.Errorf("bridge startup probe open inbox: %w", err)
	}
	outbox, err := bridge.OpenQueue(instance.BridgeDirPath, bridge.OutboxDir)
	if err != nil {
		return "", fmt.Errorf("bridge startup probe open outbox: %w", err)
	}
	state := bridgeStartupProbeState{}
	for attempt := 1; attempt <= attempts; attempt++ {
		ready, err := s.processBridgeStartupBatch(ctx, inbox, outbox, allocation.Owner, instance, &state)
		if err != nil {
			return "", err
		}
		if ready {
			return state.evidence(), nil
		}
		if attempt == attempts {
			break
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", ctx.Err()
		case <-timer.C:
		}
	}
	return "", fmt.Errorf("bridge startup probe did not complete: missing %s", state.missing())
}

func (s *Server) processBridgeStartupBatch(ctx context.Context, inbox, outbox bridge.Queue, owner string, instance store.RuntimeResourceInstance, state *bridgeStartupProbeState) (bool, error) {
	files, err := outbox.ReadAll()
	if err != nil {
		return false, fmt.Errorf("bridge startup probe read outbox: %w", err)
	}
	for _, file := range files {
		if state.ready() {
			return true, nil
		}
		envelope := file.Envelope
		if err := validateBridgeStartupEnvelope(envelope, instance); err != nil {
			return false, err
		}
		switch envelope.Type {
		case bridge.TypeHeartbeat:
			state.heartbeatSeen = true
			if state.heartbeatSeq == 0 {
				state.heartbeatSeq = file.Seq
			}
		case bridge.TypeHello:
			if _, _, err := bridge.ValidateHelloPayload(ctx, bridgeStore(s.store), envelope, 2, "RunTurn"); err != nil {
				return false, fmt.Errorf("bridge startup probe hello validation failed: %w", err)
			}
			ack, err := s.store.BridgeHelloAck(ctx, envelope.SessionID, envelope.GenerationID, owner, time.Now().UTC(), 0)
			if err != nil {
				return false, fmt.Errorf("bridge startup probe hello failed: %w", err)
			}
			if err := writeBridgeStartupResponse(ctx, inbox, envelope, bridge.TypeHelloAck, bridgeHelloAckPayload{
				LastOutputSequenceByTurn: bridgeHelloLastSequences(ack.LastOutputSequenceByTurn),
				LeasedTurnID:             ack.LeasedTurnID,
				ServerTime:               ack.ServerTime,
			}); err != nil {
				return false, fmt.Errorf("bridge startup probe hello response: %w", err)
			}
			state.helloSeen = true
			if state.helloSeq == 0 {
				state.helloSeq = file.Seq
			}
		case bridge.TypeProbeNetwork:
			if !state.helloSeen {
				return false, fmt.Errorf("bridge startup probe received probe_network before hello")
			}
			if err := writeBridgeStartupResponse(ctx, inbox, envelope, bridge.TypeNoWork, map[string]string{"status": "probe_ok"}); err != nil {
				return false, fmt.Errorf("bridge startup probe response: %w", err)
			}
			state.probeSeen = true
			if state.probeSeq == 0 {
				state.probeSeq = file.Seq
			}
		case bridge.TypeClaimNextTurn, bridge.TypeResumeTurn, bridge.TypeAckTurnStarted, bridge.TypeEmitOutput, bridge.TypeAckTurnCompleted:
			return false, fmt.Errorf("bridge startup probe received %s before ready -> live", envelope.Type)
		default:
			return false, fmt.Errorf("bridge startup probe received unsupported message type %q", envelope.Type)
		}
		if err := file.Unlink(); err != nil {
			return false, fmt.Errorf("bridge startup probe unlink %s: %w", envelope.Type, err)
		}
		if state.ready() {
			return true, nil
		}
	}
	return state.ready(), nil
}

func validateBridgeStartupEnvelope(envelope bridge.Envelope, instance store.RuntimeResourceInstance) error {
	if strings.TrimSpace(envelope.MessageID) == "" {
		return fmt.Errorf("bridge startup probe envelope missing message_id")
	}
	if envelope.SessionID != instance.SessionID || envelope.GenerationID != instance.GenerationID {
		return fmt.Errorf("bridge startup probe identity mismatch: session=%s generation=%s, want session=%s generation=%s",
			envelope.SessionID, envelope.GenerationID, instance.SessionID, instance.GenerationID)
	}
	return nil
}

func writeBridgeStartupResponse(ctx context.Context, inbox bridge.Queue, request bridge.Envelope, responseType string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = inbox.Write(ctx, bridge.Envelope{
		RequestID:    bridgeRequestID(request),
		Type:         responseType,
		SessionID:    request.SessionID,
		GenerationID: request.GenerationID,
		TurnID:       request.TurnID,
		Payload:      raw,
	})
	return err
}

func bridgeRequestID(envelope bridge.Envelope) string {
	if strings.TrimSpace(envelope.RequestID) != "" {
		return envelope.RequestID
	}
	return envelope.MessageID
}

func bridgeHelloLastSequences(values map[int64]int64) map[string]int64 {
	out := make(map[string]int64, len(values))
	for turnID, sequence := range values {
		out[fmt.Sprint(turnID)] = sequence
	}
	return out
}

func (s bridgeStartupProbeState) ready() bool {
	return s.heartbeatSeen && s.helloSeen && s.probeSeen
}

func (s bridgeStartupProbeState) missing() string {
	missing := []string{}
	if !s.heartbeatSeen {
		missing = append(missing, "heartbeat")
	}
	if !s.helloSeen {
		missing = append(missing, "hello")
	}
	if !s.probeSeen {
		missing = append(missing, "probe_network")
	}
	return strings.Join(missing, ",")
}

func (s bridgeStartupProbeState) evidence() string {
	return fmt.Sprintf("bridge_startup_probe:passed; check=bridge_bootstrap; heartbeat_seq=%d; hello_seq=%d; probe_network_seq=%d",
		s.heartbeatSeq, s.helloSeq, s.probeSeq)
}

func runtimeResourcePostStartProof(instance store.RuntimeResourceInstance, result runtime.Result, bridgeStartupEvidence string) (store.RuntimeResourcePostStartProof, error) {
	if result.PostStartProof == nil {
		return store.RuntimeResourcePostStartProof{}, fmt.Errorf("runtime start did not return post-start proof for generation %s", instance.GenerationID)
	}
	proof := *result.PostStartProof
	proof.HostID = instance.HostID
	proof.ContractID = instance.ContractID
	proof.SandboxContractVersion = instance.SandboxContractVersion
	proof.BridgeStartup = strings.TrimSpace(bridgeStartupEvidence)
	if strings.TrimSpace(proof.GenerationID) == "" {
		proof.GenerationID = instance.GenerationID
	}
	if strings.TrimSpace(proof.RunscContainerID) == "" {
		proof.RunscContainerID = instance.RunscContainerID
	}
	return proof, nil
}

func runtimeResourceCleanupEvidence(instance store.RuntimeResourceInstance, cleanup runtime.GenerationResourceCleanup) store.ResourceReconciliationEvidence {
	filesystem := make(map[string]string, len(cleanup.FilesystemLstat))
	for path, value := range cleanup.FilesystemLstat {
		filesystem[path] = value
	}
	return store.ResourceReconciliationEvidence{
		HostID:          instance.HostID,
		RunscState:      cleanup.RunscState,
		IPNetns:         cleanup.IPNetns,
		IPLink:          cleanup.IPLink,
		NFT:             cleanup.NFT,
		FilesystemLstat: filesystem,
	}
}

func runtimeResourceWorkerID(ownerUUID, leaseOwner string) string {
	workerID := strings.TrimSpace(ownerUUID)
	if workerID != "" {
		return workerID
	}
	leaseOwner = strings.TrimSpace(leaseOwner)
	suffix := ":" + store.RuntimeManagerRoleTag
	if strings.HasSuffix(leaseOwner, suffix) {
		workerID = strings.TrimSpace(strings.TrimSuffix(leaseOwner, suffix))
	}
	if workerID == "" {
		workerID = leaseOwner
	}
	return workerID
}

func runtimeResourceHostID() string {
	host, err := os.Hostname()
	if err == nil && strings.TrimSpace(host) != "" {
		return strings.TrimSpace(host)
	}
	return "unknown-host"
}

func runtimeResourceSandboxIP(cidr string) (string, error) {
	cidr = strings.TrimSpace(cidr)
	prefix, err := netip.ParsePrefix(cidr)
	if err == nil {
		return prefix.Addr().String(), nil
	}
	if before, _, ok := strings.Cut(cidr, "/"); ok && strings.TrimSpace(before) != "" {
		return strings.TrimSpace(before), nil
	}
	return "", fmt.Errorf("runtime resource sandbox ip cidr %q is invalid: %w", cidr, err)
}

func runtimeResourceNftTableName(generationID string) string {
	return "harness_gen_" + runtimeResourceIdentifier(generationID)
}

func runtimeResourceIdentifier(value string) string {
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	out := b.String()
	if out == "" {
		return "unknown"
	}
	return out
}

func (s *Server) runtimeResourceRootPrefixes() map[string]string {
	roots := s.cfg.IsolationRoots()
	if strings.TrimSpace(s.cfg.DBPath) == "" {
		roots.DataVolumeEvidenceRoot = ""
	}
	if strings.TrimSpace(s.cfg.Harness.RunDir) == "" {
		roots.ProxyInternalRoot = ""
	}
	values := map[string]string{
		"sessions_root":             roots.SessionsRoot,
		"agent_homes_root":          roots.AgentHomesRoot,
		"run_dir":                   roots.RunDir,
		"checkpoints_root":          roots.CheckpointsRoot,
		"prepared_bundle_root":      roots.PreparedBundleRoot,
		"rootfs_path":               roots.RootFSPath,
		"db_path":                   roots.DBPath,
		"schema_pack_root":          roots.SchemaPackRoot,
		"data_volume_evidence_root": roots.DataVolumeEvidenceRoot,
		"proxy_internal_root":       roots.ProxyInternalRoot,
		"provider_credential_root":  roots.ProviderCredentialRoot,
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		if strings.TrimSpace(value) == "" {
			continue
		}
		out[key] = cleanRuntimeResourceRoot(value)
	}
	return out
}

func cleanRuntimeResourceRoot(path string) string {
	path = strings.TrimSpace(path)
	absolute, err := filepath.Abs(path)
	if err != nil {
		return filepath.Clean(path)
	}
	return filepath.Clean(absolute)
}

func runtimeFailureClass(message string) string {
	if strings.Contains(message, "sandbox_secret_disallowed") {
		return "sandbox_secret_disallowed"
	}
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
		strings.Contains(message, "bridge startup probe") ||
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
	case "sandbox_secret_disallowed":
		return "sandbox generation cannot mount model secrets"
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
			s.log.Warn("cold fallback session list failed", "error", err)
		}
		return
	}
	for _, fallback := range fallbacks {
		ensured, err := s.ensureActiveGeneration(ctx, fallback.Session, owner)
		if err != nil {
			if errors.Is(err, store.ErrPoolExhausted) {
				s.log.Warn("cold fallback pool exhausted", "session_id", fallback.Session.ID, "old_generation_id", fallback.OldGeneration, "queued_turns", fallback.QueuedTurns)
				return
			}
			if !errors.Is(err, context.Canceled) {
				s.log.Warn("cold fallback allocation failed", "session_id", fallback.Session.ID, "old_generation_id", fallback.OldGeneration, "error", err)
			}
			continue
		}
		if !ensured.IsNew {
			continue
		}
		if err := s.startEnsuredGeneration(ctx, fallback.Session, ensured, startFailureInputBlocking); err != nil {
			if !errors.Is(err, context.Canceled) {
				s.log.Warn("cold fallback start failed", "session_id", fallback.Session.ID, "old_generation_id", fallback.OldGeneration, "new_generation_id", ensured.Allocation.GenerationID, "error", err)
			}
			continue
		}
		if err := s.store.UpdateSessionStatusAndActivity(ctx, fallback.Session.ID, string(sessionstate.RunningActive), nil, time.Now().UTC()); err != nil {
			if !errors.Is(err, context.Canceled) {
				s.log.Warn("cold fallback status update failed", "session_id", fallback.Session.ID, "new_generation_id", ensured.Allocation.GenerationID, "error", err)
			}
			continue
		}
		s.hub.Publish(events.Event{Type: "session." + string(sessionstate.RunningActive), SessionID: fallback.Session.ID})
	}
}

func (s *Server) RunMaintenance(ctx context.Context) error {
	if strings.TrimSpace(s.ownerUUID) == "" {
		return fmt.Errorf("maintenance requires owner uuid")
	}
	heartbeatInterval := s.cfg.Harness.Bridge.HeartbeatInterval.Duration
	if heartbeatInterval <= 0 {
		heartbeatInterval = 30 * time.Second
	}
	pollInterval := s.cfg.Harness.Bridge.PollInterval.Duration
	if pollInterval <= 0 {
		pollInterval = 5 * time.Millisecond
	}
	owner := store.GenerationLeaseOwner(s.ownerUUID)
	processor := &bridge.Processor{
		Store:           bridgeStore(s.store),
		Owner:           owner,
		LeaseTTL:        s.cfg.Harness.Bridge.LeaseTTL.Duration,
		AckStartedGrace: s.cfg.Harness.Bridge.AckStartedGrace.Duration,
		AfterCommit:     s.handleBridgeCommittedEnvelope,
	}
	touchHostHeartbeat := func(generation store.BridgePollGeneration, now time.Time) {
		if err := bridge.TouchHeartbeat(generation.BridgeDirPath, bridge.HostHeartbeatFile, now); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("bridge host heartbeat failed", "session_id", generation.SessionID, "generation_id", generation.GenerationID, "error", err)
		}
	}

	runMaintenance := func(now time.Time) {
		if s.cfg.SessionRetention == 0 {
			if _, err := s.store.ClearActiveSessionExpiry(ctx, now); err != nil && !errors.Is(err, context.Canceled) {
				s.log.Warn("active-session expiry clear failed", "error", err)
			}
		}
		if _, err := s.store.SweepExpiredSessions(ctx, now); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("expired-session sweep failed", "error", err)
		}
		if _, err := s.store.CancelTerminalSessionPendingTurns(ctx, now); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("terminal-session turn cleanup failed", "error", err)
		}
		if _, err := s.RecoverExpiredRuntimeResources(ctx, now); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("allocation recovery failed", "error", err)
		}
		if _, err := s.store.RenewLiveGenerationLeases(ctx, store.RenewLiveGenerationsParams{
			Owner:    owner,
			LeaseTTL: s.cfg.Harness.Bridge.LeaseTTL.Duration,
			Now:      now,
		}); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("generation lease renewal failed", "error", err)
		}
		s.startColdFallbackSessions(ctx, owner)
		generations, err := s.store.ListBridgePollGenerations(ctx, owner, now, s.cfg.Harness.Bridge.AckStartedGrace.Duration)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				s.log.Warn("bridge heartbeat generation list failed", "error", err)
			}
		} else {
			for _, generation := range generations {
				touchHostHeartbeat(generation, now)
			}
		}
		retiredCheckpoints, err := s.store.RetireExpiredCheckpoints(ctx, store.RetireExpiredCheckpointsParams{
			OwnerUUID:                s.ownerUUID,
			Now:                      now,
			CheckpointImageRetention: s.cfg.Harness.Reaper.CheckpointImageRetention.Duration,
		})
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				s.log.Warn("checkpoint retirement failed", "error", err)
			}
		} else {
			for _, retired := range retiredCheckpoints {
				s.publishDurableEvent(ctx, retired.EventID)
			}
		}
		if _, err := s.store.ReapResources(ctx, store.ReaperParams{
			OwnerUUID:       s.ownerUUID,
			FailedRetention: s.cfg.Harness.Reaper.FailedRetention.Duration,
			Now:             now,
		}); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("resource reaper failed", "error", err)
		}
		s.destroyReclaimableGenerationResources(ctx, now)
		if _, err := s.store.PruneEvents(ctx, store.PruneEventsParams{
			RetentionWindow: s.cfg.Harness.Events.RetentionWindow.Duration,
			RetentionRows:   s.cfg.Harness.Events.RetentionRows,
			Now:             now,
		}); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("event retention prune failed", "error", err)
		}
	}
	pollBridge := func(now time.Time) {
		generations, err := s.store.ListBridgePollGenerations(ctx, owner, now, s.cfg.Harness.Bridge.AckStartedGrace.Duration)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				s.log.Warn("bridge generation list failed", "error", err)
			}
			return
		}
		for _, generation := range generations {
			processor.MarkReady(generation.SessionID, generation.GenerationID)
			if err := processor.ProcessOnce(ctx, generation.BridgeDirPath); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				s.log.Warn("bridge poll failed", "session_id", generation.SessionID, "generation_id", generation.GenerationID, "error", err)
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

func (s *Server) RecoverExpiredRuntimeResources(ctx context.Context, now time.Time) (store.StartupRecoveryResult, error) {
	if strings.TrimSpace(s.ownerUUID) == "" {
		return store.StartupRecoveryResult{}, fmt.Errorf("runtime recovery requires owner uuid")
	}
	params := store.StartupRecoveryParams{
		OwnerUUID:       s.ownerUUID,
		Now:             now,
		LeaseTTL:        s.cfg.Harness.Bridge.LeaseTTL.Duration,
		ReconnectGrace:  s.cfg.Harness.Bridge.ReconnectGrace.Duration,
		AckStartedGrace: s.cfg.Harness.Bridge.AckStartedGrace.Duration,
	}
	candidates, err := s.store.ListExpiredRuntimeRecoveryCandidates(ctx, params)
	if err != nil {
		return store.StartupRecoveryResult{}, err
	}
	cleaned := make([]store.ExpiredRuntimeRecoveryCandidate, 0, len(candidates))
	result := store.StartupRecoveryResult{}
	for _, candidate := range candidates {
		runtimeID := strings.TrimSpace(candidate.RuntimeID)
		if runtimeID == "" {
			result.RuntimeCleanupSkipped++
			s.log.Warn("recovery candidate has no runtime id", "session_id", candidate.SessionID, "generation_id", candidate.GenerationID)
			continue
		}
		if err := s.runtime.Destroy(ctx, runtimeID); err != nil {
			if errors.Is(err, context.Canceled) {
				return result, err
			}
			result.RuntimeCleanupSkipped++
			s.log.Warn("runtime cleanup before recovery failed", "session_id", candidate.SessionID, "generation_id", candidate.GenerationID, "runtime_id", runtimeID, "error", err)
			continue
		}
		cleaned = append(cleaned, candidate)
	}
	repaired, err := s.store.RepairExpiredRuntimeRecovery(ctx, params, cleaned)
	if err != nil {
		return result, err
	}
	repaired.RuntimeCleanupSkipped += result.RuntimeCleanupSkipped
	for _, eventID := range repaired.EventIDs {
		s.publishDurableEvent(ctx, eventID)
	}
	return repaired, nil
}

func (s *Server) DestroyReclaimableGenerationResources(ctx context.Context, now time.Time) {
	s.destroyReclaimableGenerationResources(ctx, now)
}

func (s *Server) destroyReclaimableGenerationResources(ctx context.Context, now time.Time) {
	candidates, err := s.store.ListDestroyableReclaimableGenerations(ctx, now, s.cfg.Harness.Reaper.FailedRetention.Duration)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			s.log.Warn("destroyable resource list failed", "error", err)
		}
		return
	}
	for _, candidate := range candidates {
		if err := s.cleanupGenerationResources(ctx, candidate.SessionID, candidate.GenerationID, now); err != nil && !errors.Is(err, context.Canceled) {
			s.log.Warn("generation resource cleanup failed", "session_id", candidate.SessionID, "generation_id", candidate.GenerationID, "error", err)
		}
	}
}

func (s *Server) cleanupGenerationResources(ctx context.Context, sessionID, generationID string, now time.Time) error {
	details, err := s.store.GetRuntimeGenerationDetails(ctx, sessionID, generationID)
	if err != nil {
		return fmt.Errorf("lookup generation resources: %w", err)
	}
	resourceInstance, resourceTracked, err := s.runtimeResourceCleanupIdentityIfExists(ctx, generationID)
	if err != nil {
		return fmt.Errorf("lookup runtime resource instance: %w", err)
	}
	if resourceTracked {
		if err := s.claimRuntimeResourceCleanup(ctx, resourceInstance, now); err != nil {
			return err
		}
		details = runtimeDetailsWithResourceInstance(details, resourceInstance)
	}
	cleanup, err := s.runtime.DestroyGenerationResources(ctx, details)
	if err != nil {
		return fmt.Errorf("destroy generation resources: %w", err)
	}
	if err := s.store.MarkGenerationResourcesDestroyed(ctx, store.DestroyGenerationResourcesParams{
		SessionID:    sessionID,
		GenerationID: generationID,
		Now:          now,
	}); err != nil {
		return fmt.Errorf("mark generation resources destroyed: %w", err)
	}
	if resourceTracked {
		if err := s.completeRuntimeResourceCleanup(ctx, resourceInstance, cleanup, now); err != nil {
			return fmt.Errorf("mark runtime resource destroyed: %w", err)
		}
	}
	return nil
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
		Stream  string          `json:"stream"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		s.log.Warn("failed to decode bridge output payload", "session_id", envelope.SessionID, "generation_id", envelope.GenerationID, "error", err)
		return
	}
	stream := payload.Stream
	if stream == "" {
		stream = "stdout"
	}
	if len(payload.Payload) == 0 {
		return
	}
	agent := ""
	if session, err := s.store.GetSession(ctx, envelope.SessionID); err == nil {
		agent = session.Agent
	} else {
		s.log.Warn("failed to load session for bridge output", "session_id", envelope.SessionID, "error", err)
	}
	parser := s.bridgeStreamParser(envelope, agent)
	parser.handleBridgeOutput(normalizerBridgeOutput{Stream: stream, Payload: payload.Payload})
}

func (s *Server) handleBridgeCompletion(ctx context.Context, envelope bridge.Envelope) {
	s.completeBridgeStreamParser(envelope)
	if err := s.watcher.ScanSession(ctx, envelope.SessionID); err != nil && !errors.Is(err, context.Canceled) {
		s.log.Warn("failed to scan bridge-completed session artifacts", "session_id", envelope.SessionID, "error", err)
	}
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

func (s *Server) requireProxyPeerCredentials(next http.Handler) http.Handler {
	expected := s.cfg.Harness.ProxyServiceIdentity
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result, ok := r.Context().Value(proxyPeerCredentialsContextKey{}).(proxyPeerCredentialsResult)
		if !ok {
			writeError(w, http.StatusForbidden, "proxy correlation peer credentials unavailable")
			return
		}
		if result.Err != nil {
			writeError(w, http.StatusForbidden, "proxy correlation peer credentials invalid")
			return
		}
		if result.Credentials.UID != expected.UID || result.Credentials.GID != expected.GID {
			writeError(w, http.StatusForbidden, "proxy correlation peer credentials rejected")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func unixPeerCredentials(conn net.Conn) (proxyPeerCredentials, error) {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return proxyPeerCredentials{}, fmt.Errorf("proxy correlation connection is not unix")
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		return proxyPeerCredentials{}, err
	}
	var credentials *syscall.Ucred
	var controlErr error
	if err := raw.Control(func(fd uintptr) {
		credentials, controlErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	}); err != nil {
		return proxyPeerCredentials{}, err
	}
	if controlErr != nil {
		return proxyPeerCredentials{}, controlErr
	}
	if credentials == nil {
		return proxyPeerCredentials{}, fmt.Errorf("proxy correlation peer credentials missing")
	}
	return proxyPeerCredentials{
		UID: int(credentials.Uid),
		GID: int(credentials.Gid),
		PID: int(credentials.Pid),
	}, nil
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
	file, info, status, message := s.openArtifactFile(r.Context(), parts[0], parts[1])
	if file == nil {
		writeError(w, status, message)
		return
	}
	defer file.Close()

	w.Header().Set("X-Content-Type-Options", "nosniff")
	http.ServeContent(w, r, info.Name(), info.ModTime(), file)
}

func (s *Server) openArtifactFile(ctx context.Context, sessionID, artifactPath string) (*os.File, os.FileInfo, int, string) {
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

	volumeConfig, err := s.dataVolumeProvisionerConfig()
	if err != nil {
		return nil, nil, http.StatusInternalServerError, err.Error()
	}
	workspace, err := s.store.VerifySessionWorkspaceVolume(ctx, store.VerifySessionWorkspaceVolumeParams{
		SessionID: sessionID,
		Config:    volumeConfig,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, http.StatusNotFound, "artifact not found"
	}
	if err != nil {
		return nil, nil, http.StatusForbidden, "workspace evidence invalid"
	}

	sessionRoot := workspace.HostPath
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

func (s *Server) dataVolumeProvisionerConfig() (store.DataVolumeProvisionerConfig, error) {
	roots, err := config.ValidateIsolationRoots(s.cfg.IsolationRoots())
	if err != nil {
		return store.DataVolumeProvisionerConfig{}, err
	}
	identity := s.cfg.Harness.SandboxIdentity
	return store.DataVolumeProvisionerConfig{
		SessionsRoot:   roots.SessionsRoot,
		AgentHomesRoot: roots.AgentHomesRoot,
		EvidenceRoot:   roots.DataVolumeEvidenceRoot,
		LayoutVersion:  store.DataVolumeLayoutVersion,
		RuntimeIdentity: store.RuntimeIdentity{
			UID:              identity.UID,
			GID:              identity.GID,
			SupplementalGIDs: identity.SupplementalGIDs,
		},
	}, nil
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
		if err := conn.WriteJSON(publicEvent(event)); err != nil {
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
	if !s.cfg.Harness.Checkpoint.AutoEnabled {
		s.log.Info("idle checkpoint monitor disabled by policy")
		return nil
	}
	if strings.TrimSpace(s.ownerUUID) == "" {
		return fmt.Errorf("idle checkpoint monitor requires owner uuid")
	}

	owner := store.GenerationLeaseOwner(s.ownerUUID)
	interval := s.cfg.Harness.Checkpoint.MonitorInterval.Duration
	if interval <= 0 {
		interval = idleCheckpointInterval
	}
	idleThreshold := s.cfg.Harness.Checkpoint.IdleThreshold.Duration
	if idleThreshold < 0 {
		idleThreshold = idleCheckpointThreshold
	}
	heartbeatInterval := s.cfg.Harness.Bridge.HeartbeatInterval.Duration
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
		Generation:     details,
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
		RunscBinaryPath:                 details.RunscBinaryPath,
		RunscBinaryDigest:               details.RunscBinaryDigest,
		CheckpointBundleDigest:          details.BundleDigest,
		CheckpointRuntimeConfigDigest:   details.RuntimeConfigDigest,
		CheckpointControlManifestDigest: details.ProjectedControlManifestDigest,
		Now:                             completeNow,
	}); err != nil {
		return err
	}
	if err := s.reserveRuntimeResourceCheckpoint(ctx, candidate.GenerationID); err != nil {
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
