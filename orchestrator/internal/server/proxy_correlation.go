package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/store"
)

const proxyCorrelationSocket = "proxy-correlation.sock"

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

func (s *Server) ProxyCorrelationRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /internal/proxy/requests/start", s.requireProxyPeerCredentials(http.HandlerFunc(s.internalProxyRequestStart)))
	mux.Handle("POST /internal/proxy/requests/finish", s.requireProxyPeerCredentials(http.HandlerFunc(s.internalProxyRequestFinish)))
	return mux
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
