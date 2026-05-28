package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"harness-platform/orchestrator/internal/artifacts"
	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/server"
	"harness-platform/orchestrator/internal/store"
)

func main() {
	logLevel := new(slog.LevelVar)
	if os.Getenv("HARNESS_LOG_LEVEL") == "debug" {
		logLevel.Set(slog.LevelDebug)
	} else {
		logLevel.Set(slog.LevelInfo)
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	signal.Ignore(syscall.SIGPIPE)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	if _, err := config.ValidatePhase8IsolationRoots(cfg.Phase8IsolationRoots()); err != nil {
		log.Error("invalid phase8 isolation roots", "error", err)
		os.Exit(1)
	}
	for _, warning := range cfg.Warnings {
		log.Warn("config warning", "warning", warning)
	}

	owner, err := store.AcquireOwnerLock(cfg.Phase7.RunDir)
	if err != nil {
		log.Error("failed to acquire orchestrator owner lock", "error", err)
		os.Exit(1)
	}
	defer owner.Close()

	db, err := store.OpenWithOptions(ctx, cfg.DBPath, store.Options{AgentHomesRoot: cfg.AgentHomesRoot})
	if err != nil {
		log.Error("failed to open store", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := db.WriteOwner(ctx, owner); err != nil {
		log.Error("failed to write orchestrator owner", "error", err)
		os.Exit(1)
	}
	var app *server.Server
	var watcher *artifacts.Watcher
	startup, err := runStartupRuntimeRecovery(ctx, cfg, owner.UUID, startupRuntimeRecoveryHooks{
		ClearActiveSessionExpiry: db.ClearActiveSessionExpiry,
		ConstructRuntime: func() error {
			rt := runtime.New(runtime.Config{
				RestoreScript:           cfg.RestoreScript,
				RunscRoot:               cfg.RunscRoot,
				RunscNetwork:            cfg.RunscNetwork,
				RunscOverlay2:           cfg.RunscOverlay2,
				SessionsRoot:            cfg.SessionsRoot,
				AgentHomesRoot:          cfg.AgentHomesRoot,
				CheckpointsRoot:         cfg.CheckpointsRoot,
				BundleRoot:              cfg.BundleRoot,
				RootFSPath:              cfg.RootFSPath,
				DefaultAgent:            cfg.DefaultAgent,
				SandboxUID:              cfg.Phase7.SandboxIdentity.UID,
				SandboxGID:              cfg.Phase7.SandboxIdentity.GID,
				SandboxSupplementalGIDs: append([]int(nil), cfg.Phase7.SandboxIdentity.SupplementalGIDs...),
				RunDir:                  cfg.Phase7.RunDir,
				PreStartProbeAttempts:   cfg.Phase7.Probe.PreStartAttempts,
				PreStartProbeInterval:   cfg.Phase7.Probe.PreStartInterval.Duration,
				ProbeHealthzStatuses:    cfg.Phase7.Probe.AcceptStatus.GetHealthz,
				BridgeHeartbeat:         cfg.Phase7.Bridge.HeartbeatInterval.Duration,
				BridgePollInterval:      cfg.Phase7.Bridge.PollInterval.Duration,
				Claude: runtime.ClaudeConfig{
					ProxyBindURL:               cfg.Claude.ProxyBindURL,
					APIKey:                     cfg.Claude.APIKey,
					AuthToken:                  cfg.Claude.AuthToken,
					Model:                      cfg.Claude.Model,
					OutputFormat:               cfg.Claude.OutputFormat,
					DisableNonessentialTraffic: cfg.Claude.DisableNonessentialTraffic,
				},
			})
			hub := events.NewHub()
			volumeConfig, err := dataVolumeProvisionerConfig(cfg)
			if err != nil {
				return err
			}
			watcher = artifacts.New(volumeConfig, db, hub, log)
			app = server.New(cfg, db, rt, watcher, hub, log)
			app.SetOwnerUUID(owner.UUID)
			return nil
		},
		RecoverExpiredRuntimeResources: func(ctx context.Context, now time.Time) (store.StartupRecoveryResult, error) {
			return app.RecoverExpiredRuntimeResources(ctx, now)
		},
		ReapResources: db.ReapResources,
		DestroyReclaimableGenerationResources: func(ctx context.Context, now time.Time) {
			app.DestroyReclaimableGenerationResources(ctx, now)
		},
	})
	if err != nil {
		log.Error("failed startup runtime recovery", "error", err)
		os.Exit(1)
	}
	if startup.ClearedActiveSessionExpiries > 0 {
		log.Info("cleared active session expiry", "sessions", startup.ClearedActiveSessionExpiries)
	}
	ownerHeartbeatErr := db.StartOwnerHeartbeat(ctx, owner)
	if err := db.EnsureUser(ctx, "lab", "Lab User"); err != nil {
		log.Error("failed to ensure lab user", "error", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(cfg.AgentHomesRoot, 0o755); err != nil {
		log.Error("failed to create agent homes root", "error", err)
		os.Exit(1)
	}

	go func() {
		if err := <-ownerHeartbeatErr; err != nil {
			log.Error("orchestrator owner heartbeat failed", "error", err)
			stop()
		}
	}()

	go func() {
		if err := watcher.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("artifact watcher stopped", "error", err)
		}
	}()

	// Start idle session monitoring
	go func() {
		if err := app.MonitorIdleSessions(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("idle session monitor stopped", "error", err)
		}
	}()
	go func() {
		if err := app.RunPhase7Maintenance(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("phase7 maintenance stopped", "error", err)
		}
	}()

	httpServer := &http.Server{
		Addr:              cfg.Addr,
		Handler:           app.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	proxyCorrelationServer := app.ProxyCorrelationServer()
	proxyCorrelationListener, proxyCorrelationSocket, err := app.ListenProxyCorrelation()
	if err != nil {
		log.Error("failed to listen on proxy correlation socket", "error", err)
		os.Exit(1)
	}

	go func() {
		log.Info("orchestrator listening", "addr", cfg.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server stopped", "error", err)
			stop()
		}
	}()
	go func() {
		log.Info("proxy correlation listening", "socket", proxyCorrelationSocket)
		if err := proxyCorrelationServer.Serve(proxyCorrelationListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("proxy correlation server stopped", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := proxyCorrelationServer.Shutdown(shutdownCtx); err != nil {
		log.Warn("proxy correlation shutdown failed", "error", err)
	}
	_ = os.Remove(proxyCorrelationSocket)
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown failed", "error", err)
	}
}

func dataVolumeProvisionerConfig(cfg config.Config) (store.DataVolumeProvisionerConfig, error) {
	roots, err := config.ValidatePhase8IsolationRoots(cfg.Phase8IsolationRoots())
	if err != nil {
		return store.DataVolumeProvisionerConfig{}, err
	}
	identity := cfg.Phase7.SandboxIdentity
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
