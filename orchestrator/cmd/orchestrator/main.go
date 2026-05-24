package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
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
	if _, err := db.RecoverAllocations(ctx, store.StartupRecoveryParams{
		OwnerUUID:      owner.UUID,
		Now:            time.Now().UTC(),
		LeaseTTL:       cfg.Phase7.Bridge.LeaseTTL.Duration,
		ReconnectGrace: cfg.Phase7.Bridge.ReconnectGrace.Duration,
	}); err != nil {
		log.Error("failed to recover phase7 allocations", "error", err)
		os.Exit(1)
	}
	if _, err := db.ReapResources(ctx, store.ReaperParams{
		OwnerUUID:       owner.UUID,
		FailedRetention: cfg.Phase7.Reaper.FailedRetention.Duration,
		Now:             time.Now().UTC(),
	}); err != nil {
		log.Error("failed to reap phase7 resources", "error", err)
		os.Exit(1)
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

	hub := events.NewHub()
	watcher := artifacts.New(cfg.SessionsRoot, db, hub, log)
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

	rt := runtime.New(runtime.Config{
		RestoreScript:         cfg.RestoreScript,
		RunscRoot:             cfg.RunscRoot,
		RunscNetwork:          cfg.RunscNetwork,
		RunscOverlay2:         cfg.RunscOverlay2,
		SessionsRoot:          cfg.SessionsRoot,
		AgentHomesRoot:        cfg.AgentHomesRoot,
		CheckpointsRoot:       cfg.CheckpointsRoot,
		BundleRoot:            cfg.BundleRoot,
		RootFSPath:            filepath.Join(cfg.RepoRoot, "sandbox-image", "rootfs"),
		DefaultAgent:          cfg.DefaultAgent,
		Phase7RunDir:          cfg.Phase7.RunDir,
		SecretsRoot:           cfg.Phase7.Secrets.Root,
		SecretReadersGID:      cfg.Phase7.Secrets.ReadersGID,
		PreStartProbeAttempts: cfg.Phase7.Probe.PreStartAttempts,
		PreStartProbeInterval: cfg.Phase7.Probe.PreStartInterval.Duration,
		ProbeHealthzStatuses:  cfg.Phase7.Probe.AcceptStatus.GetHealthz,
		ProbeMessageStatuses:  cfg.Phase7.Probe.AcceptStatus.PostV1Messages.MalformedAuthenticated,
		BridgeHeartbeat:       cfg.Phase7.Bridge.HeartbeatInterval.Duration,
		Claude: runtime.ClaudeConfig{
			ProxyBindURL:               cfg.Claude.ProxyBindURL,
			APIKey:                     cfg.Claude.APIKey,
			AuthToken:                  cfg.Claude.AuthToken,
			Model:                      cfg.Claude.Model,
			OutputFormat:               cfg.Claude.OutputFormat,
			DisableNonessentialTraffic: cfg.Claude.DisableNonessentialTraffic,
		},
	})
	app := server.New(cfg, db, rt, watcher, hub, log)
	app.SetOwnerUUID(owner.UUID)

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

	go func() {
		log.Info("orchestrator listening", "addr", cfg.Addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server stopped", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown failed", "error", err)
	}
}
