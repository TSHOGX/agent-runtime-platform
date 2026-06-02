package server

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

func TestMonitorIdleSessionsSkipsHostNetwork(t *testing.T) {
	srv := &Server{
		cfg: config.Config{
			RunscNetwork: "host",
		},
		log: slog.Default(),
	}

	if err := srv.MonitorIdleSessions(context.Background()); err != nil {
		t.Fatalf("expected idle monitor to exit cleanly in host mode, got %v", err)
	}
}

func TestMonitorIdleSessionsReEnablesCheckpointedSessionsWhenCheckpointDisabled(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	now := time.Now().UTC()
	session := store.Session{
		ID:        "sess_checkpointed",
		UserID:    "lab",
		Status:    string(sessionstate.Checkpointed),
		DriverID:  "claude_code",
		Mode:      store.ModeForDriver("claude_code"),
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := st.CreateSession(ctx, session); err != nil {
		t.Fatalf("create session: %v", err)
	}

	srv := &Server{
		cfg: config.Config{
			RunscNetwork: "sandbox",
		},
		store: st,
		hub:   events.NewHub(),
		log:   slog.Default(),
	}
	if err := srv.MonitorIdleSessions(context.Background()); err != nil {
		t.Fatalf("monitor idle sessions: %v", err)
	}
	got, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.Status != string(sessionstate.Checkpointed) {
		t.Fatalf("disabled monitor should leave checkpointed session alone, got %s", got.Status)
	}
}

func TestBridgeCheckpointReadyRequiresFreshHeartbeatAndMarker(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC()

	if bridgeCheckpointReady(dir, now, time.Second) {
		t.Fatal("empty bridge dir should not be checkpoint-ready")
	}
	if err := bridge.TouchHeartbeat(dir, bridge.BridgeHeartbeatFile, now); err != nil {
		t.Fatalf("touch heartbeat: %v", err)
	}
	if bridgeCheckpointReady(dir, now, time.Second) {
		t.Fatal("missing ready marker should not be checkpoint-ready")
	}
	if err := bridge.TouchCheckpointReady(dir, now); err != nil {
		t.Fatalf("touch ready: %v", err)
	}
	if !bridgeCheckpointReady(dir, now, time.Second) {
		t.Fatal("fresh heartbeat and ready marker should be checkpoint-ready")
	}
	if bridgeCheckpointReady(dir, now, 0) {
		t.Fatal("non-positive heartbeat interval should not be checkpoint-ready")
	}
	if bridgeCheckpointReady(dir, now.Add(10*time.Second), time.Second) {
		t.Fatal("stale bridge control files should not be checkpoint-ready")
	}
}

func TestMonitorIdleSessionsRequiresPositiveTimingConfig(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)

	tests := []struct {
		name string
		edit func(*config.Config)
		want string
	}{
		{
			name: "monitor interval",
			edit: func(cfg *config.Config) {
				cfg.Harness.Checkpoint.MonitorInterval = config.Duration{}
			},
			want: "checkpoint monitor interval must be > 0",
		},
		{
			name: "idle threshold",
			edit: func(cfg *config.Config) {
				cfg.Harness.Checkpoint.IdleThreshold = config.Duration{}
			},
			want: "checkpoint idle threshold must be > 0",
		},
		{
			name: "bridge heartbeat interval",
			edit: func(cfg *config.Config) {
				cfg.Harness.Bridge.HeartbeatInterval = config.Duration{}
			},
			want: "bridge heartbeat interval must be > 0",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testServerConfig(filepath.Join(dir, tc.name))
			cfg.Harness.Checkpoint.AutoEnabled = true
			cfg.Harness.Checkpoint.MonitorInterval = config.Duration{Duration: time.Minute}
			cfg.Harness.Checkpoint.IdleThreshold = config.Duration{Duration: time.Minute}
			tc.edit(&cfg)
			srv := &Server{
				cfg:   cfg,
				store: st,
				hub:   events.NewHub(),
				log:   slog.Default(),
			}
			srv.SetOwnerUUID(owner.UUID)
			err := srv.MonitorIdleSessions(ctx)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("monitor err=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestMonitorIdleSessionsCheckpointsEligibleGeneration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_auto_checkpoint", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	enableSessionAutoCheckpoint(t, ctx, st, session.ID)
	cfg := testServerConfig(dir)
	cfg.Harness.Checkpoint.AutoEnabled = true
	cfg.Harness.Checkpoint.IdleThreshold = config.Duration{Duration: time.Nanosecond}
	cfg.Harness.Checkpoint.MonitorInterval = config.Duration{Duration: time.Hour}
	allocation := prepareServerIdleGeneration(t, ctx, st, cfg, owner.UUID, session.ID)
	details, err := st.GetRuntimeGenerationDetails(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	if err := bridge.TouchHeartbeat(details.BridgeDirPath, bridge.BridgeHeartbeatFile, time.Now().UTC()); err != nil {
		t.Fatalf("touch heartbeat: %v", err)
	}
	if err := bridge.TouchCheckpointReady(details.BridgeDirPath, time.Now().UTC()); err != nil {
		t.Fatalf("touch ready: %v", err)
	}
	mutateServerRuntimeArtifactDigestMirrors(t, ctx, st, allocation.GenerationID)
	rt := &recordingRuntime{}
	runCtx, cancel := context.WithCancel(ctx)
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.MonitorIdleSessions(runCtx)
	}()
	waitForSessionStatus(t, ctx, st, session.ID, string(sessionstate.Checkpointed))
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("monitor exit err=%v, want context canceled", err)
	}

	checkpoints := rt.checkpointRequests()
	if len(checkpoints) != 1 {
		t.Fatalf("checkpoint requests=%d want 1: %+v", len(checkpoints), checkpoints)
	}
	if checkpoints[0].SessionID != session.ID ||
		checkpoints[0].GenerationID != allocation.GenerationID ||
		checkpoints[0].CheckpointPath != details.CheckpointPath ||
		checkpoints[0].Generation.GenerationID != allocation.GenerationID ||
		checkpoints[0].Generation.RunscContainerID != details.RunscContainerID ||
		checkpoints[0].Generation.RunscOverlay2 != details.RunscOverlay2 {
		t.Fatalf("unexpected checkpoint request: %+v details=%+v", checkpoints[0], details)
	}
	plan, err := st.GetGenerationPlan(ctx, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get checkpointed plan: %v", err)
	}
	var generationStatus, networkState, resourceState, checkpointPath, checkpointBundle, checkpointRuntimeConfig, checkpointManifest, checkpointPlan, checkpointImageManifest string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state, COALESCE(r.checkpoint_path, ''),
       COALESCE(g.checkpoint_bundle_digest, ''), COALESCE(g.checkpoint_runtime_config_digest, ''), COALESCE(g.checkpoint_control_manifest_digest, ''),
       COALESCE(g.checkpoint_plan_digest, ''), COALESCE(g.checkpoint_image_manifest_digest, '')
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(
		&generationStatus, &networkState, &resourceState, &checkpointPath,
		&checkpointBundle, &checkpointRuntimeConfig, &checkpointManifest, &checkpointPlan, &checkpointImageManifest,
	); err != nil {
		t.Fatalf("query checkpointed generation: %v", err)
	}
	if generationStatus != "checkpointed" ||
		networkState != "reserved_checkpointed" ||
		resourceState != "reserved_checkpointed" ||
		checkpointPath != details.CheckpointPath ||
		checkpointBundle != "bundle_digest" ||
		checkpointRuntimeConfig != "runtime_config_digest" ||
		checkpointManifest != "projected_manifest_digest" ||
		checkpointPlan != plan.PlanDigest ||
		checkpointImageManifest != checkpointImageManifestDigestForTest {
		t.Fatalf("unexpected checkpoint metadata: generation=%s network=%s resource=%s path=%s bundle=%s runtime=%s manifest=%s plan=%s image_manifest=%s want_plan=%s",
			generationStatus, networkState, resourceState, checkpointPath, checkpointBundle, checkpointRuntimeConfig, checkpointManifest, checkpointPlan, checkpointImageManifest, plan.PlanDigest)
	}
}

func TestMonitorIdleSessionsAbortsFailedCheckpoint(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_auto_checkpoint_fail", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	enableSessionAutoCheckpoint(t, ctx, st, session.ID)
	cfg := testServerConfig(dir)
	cfg.Harness.Checkpoint.AutoEnabled = true
	cfg.Harness.Checkpoint.IdleThreshold = config.Duration{Duration: time.Nanosecond}
	cfg.Harness.Checkpoint.MonitorInterval = config.Duration{Duration: time.Hour}
	allocation := prepareServerIdleGeneration(t, ctx, st, cfg, owner.UUID, session.ID)
	details, err := st.GetRuntimeGenerationDetails(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	if err := bridge.TouchHeartbeat(details.BridgeDirPath, bridge.BridgeHeartbeatFile, time.Now().UTC()); err != nil {
		t.Fatalf("touch heartbeat: %v", err)
	}
	if err := bridge.TouchCheckpointReady(details.BridgeDirPath, time.Now().UTC()); err != nil {
		t.Fatalf("touch ready: %v", err)
	}
	rt := &recordingRuntime{checkpointErr: errors.New("checkpoint boom")}
	runCtx, cancel := context.WithCancel(ctx)
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.MonitorIdleSessions(runCtx)
	}()
	waitForCheckpointRequests(t, ctx, rt, 1)
	waitForGenerationStatus(t, ctx, st, allocation.GenerationID, "idle")
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("monitor exit err=%v, want context canceled", err)
	}
	var generationStatus, networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &networkState, &resourceState); err != nil {
		t.Fatalf("query aborted generation: %v", err)
	}
	if generationStatus != "idle" || networkState != "live" || resourceState != "live" {
		t.Fatalf("checkpoint failure should return generation live idle, got generation=%s network=%s resource=%s", generationStatus, networkState, resourceState)
	}
}
