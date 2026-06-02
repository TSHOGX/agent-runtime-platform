package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/netip"
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

func TestRunMaintenanceRequiresPositiveBridgeIntervals(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)

	tests := []struct {
		name string
		edit func(*config.Config)
		want string
	}{
		{
			name: "heartbeat",
			edit: func(cfg *config.Config) {
				cfg.Harness.Bridge.HeartbeatInterval = config.Duration{}
			},
			want: "bridge heartbeat interval must be > 0",
		},
		{
			name: "poll",
			edit: func(cfg *config.Config) {
				cfg.Harness.Bridge.PollInterval = config.Duration{}
			},
			want: "bridge poll interval must be > 0",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testServerConfig(filepath.Join(dir, tc.name))
			tc.edit(&cfg)
			srv := &Server{
				cfg:   cfg,
				store: st,
				hub:   events.NewHub(),
				log:   slog.Default(),
			}
			srv.SetOwnerUUID(owner.UUID)
			err := srv.RunMaintenance(ctx)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("maintenance err=%v, want %q", err, tc.want)
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

func TestRunMaintenanceDoesNotColdStartFailedActiveGenerationWithQueuedTurn(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_maintenance_no_restart", string(sessionstate.RunningActive), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	cfg.Harness.Bridge.HeartbeatInterval = config.Duration{Duration: time.Hour}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate old generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark old generation live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, session.ID, old, owner.UUID, mustRuntimeResourceHostID(t), time.Now().UTC())
	if err := st.FailGeneration(ctx, store.FailGenerationParams{
		SessionID:    session.ID,
		GenerationID: old.GenerationID,
		Owner:        old.Owner,
		ErrorClass:   "orchestrator_restart_reconnect_grace_expired",
		Reason:       "orchestrator_restart_reconnect_grace_expired",
		Now:          time.Now().UTC(),
	}); err != nil {
		t.Fatalf("fail old generation: %v", err)
	}
	turnID, err := st.EnqueueTurn(ctx, session.ID, "protected queued turn", time.Now().UTC())
	if err != nil {
		t.Fatalf("enqueue queued turn: %v", err)
	}

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
	done := make(chan error, 1)
	go func() {
		done <- srv.RunMaintenance(runCtx)
	}()
	waitForGenerationResourceStates(t, runCtx, st, old.GenerationID, "destroyed", "destroyed")
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("maintenance exit err=%v, want context canceled", err)
	}

	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if gotSession.Status != string(sessionstate.RunningActive) ||
		sessionstate.CanAcceptInput(gotSession.Status) ||
		gotSession.ActiveGenerationID != old.GenerationID {
		t.Fatalf("maintenance should not replace failed active generation: %+v old=%s", gotSession, old.GenerationID)
	}
	var generationCount int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM runtime_generations
WHERE session_id = ?`, session.ID).Scan(&generationCount); err != nil {
		t.Fatalf("count generations: %v", err)
	}
	if generationCount != 1 {
		t.Fatalf("maintenance allocated replacement generations, count=%d", generationCount)
	}
	var generationStatus, networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, old.GenerationID).Scan(&generationStatus, &networkState, &resourceState); err != nil {
		t.Fatalf("query failed generation: %v", err)
	}
	if generationStatus != "failed" ||
		networkState != "destroyed" ||
		resourceState != "destroyed" {
		t.Fatalf("unexpected failed generation after maintenance: status=%s network=%s resource=%s", generationStatus, networkState, resourceState)
	}
	var queuedStatus string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT status
FROM turns
WHERE id = ?`, turnID).Scan(&queuedStatus); err != nil {
		t.Fatalf("query queued turn: %v", err)
	}
	if queuedStatus != "queued" {
		t.Fatalf("queued turn status=%s want queued", queuedStatus)
	}
	if _, starts := rt.requests(); len(starts) != 0 {
		t.Fatalf("maintenance should not cold-start failed active generation with queued turn: %+v", starts)
	}
}

func TestRunMaintenanceRetiresExpiredCheckpoint(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_retire_checkpoint", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Reaper.CheckpointImageRetention = config.Duration{Duration: 0}
	cfg.Harness.Reaper.FailedRetention = config.Duration{Duration: time.Hour}
	allocation := prepareServerIdleGeneration(t, ctx, st, cfg, owner.UUID, session.ID)
	checkpointedAt := time.Now().UTC().Add(-2 * time.Hour)
	markServerGenerationCheckpointed(t, ctx, st, session.ID, allocation.GenerationID, checkpointedAt)
	checkpointPath := filepath.Join(dir, "checkpoint", session.ID)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE sessions
SET checkpoint_path = ?,
    restore_ms = 99,
    last_activity_at = ?
WHERE id = ?`, checkpointPath, checkpointedAt.Format(time.RFC3339Nano), session.ID); err != nil {
		t.Fatalf("seed checkpoint session metadata: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generation_resources
SET checkpoint_path = ?
WHERE generation_id = ?`, checkpointPath, allocation.GenerationID); err != nil {
		t.Fatalf("seed checkpoint resource path: %v", err)
	}

	hub := events.NewHub()
	eventsCh, cancelEvents := hub.Subscribe(session.ID)
	defer cancelEvents()
	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, hub),
		hub:     hub,
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- srv.RunMaintenance(runCtx)
	}()
	event := waitForHubEvent(t, eventsCh, "session.checkpoint_retired")
	waitForGenerationResourceStates(t, runCtx, st, allocation.GenerationID, "destroyed", "destroyed")
	cancel()
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("maintenance exit err=%v, want context canceled", err)
	}
	payload, ok := event.Payload.(json.RawMessage)
	if !ok || strings.Contains(string(payload), `"checkpoint_path"`) || !strings.Contains(string(payload), `"restore_ms":null`) {
		t.Fatalf("unexpected checkpoint retirement event payload: %#v", event.Payload)
	}

	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get retired session: %v", err)
	}
	if gotSession.Status != string(sessionstate.RunningIdle) || gotSession.CheckpointPath != "" || gotSession.RestoreMS != nil {
		t.Fatalf("unexpected retired session: %+v", gotSession)
	}
	var generationStatus, generationError, networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.error_class, ''), n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &generationError, &networkState, &resourceState); err != nil {
		t.Fatalf("query retired generation: %v", err)
	}
	if generationStatus != "failed" || generationError != "checkpoint_retired" || networkState != "destroyed" || resourceState != "destroyed" {
		t.Fatalf("unexpected retired generation: status=%s error=%s network=%s resource=%s", generationStatus, generationError, networkState, resourceState)
	}
	destroyRequests := rt.destroyGenerationRequests()
	if len(destroyRequests) != 1 || destroyRequests[0].GenerationID != allocation.GenerationID {
		t.Fatalf("unexpected destroy generation requests: %+v", destroyRequests)
	}
}

func TestEnsureActiveGenerationColdStartsAfterCheckpointRetirement(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_retire_then_send", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	allocation := prepareServerIdleGeneration(t, ctx, st, cfg, owner.UUID, session.ID)
	checkpointedAt := time.Now().UTC().Add(-2 * time.Hour)
	markServerGenerationCheckpointed(t, ctx, st, session.ID, allocation.GenerationID, checkpointedAt)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE sessions
SET checkpoint_path = ?,
    last_activity_at = ?
WHERE id = ?`, filepath.Join(dir, "checkpoint", session.ID), checkpointedAt.Format(time.RFC3339Nano), session.ID); err != nil {
		t.Fatalf("seed checkpoint session metadata: %v", err)
	}
	staleCheckpointedSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get checkpointed session: %v", err)
	}
	if _, err := st.RetireExpiredCheckpoints(ctx, store.RetireExpiredCheckpointsParams{
		OwnerUUID:                owner.UUID,
		Now:                      time.Now().UTC(),
		CheckpointImageRetention: time.Hour,
	}); err != nil {
		t.Fatalf("retire checkpoint: %v", err)
	}

	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: &recordingRuntime{},
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	ensured, err := srv.ensureActiveGeneration(ctx, staleCheckpointedSession, store.GenerationLeaseOwner(owner.UUID))
	if err != nil {
		t.Fatalf("ensure active generation after retirement: %v", err)
	}
	if !ensured.IsNew || ensured.RestoreFromCheckpoint || ensured.Allocation.GenerationID == allocation.GenerationID {
		t.Fatalf("ensure should cold-start replacement after checkpoint retirement: %+v old=%s", ensured, allocation.GenerationID)
	}
	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get replacement session: %v", err)
	}
	if gotSession.ActiveGenerationID != ensured.Allocation.GenerationID {
		t.Fatalf("session active generation=%s want replacement %s", gotSession.ActiveGenerationID, ensured.Allocation.GenerationID)
	}
}

func TestRunMaintenanceRecoversGenerationThatExpiresAfterStartup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_expiring_generation", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Bridge.HeartbeatInterval = config.Duration{Duration: 10 * time.Millisecond}
	cfg.Harness.Bridge.ReconnectGrace = config.Duration{Duration: 20 * time.Millisecond}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, session.ID, allocation, owner.UUID, mustRuntimeResourceHostID(t), time.Now().UTC())
	expiresAt := time.Now().UTC().Add(25 * time.Millisecond)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET lease_owner = ?,
    lease_expires_at = ?,
    last_seen_at = ?
WHERE generation_id = ?`,
		store.GenerationLeaseOwner("previous-owner"),
		expiresAt.Format(time.RFC3339Nano),
		expiresAt.Add(-time.Minute).Format(time.RFC3339Nano),
		allocation.GenerationID,
	); err != nil {
		t.Fatalf("move generation to previous owner: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	done := make(chan error, 1)
	go func() {
		done <- srv.RunMaintenance(runCtx)
	}()

	waitForGenerationStatus(t, runCtx, st, allocation.GenerationID, "failed")
	cancel()
	err = <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("maintenance exit err=%v, want context canceled", err)
	}

	var errorClass, leaseOwnerAfter string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COALESCE(error_class, ''), COALESCE(lease_owner, '')
FROM runtime_generations
WHERE generation_id = ?`, allocation.GenerationID).Scan(&errorClass, &leaseOwnerAfter); err != nil {
		t.Fatalf("query recovered generation: %v", err)
	}
	if errorClass != "orchestrator_restart_reconnect_grace_expired" || leaseOwnerAfter != "" {
		t.Fatalf("unexpected recovered generation: error_class=%s lease_owner=%s", errorClass, leaseOwnerAfter)
	}
	runscID := serverRunscContainerID(t, ctx, st, session.ID, allocation.GenerationID)
	if got := rt.runtimeDestroyRequests(); len(got) != 1 || got[0] != runscID {
		t.Fatalf("maintenance should destroy runtime before repair using runsc container id %q, got %+v", runscID, got)
	}
	if _, starts := rt.requests(); len(starts) != 0 {
		t.Fatalf("maintenance should not cold-start without a queued turn: %+v", starts)
	}
}

func TestRunMaintenanceRecoversCurrentOwnerExpiredLeasedTurn(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	now := time.Now().UTC()
	session := createServerTestSession(t, ctx, st, dir, "sess_current_owner_expired", string(sessionstate.RunningActive), now.Add(-2*time.Minute), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Bridge.HeartbeatInterval = config.Duration{Duration: 10 * time.Millisecond}
	cfg.Harness.Bridge.ReconnectGrace = config.Duration{Duration: 20 * time.Millisecond}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       now.Add(-2 * time.Minute),
		Config:    serverTestAllocatorConfig(cfg, session.DriverID),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, now.Add(-2*time.Minute+time.Second)); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, session.ID, allocation, owner.UUID, mustRuntimeResourceHostID(t), now.Add(-2*time.Minute+2*time.Second))
	turnID, err := st.EnqueueTurn(ctx, session.ID, "hi", now.Add(-2*time.Minute+3*time.Second))
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	expiredAt := now.Add(-time.Minute)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'active',
    lease_owner = ?,
    lease_expires_at = ?,
    last_seen_at = ?
WHERE generation_id = ?`,
		leaseOwner,
		expiredAt.Format(time.RFC3339Nano),
		expiredAt.Add(-time.Minute).Format(time.RFC3339Nano),
		allocation.GenerationID,
	); err != nil {
		t.Fatalf("expire generation: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE turns
SET status = 'leased',
    generation_id = ?,
    lease_owner = ?,
    lease_expires_at = ?,
    claim_request_id = 'claim-expired',
    claim_granted_at = ?
WHERE id = ?`,
		allocation.GenerationID,
		leaseOwner,
		expiredAt.Format(time.RFC3339Nano),
		expiredAt.Add(-time.Minute).Format(time.RFC3339Nano),
		turnID,
	); err != nil {
		t.Fatalf("expire leased turn: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	rt := &recordingRuntime{}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	done := make(chan error, 1)
	go func() {
		done <- srv.RunMaintenance(runCtx)
	}()

	waitForGenerationStatus(t, runCtx, st, allocation.GenerationID, "failed")
	cancel()
	err = <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("maintenance exit err=%v, want context canceled", err)
	}

	var turnStatus string
	var turnGeneration sql.NullString
	var attempt int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT status, generation_id, attempt
FROM turns
WHERE id = ?`, turnID).Scan(&turnStatus, &turnGeneration, &attempt); err != nil {
		t.Fatalf("query recovered turn: %v", err)
	}
	if turnStatus != "queued" || turnGeneration.Valid || attempt != 1 {
		t.Fatalf("leased turn was not requeued: status=%s generation=%v attempt=%d", turnStatus, turnGeneration, attempt)
	}
	var sessionStatus string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT status
FROM sessions
WHERE id = ?`, session.ID).Scan(&sessionStatus); err != nil {
		t.Fatalf("query recovered session: %v", err)
	}
	if sessionStatus != string(sessionstate.RunningIdle) {
		t.Fatalf("recovered session status=%s want %s", sessionStatus, sessionstate.RunningIdle)
	}
	runscID := serverRunscContainerID(t, ctx, st, session.ID, allocation.GenerationID)
	if got := rt.runtimeDestroyRequests(); len(got) != 1 || got[0] != runscID {
		t.Fatalf("maintenance should destroy expired runtime %q before repair, got %+v", runscID, got)
	}
}

func TestExpiredRuntimeRecoverySkipsRepairWhenRuntimeCleanupFails(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	now := time.Now().UTC()
	session := createServerTestSession(t, ctx, st, dir, "sess_recovery_cleanup_fail", string(sessionstate.RunningIdle), now.Add(-2*time.Minute), nil)
	cfg := testServerConfig(dir)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(-3 * time.Minute),
		Config:    serverTestAllocatorConfig(cfg, session.DriverID),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, now.Add(-3*time.Minute+time.Second)); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, session.ID, allocation, owner.UUID, mustRuntimeResourceHostID(t), now.Add(-3*time.Minute+2*time.Second))
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'idle',
    lease_owner = ?,
    lease_expires_at = ?
WHERE generation_id = ?`, store.GenerationLeaseOwner("previous-owner"), now.Add(-time.Minute).Format(time.RFC3339Nano), allocation.GenerationID); err != nil {
		t.Fatalf("expire generation: %v", err)
	}
	rt := &recordingRuntime{destroyRuntimeErr: errors.New("runsc delete failed")}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	recovered, err := srv.RecoverExpiredRuntimeResources(ctx, now)
	if err != nil {
		t.Fatalf("recover expired runtime resources: %v", err)
	}
	if recovered.RuntimeCleanupSkipped != 1 ||
		recovered.ReconnectGraceFailed != 0 ||
		recovered.ExpiredLifecycleFailed != 0 ||
		recovered.UnknownAfterAckStarted != 0 {
		t.Fatalf("cleanup failure should skip repair, got %+v", recovered)
	}
	runscID := serverRunscContainerID(t, ctx, st, session.ID, allocation.GenerationID)
	if got := rt.runtimeDestroyRequests(); len(got) != 1 || got[0] != runscID {
		t.Fatalf("expected runtime cleanup attempt for %q, got %+v", runscID, got)
	}
	var generationStatus, ownerAfter, networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.lease_owner, ''), n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &ownerAfter, &networkState, &resourceState); err != nil {
		t.Fatalf("query skipped recovery state: %v", err)
	}
	if generationStatus != "idle" ||
		ownerAfter != string(store.GenerationLeaseOwner("previous-owner")) ||
		networkState != "live" ||
		resourceState != "live" {
		t.Fatalf("cleanup failure should leave DB non-reclaimable: generation=%s owner=%s network=%s resource=%s", generationStatus, ownerAfter, networkState, resourceState)
	}
}

func TestRunMaintenancePrunesRetainedEvents(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	now := time.Now().UTC()
	createServerTestSession(t, ctx, st, dir, "sess_events_a", string(sessionstate.Created), now, nil)
	createServerTestSession(t, ctx, st, dir, "sess_events_b", string(sessionstate.Created), now, nil)
	firstID, err := st.AppendEvent(ctx, store.AppendEventParams{
		SessionID: "sess_events_a",
		Type:      "test.event",
		Payload:   map[string]string{"name": "first"},
		Now:       now.Add(-3 * time.Second),
	})
	if err != nil {
		t.Fatalf("append first event: %v", err)
	}
	secondID, err := st.AppendEvent(ctx, store.AppendEventParams{
		SessionID: "sess_events_b",
		Type:      "test.event",
		Payload:   map[string]string{"name": "second"},
		Now:       now.Add(-2 * time.Second),
	})
	if err != nil {
		t.Fatalf("append second event: %v", err)
	}
	thirdID, err := st.AppendEvent(ctx, store.AppendEventParams{
		SessionID: "sess_events_a",
		Type:      "test.event",
		Payload:   map[string]string{"name": "third"},
		Now:       now.Add(-time.Second),
	})
	if err != nil {
		t.Fatalf("append third event: %v", err)
	}

	cfg := testServerConfig(dir)
	cfg.Harness.Events.RetentionWindow = config.Duration{Duration: time.Hour}
	cfg.Harness.Events.RetentionRows = 2
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: instantRuntime{},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)
	done := make(chan error, 1)
	go func() {
		done <- srv.RunMaintenance(runCtx)
	}()
	waitForEventIDs(t, runCtx, st, []int64{secondID, thirdID})
	cancel()
	err = <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("maintenance exit err=%v, want context canceled", err)
	}
	if _, ok, err := st.GetEvent(ctx, firstID); err != nil || ok {
		t.Fatalf("first event retained ok=%v err=%v", ok, err)
	}
}
