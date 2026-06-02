package server

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/artifacts"
	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/generationplan"
	"harness-platform/orchestrator/internal/planprojection"
	"harness-platform/orchestrator/internal/runtime"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

const checkpointImageManifestDigestForTest = "sha256:checkpoint-image-manifest"

func TestSendMessageAllocatesReplacementGenerationForFailedActiveGeneration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_send_failed_generation", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
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

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after failed generation"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body %s", rec.Code, rec.Body.String())
	}

	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if gotSession.ActiveGenerationID == "" || gotSession.ActiveGenerationID == old.GenerationID {
		t.Fatalf("active generation was not replaced: %q old=%q", gotSession.ActiveGenerationID, old.GenerationID)
	}
	volumeConfig, err := srv.dataVolumeProvisionerConfig()
	if err != nil {
		t.Fatalf("data volume config: %v", err)
	}
	driverHome, err := st.VerifySessionDriverHomeVolume(ctx, store.VerifySessionDriverHomeVolumeParams{
		SessionID: session.ID,
		Driver:    session.DriverID,
		Config:    volumeConfig,
	})
	if err != nil {
		t.Fatalf("verify replacement driver home: %v", err)
	}
	var oldStatus, oldNetwork, oldResources, newStatus, newNetwork, newResources string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, old.GenerationID).Scan(&oldStatus, &oldNetwork, &oldResources); err != nil {
		t.Fatalf("query old generation: %v", err)
	}
	if oldStatus != "failed" || oldNetwork != "reclaimable" || oldResources != "reclaimable" {
		t.Fatalf("old generation not fenced/reclaimable: status=%s network=%s resources=%s", oldStatus, oldNetwork, oldResources)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, gotSession.ActiveGenerationID).Scan(&newStatus, &newNetwork, &newResources); err != nil {
		t.Fatalf("query replacement generation: %v", err)
	}
	if newStatus != "idle" || newNetwork != "live" || newResources != "live" {
		t.Fatalf("replacement generation not live idle: status=%s network=%s resources=%s", newStatus, newNetwork, newResources)
	}
	var queuedTurns int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns
WHERE session_id = ?
  AND status = 'queued'
  AND generation_id IS NULL
  AND content = 'after failed generation'`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count queued turns: %v", err)
	}
	if queuedTurns != 1 {
		t.Fatalf("queued replacement turn count=%d want 1", queuedTurns)
	}
	prepareRequests, startRequests := rt.requests()
	if len(prepareRequests) != 2 || len(startRequests) != 1 {
		t.Fatalf("runtime calls prepare=%d start=%d", len(prepareRequests), len(startRequests))
	}
	if startRequests[0].GenerationID != gotSession.ActiveGenerationID {
		t.Fatalf("unexpected replacement start request: %+v", startRequests[0])
	}
	if startRequests[0].AgentHomeHostPath != driverHome.HostPath {
		t.Fatalf("replacement start did not use driver home volume: start=%+v home=%+v", startRequests[0], driverHome)
	}
}

func TestSendMessageRestoresCheckpointedGeneration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_restore", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate checkpointed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, allocation.GenerationID, allocation.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark checkpointed generation live: %v", err)
	}
	recordServerRuntimeArtifacts(t, ctx, st, allocation.GenerationID, "restore_manifest_digest", "runsc restore-test")
	snapshot := addServerGenerationPlanSkillsSnapshot(t, ctx, st, allocation.GenerationID)
	markServerGenerationCheckpointed(t, ctx, st, session.ID, allocation.GenerationID, time.Now().UTC())
	mutateServerRuntimeArtifactDigestMirrors(t, ctx, st, allocation.GenerationID)

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

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after restore"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body %s", rec.Code, rec.Body.String())
	}

	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get restored session: %v", err)
	}
	if gotSession.ActiveGenerationID != allocation.GenerationID || gotSession.Status != string(sessionstate.RunningActive) {
		t.Fatalf("unexpected restored session: %+v allocation=%+v", gotSession, allocation)
	}
	var generationStatus, networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, allocation.GenerationID).Scan(&generationStatus, &networkState, &resourceState); err != nil {
		t.Fatalf("query restored generation: %v", err)
	}
	if generationStatus != "idle" || networkState != "live" || resourceState != "live" {
		t.Fatalf("restored generation not live idle: status=%s network=%s resources=%s", generationStatus, networkState, resourceState)
	}
	var queuedTurns int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns
WHERE session_id = ?
  AND status = 'queued'
  AND generation_id IS NULL
  AND content = 'after restore'`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count restored queued turns: %v", err)
	}
	if queuedTurns != 1 {
		t.Fatalf("queued restored turn count=%d want 1", queuedTurns)
	}
	prepareRequests, startRequests := rt.requests()
	if len(prepareRequests) != 0 || len(startRequests) != 1 {
		t.Fatalf("restore should skip prepare and start once: prepare=%d start=%d", len(prepareRequests), len(startRequests))
	}
	start := startRequests[0]
	if start.GenerationID != allocation.GenerationID ||
		!start.RestoreFromCheckpoint ||
		start.PreparedArtifacts.ManifestDigest != "restore_manifest_digest" ||
		start.PreparedArtifacts.ProjectedManifestDigest != "restore_manifest_digest" ||
		start.PreparedArtifacts.BundleDigest != "bundle_digest" ||
		start.PreparedArtifacts.RuntimeConfigDigest != "runtime_config_digest" ||
		start.PreparedArtifacts.SpecDigest != "spec_digest" ||
		start.PreparedArtifacts.RunscVersion != "runsc restore-test" ||
		start.PreparedArtifacts.RunscBinaryPath != "/usr/local/bin/runsc-test" ||
		start.PreparedArtifacts.RunscBinaryDigest != "sha256:runsc-test" ||
		start.Generation.NetworkAllocationState != "recreating" {
		t.Fatalf("unexpected restore start request: %+v", start)
	}
	if len(start.ContentSnapshots) != 1 ||
		start.ContentSnapshots[0].Kind != store.ContentSnapshotKindSkills ||
		start.ContentSnapshots[0].Digest != snapshot.Digest ||
		start.ContentSnapshots[0].ImmutableHostPath != snapshot.ImmutableHostPath ||
		start.ContentSnapshots[0].MountDestination != store.ContentSnapshotSkillsMount {
		t.Fatalf("restore start content snapshots = %+v want %+v", start.ContentSnapshots, snapshot)
	}
}

func TestSendMessageCheckpointRestoreFailureFailsExplicitly(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_restore_fail", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate checkpointed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark checkpointed generation live: %v", err)
	}
	recordServerRuntimeArtifacts(t, ctx, st, old.GenerationID, "restore_manifest_digest", "runsc restore-test")
	markServerGenerationCheckpointed(t, ctx, st, session.ID, old.GenerationID, time.Now().UTC())
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE sessions
SET checkpoint_path = ?,
    restore_ms = 123
WHERE id = ?`, filepath.Join(dir, "checkpoints", session.ID), session.ID); err != nil {
		t.Fatalf("seed checkpoint metadata: %v", err)
	}

	rt := &restoreFailingRuntime{err: errors.New("checkpoint_runsc_version mismatch")}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after restore failure"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d body %s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["error_class"] != "runtime_failed" || body["error"] != "checkpoint_runsc_version mismatch" {
		t.Fatalf("unexpected response body: %v", body)
	}
	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get restore-failed session: %v", err)
	}
	if gotSession.ActiveGenerationID != old.GenerationID || gotSession.Status != string(sessionstate.RunningIdle) {
		t.Fatalf("restore failure should keep active generation and retryable session: %+v old=%s", gotSession, old.GenerationID)
	}
	if gotSession.CheckpointPath != "" || gotSession.RestoreMS != nil {
		t.Fatalf("restore failure should clear checkpoint metadata: checkpoint=%q restore=%v", gotSession.CheckpointPath, gotSession.RestoreMS)
	}
	var oldStatus, oldNetwork, oldResources, oldErrorClass string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state, COALESCE(g.error_class, '')
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, old.GenerationID).Scan(&oldStatus, &oldNetwork, &oldResources, &oldErrorClass); err != nil {
		t.Fatalf("query old generation: %v", err)
	}
	if oldStatus != "failed" || oldNetwork != "reclaimable" || oldResources != "reclaimable" || oldErrorClass != "runtime_failed" {
		t.Fatalf("generation not failed explicitly after restore failure: status=%s network=%s resources=%s class=%s", oldStatus, oldNetwork, oldResources, oldErrorClass)
	}
	var queuedTurns int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns
WHERE session_id = ?`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if queuedTurns != 0 {
		t.Fatalf("restore failure should not enqueue a turn, got %d", queuedTurns)
	}
	prepareRequests, startRequests := rt.requests()
	if len(prepareRequests) != 0 || len(startRequests) != 1 {
		t.Fatalf("runtime calls prepare=%d start=%d", len(prepareRequests), len(startRequests))
	}
	if !startRequests[0].RestoreFromCheckpoint || startRequests[0].GenerationID != old.GenerationID {
		t.Fatalf("start was not restore: %+v", startRequests[0])
	}
	var runtimeEvents, terminalEvents int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT
  COALESCE(SUM(CASE WHEN type = 'generation.error' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN type = 'session.error' THEN 1 ELSE 0 END), 0)
FROM events
WHERE session_id = ?`, session.ID).Scan(&runtimeEvents, &terminalEvents); err != nil {
		t.Fatalf("count restore failure events: %v", err)
	}
	if runtimeEvents != 1 || terminalEvents != 0 {
		t.Fatalf("unexpected restore failure events: runtime=%d terminal=%d", runtimeEvents, terminalEvents)
	}
}

func TestSendMessageCheckpointRestoreFailureCanBeRetriedColdOnNextInput(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_restore_fail_retry", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate checkpointed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark checkpointed generation live: %v", err)
	}
	recordServerRuntimeArtifacts(t, ctx, st, old.GenerationID, "restore_manifest_digest", "runsc restore-test")
	markServerGenerationCheckpointed(t, ctx, st, session.ID, old.GenerationID, time.Now().UTC())
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE sessions
SET checkpoint_path = ?,
    restore_ms = 456
WHERE id = ?`, filepath.Join(dir, "checkpoints", session.ID), session.ID); err != nil {
		t.Fatalf("seed checkpoint metadata: %v", err)
	}

	rt := &restoreFailingRuntime{err: errors.New("checkpoint_runsc_version mismatch")}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after restore failure"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d body %s", rec.Code, rec.Body.String())
	}
	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get restore-failed session: %v", err)
	}
	if gotSession.Status != string(sessionstate.RunningIdle) || gotSession.ActiveGenerationID != old.GenerationID {
		t.Fatalf("session should stay retryable on failed checkpoint generation: %+v old=%s", gotSession, old.GenerationID)
	}
	var oldStatus, oldNetwork, oldResources string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, old.GenerationID).Scan(&oldStatus, &oldNetwork, &oldResources); err != nil {
		t.Fatalf("query old generation: %v", err)
	}
	if oldStatus != "failed" || oldNetwork != "reclaimable" || oldResources != "reclaimable" {
		t.Fatalf("generation not reclaimable after restore failure: status=%s network=%s resources=%s", oldStatus, oldNetwork, oldResources)
	}
	rt.err = nil
	req = httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"retry after restore failure"}`))
	rec = httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected retry status 202, got %d body %s", rec.Code, rec.Body.String())
	}
	gotSession, err = st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get retried session: %v", err)
	}
	if gotSession.ActiveGenerationID == "" || gotSession.ActiveGenerationID == old.GenerationID || gotSession.Status != string(sessionstate.RunningActive) {
		t.Fatalf("retry should allocate a replacement after explicit failure: %+v old=%s", gotSession, old.GenerationID)
	}
	var newStatus, newNetwork, newResources string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, gotSession.ActiveGenerationID).Scan(&newStatus, &newNetwork, &newResources); err != nil {
		t.Fatalf("query retry generation: %v", err)
	}
	if newStatus != "idle" || newNetwork != "live" || newResources != "live" {
		t.Fatalf("retry generation not live idle: status=%s network=%s resources=%s", newStatus, newNetwork, newResources)
	}
	var queuedTurns int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns
WHERE session_id = ?
  AND status = 'queued'
  AND generation_id IS NULL
  AND content = 'retry after restore failure'`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if queuedTurns != 1 {
		t.Fatalf("retry should enqueue exactly one turn, got %d", queuedTurns)
	}
	prepareRequests, startRequests := rt.requests()
	if len(prepareRequests) != 2 || len(startRequests) != 2 {
		t.Fatalf("runtime calls prepare=%d start=%d", len(prepareRequests), len(startRequests))
	}
	if !startRequests[0].RestoreFromCheckpoint || startRequests[0].GenerationID != old.GenerationID {
		t.Fatalf("first start was not restore: %+v", startRequests[0])
	}
	if startRequests[1].RestoreFromCheckpoint || startRequests[1].GenerationID != gotSession.ActiveGenerationID {
		t.Fatalf("second start was not cold retry generation: %+v", startRequests[1])
	}
}

func TestSendMessageRestoreLiveCASFailureDestroysRunscContainerIDBeforeFailing(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_restore_live_cas", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate checkpointed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark checkpointed generation live: %v", err)
	}
	recordServerRuntimeArtifacts(t, ctx, st, old.GenerationID, "restore_manifest_digest", "runsc restore-test")
	markServerGenerationCheckpointed(t, ctx, st, session.ID, old.GenerationID, time.Now().UTC())

	rt := &restoreStartHookRuntime{
		onRestoreStart: func() {
			if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reserved_checkpointed'
WHERE generation_id = ?`, old.GenerationID); err != nil {
				t.Fatalf("force restore live CAS failure: %v", err)
			}
		},
	}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after restore live cas"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d body %s", rec.Code, rec.Body.String())
	}
	destroyIDs := rt.runtimeDestroyRequests()
	oldRunscID := serverRunscContainerID(t, ctx, st, session.ID, old.GenerationID)
	if len(destroyIDs) != 1 || destroyIDs[0] != oldRunscID {
		t.Fatalf("restore live CAS cleanup should destroy runsc container id %q, got %+v", oldRunscID, destroyIDs)
	}
	if destroyIDs[0] == session.ID {
		t.Fatalf("restore live CAS cleanup used bare session id %q", session.ID)
	}
	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get restore-failed session: %v", err)
	}
	if gotSession.ActiveGenerationID != old.GenerationID || gotSession.Status != string(sessionstate.RunningIdle) {
		t.Fatalf("restore live CAS failure should not allocate replacement: %+v old=%s", gotSession, old.GenerationID)
	}
	var oldStatus, oldNetwork, oldResources string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, old.GenerationID).Scan(&oldStatus, &oldNetwork, &oldResources); err != nil {
		t.Fatalf("query old generation: %v", err)
	}
	if oldStatus != "failed" || oldNetwork != "reclaimable" || oldResources != "reclaimable" {
		t.Fatalf("generation not reclaimable after restore live CAS failure: status=%s network=%s resources=%s", oldStatus, oldNetwork, oldResources)
	}
}

func TestSendMessageRestoreLiveCASFailureDoesNotRetireWhenDestroyFails(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_restore_destroy_fail", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate checkpointed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark checkpointed generation live: %v", err)
	}
	recordServerRuntimeArtifacts(t, ctx, st, old.GenerationID, "restore_manifest_digest", "runsc restore-test")
	markServerGenerationCheckpointed(t, ctx, st, session.ID, old.GenerationID, time.Now().UTC())

	rt := &restoreStartHookRuntime{
		recordingRuntime: recordingRuntime{destroyRuntimeErr: errors.New("destroy failed")},
		onRestoreStart: func() {
			if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reserved_checkpointed'
WHERE generation_id = ?`, old.GenerationID); err != nil {
				t.Fatalf("force restore live CAS failure: %v", err)
			}
		},
	}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after restore destroy fail"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d body %s", rec.Code, rec.Body.String())
	}
	destroyIDs := rt.runtimeDestroyRequests()
	oldRunscID := serverRunscContainerID(t, ctx, st, session.ID, old.GenerationID)
	if len(destroyIDs) != 1 || destroyIDs[0] != oldRunscID {
		t.Fatalf("restore cleanup should target runsc container id %q before failing, got %+v", oldRunscID, destroyIDs)
	}
	var oldStatus, oldNetwork, oldResources string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, old.GenerationID).Scan(&oldStatus, &oldNetwork, &oldResources); err != nil {
		t.Fatalf("query old generation: %v", err)
	}
	if oldStatus == "failed" || oldNetwork == "reclaimable" || oldResources == "reclaimable" {
		t.Fatalf("restore generation should not be retired when runtime destroy fails: status=%s network=%s resources=%s", oldStatus, oldNetwork, oldResources)
	}
	var retirementEvents int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM events
WHERE session_id = ?
  AND type IN ('session.checkpoint_retired', 'generation.error')`, session.ID).Scan(&retirementEvents); err != nil {
		t.Fatalf("count restore failure events: %v", err)
	}
	if retirementEvents != 0 {
		t.Fatalf("restore failure events should not be committed when destroy fails, got %d", retirementEvents)
	}
}

func TestSendMessageCheckpointImageManifestInvalidFailsExplicitly(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_restore_manifest_invalid", string(sessionstate.RunningIdle), time.Now().UTC(), nil)
	if _, err := st.DBForTest().ExecContext(ctx, `UPDATE sessions SET driver_id = 'sh', mode = 'shell' WHERE id = ?`, session.ID); err != nil {
		t.Fatalf("set shell session agent: %v", err)
	}
	session.DriverID = "sh"
	session.Mode = "shell"
	cfg := testServerConfig(dir)
	cfg.Harness.Network.CIDRPool = config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/28")}
	leaseOwner := store.GenerationLeaseOwner(owner.UUID)
	old, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     leaseOwner,
		LeaseTTL:  time.Minute,
		Now:       time.Now().UTC(),
		Config:    serverTestAllocatorConfig(cfg, "sh"),
	})
	if err != nil {
		t.Fatalf("allocate checkpointed generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, session.ID, old.GenerationID, old.Owner, time.Now().UTC()); err != nil {
		t.Fatalf("mark checkpointed generation live: %v", err)
	}

	checkpointPath := filepath.Join(dir, "checkpoints", session.ID)
	writeServerCheckpointFilesWithoutManifest(t, checkpointPath)
	manifest, err := buildServerCheckpointImageManifest(checkpointPath)
	if err != nil {
		t.Fatalf("build checkpoint image manifest: %v", err)
	}
	if err := writeServerJSONFile(filepath.Join(checkpointPath, "harness-checkpoint-manifest.json"), manifest); err != nil {
		t.Fatalf("write checkpoint image manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkpointPath, "pages.img"), []byte("corrupt"), 0o644); err != nil {
		t.Fatalf("corrupt checkpoint image file: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generation_resources
SET checkpoint_path = ?
WHERE generation_id = ?`, checkpointPath, old.GenerationID); err != nil {
		t.Fatalf("record checkpoint path: %v", err)
	}
	runscPath, runscDigest := currentRunscBinaryMetadataForServerTest(t)
	recordServerRuntimeArtifactsWithRunsc(t, ctx, st, old.GenerationID, "restore_manifest_digest", "runsc test", runscPath, runscDigest)
	markServerGenerationCheckpointed(t, ctx, st, session.ID, old.GenerationID, time.Now().UTC())

	realRuntime := runtime.New(runtime.Config{
		SessionsRoot:    cfg.SessionsRoot,
		AgentHomesRoot:  filepath.Join(dir, "agent-homes"),
		RootFSPath:      filepath.Join(dir, "rootfs"),
		BundleRoot:      filepath.Join(dir, "run", "runtime"),
		RunscNetwork:    "host",
		RunscOverlay2:   "none",
		RunDir:          cfg.Harness.RunDir,
		CommandRunner:   serverCommandRunner{outputs: map[string][]byte{"runsc --version": []byte("runsc test")}},
		BridgeMode:      "claim-loop",
		BridgeHeartbeat: time.Second,
		SandboxUID:      cfg.Harness.SandboxIdentity.UID,
		SandboxGID:      cfg.Harness.SandboxIdentity.GID,
	})
	rt := &restoreValidationRuntime{restore: realRuntime}
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"after corrupt checkpoint"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d body %s", rec.Code, rec.Body.String())
	}
	gotSession, err := st.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("get restore-failed session: %v", err)
	}
	if gotSession.ActiveGenerationID != old.GenerationID || gotSession.Status != string(sessionstate.RunningIdle) {
		t.Fatalf("invalid checkpoint should not allocate replacement: %+v old=%s", gotSession, old.GenerationID)
	}
	var oldStatus, oldNetwork, oldResources, oldReason string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, n.allocation_state, r.resource_state, COALESCE(g.failure_reason, '')
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, old.GenerationID).Scan(&oldStatus, &oldNetwork, &oldResources, &oldReason); err != nil {
		t.Fatalf("query old generation: %v", err)
	}
	if oldStatus != "failed" || oldNetwork != "reclaimable" || oldResources != "reclaimable" {
		t.Fatalf("generation not failed after invalid checkpoint manifest: status=%s network=%s resources=%s reason=%s", oldStatus, oldNetwork, oldResources, oldReason)
	}
	if !strings.Contains(oldReason, "checkpoint image manifest") || !strings.Contains(oldReason, "pages.img") {
		t.Fatalf("old generation failure reason did not include checkpoint manifest mismatch: %q", oldReason)
	}
	var queuedTurns int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM turns
WHERE session_id = ?`, session.ID).Scan(&queuedTurns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if queuedTurns != 0 {
		t.Fatalf("invalid checkpoint should not enqueue a turn, got %d", queuedTurns)
	}
	if got := len(rt.startRequests); got != 1 {
		t.Fatalf("runtime calls start=%d want 1", got)
	}
	if !rt.startRequests[0].RestoreFromCheckpoint || rt.startRequests[0].GenerationID != old.GenerationID {
		t.Fatalf("start was not restore: %+v", rt.startRequests[0])
	}
}

func TestSendMessageRuntimeStartFailureMarksGenerationFailedAndReclaimable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_runtime_fail", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	srv := &Server{
		cfg:   cfg,
		store: st,
		runtime: failingRuntime{
			err: errors.New("pre-start sandbox network probe failed"),
		},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"hello"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d body %s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["error_class"] != "probe_failed_pre_start" ||
		body["error"] != "sandbox network probe failed before start" {
		t.Fatalf("unexpected response body: %v", body)
	}
	var generationStatus, errorClass, networkState, resourceState, sessionStatus, sessionErrorClass, sessionFailureReason string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.error_class, ''), n.allocation_state, r.resource_state, s.status,
       COALESCE(s.error_class, ''), COALESCE(s.failure_reason, '')
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
JOIN sessions s ON s.active_generation_id = g.generation_id
WHERE g.session_id = ?`, session.ID).Scan(
		&generationStatus,
		&errorClass,
		&networkState,
		&resourceState,
		&sessionStatus,
		&sessionErrorClass,
		&sessionFailureReason,
	); err != nil {
		t.Fatalf("query generation state: %v", err)
	}
	if generationStatus != "failed" ||
		errorClass != "probe_failed_pre_start" ||
		networkState != "reclaimable" ||
		resourceState != "reclaimable" ||
		sessionStatus != string(sessionstate.Created) ||
		sessionErrorClass != "" ||
		sessionFailureReason != "" {
		t.Fatalf("unexpected failed generation state: generation=%s class=%s network=%s resource=%s session=%s session_class=%s session_reason=%s", generationStatus, errorClass, networkState, resourceState, sessionStatus, sessionErrorClass, sessionFailureReason)
	}
	if !sessionstate.CanAcceptInput(sessionStatus) {
		t.Fatalf("session should remain input-acceptable after start failure, got %s", sessionStatus)
	}
	var runtimeResourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT state
FROM runtime_resource_instances
WHERE generation_id = (
  SELECT generation_id FROM runtime_generations WHERE session_id = ?
)`, session.ID).Scan(&runtimeResourceState); err != nil {
		t.Fatalf("query runtime resource instance after start failure: %v", err)
	}
	if runtimeResourceState != string(store.RuntimeResourceRetiring) {
		t.Fatalf("runtime resource state after start failure=%s want %s", runtimeResourceState, store.RuntimeResourceRetiring)
	}
	var runtimeEvents int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM events
WHERE session_id = ?
  AND type = 'generation.error'`, session.ID).Scan(&runtimeEvents); err != nil {
		t.Fatalf("count generation error events: %v", err)
	}
	if runtimeEvents != 1 {
		t.Fatalf("expected one generation.error event, got %d", runtimeEvents)
	}
	var turns int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ?`, session.ID).Scan(&turns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if turns != 0 {
		t.Fatalf("runtime start failure should happen before turn creation, got %d turns", turns)
	}
	var failedGenerationID string
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT generation_id FROM runtime_generations WHERE session_id = ?`, session.ID).Scan(&failedGenerationID); err != nil {
		t.Fatalf("query failed generation id: %v", err)
	}
	if err := srv.cleanupGenerationResources(ctx, session.ID, failedGenerationID, time.Now().UTC()); err != nil {
		t.Fatalf("cleanup failed generation resources: %v", err)
	}
	instance, err := st.GetRuntimeResourceInstance(ctx, failedGenerationID)
	if err != nil {
		t.Fatalf("get cleaned runtime resource instance: %v", err)
	}
	if instance.State != store.RuntimeResourceDestroyed ||
		instance.EvidenceDigest == "" ||
		len(instance.EvidenceJSON) == 0 ||
		instance.VerifiedAt == nil {
		t.Fatalf("runtime resource cleanup did not record destroyed evidence: %+v", instance)
	}
	var evidence store.ResourceReconciliationEvidence
	if err := json.Unmarshal(instance.EvidenceJSON, &evidence); err != nil {
		t.Fatalf("decode runtime resource cleanup evidence: %v", err)
	}
	if !strings.HasPrefix(evidence.RunscState, "runsc_container:absent") {
		t.Fatalf("runtime resource cleanup did not record runsc absence: %+v", evidence)
	}

	srv.runtime = instantRuntime{}
	retryReq := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"retry"}`))
	retryRec := httptest.NewRecorder()
	srv.sendMessage(retryRec, retryReq, session.ID)
	if retryRec.Code != http.StatusAccepted {
		t.Fatalf("expected retry status 202, got %d body %s", retryRec.Code, retryRec.Body.String())
	}
	var generationCount int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generations WHERE session_id = ?`, session.ID).Scan(&generationCount); err != nil {
		t.Fatalf("count generations after retry: %v", err)
	}
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ?`, session.ID).Scan(&turns); err != nil {
		t.Fatalf("count turns after retry: %v", err)
	}
	if generationCount != 2 || turns != 1 {
		t.Fatalf("retry should allocate generation N+1 and enqueue one turn, generations=%d turns=%d", generationCount, turns)
	}
}

func TestStartEnsuredGenerationRenewsLeaseDuringSlowPrepare(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_slow_start", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	cfg.Harness.Bridge.LeaseTTL = config.Duration{Duration: 200 * time.Millisecond}
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: session.ID,
		Owner:     store.GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  cfg.Harness.Bridge.LeaseTTL.Duration,
		Now:       now,
		Config:    serverTestAllocatorConfig(cfg, session.DriverID),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	rt := newBlockingPrepareRuntime()
	t.Cleanup(rt.release)
	srv := &Server{
		cfg:     cfg,
		store:   st,
		runtime: rt,
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.startEnsuredGeneration(ctx, session, ensuredGeneration{
			Allocation: allocation,
			IsNew:      true,
		}, startFailureInputAcceptable)
	}()

	select {
	case <-rt.prepareStarted:
	case <-time.After(time.Second):
		t.Fatalf("prepare did not start")
	}
	waitForGenerationLeaseAfter(t, ctx, st, allocation.GenerationID, allocation.LeaseExpiresAt)
	rt.release()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("start ensured generation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("start ensured generation did not finish")
	}
	waitForGenerationStatus(t, ctx, st, allocation.GenerationID, "idle")
	volumeConfig, err := srv.dataVolumeProvisionerConfig()
	if err != nil {
		t.Fatalf("data volume config: %v", err)
	}
	workspaceVolume, err := st.VerifySessionWorkspaceVolume(ctx, store.VerifySessionWorkspaceVolumeParams{
		SessionID: session.ID,
		Config:    volumeConfig,
	})
	if err != nil {
		t.Fatalf("verify workspace volume: %v", err)
	}
	driverHomeVolume, err := st.VerifySessionDriverHomeVolume(ctx, store.VerifySessionDriverHomeVolumeParams{
		SessionID: session.ID,
		Driver:    session.DriverID,
		Config:    volumeConfig,
	})
	if err != nil {
		t.Fatalf("verify driver home volume: %v", err)
	}
	prepares, starts := rt.requests()
	if len(prepares) != 2 || len(starts) != 1 {
		t.Fatalf("runtime requests prepare=%d start=%d", len(prepares), len(starts))
	}
	for _, prepare := range prepares {
		if prepare.WorkspaceHostPath != workspaceVolume.HostPath ||
			prepare.AgentHomeHostPath != driverHomeVolume.HostPath {
			t.Fatalf("runtime render did not receive data volume paths: prepare=%+v workspace=%+v home=%+v",
				prepare, workspaceVolume, driverHomeVolume)
		}
	}
	if starts[0].WorkspaceHostPath != workspaceVolume.HostPath ||
		starts[0].AgentHomeHostPath != driverHomeVolume.HostPath {
		t.Fatalf("runtime start did not receive data volume paths: start=%+v workspace=%+v home=%+v",
			starts[0], workspaceVolume, driverHomeVolume)
	}
	instance, err := st.GetRuntimeResourceInstance(ctx, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get runtime resource instance: %v", err)
	}
	if instance.State != store.RuntimeResourceLive ||
		instance.WorkerID != owner.UUID ||
		instance.RunscContainerID != serverRunscContainerID(t, ctx, st, session.ID, allocation.GenerationID) ||
		instance.RunscBinaryPath != "/usr/local/bin/runsc-test" ||
		instance.RunscBinaryDigest != "sha256:runsc-test" ||
		instance.NftTableName != mustRuntimeResourceNftTableName(t, allocation.GenerationID) {
		t.Fatalf("unexpected runtime resource instance: %+v", instance)
	}
	contract, err := st.GetSandboxContractForGeneration(ctx, session.ID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get sandbox contract: %v", err)
	}
	if contract.SandboxContractVersion != store.SandboxContractVersion ||
		contract.ContractID != sandboxContractID(allocation.GenerationID) ||
		contract.ContractGateVersion != store.SandboxContractGateDriverManifest {
		t.Fatalf("unexpected sandbox contract: %+v", contract)
	}
	var payload map[string]any
	if err := json.Unmarshal(contract.CanonicalPayload, &payload); err != nil {
		t.Fatalf("decode sandbox contract payload: %v", err)
	}
	if payload["contract_gate_version"] != store.SandboxContractGateDriverManifest {
		t.Fatalf("sandbox contract gate version should be driver_manifest_v1: %s", contract.CanonicalPayload)
	}
	inputDigests, ok := payload["input_digests"].(map[string]any)
	if !ok {
		t.Fatalf("sandbox contract missing input digests: %s", contract.CanonicalPayload)
	}
	for _, key := range []string{"runtime_config_digest", "agent_manifest_digest"} {
		value, _ := inputDigests[key].(string)
		if !strings.HasPrefix(value, "sha256:") {
			t.Fatalf("sandbox contract missing %s: %s", key, contract.CanonicalPayload)
		}
	}
	evidence, err := st.GetSandboxContractInputEvidence(ctx, contract.ContractID)
	if err != nil {
		t.Fatalf("get sandbox contract input evidence: %v", err)
	}
	if evidence.RuntimeConfigDigest != inputDigests["runtime_config_digest"] ||
		evidence.AgentManifestDigest != inputDigests["agent_manifest_digest"] ||
		!json.Valid(evidence.RuntimeConfigPreimage) ||
		!json.Valid(evidence.AgentManifestPayload) {
		t.Fatalf("sandbox contract input evidence mismatch: evidence=%+v input=%+v", evidence, inputDigests)
	}
	if inputDigests["rootfs_image_digest"] != nil {
		t.Fatalf("rootfs digest should remain null until rootfs evidence is available: %s", contract.CanonicalPayload)
	}
	adapter, ok := payload["runtime_adapter"].(map[string]any)
	if !ok || adapter["runsc_container_id"] != serverRunscContainerID(t, ctx, st, session.ID, allocation.GenerationID) {
		t.Fatalf("sandbox contract missing runsc identity: %s", contract.CanonicalPayload)
	}
	if adapter["runsc_binary_path"] != "/usr/local/bin/runsc-test" ||
		adapter["runsc_binary_digest"] != "sha256:runsc-test" {
		t.Fatalf("sandbox contract missing runsc binary metadata: %s", contract.CanonicalPayload)
	}
	ambientCaps, ok := adapter["ambient_capabilities"].([]any)
	if adapter["no_new_privileges"] != true || !ok || len(ambientCaps) != 0 {
		t.Fatalf("sandbox contract missing runtime capability policy: %s", contract.CanonicalPayload)
	}
	forbiddenCaps, ok := adapter["forbidden_capabilities"].([]any)
	if !ok || !jsonArrayContainsAll(forbiddenCaps, "CAP_NET_ADMIN", "CAP_NET_RAW", "CAP_SYS_ADMIN") {
		t.Fatalf("sandbox contract missing forbidden capability policy: %s", contract.CanonicalPayload)
	}
	requiredAnnotations, ok := adapter["required_annotations"].(map[string]any)
	if !ok {
		t.Fatalf("sandbox contract missing required annotations: %s", contract.CanonicalPayload)
	}
	bridgeAnnotations, ok := requiredAnnotations[bridge.BridgeMountDestination].(map[string]any)
	if !ok ||
		bridgeAnnotations["dev.gvisor.spec.mount./harness-control/bridge.type"] != "bind" ||
		bridgeAnnotations["dev.gvisor.spec.mount./harness-control/bridge.share"] != "exclusive" {
		t.Fatalf("sandbox contract missing bridge required annotation policy: %s", contract.CanonicalPayload)
	}
	networkIdentity, ok := payload["network_identity"].(map[string]any)
	if !ok ||
		networkIdentity["sandbox_ip"] != instance.SandboxIP ||
		networkIdentity["nft_table_name"] != instance.NftTableName {
		t.Fatalf("sandbox contract missing runtime network identity: %s instance=%+v", contract.CanonicalPayload, instance)
	}
	resourceIdentity, ok := payload["resource_identity"].(map[string]any)
	if !ok || resourceIdentity["resource_identity_digest"] != instance.ResourceIdentityDigest {
		t.Fatalf("sandbox contract missing resource identity digest: %s instance=%+v", contract.CanonicalPayload, instance)
	}
	mountPlan, ok := payload["mount_plan"].(map[string]any)
	if !ok {
		t.Fatalf("sandbox contract missing mount plan: %s", contract.CanonicalPayload)
	}
	workspaceMount, ok := mountPlan["workspace"].(map[string]any)
	if !ok || workspaceMount["source"] != workspaceVolume.HostPath {
		t.Fatalf("sandbox contract workspace mount does not use data volume: %s", contract.CanonicalPayload)
	}
	agentHomeMount, ok := mountPlan["agent_home"].(map[string]any)
	if !ok || agentHomeMount["source"] != driverHomeVolume.HostPath {
		t.Fatalf("sandbox contract agent home mount does not use data volume: %s", contract.CanonicalPayload)
	}
	dataVolumes, ok := payload["data_volumes"].(map[string]any)
	if !ok {
		t.Fatalf("sandbox contract missing data volume ownership: %s", contract.CanonicalPayload)
	}
	workspacePayload, ok := dataVolumes["workspace"].(map[string]any)
	if !ok || workspacePayload["provisioning_marker_digest"] != workspaceVolume.ProvisioningMarkerDigest {
		t.Fatalf("sandbox contract workspace data volume evidence mismatch: %s", contract.CanonicalPayload)
	}
	driverHomePayload, ok := dataVolumes["agent_home"].(map[string]any)
	if !ok || driverHomePayload["provisioning_marker_digest"] != driverHomeVolume.ProvisioningMarkerDigest {
		t.Fatalf("sandbox contract driver home data volume evidence mismatch: %s", contract.CanonicalPayload)
	}
	var manifestDigest, specDigest, bundleDigest string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT control_manifest_digest, oci_spec_digest, bundle_digest
FROM sandbox_contract_artifacts
WHERE contract_id = ?`, contract.ContractID).Scan(&manifestDigest, &specDigest, &bundleDigest); err != nil {
		t.Fatalf("query sandbox contract artifacts: %v", err)
	}
	if manifestDigest != "manifest_digest" || specDigest != "spec_digest" || bundleDigest != "bundle_digest" {
		t.Fatalf("unexpected sandbox contract artifact digests: manifest=%s spec=%s bundle=%s", manifestDigest, specDigest, bundleDigest)
	}
}

func TestSendMessagePrepareFailureMarksGenerationFailedAndReclaimable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, owner := openServerOwnedStore(t, ctx, dir)
	session := createServerTestSession(t, ctx, st, dir, "sess_prepare_fail", string(sessionstate.Created), time.Now().UTC(), nil)
	cfg := testServerConfig(dir)
	srv := &Server{
		cfg:   cfg,
		store: st,
		runtime: failingRuntime{
			prepareErr: errors.New("pre-start sandbox network probe failed"),
		},
		watcher: newServerTestWatcher(t, filepath.Join(dir, "sessions"), st, events.NewHub()),
		hub:     events.NewHub(),
		log:     slog.Default(),
	}
	srv.SetOwnerUUID(owner.UUID)

	req := httptest.NewRequest(http.MethodPost, "/api/sessions/"+session.ID+"/messages", strings.NewReader(`{"content":"hello"}`))
	rec := httptest.NewRecorder()
	srv.sendMessage(rec, req, session.ID)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d body %s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["error_class"] != "probe_failed_pre_start" ||
		body["error"] != "sandbox network probe failed before start" {
		t.Fatalf("unexpected response body: %v", body)
	}
	var generationStatus, errorClass, networkState, resourceState, sessionStatus, sessionErrorClass, sessionFailureReason string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.error_class, ''), n.allocation_state, r.resource_state, s.status,
       COALESCE(s.error_class, ''), COALESCE(s.failure_reason, '')
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
JOIN sessions s ON s.active_generation_id = g.generation_id
WHERE g.session_id = ?`, session.ID).Scan(
		&generationStatus,
		&errorClass,
		&networkState,
		&resourceState,
		&sessionStatus,
		&sessionErrorClass,
		&sessionFailureReason,
	); err != nil {
		t.Fatalf("query generation state: %v", err)
	}
	if generationStatus != "failed" ||
		errorClass != "probe_failed_pre_start" ||
		networkState != "reclaimable" ||
		resourceState != "reclaimable" ||
		sessionStatus != string(sessionstate.Created) ||
		sessionErrorClass != "" ||
		sessionFailureReason != "" {
		t.Fatalf("unexpected failed generation state: generation=%s class=%s network=%s resource=%s session=%s session_class=%s session_reason=%s", generationStatus, errorClass, networkState, resourceState, sessionStatus, sessionErrorClass, sessionFailureReason)
	}
	if !sessionstate.CanAcceptInput(sessionStatus) {
		t.Fatalf("session should remain input-acceptable after prepare failure, got %s", sessionStatus)
	}
	var runtimeEvents int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COUNT(*)
FROM events
WHERE session_id = ?
  AND type = 'generation.error'`, session.ID).Scan(&runtimeEvents); err != nil {
		t.Fatalf("count generation error events: %v", err)
	}
	if runtimeEvents != 1 {
		t.Fatalf("expected one generation.error event, got %d", runtimeEvents)
	}
	var turns int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM turns WHERE session_id = ?`, session.ID).Scan(&turns); err != nil {
		t.Fatalf("count turns: %v", err)
	}
	if turns != 0 {
		t.Fatalf("prepare failure should happen before turn creation, got %d turns", turns)
	}
}

type instantRuntime struct{}

var instantRuntimePrepareCalls int64

func (instantRuntime) PrepareGeneration(context.Context, runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	atomic.AddInt64(&instantRuntimePrepareCalls, 1)
	return testGenerationArtifacts(), nil
}

func (instantRuntime) RenderGenerationArtifacts(context.Context, runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	atomic.AddInt64(&instantRuntimePrepareCalls, 1)
	return runtime.GenerationArtifactProjection{Artifacts: testGenerationArtifacts()}, nil
}

func (instantRuntime) MaterializeGenerationArtifacts(runtime.StartRequest, runtime.GenerationArtifactProjection) error {
	return nil
}

func (instantRuntime) PrepareGenerationNetwork(context.Context, runtime.StartRequest) error {
	return nil
}

func (instantRuntime) Start(ctx context.Context, req runtime.StartRequest, output func(runtime.Output)) runtime.Result {
	if output != nil {
		output(runtime.Output{Stream: "stdout", Line: `{"type":"result","subtype":"success","result":"ok"}`})
	}
	return serverRuntimeStartResult(req)
}

func (instantRuntime) Destroy(context.Context, string) error {
	return nil
}

func (instantRuntime) DestroyGenerationResources(_ context.Context, details store.RuntimeGenerationDetails) (runtime.GenerationResourceCleanup, error) {
	return runtimeCleanupEvidenceForDetails(details), nil
}

func (instantRuntime) Interrupt(string) error {
	return nil
}

func (instantRuntime) Checkpoint(context.Context, runtime.CheckpointRequest) (runtime.CheckpointResult, error) {
	return runtime.CheckpointResult{ImageManifestDigest: checkpointImageManifestDigestForTest}, nil
}

type recordingRuntime struct {
	mu                  sync.Mutex
	prepareRequests     []runtime.StartRequest
	materializeRequests []runtime.StartRequest
	networkRequests     []runtime.StartRequest
	startRequests       []runtime.StartRequest
	destroyRuntimeIDs   []string
	destroyRuntimeErr   error
	destroyRequests     []store.RuntimeGenerationDetails
	destroyErr          error
	checkpointReqs      []runtime.CheckpointRequest
	checkpointErr       error
	interruptSessionIDs []string
}

type planOrderRuntime struct {
	recordingRuntime
	store                                   *store.Store
	t                                       *testing.T
	planSeenBeforeNetwork                   bool
	planSeenBeforeMaterializeRender         bool
	planSeenBeforeMaterialize               bool
	projectionVerificationObserved          bool
	projectionVerificationBeforeMaterialize bool
	runtimeResourceClaimedBeforeNetwork     bool
	runtimeResourceClaimedBeforeMaterialize bool
}

type corruptProjectionBeforeMaterializeRuntime struct {
	recordingRuntime
	store        *store.Store
	t            *testing.T
	corrupted    bool
	materialized bool
}

func (r *planOrderRuntime) RenderGenerationArtifacts(ctx context.Context, req runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	r.t.Helper()
	projection, err := r.recordingRuntime.RenderGenerationArtifacts(ctx, req)
	if err != nil {
		return runtime.GenerationArtifactProjection{}, err
	}
	plan, err := r.store.GetGenerationPlan(ctx, req.GenerationID)
	if errors.Is(err, sql.ErrNoRows) {
		return projection, nil
	}
	if err != nil {
		return runtime.GenerationArtifactProjection{}, fmt.Errorf("get generation plan before materialize render: %w", err)
	}
	planArtifacts, err := generationplan.RuntimeArtifacts(plan.CanonicalPayload)
	if err != nil {
		return runtime.GenerationArtifactProjection{}, fmt.Errorf("read generation plan runtime artifacts before materialize render: %w", err)
	}
	if reflect.DeepEqual(req.PreparedArtifacts, planArtifacts) {
		r.planSeenBeforeMaterializeRender = true
	}
	return projection, nil
}

func (r *corruptProjectionBeforeMaterializeRuntime) RenderGenerationArtifacts(ctx context.Context, req runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	r.t.Helper()
	projection, err := r.recordingRuntime.RenderGenerationArtifacts(ctx, req)
	if err != nil {
		return runtime.GenerationArtifactProjection{}, err
	}
	plan, err := r.store.GetGenerationPlan(ctx, req.GenerationID)
	if errors.Is(err, sql.ErrNoRows) {
		return projection, nil
	}
	if err != nil {
		return runtime.GenerationArtifactProjection{}, fmt.Errorf("get generation plan before corrupting projection: %w", err)
	}
	planArtifacts, err := generationplan.RuntimeArtifacts(plan.CanonicalPayload)
	if err != nil {
		return runtime.GenerationArtifactProjection{}, fmt.Errorf("read generation plan runtime artifacts before corrupting projection: %w", err)
	}
	if r.corrupted || !reflect.DeepEqual(req.PreparedArtifacts, planArtifacts) {
		return projection, nil
	}
	if _, err := r.store.DBForTest().ExecContext(ctx, `
UPDATE generation_plan_projections
SET payload_digest = 'sha256:changed-before-materialize'
WHERE generation_id = ?
  AND projection_kind = ?`, req.GenerationID, store.GenerationPlanProjectionBundle); err != nil {
		return runtime.GenerationArtifactProjection{}, fmt.Errorf("corrupt generation plan projection before materialize: %w", err)
	}
	r.corrupted = true
	return projection, nil
}

func (r *corruptProjectionBeforeMaterializeRuntime) MaterializeGenerationArtifacts(req runtime.StartRequest, projection runtime.GenerationArtifactProjection) error {
	r.materialized = true
	return r.recordingRuntime.MaterializeGenerationArtifacts(req, projection)
}

type blockingPrepareRuntime struct {
	recordingRuntime
	prepareStarted chan struct{}
	releasePrepare chan struct{}
	startedOnce    sync.Once
	releaseOnce    sync.Once
}

func newBlockingPrepareRuntime() *blockingPrepareRuntime {
	return &blockingPrepareRuntime{
		prepareStarted: make(chan struct{}),
		releasePrepare: make(chan struct{}),
	}
}

func (r *blockingPrepareRuntime) RenderGenerationArtifacts(ctx context.Context, req runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	r.mu.Lock()
	r.prepareRequests = append(r.prepareRequests, req)
	r.mu.Unlock()
	r.startedOnce.Do(func() { close(r.prepareStarted) })
	select {
	case <-r.releasePrepare:
		return runtime.GenerationArtifactProjection{Artifacts: testGenerationArtifacts()}, nil
	case <-ctx.Done():
		return runtime.GenerationArtifactProjection{}, ctx.Err()
	}
}

func (r *blockingPrepareRuntime) PrepareGeneration(ctx context.Context, req runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	projection, err := r.RenderGenerationArtifacts(ctx, req)
	return projection.Artifacts, err
}

func (r *blockingPrepareRuntime) release() {
	r.releaseOnce.Do(func() { close(r.releasePrepare) })
}

type startHookRuntime struct {
	recordingRuntime
	onStart func(runtime.StartRequest)
}

func (r *startHookRuntime) Start(_ context.Context, req runtime.StartRequest, _ func(runtime.Output)) runtime.Result {
	r.mu.Lock()
	r.startRequests = append(r.startRequests, req)
	r.mu.Unlock()
	if r.onStart != nil {
		r.onStart(req)
	}
	return serverRuntimeStartResult(req)
}

type claimAfterProbeRuntime struct {
	recordingRuntime
}

func (r *claimAfterProbeRuntime) Start(_ context.Context, req runtime.StartRequest, _ func(runtime.Output)) runtime.Result {
	r.mu.Lock()
	r.startRequests = append(r.startRequests, req)
	r.mu.Unlock()
	result := serverRuntimeStartResult(req)
	if result.Err != nil {
		return result
	}
	outbox, err := bridge.OpenQueue(req.Generation.BridgeDirPath, bridge.OutboxDir)
	if err != nil {
		return runtime.Result{Err: err}
	}
	if _, err := outbox.Write(context.Background(), bridge.Envelope{
		RequestID:    "test_claim_after_probe",
		Type:         bridge.TypeClaimNextTurn,
		SessionID:    req.SessionID,
		GenerationID: req.GenerationID,
	}); err != nil {
		return runtime.Result{Err: err}
	}
	return result
}

func (r *recordingRuntime) PrepareGeneration(_ context.Context, req runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	r.mu.Lock()
	r.prepareRequests = append(r.prepareRequests, req)
	r.mu.Unlock()
	return testGenerationArtifacts(), nil
}

func (r *recordingRuntime) RenderGenerationArtifacts(_ context.Context, req runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	r.mu.Lock()
	r.prepareRequests = append(r.prepareRequests, req)
	r.mu.Unlock()
	return runtime.GenerationArtifactProjection{Artifacts: testGenerationArtifacts()}, nil
}

func (r *recordingRuntime) MaterializeGenerationArtifacts(req runtime.StartRequest, _ runtime.GenerationArtifactProjection) error {
	r.mu.Lock()
	r.materializeRequests = append(r.materializeRequests, req)
	r.mu.Unlock()
	return nil
}

func (r *recordingRuntime) PrepareGenerationNetwork(_ context.Context, req runtime.StartRequest) error {
	r.mu.Lock()
	r.networkRequests = append(r.networkRequests, req)
	r.mu.Unlock()
	return nil
}

func (r *planOrderRuntime) MaterializeGenerationArtifacts(req runtime.StartRequest, projection runtime.GenerationArtifactProjection) error {
	r.t.Helper()
	if err := r.recordingRuntime.MaterializeGenerationArtifacts(req, projection); err != nil {
		return err
	}
	if err := r.requireStoredPlanAndMaterializationClaim(context.Background(), req, "materialize"); err != nil {
		return err
	}
	r.planSeenBeforeMaterialize = true
	r.projectionVerificationBeforeMaterialize = true
	r.runtimeResourceClaimedBeforeMaterialize = true
	return nil
}

func (r *planOrderRuntime) PrepareGenerationNetwork(ctx context.Context, req runtime.StartRequest) error {
	r.t.Helper()
	if err := r.recordingRuntime.PrepareGenerationNetwork(ctx, req); err != nil {
		return err
	}
	if err := r.requireStoredPlanAndMaterializationClaim(ctx, req, "network prepare"); err != nil {
		return err
	}
	r.planSeenBeforeNetwork = true
	r.projectionVerificationObserved = true
	r.runtimeResourceClaimedBeforeNetwork = true
	return nil
}

func (r *planOrderRuntime) requireStoredPlanAndMaterializationClaim(ctx context.Context, req runtime.StartRequest, phase string) error {
	plan, err := r.store.GetGenerationPlan(ctx, req.GenerationID)
	if err != nil {
		return fmt.Errorf("get generation plan before %s: %w", phase, err)
	}
	planArtifacts, err := generationplan.RuntimeArtifacts(plan.CanonicalPayload)
	if err != nil {
		return fmt.Errorf("read generation plan runtime artifacts before %s: %w", phase, err)
	}
	if !reflect.DeepEqual(req.PreparedArtifacts, planArtifacts) {
		return fmt.Errorf("prepared artifacts before %s did not come from stored plan: got %+v want %+v", phase, req.PreparedArtifacts, planArtifacts)
	}
	projections, err := r.store.ListGenerationPlanProjections(ctx, req.GenerationID)
	if err != nil {
		return fmt.Errorf("list generation plan projections before %s: %w", phase, err)
	}
	if len(projections) != len(store.GenerationPlanProjectionKinds()) {
		return fmt.Errorf("generation plan projection count before %s = %d want %d", phase, len(projections), len(store.GenerationPlanProjectionKinds()))
	}
	verified, err := r.store.VerifyGenerationPlanProjections(ctx, store.VerifyGenerationPlanProjectionsParams{
		GenerationID: req.GenerationID,
		Expected:     generationPlanProjectionExpectationsForDetails(req.Generation, req.PreparedArtifacts, ""),
	})
	if err != nil {
		return fmt.Errorf("verify generation plan projections before %s: %w", phase, err)
	}
	if !verified {
		return fmt.Errorf("generation plan projections were missing before %s", phase)
	}
	for _, projection := range projections {
		if projection.PlanDigest != plan.PlanDigest {
			return fmt.Errorf("generation plan projection %s digest before %s = %s want %s", projection.ProjectionKind, phase, projection.PlanDigest, plan.PlanDigest)
		}
	}
	instance, err := r.store.GetRuntimeResourceInstance(ctx, req.GenerationID)
	if err != nil {
		return fmt.Errorf("get runtime resource instance before %s: %w", phase, err)
	}
	if instance.State != store.RuntimeResourceMaterializing {
		return fmt.Errorf("runtime resource state before %s = %s want %s", phase, instance.State, store.RuntimeResourceMaterializing)
	}
	if strings.TrimSpace(instance.WorkerID) == "" || strings.TrimSpace(instance.HostID) == "" {
		return fmt.Errorf("runtime resource worker lease was not claimed before %s", phase)
	}
	if instance.LeaseExpiresAt == nil {
		return fmt.Errorf("runtime resource materialization lease is missing before %s", phase)
	}
	if instance.IdempotencyToken != "start:"+req.GenerationID {
		return fmt.Errorf("runtime resource idempotency token before %s = %q want %q", phase, instance.IdempotencyToken, "start:"+req.GenerationID)
	}
	return nil
}

func (r *recordingRuntime) Start(_ context.Context, req runtime.StartRequest, _ func(runtime.Output)) runtime.Result {
	r.mu.Lock()
	r.startRequests = append(r.startRequests, req)
	r.mu.Unlock()
	return serverRuntimeStartResult(req)
}

func (r *recordingRuntime) DestroyGenerationResources(_ context.Context, details store.RuntimeGenerationDetails) (runtime.GenerationResourceCleanup, error) {
	r.mu.Lock()
	r.destroyRequests = append(r.destroyRequests, details)
	err := r.destroyErr
	r.mu.Unlock()
	if err != nil {
		return runtime.GenerationResourceCleanup{}, err
	}
	return runtimeCleanupEvidenceForDetails(details), nil
}

func runtimeCleanupEvidenceForDetails(details store.RuntimeGenerationDetails) runtime.GenerationResourceCleanup {
	filesystem := map[string]string{}
	addFilesystem := func(label, path string) {
		if strings.TrimSpace(path) != "" {
			filesystem[label+":"+path] = "lstat:absent"
		}
	}
	addFilesystem("checkpoint", details.CheckpointPath)
	addFilesystem("control", details.ControlDirPath)
	addFilesystem("control_manifest", details.ControlManifestPath)
	addFilesystem("bundle", details.BundleDirPath)
	addFilesystem("spec", details.SpecPath)
	addFilesystem("bridge", details.BridgeDirPath)
	if strings.TrimSpace(details.NetworkHostsPath) != "" {
		addFilesystem("network", filepath.Dir(details.NetworkHostsPath))
	}
	addFilesystem("network_hosts", details.NetworkHostsPath)
	addFilesystem("log", details.LogDirPath)
	if len(filesystem) == 0 {
		filesystem["test:runtime_resource"] = "lstat:absent"
	}
	return runtime.GenerationResourceCleanup{
		RunscDeleted:      true,
		CheckpointDeleted: true,
		ControlDirDeleted: true,
		BundleDirDeleted:  true,
		BridgeDirDeleted:  true,
		NetworkDirDeleted: true,
		LogDirDeleted:     true,
		NetnsDeleted:      true,
		HostVethDeleted:   true,
		NftTableDeleted:   true,
		RunscState:        "runsc_container:absent; check=test",
		IPNetns:           "netns:absent; check=test",
		IPLink:            "host_veth:absent; check=test",
		NFT:               "nft_table:absent; check=test",
		FilesystemLstat:   filesystem,
	}
}

func (r *recordingRuntime) Destroy(_ context.Context, runtimeID string) error {
	r.mu.Lock()
	r.destroyRuntimeIDs = append(r.destroyRuntimeIDs, runtimeID)
	err := r.destroyRuntimeErr
	r.mu.Unlock()
	return err
}

func (r *recordingRuntime) Interrupt(sessionID string) error {
	r.mu.Lock()
	r.interruptSessionIDs = append(r.interruptSessionIDs, sessionID)
	r.mu.Unlock()
	return nil
}

func (r *recordingRuntime) Checkpoint(_ context.Context, req runtime.CheckpointRequest) (runtime.CheckpointResult, error) {
	r.mu.Lock()
	r.checkpointReqs = append(r.checkpointReqs, req)
	err := r.checkpointErr
	r.mu.Unlock()
	if err != nil {
		return runtime.CheckpointResult{}, err
	}
	return runtime.CheckpointResult{ImageManifestDigest: checkpointImageManifestDigestForTest}, nil
}

func (r *recordingRuntime) requests() ([]runtime.StartRequest, []runtime.StartRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()
	prepares := append([]runtime.StartRequest(nil), r.prepareRequests...)
	starts := append([]runtime.StartRequest(nil), r.startRequests...)
	return prepares, starts
}

func (r *recordingRuntime) networkPrepareRequests() []runtime.StartRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]runtime.StartRequest(nil), r.networkRequests...)
}

func (r *recordingRuntime) checkpointRequests() []runtime.CheckpointRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]runtime.CheckpointRequest(nil), r.checkpointReqs...)
}

func (r *recordingRuntime) destroyGenerationRequests() []store.RuntimeGenerationDetails {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]store.RuntimeGenerationDetails(nil), r.destroyRequests...)
}

func (r *recordingRuntime) runtimeDestroyRequests() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.destroyRuntimeIDs...)
}

type restoreFailingRuntime struct {
	recordingRuntime
	err error
}

func (r *restoreFailingRuntime) Start(_ context.Context, req runtime.StartRequest, _ func(runtime.Output)) runtime.Result {
	r.mu.Lock()
	r.startRequests = append(r.startRequests, req)
	r.mu.Unlock()
	if req.RestoreFromCheckpoint {
		return runtime.Result{Err: r.err}
	}
	return serverRuntimeStartResult(req)
}

type restoreStartHookRuntime struct {
	recordingRuntime
	onRestoreStart func()
}

func (r *restoreStartHookRuntime) Start(_ context.Context, req runtime.StartRequest, _ func(runtime.Output)) runtime.Result {
	r.mu.Lock()
	r.startRequests = append(r.startRequests, req)
	r.mu.Unlock()
	if req.RestoreFromCheckpoint && r.onRestoreStart != nil {
		r.onRestoreStart()
	}
	return serverRuntimeStartResult(req)
}

type restoreValidationRuntime struct {
	restore       *runtime.Runtime
	startRequests []runtime.StartRequest
}

func (r *restoreValidationRuntime) PrepareGeneration(context.Context, runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	return testGenerationArtifacts(), nil
}

func (r *restoreValidationRuntime) RenderGenerationArtifacts(context.Context, runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	return runtime.GenerationArtifactProjection{Artifacts: testGenerationArtifacts()}, nil
}

func (r *restoreValidationRuntime) MaterializeGenerationArtifacts(runtime.StartRequest, runtime.GenerationArtifactProjection) error {
	return nil
}

func (r *restoreValidationRuntime) PrepareGenerationNetwork(context.Context, runtime.StartRequest) error {
	return nil
}

func (r *restoreValidationRuntime) Start(ctx context.Context, req runtime.StartRequest, output func(runtime.Output)) runtime.Result {
	r.startRequests = append(r.startRequests, req)
	if req.RestoreFromCheckpoint {
		return r.restore.Start(ctx, req, output)
	}
	return serverRuntimeStartResult(req)
}

func (r *restoreValidationRuntime) Destroy(context.Context, string) error {
	return nil
}

func (r *restoreValidationRuntime) DestroyGenerationResources(context.Context, store.RuntimeGenerationDetails) (runtime.GenerationResourceCleanup, error) {
	return runtime.GenerationResourceCleanup{}, nil
}

func (r *restoreValidationRuntime) Interrupt(string) error {
	return nil
}

func (r *restoreValidationRuntime) Checkpoint(context.Context, runtime.CheckpointRequest) (runtime.CheckpointResult, error) {
	return runtime.CheckpointResult{ImageManifestDigest: checkpointImageManifestDigestForTest}, nil
}

type serverCommandRunner struct {
	outputs map[string][]byte
	fail    map[string]error
}

func (r serverCommandRunner) CombinedOutput(_ context.Context, name string, args ...string) ([]byte, error) {
	key := strings.Join(append([]string{name}, args...), " ")
	if err := r.fail[key]; err != nil {
		return nil, err
	}
	if out, ok := r.outputs[key]; ok {
		return out, nil
	}
	return nil, nil
}

type failingRuntime struct {
	prepareErr    error
	err           error
	checkpointErr error
}

func (f failingRuntime) PrepareGeneration(context.Context, runtime.StartRequest) (runtime.GenerationArtifacts, error) {
	if f.prepareErr != nil {
		return runtime.GenerationArtifacts{}, f.prepareErr
	}
	return testGenerationArtifacts(), nil
}

func (f failingRuntime) RenderGenerationArtifacts(context.Context, runtime.StartRequest) (runtime.GenerationArtifactProjection, error) {
	if f.prepareErr != nil {
		return runtime.GenerationArtifactProjection{}, f.prepareErr
	}
	return runtime.GenerationArtifactProjection{Artifacts: testGenerationArtifacts()}, nil
}

func (f failingRuntime) MaterializeGenerationArtifacts(runtime.StartRequest, runtime.GenerationArtifactProjection) error {
	return nil
}

func (f failingRuntime) PrepareGenerationNetwork(context.Context, runtime.StartRequest) error {
	return nil
}

func (f failingRuntime) Start(context.Context, runtime.StartRequest, func(runtime.Output)) runtime.Result {
	return runtime.Result{Err: f.err}
}

func (f failingRuntime) Destroy(context.Context, string) error {
	return nil
}

func (f failingRuntime) DestroyGenerationResources(_ context.Context, details store.RuntimeGenerationDetails) (runtime.GenerationResourceCleanup, error) {
	return runtimeCleanupEvidenceForDetails(details), nil
}

func (f failingRuntime) Interrupt(string) error {
	return nil
}

func (f failingRuntime) Checkpoint(context.Context, runtime.CheckpointRequest) (runtime.CheckpointResult, error) {
	if f.checkpointErr != nil {
		return runtime.CheckpointResult{}, f.checkpointErr
	}
	return runtime.CheckpointResult{ImageManifestDigest: checkpointImageManifestDigestForTest}, nil
}

func testGenerationArtifacts() runtime.GenerationArtifacts {
	return runtime.GenerationArtifacts{
		BundleDir:               "/tmp/bundle",
		SpecPath:                "/tmp/bundle/config.json",
		ManifestPath:            "/tmp/control/session.json",
		ManifestDigest:          "manifest_digest",
		ProjectedManifestDigest: "projected_manifest_digest",
		BundleDigest:            "bundle_digest",
		RuntimeConfigDigest:     "runtime_config_digest",
		SpecDigest:              "spec_digest",
		RunscVersion:            "runsc test",
		RunscBinaryPath:         "/usr/local/bin/runsc-test",
		RunscBinaryDigest:       "sha256:runsc-test",
	}
}

func serverRuntimeStartResult(req runtime.StartRequest) runtime.Result {
	if err := writeServerBridgeBootstrapForRequest(req); err != nil {
		return runtime.Result{Err: err}
	}
	return runtime.Result{
		ControlManifestDigest: req.PreparedArtifacts.ManifestDigest,
		RunscVersion:          req.PreparedArtifacts.RunscVersion,
		PostStartProof:        serverPostStartProofForRequest(req),
	}
}

func writeServerBridgeBootstrapForRequest(req runtime.StartRequest) error {
	if strings.TrimSpace(req.Generation.BridgeDirPath) == "" {
		return nil
	}
	if err := bridge.EnsureLayout(req.Generation.BridgeDirPath); err != nil {
		return err
	}
	if err := bridge.TouchHeartbeat(req.Generation.BridgeDirPath, bridge.BridgeHeartbeatFile, time.Now().UTC()); err != nil {
		return err
	}
	outbox, err := bridge.OpenQueue(req.Generation.BridgeDirPath, bridge.OutboxDir)
	if err != nil {
		return err
	}
	ctx := context.Background()
	helloPayload, err := json.Marshal(map[string]any{"driver_id": req.DriverID, "protocol_version": 2, "turn_input_schema": "RunTurn"})
	if err != nil {
		return err
	}
	for _, envelope := range []bridge.Envelope{
		{
			RequestID:    "test_heartbeat",
			Type:         bridge.TypeHeartbeat,
			SessionID:    req.SessionID,
			GenerationID: req.GenerationID,
		},
		{
			RequestID:    "test_hello",
			Type:         bridge.TypeHello,
			SessionID:    req.SessionID,
			GenerationID: req.GenerationID,
			Payload:      helloPayload,
		},
		{
			RequestID:    "test_probe",
			Type:         bridge.TypeProbeNetwork,
			SessionID:    req.SessionID,
			GenerationID: req.GenerationID,
		},
	} {
		if _, err := outbox.Write(ctx, envelope); err != nil {
			return err
		}
	}
	return nil
}

func serverPostStartProofForRequest(req runtime.StartRequest) *store.RuntimeResourcePostStartProof {
	containerID := strings.TrimSpace(req.Generation.RunscContainerID)
	if containerID == "" {
		containerID = "harness-gen-" + req.GenerationID
	}
	runscPlatform := strings.TrimSpace(req.Generation.RunscPlatform)
	if runscPlatform == "" {
		runscPlatform = "systrap"
	}
	runscVersion := strings.TrimSpace(req.Generation.RunscVersion)
	if runscVersion == "" {
		runscVersion = req.PreparedArtifacts.RunscVersion
	}
	runscBinaryPath := strings.TrimSpace(req.Generation.RunscBinaryPath)
	if runscBinaryPath == "" {
		runscBinaryPath = req.PreparedArtifacts.RunscBinaryPath
	}
	runscBinaryDigest := strings.TrimSpace(req.Generation.RunscBinaryDigest)
	if runscBinaryDigest == "" {
		runscBinaryDigest = req.PreparedArtifacts.RunscBinaryDigest
	}
	return &store.RuntimeResourcePostStartProof{
		GenerationID:      req.Generation.GenerationID,
		RunscContainerID:  containerID,
		RunscState:        "runsc_container:" + containerID + ":running; check=test",
		RunscPlatform:     runscPlatform,
		RunscVersion:      runscVersion,
		RunscBinaryPath:   runscBinaryPath,
		RunscBinaryDigest: runscBinaryDigest,
		IPNetns:           "netns:present; check=test",
		IPLink:            "host_veth:present; check=test",
		NFT:               "nft_table:present; check=test",
	}
}

func serverBridgeHelloPayload(t *testing.T, driverID string) json.RawMessage {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"driver_id":         driverID,
		"protocol_version":  2,
		"turn_input_schema": "RunTurn",
	})
	if err != nil {
		t.Fatalf("marshal bridge hello payload: %v", err)
	}
	return payload
}

func recordServerRuntimeArtifacts(t *testing.T, ctx context.Context, st *store.Store, generationID, manifestDigest, runscVersion string) {
	t.Helper()
	artifacts := testGenerationArtifacts()
	recordServerRuntimeArtifactsWithRunsc(t, ctx, st, generationID, manifestDigest, runscVersion, artifacts.RunscBinaryPath, artifacts.RunscBinaryDigest)
}

func recordServerRuntimeArtifactsWithRunsc(t *testing.T, ctx context.Context, st *store.Store, generationID, manifestDigest, runscVersion, runscPath, runscDigest string) {
	t.Helper()
	artifacts := testGenerationArtifacts()
	artifacts.ManifestDigest = manifestDigest
	artifacts.ProjectedManifestDigest = manifestDigest
	artifacts.RunscVersion = runscVersion
	artifacts.RunscBinaryPath = runscPath
	artifacts.RunscBinaryDigest = runscDigest
	if err := st.RecordGenerationRuntimeArtifactDigests(ctx, generationID, store.GenerationRuntimeArtifactDigests{
		ControlManifestDigest:          artifacts.ManifestDigest,
		ProjectedControlManifestDigest: artifacts.ProjectedManifestDigest,
		BundleDigest:                   artifacts.BundleDigest,
		RuntimeConfigDigest:            artifacts.RuntimeConfigDigest,
		SpecDigest:                     artifacts.SpecDigest,
		RunscVersion:                   artifacts.RunscVersion,
		RunscBinaryPath:                artifacts.RunscBinaryPath,
		RunscBinaryDigest:              artifacts.RunscBinaryDigest,
	}); err != nil {
		t.Fatalf("record runtime artifacts: %v", err)
	}
	var sessionID string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT session_id
FROM runtime_generations
WHERE generation_id = ?`, generationID).Scan(&sessionID); err != nil {
		t.Fatalf("query generation session: %v", err)
	}
	storeServerGenerationPlanForArtifacts(t, ctx, st, sessionID, generationID, artifacts)
}

func mutateServerRuntimeArtifactDigestMirrors(t *testing.T, ctx context.Context, st *store.Store, generationID string) {
	t.Helper()
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generation_resources
SET control_manifest_digest = 'mutated_manifest_digest',
    projected_control_manifest_digest = 'mutated_projected_manifest_digest',
    bundle_digest = 'mutated_bundle_digest',
    runtime_config_digest = 'mutated_runtime_config_digest',
    spec_digest = 'mutated_spec_digest'
WHERE generation_id = ?`, generationID); err != nil {
		t.Fatalf("mutate runtime artifact digest mirrors: %v", err)
	}
}

func addServerGenerationPlanSkillsSnapshot(t *testing.T, ctx context.Context, st *store.Store, generationID string) store.ContentSnapshotRecord {
	t.Helper()
	snapshotPath := filepath.Join(t.TempDir(), "skills", generationID)
	snapshotDigest := writeServerContentSnapshotFixture(t, snapshotPath)
	snapshot, err := st.StoreContentSnapshot(ctx, store.StoreContentSnapshotParams{
		Kind:                 store.ContentSnapshotKindSkills,
		Digest:               snapshotDigest,
		ImmutableHostPath:    snapshotPath,
		MountDestination:     store.ContentSnapshotSkillsMount,
		SourceEvidenceDigest: "sha256:skills-source-" + generationID,
		RetentionClass:       "generation_plan",
	})
	if err != nil {
		t.Fatalf("store generation plan skills snapshot: %v", err)
	}
	plan, err := st.GetGenerationPlan(ctx, generationID)
	if err != nil {
		t.Fatalf("get generation plan for snapshot: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(plan.CanonicalPayload, &payload); err != nil {
		t.Fatalf("decode generation plan for snapshot: %v", err)
	}
	contentSnapshots := payload["content_snapshots"].(map[string]any)
	contentSnapshots[store.ContentSnapshotKindSkills] = map[string]any{
		"kind":                   snapshot.Kind,
		"digest":                 snapshot.Digest,
		"immutable_host_path":    snapshot.ImmutableHostPath,
		"mount_destination":      snapshot.MountDestination,
		"source_evidence_digest": snapshot.SourceEvidenceDigest,
		"retention_class":        snapshot.RetentionClass,
	}
	mounts := payload["mounts"].(map[string]any)
	snapshotMounts, ok := mounts["content_snapshots"].(map[string]any)
	if !ok {
		snapshotMounts = map[string]any{}
		mounts["content_snapshots"] = snapshotMounts
	}
	snapshotMounts[store.ContentSnapshotKindSkills] = map[string]any{
		"mount_name":  "skills_snapshot",
		"type":        "bind",
		"mode":        "ro",
		"exact":       true,
		"source":      snapshot.ImmutableHostPath,
		"destination": snapshot.MountDestination,
		"digest":      snapshot.Digest,
	}
	workspace := payload["data_volumes"].(map[string]any)["workspace"].(map[string]any)
	workspace["platform_content_mount_scope"] = "immutable_content_snapshots"
	canonical, err := store.CanonicalGenerationPlanPayload(payload)
	if err != nil {
		t.Fatalf("canonical generation plan with snapshot: %v", err)
	}
	planDigest := store.GenerationPlanDigest(canonical)
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plans
SET canonical_payload = ?,
    plan_digest = ?
WHERE generation_id = ?`, string(canonical), planDigest, generationID); err != nil {
		t.Fatalf("update generation plan snapshot payload: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE generation_plan_projections
SET plan_digest = ?
WHERE generation_id = ?`, planDigest, generationID); err != nil {
		t.Fatalf("update projection plan digests for snapshot payload: %v", err)
	}
	return snapshot
}

func writeServerContentSnapshotFixture(t *testing.T, root string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "skill"), 0o755); err != nil {
		t.Fatalf("create content snapshot fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("skills fixture\n"), 0o644); err != nil {
		t.Fatalf("write content snapshot readme: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "skill", "SKILL.md"), []byte("# Fixture\n"), 0o644); err != nil {
		t.Fatalf("write content snapshot skill: %v", err)
	}
	digest, err := contentSnapshotPathDigest(root)
	if err != nil {
		t.Fatalf("digest content snapshot fixture: %v", err)
	}
	return digest
}

func currentRunscBinaryMetadataForServerTest(t *testing.T) (string, string) {
	t.Helper()
	path, err := exec.LookPath("runsc")
	if err != nil {
		t.Fatalf("lookup runsc binary: %v", err)
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatalf("resolve runsc binary %q: %v", path, err)
	}
	data, err := os.ReadFile(canonical)
	if err != nil {
		t.Fatalf("read runsc binary %q: %v", canonical, err)
	}
	sum := sha256.Sum256(data)
	return canonical, fmt.Sprintf("sha256:%x", sum[:])
}

func writeServerTestAgentImageManifest(t *testing.T, rootfs string, drivers ...agents.ID) string {
	t.Helper()
	manifestPath, err := serverTestAgentImageManifest(rootfs, drivers...)
	if err != nil {
		t.Fatalf("write test agent image manifest: %v", err)
	}
	return manifestPath
}

func mustWriteServerTestAgentImageManifest(rootfs string, drivers ...agents.ID) string {
	manifestPath, err := serverTestAgentImageManifest(rootfs, drivers...)
	if err != nil {
		panic(err)
	}
	return manifestPath
}

func serverTestAgentImageManifest(rootfs string, drivers ...agents.ID) (string, error) {
	entries := make([]imageManifestDriver, 0, len(drivers))
	buildDrivers := make([]string, 0, len(drivers))
	for _, driverID := range drivers {
		spec, ok := agents.DriverSpecFor(string(driverID))
		if !ok {
			return "", fmt.Errorf("missing driver spec for %s", driverID)
		}
		binaryPath, err := expectedDriverBinaryPath(driverID)
		if err != nil {
			return "", fmt.Errorf("expected driver binary path: %w", err)
		}
		hostPath := filepath.Join(rootfs, strings.TrimPrefix(binaryPath, "/"))
		if err := os.MkdirAll(filepath.Dir(hostPath), 0o755); err != nil {
			return "", fmt.Errorf("mkdir driver binary parent: %w", err)
		}
		content := []byte("test binary for " + string(driverID) + "\n")
		if err := os.WriteFile(hostPath, content, 0o755); err != nil {
			return "", fmt.Errorf("write driver binary: %w", err)
		}
		sum := sha256.Sum256(content)
		entry, err := manifestDriverFromSpec(spec)
		if err != nil {
			return "", fmt.Errorf("manifest driver from spec: %w", err)
		}
		entry.InstalledBinaryDigest = fmt.Sprintf("sha256:%x", sum[:])
		entries = append(entries, entry)
		buildDrivers = append(buildDrivers, string(driverID))
	}
	manifest := map[string]any{
		"schema_version": 1,
		"build_input": map[string]any{
			"sandbox_agent_drivers": buildDrivers,
		},
		"drivers": entries,
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal manifest: %w", err)
	}
	data = append(data, '\n')
	manifestPath := filepath.Join(rootfs, "etc", "harness-image", "agents.json")
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o755); err != nil {
		return "", fmt.Errorf("mkdir manifest parent: %w", err)
	}
	if err := os.WriteFile(manifestPath, data, 0o644); err != nil {
		return "", fmt.Errorf("write manifest: %w", err)
	}
	return manifestPath, nil
}

func validServerGenerationPlanPayload() map[string]any {
	driver, _ := agents.DriverSpecFor("claude_code")
	provider, _ := agents.RuntimeProviderSpecFor("local_runsc")
	featurePolicy, _ := agents.FeaturePolicyPayload(agents.DefaultFeaturePolicyForDriver(driver))
	featurePolicyPayload := map[string]any{}
	for key, value := range featurePolicy {
		featurePolicyPayload[key] = value
	}
	featurePolicyPayload["capability_schema_version"] = agents.DriverCapabilitySchemaVersion
	featurePolicyPayload["capability_vocab_version"] = provider.CapabilityVocabulary
	featurePolicyPayload["driver_capabilities"] = agents.DriverCapabilityPayload(driver)
	featurePolicyPayload["runtime_provider_capabilities"] = agents.RuntimeProviderCapabilityPayload(provider)
	featurePolicyPayload["legacy_supports_interrupt"] = driver.SupportsInterrupt
	featurePolicyPayload["legacy_supports_compaction"] = driver.SupportsCompaction
	featurePolicyPayload["unsupported_features_fail"] = true
	featurePolicyPayload["credential_bearing_mcp_scope"] = "out_of_scope"
	adapterInputDigests := serverAdapterInputDigestPayloadForTest(serverFrozenEvidenceSandboxContractPayloadForTest(
		"sess_frozen_evidence",
		"gen_frozen_evidence",
		"contract_gen_frozen_evidence",
		"claude_code",
		"sha256:driver-state",
	))
	return map[string]any{
		"plan_version": store.GenerationPlanVersion,
		"identity":     map[string]any{"session_id": "sess_frozen_evidence", "generation_id": "gen_frozen_evidence", "product_mode": "agent"},
		"driver": map[string]any{
			"driver_id":               "claude_code",
			"driver_kind":             string(driver.Kind),
			"bridge_protocol":         driver.BridgeProtocol,
			"bridge_protocol_version": driver.BridgeProtocolVersion,
			"turn_input_schema":       driver.TurnInputSchema,
			"output_schema":           driver.OutputSchema,
			"output_format":           driver.OutputFormat,
			"model":                   "claude-test",
			"initial_state_digest":    "sha256:driver-state",
			"initial_state_version":   1,
			"capability_snapshot":     agents.DriverCapabilityPayload(driver),
		},
		"runtime_provider": map[string]any{
			"provider_id":                  provider.ID,
			"provider_config_id":           "local_runsc",
			"provider_profile_id":          provider.ProviderProfileID,
			"isolation_kind":               provider.IsolationKind,
			"template_ref":                 provider.TemplateRef,
			"capability_vocab_version":     provider.CapabilityVocabulary,
			"capability_digest":            agents.CapabilityDigest(provider),
			"capability_snapshot":          agents.RuntimeProviderCapabilityPayload(provider),
			"snapshot_policy":              provider.SnapshotPolicy,
			"agent_runtime_profile_id":     "arp_gen_frozen_evidence",
			"runtime_profile_provider_ref": "systrap",
		},
		"runsc_pin":    map[string]any{"platform": "systrap", "version": "runsc test", "binary_path": "/usr/local/bin/runsc-test", "binary_digest": "sha256:runsc"},
		"image":        map[string]any{"agent_manifest_digest": "sha256:agent-manifest", "rootfs_path": "/var/lib/harness/rootfs", "rootfs_image_digest": nil},
		"bridge_probe": map[string]any{"bridge_mode": "claim-loop"},
		"network": map[string]any{
			"network_profile_id": "net_gen_frozen_evidence", "runsc_network": "sandbox", "runsc_overlay2": "none",
			"sandbox_ip": "10.240.0.2", "sandbox_ip_cidr": "10.240.0.2/30", "host_gateway_ip": "10.240.0.1",
			"sandbox_base_url": "http://10.240.0.1:8080", "host_proxy_bind_url": "http://127.0.0.1:8080",
			"netns_name": "harness-gen-frozen", "netns_path": "/var/run/netns/harness-gen-frozen",
			"host_veth": "vh-frozen", "sandbox_veth": "vs-frozen", "host_side_cidr": "10.240.0.1/30",
			"nft_table_name": "harness-gen-frozen", "egress_policy_id": "egress_frozen",
			"egress_policy_digest": "egress_digest", "dns_policy": "off",
		},
		"data_volumes": map[string]any{
			"workspace":  serverPlanVolumePayload("/var/lib/harness/sessions/sess_frozen_evidence", "/var/lib/harness/evidence/workspaces/sess_frozen_evidence.json", "/workspace"),
			"agent_home": serverPlanVolumePayload("/var/lib/harness/agent-homes/sess_frozen_evidence/claude_code", "/var/lib/harness/evidence/driver-homes/sess_frozen_evidence/claude_code.json", "/agent-home"),
		},
		"mounts": map[string]any{
			"workspace":                      map[string]any{"source": "/var/lib/harness/sessions/sess_frozen_evidence", "destination": "/workspace", "mode": "rw"},
			"agent_home":                     map[string]any{"source": "/var/lib/harness/agent-homes/sess_frozen_evidence/claude_code", "destination": "/agent-home", "mode": "rw"},
			"control":                        map[string]any{"source": "/var/lib/harness/run/control/gen_frozen_evidence", "destination": "/harness-control", "mode": "ro"},
			"bridge":                         map[string]any{"source": "/var/lib/harness/run/bridge/gen_frozen_evidence", "destination": "/harness-control/bridge", "mode": "rw"},
			"network_hosts_path":             nil,
			"driver_config_materializations": nil,
		},
		"runtime_artifacts": map[string]any{
			"control_dir_path": "/var/lib/harness/run/control/gen_frozen_evidence", "control_manifest_path": "/var/lib/harness/run/control/gen_frozen_evidence/session.json",
			"control_manifest_digest": "manifest_digest", "projected_control_manifest_digest": "projected_manifest_digest",
			"bundle_dir_path": "/var/lib/harness/run/runtime/gen_frozen_evidence", "bundle_digest": "bundle_digest",
			"runtime_config_digest": "runtime_config_digest", "spec_path": "/var/lib/harness/run/runtime/gen_frozen_evidence/config.json",
			"spec_digest": "spec_digest", "bridge_dir_path": "/var/lib/harness/run/bridge/gen_frozen_evidence",
			"log_dir_path": "/var/lib/harness/logs/gen_frozen_evidence", "network_hosts_path": nil,
			"materialized_driver_config": []map[string]any{}, "resource_identity_digest": "sha256:resource",
			"sandbox_contract_id": "contract_gen_frozen_evidence", "sandbox_contract_payload_digest": "sha256:sandbox-contract",
			"sandbox_contract_compatibility_shape": store.SandboxContractVersion,
		},
		"feature_policy":    featurePolicyPayload,
		"content_snapshots": map[string]any{"skills": nil, "managed_settings": nil},
		"source_digests": map[string]any{
			"runtime_config_digest": "sha256:runtime-config-source",
			"agent_manifest_digest": "sha256:agent-manifest",
			"adapter_input_digests": adapterInputDigests,
		},
		"mutable_state_scope": map[string]any{"leases": "runtime_generations", "events": "events", "checkpoint_state": "runtime_generations"},
	}
}

func serverFrozenEvidenceSandboxContractPayloadForTest(sessionID, generationID, contractID, driverID, driverStateDigest string) map[string]any {
	modelAccessAllowed := driverID == "claude_code"
	return map[string]any{
		"sandbox_contract_version": store.SandboxContractVersion,
		"contract_schema_version":  store.SandboxContractSchemaVersion,
		"contract_gate_version":    store.SandboxContractGateDriverManifest,
		"contract_id":              contractID,
		"session_id":               sessionID,
		"generation_id":            generationID,
		"runtime_profile_id":       "arp_gen_frozen_evidence",
		"network_profile_id":       "net_gen_frozen_evidence",
		"driver": map[string]any{
			"driver_id":                            driverID,
			"driver_version":                       "test",
			"bridge_protocol":                      "harness_bridge_v2",
			"bridge_protocol_version":              2,
			"turn_input_schema":                    "RunTurn",
			"output_schema":                        "claude_stream_json_v1",
			"command_argv_digest":                  "sha256:command",
			"driver_config_digest":                 "sha256:driver-config",
			"required_runtime_capabilities_digest": "sha256:driver-capabilities",
			"supports_interrupt":                   false,
			"supports_compaction":                  true,
		},
		"runtime_provider": map[string]any{
			"provider_id":              "local_runsc",
			"provider_profile_id":      "local_runsc_default",
			"isolation_kind":           "gvisor",
			"template_ref":             "default",
			"template_digest":          "sha256:template",
			"capability_vocab_version": "1",
			"capability_digest":        "sha256:provider-capabilities",
		},
		"identity": map[string]any{
			"model_access_allowed": modelAccessAllowed,
		},
		"network_identity": map[string]any{
			"runsc_network": "sandbox",
			"sandbox_ip":    "10.240.0.2",
		},
		"credential_policy": serverCredentialPolicyPayloadForTest(driverID),
		"model_access": map[string]any{
			"model_access_allowed":         modelAccessAllowed,
			"sandbox_model_proxy_base_url": "http://harness-model-proxy.internal:8082",
		},
		"driver_runtime": map[string]any{
			"driver_home_mount":             "/agent-home",
			"generated_driver_config_mount": "/harness-control/driver/" + driverID,
			"materialized_driver_config":    map[string]any{},
			"initial_driver_state_digest":   driverStateDigest,
		},
		"runtime_adapter": map[string]any{
			"kind":                "runsc",
			"runsc_platform":      "systrap",
			"runsc_version":       "runsc test",
			"runsc_binary_path":   "/usr/local/bin/runsc-test",
			"runsc_binary_digest": "sha256:runsc",
			"runsc_container_id":  "runsc-gen-frozen",
			"runsc_network":       "sandbox",
			"runsc_overlay2":      "none",
		},
		"input_digests": map[string]any{
			"runtime_config_digest": "sha256:runtime-config",
			"rootfs_image_digest":   nil,
			"agent_manifest_digest": "sha256:agent-manifest",
		},
	}
}

func serverAdapterInputDigestPayloadForTest(contractPayload map[string]any) map[string]any {
	digests, err := generationplan.AdapterInputDigestsFromSandboxContract(contractPayload)
	if err != nil {
		panic(err)
	}
	return map[string]any{
		"driver_adapter":  digests["driver_adapter"],
		"runtime_adapter": digests["runtime_adapter"],
	}
}

func storeServerFrozenEvidenceCanonicalPayload(t *testing.T) []byte {
	t.Helper()
	canonical, err := serverFrozenEvidenceCanonicalPayload()
	if err != nil {
		t.Fatalf("canonical frozen evidence payload: %v", err)
	}
	return canonical
}

func serverFrozenEvidenceCanonicalPayload() ([]byte, error) {
	return store.CanonicalGenerationPlanPayload(validServerGenerationPlanPayload())
}

func mustServerFrozenEvidenceCanonicalPayload() []byte {
	canonical, err := serverFrozenEvidenceCanonicalPayload()
	if err != nil {
		panic(err)
	}
	return canonical
}

func storeServerFrozenEvidencePlan(t *testing.T, ctx context.Context, st *store.Store, dir string, payload map[string]any) store.GenerationPlanRecord {
	t.Helper()
	session := createServerTestSession(t, ctx, st, dir, "sess_frozen_evidence", string(sessionstate.Created), time.Now().UTC(), nil)
	if _, err := st.DBForTest().ExecContext(ctx, `
INSERT INTO runtime_generations (generation_id, session_id, status, lease_owner, lease_expires_at)
VALUES (?, ?, 'allocating', 'owner', ?)`, "gen_frozen_evidence", session.ID, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("insert runtime generation: %v", err)
	}
	plan, err := st.StoreGenerationPlan(ctx, store.StoreGenerationPlanParams{
		GenerationID: "gen_frozen_evidence",
		Payload:      payload,
	})
	if err != nil {
		t.Fatalf("store generation plan: %v", err)
	}
	for kind, digest := range map[string]string{
		"sandbox_contract":           "sha256:sandbox-contract",
		"control_manifest":           "sha256:control-manifest",
		"control_manifest_projected": "sha256:control-manifest-projected",
		"oci_spec":                   "sha256:oci-spec",
		"bundle":                     "sha256:bundle",
		"runtime_config":             "sha256:runtime-config",
	} {
		if _, err := st.StoreGenerationPlanProjection(ctx, store.StoreGenerationPlanProjectionParams{
			GenerationID:      "gen_frozen_evidence",
			PlanDigest:        plan.PlanDigest,
			ProjectionKind:    kind,
			ProjectionVersion: 1,
			PayloadDigest:     digest,
		}); err != nil {
			t.Fatalf("store projection %s: %v", kind, err)
		}
	}
	return plan
}

type serverGenerationPlanSourceDigestsForTest struct {
	RuntimeConfigDigest string
	AgentManifestDigest string
}

func storeServerSyntheticSandboxContractParentForPlan(t *testing.T, ctx context.Context, st *store.Store, plan store.GenerationPlanRecord) {
	t.Helper()
	sessionID := serverGenerationPlanSessionID(t, plan.CanonicalPayload)
	contractID := sandboxContractID(plan.GenerationID)
	canonicalPayload, err := store.CanonicalSandboxContractPayload(serverFrozenEvidenceSandboxContractPayloadForTest(
		sessionID,
		plan.GenerationID,
		contractID,
		"claude_code",
		"sha256:driver-state",
	))
	if err != nil {
		t.Fatalf("canonical synthetic sandbox contract parent: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
INSERT INTO sandbox_contracts (
  contract_id, generation_id, session_id, sandbox_contract_version,
  contract_schema_version, contract_gate_version, canonical_payload,
  sandbox_contract_digest, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(contract_id) DO NOTHING`,
		contractID, plan.GenerationID, sessionID, store.SandboxContractVersion,
		store.SandboxContractSchemaVersion, store.SandboxContractGateDriverManifest,
		string(canonicalPayload), store.SandboxContractDigest(canonicalPayload),
		time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("store synthetic sandbox contract parent: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET sandbox_contract_id = ?,
    sandbox_contract_version = ?
WHERE generation_id = ?
  AND session_id = ?`, contractID, store.SandboxContractVersion, plan.GenerationID, sessionID); err != nil {
		t.Fatalf("store synthetic sandbox contract generation mirror: %v", err)
	}
}

func storeServerSandboxContractInputEvidenceFromGenerationPlanIfPresent(t *testing.T, ctx context.Context, st *store.Store, generationID string) {
	t.Helper()
	plan, err := st.GetGenerationPlan(ctx, generationID)
	if errors.Is(err, sql.ErrNoRows) {
		return
	}
	if err != nil {
		t.Fatalf("get generation plan for input evidence: %v", err)
	}
	storeServerSandboxContractInputEvidenceFromPlan(t, ctx, st, plan)
}

func storeServerSandboxContractInputEvidenceFromPlan(t *testing.T, ctx context.Context, st *store.Store, plan store.GenerationPlanRecord) {
	t.Helper()
	digests := serverGenerationPlanSourceDigests(t, plan.CanonicalPayload)
	contractID := sandboxContractID(plan.GenerationID)
	if _, err := st.DBForTest().ExecContext(ctx, `
INSERT INTO sandbox_contract_input_evidence (
  contract_id, runtime_config_digest, runtime_config_preimage,
  agent_manifest_digest, agent_manifest_payload, created_at
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(contract_id) DO NOTHING`,
		contractID, digests.RuntimeConfigDigest, "{}",
		digests.AgentManifestDigest, "{}", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("store sandbox contract input evidence: %v", err)
	}
	evidence, err := st.GetSandboxContractInputEvidence(ctx, contractID)
	if err != nil {
		t.Fatalf("get sandbox contract input evidence: %v", err)
	}
	if evidence.RuntimeConfigDigest != digests.RuntimeConfigDigest ||
		evidence.AgentManifestDigest != digests.AgentManifestDigest {
		t.Fatalf("sandbox contract input evidence mismatch: evidence=%+v want=%+v", evidence, digests)
	}
}

func serverGenerationPlanSessionID(t *testing.T, canonicalPayload []byte) string {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(canonicalPayload, &payload); err != nil {
		t.Fatalf("decode generation plan payload: %v", err)
	}
	identity, ok := payload["identity"].(map[string]any)
	if !ok {
		t.Fatalf("generation plan missing identity: %s", canonicalPayload)
	}
	sessionID, _ := identity["session_id"].(string)
	if strings.TrimSpace(sessionID) == "" {
		t.Fatalf("generation plan missing identity.session_id: %s", canonicalPayload)
	}
	return sessionID
}

func serverGenerationPlanSourceDigests(t *testing.T, canonicalPayload []byte) serverGenerationPlanSourceDigestsForTest {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(canonicalPayload, &payload); err != nil {
		t.Fatalf("decode generation plan payload: %v", err)
	}
	sourceDigests, ok := payload["source_digests"].(map[string]any)
	if !ok {
		t.Fatalf("generation plan missing source_digests: %s", canonicalPayload)
	}
	digests := serverGenerationPlanSourceDigestsForTest{}
	digests.RuntimeConfigDigest, _ = sourceDigests["runtime_config_digest"].(string)
	digests.AgentManifestDigest, _ = sourceDigests["agent_manifest_digest"].(string)
	if strings.TrimSpace(digests.RuntimeConfigDigest) == "" ||
		strings.TrimSpace(digests.AgentManifestDigest) == "" {
		t.Fatalf("generation plan missing source digests: %s", canonicalPayload)
	}
	return digests
}

func storeServerGenerationPlanForArtifacts(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string, artifacts runtime.GenerationArtifacts) store.GenerationPlanRecord {
	t.Helper()
	details, err := st.GetRuntimeGenerationDetails(ctx, sessionID, generationID)
	if err != nil {
		t.Fatalf("get generation details for plan %s: %v", generationID, err)
	}
	mode := "agent"
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT COALESCE(mode, '')
FROM sessions
WHERE id = ?`, sessionID).Scan(&mode); err != nil {
		t.Fatalf("get session mode for plan %s: %v", generationID, err)
	}
	if strings.TrimSpace(mode) == "" {
		mode = "agent"
	}
	driverSpec, ok := agents.DriverSpecFor(details.DriverID)
	if !ok {
		t.Fatalf("driver spec missing for %s", details.DriverID)
	}
	providerSpec, ok := agents.RuntimeProviderSpecFor("local_runsc")
	if !ok {
		t.Fatalf("provider spec missing")
	}
	featurePolicy, err := agents.FeaturePolicyPayload(agents.DefaultFeaturePolicyForDriver(driverSpec))
	if err != nil {
		t.Fatalf("feature policy for plan %s: %v", generationID, err)
	}
	featurePolicyPayload := map[string]any{}
	for key, value := range featurePolicy {
		featurePolicyPayload[key] = value
	}
	featurePolicyPayload["capability_schema_version"] = agents.DriverCapabilitySchemaVersion
	featurePolicyPayload["capability_vocab_version"] = providerSpec.CapabilityVocabulary
	featurePolicyPayload["driver_capabilities"] = agents.DriverCapabilityPayload(driverSpec)
	featurePolicyPayload["runtime_provider_capabilities"] = agents.RuntimeProviderCapabilityPayload(providerSpec)
	featurePolicyPayload["legacy_supports_interrupt"] = driverSpec.SupportsInterrupt
	featurePolicyPayload["legacy_supports_compaction"] = driverSpec.SupportsCompaction
	featurePolicyPayload["unsupported_features_fail"] = true
	featurePolicyPayload["credential_bearing_mcp_scope"] = "out_of_scope"
	payload := validServerGenerationPlanPayload()
	payload["identity"] = map[string]any{"session_id": sessionID, "generation_id": generationID, "product_mode": mode}
	workspaceVolume, driverHomeVolume := provisionServerGenerationPlanFixtureVolumes(t, ctx, st, sessionID, details)
	driverPlan := payload["driver"].(map[string]any)
	driverPlan["driver_id"] = string(driverSpec.ID)
	driverPlan["driver_kind"] = string(driverSpec.Kind)
	driverPlan["bridge_protocol"] = driverSpec.BridgeProtocol
	driverPlan["bridge_protocol_version"] = driverSpec.BridgeProtocolVersion
	driverPlan["turn_input_schema"] = driverSpec.TurnInputSchema
	driverPlan["output_schema"] = driverSpec.OutputSchema
	driverPlan["output_format"] = details.OutputFormat
	if strings.TrimSpace(details.Model) == "" {
		driverPlan["model"] = nil
	} else {
		driverPlan["model"] = details.Model
	}
	driverPlan["initial_state_digest"] = details.DriverStateDigest
	driverPlan["initial_state_version"] = details.DriverStateVersion
	driverPlan["capability_snapshot"] = agents.DriverCapabilityPayload(driverSpec)
	runtimeProvider := payload["runtime_provider"].(map[string]any)
	runtimeProvider["agent_runtime_profile_id"] = details.AgentRuntimeProfileID
	runtimeProvider["runtime_profile_provider_ref"] = details.RunscPlatform
	networkPlan := payload["network"].(map[string]any)
	networkPlan["network_profile_id"] = details.NetworkProfileID
	networkPlan["runsc_network"] = details.RunscNetwork
	networkPlan["runsc_overlay2"] = details.RunscOverlay2
	sandboxIP, err := runtimeResourceSandboxIP(details.SandboxIPCIDR)
	if err != nil {
		t.Fatalf("render sandbox ip for plan %s: %v", generationID, err)
	}
	networkPlan["sandbox_ip"] = sandboxIP
	networkPlan["sandbox_ip_cidr"] = details.SandboxIPCIDR
	networkPlan["host_gateway_ip"] = details.HostGatewayIP
	networkPlan["sandbox_base_url"] = details.SandboxBaseURL
	networkPlan["host_proxy_bind_url"] = details.HostProxyBindURL
	networkPlan["proxy_port"] = details.ProxyPort
	networkPlan["netns_name"] = details.NetnsName
	networkPlan["netns_path"] = details.NetnsPath
	networkPlan["host_veth"] = details.HostVeth
	networkPlan["sandbox_veth"] = details.SandboxVeth
	networkPlan["host_side_cidr"] = details.HostSideCIDR
	nftTableName, err := runtimeResourceNftTableName(details.GenerationID)
	if err != nil {
		t.Fatalf("render nft table for plan %s: %v", generationID, err)
	}
	networkPlan["nft_table_name"] = nftTableName
	networkPlan["egress_policy_id"] = details.EgressPolicyID
	networkPlan["egress_policy_digest"] = details.EgressPolicyDigest
	networkPlan["dns_policy"] = details.DNSPolicy
	runscPin := payload["runsc_pin"].(map[string]any)
	runscPin["version"] = artifacts.RunscVersion
	runscPin["binary_path"] = artifacts.RunscBinaryPath
	runscPin["binary_digest"] = artifacts.RunscBinaryDigest
	runtimeArtifacts := payload["runtime_artifacts"].(map[string]any)
	runtimeArtifacts["control_manifest_digest"] = artifacts.ManifestDigest
	runtimeArtifacts["projected_control_manifest_digest"] = artifacts.ProjectedManifestDigest
	runtimeArtifacts["control_dir_path"] = details.ControlDirPath
	runtimeArtifacts["control_manifest_path"] = details.ControlManifestPath
	runtimeArtifacts["bundle_dir_path"] = details.BundleDirPath
	runtimeArtifacts["bundle_digest"] = artifacts.BundleDigest
	runtimeArtifacts["runtime_config_digest"] = artifacts.RuntimeConfigDigest
	runtimeArtifacts["spec_path"] = details.SpecPath
	runtimeArtifacts["spec_digest"] = artifacts.SpecDigest
	runtimeArtifacts["bridge_dir_path"] = details.BridgeDirPath
	runtimeArtifacts["log_dir_path"] = details.LogDirPath
	if strings.TrimSpace(details.NetworkHostsPath) == "" {
		runtimeArtifacts["network_hosts_path"] = nil
	} else {
		runtimeArtifacts["network_hosts_path"] = details.NetworkHostsPath
	}
	allocation := serverGenerationAllocationForTest(t, ctx, st, sessionID, generationID)
	sandboxContractPayload := serverRuntimeResourceSandboxContractPayloadForTest(t, details, allocation, sandboxContractID(generationID))
	sandboxContractDigest := serverSandboxContractPayloadDigestForTest(t, sandboxContractPayload)
	runtimeArtifacts["sandbox_contract_id"] = sandboxContractID(generationID)
	runtimeArtifacts["sandbox_contract_payload_digest"] = sandboxContractDigest
	runtimeArtifacts["resource_identity_digest"] = serverRuntimeResourceIdentityDigestForPlanFixture(t, details, artifacts)
	sourceDigests := payload["source_digests"].(map[string]any)
	sourceDigests["adapter_input_digests"] = serverAdapterInputDigestPayloadForTest(sandboxContractPayload)
	dataVolumes := payload["data_volumes"].(map[string]any)
	serverApplyWorkspaceVolumePayload(dataVolumes["workspace"].(map[string]any), workspaceVolume)
	serverApplyDriverHomeVolumePayload(dataVolumes["agent_home"].(map[string]any), driverHomeVolume)
	mounts := payload["mounts"].(map[string]any)
	mounts["workspace"].(map[string]any)["source"] = workspaceVolume.HostPath
	mounts["agent_home"].(map[string]any)["source"] = driverHomeVolume.HostPath
	mounts["control"].(map[string]any)["source"] = details.ControlDirPath
	mounts["bridge"].(map[string]any)["source"] = details.BridgeDirPath
	if strings.TrimSpace(details.NetworkHostsPath) == "" {
		mounts["network_hosts_path"] = nil
	} else {
		mounts["network_hosts_path"] = details.NetworkHostsPath
	}
	payload["feature_policy"] = featurePolicyPayload
	plan, err := st.StoreGenerationPlan(ctx, store.StoreGenerationPlanParams{
		GenerationID: generationID,
		Payload:      payload,
	})
	if err != nil {
		t.Fatalf("store generation plan for %s: %v", generationID, err)
	}
	projections := append([]store.GenerationPlanProjectionExpectation{{
		ProjectionKind:    store.GenerationPlanProjectionSandboxContract,
		ProjectionVersion: store.GenerationPlanProjectionVersion,
		PayloadDigest:     sandboxContractDigest,
	}}, planprojection.ExpectationsForDetails(details, artifacts)...)
	for _, projection := range projections {
		if _, err := st.StoreGenerationPlanProjection(ctx, store.StoreGenerationPlanProjectionParams{
			GenerationID:      generationID,
			PlanDigest:        plan.PlanDigest,
			ProjectionKind:    projection.ProjectionKind,
			ProjectionVersion: projection.ProjectionVersion,
			PayloadDigest:     projection.PayloadDigest,
			MaterializedPath:  projection.MaterializedPath,
		}); err != nil {
			t.Fatalf("store generation plan projection %s for %s: %v", projection.ProjectionKind, generationID, err)
		}
	}
	return plan
}

func serverRuntimeResourceIdentityDigestForPlanFixture(t *testing.T, details store.RuntimeGenerationDetails, artifacts runtime.GenerationArtifacts) string {
	t.Helper()
	sandboxIP, err := runtimeResourceSandboxIP(details.SandboxIPCIDR)
	if err != nil {
		t.Fatalf("runtime resource sandbox ip for plan %s: %v", details.GenerationID, err)
	}
	nftTableName, err := runtimeResourceNftTableName(details.GenerationID)
	if err != nil {
		t.Fatalf("runtime resource nft table for plan %s: %v", details.GenerationID, err)
	}
	params := store.RuntimeResourceInstanceParams{
		GenerationID:           details.GenerationID,
		SessionID:              details.SessionID,
		ContractID:             sandboxContractID(details.GenerationID),
		SandboxContractVersion: store.SandboxContractVersion,
		HostID:                 mustRuntimeResourceHostID(t),
		RunscContainerID:       details.RunscContainerID,
		RunscPlatform:          details.RunscPlatform,
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
		NftTableName:           nftTableName,
		ControlDirPath:         details.ControlDirPath,
		ControlManifestPath:    details.ControlManifestPath,
		BundleDirPath:          details.BundleDirPath,
		SpecPath:               details.SpecPath,
		CheckpointPath:         details.CheckpointPath,
		BridgeDirPath:          details.BridgeDirPath,
		NetworkHostsPath:       details.NetworkHostsPath,
		LogDirPath:             details.LogDirPath,
		RootPrefixes: map[string]string{
			"run_dir":      filepath.Dir(filepath.Dir(details.ControlDirPath)),
			"control_root": filepath.Dir(details.ControlDirPath),
			"bridge_root":  filepath.Dir(details.BridgeDirPath),
		},
	}
	_, digest, err := store.RuntimeResourceIdentityForParams(params)
	if err != nil {
		t.Fatalf("runtime resource identity for plan %s: %v", details.GenerationID, err)
	}
	return digest
}

func provisionServerGenerationPlanFixtureVolumes(t *testing.T, ctx context.Context, st *store.Store, sessionID string, details store.RuntimeGenerationDetails) (store.SessionWorkspaceVolume, store.SessionDriverHomeVolume) {
	t.Helper()
	cfg := serverGenerationPlanFixtureVolumeConfig(t, details)
	now := time.Now().UTC()
	workspace, err := st.ProvisionSessionWorkspace(ctx, store.ProvisionSessionWorkspaceParams{
		SessionID: sessionID,
		Config:    cfg,
		Now:       now,
	})
	if err != nil {
		t.Fatalf("provision workspace volume for plan %s: %v", details.GenerationID, err)
	}
	driverHome, err := st.ProvisionSessionDriverHome(ctx, store.ProvisionSessionDriverHomeParams{
		SessionID: sessionID,
		Driver:    details.DriverID,
		Config:    cfg,
		Now:       now,
	})
	if err != nil {
		t.Fatalf("provision driver-home volume for plan %s: %v", details.GenerationID, err)
	}
	return workspace, driverHome
}

func serverGenerationPlanFixtureVolumeConfig(t *testing.T, details store.RuntimeGenerationDetails) store.DataVolumeProvisionerConfig {
	t.Helper()
	controlDir := strings.TrimSpace(details.ControlDirPath)
	if controlDir == "" {
		t.Fatalf("generation %s control dir path is required for plan fixture volumes", details.GenerationID)
	}
	runDir := filepath.Dir(filepath.Dir(controlDir))
	fixtureRoot := filepath.Dir(runDir)
	if runDir == "." || fixtureRoot == "." {
		t.Fatalf("generation %s control dir path %q cannot derive plan fixture roots", details.GenerationID, controlDir)
	}
	return store.DataVolumeProvisionerConfig{
		SessionsRoot:   filepath.Join(fixtureRoot, "sessions"),
		AgentHomesRoot: filepath.Join(fixtureRoot, "agent-homes"),
		EvidenceRoot:   filepath.Join(fixtureRoot, "state", "volume-evidence"),
		LayoutVersion:  store.DataVolumeLayoutVersion,
		RuntimeIdentity: store.RuntimeIdentity{
			UID: serverTestSandboxUID(),
			GID: serverTestSandboxGID(),
		},
	}
}

func serverApplyWorkspaceVolumePayload(payload map[string]any, volume store.SessionWorkspaceVolume) {
	payload["session_id"] = volume.SessionID
	payload["host_path"] = volume.HostPath
	payload["layout_version"] = volume.LayoutVersion
	payload["runtime_identity_digest"] = volume.RuntimeIdentityDigest
	payload["provisioning_marker_path"] = volume.ProvisioningMarkerPath
	payload["provisioning_marker_digest"] = volume.ProvisioningMarkerDigest
	payload["sandbox_uid"] = volume.SandboxUID
	payload["sandbox_gid"] = volume.SandboxGID
	payload["sandbox_supplemental_gids"] = append([]int(nil), volume.SandboxSupplementalGIDs...)
}

func serverApplyDriverHomeVolumePayload(payload map[string]any, volume store.SessionDriverHomeVolume) {
	payload["session_id"] = volume.SessionID
	payload["driver"] = volume.Driver
	payload["host_path"] = volume.HostPath
	payload["layout_version"] = volume.LayoutVersion
	payload["runtime_identity_digest"] = volume.RuntimeIdentityDigest
	payload["provisioning_marker_path"] = volume.ProvisioningMarkerPath
	payload["provisioning_marker_digest"] = volume.ProvisioningMarkerDigest
	payload["sandbox_uid"] = volume.SandboxUID
	payload["sandbox_gid"] = volume.SandboxGID
	payload["sandbox_supplemental_gids"] = append([]int(nil), volume.SandboxSupplementalGIDs...)
}

func serverPlanVolumePayload(hostPath, markerPath, destination string) map[string]any {
	return map[string]any{
		"session_id": "sess_frozen_evidence", "host_path": hostPath, "layout_version": 1,
		"runtime_identity_digest": "sha256:identity", "provisioning_marker_path": markerPath,
		"provisioning_marker_digest": "sha256:marker", "sandbox_destination": destination,
		"sandbox_uid": 65534, "sandbox_gid": 65534, "sandbox_supplemental_gids": []int{},
	}
}

func serverGenerationPlanFrozenEvidenceDetails() store.RuntimeGenerationDetails {
	return store.RuntimeGenerationDetails{
		GenerationID:                    "gen_frozen_evidence",
		SessionID:                       "sess_frozen_evidence",
		RunscPlatform:                   "systrap",
		CheckpointBundleDigest:          "sha256:bundle",
		CheckpointRuntimeConfigDigest:   "sha256:runtime-config",
		CheckpointControlManifestDigest: "sha256:control-manifest-projected",
		CheckpointDriverStatesDigest:    "sha256:driver-state-fence",
		CheckpointPlanDigest:            store.GenerationPlanDigest(mustServerFrozenEvidenceCanonicalPayload()),
	}
}

func serverGenerationPlanFrozenEvidenceArtifacts() runtime.GenerationArtifacts {
	return runtime.GenerationArtifacts{
		ManifestDigest:          "sha256:control-manifest",
		ProjectedManifestDigest: "sha256:control-manifest-projected",
		BundleDigest:            "sha256:bundle",
		RuntimeConfigDigest:     "sha256:runtime-config",
		SpecDigest:              "sha256:oci-spec",
		RunscVersion:            "runsc test",
		RunscBinaryPath:         "/usr/local/bin/runsc-test",
		RunscBinaryDigest:       "sha256:runsc",
	}
}

func createServerRuntimeResourceLive(t *testing.T, ctx context.Context, st *store.Store, sessionID string, allocation store.GenerationAllocation, ownerUUID, hostID string, now time.Time) store.RuntimeResourceInstance {
	t.Helper()
	contractID := sandboxContractID(allocation.GenerationID)
	details, err := st.GetRuntimeGenerationDetails(ctx, sessionID, allocation.GenerationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	prefix, err := netip.ParsePrefix(details.SandboxIPCIDR)
	if err != nil {
		t.Fatalf("parse sandbox cidr: %v", err)
	}
	if _, err := st.StoreSandboxContract(ctx, store.StoreSandboxContractParams{
		ContractID:             contractID,
		SessionID:              sessionID,
		GenerationID:           allocation.GenerationID,
		SandboxContractVersion: store.SandboxContractVersion,
		ContractSchemaVersion:  store.SandboxContractSchemaVersion,
		ContractGateVersion:    store.SandboxContractGateDriverManifest,
		Payload:                serverRuntimeResourceSandboxContractPayloadForTest(t, details, allocation, contractID),
		Now:                    now,
	}); err != nil {
		t.Fatalf("store sandbox contract: %v", err)
	}
	storeServerSandboxContractInputEvidenceFromGenerationPlanIfPresent(t, ctx, st, allocation.GenerationID)
	artifacts := testGenerationArtifacts()
	if strings.TrimSpace(details.RunscVersion) != "" {
		artifacts.RunscVersion = details.RunscVersion
	}
	if strings.TrimSpace(details.RunscBinaryPath) != "" {
		artifacts.RunscBinaryPath = details.RunscBinaryPath
	}
	if strings.TrimSpace(details.RunscBinaryDigest) != "" {
		artifacts.RunscBinaryDigest = details.RunscBinaryDigest
	}
	instance, err := st.CreateRuntimeResourceInstance(ctx, store.RuntimeResourceInstanceParams{
		GenerationID:           allocation.GenerationID,
		SessionID:              sessionID,
		ContractID:             contractID,
		SandboxContractVersion: store.SandboxContractVersion,
		HostID:                 hostID,
		RunscContainerID:       details.RunscContainerID,
		RunscPlatform:          "systrap",
		RunscVersion:           artifacts.RunscVersion,
		RunscBinaryPath:        artifacts.RunscBinaryPath,
		RunscBinaryDigest:      artifacts.RunscBinaryDigest,
		NetworkProfileID:       allocation.NetworkProfileID,
		NetnsName:              details.NetnsName,
		NetnsPath:              details.NetnsPath,
		HostVeth:               details.HostVeth,
		SandboxVeth:            details.SandboxVeth,
		HostGatewayIP:          details.HostGatewayIP,
		SandboxIP:              prefix.Addr().String(),
		SandboxIPCIDR:          details.SandboxIPCIDR,
		HostSideCIDR:           details.HostSideCIDR,
		NftTableName:           mustRuntimeResourceNftTableName(t, allocation.GenerationID),
		ControlDirPath:         details.ControlDirPath,
		ControlManifestPath:    details.ControlManifestPath,
		BundleDirPath:          details.BundleDirPath,
		SpecPath:               details.SpecPath,
		CheckpointPath:         details.CheckpointPath,
		BridgeDirPath:          details.BridgeDirPath,
		NetworkHostsPath:       details.NetworkHostsPath,
		LogDirPath:             details.LogDirPath,
		RootPrefixes: map[string]string{
			"run_dir":      filepath.Dir(filepath.Dir(details.ControlDirPath)),
			"control_root": filepath.Dir(details.ControlDirPath),
			"bridge_root":  filepath.Dir(details.BridgeDirPath),
		},
		Now: now,
	})
	if err != nil {
		t.Fatalf("create runtime resource instance: %v", err)
	}
	workerID := strings.TrimSpace(ownerUUID)
	if workerID == "" {
		workerID = strings.TrimSuffix(strings.TrimSpace(allocation.Owner), ":"+store.RuntimeManagerRoleTag)
	}
	if err := st.ClaimRuntimeResourceMaterialization(ctx, store.RuntimeResourceMaterializationClaimParams{
		GenerationID:     allocation.GenerationID,
		WorkerID:         workerID,
		HostID:           hostID,
		LeaseExpiresAt:   now.Add(time.Minute),
		IdempotencyToken: "test:" + allocation.GenerationID,
		Now:              now.Add(time.Millisecond),
	}); err != nil {
		t.Fatalf("claim runtime resource materialization: %v", err)
	}
	if err := st.MarkRuntimeResourceReady(ctx, store.RuntimeResourceWorkerTransitionParams{
		GenerationID: allocation.GenerationID,
		WorkerID:     workerID,
		HostID:       hostID,
		Now:          now.Add(2 * time.Millisecond),
	}); err != nil {
		t.Fatalf("mark runtime resource ready: %v", err)
	}
	if err := st.MarkRuntimeResourceLive(ctx, store.RuntimeResourceWorkerTransitionParams{
		GenerationID: allocation.GenerationID,
		WorkerID:     workerID,
		HostID:       hostID,
		PostStart:    serverPostStartProofForTest(instance),
		Now:          now.Add(3 * time.Millisecond),
	}); err != nil {
		t.Fatalf("mark runtime resource live: %v", err)
	}
	return instance
}

func serverRuntimeResourceSandboxContractPayloadForTest(t *testing.T, details store.RuntimeGenerationDetails, allocation store.GenerationAllocation, contractID string) map[string]any {
	t.Helper()
	sandboxIP, err := runtimeResourceSandboxIP(details.SandboxIPCIDR)
	if err != nil {
		t.Fatalf("sandbox contract sandbox ip for %s: %v", allocation.GenerationID, err)
	}
	driverID := allocation.DriverState.DriverID
	credentialPolicy := serverCredentialPolicyForTest(t, driverID)
	modelAccessAllowed := driverID == "claude_code"
	return map[string]any{
		"sandbox_contract_version": store.SandboxContractVersion,
		"contract_schema_version":  store.SandboxContractSchemaVersion,
		"contract_gate_version":    store.SandboxContractGateDriverManifest,
		"contract_id":              contractID,
		"session_id":               details.SessionID,
		"generation_id":            allocation.GenerationID,
		"runtime_profile_id":       allocation.AgentRuntimeProfileID,
		"network_profile_id":       allocation.NetworkProfileID,
		"driver": map[string]any{
			"driver_id":                            driverID,
			"driver_version":                       "test",
			"bridge_protocol":                      "harness_bridge_v2",
			"bridge_protocol_version":              2,
			"turn_input_schema":                    "RunTurn",
			"output_schema":                        "claude_stream_json_v1",
			"command_argv_digest":                  "sha256:command",
			"driver_config_digest":                 "sha256:driver-config",
			"required_runtime_capabilities_digest": "sha256:driver-capabilities",
			"supports_interrupt":                   false,
			"supports_compaction":                  true,
		},
		"runtime_provider": map[string]any{
			"provider_id":              "local_runsc",
			"provider_profile_id":      "local_runsc_default",
			"isolation_kind":           "gvisor",
			"template_ref":             "default",
			"template_digest":          "sha256:template",
			"capability_vocab_version": "1",
			"capability_digest":        "sha256:provider-capabilities",
		},
		"identity": map[string]any{
			"model_access_allowed": modelAccessAllowed,
		},
		"network_identity": map[string]any{
			"runsc_network": details.RunscNetwork,
			"sandbox_ip":    sandboxIP,
		},
		"credential_policy": credentialPolicy,
		"model_access": map[string]any{
			"model_access_allowed": modelAccessAllowed,
		},
		"driver_runtime": map[string]any{
			"driver_home_mount":             "/agent-home",
			"generated_driver_config_mount": "/harness-control/driver/" + driverID,
			"materialized_driver_config":    map[string]any{},
			"initial_driver_state_digest":   allocation.DriverState.StateDigest,
		},
		"runtime_adapter": map[string]any{
			"kind":                "runsc",
			"runsc_platform":      details.RunscPlatform,
			"runsc_version":       details.RunscVersion,
			"runsc_binary_path":   details.RunscBinaryPath,
			"runsc_binary_digest": details.RunscBinaryDigest,
			"runsc_container_id":  details.RunscContainerID,
			"runsc_network":       details.RunscNetwork,
			"runsc_overlay2":      details.RunscOverlay2,
		},
		"input_digests": map[string]any{
			"runtime_config_digest": "sha256:runtime-config",
			"rootfs_image_digest":   nil,
			"agent_manifest_digest": "sha256:agent-manifest",
		},
	}
}

func serverSandboxContractPayloadDigestForTest(t *testing.T, payload map[string]any) string {
	t.Helper()
	canonical, err := store.CanonicalSandboxContractPayload(payload)
	if err != nil {
		t.Fatalf("canonical sandbox contract payload: %v", err)
	}
	return store.SandboxContractDigest(canonical)
}

func serverCredentialPolicyForTest(t *testing.T, driverID string) map[string]any {
	t.Helper()
	return serverCredentialPolicyPayloadForTest(driverID)
}

func serverCredentialPolicyPayloadForTest(driverID string) map[string]any {
	secretGrants := []map[string]any{}
	if driverID == "claude_code" {
		secretGrants = append(secretGrants, map[string]any{
			"grant_id":                  "model_provider:anthropic_proxy",
			"domain":                    "model_provider",
			"scope":                     "anthropic_messages",
			"exposure_mode":             "proxy_only",
			"ttl_seconds":               nil,
			"allowed_drivers":           []string{driverID},
			"allowed_runtime_providers": []string{"local_runsc"},
		})
	}
	policy := map[string]any{
		"provider_credentials": "host-only",
		"sandbox_secret_mount": "absent",
		"proxy_token":          "absent",
		"secret_grants":        secretGrants,
	}
	digest, err := store.CredentialPolicyDigest(policy)
	if err != nil {
		panic(err)
	}
	policy["digest"] = digest
	return policy
}

func serverPostStartProofForTest(instance store.RuntimeResourceInstance) *store.RuntimeResourcePostStartProof {
	return &store.RuntimeResourcePostStartProof{
		HostID:                 instance.HostID,
		GenerationID:           instance.GenerationID,
		ContractID:             instance.ContractID,
		SandboxContractVersion: instance.SandboxContractVersion,
		RunscContainerID:       instance.RunscContainerID,
		RunscState:             "runsc_container:" + instance.RunscContainerID + ":running; check=test",
		RunscPlatform:          instance.RunscPlatform,
		RunscVersion:           instance.RunscVersion,
		RunscBinaryPath:        instance.RunscBinaryPath,
		RunscBinaryDigest:      instance.RunscBinaryDigest,
		IPNetns:                "netns:present; check=test",
		IPLink:                 "host_veth:present; check=test",
		NFT:                    "nft_table:present; check=test",
		BridgeStartup:          "bridge_startup_probe:passed; check=test",
	}
}

type serverCheckpointImageManifest struct {
	Version int                                 `json:"version"`
	Files   []serverCheckpointImageManifestFile `json:"files"`
}

type serverCheckpointImageManifestFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

func writeServerCheckpointFilesWithoutManifest(t *testing.T, checkpointPath string) {
	t.Helper()
	if err := os.MkdirAll(checkpointPath, 0o755); err != nil {
		t.Fatalf("create checkpoint path: %v", err)
	}
	for _, name := range []string{"checkpoint.img", "pages.img", "pages_meta.img"} {
		if err := os.WriteFile(filepath.Join(checkpointPath, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("write checkpoint file %s: %v", name, err)
		}
	}
}

func buildServerCheckpointImageManifest(checkpointPath string) (serverCheckpointImageManifest, error) {
	manifest := serverCheckpointImageManifest{Version: 1}
	for _, name := range []string{"checkpoint.img", "pages.img", "pages_meta.img"} {
		path := filepath.Join(checkpointPath, name)
		data, err := os.ReadFile(path)
		if err != nil {
			return serverCheckpointImageManifest{}, err
		}
		sum := sha256.Sum256(data)
		manifest.Files = append(manifest.Files, serverCheckpointImageManifestFile{
			Path:   name,
			Size:   int64(len(data)),
			SHA256: fmt.Sprintf("%x", sum),
		})
	}
	return manifest, nil
}

func writeServerJSONFile(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func openServerOwnedStore(t *testing.T, ctx context.Context, dir string) (*store.Store, *store.OwnerLock) {
	t.Helper()
	st, err := store.Open(ctx, filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	owner, err := store.AcquireOwnerLock(filepath.Join(dir, "run"))
	if err != nil {
		t.Fatalf("acquire owner: %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	if err := st.WriteOwner(ctx, owner); err != nil {
		t.Fatalf("write owner: %v", err)
	}
	return st, owner
}

func createServerTestSession(t *testing.T, ctx context.Context, st *store.Store, dir, id, status string, now time.Time, expiresAt *time.Time) store.Session {
	t.Helper()
	session := store.Session{
		ID:        id,
		UserID:    labUserID,
		Status:    status,
		DriverID:  "claude_code",
		Mode:      store.ModeForDriver("claude_code"),
		CreatedAt: now,
		UpdatedAt: now,
		ExpiresAt: expiresAt,
	}
	if err := os.MkdirAll(filepath.Join(dir, "sessions", id), 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := st.CreateSession(ctx, session); err != nil {
		t.Fatalf("create session: %v", err)
	}
	return session
}

func createServerPlannedActiveGeneration(t *testing.T, ctx context.Context, st *store.Store, cfg config.Config, ownerUUID, dir, sessionID string, driver agents.ID) (store.Session, store.GenerationAllocation) {
	t.Helper()
	now := time.Now().UTC()
	driverID := string(driver)
	session := store.Session{
		ID:        sessionID,
		UserID:    labUserID,
		Status:    string(sessionstate.Created),
		DriverID:  driverID,
		Mode:      store.ModeForDriver(driverID),
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := os.MkdirAll(filepath.Join(dir, "sessions", sessionID), 0o755); err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	if err := st.CreateSession(ctx, session); err != nil {
		t.Fatalf("create planned active session: %v", err)
	}
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     store.GenerationLeaseOwner(ownerUUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    serverTestAllocatorConfig(cfg, driverID),
	})
	if err != nil {
		t.Fatalf("allocate planned active generation: %v", err)
	}
	artifacts := testGenerationArtifacts()
	recordServerRuntimeArtifacts(t, ctx, st, allocation.GenerationID, artifacts.ManifestDigest, artifacts.RunscVersion)
	if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark planned active generation live: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE sessions
SET status = ?,
    updated_at = ?
WHERE id = ?`, string(sessionstate.RunningActive), now.Add(2*time.Second).Format(time.RFC3339Nano), sessionID); err != nil {
		t.Fatalf("mark planned active session running: %v", err)
	}
	session.Status = string(sessionstate.RunningActive)
	session.ActiveGenerationID = allocation.GenerationID
	session.UpdatedAt = now.Add(2 * time.Second)
	return session, allocation
}

func serverRunscContainerID(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string) string {
	t.Helper()
	details, err := st.GetRuntimeGenerationDetails(ctx, sessionID, generationID)
	if err != nil {
		t.Fatalf("get generation details: %v", err)
	}
	if strings.TrimSpace(details.RunscContainerID) == "" {
		t.Fatalf("generation %s has no runsc container id", generationID)
	}
	return details.RunscContainerID
}

func enableSessionAutoCheckpoint(t *testing.T, ctx context.Context, st *store.Store, sessionID string) {
	t.Helper()
	if _, err := st.DBForTest().ExecContext(ctx, `UPDATE sessions SET auto_checkpoint_enabled = 1 WHERE id = ?`, sessionID); err != nil {
		t.Fatalf("enable auto checkpoint: %v", err)
	}
}

func mustRuntimeResourceHostID(t *testing.T) string {
	t.Helper()
	hostID, err := runtimeResourceHostID()
	if err != nil {
		t.Fatalf("runtime resource host id: %v", err)
	}
	return hostID
}

func mustRuntimeResourceNftTableName(t *testing.T, generationID string) string {
	t.Helper()
	tableName, err := runtimeResourceNftTableName(generationID)
	if err != nil {
		t.Fatalf("runtime resource nft table name: %v", err)
	}
	return tableName
}

func prepareServerIdleGeneration(t *testing.T, ctx context.Context, st *store.Store, cfg config.Config, ownerUUID, sessionID string) store.GenerationAllocation {
	t.Helper()
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, store.AllocateGenerationParams{
		SessionID: sessionID,
		Owner:     store.GenerationLeaseOwner(ownerUUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    serverTestAllocatorConfig(cfg, "claude_code"),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	artifacts := testGenerationArtifacts()
	if err := st.RecordGenerationRuntimeArtifactDigests(ctx, allocation.GenerationID, store.GenerationRuntimeArtifactDigests{
		ControlManifestDigest:          artifacts.ManifestDigest,
		ProjectedControlManifestDigest: artifacts.ProjectedManifestDigest,
		BundleDigest:                   artifacts.BundleDigest,
		RuntimeConfigDigest:            artifacts.RuntimeConfigDigest,
		SpecDigest:                     artifacts.SpecDigest,
		RunscVersion:                   artifacts.RunscVersion,
		RunscBinaryPath:                artifacts.RunscBinaryPath,
		RunscBinaryDigest:              artifacts.RunscBinaryDigest,
	}); err != nil {
		t.Fatalf("record runtime artifacts: %v", err)
	}
	storeServerGenerationPlanForArtifacts(t, ctx, st, sessionID, allocation.GenerationID, artifacts)
	if err := st.MarkGenerationResourcesLive(ctx, sessionID, allocation.GenerationID, allocation.Owner, now.Add(time.Second)); err != nil {
		t.Fatalf("mark generation live: %v", err)
	}
	createServerRuntimeResourceLive(t, ctx, st, sessionID, allocation, ownerUUID, mustRuntimeResourceHostID(t), now.Add(2*time.Second))
	if err := st.UpdateSessionStatusAndActivity(ctx, sessionID, string(sessionstate.RunningIdle), nil, now.Add(-time.Minute)); err != nil {
		t.Fatalf("mark session idle: %v", err)
	}
	return allocation
}

func markServerGenerationCheckpointed(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string, now time.Time) {
	t.Helper()
	ensureServerRuntimeResourceLiveForCheckpoint(t, ctx, st, sessionID, generationID, now.Add(-time.Millisecond))
	formattedNow := now.UTC().Format(time.RFC3339Nano)
	fence := serverCheckpointDriverStateFenceForTest(t, ctx, st, sessionID, generationID)
	checkpointPlanDigest := "sha256:plan"
	if plan, err := st.GetGenerationPlan(ctx, generationID); err == nil {
		checkpointPlanDigest = plan.PlanDigest
	} else if err != sql.ErrNoRows {
		t.Fatalf("get checkpoint generation plan: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'checkpointed',
    checkpoint_created_at = ?,
    checkpoint_network_profile_id = network_profile_id,
    checkpoint_agent_runtime_profile_id = agent_runtime_profile_id,
    checkpoint_runsc_version = runsc_version,
    checkpoint_runsc_platform = runsc_platform,
    checkpoint_runsc_binary_path = (
      SELECT runsc_binary_path
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ),
    checkpoint_runsc_binary_digest = (
      SELECT runsc_binary_digest
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ),
    checkpoint_bundle_digest = 'bundle_digest',
    checkpoint_runtime_config_digest = 'runtime_config_digest',
    checkpoint_control_manifest_digest = (
      SELECT control_manifest_digest
      FROM runtime_generation_resources
      WHERE runtime_generation_resources.generation_id = runtime_generations.generation_id
    ),
    checkpoint_driver_states_digest = ?,
    checkpoint_plan_digest = ?,
    checkpoint_image_manifest_digest = ?,
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_seen_at = ?
WHERE generation_id = ?
  AND session_id = ?`, formattedNow, fence, checkpointPlanDigest, checkpointImageManifestDigestForTest, formattedNow, generationID, sessionID); err != nil {
		t.Fatalf("set checkpointed generation: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE network_profiles
SET allocation_state = 'reserved_checkpointed'
WHERE generation_id = ?
  AND session_id = ?`, generationID, sessionID); err != nil {
		t.Fatalf("reserve checkpointed network: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_generation_resources
SET resource_state = 'reserved_checkpointed'
WHERE generation_id = ?`, generationID); err != nil {
		t.Fatalf("reserve checkpointed resources: %v", err)
	}
	if _, err := st.DBForTest().ExecContext(ctx, `
UPDATE runtime_resource_instances
SET state = 'checkpoint_reserved',
    lease_expires_at = NULL,
    idempotency_token = NULL,
    updated_at = ?
WHERE generation_id = ?
  AND state IN ('live', 'checkpoint_reserved')`, formattedNow, generationID); err != nil {
		t.Fatalf("reserve checkpointed runtime resource: %v", err)
	}
	if err := st.UpdateSessionStatus(ctx, sessionID, string(sessionstate.Checkpointed), nil); err != nil {
		t.Fatalf("set checkpointed session: %v", err)
	}
}

func ensureServerRuntimeResourceLiveForCheckpoint(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string, now time.Time) {
	t.Helper()
	if _, err := st.GetRuntimeResourceInstance(ctx, generationID); err == nil {
		return
	} else if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("get checkpoint runtime resource instance: %v", err)
	}
	allocation := serverGenerationAllocationForTest(t, ctx, st, sessionID, generationID)
	createServerRuntimeResourceLive(t, ctx, st, sessionID, allocation, "checkpoint-test-owner", mustRuntimeResourceHostID(t), now)
}

func serverGenerationAllocationForTest(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string) store.GenerationAllocation {
	t.Helper()
	allocation := store.GenerationAllocation{GenerationID: generationID}
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT network_profile_id, agent_runtime_profile_id, COALESCE(lease_owner, '')
FROM runtime_generations
WHERE session_id = ?
  AND generation_id = ?`, sessionID, generationID).Scan(
		&allocation.NetworkProfileID,
		&allocation.AgentRuntimeProfileID,
		&allocation.Owner,
	); err != nil {
		t.Fatalf("query generation allocation for checkpoint: %v", err)
	}
	if strings.TrimSpace(allocation.Owner) == "" {
		allocation.Owner = store.GenerationLeaseOwner("checkpoint-test-owner")
	}
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT driver_id, state_digest, state_version
FROM session_driver_states
WHERE session_id = ?`, sessionID).Scan(
		&allocation.DriverState.DriverID,
		&allocation.DriverState.StateDigest,
		&allocation.DriverState.StateVersion,
	); err != nil {
		t.Fatalf("query driver state for checkpoint: %v", err)
	}
	return allocation
}

func serverCheckpointDriverStateFenceForTest(t *testing.T, ctx context.Context, st *store.Store, sessionID, generationID string) string {
	t.Helper()
	var driverID, stateDigest string
	var stateVersion int
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT ds.driver_id, ds.state_digest, ds.state_version
FROM session_driver_states ds
JOIN runtime_generations g ON g.session_id = ds.session_id
JOIN agent_runtime_profiles a ON a.agent_runtime_profile_id = g.agent_runtime_profile_id
WHERE g.session_id = ?
  AND g.generation_id = ?
  AND ds.driver_id = a.driver_id`, sessionID, generationID).Scan(&driverID, &stateDigest, &stateVersion); err != nil {
		t.Fatalf("query driver state fence input: %v", err)
	}
	fence, err := store.CheckpointDriverStatesDigest(generationID, []store.DriverStateToken{{
		DriverID:     driverID,
		StateDigest:  stateDigest,
		StateVersion: stateVersion,
	}})
	if err != nil {
		t.Fatalf("compute driver state fence: %v", err)
	}
	return fence
}

func newServerTestWatcher(t *testing.T, sessionsRoot string, st *store.Store, hub *events.Hub) *artifacts.Watcher {
	t.Helper()
	return artifacts.New(store.DataVolumeProvisionerConfig{
		SessionsRoot:   sessionsRoot,
		AgentHomesRoot: filepath.Join(t.TempDir(), "agent-homes"),
		EvidenceRoot:   filepath.Join(t.TempDir(), "volume-evidence"),
		RuntimeIdentity: store.RuntimeIdentity{
			UID: serverTestSandboxUID(),
			GID: serverTestSandboxGID(),
		},
	}, st, hub, slog.Default())
}

func serverDataVolumeConfigForTest(cfg config.Config) (store.DataVolumeProvisionerConfig, error) {
	roots, err := config.ValidateIsolationRoots(cfg.IsolationRoots())
	if err != nil {
		return store.DataVolumeProvisionerConfig{}, err
	}
	identity := cfg.Harness.SandboxIdentity
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

func applyServerTestDeploymentConfig(cfg *config.Config) {
	enabled := true
	disableNonessentialTraffic := true
	cfg.Harness.DefaultAgent = cfg.DefaultAgent
	cfg.Harness.Agents = map[string]config.AgentConfig{
		"claude_code": {
			Enabled:                    &enabled,
			DriverID:                   "claude_code",
			ModelProfile:               "anthropic_default",
			RuntimeProvider:            "local_runsc",
			DisableNonessentialTraffic: &disableNonessentialTraffic,
		},
		"pi": {
			Enabled:                    &enabled,
			DriverID:                   "pi",
			ModelProfile:               "anthropic_default",
			RuntimeProvider:            "local_runsc",
			DisableNonessentialTraffic: &disableNonessentialTraffic,
		},
		"sh": {
			Enabled:         &enabled,
			DriverID:        "sh",
			RuntimeProvider: "local_runsc",
		},
	}
	cfg.Harness.ModelProfiles = map[string]config.ModelProfileConfig{
		"anthropic_default": {
			Enabled:  &enabled,
			Provider: "anthropic_messages",
			Model:    "sonnet",
			ProxyRef: config.DefaultModelProxyRef,
		},
	}
	cfg.Harness.RuntimeProviders = map[string]config.RuntimeProviderConfig{
		"local_runsc": {
			Enabled:    &enabled,
			ProviderID: "local_runsc",
			ProfileID:  "local_runsc_default",
		},
	}
}

func testServerConfig(dir string) config.Config {
	rootfs := filepath.Join(dir, "rootfs")
	mustWriteServerTestAgentImageManifest(rootfs, agents.ClaudeCode, agents.Pi, agents.Shell)
	cfg := config.Config{
		SessionsRoot:     filepath.Join(dir, "sessions"),
		AgentHomesRoot:   filepath.Join(dir, "agent-homes"),
		BundleRoot:       filepath.Join(dir, "bundle"),
		RootFSPath:       rootfs,
		DBPath:           filepath.Join(dir, "state", "orchestrator.db"),
		RepoRoot:         dir,
		SessionRetention: time.Hour,
		MaxSessions:      10,
		DefaultAgent:     "claude_code",
		ModelProxy: config.ModelProxyConfig{
			BindURL:        "http://0.0.0.0:8082",
			SandboxBaseURL: "http://harness-model-proxy.internal:8082",
			BindPort:       8082,
		},
		Harness: config.HarnessConfig{
			RunDir: filepath.Join(dir, "run"),
			ModelProxy: config.ModelProxyConfig{
				BindURL:        "http://0.0.0.0:8082",
				SandboxBaseURL: "http://harness-model-proxy.internal:8082",
				BindPort:       8082,
			},
			Network: config.NetworkConfig{
				CIDRPool: config.CIDRPrefix{Prefix: netip.MustParsePrefix("10.241.0.0/29")},
				Egress: config.EgressConfig{
					DorisFEHosts: []string{"172.16.0.138"},
					DorisBEHosts: []string{"172.16.0.139"},
					DorisPorts:   []int{9030},
					DNSPolicy:    config.DNSPolicyHostnamesOnly,
				},
			},
			Bridge: config.BridgeConfig{
				LeaseTTL:          config.Duration{Duration: time.Minute},
				HeartbeatInterval: config.Duration{Duration: 10 * time.Millisecond},
				PollInterval:      config.Duration{Duration: 10 * time.Millisecond},
				AckStartedGrace:   config.Duration{Duration: 90 * time.Second},
				ReconnectGrace:    config.Duration{Duration: 30 * time.Second},
			},
			Events: config.EventsConfig{
				RetentionWindow:        config.Duration{Duration: time.Hour},
				RetentionRows:          1_000,
				EmitOutputBatchMaxRows: 64,
				EmitOutputBatchMaxAge:  config.Duration{Duration: 100 * time.Millisecond},
			},
			Reaper: config.ReaperConfig{
				FailedRetention: config.Duration{Duration: 0},
			},
			SandboxIdentity: config.SandboxIdentity{
				UID: serverTestSandboxUID(),
				GID: serverTestSandboxGID(),
			},
			ProxyServiceIdentity: config.ProxyServiceIdentity{
				UID: os.Geteuid(),
				GID: os.Getegid(),
			},
		},
	}
	applyServerTestDeploymentConfig(&cfg)
	return cfg
}

func serverTestSandboxUID() int {
	uid := os.Getuid()
	if uid > 0 {
		return uid
	}
	return 65534
}

func serverTestSandboxGID() int {
	gid := os.Getgid()
	if gid > 0 {
		return gid
	}
	return 65534
}

func serverTestAllocatorConfig(cfg config.Config, driverID string) store.ResourceAllocatorConfig {
	if canonical, err := agents.CanonicalDriverID(driverID); err == nil {
		driverID = string(canonical)
	}
	outputFormat := ""
	modelAccess := false
	if spec, ok := agents.DriverSpecFor(driverID); ok {
		outputFormat = spec.OutputFormat
		modelAccess = spec.ModelAccess
	}
	model := ""
	disableNonessentialTraffic := false
	if _, agentCfg, ok := config.EnabledAgentConfigForDriver(cfg.DeploymentAgents(), driverID); ok {
		if agentCfg.DisableNonessentialTraffic != nil {
			disableNonessentialTraffic = *agentCfg.DisableNonessentialTraffic
		}
		if strings.TrimSpace(agentCfg.ModelProfile) != "" {
			if profile, ok := cfg.DeploymentModelProfiles()[agentCfg.ModelProfile]; ok && strings.TrimSpace(profile.Model) != "" {
				model = strings.TrimSpace(profile.Model)
			}
		}
	}
	return store.ResourceAllocatorConfig{
		RunDir:                      cfg.Harness.RunDir,
		CIDRPool:                    cfg.Harness.Network.CIDRPool.Prefix,
		EgressDorisFEHosts:          cfg.Harness.Network.Egress.DorisFEHosts,
		EgressDorisBEHosts:          cfg.Harness.Network.Egress.DorisBEHosts,
		EgressDorisPorts:            cfg.Harness.Network.Egress.DorisPorts,
		EgressDNSPolicy:             string(cfg.Harness.Network.Egress.DNSPolicy),
		HostProxyBindURL:            cfg.ModelProxy.BindURL,
		ProxyPort:                   cfg.ModelProxy.BindPort,
		DriverID:                    driverID,
		Model:                       model,
		OutputFormat:                outputFormat,
		DisableNonessentialTraffic:  disableNonessentialTraffic,
		SandboxUID:                  cfg.Harness.SandboxIdentity.UID,
		SandboxGID:                  cfg.Harness.SandboxIdentity.GID,
		SandboxSupplementalGIDs:     cfg.Harness.SandboxIdentity.SupplementalGIDs,
		ModelAccessAllowed:          &modelAccess,
		ProviderCredentialsHostOnly: modelAccess,
		SandboxModelProxyBaseURL:    cfg.ModelProxy.SandboxBaseURL,
	}
}

func createServerGenerationFilesystem(t *testing.T, details store.RuntimeGenerationDetails) {
	t.Helper()
	for _, path := range []string{details.CheckpointPath, details.ControlDirPath, details.BundleDirPath, details.BridgeDirPath, details.LogDirPath} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("create generation filesystem path %s: %v", path, err)
		}
		if err := os.WriteFile(filepath.Join(path, ".keep"), []byte("keep"), 0o644); err != nil {
			t.Fatalf("write generation filesystem marker %s: %v", path, err)
		}
	}
}

func waitForSessionStatus(t *testing.T, ctx context.Context, st *store.Store, sessionID, want string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		got, err := st.GetSession(ctx, sessionID)
		if err != nil {
			t.Fatalf("get session: %v", err)
		}
		if got.Status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, err := st.GetSession(ctx, sessionID)
	if err != nil {
		t.Fatalf("get final session: %v", err)
	}
	data, _ := json.Marshal(got)
	t.Fatalf("session did not reach %s: %s", want, data)
}

func waitForGenerationResourceStates(t *testing.T, ctx context.Context, st *store.Store, generationID, wantNetwork, wantResource string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var networkState, resourceState string
		if err := st.DBForTest().QueryRowContext(ctx, `
SELECT n.allocation_state, r.resource_state
FROM network_profiles n
JOIN runtime_generation_resources r ON r.generation_id = n.generation_id
WHERE n.generation_id = ?`, generationID).Scan(&networkState, &resourceState); err != nil {
			t.Fatalf("query generation resource states: %v", err)
		}
		if networkState == wantNetwork && resourceState == wantResource {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	var networkState, resourceState string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT n.allocation_state, r.resource_state
FROM network_profiles n
JOIN runtime_generation_resources r ON r.generation_id = n.generation_id
WHERE n.generation_id = ?`, generationID).Scan(&networkState, &resourceState); err != nil {
		t.Fatalf("query final generation resource states: %v", err)
	}
	t.Fatalf("generation %s resource states did not reach %s/%s: network=%s resource=%s", generationID, wantNetwork, wantResource, networkState, resourceState)
}

func waitForCheckpointRequests(t *testing.T, ctx context.Context, rt *recordingRuntime, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := len(rt.checkpointRequests()); got >= want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context canceled before checkpoint requests reached %d", want)
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatalf("checkpoint requests=%d want at least %d", len(rt.checkpointRequests()), want)
}

func waitForGenerationStatus(t *testing.T, ctx context.Context, st *store.Store, generationID, want string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var got string
		if err := st.DBForTest().QueryRowContext(ctx, `SELECT status FROM runtime_generations WHERE generation_id = ?`, generationID).Scan(&got); err != nil {
			t.Fatalf("query generation status: %v", err)
		}
		if got == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context canceled before generation reached %s", want)
		case <-time.After(10 * time.Millisecond):
		}
	}
	var got string
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT status FROM runtime_generations WHERE generation_id = ?`, generationID).Scan(&got); err != nil {
		t.Fatalf("query final generation status: %v", err)
	}
	t.Fatalf("generation did not reach %s: got %s", want, got)
}

func waitForGenerationLeaseAfter(t *testing.T, ctx context.Context, st *store.Store, generationID string, after time.Time) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var raw string
		if err := st.DBForTest().QueryRowContext(ctx, `SELECT lease_expires_at FROM runtime_generations WHERE generation_id = ?`, generationID).Scan(&raw); err != nil {
			t.Fatalf("query generation lease: %v", err)
		}
		if got, err := time.Parse(time.RFC3339Nano, raw); err == nil && got.After(after) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context canceled before generation lease renewed")
		case <-time.After(5 * time.Millisecond):
		}
	}
	var raw string
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT lease_expires_at FROM runtime_generations WHERE generation_id = ?`, generationID).Scan(&raw); err != nil {
		t.Fatalf("query final generation lease: %v", err)
	}
	t.Fatalf("generation %s lease was not renewed after %s: got %s", generationID, after, raw)
}

func waitForEventIDs(t *testing.T, ctx context.Context, st *store.Store, want []int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		records, err := st.ListEvents(ctx, store.ListEventsParams{})
		if err != nil {
			t.Fatalf("list events: %v", err)
		}
		got := make([]int64, 0, len(records))
		for _, record := range records {
			got = append(got, record.EventID)
		}
		if int64sEqual(got, want) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context canceled before retained events reached %v", want)
		case <-time.After(10 * time.Millisecond):
		}
	}
	records, err := st.ListEvents(context.Background(), store.ListEventsParams{})
	if err != nil {
		t.Fatalf("list final events: %v", err)
	}
	got := make([]int64, 0, len(records))
	for _, record := range records {
		got = append(got, record.EventID)
	}
	t.Fatalf("event ids=%v want %v", got, want)
}

func int64sEqual(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func serverSandboxSourceIPForGeneration(t *testing.T, ctx context.Context, st *store.Store, generationID string) string {
	t.Helper()
	var sandboxCIDR string
	if err := st.DBForTest().QueryRowContext(ctx, `
SELECT sandbox_ip_cidr
FROM network_profiles
WHERE generation_id = ?`, generationID).Scan(&sandboxCIDR); err != nil {
		t.Fatalf("query sandbox ip cidr: %v", err)
	}
	parts := strings.SplitN(sandboxCIDR, "/", 2)
	if len(parts) != 2 || parts[0] == "" {
		t.Fatalf("unexpected sandbox ip cidr: %q", sandboxCIDR)
	}
	return parts[0]
}

func waitForHubEvent(t *testing.T, ch <-chan events.Event, eventType string) events.Event {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case event := <-ch:
			if event.Type == eventType {
				return event
			}
		case <-deadline:
			t.Fatalf("timeout waiting for hub event %s", eventType)
		}
	}
}

func assertPublicSessionJSONOmitsHostFields(t *testing.T, payload []byte) {
	t.Helper()
	body := string(payload)
	for _, field := range []string{
		`"workspace"`,
		`"agent_home_path"`,
		`"agent":`,
		`"active_generation_id":`,
		`"restore_id"`,
		`"checkpoint_path"`,
		`"claude_session_uuid"`,
	} {
		if strings.Contains(body, field) {
			t.Fatalf("public session payload exposed host-only field %s: %s", field, body)
		}
	}
}

func assertContains(t *testing.T, value, want string) {
	t.Helper()
	if !strings.Contains(value, want) {
		t.Fatalf("expected %q to contain %q", value, want)
	}
}

func jsonArrayContainsAll(values []any, want ...string) bool {
	seen := map[string]struct{}{}
	for _, value := range values {
		if text, ok := value.(string); ok {
			seen[text] = struct{}{}
		}
	}
	for _, value := range want {
		if _, ok := seen[value]; !ok {
			return false
		}
	}
	return true
}

func drainHasEvent(ch <-chan events.Event, eventType string) bool {
	for {
		select {
		case event := <-ch:
			if event.Type == eventType {
				return true
			}
		default:
			return false
		}
	}
}
