package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/config"
	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/sessionstate"
	"harness-platform/orchestrator/internal/store"
)

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
