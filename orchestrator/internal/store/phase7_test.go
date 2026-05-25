package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/sessionstate"

	_ "modernc.org/sqlite"
)

func TestPhase7MigrationsCreateSchemaAndBackfillLegacySessions(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "legacy.db")
	agentHomes := filepath.Join(dir, "agent-homes")

	createLegacyPhase6DB(t, dbPath, []legacySession{
		{id: "sess_created", status: string(sessionstate.Created)},
		{id: "sess_running", status: string(sessionstate.RunningActive)},
		{id: "sess_checkpointed", status: string(sessionstate.Checkpointed), checkpointPath: filepath.Join(dir, "cp")},
	})

	st, err := OpenWithOptions(ctx, dbPath, Options{AgentHomesRoot: agentHomes})
	if err != nil {
		t.Fatalf("open migrated store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	assertMigrationVersions(t, st.db, 9)
	for _, table := range []string{
		"runtime_generations", "runtime_generation_resources", "turns", "events",
		"active_model_request_contexts", "network_profiles", "agent_runtime_profiles",
		"egress_policies", "orchestrator_owner",
	} {
		assertTableExists(t, st.db, table)
	}
	for _, column := range []string{"active_generation_id", "agent_home_path", "failure_reason", "error_class", "auto_checkpoint_enabled"} {
		assertColumnExists(t, st.db, "sessions", column)
	}
	for _, column := range []string{"auto_checkpoint_enabled"} {
		assertColumnExists(t, st.db, "runtime_generations", column)
	}
	for _, column := range []string{"projected_control_manifest_digest", "bundle_digest", "runtime_config_digest", "spec_digest"} {
		assertColumnExists(t, st.db, "runtime_generation_resources", column)
	}
	for _, index := range []string{"events_proxy_started_request_uq", "events_proxy_finished_request_uq", "events_created_at_idx"} {
		assertIndexExists(t, st.db, index)
	}

	created, err := st.GetSession(ctx, "sess_created")
	if err != nil {
		t.Fatalf("get created: %v", err)
	}
	if created.Status != string(sessionstate.Created) || created.ActiveGenerationID != "" {
		t.Fatalf("created session should remain created with no generation: %+v", created)
	}
	if created.AgentHomePath != filepath.Join(agentHomes, "sess_created") {
		t.Fatalf("created agent home backfill: %q", created.AgentHomePath)
	}

	running, err := st.GetSession(ctx, "sess_running")
	if err != nil {
		t.Fatalf("get running: %v", err)
	}
	if running.Status != string(sessionstate.Failed) ||
		running.ErrorClass != "legacy_pre_phase7_no_generation" ||
		running.FailureReason != "legacy_pre_phase7_no_generation" ||
		running.EndedAt == nil {
		t.Fatalf("running legacy session not fenced as failed: %+v", running)
	}

	checkpointed, err := st.GetSession(ctx, "sess_checkpointed")
	if err != nil {
		t.Fatalf("get checkpointed: %v", err)
	}
	if checkpointed.Status != string(sessionstate.Failed) ||
		checkpointed.ErrorClass != "legacy_checkpoint_unrestorable" ||
		checkpointed.FailureReason != "legacy_checkpoint_unrestorable" ||
		checkpointed.CheckpointPath == "" {
		t.Fatalf("checkpointed legacy session not fenced as unrestorable: %+v", checkpointed)
	}

	var generations int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_generations`).Scan(&generations); err != nil {
		t.Fatalf("count runtime generations: %v", err)
	}
	if generations != 0 {
		t.Fatalf("legacy migration must not synthesize runtime generations, got %d", generations)
	}
}

func TestPhase7MigrationsAreIdempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.db")
	st, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := st.migrate(ctx); err != nil {
		t.Fatalf("rerun migrate: %v", err)
	}
	assertMigrationVersions(t, st.db, 9)
	_ = st.Close()

	st, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	assertMigrationVersions(t, st.db, 9)
}

func TestPhase7EventTimeMigrationNormalizesLegacyTimestamps(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dir, "legacy-events.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })

	st := &Store{db: db, options: Options{AgentHomesRoot: filepath.Join(dir, "agent-homes")}}
	migrations := defaultMigrations(st.options)
	if err := st.runMigrations(ctx, migrations[:7]); err != nil {
		t.Fatalf("run migrations through v7: %v", err)
	}
	legacyTime := "2026-05-25T10:00:00.1Z"
	if _, err := st.db.ExecContext(ctx, `
INSERT INTO events (type, payload, created_at)
VALUES ('legacy.event', '{}', ?)`, legacyTime); err != nil {
		t.Fatalf("insert legacy event: %v", err)
	}
	if err := st.runMigration(ctx, migrations[7]); err != nil {
		t.Fatalf("run v8 migration: %v", err)
	}

	var createdAt string
	if err := st.db.QueryRowContext(ctx, `SELECT created_at FROM events WHERE type = 'legacy.event'`).Scan(&createdAt); err != nil {
		t.Fatalf("query normalized event time: %v", err)
	}
	want := formatEventTime(parseTime(legacyTime))
	if createdAt != want {
		t.Fatalf("created_at=%q want %q", createdAt, want)
	}
	assertIndexExists(t, st.db, "events_created_at_idx")
	assertMigrationVersions(t, st.db, 8)
}

func TestPruneEventsAppliesRetentionWindowAndRows(t *testing.T) {
	ctx := context.Background()
	base := time.Date(2026, 5, 25, 10, 0, 0, 123456789, time.UTC)

	t.Run("rows", func(t *testing.T) {
		st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { _ = st.Close() })
		createStoreSession(t, ctx, st, "sess_a")
		createStoreSession(t, ctx, st, "sess_b")
		firstID := appendStoreTestEvent(t, ctx, st, "sess_a", "first", base)
		secondID := appendStoreTestEvent(t, ctx, st, "sess_b", "second", base.Add(time.Second))
		thirdID := appendStoreTestEvent(t, ctx, st, "sess_a", "third", base.Add(2*time.Second))
		fourthID := appendStoreTestEvent(t, ctx, st, "sess_b", "fourth", base.Add(3*time.Second))

		deleted, err := st.PruneEvents(ctx, PruneEventsParams{
			RetentionRows: 2,
			Now:           base.Add(4 * time.Second),
		})
		if err != nil {
			t.Fatalf("prune by rows: %v", err)
		}
		if deleted != 2 {
			t.Fatalf("deleted=%d want 2", deleted)
		}
		records, err := st.ListEvents(ctx, ListEventsParams{})
		if err != nil {
			t.Fatalf("list events: %v", err)
		}
		if got := eventIDs(records); len(got) != 2 || got[0] != thirdID || got[1] != fourthID {
			t.Fatalf("retained ids=%v want [%d %d] after deleting %d/%d", got, thirdID, fourthID, firstID, secondID)
		}
		oldest, ok, err := st.OldestEventID(ctx, "")
		if err != nil || !ok || oldest != thirdID {
			t.Fatalf("oldest global=%d ok=%v err=%v want %d", oldest, ok, err, thirdID)
		}
		oldest, ok, err = st.OldestEventID(ctx, "sess_b")
		if err != nil || !ok || oldest != fourthID {
			t.Fatalf("oldest sess_b=%d ok=%v err=%v want %d", oldest, ok, err, fourthID)
		}
	})

	t.Run("window", func(t *testing.T) {
		st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { _ = st.Close() })
		createStoreSession(t, ctx, st, "sess_a")
		createStoreSession(t, ctx, st, "sess_b")
		firstID := appendStoreTestEvent(t, ctx, st, "sess_a", "first", base)
		secondID := appendStoreTestEvent(t, ctx, st, "sess_b", "second", base.Add(time.Second))
		thirdID := appendStoreTestEvent(t, ctx, st, "sess_a", "third", base.Add(2*time.Second))
		fourthID := appendStoreTestEvent(t, ctx, st, "sess_b", "fourth", base.Add(3*time.Second))

		deleted, err := st.PruneEvents(ctx, PruneEventsParams{
			RetentionWindow: 2 * time.Second,
			Now:             base.Add(4 * time.Second),
		})
		if err != nil {
			t.Fatalf("prune by window: %v", err)
		}
		if deleted != 2 {
			t.Fatalf("deleted=%d want 2", deleted)
		}
		records, err := st.ListEvents(ctx, ListEventsParams{})
		if err != nil {
			t.Fatalf("list events: %v", err)
		}
		if got := eventIDs(records); len(got) != 2 || got[0] != thirdID || got[1] != fourthID {
			t.Fatalf("retained ids=%v want [%d %d] after deleting %d/%d", got, thirdID, fourthID, firstID, secondID)
		}
		oldest, ok, err := st.OldestEventID(ctx, "")
		if err != nil || !ok || oldest != thirdID {
			t.Fatalf("oldest global=%d ok=%v err=%v want %d", oldest, ok, err, thirdID)
		}
	})

	t.Run("invalid params", func(t *testing.T) {
		st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { _ = st.Close() })
		if _, err := st.PruneEvents(ctx, PruneEventsParams{RetentionWindow: -time.Second}); err == nil {
			t.Fatalf("negative retention window should fail")
		}
		if _, err := st.PruneEvents(ctx, PruneEventsParams{RetentionRows: -1}); err == nil {
			t.Fatalf("negative retention rows should fail")
		}
	})
}

func TestRuntimeGenerationPartialUniqueIndex(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_unique")

	now := formatTime(time.Now().UTC())
	if _, err := st.db.ExecContext(ctx, `
INSERT INTO runtime_generations (generation_id, session_id, status, lease_owner, lease_expires_at)
VALUES ('gen_a', 'sess_unique', 'idle', 'owner', ?)`, now); err != nil {
		t.Fatalf("insert gen a: %v", err)
	}
	_, err = st.db.ExecContext(ctx, `
INSERT INTO runtime_generations (generation_id, session_id, status, lease_owner, lease_expires_at)
VALUES ('gen_b', 'sess_unique', 'allocating', 'owner', ?)`, now)
	if err == nil || !strings.Contains(err.Error(), "constraint") {
		t.Fatalf("expected nonterminal uniqueness error, got %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE runtime_generations SET status = 'failed' WHERE generation_id = 'gen_a'`); err != nil {
		t.Fatalf("fail gen a: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
INSERT INTO runtime_generations (generation_id, session_id, status, lease_owner, lease_expires_at)
VALUES ('gen_b', 'sess_unique', 'allocating', 'owner', ?)`, now); err != nil {
		t.Fatalf("insert gen b after fail: %v", err)
	}
}

func TestOwnerLockContentionAndTamperDetection(t *testing.T) {
	ctx := context.Background()
	runDir := t.TempDir()
	owner, err := AcquireOwnerLock(runDir)
	if err != nil {
		t.Fatalf("acquire owner: %v", err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	if second, err := AcquireOwnerLock(runDir); err == nil {
		_ = second.Close()
		t.Fatalf("second owner lock should fail")
	}

	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.WriteOwner(ctx, owner); err != nil {
		t.Fatalf("write owner: %v", err)
	}
	if err := st.AssertOwner(ctx, owner.UUID); err != nil {
		t.Fatalf("assert owner: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE orchestrator_owner SET uuid = 'tampered' WHERE singleton = 1`); err != nil {
		t.Fatalf("tamper owner: %v", err)
	}
	if err := st.AssertOwner(ctx, owner.UUID); err == nil {
		t.Fatalf("tampered owner should fail assertion")
	}
}

func TestTurnHelperClaimAckComplete(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_turn")
	createActiveGeneration(t, ctx, st, "sess_turn", "gen_turn", "owner")
	if _, err := st.EnqueueTurn(ctx, "sess_turn", "first", time.Now().UTC()); err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	if _, err := st.EnqueueTurn(ctx, "sess_turn", "second", time.Now().UTC()); err != nil {
		t.Fatalf("enqueue second: %v", err)
	}

	now := time.Now().UTC()
	claim := ClaimNextTurnParams{
		SessionID:    "sess_turn",
		GenerationID: "gen_turn",
		Owner:        "owner",
		RequestID:    "req-1",
		LeaseTTL:     time.Minute,
		Now:          now,
	}
	grant, ok, err := st.ClaimNextTurn(ctx, claim)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if !ok || grant.Sequence != 1 || grant.Content != "first" || grant.Replayed {
		t.Fatalf("unexpected grant: ok=%v grant=%+v", ok, grant)
	}
	replay, ok, err := st.ClaimNextTurn(ctx, claim)
	if err != nil {
		t.Fatalf("replay claim: %v", err)
	}
	if !ok || !replay.Replayed || replay.TurnID != grant.TurnID {
		t.Fatalf("unexpected replay grant: ok=%v replay=%+v", ok, replay)
	}
	if _, err := st.AckTurnStarted(ctx, AckStartedParams{
		SessionID:       "sess_turn",
		GenerationID:    "gen_turn",
		TurnID:          grant.TurnID,
		Owner:           "owner",
		SandboxSourceIP: "10.0.0.2",
		LeaseTTL:        time.Minute,
		Now:             now.Add(time.Second),
	}); err != nil {
		t.Fatalf("ack started: %v", err)
	}
	if _, err := st.CompleteTurn(ctx, CompleteTurnParams{
		SessionID:      "sess_turn",
		GenerationID:   "gen_turn",
		TurnID:         grant.TurnID,
		Owner:          "owner",
		TerminalStatus: "completed",
		Now:            now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	var turnStatus, generationStatus string
	if err := st.db.QueryRowContext(ctx, `SELECT status FROM turns WHERE id = ?`, grant.TurnID).Scan(&turnStatus); err != nil {
		t.Fatalf("turn status: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT status FROM runtime_generations WHERE generation_id = 'gen_turn'`).Scan(&generationStatus); err != nil {
		t.Fatalf("generation status: %v", err)
	}
	if turnStatus != "completed" || generationStatus != "idle" {
		t.Fatalf("unexpected statuses: turn=%s generation=%s", turnStatus, generationStatus)
	}
	var contexts int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM active_model_request_contexts`).Scan(&contexts); err != nil {
		t.Fatalf("context count: %v", err)
	}
	if contexts != 0 {
		t.Fatalf("expected context cleanup, got %d", contexts)
	}
}

func TestResumeTurnRenewsOnlyActiveGenerationLease(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_resume")
	createActiveGeneration(t, ctx, st, "sess_resume", "gen_resume", "owner")
	turnID, err := st.EnqueueTurn(ctx, "sess_resume", "resume", time.Now().UTC())
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	now := time.Now().UTC()
	grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
		SessionID:    "sess_resume",
		GenerationID: "gen_resume",
		Owner:        "owner",
		RequestID:    "req-1",
		LeaseTTL:     time.Minute,
		Now:          now,
	})
	if err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim setup: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	resumed, ok, err := st.ResumeTurn(ctx, ResumeTurnParams{
		SessionID:    "sess_resume",
		GenerationID: "gen_resume",
		TurnID:       turnID,
		Owner:        "owner",
		LeaseTTL:     2 * time.Minute,
		Now:          now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !ok || resumed.TurnID != turnID || resumed.Content != "resume" {
		t.Fatalf("unexpected resume grant: ok=%v grant=%+v", ok, resumed)
	}
	if !resumed.ExpiresAt.Equal(now.Add(time.Second).Add(2 * time.Minute)) {
		t.Fatalf("resume expires_at=%s want %s", resumed.ExpiresAt, now.Add(time.Second).Add(2*time.Minute))
	}
	_, ok, err = st.ResumeTurn(ctx, ResumeTurnParams{
		SessionID:    "sess_resume",
		GenerationID: "gen_wrong",
		TurnID:       turnID,
		Owner:        "owner",
		LeaseTTL:     time.Minute,
		Now:          now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("resume wrong generation: %v", err)
	}
	if ok {
		t.Fatalf("resume with wrong generation must return no work")
	}
}

func TestResumeTurnRecoversAckStartedExpiredLeaseDuringGrace(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	createStoreSession(t, ctx, st, "sess_resume_ack")
	now := time.Now().UTC()
	allocation, turnID := createExpiredAckStartedTurn(t, ctx, st, owner.UUID, cfg, "sess_resume_ack", now, 30*time.Second)
	previousOwner := GenerationLeaseOwner("previous-owner")
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET lease_owner = ?
WHERE generation_id = ?`, previousOwner, allocation.GenerationID); err != nil {
		t.Fatalf("move generation to previous owner: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE turns
SET lease_owner = ?
WHERE id = ?`, previousOwner, turnID); err != nil {
		t.Fatalf("move turn to previous owner: %v", err)
	}

	resumed, ok, err := st.ResumeTurn(ctx, ResumeTurnParams{
		SessionID:       "sess_resume_ack",
		GenerationID:    allocation.GenerationID,
		TurnID:          turnID,
		Owner:           allocation.Owner,
		LeaseTTL:        2 * time.Minute,
		AckStartedGrace: time.Minute,
		Now:             now,
	})
	if err != nil {
		t.Fatalf("resume expired ack-started turn: %v", err)
	}
	if !ok || resumed.TurnID != turnID || resumed.Content != "maybe already ran" || resumed.Attempt != 0 {
		t.Fatalf("unexpected resumed grant: ok=%v grant=%+v", ok, resumed)
	}
	if !resumed.ExpiresAt.Equal(now.Add(2 * time.Minute)) {
		t.Fatalf("resume expires_at=%s want %s", resumed.ExpiresAt, now.Add(2*time.Minute))
	}

	var turnStatus, turnOwner, turnExpires, generationStatus, generationOwner, generationExpires string
	if err := st.db.QueryRowContext(ctx, `
SELECT t.status, t.lease_owner, t.lease_expires_at, g.status, g.lease_owner, g.lease_expires_at
FROM turns t
JOIN runtime_generations g ON g.generation_id = t.generation_id
WHERE t.id = ?`, turnID).Scan(&turnStatus, &turnOwner, &turnExpires, &generationStatus, &generationOwner, &generationExpires); err != nil {
		t.Fatalf("query recovered leases: %v", err)
	}
	if turnStatus != "running" || generationStatus != "active" || turnOwner != allocation.Owner || generationOwner != allocation.Owner {
		t.Fatalf("unexpected recovered state: turn=%s/%s generation=%s/%s want owner %s", turnStatus, turnOwner, generationStatus, generationOwner, allocation.Owner)
	}
	if !parseTime(turnExpires).Equal(now.Add(2*time.Minute)) || !parseTime(generationExpires).Equal(now.Add(2*time.Minute)) {
		t.Fatalf("leases not renewed: turn=%s generation=%s", turnExpires, generationExpires)
	}
}

func TestResumeTurnRejectsAckStartedExpiredLeaseAfterGrace(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	cfg := testAllocatorConfig(t)
	createStoreSession(t, ctx, st, "sess_resume_ack_expired")
	now := time.Now().UTC()
	allocation, turnID := createExpiredAckStartedTurn(t, ctx, st, owner.UUID, cfg, "sess_resume_ack_expired", now, 2*time.Minute)

	_, ok, err := st.ResumeTurn(ctx, ResumeTurnParams{
		SessionID:       "sess_resume_ack_expired",
		GenerationID:    allocation.GenerationID,
		TurnID:          turnID,
		Owner:           allocation.Owner,
		LeaseTTL:        time.Minute,
		AckStartedGrace: time.Minute,
		Now:             now,
	})
	if err != nil {
		t.Fatalf("resume expired ack-started turn after grace: %v", err)
	}
	if ok {
		t.Fatalf("resume after ack-started grace must return no work")
	}
}

func TestTurnHelperRejectsWrongSessionGenerationBinding(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_a")
	createStoreSession(t, ctx, st, "sess_b")
	createActiveGeneration(t, ctx, st, "sess_b", "gen_b", "owner")
	if _, err := st.EnqueueTurn(ctx, "sess_a", "work", time.Now().UTC()); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	_, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
		SessionID:    "sess_a",
		GenerationID: "gen_b",
		Owner:        "owner",
		RequestID:    "req",
		LeaseTTL:     time.Minute,
		Now:          time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("claim wrong binding: %v", err)
	}
	if ok {
		t.Fatalf("generation from another session must not claim turn")
	}
}

func TestGenerationHeartbeatAndFailureCAS(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_hb")
	createActiveGeneration(t, ctx, st, "sess_hb", "gen_hb", "owner")

	now := time.Now().UTC()
	if err := st.RenewGenerationHeartbeat(ctx, RenewHeartbeatParams{
		SessionID:    "sess_hb",
		GenerationID: "gen_hb",
		Owner:        "owner",
		LeaseTTL:     time.Minute,
		Now:          now,
	}); err != nil {
		t.Fatalf("renew heartbeat: %v", err)
	}
	var leaseExpires string
	if err := st.db.QueryRowContext(ctx, `SELECT lease_expires_at FROM runtime_generations WHERE generation_id = 'gen_hb'`).Scan(&leaseExpires); err != nil {
		t.Fatalf("query lease expiry: %v", err)
	}
	if leaseExpires == "" {
		t.Fatalf("expected renewed lease expiry")
	}

	if err := st.FailGeneration(ctx, FailGenerationParams{
		SessionID:    "sess_hb",
		GenerationID: "gen_hb",
		Owner:        "owner",
		ErrorClass:   "lifecycle_failure",
		Reason:       "boom",
		Now:          now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("fail generation: %v", err)
	}
	var status, failureReason string
	if err := st.db.QueryRowContext(ctx, `SELECT status, failure_reason FROM runtime_generations WHERE generation_id = 'gen_hb'`).Scan(&status, &failureReason); err != nil {
		t.Fatalf("query failed generation: %v", err)
	}
	if status != "failed" || failureReason != "boom" {
		t.Fatalf("unexpected failed generation state: %s %s", status, failureReason)
	}
}

func TestSessionActiveGenerationCAS(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_cas")
	now := time.Now().UTC()
	if _, err := st.db.ExecContext(ctx, `
INSERT INTO runtime_generations (generation_id, session_id, status, lease_owner, lease_expires_at)
VALUES ('gen_a', 'sess_cas', 'idle', 'owner', ?)`, formatTime(now.Add(time.Minute))); err != nil {
		t.Fatalf("insert gen_a: %v", err)
	}
	if err := st.UpdateSessionActiveGeneration(ctx, SessionActiveGenerationCASParams{
		SessionID:        "sess_cas",
		NextGenerationID: "gen_a",
	}); err != nil {
		t.Fatalf("initial update active generation: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations SET status = 'failed' WHERE generation_id = 'gen_a'`); err != nil {
		t.Fatalf("mark gen_a terminal: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
INSERT INTO runtime_generations (generation_id, session_id, status, lease_owner, lease_expires_at)
VALUES ('gen_b', 'sess_cas', 'idle', 'owner', ?)`, formatTime(now.Add(time.Minute))); err != nil {
		t.Fatalf("insert gen_b: %v", err)
	}
	if err := st.UpdateSessionActiveGeneration(ctx, SessionActiveGenerationCASParams{
		SessionID:            "sess_cas",
		ExpectedGenerationID: sql.NullString{String: "gen_a", Valid: true},
		NextGenerationID:     "gen_b",
	}); err != nil {
		t.Fatalf("update active generation with expected value: %v", err)
	}
	if err := st.UpdateSessionActiveGeneration(ctx, SessionActiveGenerationCASParams{
		SessionID:            "sess_cas",
		ExpectedGenerationID: sql.NullString{String: "gen_a", Valid: true},
		NextGenerationID:     "gen_c",
	}); err == nil {
		t.Fatalf("stale expected generation should fail")
	}
	var activeGeneration string
	if err := st.db.QueryRowContext(ctx, `SELECT active_generation_id FROM sessions WHERE id = 'sess_cas'`).Scan(&activeGeneration); err != nil {
		t.Fatalf("query active generation: %v", err)
	}
	if activeGeneration != "gen_b" {
		t.Fatalf("unexpected active generation: %s", activeGeneration)
	}
}

type legacySession struct {
	id             string
	status         string
	checkpointPath string
}

func createLegacyPhase6DB(t *testing.T, path string, sessions []legacySession) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open legacy db: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
CREATE TABLE users (
  id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE sessions (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  status TEXT NOT NULL,
  agent TEXT NOT NULL,
  workspace TEXT NOT NULL,
  restore_id TEXT NOT NULL,
  restore_ms INTEGER,
  claude_session_uuid TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  expires_at TEXT,
  ended_at TEXT,
  last_activity_at TEXT,
  checkpoint_path TEXT
);
CREATE TABLE messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  role TEXT NOT NULL,
  content TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE artifacts (
  session_id TEXT NOT NULL,
  path TEXT NOT NULL,
  size INTEGER NOT NULL,
  mod_time TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(session_id, path)
);`); err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}
	now := formatTime(time.Now().UTC())
	for _, session := range sessions {
		if _, err := db.ExecContext(ctx, `
INSERT INTO sessions (
  id, user_id, status, agent, workspace, restore_id, claude_session_uuid,
  created_at, updated_at, checkpoint_path
) VALUES (?, 'lab', ?, 'claude', ?, ?, ?, ?, ?, ?)`,
			session.id, session.status, filepath.Join(filepath.Dir(path), "sessions", session.id),
			"phase3-"+session.id, "11111111-2222-3333-4444-555555555555", now, now, nullableString(session.checkpointPath)); err != nil {
			t.Fatalf("insert legacy session %s: %v", session.id, err)
		}
	}
}

func assertMigrationVersions(t *testing.T, db *sql.DB, wantMax int) {
	t.Helper()
	var count, maxVersion int
	if err := db.QueryRow(`SELECT COUNT(*), COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&count, &maxVersion); err != nil {
		t.Fatalf("schema migrations: %v", err)
	}
	if count != wantMax || maxVersion != wantMax {
		t.Fatalf("schema migrations count/max = %d/%d, want %d/%d", count, maxVersion, wantMax, wantMax)
	}
}

func assertTableExists(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	var name string
	err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&name)
	if err != nil {
		t.Fatalf("table %s missing: %v", table, err)
	}
}

func assertIndexExists(t *testing.T, db *sql.DB, index string) {
	t.Helper()
	var name string
	err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?`, index).Scan(&name)
	if err != nil {
		t.Fatalf("index %s missing: %v", index, err)
	}
}

func assertColumnExists(t *testing.T, db *sql.DB, table, column string) {
	t.Helper()
	rows, err := db.Query(`PRAGMA table_info(` + quoteSQLiteIdent(table) + `)`)
	if err != nil {
		t.Fatalf("table info %s: %v", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			t.Fatalf("scan table info: %v", err)
		}
		if name == column {
			return
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("table info rows: %v", err)
	}
	t.Fatalf("column %s.%s missing", table, column)
}

func createStoreSession(t *testing.T, ctx context.Context, st *Store, id string) {
	t.Helper()
	now := time.Now().UTC()
	if err := st.CreateSession(ctx, Session{
		ID:        id,
		UserID:    "lab",
		Status:    string(sessionstate.Created),
		Agent:     "claude",
		Workspace: filepath.Join(t.TempDir(), id),
		RestoreID: "phase3-" + id,
		CreatedAt: now,
		UpdatedAt: now,
	}); err != nil {
		t.Fatalf("create session %s: %v", id, err)
	}
}

func appendStoreTestEvent(t *testing.T, ctx context.Context, st *Store, sessionID, name string, now time.Time) int64 {
	t.Helper()
	eventID, err := st.AppendEvent(ctx, AppendEventParams{
		SessionID: sessionID,
		Type:      "test.event",
		Payload:   map[string]string{"name": name},
		Now:       now,
	})
	if err != nil {
		t.Fatalf("append event %s: %v", name, err)
	}
	return eventID
}

func eventIDs(records []EventRecord) []int64 {
	ids := make([]int64, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.EventID)
	}
	return ids
}

func createActiveGeneration(t *testing.T, ctx context.Context, st *Store, sessionID, generationID, owner string) {
	t.Helper()
	now := time.Now().UTC()
	expires := now.Add(time.Minute)
	if _, err := st.db.ExecContext(ctx, `
INSERT INTO runtime_generations (generation_id, session_id, status, lease_owner, lease_expires_at, last_seen_at)
VALUES (?, ?, 'idle', ?, ?, ?)`, generationID, sessionID, owner, formatTime(expires), formatTime(now)); err != nil {
		t.Fatalf("insert generation: %v", err)
	}
	if err := st.UpdateSessionActiveGeneration(ctx, SessionActiveGenerationCASParams{
		SessionID:        sessionID,
		NextGenerationID: generationID,
	}); err != nil {
		t.Fatalf("activate generation: %v", err)
	}
}
