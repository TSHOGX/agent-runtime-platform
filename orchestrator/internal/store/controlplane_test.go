package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/sessionstate"

	_ "modernc.org/sqlite"
)

func TestFreshSchemaCreatesCurrentRuntimeTables(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "fresh.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	for _, table := range []string{
		"runtime_generations", "runtime_generation_resources", "turns", "events",
		"active_model_request_contexts", "network_profiles", "agent_runtime_profiles",
		"egress_policies", "orchestrator_owner", "sandbox_contracts",
		"sandbox_contract_artifacts", "sandbox_contract_input_evidence",
		"generation_plans", "generation_plan_projections", "content_snapshots",
		"session_workspaces", "session_driver_homes",
		"runtime_resource_instances", "session_driver_states",
	} {
		assertTableExists(t, st.db, table)
	}
	for _, column := range []string{"driver_id", "active_generation_id", "failure_reason", "error_class", "auto_checkpoint_enabled"} {
		assertColumnExists(t, st.db, "sessions", column)
	}
	for _, column := range []string{"auto_checkpoint_enabled"} {
		assertColumnExists(t, st.db, "runtime_generations", column)
	}
	for _, column := range []string{
		"driver_id", "model_access_allowed", "sandbox_uid", "sandbox_gid",
		"sandbox_supplemental_gids", "manifest_model_proxy_base_url",
		"model_proxy_api_key_secret_id", "model_proxy_auth_token_secret_id",
	} {
		assertColumnExists(t, st.db, "agent_runtime_profiles", column)
	}
	for _, column := range []string{"model_access_allowed"} {
		assertColumnExists(t, st.db, "active_model_request_contexts", column)
	}
	for _, column := range []string{"sandbox_contract_id", "sandbox_contract_version", "checkpoint_runsc_binary_path", "checkpoint_runsc_binary_digest", "checkpoint_image_manifest_digest"} {
		assertColumnExists(t, st.db, "runtime_generations", column)
	}
	for _, column := range []string{"projected_control_manifest_digest", "bundle_digest", "runtime_config_digest", "spec_digest"} {
		assertColumnExists(t, st.db, "runtime_generation_resources", column)
	}
	for _, column := range []string{"contract_id", "sandbox_contract_version", "runsc_container_id", "runsc_platform", "runsc_binary_path", "runsc_binary_digest", "sandbox_ip", "network_hosts_path", "resource_identity_payload", "resource_identity_digest"} {
		assertColumnExists(t, st.db, "runtime_generation_resources", column)
	}
	for _, column := range []string{"host_path", "layout_version", "sandbox_uid", "sandbox_gid", "sandbox_supplemental_gids", "runtime_identity_digest", "provisioning_marker_path", "provisioning_marker_digest"} {
		assertColumnExists(t, st.db, "session_workspaces", column)
	}
	for _, column := range removedSessionColumnsForTest() {
		assertColumnMissing(t, st.db, "sessions", column)
	}
	for _, column := range []string{"driver", "host_path", "layout_version", "sandbox_uid", "sandbox_gid", "sandbox_supplemental_gids", "runtime_identity_digest", "provisioning_marker_path", "provisioning_marker_digest"} {
		assertColumnExists(t, st.db, "session_driver_homes", column)
	}
	for _, column := range []string{"contract_id", "sandbox_contract_version", "state", "runsc_container_id", "runsc_binary_digest", "sandbox_ip", "resource_identity_payload", "resource_identity_digest", "evidence_json", "evidence_digest", "verified_at"} {
		assertColumnExists(t, st.db, "runtime_resource_instances", column)
	}
	for _, index := range []string{"events_proxy_started_request_uq", "events_proxy_finished_request_uq", "events_created_at_idx", "runtime_generations_sandbox_contract_id_uq", "runtime_generation_resources_contract_id_uq", "session_driver_homes_session_idx", "runtime_resource_instances_runsc_container_id_active_uq", "runtime_resource_instances_sandbox_ip_active_uq", "agent_runtime_profiles_tuple_uq"} {
		assertIndexExists(t, st.db, index)
	}
}

func TestFreshSchemaIsIdempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.db")
	st, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := st.migrate(ctx); err != nil {
		t.Fatalf("rerun migrate: %v", err)
	}
	_ = st.Close()

	st, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
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
		SandboxSourceIP: "10.240.0.2",
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

	var turnStatus, generationStatus, sessionStatus string
	if err := st.db.QueryRowContext(ctx, `
SELECT t.status, g.status, s.status
FROM turns t
JOIN runtime_generations g ON g.generation_id = t.generation_id
JOIN sessions s ON s.id = t.session_id
WHERE t.id = ?`, grant.TurnID).Scan(&turnStatus, &generationStatus, &sessionStatus); err != nil {
		t.Fatalf("query completion state: %v", err)
	}
	if turnStatus != "completed" || generationStatus != "active" || sessionStatus == string(sessionstate.RunningIdle) {
		t.Fatalf("unexpected statuses: turn=%s generation=%s session=%s", turnStatus, generationStatus, sessionStatus)
	}

	secondClaim := claim
	secondClaim.RequestID = "req-2"
	secondClaim.Now = now.Add(3 * time.Second)
	secondGrant, ok, err := st.ClaimNextTurn(ctx, secondClaim)
	if err != nil {
		t.Fatalf("claim second: %v", err)
	}
	if !ok || secondGrant.Sequence != 2 || secondGrant.Content != "second" || secondGrant.Replayed {
		t.Fatalf("unexpected second grant: ok=%v grant=%+v", ok, secondGrant)
	}
	if _, err := st.AckTurnStarted(ctx, AckStartedParams{
		SessionID:       "sess_turn",
		GenerationID:    "gen_turn",
		TurnID:          secondGrant.TurnID,
		Owner:           "owner",
		SandboxSourceIP: "10.240.0.2",
		LeaseTTL:        time.Minute,
		Now:             now.Add(4 * time.Second),
	}); err != nil {
		t.Fatalf("ack second started: %v", err)
	}
	if _, err := st.CompleteTurn(ctx, CompleteTurnParams{
		SessionID:      "sess_turn",
		GenerationID:   "gen_turn",
		TurnID:         secondGrant.TurnID,
		Owner:          "owner",
		TerminalStatus: "completed",
		Now:            now.Add(5 * time.Second),
	}); err != nil {
		t.Fatalf("complete second: %v", err)
	}

	var lastActivityAt string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, s.status, COALESCE(s.last_activity_at, '')
FROM runtime_generations g
JOIN sessions s ON s.active_generation_id = g.generation_id
WHERE g.generation_id = ?`, "gen_turn").Scan(&generationStatus, &sessionStatus, &lastActivityAt); err != nil {
		t.Fatalf("query final completion state: %v", err)
	}
	if generationStatus != "idle" || sessionStatus != string(sessionstate.RunningIdle) {
		t.Fatalf("unexpected final statuses: generation=%s session=%s", generationStatus, sessionStatus)
	}
	if lastActivityAt != formatTime(now.Add(5*time.Second)) {
		t.Fatalf("last_activity_at=%s want %s", lastActivityAt, formatTime(now.Add(5*time.Second)))
	}
	var contexts int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM active_model_request_contexts`).Scan(&contexts); err != nil {
		t.Fatalf("context count: %v", err)
	}
	if contexts != 0 {
		t.Fatalf("expected context cleanup, got %d", contexts)
	}
}

func TestClaimNextTurnRequiresLiveRuntimeResourceInstance(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_turn_resource")
	createActiveGeneration(t, ctx, st, "sess_turn_resource", "gen_turn_resource", "owner")
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_resource_instances
SET state = 'ready'
WHERE generation_id = 'gen_turn_resource'`); err != nil {
		t.Fatalf("downgrade runtime resource state: %v", err)
	}
	if _, err := st.EnqueueTurn(ctx, "sess_turn_resource", "blocked", time.Now().UTC()); err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
		SessionID:    "sess_turn_resource",
		GenerationID: "gen_turn_resource",
		Owner:        "owner",
		RequestID:    "req-resource-not-live",
		LeaseTTL:     time.Minute,
		Now:          time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("claim without live resource should return no work, got err=%v", err)
	}
	if ok {
		t.Fatalf("claim should require live runtime resource, got grant=%+v", grant)
	}
}

func TestAckTurnStartedRequiresLiveRuntimeResourceInstance(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_ack_resource")
	createActiveGeneration(t, ctx, st, "sess_ack_resource", "gen_ack_resource", "owner")
	turnID, err := st.EnqueueTurn(ctx, "sess_ack_resource", "blocked ack", time.Now().UTC())
	if err != nil {
		t.Fatalf("enqueue turn: %v", err)
	}
	now := time.Now().UTC()
	grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
		SessionID:    "sess_ack_resource",
		GenerationID: "gen_ack_resource",
		Owner:        "owner",
		RequestID:    "req-ack-resource",
		LeaseTTL:     time.Minute,
		Now:          now,
	})
	if err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim setup: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_resource_instances
SET state = 'ready'
WHERE generation_id = 'gen_ack_resource'`); err != nil {
		t.Fatalf("downgrade runtime resource state: %v", err)
	}
	if _, err := st.AckTurnStarted(ctx, AckStartedParams{
		SessionID:    "sess_ack_resource",
		GenerationID: "gen_ack_resource",
		TurnID:       turnID,
		Owner:        "owner",
		LeaseTTL:     time.Minute,
		Now:          now.Add(time.Second),
	}); err == nil || !strings.Contains(err.Error(), "generation ack_started CAS failed") {
		t.Fatalf("expected ack failure without live resource, got %v", err)
	}
	var turnStatus string
	var contexts int
	if err := st.db.QueryRowContext(ctx, `SELECT status FROM turns WHERE id = ?`, turnID).Scan(&turnStatus); err != nil {
		t.Fatalf("query turn: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM active_model_request_contexts`).Scan(&contexts); err != nil {
		t.Fatalf("query active contexts: %v", err)
	}
	if turnStatus != "leased" || contexts != 0 {
		t.Fatalf("ack should not commit turn/context without live resource: turn=%s contexts=%d", turnStatus, contexts)
	}
}

func TestTurnHelperTerminalFailureAndCancelKeepGenerationCacheConsistent(t *testing.T) {
	for _, terminalStatus := range []string{"failed", "canceled"} {
		t.Run(terminalStatus, func(t *testing.T) {
			ctx := context.Background()
			st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			t.Cleanup(func() { _ = st.Close() })

			sessionID := "sess_terminal_" + terminalStatus
			generationID := "gen_terminal_" + terminalStatus
			createStoreSession(t, ctx, st, sessionID)
			createActiveGeneration(t, ctx, st, sessionID, generationID, "owner")
			turnID, err := st.EnqueueTurn(ctx, sessionID, terminalStatus+" turn", time.Now().UTC())
			if err != nil {
				t.Fatalf("enqueue: %v", err)
			}

			now := time.Now().UTC()
			grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
				SessionID:    sessionID,
				GenerationID: generationID,
				Owner:        "owner",
				RequestID:    "req-" + terminalStatus,
				LeaseTTL:     time.Minute,
				Now:          now,
			})
			if err != nil || !ok || grant.TurnID != turnID {
				t.Fatalf("claim: ok=%v grant=%+v err=%v", ok, grant, err)
			}
			if _, err := st.AckTurnStarted(ctx, AckStartedParams{
				SessionID:       sessionID,
				GenerationID:    generationID,
				TurnID:          turnID,
				Owner:           "owner",
				SandboxSourceIP: "10.240.0.2",
				LeaseTTL:        time.Minute,
				Now:             now.Add(time.Second),
			}); err != nil {
				t.Fatalf("ack started: %v", err)
			}

			eventID, err := st.CompleteTurn(ctx, CompleteTurnParams{
				SessionID:      sessionID,
				GenerationID:   generationID,
				TurnID:         turnID,
				Owner:          "owner",
				TerminalStatus: terminalStatus,
				ErrorClass:     "test_" + terminalStatus,
				Error:          "terminal " + terminalStatus,
				EventType:      "ack_turn_completed",
				EventDedupeKey: "ack_completed:" + generationID,
				EventPayload: map[string]string{
					"status":      terminalStatus,
					"error_class": "test_" + terminalStatus,
					"error":       "terminal " + terminalStatus,
				},
				Now: now.Add(2 * time.Second),
			})
			if err != nil {
				t.Fatalf("complete %s: %v", terminalStatus, err)
			}
			if eventID == 0 {
				t.Fatalf("expected completion event id")
			}

			var turnStatus, turnErrorClass, generationStatus, sessionStatus, eventPayload string
			if err := st.db.QueryRowContext(ctx, `
SELECT t.status, COALESCE(t.error_class, ''), g.status, s.status
FROM turns t
JOIN runtime_generations g ON g.generation_id = t.generation_id
JOIN sessions s ON s.id = t.session_id
WHERE t.id = ?`, turnID).Scan(&turnStatus, &turnErrorClass, &generationStatus, &sessionStatus); err != nil {
				t.Fatalf("query terminal state: %v", err)
			}
			if turnStatus != terminalStatus ||
				turnErrorClass != "test_"+terminalStatus ||
				generationStatus != "idle" ||
				sessionStatus != string(sessionstate.RunningIdle) {
				t.Fatalf("unexpected terminal state: turn=%s error=%s generation=%s session=%s", turnStatus, turnErrorClass, generationStatus, sessionStatus)
			}
			if err := st.db.QueryRowContext(ctx, `SELECT payload FROM events WHERE event_id = ?`, eventID).Scan(&eventPayload); err != nil {
				t.Fatalf("query completion event payload: %v", err)
			}
			if !strings.Contains(eventPayload, `"status":"`+terminalStatus+`"`) ||
				!strings.Contains(eventPayload, `"session_marked_idle":true`) ||
				!strings.Contains(eventPayload, `"session_status":"running_idle"`) ||
				!strings.Contains(eventPayload, `"session_terminal":false`) {
				t.Fatalf("completion event payload missing session effect: %s", eventPayload)
			}
			var contexts int
			if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM active_model_request_contexts`).Scan(&contexts); err != nil {
				t.Fatalf("context count: %v", err)
			}
			if contexts != 0 {
				t.Fatalf("expected active proxy context cleanup, got %d", contexts)
			}
		})
	}
}

func TestClaimNextTurnConcurrentAttemptsOnlyOneWins(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_claim_race")
	createActiveGeneration(t, ctx, st, "sess_claim_race", "gen_claim_race", "owner")
	turnID, err := st.EnqueueTurn(ctx, "sess_claim_race", "race", time.Now().UTC())
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	requestIDs := []string{"req-a", "req-b", "req-c", "req-d", "req-e", "req-f", "req-g", "req-h"}
	stores := make([]*Store, len(requestIDs))
	for i := range stores {
		storeConn, err := Open(ctx, dbPath)
		if err != nil {
			t.Fatalf("open contender %d: %v", i, err)
		}
		stores[i] = storeConn
		t.Cleanup(func() { _ = storeConn.Close() })
	}

	type claimResult struct {
		requestID string
		grant     TurnGrant
		ok        bool
		err       error
	}
	results := make(chan claimResult, len(requestIDs))
	start := make(chan struct{})
	var wg sync.WaitGroup
	claimAt := time.Now().UTC()
	for i, requestID := range requestIDs {
		wg.Add(1)
		go func(storeConn *Store, requestID string) {
			defer wg.Done()
			<-start
			grant, ok, err := storeConn.ClaimNextTurn(ctx, ClaimNextTurnParams{
				SessionID:    "sess_claim_race",
				GenerationID: "gen_claim_race",
				Owner:        "owner",
				RequestID:    requestID,
				LeaseTTL:     time.Minute,
				Now:          claimAt,
			})
			results <- claimResult{requestID: requestID, grant: grant, ok: ok, err: err}
		}(stores[i], requestID)
	}
	close(start)
	wg.Wait()
	close(results)

	var winner *claimResult
	for result := range results {
		if result.err != nil {
			if !strings.Contains(result.err.Error(), "database is locked") && !strings.Contains(result.err.Error(), "SQLITE_BUSY") {
				t.Fatalf("unexpected claim error for %s: %v", result.requestID, result.err)
			}
			continue
		}
		if !result.ok {
			continue
		}
		if winner != nil {
			t.Fatalf("multiple concurrent claims won: first=%+v second=%+v", *winner, result)
		}
		resultCopy := result
		winner = &resultCopy
	}
	if winner == nil {
		t.Fatalf("no concurrent claim won")
	}
	if winner.grant.TurnID != turnID || winner.grant.Sequence != 1 || winner.grant.Content != "race" {
		t.Fatalf("unexpected winning grant: %+v turnID=%d", winner.grant, turnID)
	}

	var status, generationID, owner, claimRequestID string
	if err := st.db.QueryRowContext(ctx, `
SELECT status, generation_id, lease_owner, claim_request_id
FROM turns
WHERE id = ?`, turnID).Scan(&status, &generationID, &owner, &claimRequestID); err != nil {
		t.Fatalf("query raced turn: %v", err)
	}
	if status != "leased" || generationID != "gen_claim_race" || owner != "owner" || claimRequestID != winner.requestID {
		t.Fatalf("turn lease was stolen or not written atomically: status=%s generation=%s owner=%s request=%s winner=%s",
			status, generationID, owner, claimRequestID, winner.requestID)
	}
	for _, requestID := range requestIDs {
		if requestID == winner.requestID {
			continue
		}
		replay, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
			SessionID:    "sess_claim_race",
			GenerationID: "gen_claim_race",
			Owner:        "owner",
			RequestID:    requestID,
			LeaseTTL:     time.Minute,
			Now:          claimAt.Add(time.Second),
		})
		if err != nil {
			t.Fatalf("loser replay %s: %v", requestID, err)
		}
		if ok {
			t.Fatalf("loser request %s replayed or stole winner grant: %+v", requestID, replay)
		}
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

func TestTurnHelperRejectsStaleGenerationLifecycleWrites(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	createStoreSession(t, ctx, st, "sess_stale_writes")
	createActiveGeneration(t, ctx, st, "sess_stale_writes", "gen_old", "owner")
	turnID, err := st.EnqueueTurn(ctx, "sess_stale_writes", "old turn", time.Now().UTC())
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	claimAt := time.Now().UTC()
	if grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
		SessionID:    "sess_stale_writes",
		GenerationID: "gen_old",
		Owner:        "owner",
		RequestID:    "req_old",
		LeaseTTL:     time.Minute,
		Now:          claimAt,
	}); err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim setup: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'failed'
WHERE generation_id = 'gen_old'`); err != nil {
		t.Fatalf("fail old generation directly: %v", err)
	}
	if _, err := st.db.ExecContext(ctx, `
INSERT INTO runtime_generations (generation_id, session_id, status, lease_owner, lease_expires_at, last_seen_at)
VALUES ('gen_new', 'sess_stale_writes', 'idle', 'owner', ?, ?)`,
		formatTime(claimAt.Add(time.Minute)), formatTime(claimAt)); err != nil {
		t.Fatalf("insert replacement generation: %v", err)
	}
	if err := st.UpdateSessionActiveGeneration(ctx, SessionActiveGenerationCASParams{
		SessionID:            "sess_stale_writes",
		ExpectedGenerationID: sql.NullString{String: "gen_old", Valid: true},
		NextGenerationID:     "gen_new",
	}); err != nil {
		t.Fatalf("activate replacement generation: %v", err)
	}

	if _, err := st.AckTurnStarted(ctx, AckStartedParams{
		SessionID:       "sess_stale_writes",
		GenerationID:    "gen_old",
		TurnID:          turnID,
		Owner:           "owner",
		SandboxSourceIP: "10.240.0.2",
		LeaseTTL:        time.Minute,
		Now:             claimAt.Add(time.Second),
	}); err == nil || !strings.Contains(err.Error(), "generation ack_started CAS failed") {
		t.Fatalf("stale ack_started err=%v, want generation CAS failure", err)
	}
	seq := int64(1)
	if _, err := st.AppendEvent(ctx, AppendEventParams{
		SessionID:      "sess_stale_writes",
		GenerationID:   "gen_old",
		TurnID:         &turnID,
		Owner:          "owner",
		OutputSequence: &seq,
		Type:           "bridge.emit_output",
		Payload:        map[string]string{"line": "stale"},
		Now:            claimAt.Add(2 * time.Second),
	}); err == nil || !strings.Contains(err.Error(), "output event turn CAS failed") {
		t.Fatalf("stale output event err=%v, want output CAS failure", err)
	}
	if _, err := st.CompleteTurn(ctx, CompleteTurnParams{
		SessionID:      "sess_stale_writes",
		GenerationID:   "gen_old",
		TurnID:         turnID,
		Owner:          "owner",
		TerminalStatus: "completed",
		Now:            claimAt.Add(3 * time.Second),
	}); err == nil || !strings.Contains(err.Error(), "generation idle CAS failed") {
		t.Fatalf("stale completion err=%v, want generation CAS failure", err)
	}
	if err := st.FailGeneration(ctx, FailGenerationParams{
		SessionID:    "sess_stale_writes",
		GenerationID: "gen_old",
		TurnID:       turnID,
		Owner:        "owner",
		ErrorClass:   "stale_failure",
		Reason:       "stale",
		Now:          claimAt.Add(4 * time.Second),
	}); err == nil || !strings.Contains(err.Error(), "generation failure CAS failed") {
		t.Fatalf("stale generation failure err=%v, want generation CAS failure", err)
	}

	var status, activeGeneration string
	if err := st.db.QueryRowContext(ctx, `SELECT status FROM turns WHERE id = ?`, turnID).Scan(&status); err != nil {
		t.Fatalf("query turn status: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT active_generation_id FROM sessions WHERE id = 'sess_stale_writes'`).Scan(&activeGeneration); err != nil {
		t.Fatalf("query active generation: %v", err)
	}
	if status != "leased" || activeGeneration != "gen_new" {
		t.Fatalf("stale writes mutated state: turn=%s active_generation=%s", status, activeGeneration)
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
	turnID, err := st.EnqueueTurn(ctx, "sess_hb", "heartbeat turn", time.Now().UTC())
	if err != nil {
		t.Fatalf("enqueue heartbeat turn: %v", err)
	}

	now := time.Now().UTC()
	grant, ok, err := st.ClaimNextTurn(ctx, ClaimNextTurnParams{
		SessionID:    "sess_hb",
		GenerationID: "gen_hb",
		Owner:        "owner",
		RequestID:    "req_hb",
		LeaseTTL:     time.Minute,
		Now:          now.Add(-2 * time.Second),
	})
	if err != nil || !ok || grant.TurnID != turnID {
		t.Fatalf("claim heartbeat turn: ok=%v grant=%+v err=%v", ok, grant, err)
	}
	if _, err := st.AckTurnStarted(ctx, AckStartedParams{
		SessionID:       "sess_hb",
		GenerationID:    "gen_hb",
		TurnID:          turnID,
		Owner:           "owner",
		SandboxSourceIP: "10.240.0.2",
		LeaseTTL:        time.Minute,
		Now:             now.Add(-time.Second),
	}); err != nil {
		t.Fatalf("ack heartbeat turn started: %v", err)
	}

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
	wantExpires := now.Add(time.Minute)
	if got := parseTime(leaseExpires); !got.Equal(wantExpires) {
		t.Fatalf("generation lease_expires_at=%s want %s", got, wantExpires)
	}
	var turnExpires, contextSourceIP, contextExpires, contextOwner, resourceSourceIP string
	if err := st.db.QueryRowContext(ctx, `SELECT lease_expires_at FROM turns WHERE id = ?`, turnID).Scan(&turnExpires); err != nil {
		t.Fatalf("query turn lease expiry: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `
SELECT sandbox_source_ip, expires_at, lease_owner
FROM active_model_request_contexts
WHERE generation_id = 'gen_hb'`).Scan(&contextSourceIP, &contextExpires, &contextOwner); err != nil {
		t.Fatalf("query active proxy context expiry: %v", err)
	}
	if err := st.db.QueryRowContext(ctx, `
SELECT sandbox_ip
FROM runtime_resource_instances
WHERE generation_id = 'gen_hb'`).Scan(&resourceSourceIP); err != nil {
		t.Fatalf("query runtime resource source ip: %v", err)
	}
	if got := parseTime(turnExpires); !got.Equal(wantExpires) {
		t.Fatalf("turn lease_expires_at=%s want %s", got, wantExpires)
	}
	if got := parseTime(contextExpires); !got.Equal(wantExpires) || contextOwner != "owner" || contextSourceIP != resourceSourceIP {
		t.Fatalf("context source=%s expires_at=%s owner=%s want %s %s/owner", contextSourceIP, got, contextOwner, resourceSourceIP, wantExpires)
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

func TestRenewGenerationStartLeaseKeepsAllocatingAttemptAlive(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	createStoreSession(t, ctx, st, "sess_start_renew")
	now := time.Now().UTC()
	allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_start_renew",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now,
		Config:    testAllocatorConfig(t),
	})
	if err != nil {
		t.Fatalf("allocate generation: %v", err)
	}
	renewAt := now.Add(10 * time.Second)
	if err := st.RenewGenerationStartLease(ctx, RenewGenerationStartLeaseParams{
		SessionID:    "sess_start_renew",
		GenerationID: allocation.GenerationID,
		Owner:        allocation.Owner,
		LeaseTTL:     2 * time.Minute,
		Now:          renewAt,
	}); err != nil {
		t.Fatalf("renew start lease: %v", err)
	}
	var leaseExpires, lastSeen string
	if err := st.db.QueryRowContext(ctx, `
SELECT lease_expires_at, last_seen_at
FROM runtime_generations
WHERE generation_id = ?`, allocation.GenerationID).Scan(&leaseExpires, &lastSeen); err != nil {
		t.Fatalf("query generation lease: %v", err)
	}
	if got, want := parseTime(leaseExpires), renewAt.Add(2*time.Minute); !got.Equal(want) {
		t.Fatalf("lease_expires_at=%s want %s", got, want)
	}
	if got := parseTime(lastSeen); !got.Equal(renewAt) {
		t.Fatalf("last_seen_at=%s want %s", got, renewAt)
	}
}

func TestFailGenerationStartCanFinalizeExpiredOwnedAttempt(t *testing.T) {
	ctx := context.Background()
	st, owner := openOwnedStore(t, ctx)
	now := time.Now().UTC()

	createStoreSession(t, ctx, st, "sess_expired_start")
	startAllocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_expired_start",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(-2 * time.Minute),
		Config:    testAllocatorConfig(t),
	})
	if err != nil {
		t.Fatalf("allocate expired start generation: %v", err)
	}
	eventID, err := st.FailGenerationStart(ctx, FailGenerationStartParams{
		SessionID:      "sess_expired_start",
		GenerationID:   startAllocation.GenerationID,
		Owner:          startAllocation.Owner,
		SessionStatus:  string(sessionstate.Created),
		ErrorClass:     "probe_failure",
		Reason:         "late probe failure",
		EventType:      "generation.error",
		EventDedupeKey: "generation_error:" + startAllocation.GenerationID,
		Now:            now,
	})
	if err != nil {
		t.Fatalf("fail expired start generation: %v", err)
	}
	if eventID == 0 {
		t.Fatalf("expected durable generation error event")
	}
	var sessionStatus, generationStatus, generationOwner, networkState, resourceState string
	if err := st.db.QueryRowContext(ctx, `
SELECT s.status, g.status, COALESCE(g.lease_owner, ''), n.allocation_state, r.resource_state
FROM sessions s
JOIN runtime_generations g ON g.session_id = s.id
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE s.id = ?`, "sess_expired_start").Scan(&sessionStatus, &generationStatus, &generationOwner, &networkState, &resourceState); err != nil {
		t.Fatalf("query expired start failure state: %v", err)
	}
	if sessionStatus != string(sessionstate.Created) ||
		generationStatus != "failed" ||
		generationOwner != "" ||
		networkState != "reclaimable" ||
		resourceState != "reclaimable" {
		t.Fatalf("unexpected expired start failure state: session=%s generation=%s owner=%q network=%s resource=%s",
			sessionStatus, generationStatus, generationOwner, networkState, resourceState)
	}

	createStoreSession(t, ctx, st, "sess_expired_normal")
	normalAllocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
		SessionID: "sess_expired_normal",
		Owner:     GenerationLeaseOwner(owner.UUID),
		LeaseTTL:  time.Minute,
		Now:       now.Add(-2 * time.Minute),
		Config:    testAllocatorConfig(t),
	})
	if err != nil {
		t.Fatalf("allocate expired ordinary generation: %v", err)
	}
	if err := st.MarkGenerationResourcesLive(ctx, "sess_expired_normal", normalAllocation.GenerationID, normalAllocation.Owner, now); err == nil || !strings.Contains(err.Error(), "generation live CAS failed") {
		t.Fatalf("expected expired live CAS failure, got %v", err)
	}
	if err := st.FailGeneration(ctx, FailGenerationParams{
		SessionID:    "sess_expired_normal",
		GenerationID: normalAllocation.GenerationID,
		Owner:        normalAllocation.Owner,
		ErrorClass:   "ordinary_failure",
		Reason:       "expired",
		Now:          now,
	}); err == nil || !strings.Contains(err.Error(), "generation failure CAS failed") {
		t.Fatalf("expected expired ordinary failure CAS rejection, got %v", err)
	}
	var normalStatus, normalOwner, normalNetwork, normalResource string
	if err := st.db.QueryRowContext(ctx, `
SELECT g.status, COALESCE(g.lease_owner, ''), n.allocation_state, r.resource_state
FROM runtime_generations g
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE g.generation_id = ?`, normalAllocation.GenerationID).Scan(&normalStatus, &normalOwner, &normalNetwork, &normalResource); err != nil {
		t.Fatalf("query ordinary expired state: %v", err)
	}
	if normalStatus != "allocating" ||
		normalOwner != normalAllocation.Owner ||
		normalNetwork != "allocating" ||
		normalResource != "allocating" {
		t.Fatalf("ordinary expired helpers should not mutate state: generation=%s owner=%q network=%s resource=%s",
			normalStatus, normalOwner, normalNetwork, normalResource)
	}
}

func TestFailGenerationStartRejectsStaleOwnerActiveGenerationAndLifecycle(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	type generationSnapshot struct {
		sessionStatus      string
		activeGenerationID string
		generationStatus   string
		generationOwner    string
		networkState       string
		resourceState      string
		eventCount         int
	}
	readSnapshot := func(t *testing.T, st *Store, sessionID, generationID string) generationSnapshot {
		t.Helper()
		var snap generationSnapshot
		if err := st.db.QueryRowContext(ctx, `
SELECT s.status, COALESCE(s.active_generation_id, ''), g.status, COALESCE(g.lease_owner, ''),
       n.allocation_state, r.resource_state
FROM sessions s
JOIN runtime_generations g ON g.session_id = s.id
JOIN network_profiles n ON n.generation_id = g.generation_id
JOIN runtime_generation_resources r ON r.generation_id = g.generation_id
WHERE s.id = ?
  AND g.generation_id = ?`, sessionID, generationID).Scan(
			&snap.sessionStatus,
			&snap.activeGenerationID,
			&snap.generationStatus,
			&snap.generationOwner,
			&snap.networkState,
			&snap.resourceState,
		); err != nil {
			t.Fatalf("query generation snapshot: %v", err)
		}
		if err := st.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM events
WHERE session_id = ?
  AND generation_id = ?`, sessionID, generationID).Scan(&snap.eventCount); err != nil {
			t.Fatalf("count events: %v", err)
		}
		return snap
	}

	cases := []struct {
		name   string
		mutate func(t *testing.T, st *Store, sessionID string, allocation GenerationAllocation)
	}{
		{
			name: "owner changed",
			mutate: func(t *testing.T, st *Store, _ string, allocation GenerationAllocation) {
				t.Helper()
				if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET lease_owner = 'other-owner'
WHERE generation_id = ?`, allocation.GenerationID); err != nil {
					t.Fatalf("change owner: %v", err)
				}
			},
		},
		{
			name: "active generation changed",
			mutate: func(t *testing.T, st *Store, sessionID string, allocation GenerationAllocation) {
				t.Helper()
				replacementID := "gen_replacement_" + strings.TrimPrefix(sessionID, "sess_start_stale_")
				if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'failed'
WHERE generation_id = ?`, allocation.GenerationID); err != nil {
					t.Fatalf("mark old generation failed: %v", err)
				}
				if _, err := st.db.ExecContext(ctx, `
INSERT INTO runtime_generations (generation_id, session_id, status, lease_owner, lease_expires_at)
VALUES (?, ?, 'idle', ?, ?)`, replacementID, sessionID, allocation.Owner, formatTime(now.Add(time.Minute))); err != nil {
					t.Fatalf("insert replacement generation: %v", err)
				}
				if _, err := st.db.ExecContext(ctx, `
UPDATE sessions
SET active_generation_id = ?
WHERE id = ?`, replacementID, sessionID); err != nil {
					t.Fatalf("change active generation: %v", err)
				}
			},
		},
		{
			name: "lifecycle became active",
			mutate: func(t *testing.T, st *Store, _ string, allocation GenerationAllocation) {
				t.Helper()
				if _, err := st.db.ExecContext(ctx, `
UPDATE runtime_generations
SET status = 'active'
WHERE generation_id = ?`, allocation.GenerationID); err != nil {
					t.Fatalf("change lifecycle: %v", err)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, owner := openOwnedStore(t, ctx)
			sessionID := "sess_start_stale_" + strings.ReplaceAll(tc.name, " ", "_")
			createStoreSession(t, ctx, st, sessionID)
			allocation, err := st.AllocateGeneration(ctx, AllocateGenerationParams{
				SessionID: sessionID,
				Owner:     GenerationLeaseOwner(owner.UUID),
				LeaseTTL:  time.Minute,
				Now:       now.Add(-2 * time.Minute),
				Config:    testAllocatorConfig(t),
			})
			if err != nil {
				t.Fatalf("allocate generation: %v", err)
			}
			tc.mutate(t, st, sessionID, allocation)
			before := readSnapshot(t, st, sessionID, allocation.GenerationID)

			_, err = st.FailGenerationStart(ctx, FailGenerationStartParams{
				SessionID:      sessionID,
				GenerationID:   allocation.GenerationID,
				Owner:          allocation.Owner,
				SessionStatus:  string(sessionstate.Created),
				ErrorClass:     "late_start_failure",
				Reason:         "late start failure",
				EventType:      "generation.error",
				EventDedupeKey: "generation_error:" + allocation.GenerationID,
				Now:            now,
			})
			if err == nil || !strings.Contains(err.Error(), "generation start failure CAS failed") {
				t.Fatalf("expected stale start-failure CAS rejection, got %v", err)
			}
			after := readSnapshot(t, st, sessionID, allocation.GenerationID)
			if after != before {
				t.Fatalf("stale start failure mutated state: before=%+v after=%+v", before, after)
			}
		})
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
	if !tableHasColumn(t, db, table, column) {
		t.Fatalf("column %s.%s missing", table, column)
	}
}

func assertColumnMissing(t *testing.T, db *sql.DB, table, column string) {
	t.Helper()
	if tableHasColumn(t, db, table, column) {
		t.Fatalf("column %s.%s should be absent", table, column)
	}
}

func tableHasColumn(t *testing.T, db *sql.DB, table, column string) bool {
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
			return true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("table info rows: %v", err)
	}
	return false
}

func createStoreSession(t *testing.T, ctx context.Context, st *Store, id string) {
	t.Helper()
	createStoreSessionWithAgent(t, ctx, st, id, "claude_code")
}

func createStoreSessionWithAgent(t *testing.T, ctx context.Context, st *Store, id, agent string) {
	t.Helper()
	now := time.Now().UTC()
	if err := st.CreateSession(ctx, Session{
		ID:        id,
		UserID:    "lab",
		Status:    string(sessionstate.Created),
		DriverID:  agent,
		Mode:      ModeForDriver(agent),
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
	networkProfileID := "net_" + generationID
	agentRuntimeProfileID := "arp_" + generationID
	egressPolicyID := "egress_" + generationID
	ipOctet := testRuntimeResourceIPOctet(generationID)
	hostGatewayIP := fmt.Sprintf("10.241.%d.1", ipOctet)
	sandboxBaseURL := "http://" + hostGatewayIP + ":8082"
	sandboxIPCIDR := fmt.Sprintf("10.241.%d.2/30", ipOctet)
	hostSideCIDR := fmt.Sprintf("10.241.%d.0/30", ipOctet)
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin generation helper tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO egress_policies (
  egress_policy_id, policy_digest, allowed_egress_rules,
  doris_fe_hosts, doris_be_hosts, doris_ports, dns_policy, created_at
) VALUES (?, ?, '[]', '[]', '[]', '[]', 'off', ?)`, egressPolicyID, egressPolicyID, formatTime(now)); err != nil {
		t.Fatalf("insert egress policy: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO agent_runtime_profiles (
  agent_runtime_profile_id, driver_id, model, output_format,
  disable_nonessential_traffic, sandbox_uid, sandbox_gid,
  sandbox_supplemental_gids, requires_secret_drop, model_access_allowed,
  manifest_model_proxy_base_url, model_proxy_api_key_secret_id,
  model_proxy_auth_token_secret_id, secret_version, created_at
) VALUES (?, 'claude_code', NULL, 'stream-json', 1, 65534, 65534, '[]', 0, 1,
  'http://harness-model-proxy.internal:8082',
  'anthropic_proxy_api_key',
  'anthropic_proxy_auth_token',
  'test-secret-version',
  ?)`, agentRuntimeProfileID, formatTime(now)); err != nil {
		t.Fatalf("insert agent runtime profile: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO runtime_generations (
  generation_id, session_id, status, network_profile_id, agent_runtime_profile_id,
  sandbox_contract_version, lease_owner, lease_expires_at, last_seen_at
) VALUES (?, ?, 'idle', ?, ?, ?, ?, ?, ?)`,
		generationID, sessionID, networkProfileID, agentRuntimeProfileID, SandboxContractVersion, owner, formatTime(expires), formatTime(now)); err != nil {
		t.Fatalf("insert generation: %v", err)
	}
	payload, digest, err := canonicalBootstrapDriverState("claude_code", sessionID)
	if err != nil {
		t.Fatalf("build driver state: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO session_driver_states (
  session_id, driver_id, state_payload, state_digest, state_version,
  updated_generation_id, updated_turn_id, updated_at
) VALUES (?, 'claude_code', ?, ?, 1, ?, NULL, ?)
ON CONFLICT(session_id, driver_id) DO NOTHING`,
		sessionID, string(payload), digest, generationID, formatTime(now)); err != nil {
		t.Fatalf("insert driver state: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO network_profiles (
  network_profile_id, session_id, generation_id,
  host_proxy_bind_url, proxy_port, host_gateway_ip, sandbox_base_url, probe_url,
  netns_name, netns_path, host_veth, sandbox_veth, sandbox_ip_cidr,
  egress_policy_id, allowed_egress_rules, doris_fe_hosts, doris_be_hosts,
  doris_ports, dns_policy, host_side_cidr, allocation_state, created_at
) VALUES (?, ?, ?, '127.0.0.1:8082', 8082, ?, ?, ?,
  ?, ?, ?, ?, ?, ?, '[]', '[]', '[]', '[]', 'off', ?, 'live', ?)`,
		networkProfileID, sessionID, generationID,
		hostGatewayIP, sandboxBaseURL, sandboxBaseURL,
		"hns-"+generationID, "/run/netns/hns-"+generationID, "hv-"+generationID, "sv-"+generationID,
		sandboxIPCIDR, egressPolicyID, hostSideCIDR, formatTime(now)); err != nil {
		t.Fatalf("insert network profile: %v", err)
	}
	resourceBase := filepath.Join(t.TempDir(), generationID)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO runtime_generation_resources (
  generation_id, network_profile_id, agent_runtime_profile_id,
  control_dir_path, control_manifest_path, bundle_dir_path, spec_path,
  checkpoint_path, bridge_dir_path, log_dir_path,
  sandbox_contract_version, runsc_container_id, resource_state, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'live', ?)`,
		generationID, networkProfileID, agentRuntimeProfileID,
		filepath.Join(resourceBase, "control"),
		filepath.Join(resourceBase, "control", "session.json"),
		filepath.Join(resourceBase, "runtime"),
		filepath.Join(resourceBase, "runtime", "config.json"),
		filepath.Join(resourceBase, "checkpoint"),
		filepath.Join(resourceBase, "bridge"),
		filepath.Join(resourceBase, "logs"),
		SandboxContractVersion,
		"harness-gen-"+generationID,
		formatTime(now)); err != nil {
		t.Fatalf("insert generation resources: %v", err)
	}
	if err := updateSessionActiveGenerationTx(ctx, tx, SessionActiveGenerationCASParams{
		SessionID:        sessionID,
		NextGenerationID: generationID,
	}); err != nil {
		t.Fatalf("activate generation: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit generation helper tx: %v", err)
	}
	allocation := GenerationAllocation{
		GenerationID:          generationID,
		NetworkProfileID:      networkProfileID,
		AgentRuntimeProfileID: agentRuntimeProfileID,
		Owner:                 owner,
		LeaseExpiresAt:        now.Add(time.Minute),
	}
	createLiveRuntimeResourceInstanceForAllocation(t, ctx, st, sessionID, allocation, owner, "host-"+generationID, now)
}

func testRuntimeResourceIPOctet(value string) int {
	acc := 0
	for _, r := range value {
		acc = (acc*31 + int(r)) % 200
	}
	return acc + 1
}
