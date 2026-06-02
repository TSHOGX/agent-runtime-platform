package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"harness-platform/orchestrator/internal/events"
	"harness-platform/orchestrator/internal/store"
)

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
