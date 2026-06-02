package server

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/store"
)

func TestValidateDriverStateForRuntimeLaunchPiHostState(t *testing.T) {
	agentHome := t.TempDir()
	volumes := sessionRuntimeDataVolumes{
		DriverHome: store.SessionDriverHomeVolume{HostPath: agentHome},
	}
	uninitialized := []byte(`{"driver_id":"pi","schema_version":1,"session_dir":"/agent-home/.pi/agent/sessions","state_kind":"pi_uninitialized"}`)
	if err := validateDriverStateForRuntimeLaunch(store.RuntimeGenerationDetails{
		DriverID:           "pi",
		DriverStatePayload: uninitialized,
	}, volumes); err != nil {
		t.Fatalf("pi uninitialized launch state rejected: %v", err)
	}
	if err := validateDriverStateForRuntimeLaunch(store.RuntimeGenerationDetails{DriverID: "pi"}, volumes); err == nil || !strings.Contains(err.Error(), "requires driver state payload") {
		t.Fatalf("expected missing pi driver state rejection, got %v", err)
	}

	sessionPayload := []byte(fmt.Sprintf(
		`{"driver_id":"pi","last_completed_turn_id":"9","schema_version":1,"selected_session_file":%q,"selected_session_id":"pi-session-1","selected_session_relpath":"session-1.jsonl","session_dir":%q,"state_kind":"pi_session"}`,
		agents.PiSessionDir+"/session-1.jsonl",
		agents.PiSessionDir,
	))
	if err := validateDriverStateForRuntimeLaunch(store.RuntimeGenerationDetails{
		DriverID:           "pi",
		DriverStatePayload: sessionPayload,
	}, volumes); err == nil || !strings.Contains(err.Error(), "host file missing") {
		t.Fatalf("expected missing pi session file rejection, got %v", err)
	}

	sessionRoot := filepath.Join(agentHome, ".pi", "agent", "sessions")
	if err := os.MkdirAll(sessionRoot, 0o755); err != nil {
		t.Fatalf("create pi session root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sessionRoot, "session-1.jsonl"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write pi session file: %v", err)
	}
	if err := validateDriverStateForRuntimeLaunch(store.RuntimeGenerationDetails{
		DriverID:           "pi",
		DriverStatePayload: sessionPayload,
	}, volumes); err != nil {
		t.Fatalf("pi session launch state rejected: %v", err)
	}
	if err := validateDriverStateForRuntimeLaunch(store.RuntimeGenerationDetails{DriverID: "sh"}, sessionRuntimeDataVolumes{}); err != nil {
		t.Fatalf("non-pi runtime launch should not require driver state: %v", err)
	}
}
