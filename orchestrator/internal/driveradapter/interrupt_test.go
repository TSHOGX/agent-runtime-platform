package driveradapter

import (
	"encoding/json"
	"strings"
	"testing"

	"harness-platform/orchestrator/internal/agents"
)

func TestInterruptPayloadForShell(t *testing.T) {
	if err := InterruptSupportedFor(agents.Shell); err != nil {
		t.Fatalf("shell interrupt support: %v", err)
	}
	payload, err := InterruptPayloadFor(agents.Shell)
	if err != nil {
		t.Fatalf("shell interrupt payload: %v", err)
	}
	if !strings.HasSuffix(string(payload), "\n") {
		t.Fatalf("shell interrupt payload should be newline framed: %q", string(payload))
	}
	var frame struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(payload))), &frame); err != nil {
		t.Fatalf("invalid shell interrupt payload %q: %v", string(payload), err)
	}
	if frame.Type != "interrupt" {
		t.Fatalf("shell interrupt payload = %+v", frame)
	}
}

func TestInterruptSupportedForRequiresRegisteredAdapter(t *testing.T) {
	renderer := interruptPayloadRenderers[agents.Shell]
	delete(interruptPayloadRenderers, agents.Shell)
	t.Cleanup(func() { interruptPayloadRenderers[agents.Shell] = renderer })

	err := InterruptSupportedFor(agents.Shell)
	if err == nil || !strings.Contains(err.Error(), "interrupt adapter is missing for driver") {
		t.Fatalf("expected missing interrupt adapter error, got %v", err)
	}
}

func TestInterruptPayloadForUnsupportedDriver(t *testing.T) {
	if _, err := InterruptPayloadFor(agents.ClaudeCode); err == nil ||
		!strings.Contains(err.Error(), "interrupt not supported for driver") {
		t.Fatalf("expected unsupported interrupt error, got %v", err)
	}
}

func TestInterruptPayloadForUnknownDriver(t *testing.T) {
	if _, err := InterruptPayloadFor("unknown"); err == nil ||
		!strings.Contains(err.Error(), "unsupported driver") {
		t.Fatalf("expected unknown driver error, got %v", err)
	}
}
