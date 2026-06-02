package driveradapter

import (
	"encoding/json"
	"strings"
	"testing"

	"harness-platform/orchestrator/internal/agents"
)

func TestCompactionPayloadForClaudeCode(t *testing.T) {
	if err := CompactionSupportedFor(agents.ClaudeCode); err != nil {
		t.Fatalf("claude compaction support: %v", err)
	}
	payload, err := CompactionPayloadFor(agents.ClaudeCode)
	if err != nil {
		t.Fatalf("claude compaction payload: %v", err)
	}
	if !strings.HasSuffix(string(payload), "\n") {
		t.Fatalf("claude compaction payload should be newline framed: %q", string(payload))
	}
	var frame struct {
		Type    string `json:"type"`
		Message struct {
			Role    string `json:"role"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(payload))), &frame); err != nil {
		t.Fatalf("invalid claude compaction payload %q: %v", string(payload), err)
	}
	if frame.Type != "user" ||
		frame.Message.Role != "user" ||
		len(frame.Message.Content) != 1 ||
		frame.Message.Content[0].Type != "text" ||
		frame.Message.Content[0].Text != "/compact" {
		t.Fatalf("claude compaction payload = %+v", frame)
	}
}

func TestCompactionSupportedForRequiresRegisteredAdapter(t *testing.T) {
	renderer := compactionPayloadRenderers[agents.ClaudeCode]
	delete(compactionPayloadRenderers, agents.ClaudeCode)
	t.Cleanup(func() { compactionPayloadRenderers[agents.ClaudeCode] = renderer })

	err := CompactionSupportedFor(agents.ClaudeCode)
	if err == nil || !strings.Contains(err.Error(), "compaction adapter is missing for driver") {
		t.Fatalf("expected missing compaction adapter error, got %v", err)
	}
}

func TestCompactionPayloadForUnsupportedDriver(t *testing.T) {
	if _, err := CompactionPayloadFor(agents.Pi); err == nil ||
		!strings.Contains(err.Error(), "compaction not supported for driver") {
		t.Fatalf("expected unsupported compaction error, got %v", err)
	}
}

func TestValidateRequiredFeatureAdapters(t *testing.T) {
	claude, ok := agents.DriverSpecFor("claude_code")
	if !ok {
		t.Fatalf("claude driver spec missing")
	}
	shell, ok := agents.DriverSpecFor("sh")
	if !ok {
		t.Fatalf("shell driver spec missing")
	}
	if err := ValidateRequiredFeatureAdapters(agents.FeaturePolicy{agents.FeatureCompaction: agents.FeaturePolicyRequired}, claude); err != nil {
		t.Fatalf("claude compaction adapter should validate: %v", err)
	}
	if err := ValidateRequiredFeatureAdapters(agents.FeaturePolicy{agents.FeatureInterrupt: agents.FeaturePolicyRequired}, shell); err != nil {
		t.Fatalf("shell interrupt adapter should validate: %v", err)
	}
	err := ValidateRequiredFeatureAdapters(agents.FeaturePolicy{agents.FeatureSkillsSnapshot: agents.FeaturePolicyRequired}, claude)
	if err == nil || !strings.Contains(err.Error(), "feature skills_snapshot adapter is not registered") {
		t.Fatalf("expected missing skills adapter error, got %v", err)
	}
}
