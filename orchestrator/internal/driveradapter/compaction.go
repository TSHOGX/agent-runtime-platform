package driveradapter

import (
	"encoding/json"
	"fmt"
	"strings"

	"harness-platform/orchestrator/internal/agents"
)

type CompactionPayloadRenderer func() ([]byte, error)

var compactionPayloadRenderers = map[agents.ID]CompactionPayloadRenderer{
	agents.ClaudeCode: renderClaudeCodeCompactionPayload,
}

type claudeCodeCompactionContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type claudeCodeCompactionMessage struct {
	Role    string                        `json:"role"`
	Content []claudeCodeCompactionContent `json:"content"`
}

type claudeCodeCompactionFrame struct {
	Type    string                      `json:"type"`
	Message claudeCodeCompactionMessage `json:"message"`
}

func CompactionPayloadFor(driver agents.ID) ([]byte, error) {
	driver = agents.ID(strings.TrimSpace(string(driver)))
	if err := CompactionSupportedFor(driver); err != nil {
		return nil, err
	}
	renderer := compactionPayloadRenderers[driver]
	return renderer()
}

func CompactionSupportedFor(driver agents.ID) error {
	driver = agents.ID(strings.TrimSpace(string(driver)))
	spec, ok := agents.DriverSpecFor(string(driver))
	if !ok {
		return fmt.Errorf("unsupported driver %q", driver)
	}
	if !agents.DriverSupportsFeature(spec, agents.FeatureCompaction) ||
		!agents.DriverSupportsSubCapability(spec, agents.SubCapabilityCompactionAdapter) {
		return fmt.Errorf("compaction not supported for driver %q", driver)
	}
	if _, ok := compactionPayloadRenderers[driver]; !ok {
		return fmt.Errorf("compaction adapter is missing for driver %q", driver)
	}
	return nil
}

func ValidateRequiredFeatureAdapters(policy agents.FeaturePolicy, driver agents.DriverSpec) error {
	normalized, err := agents.NormalizeFeaturePolicy(policy)
	if err != nil {
		return err
	}
	for _, feature := range agents.AllFeatureIDs() {
		if normalized[feature] != agents.FeaturePolicyRequired {
			continue
		}
		switch feature {
		case agents.FeatureCompaction:
			if err := CompactionSupportedFor(driver.ID); err != nil {
				return fmt.Errorf("feature %s adapter: %w", feature, err)
			}
		case agents.FeatureInterrupt:
			if err := InterruptSupportedFor(driver.ID); err != nil {
				return fmt.Errorf("feature %s adapter: %w", feature, err)
			}
		default:
			return fmt.Errorf("feature %s adapter is not registered for driver %q", feature, driver.ID)
		}
	}
	return nil
}

func renderClaudeCodeCompactionPayload() ([]byte, error) {
	encoded, err := json.Marshal(claudeCodeCompactionFrame{
		Type: "user",
		Message: claudeCodeCompactionMessage{
			Role: "user",
			Content: []claudeCodeCompactionContent{
				{Type: "text", Text: "/compact"},
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("encode claude compaction: %w", err)
	}
	return append(encoded, '\n'), nil
}
