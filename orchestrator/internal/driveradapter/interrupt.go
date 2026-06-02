package driveradapter

import (
	"encoding/json"
	"fmt"
	"strings"

	"harness-platform/orchestrator/internal/agents"
)

type InterruptPayloadRenderer func() ([]byte, error)

var interruptPayloadRenderers = map[agents.ID]InterruptPayloadRenderer{
	agents.Shell: renderShellInterruptPayload,
}

type shellInterruptFrame struct {
	Type string `json:"type"`
}

func InterruptPayloadFor(driver agents.ID) ([]byte, error) {
	driver = agents.ID(strings.TrimSpace(string(driver)))
	if err := InterruptSupportedFor(driver); err != nil {
		return nil, err
	}
	renderer := interruptPayloadRenderers[driver]
	return renderer()
}

func InterruptSupportedFor(driver agents.ID) error {
	driver = agents.ID(strings.TrimSpace(string(driver)))
	spec, ok := agents.DriverSpecFor(string(driver))
	if !ok {
		return fmt.Errorf("unsupported driver %q", driver)
	}
	if !agents.DriverSupportsFeature(spec, agents.FeatureInterrupt) ||
		!agents.DriverSupportsSubCapability(spec, agents.SubCapabilityInterruptAdapter) {
		return fmt.Errorf("interrupt not supported for driver %q", driver)
	}
	if _, ok := interruptPayloadRenderers[driver]; !ok {
		return fmt.Errorf("interrupt adapter is missing for driver %q", driver)
	}
	return nil
}

func renderShellInterruptPayload() ([]byte, error) {
	encoded, err := json.Marshal(shellInterruptFrame{Type: "interrupt"})
	if err != nil {
		return nil, fmt.Errorf("encode shell interrupt: %w", err)
	}
	return append(encoded, '\n'), nil
}
