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
	spec, ok := agents.DriverSpecFor(string(driver))
	if !ok {
		return nil, fmt.Errorf("unsupported driver %q", driver)
	}
	if !agents.DriverSupportsFeature(spec, agents.FeatureInterrupt) ||
		!agents.DriverSupportsSubCapability(spec, agents.SubCapabilityInterruptAdapter) {
		return nil, fmt.Errorf("interrupt not supported for driver %q", driver)
	}
	renderer, ok := interruptPayloadRenderers[driver]
	if !ok {
		return nil, fmt.Errorf("interrupt adapter is missing for driver %q", driver)
	}
	return renderer()
}

func renderShellInterruptPayload() ([]byte, error) {
	encoded, err := json.Marshal(shellInterruptFrame{Type: "interrupt"})
	if err != nil {
		return nil, fmt.Errorf("encode shell interrupt: %w", err)
	}
	return append(encoded, '\n'), nil
}
