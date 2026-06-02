package server

import (
	"net/http"

	"harness-platform/orchestrator/internal/agents"
)

func (s *Server) operatorAgentsCatalog(w http.ResponseWriter, r *http.Request) {
	type driverDTO struct {
		DriverID                    string         `json:"driver_id"`
		Label                       string         `json:"label"`
		Kind                        string         `json:"kind"`
		BridgeProtocol              string         `json:"bridge_protocol"`
		OutputSchema                string         `json:"output_schema"`
		RequiredRuntimeCapabilities []string       `json:"required_runtime_capabilities"`
		ModelAccess                 bool           `json:"model_access"`
		SupportsInterrupt           bool           `json:"supports_interrupt"`
		SupportsCompaction          bool           `json:"supports_compaction"`
		Capabilities                map[string]any `json:"capabilities"`
	}
	drivers := []driverDTO{}
	for _, spec := range agents.AllDriverSpecs() {
		drivers = append(drivers, driverDTO{
			DriverID:                    string(spec.ID),
			Label:                       spec.Label,
			Kind:                        string(spec.Kind),
			BridgeProtocol:              spec.BridgeProtocol,
			OutputSchema:                spec.OutputSchema,
			RequiredRuntimeCapabilities: append([]string(nil), spec.RequiredRuntimeCapabilities...),
			ModelAccess:                 spec.ModelAccess,
			SupportsInterrupt:           spec.SupportsInterrupt,
			SupportsCompaction:          spec.SupportsCompaction,
			Capabilities:                agents.DriverCapabilityPayload(spec),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"schema_version": 1,
		"drivers":        drivers,
	})
}
