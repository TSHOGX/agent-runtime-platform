package generationplan

import "harness-platform/orchestrator/internal/runtime"

func MaterializedDriverConfigPayload(entries []runtime.DriverConfigMaterialization) []map[string]any {
	payload := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		payload = append(payload, map[string]any{
			"name":                            entry.Name,
			"source_projection_path":          entry.SourceProjectionPath,
			"source_digest":                   entry.SourceDigest,
			"sandbox_destination":             entry.SandboxDestination,
			"destination_mutable_by_sandbox":  entry.DestinationMutableBySandbox,
			"projection_materialization_kind": "driver_config",
		})
	}
	return payload
}
