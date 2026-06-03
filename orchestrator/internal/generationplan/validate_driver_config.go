package generationplan

import (
	"fmt"

	"harness-platform/orchestrator/internal/agents"
)

func validateDriverConfigMaterializationEvidence(object map[string]any, artifacts map[string]any, driverID agents.ID) error {
	specs := agents.DriverConfigMaterializationSpecsFor(driverID)
	entries, ok := artifacts["materialized_driver_config"].([]any)
	if !ok {
		return fmt.Errorf("generation plan runtime_artifacts.materialized_driver_config must be an array")
	}
	if len(specs) == 0 {
		if len(entries) != 0 {
			return fmt.Errorf("generation plan driver %s does not support driver config materialization", driverID)
		}
		if mounts, ok := object["mounts"].(map[string]any); ok {
			if mountMaterializations, ok := mounts["driver_config_materializations"].(map[string]any); ok && len(mountMaterializations) != 0 {
				return fmt.Errorf("generation plan driver %s does not support driver config materialization", driverID)
			}
		}
		return nil
	}
	mounts, err := requireObject(object, "mounts")
	if err != nil {
		return err
	}
	mountMaterializations, ok := mounts["driver_config_materializations"].(map[string]any)
	if !ok {
		return fmt.Errorf("generation plan mounts.driver_config_materializations is required for driver %s", driverID)
	}
	expected := map[string]agents.DriverConfigMaterializationSpec{}
	for _, spec := range specs {
		expected[spec.Name] = spec
	}
	if len(entries) != len(expected) || len(mountMaterializations) != len(expected) {
		return fmt.Errorf("generation plan driver %s config materialization must contain exactly %d projections", driverID, len(expected))
	}
	seen := map[string]struct{}{}
	for _, value := range entries {
		entry, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("generation plan runtime_artifacts.materialized_driver_config entries must be objects")
		}
		name := stringField(entry, "name")
		want, ok := expected[name]
		if !ok {
			return fmt.Errorf("generation plan unsupported %s driver config materialization %q", driverID, name)
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("generation plan duplicate %s driver config materialization %q", driverID, name)
		}
		seen[name] = struct{}{}
		if err := validateDriverConfigRuntimeEntry(driverID, name, want, entry); err != nil {
			return err
		}
		mountEntry, ok := mountMaterializations[name].(map[string]any)
		if !ok {
			return fmt.Errorf("generation plan mounts.driver_config_materializations.%s is required", name)
		}
		if err := validateDriverConfigMountEntry(driverID, name, want, mountEntry); err != nil {
			return err
		}
	}
	if len(seen) != len(expected) {
		return fmt.Errorf("generation plan driver %s config materialization missing required projections", driverID)
	}
	return nil
}

func validateDriverConfigRuntimeEntry(driverID agents.ID, name string, want agents.DriverConfigMaterializationSpec, entry map[string]any) error {
	if stringField(entry, "projection_materialization_kind") != "driver_config" {
		return fmt.Errorf("generation plan %s runtime %s projection_materialization_kind must be driver_config", driverID, name)
	}
	if source := stringField(entry, "source_projection_path"); source != want.SourceProjectionPath {
		return fmt.Errorf("generation plan %s runtime %s source_projection_path = %q", driverID, name, source)
	}
	if digest := stringField(entry, "source_digest"); !isSha256(digest) {
		return fmt.Errorf("generation plan %s runtime %s source_digest is required", driverID, name)
	}
	if destination := stringField(entry, "sandbox_destination"); destination != want.SandboxDestination {
		return fmt.Errorf("generation plan %s runtime %s sandbox_destination = %q", driverID, name, destination)
	}
	if mutable := boolField(entry, "destination_mutable_by_sandbox"); mutable != want.DestinationMutableBySandbox {
		return fmt.Errorf("generation plan %s runtime %s destination mutability mismatch", driverID, name)
	}
	return nil
}

func validateDriverConfigMountEntry(driverID agents.ID, name string, want agents.DriverConfigMaterializationSpec, entry map[string]any) error {
	if typ := stringField(entry, "type"); typ != want.MountType {
		return fmt.Errorf("generation plan %s mount %s type = %q", driverID, name, typ)
	}
	if mode := stringField(entry, "mode"); mode != want.MountMode {
		return fmt.Errorf("generation plan %s mount %s mode = %q", driverID, name, mode)
	}
	if exact := boolField(entry, "exact"); exact != want.MountExact {
		return fmt.Errorf("generation plan %s mount %s exactness mismatch", driverID, name)
	}
	if source := stringField(entry, "source_projection_path"); source != want.SourceProjectionPath {
		return fmt.Errorf("generation plan %s mount %s source_projection_path = %q", driverID, name, source)
	}
	if destination := stringField(entry, "sandbox_destination"); destination != want.SandboxDestination {
		return fmt.Errorf("generation plan %s mount %s sandbox_destination = %q", driverID, name, destination)
	}
	if mutable := boolField(entry, "destination_mutable_by_sandbox"); mutable != want.DestinationMutableBySandbox {
		return fmt.Errorf("generation plan %s mount %s destination mutability mismatch", driverID, name)
	}
	return nil
}
