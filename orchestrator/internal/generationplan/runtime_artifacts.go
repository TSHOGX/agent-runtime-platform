package generationplan

import (
	"fmt"
	"path/filepath"
	"strings"

	"harness-platform/orchestrator/internal/runtime"
)

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

func RuntimeArtifacts(payload []byte) (runtime.GenerationArtifacts, error) {
	object, err := decodePlanObject(payload)
	if err != nil {
		return runtime.GenerationArtifacts{}, err
	}
	artifacts, err := requireObject(object, "runtime_artifacts")
	if err != nil {
		return runtime.GenerationArtifacts{}, err
	}
	runsc, err := requireObject(object, "runsc_pin")
	if err != nil {
		return runtime.GenerationArtifacts{}, err
	}
	value := func(section map[string]any, sectionName, key string) (string, error) {
		out := strings.TrimSpace(stringField(section, key))
		if out == "" {
			return "", fmt.Errorf("generation plan %s.%s is required", sectionName, key)
		}
		return out, nil
	}
	bundleDir, err := value(artifacts, "runtime_artifacts", "bundle_dir_path")
	if err != nil {
		return runtime.GenerationArtifacts{}, err
	}
	specPath, err := value(artifacts, "runtime_artifacts", "spec_path")
	if err != nil {
		return runtime.GenerationArtifacts{}, err
	}
	manifestPath, err := value(artifacts, "runtime_artifacts", "control_manifest_path")
	if err != nil {
		return runtime.GenerationArtifacts{}, err
	}
	manifestDigest, err := value(artifacts, "runtime_artifacts", "control_manifest_digest")
	if err != nil {
		return runtime.GenerationArtifacts{}, err
	}
	projectedManifestDigest, err := value(artifacts, "runtime_artifacts", "projected_control_manifest_digest")
	if err != nil {
		return runtime.GenerationArtifacts{}, err
	}
	bundleDigest, err := value(artifacts, "runtime_artifacts", "bundle_digest")
	if err != nil {
		return runtime.GenerationArtifacts{}, err
	}
	runtimeConfigDigest, err := value(artifacts, "runtime_artifacts", "runtime_config_digest")
	if err != nil {
		return runtime.GenerationArtifacts{}, err
	}
	specDigest, err := value(artifacts, "runtime_artifacts", "spec_digest")
	if err != nil {
		return runtime.GenerationArtifacts{}, err
	}
	runscVersion, err := value(runsc, "runsc_pin", "version")
	if err != nil {
		return runtime.GenerationArtifacts{}, err
	}
	runscBinaryPath, err := value(runsc, "runsc_pin", "binary_path")
	if err != nil {
		return runtime.GenerationArtifacts{}, err
	}
	runscBinaryDigest, err := value(runsc, "runsc_pin", "binary_digest")
	if err != nil {
		return runtime.GenerationArtifacts{}, err
	}
	materializedDriverConfig, err := materializedDriverConfigArtifacts(artifacts, strings.TrimSpace(stringField(artifacts, "control_dir_path")))
	if err != nil {
		return runtime.GenerationArtifacts{}, err
	}
	return runtime.GenerationArtifacts{
		BundleDir:                bundleDir,
		SpecPath:                 specPath,
		ManifestPath:             manifestPath,
		ManifestDigest:           manifestDigest,
		ProjectedManifestDigest:  projectedManifestDigest,
		BundleDigest:             bundleDigest,
		RuntimeConfigDigest:      runtimeConfigDigest,
		SpecDigest:               specDigest,
		RunscVersion:             runscVersion,
		RunscBinaryPath:          runscBinaryPath,
		RunscBinaryDigest:        runscBinaryDigest,
		MaterializedDriverConfig: materializedDriverConfig,
	}, nil
}

func materializedDriverConfigArtifacts(artifacts map[string]any, controlDir string) ([]runtime.DriverConfigMaterialization, error) {
	values, ok := artifacts["materialized_driver_config"].([]any)
	if !ok {
		return nil, fmt.Errorf("generation plan runtime_artifacts.materialized_driver_config must be an array")
	}
	out := make([]runtime.DriverConfigMaterialization, 0, len(values))
	for _, value := range values {
		entry, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("generation plan runtime_artifacts.materialized_driver_config entries must be objects")
		}
		sourceProjectionPath := stringField(entry, "source_projection_path")
		hostSourcePath, err := materializedDriverConfigHostSourcePath(controlDir, sourceProjectionPath)
		if err != nil {
			return nil, err
		}
		out = append(out, runtime.DriverConfigMaterialization{
			Name:                        stringField(entry, "name"),
			SourceProjectionPath:        sourceProjectionPath,
			HostSourcePath:              hostSourcePath,
			SourceDigest:                stringField(entry, "source_digest"),
			SandboxDestination:          stringField(entry, "sandbox_destination"),
			DestinationMutableBySandbox: boolField(entry, "destination_mutable_by_sandbox"),
		})
	}
	return out, nil
}

func materializedDriverConfigHostSourcePath(controlDir, sourceProjectionPath string) (string, error) {
	controlDir = strings.TrimSpace(controlDir)
	if controlDir == "" {
		return "", fmt.Errorf("generation plan runtime_artifacts.control_dir_path is required")
	}
	const controlPrefix = "/harness-control/"
	sourceProjectionPath = strings.TrimSpace(sourceProjectionPath)
	if !strings.HasPrefix(sourceProjectionPath, controlPrefix) {
		return "", fmt.Errorf("generation plan runtime_artifacts.materialized_driver_config source_projection_path must be under /harness-control")
	}
	relative := filepath.Clean(filepath.FromSlash(strings.TrimPrefix(sourceProjectionPath, controlPrefix)))
	if relative == "." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || relative == ".." {
		return "", fmt.Errorf("generation plan runtime_artifacts.materialized_driver_config source_projection_path escapes control dir")
	}
	return filepath.Join(controlDir, relative), nil
}
