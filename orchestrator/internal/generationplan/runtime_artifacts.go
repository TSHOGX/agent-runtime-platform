package generationplan

import (
	"fmt"
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
	return runtime.GenerationArtifacts{
		BundleDir:               bundleDir,
		SpecPath:                specPath,
		ManifestPath:            manifestPath,
		ManifestDigest:          manifestDigest,
		ProjectedManifestDigest: projectedManifestDigest,
		BundleDigest:            bundleDigest,
		RuntimeConfigDigest:     runtimeConfigDigest,
		SpecDigest:              specDigest,
		RunscVersion:            runscVersion,
		RunscBinaryPath:         runscBinaryPath,
		RunscBinaryDigest:       runscBinaryDigest,
	}, nil
}
