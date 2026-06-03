package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"harness-platform/orchestrator/internal/bridge"
)

type GenerationArtifacts struct {
	BundleDir                string
	SpecPath                 string
	ManifestPath             string
	ManifestDigest           string
	ProjectedManifestDigest  string
	BundleDigest             string
	RuntimeConfigDigest      string
	SpecDigest               string
	RunscVersion             string
	RunscBinaryPath          string
	RunscBinaryDigest        string
	NetworkPrepared          bool
	MaterializedDriverConfig []DriverConfigMaterialization
}

type DriverConfigMaterialization struct {
	Name                        string
	SourceProjectionPath        string
	HostSourcePath              string
	SourceDigest                string
	SandboxDestination          string
	DestinationMutableBySandbox bool
}

type GenerationArtifactProjection struct {
	Artifacts           GenerationArtifacts
	NetworkHosts        networkHostsProjection
	DriverConfig        driverConfigProjection
	RuntimeSpec         runtimeSpec
	ControlManifestFile controlManifestFile
}

func (r *Runtime) generationArtifacts(ctx context.Context, req StartRequest) (GenerationArtifacts, error) {
	if strings.TrimSpace(req.Generation.GenerationID) == "" {
		return GenerationArtifacts{}, fmt.Errorf("generation details are required")
	}
	artifacts := req.PreparedArtifacts
	if strings.TrimSpace(artifacts.BundleDir) != "" &&
		strings.TrimSpace(artifacts.SpecPath) != "" &&
		strings.TrimSpace(artifacts.ManifestPath) != "" &&
		strings.TrimSpace(artifacts.ManifestDigest) != "" {
		return artifacts, nil
	}
	return r.renderGenerationArtifacts(ctx, req)
}

func restoreGenerationArtifacts(req StartRequest) (GenerationArtifacts, error) {
	if strings.TrimSpace(req.Generation.GenerationID) == "" {
		return GenerationArtifacts{}, fmt.Errorf("generation details are required")
	}
	artifacts := req.PreparedArtifacts
	required := map[string]string{
		"bundle dir":                        artifacts.BundleDir,
		"spec path":                         artifacts.SpecPath,
		"control manifest path":             artifacts.ManifestPath,
		"control manifest digest":           artifacts.ManifestDigest,
		"projected control manifest digest": artifacts.ProjectedManifestDigest,
		"bundle digest":                     artifacts.BundleDigest,
		"runtime config digest":             artifacts.RuntimeConfigDigest,
		"spec digest":                       artifacts.SpecDigest,
		"runsc version":                     artifacts.RunscVersion,
		"runsc binary path":                 artifacts.RunscBinaryPath,
		"runsc binary digest":               artifacts.RunscBinaryDigest,
	}
	for label, value := range required {
		if strings.TrimSpace(value) == "" {
			return GenerationArtifacts{}, fmt.Errorf("restore requires stored generation artifact %s", label)
		}
	}
	checks := []struct {
		label string
		got   string
		want  string
	}{
		{"bundle dir", artifacts.BundleDir, req.Generation.BundleDirPath},
		{"spec path", artifacts.SpecPath, req.Generation.SpecPath},
		{"control manifest path", artifacts.ManifestPath, req.Generation.ControlManifestPath},
	}
	for _, check := range checks {
		if err := validateRuntimeArtifactPathEvidence("restore artifact", check.label, check.got); err != nil {
			return GenerationArtifacts{}, err
		}
		if err := validateRuntimeArtifactPathEvidence("restore generation", check.label, check.want); err != nil {
			return GenerationArtifacts{}, err
		}
		if check.got != check.want {
			return GenerationArtifacts{}, fmt.Errorf("restore artifact %s %q does not match generation path %q", check.label, check.got, check.want)
		}
	}
	return artifacts, nil
}

func (r *Runtime) renderGenerationArtifacts(ctx context.Context, req StartRequest) (GenerationArtifacts, error) {
	projection, err := r.RenderGenerationArtifacts(ctx, req)
	if err != nil {
		return GenerationArtifacts{}, err
	}
	if err := r.MaterializeGenerationArtifacts(req, projection); err != nil {
		return GenerationArtifacts{}, err
	}
	return projection.Artifacts, nil
}

func (r *Runtime) RenderGenerationArtifacts(ctx context.Context, req StartRequest) (GenerationArtifactProjection, error) {
	details := req.Generation
	if strings.TrimSpace(details.GenerationID) == "" {
		return GenerationArtifactProjection{}, fmt.Errorf("generation details are required")
	}
	if err := validateGenerationDetails(req); err != nil {
		return GenerationArtifactProjection{}, err
	}
	driverSpec, err := runtimeDriverSpec(req)
	if err != nil {
		return GenerationArtifactProjection{}, err
	}
	if strings.TrimSpace(details.SpecPath) == "" || strings.TrimSpace(details.ControlManifestPath) == "" {
		return GenerationArtifactProjection{}, fmt.Errorf("generation resource paths are required")
	}
	networkHosts, err := renderOptionalNetworkHostsProjection(details)
	if err != nil {
		return GenerationArtifactProjection{}, err
	}
	driverConfig, err := r.renderDriverConfigProjection(req)
	if err != nil {
		return GenerationArtifactProjection{}, err
	}
	spec, specDigest, err := r.renderRuntimeSpecWithDriverSpec(req, driverSpec)
	if err != nil {
		return GenerationArtifactProjection{}, err
	}
	currentRunsc, err := r.currentRunscPin(ctx)
	if err != nil {
		return GenerationArtifactProjection{}, err
	}
	runscNetwork, err := r.runscNetwork(details)
	if err != nil {
		return GenerationArtifactProjection{}, err
	}
	runscOverlay2, err := r.runscOverlay2(details)
	if err != nil {
		return GenerationArtifactProjection{}, err
	}
	bundleDigestPayload, err := canonicalJSON(map[string]any{
		"bundle_dir":  filepath.Clean(details.BundleDirPath),
		"rootfs":      spec.Root.Path,
		"spec_digest": specDigest,
	})
	if err != nil {
		return GenerationArtifactProjection{}, fmt.Errorf("bundle digest: %w", err)
	}
	bundleDigest := digestHex(bundleDigestPayload)
	runtimeConfigDigestPayload, err := canonicalJSON(map[string]any{
		"runsc_network":       runscNetwork,
		"runsc_overlay2":      runscOverlay2,
		"runsc_platform":      currentRunsc.Platform,
		"runsc_binary_path":   currentRunsc.BinaryPath,
		"runsc_binary_digest": currentRunsc.BinaryDigest,
		"rootfs":              spec.Root.Path,
	})
	if err != nil {
		return GenerationArtifactProjection{}, fmt.Errorf("runtime config digest: %w", err)
	}
	runtimeConfigDigest := digestHex(runtimeConfigDigestPayload)
	manifest, err := r.buildGenerationManifest(req, driverSpec, currentRunsc.Version, bundleDigest, runtimeConfigDigest, specDigest)
	if err != nil {
		return GenerationArtifactProjection{}, err
	}
	manifestDigest, manifestFile, err := wrapControlManifest(manifest)
	if err != nil {
		return GenerationArtifactProjection{}, err
	}
	projectedManifestDigest, err := projectedControlManifestDigest(manifest)
	if err != nil {
		return GenerationArtifactProjection{}, err
	}
	return GenerationArtifactProjection{
		Artifacts: GenerationArtifacts{
			BundleDir:                details.BundleDirPath,
			SpecPath:                 details.SpecPath,
			ManifestPath:             details.ControlManifestPath,
			ManifestDigest:           manifestDigest,
			ProjectedManifestDigest:  projectedManifestDigest,
			BundleDigest:             bundleDigest,
			RuntimeConfigDigest:      runtimeConfigDigest,
			SpecDigest:               specDigest,
			RunscVersion:             currentRunsc.Version,
			RunscBinaryPath:          currentRunsc.BinaryPath,
			RunscBinaryDigest:        currentRunsc.BinaryDigest,
			MaterializedDriverConfig: driverConfig.Entries,
		},
		NetworkHosts:        networkHosts,
		DriverConfig:        driverConfig,
		RuntimeSpec:         spec,
		ControlManifestFile: manifestFile,
	}, nil
}

func (r *Runtime) MaterializeGenerationArtifacts(req StartRequest, projection GenerationArtifactProjection) error {
	details := req.Generation
	if err := r.verifyMaterializationProjection(req, projection); err != nil {
		return err
	}
	if err := r.prepareGenerationDirs(req); err != nil {
		return err
	}
	if strings.TrimSpace(projection.NetworkHosts.Path) != "" {
		if err := writeFileAtomic(projection.NetworkHosts.Path, projection.NetworkHosts.Payload, 0o644); err != nil {
			return fmt.Errorf("write network hosts projection: %w", err)
		}
	}
	for _, entry := range projection.DriverConfig.Entries {
		payload, ok := projection.DriverConfig.Payloads[entry.Name]
		if !ok {
			return fmt.Errorf("write %s %s config: rendered payload is missing", driverID(req), entry.Name)
		}
		if err := writeFileAtomic(entry.HostSourcePath, payload, 0o644); err != nil {
			return fmt.Errorf("write %s %s config: %w", driverID(req), entry.Name, err)
		}
	}
	if err := writeJSONFileAtomic(details.SpecPath, projection.RuntimeSpec, 0o644); err != nil {
		return fmt.Errorf("write runtime spec: %w", err)
	}
	if err := writeJSONFileAtomic(details.ControlManifestPath, projection.ControlManifestFile, 0o644); err != nil {
		return fmt.Errorf("write control manifest: %w", err)
	}
	return nil
}

func (r *Runtime) verifyMaterializationProjection(req StartRequest, projection GenerationArtifactProjection) error {
	expected := req.PreparedArtifacts
	if !generationArtifactDigestEvidenceComplete(expected) {
		expected = projection.Artifacts
	}
	actual, err := r.materializationProjectionArtifacts(req, projection, expected)
	if err != nil {
		return err
	}
	checks := []struct {
		field string
		got   string
		want  string
		path  bool
	}{
		{"bundle dir", actual.BundleDir, expected.BundleDir, true},
		{"spec path", actual.SpecPath, expected.SpecPath, true},
		{"control manifest path", actual.ManifestPath, expected.ManifestPath, true},
		{"spec digest", actual.SpecDigest, expected.SpecDigest, false},
		{"control manifest digest", actual.ManifestDigest, expected.ManifestDigest, false},
		{"projected control manifest digest", actual.ProjectedManifestDigest, expected.ProjectedManifestDigest, false},
		{"bundle digest", actual.BundleDigest, expected.BundleDigest, false},
		{"runtime config digest", actual.RuntimeConfigDigest, expected.RuntimeConfigDigest, false},
		{"runsc version", actual.RunscVersion, expected.RunscVersion, false},
		{"runsc binary path", actual.RunscBinaryPath, expected.RunscBinaryPath, true},
		{"runsc binary digest", actual.RunscBinaryDigest, expected.RunscBinaryDigest, false},
	}
	for _, check := range checks {
		got, want := check.got, check.want
		if check.path {
			if err := validateRuntimeArtifactPathEvidence("materialization projection actual", check.field, got); err != nil {
				return err
			}
			if err := validateRuntimeArtifactPathEvidence("materialization projection expected", check.field, want); err != nil {
				return err
			}
		} else {
			got, want = strings.TrimSpace(got), strings.TrimSpace(want)
		}
		if strings.TrimSpace(want) == "" {
			return fmt.Errorf("materialization projection expected %s is required", check.field)
		}
		if got != want {
			return fmt.Errorf("materialization projection %s mismatch: got %q want %q", check.field, check.got, check.want)
		}
	}
	if !driverConfigMaterializationsEqual(actual.MaterializedDriverConfig, expected.MaterializedDriverConfig) {
		return fmt.Errorf("materialization projection driver config mismatch")
	}
	return nil
}

func validateRuntimeArtifactPathEvidence(scope, field, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s %s is required", scope, field)
	}
	if strings.TrimSpace(value) != value || !filepath.IsAbs(value) || filepath.Clean(value) != value {
		return fmt.Errorf("%s %s must be canonical absolute", scope, field)
	}
	return nil
}

func (r *Runtime) materializationProjectionArtifacts(req StartRequest, projection GenerationArtifactProjection, expected GenerationArtifacts) (GenerationArtifacts, error) {
	details := req.Generation
	specPayload, err := canonicalJSON(projection.RuntimeSpec)
	if err != nil {
		return GenerationArtifacts{}, fmt.Errorf("materialization projection spec digest: %w", err)
	}
	specDigest := digestHex(specPayload)
	manifestDigest, manifestFile, err := wrapControlManifest(projection.ControlManifestFile.Payload)
	if err != nil {
		return GenerationArtifacts{}, fmt.Errorf("materialization projection control manifest digest: %w", err)
	}
	if strings.TrimSpace(projection.ControlManifestFile.Digest) != "" && projection.ControlManifestFile.Digest != manifestFile.Digest {
		return GenerationArtifacts{}, fmt.Errorf("materialization projection control manifest embedded digest mismatch")
	}
	projectedManifestDigest, err := projectedControlManifestDigest(projection.ControlManifestFile.Payload)
	if err != nil {
		return GenerationArtifacts{}, fmt.Errorf("materialization projection projected control manifest digest: %w", err)
	}
	bundleDigestPayload, err := canonicalJSON(map[string]any{
		"bundle_dir":  filepath.Clean(details.BundleDirPath),
		"rootfs":      projection.RuntimeSpec.Root.Path,
		"spec_digest": specDigest,
	})
	if err != nil {
		return GenerationArtifacts{}, fmt.Errorf("materialization projection bundle digest: %w", err)
	}
	runscNetwork, err := r.runscNetwork(details)
	if err != nil {
		return GenerationArtifacts{}, err
	}
	runscOverlay2, err := r.runscOverlay2(details)
	if err != nil {
		return GenerationArtifacts{}, err
	}
	runscPlatform, err := requiredRunscPlatform(details)
	if err != nil {
		return GenerationArtifacts{}, err
	}
	runtimeConfigDigestPayload, err := canonicalJSON(map[string]any{
		"runsc_network":       runscNetwork,
		"runsc_overlay2":      runscOverlay2,
		"runsc_platform":      runscPlatform,
		"runsc_binary_path":   expected.RunscBinaryPath,
		"runsc_binary_digest": expected.RunscBinaryDigest,
		"rootfs":              projection.RuntimeSpec.Root.Path,
	})
	if err != nil {
		return GenerationArtifacts{}, fmt.Errorf("materialization projection runtime config digest: %w", err)
	}
	return GenerationArtifacts{
		BundleDir:                details.BundleDirPath,
		SpecPath:                 details.SpecPath,
		ManifestPath:             details.ControlManifestPath,
		ManifestDigest:           manifestDigest,
		ProjectedManifestDigest:  projectedManifestDigest,
		BundleDigest:             digestHex(bundleDigestPayload),
		RuntimeConfigDigest:      digestHex(runtimeConfigDigestPayload),
		SpecDigest:               specDigest,
		RunscVersion:             projection.ControlManifestFile.Payload.RunscVersion,
		RunscBinaryPath:          expected.RunscBinaryPath,
		RunscBinaryDigest:        expected.RunscBinaryDigest,
		MaterializedDriverConfig: projection.DriverConfig.Entries,
	}, nil
}

func generationArtifactDigestEvidenceComplete(artifacts GenerationArtifacts) bool {
	return strings.TrimSpace(artifacts.BundleDir) != "" &&
		strings.TrimSpace(artifacts.SpecPath) != "" &&
		strings.TrimSpace(artifacts.ManifestPath) != "" &&
		strings.TrimSpace(artifacts.ManifestDigest) != "" &&
		strings.TrimSpace(artifacts.ProjectedManifestDigest) != "" &&
		strings.TrimSpace(artifacts.BundleDigest) != "" &&
		strings.TrimSpace(artifacts.RuntimeConfigDigest) != "" &&
		strings.TrimSpace(artifacts.SpecDigest) != "" &&
		strings.TrimSpace(artifacts.RunscVersion) != "" &&
		strings.TrimSpace(artifacts.RunscBinaryPath) != "" &&
		strings.TrimSpace(artifacts.RunscBinaryDigest) != ""
}

func driverConfigMaterializationsEqual(left, right []DriverConfigMaterialization) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].Name != right[i].Name ||
			left[i].SourceProjectionPath != right[i].SourceProjectionPath ||
			left[i].HostSourcePath != right[i].HostSourcePath ||
			left[i].SourceDigest != right[i].SourceDigest ||
			left[i].SandboxDestination != right[i].SandboxDestination ||
			left[i].DestinationMutableBySandbox != right[i].DestinationMutableBySandbox {
			return false
		}
	}
	return true
}

func (r *Runtime) prepareGenerationDirs(req StartRequest) error {
	details := req.Generation
	for _, path := range []string{
		filepath.Dir(details.ControlManifestPath),
		details.BundleDirPath,
		filepath.Dir(details.SpecPath),
		details.LogDirPath,
	} {
		if strings.TrimSpace(path) == "" {
			continue
		}
		if err := os.MkdirAll(path, 0o755); err != nil {
			return fmt.Errorf("create generation dir %s: %w", path, err)
		}
	}
	if strings.TrimSpace(details.BridgeDirPath) != "" {
		if err := bridge.EnsureLayout(details.BridgeDirPath); err != nil {
			return fmt.Errorf("create generation bridge dir: %w", err)
		}
	}
	return r.prepareRuntimeDataDirs(req)
}
