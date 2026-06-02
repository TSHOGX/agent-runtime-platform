package runtime

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"

	"harness-platform/orchestrator/internal/store"
)

type runscPin struct {
	Platform     string
	Version      string
	BinaryPath   string
	BinaryDigest string
}

func (r *Runtime) runscVersion(ctx context.Context) (string, error) {
	out, err := r.runner.CombinedOutput(ctx, "runsc", "--version")
	if err != nil {
		return "", fmt.Errorf("lookup current runsc version: %w: %s", err, strings.TrimSpace(string(out)))
	}
	version := strings.TrimSpace(strings.Join(strings.Fields(string(out)), " "))
	if version == "" {
		return "", fmt.Errorf("lookup current runsc version: empty output")
	}
	return version, nil
}

func (r *Runtime) currentRunscPin(ctx context.Context) (runscPin, error) {
	path, digest, err := runscBinaryMetadata()
	if err != nil {
		return runscPin{}, err
	}
	version, err := r.runscVersion(ctx)
	if err != nil {
		return runscPin{}, err
	}
	return runscPin{
		Platform:     supportedRunscPlatform,
		Version:      version,
		BinaryPath:   path,
		BinaryDigest: digest,
	}, nil
}

func requiredRunscPlatform(details store.RuntimeGenerationDetails) (string, error) {
	platform := strings.TrimSpace(details.RunscPlatform)
	if platform == "" {
		return "", fmt.Errorf("runsc platform is required")
	}
	if platform != supportedRunscPlatform {
		return "", fmt.Errorf("unsupported runsc platform %q", platform)
	}
	return platform, nil
}

func runscPinFromArtifacts(details store.RuntimeGenerationDetails, artifacts GenerationArtifacts) runscPin {
	return runscPin{
		Platform:     strings.TrimSpace(details.RunscPlatform),
		Version:      artifacts.RunscVersion,
		BinaryPath:   artifacts.RunscBinaryPath,
		BinaryDigest: artifacts.RunscBinaryDigest,
	}
}

func runscPinFromDetails(details store.RuntimeGenerationDetails) runscPin {
	return runscPin{
		Platform:     strings.TrimSpace(details.RunscPlatform),
		Version:      details.RunscVersion,
		BinaryPath:   details.RunscBinaryPath,
		BinaryDigest: details.RunscBinaryDigest,
	}
}

func (r *Runtime) verifyLaunchRunscPin(ctx context.Context, operation string, details store.RuntimeGenerationDetails, artifacts GenerationArtifacts) (runscPin, error) {
	current, err := r.currentRunscPin(ctx)
	if err != nil {
		return runscPin{}, err
	}
	if err := verifyRequiredRunscPin(operation, "prepared artifacts", current, runscPinFromArtifacts(details, artifacts)); err != nil {
		return current, err
	}
	if err := verifyRequiredRunscPin(operation, "resource instance", current, runscPinFromDetails(details)); err != nil {
		return current, err
	}
	return current, nil
}

func (r *Runtime) verifyGenerationRunscPin(ctx context.Context, operation string, details store.RuntimeGenerationDetails) (runscPin, error) {
	current, err := r.currentRunscPin(ctx)
	if err != nil {
		return runscPin{}, err
	}
	if err := verifyRequiredRunscPin(operation, "resource instance", current, runscPinFromDetails(details)); err != nil {
		return current, err
	}
	return current, nil
}

func verifyRequiredRunscPin(operation, source string, current, pinned runscPin) error {
	checks := []struct {
		field   string
		current string
		pinned  string
	}{
		{"runsc_platform", current.Platform, pinned.Platform},
		{"runsc_version", current.Version, pinned.Version},
		{"runsc_binary_path", current.BinaryPath, pinned.BinaryPath},
		{"runsc_binary_digest", current.BinaryDigest, pinned.BinaryDigest},
	}
	for _, check := range checks {
		if strings.TrimSpace(check.pinned) == "" {
			return fmt.Errorf("runsc pin missing before %s: %s %s", operation, source, check.field)
		}
		if check.current != check.pinned {
			return fmt.Errorf("runsc pin mismatch before %s: %s %s current %q pinned %q", operation, source, check.field, check.current, check.pinned)
		}
	}
	return nil
}

func requireCompleteRunscPin(source string, pinned runscPin) error {
	checks := []struct {
		field string
		value string
	}{
		{"runsc_platform", pinned.Platform},
		{"runsc_version", pinned.Version},
		{"runsc_binary_path", pinned.BinaryPath},
		{"runsc_binary_digest", pinned.BinaryDigest},
	}
	for _, check := range checks {
		if strings.TrimSpace(check.value) == "" {
			return fmt.Errorf("runsc pin missing: %s %s", source, check.field)
		}
	}
	return nil
}

func runscBinaryMetadata() (string, string, error) {
	path, err := exec.LookPath("runsc")
	if err != nil {
		return "", "", fmt.Errorf("lookup current runsc binary: %w", err)
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", "", fmt.Errorf("resolve current runsc binary %q: %w", path, err)
	}
	digest, err := fileSHA256(canonical)
	if err != nil {
		return "", "", fmt.Errorf("hash current runsc binary %q: %w", canonical, err)
	}
	return canonical, "sha256:" + digest, nil
}

func requiredRunscBinary(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("runsc binary path is required")
	}
	return value, nil
}
