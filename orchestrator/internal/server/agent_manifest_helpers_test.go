package server

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"harness-platform/orchestrator/internal/agents"
)

func writeServerTestAgentImageManifest(t *testing.T, rootfs string, drivers ...agents.ID) string {
	t.Helper()
	manifestPath, err := serverTestAgentImageManifest(rootfs, drivers...)
	if err != nil {
		t.Fatalf("write test agent image manifest: %v", err)
	}
	return manifestPath
}

func mustWriteServerTestAgentImageManifest(rootfs string, drivers ...agents.ID) string {
	manifestPath, err := serverTestAgentImageManifest(rootfs, drivers...)
	if err != nil {
		panic(err)
	}
	return manifestPath
}

func serverTestAgentImageManifest(rootfs string, drivers ...agents.ID) (string, error) {
	entries := make([]imageManifestDriver, 0, len(drivers))
	buildDrivers := make([]string, 0, len(drivers))
	for _, driverID := range drivers {
		spec, ok := agents.DriverSpecFor(string(driverID))
		if !ok {
			return "", fmt.Errorf("missing driver spec for %s", driverID)
		}
		binaryPath, err := expectedDriverBinaryPath(driverID)
		if err != nil {
			return "", fmt.Errorf("expected driver binary path: %w", err)
		}
		hostPath := filepath.Join(rootfs, strings.TrimPrefix(binaryPath, "/"))
		if err := os.MkdirAll(filepath.Dir(hostPath), 0o755); err != nil {
			return "", fmt.Errorf("mkdir driver binary parent: %w", err)
		}
		content := []byte("test binary for " + string(driverID) + "\n")
		if err := os.WriteFile(hostPath, content, 0o755); err != nil {
			return "", fmt.Errorf("write driver binary: %w", err)
		}
		sum := sha256.Sum256(content)
		entry, err := manifestDriverFromSpec(spec)
		if err != nil {
			return "", fmt.Errorf("manifest driver from spec: %w", err)
		}
		entry.InstalledBinaryDigest = fmt.Sprintf("sha256:%x", sum[:])
		entries = append(entries, entry)
		buildDrivers = append(buildDrivers, string(driverID))
	}
	manifest := map[string]any{
		"schema_version": 1,
		"build_input": map[string]any{
			"sandbox_agent_drivers": buildDrivers,
		},
		"drivers": entries,
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal manifest: %w", err)
	}
	data = append(data, '\n')
	manifestPath := filepath.Join(rootfs, "etc", "harness-image", "agents.json")
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o755); err != nil {
		return "", fmt.Errorf("mkdir manifest parent: %w", err)
	}
	if err := os.WriteFile(manifestPath, data, 0o644); err != nil {
		return "", fmt.Errorf("write manifest: %w", err)
	}
	return manifestPath, nil
}
