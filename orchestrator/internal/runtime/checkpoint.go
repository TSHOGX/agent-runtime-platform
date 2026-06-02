package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"harness-platform/orchestrator/internal/store"
)

const checkpointImageManifestFileName = "harness-checkpoint-manifest.json"
const checkpointImageManifestVersion = 1

var requiredCheckpointImageFiles = []string{"checkpoint.img", "pages.img", "pages_meta.img"}

type CheckpointRequest struct {
	SessionID      string
	GenerationID   string
	CheckpointPath string
	Generation     store.RuntimeGenerationDetails
}

type CheckpointResult struct {
	ImageManifestDigest string
}

type checkpointImageManifest struct {
	Version int                           `json:"version"`
	Files   []checkpointImageManifestFile `json:"files"`
}

type checkpointImageManifestFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

func (r *Runtime) resolveCheckpointPath(req StartRequest) (string, error) {
	path := strings.TrimSpace(req.Generation.CheckpointPath)
	if path == "" {
		return "", errors.New("checkpoint path is required")
	}
	if req.Generation.CheckpointPath != path || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", fmt.Errorf("checkpoint path %q must be canonical absolute path", req.Generation.CheckpointPath)
	}
	if _, err := os.Stat(path); err == nil {
		return path, nil
	}
	return "", fmt.Errorf("checkpoint image not found: %s", path)
}

func validateCheckpointRestore(details store.RuntimeGenerationDetails, artifacts GenerationArtifacts, checkpointPath string) error {
	if err := validateCheckpointImageManifest(checkpointPath); err != nil {
		return err
	}
	imageManifestDigest, err := CheckpointImageManifestDigest(checkpointPath)
	if err != nil {
		return err
	}
	runscPlatform, err := requiredRunscPlatform(details)
	if err != nil {
		return err
	}
	checks := []struct {
		field string
		got   string
		want  string
	}{
		{"checkpoint_network_profile_id", details.NetworkProfileID, details.CheckpointNetworkProfileID},
		{"checkpoint_agent_runtime_profile_id", details.AgentRuntimeProfileID, details.CheckpointAgentRuntimeProfileID},
		{"checkpoint_runsc_platform", runscPlatform, details.CheckpointRunscPlatform},
		{"checkpoint_runsc_version", artifacts.RunscVersion, details.CheckpointRunscVersion},
		{"checkpoint_runsc_binary_path", artifacts.RunscBinaryPath, details.CheckpointRunscBinaryPath},
		{"checkpoint_runsc_binary_digest", artifacts.RunscBinaryDigest, details.CheckpointRunscBinaryDigest},
		{"checkpoint_bundle_digest", artifacts.BundleDigest, details.CheckpointBundleDigest},
		{"checkpoint_runtime_config_digest", artifacts.RuntimeConfigDigest, details.CheckpointRuntimeConfigDigest},
		{"checkpoint_control_manifest_digest", artifacts.ProjectedManifestDigest, details.CheckpointControlManifestDigest},
		{"checkpoint_image_manifest_digest", imageManifestDigest, details.CheckpointImageManifestDigest},
	}
	for _, check := range checks {
		if strings.TrimSpace(check.want) == "" {
			return fmt.Errorf("checkpoint metadata missing: %s", check.field)
		}
		if check.got != check.want {
			return fmt.Errorf("checkpoint metadata mismatch: %s got %q want %q", check.field, check.got, check.want)
		}
	}
	return nil
}

func writeCheckpointImageManifest(checkpointPath string) error {
	manifest, err := buildCheckpointImageManifest(checkpointPath)
	if err != nil {
		return err
	}
	path := filepath.Join(checkpointPath, checkpointImageManifestFileName)
	if err := writeJSONFileAtomic(path, manifest, 0o644); err != nil {
		return fmt.Errorf("write checkpoint image manifest: %w", err)
	}
	return nil
}

func buildCheckpointImageManifest(checkpointPath string) (checkpointImageManifest, error) {
	manifest := checkpointImageManifest{
		Version: checkpointImageManifestVersion,
		Files:   make([]checkpointImageManifestFile, 0, len(requiredCheckpointImageFiles)),
	}
	for _, name := range requiredCheckpointImageFiles {
		entry, err := checkpointImageManifestEntry(checkpointPath, name)
		if err != nil {
			return checkpointImageManifest{}, err
		}
		manifest.Files = append(manifest.Files, entry)
	}
	return manifest, nil
}

func checkpointImageManifestEntry(checkpointPath, name string) (checkpointImageManifestFile, error) {
	if strings.TrimSpace(name) == "" || filepath.IsAbs(name) || filepath.Clean(name) != name || strings.HasPrefix(name, "..") {
		return checkpointImageManifestFile{}, fmt.Errorf("checkpoint image manifest invalid path %q", name)
	}
	path := filepath.Join(checkpointPath, name)
	info, err := os.Stat(path)
	if err != nil {
		return checkpointImageManifestFile{}, fmt.Errorf("checkpoint image incomplete: %s: %w", path, err)
	}
	if info.IsDir() || info.Size() == 0 {
		return checkpointImageManifestFile{}, fmt.Errorf("checkpoint image incomplete: %s is not a non-empty file", path)
	}
	digest, err := fileSHA256(path)
	if err != nil {
		return checkpointImageManifestFile{}, fmt.Errorf("digest checkpoint image file %s: %w", path, err)
	}
	return checkpointImageManifestFile{
		Path:   name,
		Size:   info.Size(),
		SHA256: digest,
	}, nil
}

func validateCheckpointImageManifest(checkpointPath string) error {
	path := filepath.Join(checkpointPath, checkpointImageManifestFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("checkpoint image manifest missing: %s: %w", path, err)
	}
	var manifest checkpointImageManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return fmt.Errorf("checkpoint image manifest invalid: %w", err)
	}
	if manifest.Version != checkpointImageManifestVersion {
		return fmt.Errorf("checkpoint image manifest unsupported version: got %d want %d", manifest.Version, checkpointImageManifestVersion)
	}
	entries := map[string]checkpointImageManifestFile{}
	for _, entry := range manifest.Files {
		name := strings.TrimSpace(entry.Path)
		if name == "" || filepath.IsAbs(name) || filepath.Clean(name) != name || strings.HasPrefix(name, "..") {
			return fmt.Errorf("checkpoint image manifest invalid path %q", entry.Path)
		}
		if _, exists := entries[name]; exists {
			return fmt.Errorf("checkpoint image manifest duplicate path %q", name)
		}
		current, err := checkpointImageManifestEntry(checkpointPath, name)
		if err != nil {
			return err
		}
		if entry.Size != current.Size {
			return fmt.Errorf("checkpoint image manifest size mismatch for %s: got %d want %d", name, current.Size, entry.Size)
		}
		if !strings.EqualFold(entry.SHA256, current.SHA256) {
			return fmt.Errorf("checkpoint image manifest sha256 mismatch for %s", name)
		}
		entries[name] = entry
	}
	for _, name := range requiredCheckpointImageFiles {
		if _, ok := entries[name]; !ok {
			return fmt.Errorf("checkpoint image manifest missing required file %q", name)
		}
	}
	return nil
}

func CheckpointImageManifestDigest(checkpointPath string) (string, error) {
	digest, err := fileSHA256(filepath.Join(checkpointPath, checkpointImageManifestFileName))
	if err != nil {
		return "", fmt.Errorf("digest checkpoint image manifest: %w", err)
	}
	return "sha256:" + digest, nil
}

func (r *Runtime) Checkpoint(ctx context.Context, req CheckpointRequest) (CheckpointResult, error) {
	if strings.TrimSpace(req.SessionID) == "" {
		return CheckpointResult{}, errors.New("session id is required")
	}
	if strings.TrimSpace(req.GenerationID) == "" {
		return CheckpointResult{}, errors.New("generation id is required")
	}
	r.mu.RLock()
	container, exists := r.containers[req.SessionID]
	r.mu.RUnlock()

	if !exists {
		return CheckpointResult{}, errors.New("container not found")
	}
	if container.GenerationID != req.GenerationID {
		return CheckpointResult{}, fmt.Errorf("container generation mismatch: got %s want %s", container.GenerationID, req.GenerationID)
	}
	if strings.TrimSpace(req.Generation.SessionID) != "" && req.Generation.SessionID != req.SessionID {
		return CheckpointResult{}, fmt.Errorf("checkpoint generation session mismatch")
	}
	if strings.TrimSpace(req.Generation.GenerationID) != "" && req.Generation.GenerationID != req.GenerationID {
		return CheckpointResult{}, fmt.Errorf("checkpoint generation id mismatch")
	}
	if strings.TrimSpace(req.Generation.RunscContainerID) != "" && req.Generation.RunscContainerID != container.RunscContainerID {
		return CheckpointResult{}, fmt.Errorf("checkpoint runsc container mismatch")
	}
	generationCheckpointPath := strings.TrimSpace(req.Generation.CheckpointPath)
	checkpointPath := strings.TrimSpace(req.CheckpointPath)
	if req.Generation.CheckpointPath != "" && req.Generation.CheckpointPath != generationCheckpointPath {
		return CheckpointResult{}, fmt.Errorf("generation checkpoint path %q must be canonical absolute path", req.Generation.CheckpointPath)
	}
	if req.CheckpointPath != "" && req.CheckpointPath != checkpointPath {
		return CheckpointResult{}, fmt.Errorf("generation checkpoint path %q must be canonical absolute path", req.CheckpointPath)
	}
	if checkpointPath == "" {
		checkpointPath = generationCheckpointPath
	}
	if checkpointPath == "" {
		return CheckpointResult{}, errors.New("generation checkpoint path is required")
	}
	if !filepath.IsAbs(checkpointPath) || filepath.Clean(checkpointPath) != checkpointPath {
		return CheckpointResult{}, fmt.Errorf("generation checkpoint path %q must be canonical absolute path", checkpointPath)
	}
	if generationCheckpointPath != "" && (!filepath.IsAbs(generationCheckpointPath) || filepath.Clean(generationCheckpointPath) != generationCheckpointPath) {
		return CheckpointResult{}, fmt.Errorf("generation checkpoint path %q must be canonical absolute path", generationCheckpointPath)
	}
	if generationCheckpointPath != "" && generationCheckpointPath != checkpointPath {
		return CheckpointResult{}, fmt.Errorf("checkpoint path mismatch: got %q want generation path %q", checkpointPath, generationCheckpointPath)
	}
	currentRunsc, err := r.verifyGenerationRunscPin(ctx, "checkpoint", req.Generation)
	if err != nil {
		return CheckpointResult{}, err
	}
	runscOverlay2, err := r.runscOverlay2(req.Generation)
	if err != nil {
		return CheckpointResult{}, err
	}

	if err := os.MkdirAll(filepath.Dir(checkpointPath), 0o755); err != nil {
		return CheckpointResult{}, fmt.Errorf("create checkpoint dir: %w", err)
	}
	if err := os.RemoveAll(checkpointPath); err != nil {
		return CheckpointResult{}, fmt.Errorf("clear checkpoint dir: %w", err)
	}
	if err := os.MkdirAll(checkpointPath, 0o755); err != nil {
		return CheckpointResult{}, fmt.Errorf("create checkpoint image dir: %w", err)
	}

	cmd := exec.CommandContext(ctx, currentRunsc.BinaryPath,
		"-root", r.cfg.RunscRoot,
		"-overlay2", runscOverlay2,
		"checkpoint",
		"-image-path", checkpointPath,
		container.RunscContainerID,
	)

	if output, err := cmd.CombinedOutput(); err != nil {
		_ = os.RemoveAll(checkpointPath)
		return CheckpointResult{}, fmt.Errorf("runsc checkpoint: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if err := writeCheckpointImageManifest(checkpointPath); err != nil {
		_ = os.RemoveAll(checkpointPath)
		return CheckpointResult{}, err
	}
	imageManifestDigest, err := CheckpointImageManifestDigest(checkpointPath)
	if err != nil {
		_ = os.RemoveAll(checkpointPath)
		return CheckpointResult{}, err
	}

	r.mu.Lock()
	if current := r.containers[req.SessionID]; current == container {
		delete(r.containers, req.SessionID)
	}
	r.mu.Unlock()

	// The checkpoint image is durable once runsc checkpoint returns. Do not wait
	// synchronously for the attached runsc run process here; that teardown can
	// block status finalization and leave the session stuck in checkpointing.
	container.Cancel()

	return CheckpointResult{ImageManifestDigest: imageManifestDigest}, nil
}
