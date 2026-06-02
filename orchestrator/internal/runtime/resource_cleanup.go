package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"harness-platform/orchestrator/internal/store"
)

type GenerationResourceCleanup struct {
	RunscDeleted      bool
	CheckpointDeleted bool
	ControlDirDeleted bool
	BundleDirDeleted  bool
	BridgeDirDeleted  bool
	NetworkDirDeleted bool
	LogDirDeleted     bool
	NetnsDeleted      bool
	HostVethDeleted   bool
	NftTableDeleted   bool
	RunscState        string
	RunscPinEvidence  string
	IPNetns           string
	IPLink            string
	NFT               string
	FilesystemLstat   map[string]string
}

func (r *Runtime) DestroyGenerationResources(ctx context.Context, details store.RuntimeGenerationDetails) (GenerationResourceCleanup, error) {
	var cleanup GenerationResourceCleanup
	if strings.TrimSpace(details.GenerationID) == "" {
		return cleanup, fmt.Errorf("generation id is required")
	}
	if strings.TrimSpace(details.SessionID) == "" {
		return cleanup, fmt.Errorf("session id is required")
	}

	var errs []error
	targets, err := r.generationFilesystemCleanupTargets(details)
	if err != nil {
		return cleanup, err
	}
	for _, target := range targets {
		deleted, err := removeFilesystemCleanupTarget(target)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		switch target.kind {
		case cleanupTargetCheckpoint:
			cleanup.CheckpointDeleted = deleted
		case cleanupTargetControl:
			cleanup.ControlDirDeleted = deleted
		case cleanupTargetBundle:
			cleanup.BundleDirDeleted = deleted
		case cleanupTargetBridge:
			cleanup.BridgeDirDeleted = deleted
		case cleanupTargetNetwork:
			cleanup.NetworkDirDeleted = deleted
		case cleanupTargetLog:
			cleanup.LogDirDeleted = deleted
		}
	}
	if len(errs) > 0 {
		return cleanup, errors.Join(errs...)
	}

	containerID, err := runscContainerID(details)
	if err != nil {
		return cleanup, err
	}
	runscBinary, runscPinEvidence, err := r.deleteGenerationRunscContainer(ctx, details, containerID)
	cleanup.RunscPinEvidence = runscPinEvidence
	if err != nil {
		errs = append(errs, fmt.Errorf("delete runsc container %s: %w", containerID, err))
	} else {
		cleanup.RunscDeleted = true
	}

	runscNetwork, err := r.runscNetwork(details)
	if err != nil {
		return cleanup, err
	}
	if strings.EqualFold(runscNetwork, "sandbox") {
		tableName, err := generationNftTableName(details)
		if err != nil {
			return cleanup, err
		}
		if err := r.deleteNetworkResource(ctx, "nft", []string{"delete", "table", "inet", tableName}, true); err != nil {
			errs = append(errs, err)
		} else {
			cleanup.NftTableDeleted = true
		}
		if strings.TrimSpace(details.HostVeth) != "" {
			if err := r.deleteNetworkResource(ctx, "ip", []string{"link", "delete", details.HostVeth}, true); err != nil {
				errs = append(errs, err)
			} else {
				cleanup.HostVethDeleted = true
			}
		}
		if strings.TrimSpace(details.NetnsName) != "" {
			if err := r.deleteNetworkResource(ctx, "ip", []string{"netns", "delete", details.NetnsName}, true); err != nil {
				errs = append(errs, err)
			} else {
				cleanup.NetnsDeleted = true
			}
		}
	}
	if len(errs) > 0 {
		return cleanup, errors.Join(errs...)
	}
	if err := r.recordGenerationResourceAbsenceEvidence(ctx, details, runscBinary, containerID, targets, &cleanup); err != nil {
		return cleanup, err
	}
	return cleanup, nil
}

type cleanupTargetKind string

const (
	cleanupTargetCheckpoint      cleanupTargetKind = "checkpoint"
	cleanupTargetControl         cleanupTargetKind = "control"
	cleanupTargetControlManifest cleanupTargetKind = "control_manifest"
	cleanupTargetBundle          cleanupTargetKind = "bundle"
	cleanupTargetSpec            cleanupTargetKind = "spec"
	cleanupTargetBridge          cleanupTargetKind = "bridge"
	cleanupTargetNetwork         cleanupTargetKind = "network"
	cleanupTargetNetworkHosts    cleanupTargetKind = "network_hosts"
	cleanupTargetLog             cleanupTargetKind = "log"
)

type filesystemCleanupTarget struct {
	kind cleanupTargetKind
	path string
	root string
}

func (r *Runtime) generationFilesystemCleanupTargets(details store.RuntimeGenerationDetails) ([]filesystemCleanupTarget, error) {
	runRoot, err := cleanAbsoluteRoot(r.cfg.RunDir, "runtime run dir")
	if err != nil {
		return nil, err
	}
	generationDir, err := safePathComponent("generation id", "gen-"+strings.TrimSpace(details.GenerationID))
	if err != nil {
		return nil, err
	}
	targets := []filesystemCleanupTarget{
		{kind: cleanupTargetControl, path: details.ControlDirPath, root: runRoot},
		{kind: cleanupTargetBundle, path: details.BundleDirPath, root: runRoot},
		{kind: cleanupTargetBridge, path: details.BridgeDirPath, root: runRoot},
		{kind: cleanupTargetLog, path: details.LogDirPath, root: runRoot},
	}
	if strings.TrimSpace(details.NetworkHostsPath) != "" {
		targets = append(targets, filesystemCleanupTarget{kind: cleanupTargetNetwork, path: filepath.Dir(details.NetworkHostsPath), root: runRoot})
	}
	expected := map[cleanupTargetKind]string{
		cleanupTargetControl: filepath.Join(runRoot, "control", generationDir),
		cleanupTargetBundle:  filepath.Join(runRoot, "runtime", generationDir),
		cleanupTargetBridge:  filepath.Join(runRoot, "bridge", generationDir),
		cleanupTargetNetwork: filepath.Join(runRoot, "network", generationDir),
		cleanupTargetLog:     filepath.Join(runRoot, "logs", generationDir),
	}
	for _, target := range targets {
		if err := validateFilesystemCleanupTarget(target.kind, target.path, expected[target.kind], target.root); err != nil {
			return nil, err
		}
	}
	if err := validateFilesystemCleanupTarget(cleanupTargetControlManifest, details.ControlManifestPath, filepath.Join(runRoot, "control", generationDir, controlFileName), runRoot); err != nil {
		return nil, err
	}
	if err := validateFilesystemCleanupTarget(cleanupTargetSpec, details.SpecPath, filepath.Join(runRoot, "runtime", generationDir, "config.json"), runRoot); err != nil {
		return nil, err
	}
	if strings.TrimSpace(details.NetworkHostsPath) != "" {
		if err := validateFilesystemCleanupTarget(cleanupTargetNetworkHosts, details.NetworkHostsPath, filepath.Join(runRoot, "network", generationDir, "hosts"), runRoot); err != nil {
			return nil, err
		}
	}

	checkpointPath, err := validateCheckpointCleanupTarget(details, runRoot, generationDir)
	if err != nil {
		return nil, err
	}
	targets = append([]filesystemCleanupTarget{{kind: cleanupTargetCheckpoint, path: checkpointPath.path, root: checkpointPath.root}}, targets...)
	return targets, nil
}

func (r *Runtime) recordGenerationResourceAbsenceEvidence(ctx context.Context, details store.RuntimeGenerationDetails, runscBinary, containerID string, targets []filesystemCleanupTarget, cleanup *GenerationResourceCleanup) error {
	runscState, err := r.runscContainerAbsenceEvidence(ctx, runscBinary, containerID)
	if err != nil {
		return err
	}
	cleanup.RunscState = appendEvidence(runscState, cleanup.RunscPinEvidence)
	runscNetwork, err := r.runscNetwork(details)
	if err != nil {
		return err
	}
	if strings.EqualFold(runscNetwork, "sandbox") {
		ipNetns, err := r.netnsAbsenceEvidence(ctx, details.NetnsName)
		if err != nil {
			return err
		}
		ipLink, err := r.ipLinkAbsenceEvidence(ctx, details.HostVeth)
		if err != nil {
			return err
		}
		tableName, err := generationNftTableName(details)
		if err != nil {
			return err
		}
		nft, err := r.nftTableAbsenceEvidence(ctx, tableName)
		if err != nil {
			return err
		}
		cleanup.IPNetns = ipNetns
		cleanup.IPLink = ipLink
		cleanup.NFT = nft
	} else {
		cleanup.IPNetns = "netns:absent; check=runsc_network_not_sandbox"
		cleanup.IPLink = "host_veth:absent; check=runsc_network_not_sandbox"
		cleanup.NFT = "nft_table:absent; check=runsc_network_not_sandbox"
	}
	filesystem, err := filesystemAbsenceEvidence(generationFilesystemEvidenceTargets(details, targets))
	if err != nil {
		return err
	}
	cleanup.FilesystemLstat = filesystem
	return nil
}

func (r *Runtime) runscContainerAbsenceEvidence(ctx context.Context, runscBinary, containerID string) (string, error) {
	runscBinary, err := requiredRunscBinary(runscBinary)
	if err != nil {
		return "", err
	}
	output, err := r.runner.CombinedOutput(ctx, runscBinary, "-root", r.cfg.RunscRoot, "state", containerID)
	if err != nil {
		if commandFailureContains(output, err, "does not exist", "not found", "no such container", "no such file") {
			return "runsc_container:absent; check=" + runscBinary + " state " + containerID, nil
		}
		return "", fmt.Errorf("verify runsc container %s absence: %w: %s", containerID, err, strings.TrimSpace(string(output)))
	}
	return "", fmt.Errorf("verify runsc container %s absence: container still present", containerID)
}

func appendEvidence(base, extra string) string {
	extra = strings.TrimSpace(extra)
	if extra == "" {
		return base
	}
	if strings.TrimSpace(base) == "" {
		return extra
	}
	return base + "; " + extra
}

func (r *Runtime) netnsAbsenceEvidence(ctx context.Context, netnsName string) (string, error) {
	netnsName = strings.TrimSpace(netnsName)
	if netnsName == "" {
		return "netns:absent; check=not_configured", nil
	}
	output, err := r.runner.CombinedOutput(ctx, "ip", "netns", "list")
	if err != nil {
		return "", fmt.Errorf("verify network namespace %s absence: %w: %s", netnsName, err, strings.TrimSpace(string(output)))
	}
	if netnsListContains(string(output), netnsName) {
		return "", fmt.Errorf("verify network namespace %s absence: namespace still present", netnsName)
	}
	return "netns:absent; check=ip netns list " + netnsName, nil
}

func (r *Runtime) ipLinkAbsenceEvidence(ctx context.Context, linkName string) (string, error) {
	linkName = strings.TrimSpace(linkName)
	if linkName == "" {
		return "host_veth:absent; check=not_configured", nil
	}
	output, err := r.runner.CombinedOutput(ctx, "ip", "link", "show", linkName)
	if err != nil {
		if commandFailureContains(output, err, "cannot find device", "does not exist", "not found", "no such device") {
			return "host_veth:absent; check=ip link show " + linkName, nil
		}
		return "", fmt.Errorf("verify host veth %s absence: %w: %s", linkName, err, strings.TrimSpace(string(output)))
	}
	return "", fmt.Errorf("verify host veth %s absence: link still present", linkName)
}

func (r *Runtime) nftTableAbsenceEvidence(ctx context.Context, tableName string) (string, error) {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return "nft_table:absent; check=not_configured", nil
	}
	output, err := r.runner.CombinedOutput(ctx, "nft", "list", "table", "inet", tableName)
	if err != nil {
		if commandFailureContains(output, err, "does not exist", "not found", "no such file", "no such table") {
			return "nft_table:absent; check=nft list table inet " + tableName, nil
		}
		return "", fmt.Errorf("verify nft table %s absence: %w: %s", tableName, err, strings.TrimSpace(string(output)))
	}
	return "", fmt.Errorf("verify nft table %s absence: table still present", tableName)
}

func netnsListContains(output, netnsName string) bool {
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == netnsName {
			return true
		}
	}
	return false
}

func generationFilesystemEvidenceTargets(details store.RuntimeGenerationDetails, cleanupTargets []filesystemCleanupTarget) []filesystemCleanupTarget {
	targets := append([]filesystemCleanupTarget(nil), cleanupTargets...)
	for _, target := range cleanupTargets {
		switch target.kind {
		case cleanupTargetControl:
			targets = append(targets, filesystemCleanupTarget{kind: cleanupTargetControlManifest, path: cleanAbsolutePath(details.ControlManifestPath), root: target.root})
		case cleanupTargetBundle:
			targets = append(targets, filesystemCleanupTarget{kind: cleanupTargetSpec, path: cleanAbsolutePath(details.SpecPath), root: target.root})
		case cleanupTargetNetwork:
			if strings.TrimSpace(details.NetworkHostsPath) != "" {
				targets = append(targets, filesystemCleanupTarget{kind: cleanupTargetNetworkHosts, path: cleanAbsolutePath(details.NetworkHostsPath), root: target.root})
			}
		}
	}
	return targets
}

func filesystemAbsenceEvidence(targets []filesystemCleanupTarget) (map[string]string, error) {
	evidence := make(map[string]string, len(targets))
	for _, target := range targets {
		if _, err := os.Lstat(target.path); err == nil {
			return nil, fmt.Errorf("verify %s cleanup path %q absence: path still exists", target.kind, target.path)
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("verify %s cleanup path %q absence: %w", target.kind, target.path, err)
		}
		evidence[string(target.kind)+":"+target.path] = "lstat:absent"
	}
	return evidence, nil
}

func validateCheckpointCleanupTarget(details store.RuntimeGenerationDetails, runRoot, generationDir string) (filesystemCleanupTarget, error) {
	expectedGenerationCheckpoint := filepath.Join(runRoot, generationDir, "checkpoint")
	if err := validateFilesystemCleanupTarget(cleanupTargetCheckpoint, details.CheckpointPath, expectedGenerationCheckpoint, runRoot); err != nil {
		return filesystemCleanupTarget{}, err
	}
	return filesystemCleanupTarget{kind: cleanupTargetCheckpoint, path: cleanAbsolutePath(details.CheckpointPath), root: runRoot}, nil
}

func validateFilesystemCleanupTarget(kind cleanupTargetKind, actual, expected, root string) error {
	actual = strings.TrimSpace(actual)
	if actual == "" {
		return fmt.Errorf("%s cleanup path is required", kind)
	}
	if pathContainsDotDot(actual) {
		return fmt.Errorf("%s cleanup path %q must not contain '..'", kind, actual)
	}
	cleaned := cleanAbsolutePath(actual)
	if cleaned == "" {
		return fmt.Errorf("%s cleanup path %q must be absolute", kind, actual)
	}
	if cleaned != filepath.Clean(expected) {
		return fmt.Errorf("%s cleanup path %q does not match expected %q", kind, cleaned, filepath.Clean(expected))
	}
	if err := ensurePathStaysWithinRoot(cleaned, root); err != nil {
		return fmt.Errorf("%s cleanup path %q is unsafe: %w", kind, cleaned, err)
	}
	return nil
}

func removeFilesystemCleanupTarget(target filesystemCleanupTarget) (bool, error) {
	if _, err := os.Lstat(target.path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("stat %s cleanup path %q: %w", target.kind, target.path, err)
	}
	if err := os.RemoveAll(target.path); err != nil {
		return false, fmt.Errorf("remove %s cleanup path %q: %w", target.kind, target.path, err)
	}
	return true, nil
}

func (r *Runtime) deleteNetworkResource(ctx context.Context, name string, args []string, missingOK bool) error {
	output, err := r.runner.CombinedOutput(ctx, name, args...)
	if err == nil {
		return nil
	}
	if missingOK && commandFailureContains(output, err, "cannot find device", "does not exist", "not found", "no such file", "no such process", "no such table") {
		return nil
	}
	return fmt.Errorf("destroy sandbox network resource %q: %w: %s", strings.Join(append([]string{name}, args...), " "), err, strings.TrimSpace(string(output)))
}

func (r *Runtime) deleteGenerationRunscContainer(ctx context.Context, details store.RuntimeGenerationDetails, containerID string) (string, string, error) {
	pinned := runscPinFromDetails(details)
	if err := requireCompleteRunscPin("generation details", pinned); err != nil {
		return "", "", err
	}
	current, err := r.currentRunscPin(ctx)
	if err != nil {
		return "", "", err
	}
	currentBinary := current.BinaryPath
	if cleanupRunscPinMatches(current, pinned) {
		return currentBinary, "", r.deleteRunscContainer(ctx, currentBinary, containerID)
	}
	evidence := cleanupRunscPinMismatchEvidence(current, pinned, "current")
	currentResult, currentErr := r.deleteRunscContainerDetailed(ctx, currentBinary, containerID)
	if currentErr == nil && !currentResult.Missing {
		return currentBinary, evidence, nil
	}
	if currentErr == nil && currentResult.Missing {
		currentErr = fmt.Errorf("current runsc reported container missing under mismatched pin")
	}
	return currentBinary, evidence, fmt.Errorf("current runsc pin mismatch; current delete failed: %w", currentErr)
}

func cleanupRunscPinMatches(current, pinned runscPin) bool {
	checks := []struct {
		current string
		pinned  string
	}{
		{current.Platform, pinned.Platform},
		{current.Version, pinned.Version},
		{current.BinaryPath, pinned.BinaryPath},
		{current.BinaryDigest, pinned.BinaryDigest},
	}
	for _, check := range checks {
		if strings.TrimSpace(check.pinned) != "" && check.current != check.pinned {
			return false
		}
	}
	return true
}

func cleanupRunscPinMismatchEvidence(current, pinned runscPin, cleanupBinary string) string {
	return fmt.Sprintf("runsc_pin:mismatch; current_platform=%s; pinned_platform=%s; current_version=%s; pinned_version=%s; current_binary_path=%s; pinned_binary_path=%s; current_binary_digest=%s; pinned_binary_digest=%s; cleanup_binary=%s",
		current.Platform,
		pinned.Platform,
		current.Version,
		pinned.Version,
		current.BinaryPath,
		pinned.BinaryPath,
		current.BinaryDigest,
		pinned.BinaryDigest,
		cleanupBinary,
	)
}

type runscContainerDeleteResult struct {
	Missing bool
}

func (r *Runtime) deleteRunscContainer(ctx context.Context, runscBinary, containerID string) error {
	_, err := r.deleteRunscContainerDetailed(ctx, runscBinary, containerID)
	return err
}

func (r *Runtime) deleteRunscContainerDetailed(ctx context.Context, runscBinary, containerID string) (runscContainerDeleteResult, error) {
	runscBinary, err := requiredRunscBinary(runscBinary)
	if err != nil {
		return runscContainerDeleteResult{}, err
	}
	_, _ = r.runner.CombinedOutput(ctx, runscBinary, "-root", r.cfg.RunscRoot, "kill", containerID, "KILL")
	output, err := r.runner.CombinedOutput(ctx, runscBinary, "-root", r.cfg.RunscRoot, "delete", "-force", containerID)
	if err != nil {
		if commandFailureContains(output, err, "does not exist", "not found", "no such container", "no such file") {
			return runscContainerDeleteResult{Missing: true}, nil
		}
		return runscContainerDeleteResult{}, fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}
	return runscContainerDeleteResult{}, nil
}
