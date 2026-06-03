package runtime

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/driveradapter"
	"harness-platform/orchestrator/internal/store"
)

type Config struct {
	RunscRoot               string
	RunscNetwork            string
	RunscOverlay2           string
	SessionsRoot            string
	AgentHomesRoot          string
	BundleRoot              string
	RootFSPath              string
	SandboxUID              int
	SandboxGID              int
	SandboxSupplementalGIDs []int
	RunDir                  string
	PreStartProbeAttempts   int
	PreStartProbeInterval   time.Duration
	ProbeHealthzStatuses    []int
	BridgeHeartbeat         time.Duration
	BridgePollInterval      time.Duration
	BridgeMode              string
	CommandRunner           CommandRunner
}

const controlFileName = "session.json"
const supportedRunscPlatform = "systrap"

type CommandRunner interface {
	CombinedOutput(context.Context, string, ...string) ([]byte, error)
}

type execCommandRunner struct{}

func (execCommandRunner) CombinedOutput(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

type StartRequest struct {
	SessionID             string
	GenerationID          string
	DriverID              string
	RestoreFromCheckpoint bool
	Generation            store.RuntimeGenerationDetails
	PreparedArtifacts     GenerationArtifacts
	WorkspaceHostPath     string
	AgentHomeHostPath     string
	ContentSnapshots      []store.ContentSnapshotRecord
}

type Output struct {
	Stream string `json:"stream"`
	Line   string `json:"line"`
}

type Result struct {
	RestoreMS             *int64
	ControlManifestDigest string
	RunscVersion          string
	PostStartProof        *store.RuntimeResourcePostStartProof
	Err                   error
}

type Runtime struct {
	cfg        Config
	runner     CommandRunner
	mu         sync.RWMutex
	containers map[string]*Container
}

type Container struct {
	SessionID        string
	GenerationID     string
	RunscContainerID string
	DriverID         string
	Cmd              *exec.Cmd
	Stdin            io.WriteCloser
	Stdout           io.ReadCloser
	Stderr           io.ReadCloser
	Cancel           context.CancelFunc
	InputMu          sync.Mutex
	OutputHub        *OutputHub // Per-container pub/sub for output events
}

func New(cfg Config) *Runtime {
	runner := cfg.CommandRunner
	if runner == nil {
		runner = execCommandRunner{}
	}
	return &Runtime{
		cfg:        cfg,
		runner:     runner,
		containers: make(map[string]*Container),
	}
}

func (r *Runtime) Start(ctx context.Context, req StartRequest, output func(Output)) Result {
	driverID, err := resolveDriverID(req.DriverID)
	if err != nil {
		return Result{Err: err}
	}
	req.DriverID = driverID

	// Check if container already exists (hot path)
	r.mu.RLock()
	container, exists := r.containers[req.SessionID]
	r.mu.RUnlock()

	if exists {
		if container.GenerationID == req.GenerationID {
			// Turns are delivered by the bridge claim/ack loop, not by writing
			// directly to the runtime process stdin.
			return Result{}
		}
		r.stopContainer(container)
	}

	if req.RestoreFromCheckpoint {
		return r.resumeFromCheckpoint(ctx, req, output)
	}

	// Fresh start (cold path)
	return r.startFresh(ctx, req, output)
}

func (r *Runtime) PrepareGeneration(ctx context.Context, req StartRequest) (GenerationArtifacts, error) {
	artifacts, err := r.renderGenerationArtifacts(ctx, req)
	if err != nil {
		return GenerationArtifacts{}, err
	}
	return artifacts, nil
}

func (r *Runtime) PrepareGenerationNetwork(ctx context.Context, req StartRequest) error {
	if err := r.ensureSandboxNetwork(ctx, req.Generation); err != nil {
		return err
	}
	return nil
}

func resolveDriverID(driverID string) (string, error) {
	driverID = strings.TrimSpace(driverID)
	if driverID == "" {
		return "", errors.New("driver id is required")
	}
	if _, ok := agents.Lookup(driverID); !ok {
		return "", fmt.Errorf("unsupported driver %q", driverID)
	}
	return driverID, nil
}

func scanLines(wg *sync.WaitGroup, r io.Reader, stream string, hub *OutputHub) {
	defer wg.Done()
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		hub.Publish(OutputEvent{Stream: stream, Line: scanner.Text()})
	}
}

func (r *Runtime) Destroy(ctx context.Context, containerID string) error {
	if containerID == "" {
		return errors.New("runsc container id is required")
	}
	if err := r.deleteRunscContainer(ctx, "runsc", containerID); err != nil {
		return fmt.Errorf("runsc delete %s: %w", containerID, err)
	}
	r.evictContainerByRunscID(containerID)
	return nil
}

func (r *Runtime) cleanupRunscContainer(ctx context.Context, containerID string) {
	_ = r.deleteRunscContainer(ctx, "runsc", containerID)
}

func (r *Runtime) removeContainer(container *Container) {
	r.mu.Lock()
	if current := r.containers[container.SessionID]; current == container {
		delete(r.containers, container.SessionID)
	}
	r.mu.Unlock()
}

func (r *Runtime) cleanupExitedContainer(container *Container) {
	r.mu.Lock()
	current := r.containers[container.SessionID]
	if current == container {
		delete(r.containers, container.SessionID)
	}
	r.mu.Unlock()
	if current == container {
		r.cleanupRunscContainer(context.Background(), container.RunscContainerID)
	}
}

func (r *Runtime) stopContainer(container *Container) {
	r.removeContainer(container)
	if container.Cancel != nil {
		container.Cancel()
	}
	r.cleanupRunscContainer(context.Background(), container.RunscContainerID)
}

func (r *Runtime) evictContainerByRunscID(runscContainerID string) {
	var evicted []*Container
	r.mu.Lock()
	for sessionID, container := range r.containers {
		if container.RunscContainerID == runscContainerID {
			delete(r.containers, sessionID)
			evicted = append(evicted, container)
		}
	}
	r.mu.Unlock()
	for _, container := range evicted {
		if container.Cancel != nil {
			container.Cancel()
		}
	}
}

func (r *Runtime) rootFSPath() string {
	if strings.TrimSpace(r.cfg.RootFSPath) != "" {
		return strings.TrimSpace(r.cfg.RootFSPath)
	}
	return filepath.Join(r.repoRoot(), "sandbox-image", "rootfs")
}

func (r *Runtime) repoRoot() string {
	if strings.TrimSpace(r.cfg.BundleRoot) == "" {
		wd, err := os.Getwd()
		if err == nil {
			return filepath.Dir(wd)
		}
		return "/home/harness-platform"
	}
	return filepath.Clean(filepath.Join(r.cfg.BundleRoot, "..", ".."))
}

func writeJSONFileAtomic(path string, value any, mode os.FileMode) error {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return err
	}
	return writeFileAtomic(path, buf.Bytes(), mode)
}

func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	parent, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer parent.Close()
	return parent.Sync()
}

func canonicalJSON(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var normalized any
	if err := json.Unmarshal(data, &normalized); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(normalized); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func digestHex(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:])
}

func prefixedSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + fmt.Sprintf("%x", sum[:])
}

func driverAgentHomeDirHostPath(agentHomeHostPath, relativePath string) (string, error) {
	relativePath = strings.TrimSpace(relativePath)
	if relativePath == "" {
		return "", fmt.Errorf("agent home relative path is required")
	}
	cleaned := filepath.Clean(filepath.FromSlash(relativePath))
	if cleaned == "." || cleaned == ".." || filepath.IsAbs(cleaned) || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("agent home relative path %q escapes /agent-home", relativePath)
	}
	return filepath.Join(agentHomeHostPath, cleaned), nil
}

func (r *Runtime) prepareRuntimeDataDirs(req StartRequest) error {
	if isSandboxIsolatedRequest(req) {
		return r.prepareSandboxIsolationDataDirs(req)
	}
	selectedDriver := driverID(req)
	if selectedDriver == "" {
		return fmt.Errorf("driver id is required")
	}
	return fmt.Errorf("unsupported driver %q", selectedDriver)
}

func (r *Runtime) prepareSandboxIsolationDataDirs(req StartRequest) error {
	identity, err := r.requiredSandboxIdentity(req.Generation)
	if err != nil {
		return err
	}
	workspaceHostPath, agentHomeHostPath, err := r.sandboxIsolationDataPaths(req)
	if err != nil {
		return err
	}
	for _, item := range []struct {
		label string
		path  string
		mode  os.FileMode
	}{
		{label: "sandbox workspace", path: workspaceHostPath, mode: 0o750},
		{label: "sandbox agent home", path: agentHomeHostPath, mode: 0o750},
	} {
		if err := ensureSandboxOwnedDir(item.path, identity.UID, identity.GID, item.mode); err != nil {
			return fmt.Errorf("%s: %w", item.label, err)
		}
	}
	if layout, ok := driveradapter.RuntimeLayoutSpecFor(agents.ID(driverID(req))); ok {
		for _, item := range layout.HomeDirs {
			hostPath, err := driverAgentHomeDirHostPath(agentHomeHostPath, item.AgentHomeRelativePath)
			if err != nil {
				return fmt.Errorf("%s: %w", item.Label, err)
			}
			if err := ensureSandboxOwnedDir(hostPath, identity.UID, identity.GID, item.Mode); err != nil {
				return fmt.Errorf("%s: %w", item.Label, err)
			}
		}
	}
	if err := prepareBridgeMountPlaceholder(req.Generation.ControlDirPath); err != nil {
		return err
	}
	if strings.TrimSpace(req.Generation.BridgeDirPath) != "" {
		if err := bridge.EnsureLayout(req.Generation.BridgeDirPath); err != nil {
			return fmt.Errorf("create sandbox bridge dir: %w", err)
		}
		if err := prepareBridgeDirectoryPermissions(req.Generation.BridgeDirPath, identity.UID, identity.GID); err != nil {
			return fmt.Errorf("sandbox bridge dir: %w", err)
		}
	}
	return nil
}

func (r *Runtime) sandboxIsolationDataPaths(req StartRequest) (string, string, error) {
	if strings.TrimSpace(req.WorkspaceHostPath) == "" || strings.TrimSpace(req.AgentHomeHostPath) == "" {
		return "", "", fmt.Errorf("workspace and agent home data volume paths are required")
	}
	workspaceHostPath, err := cleanSandboxDataPath(req.WorkspaceHostPath, "workspace data volume path")
	if err != nil {
		return "", "", err
	}
	agentHomeHostPath, err := cleanSandboxDataPath(req.AgentHomeHostPath, "agent home data volume path")
	if err != nil {
		return "", "", err
	}
	return workspaceHostPath, agentHomeHostPath, nil
}

func cleanSandboxDataPath(path, label string) (string, error) {
	for _, part := range strings.Split(path, string(filepath.Separator)) {
		if part == ".." {
			return "", fmt.Errorf("%s %q must not contain '..'", label, path)
		}
	}
	cleaned := cleanAbsolutePath(path)
	if cleaned == "" {
		return "", fmt.Errorf("%s is required and must be absolute", label)
	}
	if cleaned == string(filepath.Separator) {
		return "", fmt.Errorf("%s must not be filesystem root", label)
	}
	if cleaned != strings.TrimSpace(path) {
		return "", fmt.Errorf("%s %q must be canonical", label, path)
	}
	return cleaned, nil
}

func driverID(req StartRequest) string {
	if driverID := strings.TrimSpace(req.DriverID); driverID != "" {
		return driverID
	}
	return strings.TrimSpace(req.Generation.DriverID)
}

func isSandboxIsolatedRequest(req StartRequest) bool {
	switch driverID(req) {
	case string(agents.ClaudeCode), string(agents.Pi), string(agents.Shell):
		return true
	default:
		return false
	}
}

func prepareBridgeMountPlaceholder(controlDir string) error {
	controlDir = strings.TrimSpace(controlDir)
	if controlDir == "" {
		return fmt.Errorf("sandbox control dir is required")
	}
	placeholder := filepath.Join(controlDir, "bridge")
	if err := os.MkdirAll(placeholder, 0o755); err != nil {
		return fmt.Errorf("create bridge mount placeholder: %w", err)
	}
	info, err := os.Lstat(placeholder)
	if err != nil {
		return fmt.Errorf("stat bridge mount placeholder: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("bridge mount placeholder %q must not be a symlink", placeholder)
	}
	if !info.IsDir() {
		return fmt.Errorf("bridge mount placeholder %q must be a directory", placeholder)
	}
	entries, err := os.ReadDir(placeholder)
	if err != nil {
		return fmt.Errorf("read bridge mount placeholder: %w", err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("bridge mount placeholder %q must be empty", placeholder)
	}
	mountpoint, err := pathIsMountPoint(placeholder)
	if err != nil {
		return fmt.Errorf("inspect bridge mount placeholder mountpoint: %w", err)
	}
	if mountpoint {
		return fmt.Errorf("bridge mount placeholder %q must not be a mountpoint", placeholder)
	}
	return nil
}

func pathIsMountPoint(path string) (bool, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return false, err
	}
	parent := filepath.Dir(path)
	parentInfo, err := os.Lstat(parent)
	if err != nil {
		return false, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false, fmt.Errorf("stat metadata unavailable for %s", path)
	}
	parentStat, ok := parentInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return false, fmt.Errorf("stat metadata unavailable for %s", parent)
	}
	return stat.Dev != parentStat.Dev || stat.Ino == parentStat.Ino, nil
}

func ensureSandboxOwnedDir(path string, uid, gid int, mode os.FileMode) error {
	return ensureOwnedDir(path, uid, gid, mode)
}

func ensureOwnedDir(path string, uid, gid int, mode os.FileMode) error {
	if err := os.MkdirAll(path, mode); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%q must not be a symlink", path)
	}
	if !info.IsDir() {
		return fmt.Errorf("%q must be a directory", path)
	}
	if err := ensureSandboxOwnership(path, info, uid, gid); err != nil {
		return err
	}
	return os.Chmod(path, mode)
}

func prepareBridgeDirectoryPermissions(root string, sandboxUID, sandboxGID int) error {
	hostUID := 0
	if os.Geteuid() != 0 {
		hostUID = os.Geteuid()
	}
	for _, item := range []struct {
		path string
		uid  int
		gid  int
		mode os.FileMode
	}{
		{path: root, uid: hostUID, gid: sandboxGID, mode: 0o750},
		{path: filepath.Join(root, bridge.InboxDir), uid: hostUID, gid: sandboxGID, mode: 0o550},
		{path: filepath.Join(root, bridge.HostHeartbeatDir), uid: hostUID, gid: sandboxGID, mode: 0o550},
		{path: filepath.Join(root, bridge.HostTmpDir), uid: hostUID, gid: sandboxGID, mode: 0o550},
		{path: filepath.Join(root, bridge.OutboxDir), uid: sandboxUID, gid: sandboxGID, mode: 0o770},
		{path: filepath.Join(root, bridge.SandboxTmpDir), uid: sandboxUID, gid: sandboxGID, mode: 0o770},
		{path: filepath.Join(root, bridge.HeartbeatDir), uid: sandboxUID, gid: sandboxGID, mode: 0o770},
		{path: bridge.HostControlRoot(root), uid: hostUID, gid: sandboxGID, mode: 0o750},
		{path: bridge.HostOwnedPath(root, bridge.InboxDir), uid: hostUID, gid: sandboxGID, mode: 0o750},
		{path: bridge.HostOwnedPath(root, bridge.HostHeartbeatDir), uid: hostUID, gid: sandboxGID, mode: 0o750},
		{path: bridge.HostOwnedPath(root, bridge.HostTmpDir), uid: hostUID, gid: sandboxGID, mode: 0o750},
	} {
		if err := ensureOwnedDir(item.path, item.uid, item.gid, item.mode); err != nil {
			return err
		}
	}
	return nil
}

func ensureSandboxOwnership(path string, info os.FileInfo, uid, gid int) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("stat ownership unavailable for %s", path)
	}
	if int(stat.Uid) == uid && int(stat.Gid) == gid {
		return nil
	}
	if os.Geteuid() != 0 {
		return fmt.Errorf("%s owner is %d:%d, want %d:%d", path, stat.Uid, stat.Gid, uid, gid)
	}
	return os.Chown(path, uid, gid)
}

func commandOutputContains(output string, needles ...string) bool {
	output = strings.ToLower(output)
	for _, needle := range needles {
		if strings.Contains(output, needle) {
			return true
		}
	}
	return false
}

func commandFailureContains(output []byte, err error, needles ...string) bool {
	text := string(output)
	if err != nil {
		text += " " + err.Error()
	}
	return commandOutputContains(text, needles...)
}

func writeInterrupt(stdin io.Writer, driverID string) error {
	payload, err := driveradapter.InterruptPayloadFor(agents.ID(driverID))
	if err != nil {
		return err
	}
	if _, err := stdin.Write(payload); err != nil {
		return err
	}
	return nil
}

func (r *Runtime) writeContainerInput(container *Container, fn func(io.Writer) error) error {
	container.InputMu.Lock()
	defer container.InputMu.Unlock()
	return fn(container.Stdin)
}

func (r *Runtime) startFresh(ctx context.Context, req StartRequest, _ func(Output)) Result {
	hub := NewOutputHub()

	hub.Publish(OutputEvent{Stream: "runtime", Line: "starting fresh container"})

	artifacts, err := r.generationArtifacts(ctx, req)
	if err != nil {
		return Result{Err: err}
	}
	req.PreparedArtifacts = artifacts
	currentRunsc, err := r.verifyLaunchRunscPin(ctx, "fresh launch", req.Generation, artifacts)
	if err != nil {
		return Result{Err: err}
	}
	if err := r.prepareRuntimeDataDirs(req); err != nil {
		return Result{Err: err}
	}
	if !req.PreparedArtifacts.NetworkPrepared {
		if err := r.ensureSandboxNetwork(ctx, req.Generation); err != nil {
			return Result{Err: err}
		}
		req.PreparedArtifacts.NetworkPrepared = true
	}

	containerID, err := runscContainerID(req.Generation)
	if err != nil {
		return Result{Err: err}
	}
	runscOverlay2, err := r.runscOverlay2(req.Generation)
	if err != nil {
		return Result{Err: err}
	}
	runscNetwork, err := r.runscNetwork(req.Generation)
	if err != nil {
		return Result{Err: err}
	}
	bundlePath := artifacts.BundleDir
	hub.Publish(OutputEvent{Stream: "runtime", Line: "using per-generation runtime bundle"})
	r.cleanupRunscContainer(ctx, containerID)

	cmdCtx, cancelCmd := context.WithCancel(context.Background())
	cmd := exec.CommandContext(cmdCtx, currentRunsc.BinaryPath,
		"-root", r.cfg.RunscRoot,
		"-platform", currentRunsc.Platform,
		"-overlay2", runscOverlay2,
		"-network", runscNetwork,
		"run",
		"-bundle", bundlePath,
		containerID,
	)

	// Get pipes
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancelCmd()
		return Result{Err: fmt.Errorf("stdin pipe: %w", err)}
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancelCmd()
		return Result{Err: fmt.Errorf("stdout pipe: %w", err)}
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancelCmd()
		return Result{Err: fmt.Errorf("stderr pipe: %w", err)}
	}

	if err := cmd.Start(); err != nil {
		cancelCmd()
		return Result{Err: fmt.Errorf("runsc run start: %w", err)}
	}

	// Store container
	container := &Container{
		SessionID:        req.SessionID,
		GenerationID:     req.GenerationID,
		RunscContainerID: containerID,
		DriverID:         req.DriverID,
		Cmd:              cmd,
		Stdin:            stdin,
		Stdout:           stdout,
		Stderr:           stderr,
		Cancel:           cancelCmd,
		OutputHub:        hub,
	}

	r.mu.Lock()
	r.containers[req.SessionID] = container
	r.mu.Unlock()

	// Start streaming output to hub
	var wg sync.WaitGroup
	wg.Add(2)
	go scanLines(&wg, stdout, "stdout", hub)
	go scanLines(&wg, stderr, "stderr", hub)

	// Monitor container exit in background
	go func() {
		wg.Wait()
		_ = cmd.Wait()
		hub.Close() // Close hub when container exits
		r.cleanupExitedContainer(container)
	}()

	postStartProof, err := r.runtimePostStartProof(ctx, req.Generation, currentRunsc, containerID)
	if err != nil {
		r.stopContainer(container)
		return Result{Err: err}
	}

	return Result{
		ControlManifestDigest: req.PreparedArtifacts.ManifestDigest,
		RunscVersion:          req.PreparedArtifacts.RunscVersion,
		PostStartProof:        &postStartProof,
	}
}

func (r *Runtime) resumeFromCheckpoint(ctx context.Context, req StartRequest, _ func(Output)) Result {
	hub := NewOutputHub()

	hub.Publish(OutputEvent{Stream: "runtime", Line: "resuming from checkpoint"})

	checkpointPath, err := r.resolveCheckpointPath(req)
	if err != nil {
		return Result{Err: err}
	}
	artifacts, err := restoreGenerationArtifacts(req)
	if err != nil {
		return Result{Err: err}
	}
	req.PreparedArtifacts = artifacts
	currentRunsc, err := r.verifyLaunchRunscPin(ctx, "restore", req.Generation, artifacts)
	if err != nil {
		return Result{Err: err}
	}
	if err := validateCheckpointRestore(req.Generation, artifacts, checkpointPath); err != nil {
		return Result{Err: err}
	}
	if !req.PreparedArtifacts.NetworkPrepared {
		if err := r.ensureSandboxNetwork(ctx, req.Generation); err != nil {
			return Result{Err: err}
		}
		req.PreparedArtifacts.NetworkPrepared = true
	}
	containerID, err := runscContainerID(req.Generation)
	if err != nil {
		return Result{Err: err}
	}
	runscOverlay2, err := r.runscOverlay2(req.Generation)
	if err != nil {
		return Result{Err: err}
	}
	runscNetwork, err := r.runscNetwork(req.Generation)
	if err != nil {
		return Result{Err: err}
	}
	bundlePath := artifacts.BundleDir
	r.cleanupRunscContainer(ctx, containerID)

	cmdCtx, cancelCmd := context.WithCancel(context.Background())
	cmd := exec.CommandContext(cmdCtx, currentRunsc.BinaryPath,
		"-root", r.cfg.RunscRoot,
		"-platform", currentRunsc.Platform,
		"-overlay2", runscOverlay2,
		"-network", runscNetwork,
		"restore",
		"-bundle", bundlePath,
		"-image-path", checkpointPath,
		containerID,
	)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancelCmd()
		return Result{Err: fmt.Errorf("stdin pipe: %w", err)}
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancelCmd()
		return Result{Err: fmt.Errorf("stdout pipe: %w", err)}
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancelCmd()
		return Result{Err: fmt.Errorf("stderr pipe: %w", err)}
	}

	if err := cmd.Start(); err != nil {
		cancelCmd()
		return Result{Err: fmt.Errorf("runsc restore start: %w", err)}
	}

	container := &Container{
		SessionID:        req.SessionID,
		GenerationID:     req.GenerationID,
		RunscContainerID: containerID,
		DriverID:         req.DriverID,
		Cmd:              cmd,
		Stdin:            stdin,
		Stdout:           stdout,
		Stderr:           stderr,
		Cancel:           cancelCmd,
		OutputHub:        hub,
	}

	r.mu.Lock()
	r.containers[req.SessionID] = container
	r.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(2)
	go scanLines(&wg, stdout, "stdout", hub)
	go scanLines(&wg, stderr, "stderr", hub)

	go func() {
		wg.Wait()
		_ = cmd.Wait()
		hub.Close() // Close hub when container exits
		r.cleanupExitedContainer(container)
	}()

	postStartProof, err := r.runtimePostStartProof(ctx, req.Generation, currentRunsc, containerID)
	if err != nil {
		r.stopContainer(container)
		return Result{Err: err}
	}

	return Result{
		ControlManifestDigest: req.PreparedArtifacts.ManifestDigest,
		RunscVersion:          req.PreparedArtifacts.RunscVersion,
		PostStartProof:        &postStartProof,
	}
}

func runscContainerID(details store.RuntimeGenerationDetails) (string, error) {
	containerID := strings.TrimSpace(details.RunscContainerID)
	if containerID == "" {
		return "", fmt.Errorf("runsc container id is required")
	}
	return containerID, nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (r *Runtime) Interrupt(sessionID string) error {
	r.mu.RLock()
	container, exists := r.containers[sessionID]
	r.mu.RUnlock()
	if !exists {
		return errors.New("container not found")
	}
	return r.writeContainerInput(container, func(stdin io.Writer) error {
		return writeInterrupt(stdin, container.DriverID)
	})
}
