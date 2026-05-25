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
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"harness-platform/orchestrator/internal/agents"
	"harness-platform/orchestrator/internal/bridge"
	"harness-platform/orchestrator/internal/store"
)

type Config struct {
	RestoreScript         string
	RunscRoot             string
	RunscNetwork          string
	RunscOverlay2         string
	SessionsRoot          string
	AgentHomesRoot        string
	CheckpointsRoot       string
	BundleRoot            string
	RootFSPath            string
	DefaultAgent          string
	Claude                ClaudeConfig
	RestoreFromCheckpoint bool
	Phase7RunDir          string
	SecretsRoot           string
	SecretReadersGID      int
	PreStartProbeAttempts int
	PreStartProbeInterval time.Duration
	ProbeHealthzStatuses  []int
	ProbeMessageStatuses  []int
	BridgeHeartbeat       time.Duration
	BridgePollInterval    time.Duration
	BridgeMode            string
	CommandRunner         CommandRunner
}

const controlFileName = "session.json"
const checkpointImageManifestFileName = "harness-checkpoint-manifest.json"
const checkpointImageManifestVersion = 1
const secretPublishValidationWait = 500 * time.Millisecond

var requiredCheckpointImageFiles = []string{"checkpoint.img", "pages.img", "pages_meta.img"}

type GenerationResourceCleanup struct {
	NetnsDeleted    bool
	HostVethDeleted bool
	NftTableDeleted bool
}

type CommandRunner interface {
	CombinedOutput(context.Context, string, ...string) ([]byte, error)
}

type execCommandRunner struct{}

func (execCommandRunner) CombinedOutput(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

type ClaudeConfig struct {
	ProxyBindURL               string
	APIKey                     string
	AuthToken                  string
	Model                      string
	OutputFormat               string
	DisableNonessentialTraffic bool
}

type StartRequest struct {
	SessionID             string
	RestoreID             string
	GenerationID          string
	Agent                 string
	FirstMessage          string
	WaitForTurn           bool
	ClaudeSessionUUID     string
	ResumeClaude          bool
	RestoreFromCheckpoint bool
	Done                  <-chan struct{}
	Generation            store.RuntimeGenerationDetails
	PreparedArtifacts     GenerationArtifacts
}

type Output struct {
	Stream string `json:"stream"`
	Line   string `json:"line"`
}

type Result struct {
	RestoreMS             *int64
	ControlManifestDigest string
	RunscVersion          string
	Err                   error
}

type CheckpointRequest struct {
	SessionID      string
	GenerationID   string
	CheckpointPath string
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

type controlManifest struct {
	SessionID                            string `json:"session_id"`
	GenerationID                         string `json:"generation_id"`
	CreatedAt                            string `json:"created_at"`
	AttemptID                            string `json:"attempt_id"`
	NetworkProfileID                     string `json:"network_profile_id"`
	AgentRuntimeProfileID                string `json:"agent_runtime_profile_id"`
	Agent                                string `json:"agent"`
	ClaudeSessionUUID                    string `json:"claude_session_uuid,omitempty"`
	ResumeClaude                         bool   `json:"resume_claude"`
	RunscPlatform                        string `json:"runsc_platform"`
	RunscVersion                         string `json:"runsc_version"`
	AnthropicBaseURL                     string `json:"anthropic_base_url,omitempty"`
	AnthropicAPIKeySecretID              string `json:"anthropic_api_key_secret_id,omitempty"`
	AnthropicAuthTokenSecretID           string `json:"anthropic_auth_token_secret_id,omitempty"`
	SecretVersion                        string `json:"secret_version,omitempty"`
	SecretMountPath                      string `json:"secret_mount_path,omitempty"`
	Model                                string `json:"model,omitempty"`
	OutputFormat                         string `json:"output_format"`
	WorkspacePath                        string `json:"workspace_path"`
	AgentHomePath                        string `json:"agent_home_path"`
	HostHostname                         string `json:"host_hostname"`
	NetnsName                            string `json:"netns_name"`
	HostGatewayIP                        string `json:"host_gateway_ip"`
	SandboxSourceIP                      string `json:"sandbox_source_ip"`
	BridgeDirPath                        string `json:"bridge_dir_path"`
	BundleDigest                         string `json:"bundle_digest"`
	RuntimeConfigDigest                  string `json:"runtime_config_digest"`
	SpecDigest                           string `json:"spec_digest"`
	EgressPolicyDigest                   string `json:"egress_policy_digest"`
	ManifestVersion                      int    `json:"manifest_version"`
	ClaudeCodeDisableNonessentialTraffic bool   `json:"claude_code_disable_nonessential_traffic"`
	ProxyBindURL                         string `json:"proxy_bind_url"`
}

type controlManifestFile struct {
	Payload controlManifest `json:"payload"`
	Digest  string          `json:"digest"`
}

type runtimeSpec struct {
	OCIVersion string `json:"ociVersion"`
	Process    struct {
		Terminal        bool     `json:"terminal"`
		User            specUser `json:"user"`
		Args            []string `json:"args"`
		Env             []string `json:"env"`
		Cwd             string   `json:"cwd"`
		Capabilities    any      `json:"capabilities,omitempty"`
		Rlimits         any      `json:"rlimits,omitempty"`
		NoNewPrivileges bool     `json:"noNewPrivileges"`
	} `json:"process"`
	Root     specRoot        `json:"root"`
	Hostname string          `json:"hostname"`
	Mounts   []specMount     `json:"mounts"`
	Linux    json.RawMessage `json:"linux"`
}

type specUser struct {
	UID int `json:"uid"`
	GID int `json:"gid"`
}

type specRoot struct {
	Path     string `json:"path"`
	Readonly bool   `json:"readonly"`
}

type specMount struct {
	Destination string            `json:"destination"`
	Type        string            `json:"type"`
	Source      string            `json:"source"`
	Options     []string          `json:"options,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type GenerationArtifacts struct {
	BundleDir               string
	SpecPath                string
	ManifestPath            string
	ManifestDigest          string
	ProjectedManifestDigest string
	BundleDigest            string
	RuntimeConfigDigest     string
	SpecDigest              string
	RunscVersion            string
	NetworkPrepared         bool
}

type Runtime struct {
	cfg        Config
	runner     CommandRunner
	mu         sync.RWMutex
	containers map[string]*Container
}

type Container struct {
	SessionID    string
	GenerationID string
	RestoreID    string
	Agent        string
	Cmd          *exec.Cmd
	Stdin        io.WriteCloser
	Stdout       io.ReadCloser
	Stderr       io.ReadCloser
	Cancel       context.CancelFunc
	InputMu      sync.Mutex
	OutputHub    *OutputHub // Per-container pub/sub for output events
}

func New(cfg Config) *Runtime {
	cfg.Claude = normalizeClaudeConfig(cfg.Claude)
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
	agent, err := resolveAgent(req.Agent, r.cfg.DefaultAgent)
	if err != nil {
		return Result{Err: err}
	}
	req.Agent = agent

	// Check if container already exists (hot path)
	r.mu.RLock()
	container, exists := r.containers[req.SessionID]
	r.mu.RUnlock()

	if exists {
		if !req.WaitForTurn {
			return Result{}
		}
		return r.sendMessage(ctx, container, req.FirstMessage, req.Done, output)
	}

	if req.RestoreFromCheckpoint {
		return r.resumeFromCheckpoint(ctx, req, output)
	}

	// Check if checkpoint exists (legacy resume path). This stays opt-in because
	// runsc restore currently cannot reliably reconnect the long-lived stdin
	// turn channel used by the agent entrypoint.
	if r.cfg.RestoreFromCheckpoint {
		if _, err := r.resolveCheckpointPath(req); err == nil {
			return r.resumeFromCheckpoint(ctx, req, output)
		}
	}

	// Fresh start (cold path)
	return r.startFresh(ctx, req, output)
}

func (r *Runtime) PrepareGeneration(ctx context.Context, req StartRequest) (GenerationArtifacts, error) {
	artifacts, err := r.renderGenerationArtifacts(ctx, req)
	if err != nil {
		return GenerationArtifacts{}, err
	}
	if err := r.ensureSandboxNetwork(ctx, req.Generation); err != nil {
		return GenerationArtifacts{}, err
	}
	artifacts.NetworkPrepared = true
	return artifacts, nil
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

func resolveAgent(agent, fallback string) (string, error) {
	agent = strings.TrimSpace(agent)
	if agent == "" {
		agent = strings.TrimSpace(fallback)
	}
	if agent == "" {
		return "", errors.New("agent is required")
	}
	if _, ok := agents.Lookup(agent); !ok {
		return "", fmt.Errorf("unsupported agent %q", agent)
	}
	return agent, nil
}

func scanLines(wg *sync.WaitGroup, r io.Reader, stream string, hub *OutputHub) {
	defer wg.Done()
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		hub.Publish(OutputEvent{Stream: stream, Line: scanner.Text()})
	}
}

func (r *Runtime) Destroy(ctx context.Context, restoreID string) error {
	if restoreID == "" {
		return errors.New("restore id is required")
	}
	if err := r.deleteRunscContainer(ctx, restoreID); err != nil {
		return fmt.Errorf("runsc delete %s: %w", restoreID, err)
	}
	return nil
}

func (r *Runtime) DestroyGenerationResources(ctx context.Context, details store.RuntimeGenerationDetails) (GenerationResourceCleanup, error) {
	var cleanup GenerationResourceCleanup
	if strings.TrimSpace(details.GenerationID) == "" {
		return cleanup, fmt.Errorf("generation id is required")
	}
	if !strings.EqualFold(strings.TrimSpace(r.runscNetwork(details)), "sandbox") {
		return cleanup, nil
	}
	if strings.TrimSpace(details.NetnsName) == "" || strings.TrimSpace(details.HostVeth) == "" {
		return cleanup, fmt.Errorf("sandbox resource cleanup requires netns and host veth")
	}

	var errs []error
	tableName := hostEgressTableName(details.GenerationID)
	if err := r.deleteNetworkResource(ctx, "nft", []string{"delete", "table", "inet", tableName}, true); err != nil {
		errs = append(errs, err)
	} else {
		cleanup.NftTableDeleted = true
	}
	if err := r.deleteNetworkResource(ctx, "ip", []string{"link", "delete", details.HostVeth}, true); err != nil {
		errs = append(errs, err)
	} else {
		cleanup.HostVethDeleted = true
	}
	if err := r.deleteNetworkResource(ctx, "ip", []string{"netns", "delete", details.NetnsName}, true); err != nil {
		errs = append(errs, err)
	} else {
		cleanup.NetnsDeleted = true
	}
	if len(errs) > 0 {
		return cleanup, errors.Join(errs...)
	}
	return cleanup, nil
}

func (r *Runtime) deleteNetworkResource(ctx context.Context, name string, args []string, missingOK bool) error {
	output, err := r.runner.CombinedOutput(ctx, name, args...)
	if err == nil {
		return nil
	}
	if missingOK && commandOutputContains(string(output), "cannot find device", "does not exist", "not found", "no such file", "no such process", "no such table") {
		return nil
	}
	return fmt.Errorf("destroy sandbox network resource %q: %w: %s", strings.Join(append([]string{name}, args...), " "), err, strings.TrimSpace(string(output)))
}

func (r *Runtime) deleteRunscContainer(ctx context.Context, restoreID string) error {
	kill := exec.CommandContext(ctx, "runsc", "-root", r.cfg.RunscRoot, "kill", restoreID, "KILL")
	deleteCmd := exec.CommandContext(ctx, "runsc", "-root", r.cfg.RunscRoot, "delete", restoreID)
	_ = kill.Run()
	return deleteCmd.Run()
}

func (r *Runtime) cleanupRunscContainer(ctx context.Context, restoreID string) {
	_ = r.deleteRunscContainer(ctx, restoreID)
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
		r.cleanupRunscContainer(context.Background(), container.RestoreID)
	}
}

func (r *Runtime) stopContainer(container *Container) {
	r.removeContainer(container)
	container.Cancel()
	r.cleanupRunscContainer(context.Background(), container.RestoreID)
}

func (r *Runtime) readRestoreMS(sessionID string) *int64 {
	data, err := os.ReadFile(filepath.Join(r.cfg.SessionsRoot, sessionID, "restore_ms.txt"))
	if err != nil {
		return nil
	}
	value, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return nil
	}
	return &value
}

func (r *Runtime) renderGenerationArtifacts(ctx context.Context, req StartRequest) (GenerationArtifacts, error) {
	details := req.Generation
	if strings.TrimSpace(details.GenerationID) == "" {
		return GenerationArtifacts{}, fmt.Errorf("generation details are required")
	}
	if err := validateGenerationDetails(req); err != nil {
		return GenerationArtifacts{}, err
	}
	if strings.TrimSpace(details.SpecPath) == "" || strings.TrimSpace(details.ControlManifestPath) == "" {
		return GenerationArtifacts{}, fmt.Errorf("generation resource paths are required")
	}
	if err := r.prepareGenerationDirs(req); err != nil {
		return GenerationArtifacts{}, err
	}
	if err := r.materializeSecrets(details); err != nil {
		return GenerationArtifacts{}, err
	}
	spec, specDigest, err := r.renderRuntimeSpec(req)
	if err != nil {
		return GenerationArtifacts{}, err
	}
	if err := writeJSONFileAtomic(details.SpecPath, spec, 0o644); err != nil {
		return GenerationArtifacts{}, fmt.Errorf("write runtime spec: %w", err)
	}
	runscVersion := r.runscVersion(ctx)
	bundleDigest := digestHex(mustCanonicalJSON(map[string]any{
		"bundle_dir":  filepath.Clean(details.BundleDirPath),
		"rootfs":      spec.Root.Path,
		"spec_digest": specDigest,
	}))
	runtimeConfigDigest := digestHex(mustCanonicalJSON(map[string]any{
		"runsc_network":  r.runscNetwork(details),
		"runsc_overlay2": r.runscOverlay2(details),
		"runsc_platform": details.RunscPlatform,
		"rootfs":         spec.Root.Path,
	}))
	manifest, err := r.buildGenerationManifest(req, runscVersion, bundleDigest, runtimeConfigDigest, specDigest)
	if err != nil {
		return GenerationArtifacts{}, err
	}
	manifestDigest, manifestFile, err := wrapControlManifest(manifest)
	if err != nil {
		return GenerationArtifacts{}, err
	}
	if err := writeJSONFileAtomic(details.ControlManifestPath, manifestFile, 0o644); err != nil {
		return GenerationArtifacts{}, fmt.Errorf("write control manifest: %w", err)
	}
	projectedManifestDigest, err := projectedControlManifestDigest(manifest)
	if err != nil {
		return GenerationArtifacts{}, err
	}
	return GenerationArtifacts{
		BundleDir:               details.BundleDirPath,
		SpecPath:                details.SpecPath,
		ManifestPath:            details.ControlManifestPath,
		ManifestDigest:          manifestDigest,
		ProjectedManifestDigest: projectedManifestDigest,
		BundleDigest:            bundleDigest,
		RuntimeConfigDigest:     runtimeConfigDigest,
		SpecDigest:              specDigest,
		RunscVersion:            runscVersion,
	}, nil
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
	if strings.TrimSpace(details.SecretsDirPath) != "" {
		if err := ensureSecretDir(details.SecretsDirPath, r.cfg.SecretReadersGID); err != nil {
			return fmt.Errorf("create generation secrets dir: %w", err)
		}
	}
	return r.prepareSessionDirs(req.SessionID)
}

func (r *Runtime) materializeSecrets(details store.RuntimeGenerationDetails) error {
	if !details.RequiresSecretDrop {
		if strings.TrimSpace(details.SecretsDirPath) != "" {
			return fmt.Errorf("shell generation must not carry a secrets dir")
		}
		return nil
	}
	if strings.TrimSpace(details.SecretsDirPath) == "" {
		return fmt.Errorf("secret-backed generation requires secrets dir")
	}
	if strings.TrimSpace(r.cfg.SecretsRoot) == "" {
		return fmt.Errorf("secret-backed generation requires secrets root")
	}
	if r.cfg.SecretReadersGID <= 0 {
		return fmt.Errorf("secret-backed generation requires secret readers gid")
	}
	if err := ensureSecretDir(details.SecretsDirPath, r.cfg.SecretReadersGID); err != nil {
		return fmt.Errorf("create materialized secrets root: %w", err)
	}
	secrets := map[string]string{
		details.AnthropicAPIKeySecretID:    r.cfg.Claude.APIKey,
		details.AnthropicAuthTokenSecretID: r.cfg.Claude.AuthToken,
	}
	for secretID, fallbackValue := range secrets {
		if strings.TrimSpace(secretID) == "" || strings.TrimSpace(details.SecretVersion) == "" {
			return fmt.Errorf("secret-backed generation requires secret id and version")
		}
		src := filepath.Join(r.cfg.SecretsRoot, secretID, details.SecretVersion)
		if err := publishLocalSecretVersion(src, fallbackValue, r.cfg.SecretReadersGID); err != nil {
			return err
		}
		dst := filepath.Join(details.SecretsDirPath, secretID, details.SecretVersion)
		if err := materializeSecretVersion(src, dst, r.cfg.SecretReadersGID); err != nil {
			return err
		}
	}
	return nil
}

func publishLocalSecretVersion(path, value string, readersGID int) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("secret %s is missing and no publish value was configured", path)
	}
	if readersGID <= 0 {
		return fmt.Errorf("secret readers gid must be > 0")
	}
	if err := ensureSecretDir(filepath.Dir(path), readersGID); err != nil {
		return fmt.Errorf("create secret dir: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o440)
	if err != nil {
		if os.IsExist(err) {
			return waitForSecretVersion(path, readersGID, secretPublishValidationWait)
		}
		return fmt.Errorf("publish secret version: %w", err)
	}
	if _, err := file.WriteString(value); err != nil {
		_ = file.Close()
		return fmt.Errorf("write secret version: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync secret version: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close secret version: %w", err)
	}
	if err := chownPathIfNeeded(path, readersGID); err != nil {
		return fmt.Errorf("chown secret version: %w", err)
	}
	if err := os.Chmod(path, 0o440); err != nil {
		return fmt.Errorf("chmod secret version: %w", err)
	}
	return validateSecretVersion(path, readersGID)
}

func materializeSecretVersion(src, dst string, readersGID int) error {
	info, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("stat secret version %s: %w", src, err)
	}
	if info.IsDir() {
		return fmt.Errorf("secret version %s is a directory", src)
	}
	if err := validateSecretVersion(src, readersGID); err != nil {
		return err
	}
	if err := ensureSecretDir(filepath.Dir(dst), readersGID); err != nil {
		return fmt.Errorf("create materialized secret dir: %w", err)
	}
	if err := os.Link(src, dst); err == nil {
		return validateSecretVersion(dst, readersGID)
	} else if os.IsExist(err) {
		return waitForSecretVersion(dst, readersGID, secretPublishValidationWait)
	}
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open secret version: %w", err)
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o440)
	if err != nil {
		if os.IsExist(err) {
			return waitForSecretVersion(dst, readersGID, secretPublishValidationWait)
		}
		return fmt.Errorf("create materialized secret: %w", err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return fmt.Errorf("copy materialized secret: %w", err)
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return fmt.Errorf("sync materialized secret: %w", err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close materialized secret: %w", err)
	}
	if err := chownPathIfNeeded(dst, readersGID); err != nil {
		return fmt.Errorf("chown materialized secret: %w", err)
	}
	if err := os.Chmod(dst, 0o440); err != nil {
		return fmt.Errorf("chmod materialized secret: %w", err)
	}
	return validateSecretVersion(dst, readersGID)
}

func waitForSecretVersion(path string, readersGID int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		if err := validateSecretVersion(path, readersGID); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			return lastErr
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func ensureSecretDir(path string, readersGID int) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("secret dir path is required")
	}
	if err := os.MkdirAll(path, 0o750); err != nil {
		return err
	}
	current := filepath.Clean(path)
	var dirs []string
	for {
		dirs = append(dirs, current)
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		info, err := os.Stat(parent)
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0o750 {
			break
		}
		current = parent
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := chownPathIfNeeded(dirs[i], readersGID); err != nil {
			return err
		}
		if err := os.Chmod(dirs[i], 0o750); err != nil {
			return err
		}
	}
	return nil
}

func chownPathIfNeeded(path string, readersGID int) error {
	if readersGID <= 0 {
		return nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("stat ownership unavailable for %s", path)
	}
	if int(stat.Gid) == readersGID {
		return nil
	}
	return os.Chown(path, int(stat.Uid), readersGID)
}

func validateSecretVersion(path string, readersGID int) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat secret version %s: %w", path, err)
	}
	if info.IsDir() {
		return fmt.Errorf("secret version %s is a directory", path)
	}
	if mode := info.Mode().Perm(); mode != 0o440 {
		return fmt.Errorf("secret version %s must have mode 0440, got %04o", path, mode)
	}
	if readersGID > 0 {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("stat ownership unavailable for %s", path)
		}
		if int(stat.Gid) != readersGID {
			return fmt.Errorf("secret version %s must have group %d, got %d", path, readersGID, stat.Gid)
		}
	}
	return nil
}

func (r *Runtime) buildGenerationManifest(req StartRequest, runscVersion, bundleDigest, runtimeConfigDigest, specDigest string) (controlManifest, error) {
	details := req.Generation
	hostname, err := os.Hostname()
	if err != nil {
		return controlManifest{}, err
	}
	agentHomePath := filepath.Join("/agent-homes", req.SessionID)
	workspacePath := filepath.Join("/sessions", req.SessionID)
	secretMountPath := ""
	if details.RequiresSecretDrop {
		secretMountPath = "/harness-secrets"
	}
	sandboxSourceIP, err := sandboxSourceIP(details.SandboxIPCIDR)
	if err != nil {
		return controlManifest{}, err
	}
	return controlManifest{
		SessionID:                            req.SessionID,
		GenerationID:                         details.GenerationID,
		CreatedAt:                            time.Now().UTC().Format(time.RFC3339Nano),
		AttemptID:                            "attempt-0",
		NetworkProfileID:                     details.NetworkProfileID,
		AgentRuntimeProfileID:                details.AgentRuntimeProfileID,
		Agent:                                req.Agent,
		ClaudeSessionUUID:                    req.ClaudeSessionUUID,
		ResumeClaude:                         req.ResumeClaude,
		RunscPlatform:                        defaultString(details.RunscPlatform, "systrap"),
		RunscVersion:                         runscVersion,
		AnthropicBaseURL:                     details.ManifestAnthropicBaseURL,
		AnthropicAPIKeySecretID:              details.AnthropicAPIKeySecretID,
		AnthropicAuthTokenSecretID:           details.AnthropicAuthTokenSecretID,
		SecretVersion:                        details.SecretVersion,
		SecretMountPath:                      secretMountPath,
		Model:                                details.Model,
		OutputFormat:                         details.OutputFormat,
		WorkspacePath:                        workspacePath,
		AgentHomePath:                        agentHomePath,
		HostHostname:                         hostname,
		NetnsName:                            details.NetnsName,
		HostGatewayIP:                        details.HostGatewayIP,
		SandboxSourceIP:                      sandboxSourceIP,
		BridgeDirPath:                        details.BridgeDirPath,
		BundleDigest:                         bundleDigest,
		RuntimeConfigDigest:                  runtimeConfigDigest,
		SpecDigest:                           specDigest,
		EgressPolicyDigest:                   details.EgressPolicyDigest,
		ManifestVersion:                      1,
		ClaudeCodeDisableNonessentialTraffic: details.DisableNonessentialTraffic,
		ProxyBindURL:                         r.cfg.Claude.ProxyBindURL,
	}, nil
}

func sandboxSourceIP(sandboxCIDR string) (string, error) {
	if strings.TrimSpace(sandboxCIDR) == "" {
		return "", nil
	}
	prefix, err := netip.ParsePrefix(strings.TrimSpace(sandboxCIDR))
	if err != nil {
		return "", fmt.Errorf("invalid sandbox ip cidr %q: %w", sandboxCIDR, err)
	}
	return prefix.Addr().String(), nil
}

func validateGenerationDetails(req StartRequest) error {
	details := req.Generation
	if strings.TrimSpace(details.SessionID) != "" && strings.TrimSpace(req.SessionID) != "" && details.SessionID != req.SessionID {
		return fmt.Errorf("generation session mismatch")
	}
	if strings.TrimSpace(req.GenerationID) != "" && req.GenerationID != details.GenerationID {
		return fmt.Errorf("generation id mismatch")
	}
	if strings.TrimSpace(details.Agent) != "" && strings.TrimSpace(req.Agent) != "" && details.Agent != req.Agent {
		return fmt.Errorf("generation agent mismatch")
	}
	if !details.RequiresSecretDrop {
		if strings.TrimSpace(details.SecretsDirPath) != "" ||
			strings.TrimSpace(details.AnthropicAPIKeySecretID) != "" ||
			strings.TrimSpace(details.AnthropicAuthTokenSecretID) != "" ||
			strings.TrimSpace(details.SecretVersion) != "" {
			return fmt.Errorf("shell_secret_disallowed")
		}
		return nil
	}
	if strings.TrimSpace(details.SecretsDirPath) == "" ||
		strings.TrimSpace(details.AnthropicAPIKeySecretID) == "" ||
		strings.TrimSpace(details.AnthropicAuthTokenSecretID) == "" ||
		strings.TrimSpace(details.SecretVersion) == "" {
		return fmt.Errorf("secret-backed generation requires secret references")
	}
	return nil
}

func wrapControlManifest(manifest controlManifest) (string, controlManifestFile, error) {
	payloadBytes, err := canonicalJSON(manifest)
	if err != nil {
		return "", controlManifestFile{}, err
	}
	digest := digestHex(payloadBytes)
	return digest, controlManifestFile{Payload: manifest, Digest: digest}, nil
}

func projectedControlManifestDigest(manifest controlManifest) (string, error) {
	data, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		return "", err
	}
	strictFields := map[string]struct{}{
		"session_id":                     {},
		"generation_id":                  {},
		"network_profile_id":             {},
		"agent_runtime_profile_id":       {},
		"agent":                          {},
		"claude_session_uuid":            {},
		"resume_claude":                  {},
		"runsc_platform":                 {},
		"runsc_version":                  {},
		"anthropic_base_url":             {},
		"anthropic_api_key_secret_id":    {},
		"anthropic_auth_token_secret_id": {},
		"secret_version":                 {},
		"secret_mount_path":              {},
		"model":                          {},
		"output_format":                  {},
		"workspace_path":                 {},
		"agent_home_path":                {},
		"bundle_digest":                  {},
		"runtime_config_digest":          {},
		"spec_digest":                    {},
		"egress_policy_digest":           {},
		"manifest_version":               {},
		"claude_code_disable_nonessential_traffic": {},
		"proxy_bind_url": {},
	}
	regenerableFields := map[string]struct{}{
		"created_at":        {},
		"attempt_id":        {},
		"host_hostname":     {},
		"netns_name":        {},
		"host_gateway_ip":   {},
		"sandbox_source_ip": {},
		"bridge_dir_path":   {},
	}
	projected := map[string]any{}
	for key, value := range fields {
		if _, ok := regenerableFields[key]; ok {
			continue
		}
		if _, ok := strictFields[key]; !ok {
			return "", fmt.Errorf("unclassified control manifest field %q", key)
		}
		projected[key] = value
	}
	payloadBytes, err := canonicalJSON(projected)
	if err != nil {
		return "", err
	}
	return digestHex(payloadBytes), nil
}

func (r *Runtime) renderRuntimeSpec(req StartRequest) (runtimeSpec, string, error) {
	var spec runtimeSpec
	details := req.Generation
	spec.OCIVersion = "1.0.2"
	spec.Process.Terminal = false
	spec.Process.User = specUser{UID: 0, GID: 0}
	spec.Process.Args = []string{"/usr/local/bin/harness-agent-entrypoint"}
	spec.Process.Env = []string{
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"LANG=C.UTF-8",
		"MPLCONFIGDIR=/tmp/matplotlib",
	}
	spec.Process.Cwd = "/"
	spec.Process.Capabilities = map[string]any{
		"bounding":    []string{"CAP_AUDIT_WRITE", "CAP_CHOWN", "CAP_KILL", "CAP_NET_BIND_SERVICE", "CAP_SETGID", "CAP_SETUID"},
		"effective":   []string{"CAP_AUDIT_WRITE", "CAP_CHOWN", "CAP_KILL", "CAP_NET_BIND_SERVICE", "CAP_SETGID", "CAP_SETUID"},
		"inheritable": []string{},
		"permitted":   []string{"CAP_AUDIT_WRITE", "CAP_CHOWN", "CAP_KILL", "CAP_NET_BIND_SERVICE", "CAP_SETGID", "CAP_SETUID"},
		"ambient":     []string{},
	}
	spec.Process.Rlimits = []map[string]any{{"type": "RLIMIT_NOFILE", "hard": 1024, "soft": 1024}}
	spec.Process.NoNewPrivileges = true
	spec.Root = specRoot{Path: r.rootFSPath(), Readonly: false}
	spec.Hostname = "harness-gen-" + shortID(details.GenerationID)
	spec.Process.Env = append(spec.Process.Env,
		"HARNESS_EXPECTED_SESSION_ID="+req.SessionID,
		"HARNESS_EXPECTED_GENERATION_ID="+details.GenerationID,
		"HARNESS_EXPECTED_NETWORK_PROFILE_ID="+details.NetworkProfileID,
		"HARNESS_EXPECTED_AGENT_RUNTIME_PROFILE_ID="+details.AgentRuntimeProfileID,
		"HARNESS_EXPECTED_MANIFEST_VERSION=1",
		"HARNESS_EXPECTED_API_KEY_SECRET_ID="+details.AnthropicAPIKeySecretID,
		"HARNESS_EXPECTED_AUTH_TOKEN_SECRET_ID="+details.AnthropicAuthTokenSecretID,
		"HARNESS_EXPECTED_SECRET_VERSION="+details.SecretVersion,
		fmt.Sprintf("HARNESS_SECRET_READERS_GID=%d", r.cfg.SecretReadersGID),
		"HARNESS_BRIDGE_DIR="+bridge.BridgeMountDestination,
		"HARNESS_BRIDGE_MODE="+defaultString(r.cfg.BridgeMode, "claim-loop"),
		"HARNESS_BRIDGE_HEARTBEAT_INTERVAL="+formatSeconds(defaultDuration(r.cfg.BridgeHeartbeat, 30*time.Second)),
		"HARNESS_BRIDGE_POLL_INTERVAL="+formatSeconds(defaultDuration(r.cfg.BridgePollInterval, 5*time.Millisecond)),
		"HARNESS_BRIDGE_IDLE_INTERVAL="+formatSeconds(defaultDuration(r.cfg.BridgePollInterval, 5*time.Millisecond)),
		"HARNESS_PROBE_HEALTHZ_STATUSES="+joinInts(defaultIntSlice(r.cfg.ProbeHealthzStatuses, []int{200})),
		"HARNESS_PROBE_MESSAGE_STATUSES="+joinInts(defaultIntSlice(r.cfg.ProbeMessageStatuses, []int{400})),
	)
	spec.Mounts = []specMount{
		{Destination: "/proc", Type: "proc", Source: "proc"},
		{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
		{Destination: "/dev/pts", Type: "devpts", Source: "devpts", Options: []string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"}},
		{Destination: "/dev/shm", Type: "tmpfs", Source: "shm", Options: []string{"nosuid", "noexec", "nodev", "mode=1777", "size=65536k"}},
		{Destination: "/dev/mqueue", Type: "mqueue", Source: "mqueue", Options: []string{"nosuid", "noexec", "nodev"}},
		{Destination: "/sys", Type: "sysfs", Source: "sysfs", Options: []string{"nosuid", "noexec", "nodev", "ro"}},
		{Destination: "/sessions", Type: "bind", Source: r.cfg.SessionsRoot, Options: []string{"rbind", "rw"}},
		{Destination: "/agent-homes", Type: "bind", Source: r.cfg.AgentHomesRoot, Options: []string{"rbind", "rw"}},
		{Destination: "/harness-control", Type: "bind", Source: details.ControlDirPath, Options: []string{"rbind", "ro"}},
		{
			Destination: bridge.BridgeMountDestination,
			Type:        "bind",
			Source:      details.BridgeDirPath,
			Options:     []string{"rbind", "rw"},
			Annotations: map[string]string{
				"dev.gvisor.spec.mount./harness-control/bridge.type":  "bind",
				"dev.gvisor.spec.mount./harness-control/bridge.share": "exclusive",
			},
		},
	}
	if schemaPack := r.schemaPackPath(); schemaPack != "" {
		spec.Mounts = append(spec.Mounts, specMount{Destination: "/schema-pack", Type: "bind", Source: schemaPack, Options: []string{"rbind", "ro"}})
	}
	if details.RequiresSecretDrop {
		spec.Mounts = append(spec.Mounts, specMount{
			Destination: "/harness-secrets",
			Type:        "bind",
			Source:      details.SecretsDirPath,
			Options:     []string{"rbind", "ro", "nosuid", "nodev", "noexec"},
		})
	}
	linux := map[string]any{
		"resources": map[string]any{
			"memory": map[string]any{"limit": 1073741824},
			"cpu":    map[string]any{"shares": 1024},
			"pids":   map[string]any{"limit": 256},
		},
		"namespaces": []map[string]any{
			{"type": "pid"},
			{"type": "ipc"},
			{"type": "uts"},
			{"type": "mount"},
		},
	}
	if strings.EqualFold(strings.TrimSpace(r.runscNetwork(details)), "sandbox") {
		if strings.TrimSpace(details.NetnsPath) == "" {
			return runtimeSpec{}, "", fmt.Errorf("sandbox generation requires netns path")
		}
		linux["namespaces"] = append(linux["namespaces"].([]map[string]any), map[string]any{"type": "network", "path": details.NetnsPath})
	}
	linuxBytes, err := canonicalJSON(linux)
	if err != nil {
		return runtimeSpec{}, "", err
	}
	spec.Linux = linuxBytes
	payload, err := canonicalJSON(spec)
	if err != nil {
		return runtimeSpec{}, "", err
	}
	return spec, digestHex(payload), nil
}

func (r *Runtime) rootFSPath() string {
	if strings.TrimSpace(r.cfg.RootFSPath) != "" {
		return strings.TrimSpace(r.cfg.RootFSPath)
	}
	return filepath.Join(r.repoRoot(), "sandbox-image", "rootfs")
}

func (r *Runtime) schemaPackPath() string {
	path := filepath.Join(r.repoRoot(), "schema-pack")
	if _, err := os.Stat(path); err == nil {
		return path
	}
	return ""
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
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
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

func mustCanonicalJSON(value any) []byte {
	data, err := canonicalJSON(value)
	if err != nil {
		return []byte("{}")
	}
	return data
}

func digestHex(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:])
}

func shortID(id string) string {
	token := strings.NewReplacer("gen_", "", "-", "").Replace(id)
	if len(token) > 12 {
		return token[:12]
	}
	if token == "" {
		return "unknown"
	}
	return token
}

func joinInts(values []int) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.Itoa(value))
	}
	return strings.Join(parts, ",")
}

func defaultDuration(value, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return fallback
}

func formatSeconds(value time.Duration) string {
	if value%time.Second == 0 {
		return strconv.FormatInt(int64(value/time.Second), 10)
	}
	return strconv.FormatFloat(float64(value)/float64(time.Second), 'f', -1, 64)
}

func (r *Runtime) runscVersion(ctx context.Context) string {
	out, err := r.runner.CombinedOutput(ctx, "runsc", "--version")
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(strings.Join(strings.Fields(string(out)), " "))
}

func normalizeClaudeConfig(cfg ClaudeConfig) ClaudeConfig {
	empty := cfg == ClaudeConfig{}
	cfg.ProxyBindURL = defaultString(cfg.ProxyBindURL, "http://0.0.0.0:8082")
	cfg.APIKey = defaultString(cfg.APIKey, "123")
	cfg.AuthToken = defaultString(cfg.AuthToken, cfg.APIKey)
	cfg.Model = defaultString(cfg.Model, "sonnet")
	cfg.OutputFormat = defaultString(cfg.OutputFormat, "stream-json")
	if empty {
		cfg.DisableNonessentialTraffic = true
	}
	return cfg
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func (r *Runtime) runscNetwork(details store.RuntimeGenerationDetails) string {
	return defaultString(details.RunscNetwork, r.cfg.RunscNetwork)
}

func (r *Runtime) runscOverlay2(details store.RuntimeGenerationDetails) string {
	return defaultString(details.RunscOverlay2, r.cfg.RunscOverlay2)
}

func (r *Runtime) prepareSessionDirs(sessionID string) error {
	if r.cfg.SessionsRoot != "" {
		if err := os.MkdirAll(filepath.Join(r.cfg.SessionsRoot, sessionID), 0o755); err != nil {
			return fmt.Errorf("create session workspace: %w", err)
		}
	}
	if r.cfg.AgentHomesRoot != "" {
		if err := os.MkdirAll(filepath.Join(r.cfg.AgentHomesRoot, sessionID), 0o755); err != nil {
			return fmt.Errorf("create agent home: %w", err)
		}
	}
	return nil
}

func (r *Runtime) ensureSandboxNetwork(ctx context.Context, details store.RuntimeGenerationDetails) error {
	if !strings.EqualFold(strings.TrimSpace(r.runscNetwork(details)), "sandbox") {
		return nil
	}
	if strings.TrimSpace(details.NetnsName) == "" ||
		strings.TrimSpace(details.HostVeth) == "" ||
		strings.TrimSpace(details.SandboxVeth) == "" ||
		strings.TrimSpace(details.HostSideCIDR) == "" ||
		strings.TrimSpace(details.SandboxIPCIDR) == "" ||
		strings.TrimSpace(details.HostGatewayIP) == "" ||
		strings.TrimSpace(details.ProbeURL) == "" {
		return fmt.Errorf("sandbox network allocation is incomplete")
	}
	hostGatewayCIDR, err := hostGatewayCIDR(details)
	if err != nil {
		return err
	}

	commands := [][]string{
		{"ip", "netns", "add", details.NetnsName},
		{"ip", "link", "delete", details.HostVeth},
		{"ip", "netns", "exec", details.NetnsName, "ip", "link", "delete", details.SandboxVeth},
		{"ip", "link", "add", details.HostVeth, "type", "veth", "peer", "name", details.SandboxVeth},
		{"ip", "link", "set", details.SandboxVeth, "netns", details.NetnsName},
		{"ip", "addr", "replace", hostGatewayCIDR, "dev", details.HostVeth},
		{"ip", "link", "set", details.HostVeth, "up"},
		{"ip", "netns", "exec", details.NetnsName, "ip", "addr", "replace", details.SandboxIPCIDR, "dev", details.SandboxVeth},
		{"ip", "netns", "exec", details.NetnsName, "ip", "link", "set", "lo", "up"},
		{"ip", "netns", "exec", details.NetnsName, "ip", "link", "set", details.SandboxVeth, "up"},
		{"ip", "netns", "exec", details.NetnsName, "ip", "route", "replace", "default", "via", details.HostGatewayIP, "dev", details.SandboxVeth},
	}
	for _, args := range commands {
		output, err := r.runner.CombinedOutput(ctx, args[0], args[1:]...)
		if err != nil {
			if ignoreSandboxNetworkCommandError(args, string(output), err) {
				continue
			}
			return fmt.Errorf("configure sandbox network %q: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
		}
	}
	if err := r.applySandboxEgressPolicy(ctx, details); err != nil {
		return err
	}
	if err := r.applyHostEgressPolicy(ctx, details); err != nil {
		return err
	}
	if err := r.probeSandboxNetwork(ctx, details); err != nil {
		return err
	}
	return nil
}

func hostGatewayCIDR(details store.RuntimeGenerationDetails) (string, error) {
	_, suffix, ok := strings.Cut(strings.TrimSpace(details.HostSideCIDR), "/")
	if !ok || strings.TrimSpace(suffix) == "" {
		return "", fmt.Errorf("invalid host side cidr %q", details.HostSideCIDR)
	}
	return strings.TrimSpace(details.HostGatewayIP) + "/" + strings.TrimSpace(suffix), nil
}

func ignoreSandboxNetworkCommandError(args []string, output string, err error) bool {
	if len(args) >= 4 && args[0] == "ip" && args[1] == "netns" && args[2] == "add" {
		return commandOutputContains(output, "file exists", "already exists")
	}
	if len(args) >= 4 && args[0] == "ip" && args[1] == "link" && args[2] == "delete" {
		return commandOutputContains(output, "cannot find device", "does not exist", "not found")
	}
	if len(args) >= 8 && args[0] == "ip" && args[1] == "netns" && args[2] == "exec" && args[4] == "ip" && args[5] == "link" && args[6] == "delete" {
		return commandOutputContains(output, "cannot find device", "does not exist", "not found")
	}
	return false
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

func (r *Runtime) applySandboxEgressPolicy(ctx context.Context, details store.RuntimeGenerationDetails) error {
	rules, err := parseAllowedEgressRules(details.AllowedEgressRules)
	if err != nil {
		return err
	}
	const tableName = "harness_egress"
	base := []string{"netns", "exec", details.NetnsName, "nft"}
	if _, err := r.runner.CombinedOutput(ctx, "ip", append(base, "list", "table", "inet", tableName)...); err == nil {
		if err := r.runNetworkCommand(ctx, "ip", append(base, "delete", "table", "inet", tableName)...); err != nil {
			return err
		}
	}
	if err := r.runNetworkCommand(ctx, "ip", append(base, "add", "table", "inet", tableName)...); err != nil {
		return err
	}
	if err := r.runNetworkCommand(ctx, "ip", append(base, "add", "chain", "inet", tableName, "output", "{", "type", "filter", "hook", "output", "priority", "0", ";", "policy", "drop", ";", "}")...); err != nil {
		return err
	}
	if err := r.runNetworkCommand(ctx, "ip", append(base, "add", "rule", "inet", tableName, "output", "oifname", "lo", "accept")...); err != nil {
		return err
	}
	for _, rule := range rules {
		args := append([]string{}, base...)
		args = append(args, "add", "rule", "inet", tableName, "output")
		if rule.Host != "" {
			args = append(args, "ip", "daddr", rule.Host)
		}
		args = append(args, rule.Proto, "dport", strconv.Itoa(rule.Port), "accept")
		if err := r.runNetworkCommand(ctx, "ip", args...); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) applyHostEgressPolicy(ctx context.Context, details store.RuntimeGenerationDetails) error {
	rules, err := parseAllowedEgressRules(details.AllowedEgressRules)
	if err != nil {
		return err
	}
	if err := r.runNetworkCommand(ctx, "sysctl", "-w", "net.ipv4.ip_forward=1"); err != nil {
		return err
	}
	tableName := hostEgressTableName(details.GenerationID)
	if _, err := r.runner.CombinedOutput(ctx, "nft", "list", "table", "inet", tableName); err == nil {
		if err := r.runNetworkCommand(ctx, "nft", "delete", "table", "inet", tableName); err != nil {
			return err
		}
	}
	if err := r.runNetworkCommand(ctx, "nft", "add", "table", "inet", tableName); err != nil {
		return err
	}
	if err := r.runNetworkCommand(ctx, "nft", "add", "chain", "inet", tableName, "forward", "{", "type", "filter", "hook", "forward", "priority", "0", ";", "policy", "accept", ";", "}"); err != nil {
		return err
	}
	if err := r.runNetworkCommand(ctx, "nft", "add", "chain", "inet", tableName, "postrouting", "{", "type", "nat", "hook", "postrouting", "priority", "100", ";", "policy", "accept", ";", "}"); err != nil {
		return err
	}
	for _, rule := range rules {
		if rule.Host == details.HostGatewayIP {
			continue
		}
		args := []string{"add", "rule", "inet", tableName, "forward", "iifname", details.HostVeth}
		if rule.Host != "" {
			args = append(args, "ip", "daddr", rule.Host)
		}
		args = append(args, rule.Proto, "dport", strconv.Itoa(rule.Port), "accept")
		if err := r.runNetworkCommand(ctx, "nft", args...); err != nil {
			return err
		}
	}
	if err := r.runNetworkCommand(ctx, "nft", "add", "rule", "inet", tableName, "forward", "oifname", details.HostVeth, "ct", "state", "established,related", "accept"); err != nil {
		return err
	}
	if err := r.runNetworkCommand(ctx, "nft", "add", "rule", "inet", tableName, "forward", "iifname", details.HostVeth, "drop"); err != nil {
		return err
	}
	if err := r.runNetworkCommand(ctx, "nft", "add", "rule", "inet", tableName, "postrouting", "ip", "saddr", details.HostSideCIDR, "masquerade"); err != nil {
		return err
	}
	return nil
}

func nftIdentifier(value string) string {
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	out := b.String()
	if out == "" {
		return "unknown"
	}
	return out
}

func hostEgressTableName(generationID string) string {
	return "harness_gen_" + nftIdentifier(generationID)
}

func (r *Runtime) runNetworkCommand(ctx context.Context, name string, args ...string) error {
	output, err := r.runner.CombinedOutput(ctx, name, args...)
	if err != nil {
		return fmt.Errorf("configure sandbox network %q: %w: %s", strings.Join(append([]string{name}, args...), " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (r *Runtime) probeSandboxNetwork(ctx context.Context, details store.RuntimeGenerationDetails) error {
	attempts := r.cfg.PreStartProbeAttempts
	if attempts <= 0 {
		attempts = 3
	}
	interval := r.cfg.PreStartProbeInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := r.runSandboxNetworkProbeOnce(ctx, details); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt == attempts {
			break
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func (r *Runtime) runSandboxNetworkProbeOnce(ctx context.Context, details store.RuntimeGenerationDetails) error {
	baseURL := strings.TrimRight(details.ProbeURL, "/")
	probes := []struct {
		args           []string
		acceptStatuses []int
	}{
		{
			args:           []string{"netns", "exec", details.NetnsName, "curl", "-sS", "--max-time", "2", "-o", "/dev/null", "-w", "%{http_code}", baseURL + "/healthz"},
			acceptStatuses: defaultIntSlice(r.cfg.ProbeHealthzStatuses, []int{200}),
		},
		{
			args:           []string{"netns", "exec", details.NetnsName, "curl", "-sS", "--max-time", "2", "-o", "/dev/null", "-w", "%{http_code}", "-X", "POST", "-H", "content-type: application/json", "-H", "x-api-key: " + r.cfg.Claude.APIKey, "--data", "{}", baseURL + "/v1/messages"},
			acceptStatuses: defaultIntSlice(r.cfg.ProbeMessageStatuses, []int{400}),
		},
	}
	for _, probe := range probes {
		output, err := r.runner.CombinedOutput(ctx, "ip", probe.args...)
		if err != nil {
			return fmt.Errorf("pre-start sandbox network probe %q: %w: %s", strings.Join(append([]string{"ip"}, probe.args...), " "), err, strings.TrimSpace(string(output)))
		}
		status, err := strconv.Atoi(strings.TrimSpace(string(output)))
		if err != nil {
			return fmt.Errorf("pre-start sandbox network probe %q: invalid status %s", strings.Join(append([]string{"ip"}, probe.args...), " "), strings.TrimSpace(string(output)))
		}
		if !statusAccepted(status, probe.acceptStatuses) {
			return fmt.Errorf("pre-start sandbox network probe %q: unexpected status %d", strings.Join(append([]string{"ip"}, probe.args...), " "), status)
		}
	}
	return nil
}

func defaultIntSlice(values, fallback []int) []int {
	if len(values) == 0 {
		return fallback
	}
	return values
}

func statusAccepted(status int, accepted []int) bool {
	for _, value := range accepted {
		if status == value {
			return true
		}
	}
	return false
}

type egressRule struct {
	Proto string
	Host  string
	Port  int
}

func parseAllowedEgressRules(raw string) ([]egressRule, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var values []string
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return nil, fmt.Errorf("parse egress rules: %w", err)
	}
	rules := make([]egressRule, 0, len(values))
	for _, value := range values {
		rule, err := parseAllowedEgressRule(value)
		if err != nil {
			return nil, err
		}
		rules = append(rules, rule)
	}
	return rules, nil
}

func parseAllowedEgressRule(value string) (egressRule, error) {
	value = strings.TrimSpace(value)
	parts := strings.Split(value, ":")
	if len(parts) == 2 {
		port, err := strconv.Atoi(parts[1])
		if err != nil || port <= 0 || port > 65535 {
			return egressRule{}, fmt.Errorf("invalid egress rule %q", value)
		}
		if parts[0] != "tcp" && parts[0] != "udp" {
			return egressRule{}, fmt.Errorf("invalid egress rule %q", value)
		}
		return egressRule{Proto: parts[0], Port: port}, nil
	}
	if len(parts) != 3 {
		return egressRule{}, fmt.Errorf("invalid egress rule %q", value)
	}
	port, err := strconv.Atoi(parts[2])
	if err != nil || port <= 0 || port > 65535 {
		return egressRule{}, fmt.Errorf("invalid egress rule %q", value)
	}
	if parts[0] != "tcp" && parts[0] != "udp" {
		return egressRule{}, fmt.Errorf("invalid egress rule %q", value)
	}
	if strings.TrimSpace(parts[1]) == "" {
		return egressRule{}, fmt.Errorf("invalid egress rule %q", value)
	}
	return egressRule{Proto: parts[0], Host: parts[1], Port: port}, nil
}

type claudeInputFrame struct {
	Type    string             `json:"type"`
	Message claudeInputMessage `json:"message"`
}

type claudeInputMessage struct {
	Role    string               `json:"role"`
	Content []claudeContentBlock `json:"content"`
}

type claudeContentBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type shellInputFrame struct {
	Type    string `json:"type"`
	Content string `json:"content,omitempty"`
}

// writeUserTurn delivers a user message to the agent's stdin.
//
// Claude Code runs with `--input-format stream-json`, which expects one JSONL
// frame per turn and keeps stdin open between turns. Shell runs a JSON turn
// protocol over a PTY-backed shim. The server holds the session in
// running_active until the current turn's parser sees a completion event.
func writeUserTurn(stdin io.Writer, agent, message string) error {
	def, ok := agents.Lookup(agent)
	if !ok {
		return fmt.Errorf("unsupported agent %q", agent)
	}
	if def.Protocol == agents.ProtocolClaudeStreamJSON {
		frame := claudeInputFrame{
			Type: "user",
			Message: claudeInputMessage{
				Role: "user",
				Content: []claudeContentBlock{
					{Type: "text", Text: message},
				},
			},
		}
		encoded, err := json.Marshal(frame)
		if err != nil {
			return fmt.Errorf("encode user turn: %w", err)
		}
		if _, err := stdin.Write(append(encoded, '\n')); err != nil {
			return err
		}
		return nil
	}
	if def.Protocol == agents.ProtocolShellPTY {
		frame := shellInputFrame{
			Type:    "turn",
			Content: message,
		}
		encoded, err := json.Marshal(frame)
		if err != nil {
			return fmt.Errorf("encode shell turn: %w", err)
		}
		if _, err := stdin.Write(append(encoded, '\n')); err != nil {
			return err
		}
		return nil
	}
	_, err := fmt.Fprintln(stdin, message)
	return err
}

func writeInterrupt(stdin io.Writer, agent string) error {
	def, ok := agents.Lookup(agent)
	if !ok {
		return fmt.Errorf("unsupported agent %q", agent)
	}
	if def.Protocol != agents.ProtocolShellPTY {
		return fmt.Errorf("interrupt not supported for agent %q", agent)
	}
	frame := shellInputFrame{Type: "interrupt"}
	encoded, err := json.Marshal(frame)
	if err != nil {
		return fmt.Errorf("encode shell interrupt: %w", err)
	}
	if _, err := stdin.Write(append(encoded, '\n')); err != nil {
		return err
	}
	return nil
}

func (r *Runtime) writeContainerInput(container *Container, fn func(io.Writer) error) error {
	container.InputMu.Lock()
	defer container.InputMu.Unlock()
	return fn(container.Stdin)
}

func forwardOutput(ctx context.Context, outputCh <-chan OutputEvent, done <-chan struct{}, output func(Output)) Result {
	for {
		select {
		case event, ok := <-outputCh:
			if !ok {
				select {
				case <-done:
					return Result{}
				default:
					return Result{Err: errors.New("runtime output closed before turn completed")}
				}
			}
			if output != nil {
				output(Output{Stream: event.Stream, Line: event.Line})
			}
		case <-done:
			return Result{}
		case <-ctx.Done():
			return Result{Err: ctx.Err()}
		}
	}
}

func forwardUntilClosed(ctx context.Context, outputCh <-chan OutputEvent, output func(Output)) Result {
	for {
		select {
		case event, ok := <-outputCh:
			if !ok {
				return Result{}
			}
			if output != nil {
				output(Output{Stream: event.Stream, Line: event.Line})
			}
		case <-ctx.Done():
			return Result{Err: ctx.Err()}
		}
	}
}

func (r *Runtime) sendMessage(ctx context.Context, container *Container, message string, done <-chan struct{}, output func(Output)) Result {
	outputCh, cancel := container.OutputHub.Subscribe()
	defer cancel()

	// The sandbox network namespace must not be reconfigured while a gVisor
	// sandbox is attached to it. Replacing the address or default route on a
	// live netns breaks the sentry netstack and subsequent TCP writes fail with
	// ECONNRESET before requests reach the local proxy.
	if err := r.writeContainerInput(container, func(stdin io.Writer) error {
		return writeUserTurn(stdin, container.Agent, message)
	}); err != nil {
		r.stopContainer(container)
		return Result{Err: fmt.Errorf("write to stdin: %w", err)}
	}

	result := forwardOutput(ctx, outputCh, done, output)
	if result.Err != nil {
		r.stopContainer(container)
	}
	return result
}

func (r *Runtime) startFresh(ctx context.Context, req StartRequest, output func(Output)) Result {
	hub := NewOutputHub()
	outputCh, cancel := hub.Subscribe()
	defer cancel()

	hub.Publish(OutputEvent{Stream: "runtime", Line: "starting fresh container"})

	artifacts, err := r.generationArtifacts(ctx, req)
	if err != nil {
		return Result{Err: err}
	}
	req.PreparedArtifacts = artifacts
	if err := r.prepareSessionDirs(req.SessionID); err != nil {
		return Result{Err: err}
	}
	if !req.PreparedArtifacts.NetworkPrepared {
		if err := r.ensureSandboxNetwork(ctx, req.Generation); err != nil {
			return Result{Err: err}
		}
		req.PreparedArtifacts.NetworkPrepared = true
	}

	// Build runsc run command
	containerID := fmt.Sprintf("phase3-%s", req.SessionID)
	bundlePath := artifacts.BundleDir
	hub.Publish(OutputEvent{Stream: "runtime", Line: "using per-generation runtime bundle"})
	r.cleanupRunscContainer(ctx, containerID)

	cmdCtx, cancelCmd := context.WithCancel(context.Background())
	cmd := exec.CommandContext(cmdCtx, "runsc",
		"-root", r.cfg.RunscRoot,
		"-platform", "systrap",
		"-overlay2", r.runscOverlay2(req.Generation),
		"-network", r.runscNetwork(req.Generation),
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
		SessionID:    req.SessionID,
		GenerationID: req.GenerationID,
		RestoreID:    containerID,
		Agent:        req.Agent,
		Cmd:          cmd,
		Stdin:        stdin,
		Stdout:       stdout,
		Stderr:       stderr,
		Cancel:       cancelCmd,
		OutputHub:    hub,
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

	if !req.WaitForTurn {
		return Result{
			ControlManifestDigest: req.PreparedArtifacts.ManifestDigest,
			RunscVersion:          req.PreparedArtifacts.RunscVersion,
		}
	}

	// Send first message
	if req.FirstMessage != "" {
		if err := r.writeContainerInput(container, func(stdin io.Writer) error {
			return writeUserTurn(stdin, req.Agent, req.FirstMessage)
		}); err != nil {
			r.stopContainer(container)
			return Result{Err: fmt.Errorf("write first message: %w", err)}
		}
	}

	result := forwardOutput(ctx, outputCh, req.Done, output)
	if result.Err != nil {
		r.stopContainer(container)
	}
	result.ControlManifestDigest = req.PreparedArtifacts.ManifestDigest
	result.RunscVersion = req.PreparedArtifacts.RunscVersion
	return result
}

func (r *Runtime) resumeFromCheckpoint(ctx context.Context, req StartRequest, output func(Output)) Result {
	hub := NewOutputHub()
	outputCh, cancel := hub.Subscribe()
	defer cancel()

	hub.Publish(OutputEvent{Stream: "runtime", Line: "resuming from checkpoint"})

	// Restore re-renders regenerable host artifacts before comparing them with
	// checkpoint metadata, then uses runsc restore instead of run.
	checkpointPath, err := r.resolveCheckpointPath(req)
	if err != nil {
		return Result{Err: err}
	}
	artifacts, err := r.renderGenerationArtifacts(ctx, req)
	if err != nil {
		return Result{Err: err}
	}
	req.PreparedArtifacts = artifacts
	if err := validateCheckpointRestore(req.Generation, artifacts, checkpointPath); err != nil {
		return Result{Err: err}
	}
	if !req.PreparedArtifacts.NetworkPrepared {
		if err := r.ensureSandboxNetwork(ctx, req.Generation); err != nil {
			return Result{Err: err}
		}
		req.PreparedArtifacts.NetworkPrepared = true
	}
	containerID := fmt.Sprintf("phase3-%s", req.SessionID)
	bundlePath := artifacts.BundleDir
	r.cleanupRunscContainer(ctx, containerID)

	cmdCtx, cancelCmd := context.WithCancel(context.Background())
	cmd := exec.CommandContext(cmdCtx, "runsc",
		"-root", r.cfg.RunscRoot,
		"-platform", "systrap",
		"-overlay2", r.runscOverlay2(req.Generation),
		"-network", r.runscNetwork(req.Generation),
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
		SessionID:    req.SessionID,
		GenerationID: req.GenerationID,
		RestoreID:    containerID,
		Agent:        req.Agent,
		Cmd:          cmd,
		Stdin:        stdin,
		Stdout:       stdout,
		Stderr:       stderr,
		Cancel:       cancelCmd,
		OutputHub:    hub,
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

	if !req.WaitForTurn {
		return Result{
			ControlManifestDigest: req.PreparedArtifacts.ManifestDigest,
			RunscVersion:          req.PreparedArtifacts.RunscVersion,
		}
	}

	if req.FirstMessage != "" {
		if err := r.writeContainerInput(container, func(stdin io.Writer) error {
			return writeUserTurn(stdin, req.Agent, req.FirstMessage)
		}); err != nil {
			r.stopContainer(container)
			return Result{Err: fmt.Errorf("write first message: %w", err)}
		}
	}

	result := forwardOutput(ctx, outputCh, req.Done, output)
	if result.Err != nil {
		r.stopContainer(container)
	}
	result.ControlManifestDigest = req.PreparedArtifacts.ManifestDigest
	result.RunscVersion = req.PreparedArtifacts.RunscVersion
	return result
}

func (r *Runtime) resolveCheckpointPath(req StartRequest) (string, error) {
	candidates := []string{}
	if path := strings.TrimSpace(req.Generation.CheckpointPath); path != "" {
		candidates = append(candidates, path)
	}
	if root := strings.TrimSpace(r.cfg.CheckpointsRoot); root != "" {
		path := filepath.Join(root, req.SessionID)
		if len(candidates) == 0 || candidates[len(candidates)-1] != path {
			candidates = append(candidates, path)
		}
	}
	if len(candidates) == 0 {
		return "", errors.New("checkpoint path is required")
	}
	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return path, nil
		}
	}
	return "", fmt.Errorf("checkpoint image not found in %s", strings.Join(candidates, ", "))
}

func validateCheckpointRestore(details store.RuntimeGenerationDetails, artifacts GenerationArtifacts, checkpointPath string) error {
	if err := validateCheckpointImageManifest(checkpointPath); err != nil {
		return err
	}
	checks := []struct {
		field string
		got   string
		want  string
	}{
		{"checkpoint_network_profile_id", details.NetworkProfileID, details.CheckpointNetworkProfileID},
		{"checkpoint_agent_runtime_profile_id", details.AgentRuntimeProfileID, details.CheckpointAgentRuntimeProfileID},
		{"checkpoint_runsc_platform", defaultString(details.RunscPlatform, "systrap"), details.CheckpointRunscPlatform},
		{"checkpoint_runsc_version", artifacts.RunscVersion, details.CheckpointRunscVersion},
		{"checkpoint_bundle_digest", artifacts.BundleDigest, details.CheckpointBundleDigest},
		{"checkpoint_runtime_config_digest", artifacts.RuntimeConfigDigest, details.CheckpointRuntimeConfigDigest},
		{"checkpoint_control_manifest_digest", artifacts.ProjectedManifestDigest, details.CheckpointControlManifestDigest},
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

func (r *Runtime) Checkpoint(ctx context.Context, req CheckpointRequest) error {
	if strings.TrimSpace(req.SessionID) == "" {
		return errors.New("session id is required")
	}
	if strings.TrimSpace(req.GenerationID) == "" {
		return errors.New("generation id is required")
	}
	r.mu.RLock()
	container, exists := r.containers[req.SessionID]
	r.mu.RUnlock()

	if !exists {
		return errors.New("container not found")
	}
	if container.GenerationID != req.GenerationID {
		return fmt.Errorf("container generation mismatch: got %s want %s", container.GenerationID, req.GenerationID)
	}

	checkpointPath := strings.TrimSpace(req.CheckpointPath)
	if checkpointPath == "" {
		checkpointPath = filepath.Join(r.cfg.CheckpointsRoot, req.SessionID)
	}
	if err := os.MkdirAll(filepath.Dir(checkpointPath), 0o755); err != nil {
		return fmt.Errorf("create checkpoint dir: %w", err)
	}
	if err := os.RemoveAll(checkpointPath); err != nil {
		return fmt.Errorf("clear checkpoint dir: %w", err)
	}
	if err := os.MkdirAll(checkpointPath, 0o755); err != nil {
		return fmt.Errorf("create checkpoint image dir: %w", err)
	}

	// Create checkpoint
	cmd := exec.CommandContext(ctx, "runsc",
		"-root", r.cfg.RunscRoot,
		"-overlay2", r.cfg.RunscOverlay2,
		"checkpoint",
		"-image-path", checkpointPath,
		container.RestoreID,
	)

	if output, err := cmd.CombinedOutput(); err != nil {
		_ = os.RemoveAll(checkpointPath)
		return fmt.Errorf("runsc checkpoint: %w: %s", err, strings.TrimSpace(string(output)))
	}
	if err := writeCheckpointImageManifest(checkpointPath); err != nil {
		_ = os.RemoveAll(checkpointPath)
		return err
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

	return nil
}

func (r *Runtime) Interrupt(sessionID string) error {
	r.mu.RLock()
	container, exists := r.containers[sessionID]
	r.mu.RUnlock()
	if !exists {
		return errors.New("container not found")
	}
	return r.writeContainerInput(container, func(stdin io.Writer) error {
		return writeInterrupt(stdin, container.Agent)
	})
}
