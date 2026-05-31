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
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
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
	RunscRoot               string
	RunscNetwork            string
	RunscOverlay2           string
	SessionsRoot            string
	AgentHomesRoot          string
	CheckpointsRoot         string
	BundleRoot              string
	RootFSPath              string
	DefaultAgent            string
	SandboxUID              int
	SandboxGID              int
	SandboxSupplementalGIDs []int
	Claude                  ClaudeConfig
	RestoreFromCheckpoint   bool
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
const checkpointImageManifestFileName = "harness-checkpoint-manifest.json"
const checkpointImageManifestVersion = 1
const defaultRunscPlatform = "systrap"
const runscRunningProofTimeout = 2 * time.Second
const runscRunningProofPollInterval = 25 * time.Millisecond

var requiredCheckpointImageFiles = []string{"checkpoint.img", "pages.img", "pages_meta.img"}

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
	WorkspaceHostPath     string
	AgentHomeHostPath     string
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

type CheckpointRequest struct {
	SessionID      string
	GenerationID   string
	CheckpointPath string
	Generation     store.RuntimeGenerationDetails
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
	SandboxContractVersion               string `json:"sandbox_contract_version"`
	CreatedAt                            string `json:"created_at"`
	AttemptID                            string `json:"attempt_id"`
	NetworkProfileID                     string `json:"network_profile_id"`
	AgentRuntimeProfileID                string `json:"agent_runtime_profile_id"`
	Agent                                string `json:"agent"`
	DriverID                             string `json:"driver_id"`
	BridgeProtocolVersion                int    `json:"bridge_protocol_version"`
	TurnInputSchema                      string `json:"turn_input_schema"`
	ClaudeSessionUUID                    string `json:"claude_session_uuid,omitempty"`
	ResumeClaude                         bool   `json:"resume_claude"`
	RunscPlatform                        string `json:"runsc_platform"`
	RunscVersion                         string `json:"runsc_version"`
	SandboxModelProxyBaseURL             string `json:"sandbox_model_proxy_base_url,omitempty"`
	Model                                string `json:"model,omitempty"`
	OutputFormat                         string `json:"output_format"`
	WorkspacePath                        string `json:"workspace_path"`
	AgentHomePath                        string `json:"agent_home_path"`
	BundleDigest                         string `json:"bundle_digest"`
	RuntimeConfigDigest                  string `json:"runtime_config_digest"`
	SpecDigest                           string `json:"spec_digest"`
	EgressPolicyDigest                   string `json:"egress_policy_digest"`
	ManifestVersion                      int    `json:"manifest_version"`
	ClaudeCodeDisableNonessentialTraffic bool   `json:"claude_code_disable_nonessential_traffic"`
	PiCodingAgentDir                     string `json:"pi_coding_agent_dir,omitempty"`
	PiCodingAgentSessionDir              string `json:"pi_coding_agent_session_dir,omitempty"`
	PiOffline                            bool   `json:"pi_offline,omitempty"`
	PiSkipVersionCheck                   bool   `json:"pi_skip_version_check,omitempty"`
	PiTelemetryDisabled                  bool   `json:"pi_telemetry_disabled,omitempty"`
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
	UID            int   `json:"uid"`
	GID            int   `json:"gid"`
	AdditionalGIDs []int `json:"additionalGids,omitempty"`
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

type runscPin struct {
	Platform     string
	Version      string
	BinaryPath   string
	BinaryDigest string
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
	Agent            string
	Cmd              *exec.Cmd
	Stdin            io.WriteCloser
	Stdout           io.ReadCloser
	Stderr           io.ReadCloser
	Cancel           context.CancelFunc
	InputMu          sync.Mutex
	OutputHub        *OutputHub // Per-container pub/sub for output events
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
		if container.GenerationID == req.GenerationID {
			if !req.WaitForTurn {
				return Result{}
			}
			return r.sendMessage(ctx, container, req.FirstMessage, req.Done, output)
		}
		r.stopContainer(container)
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
		if filepath.Clean(check.got) != filepath.Clean(check.want) {
			return GenerationArtifacts{}, fmt.Errorf("restore artifact %s %q does not match generation path %q", check.label, check.got, check.want)
		}
	}
	return artifacts, nil
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

	if strings.EqualFold(strings.TrimSpace(r.runscNetwork(details)), "sandbox") {
		tableName := generationNftTableName(details)
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

	checkpointPath, err := validateCheckpointCleanupTarget(details, runRoot, generationDir, r.cfg.CheckpointsRoot)
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
	if strings.EqualFold(strings.TrimSpace(r.runscNetwork(details)), "sandbox") {
		ipNetns, err := r.netnsAbsenceEvidence(ctx, details.NetnsName)
		if err != nil {
			return err
		}
		ipLink, err := r.ipLinkAbsenceEvidence(ctx, details.HostVeth)
		if err != nil {
			return err
		}
		nft, err := r.nftTableAbsenceEvidence(ctx, generationNftTableName(details))
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
	runscBinary = defaultString(runscBinary, "runsc")
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

func (r *Runtime) runtimePostStartProof(ctx context.Context, details store.RuntimeGenerationDetails, pin runscPin, containerID string) (store.RuntimeResourcePostStartProof, error) {
	runscState, err := r.runscContainerRunningEvidence(ctx, pin.BinaryPath, containerID)
	if err != nil {
		return store.RuntimeResourcePostStartProof{}, err
	}
	ipNetns, ipLink, nft := "network_namespace:present; check=runsc_network_not_sandbox", "host_veth:present; check=runsc_network_not_sandbox", "nft_table:present; check=runsc_network_not_sandbox"
	if strings.EqualFold(strings.TrimSpace(r.runscNetwork(details)), "sandbox") {
		ipNetns, err = r.netnsPresenceEvidence(ctx, details.NetnsName)
		if err != nil {
			return store.RuntimeResourcePostStartProof{}, err
		}
		ipLink, err = r.ipLinkPresenceEvidence(ctx, details.HostVeth)
		if err != nil {
			return store.RuntimeResourcePostStartProof{}, err
		}
		nft, err = r.nftTablePresenceEvidence(ctx, generationNftTableName(details))
		if err != nil {
			return store.RuntimeResourcePostStartProof{}, err
		}
	}
	return store.RuntimeResourcePostStartProof{
		GenerationID:      details.GenerationID,
		RunscContainerID:  containerID,
		RunscState:        runscState,
		RunscPlatform:     pin.Platform,
		RunscVersion:      pin.Version,
		RunscBinaryPath:   pin.BinaryPath,
		RunscBinaryDigest: pin.BinaryDigest,
		IPNetns:           ipNetns,
		IPLink:            ipLink,
		NFT:               nft,
	}, nil
}

func (r *Runtime) runscContainerRunningEvidence(ctx context.Context, runscBinary, containerID string) (string, error) {
	runscBinary = defaultString(runscBinary, "runsc")
	deadline := time.Now().Add(runscRunningProofTimeout)
	var lastErr error
	for {
		output, err := r.runner.CombinedOutput(ctx, runscBinary, "-root", r.cfg.RunscRoot, "state", containerID)
		trimmed := strings.TrimSpace(string(output))
		if err != nil {
			lastErr = fmt.Errorf("verify runsc container %s running: %w: %s", containerID, err, trimmed)
		} else if trimmed == "" {
			return "runsc_container:" + containerID + ":running; check=" + runscBinary + " state " + containerID, nil
		} else {
			lower := strings.ToLower(trimmed)
			if strings.Contains(trimmed, containerID) && strings.Contains(lower, "running") {
				return "runsc_container:" + containerID + ":running; check=" + runscBinary + " state " + containerID + "; output=" + trimmed, nil
			}
			lastErr = fmt.Errorf("verify runsc container %s running: unexpected state %q", containerID, trimmed)
		}
		if time.Now().After(deadline) {
			return "", lastErr
		}
		timer := time.NewTimer(runscRunningProofPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", fmt.Errorf("verify runsc container %s running: %w", containerID, ctx.Err())
		case <-timer.C:
		}
	}
}

func (r *Runtime) netnsPresenceEvidence(ctx context.Context, netnsName string) (string, error) {
	netnsName = strings.TrimSpace(netnsName)
	if netnsName == "" {
		return "", fmt.Errorf("verify network namespace presence: netns name is required")
	}
	output, err := r.runner.CombinedOutput(ctx, "ip", "netns", "list")
	if err != nil {
		return "", fmt.Errorf("verify network namespace %s presence: %w: %s", netnsName, err, strings.TrimSpace(string(output)))
	}
	if !netnsListContains(string(output), netnsName) {
		return "", fmt.Errorf("verify network namespace %s presence: namespace not found", netnsName)
	}
	return "netns:present; check=ip netns list " + netnsName, nil
}

func (r *Runtime) ipLinkPresenceEvidence(ctx context.Context, linkName string) (string, error) {
	linkName = strings.TrimSpace(linkName)
	if linkName == "" {
		return "", fmt.Errorf("verify host veth presence: link name is required")
	}
	output, err := r.runner.CombinedOutput(ctx, "ip", "link", "show", linkName)
	if err != nil {
		return "", fmt.Errorf("verify host veth %s presence: %w: %s", linkName, err, strings.TrimSpace(string(output)))
	}
	return "host_veth:present; check=ip link show " + linkName + "; output=" + strings.TrimSpace(string(output)), nil
}

func (r *Runtime) nftTablePresenceEvidence(ctx context.Context, tableName string) (string, error) {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return "", fmt.Errorf("verify nft table presence: table name is required")
	}
	output, err := r.runner.CombinedOutput(ctx, "nft", "list", "table", "inet", tableName)
	if err != nil {
		return "", fmt.Errorf("verify nft table %s presence: %w: %s", tableName, err, strings.TrimSpace(string(output)))
	}
	return "nft_table:present; check=nft list table inet " + tableName + "; output=" + strings.TrimSpace(string(output)), nil
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

func validateCheckpointCleanupTarget(details store.RuntimeGenerationDetails, runRoot, generationDir, checkpointsRoot string) (filesystemCleanupTarget, error) {
	expectedGenerationCheckpoint := filepath.Join(runRoot, generationDir, "checkpoint")
	if err := validateFilesystemCleanupTarget(cleanupTargetCheckpoint, details.CheckpointPath, expectedGenerationCheckpoint, runRoot); err == nil {
		return filesystemCleanupTarget{kind: cleanupTargetCheckpoint, path: cleanAbsolutePath(details.CheckpointPath), root: runRoot}, nil
	}

	root, err := cleanAbsoluteRoot(checkpointsRoot, "checkpoints root")
	if err != nil {
		return filesystemCleanupTarget{}, fmt.Errorf("checkpoint cleanup target %q is not the generation checkpoint path and legacy cleanup is unavailable: %w", details.CheckpointPath, err)
	}
	sessionComponent, err := safePathComponent("session id", strings.TrimSpace(details.SessionID))
	if err != nil {
		return filesystemCleanupTarget{}, err
	}
	expectedLegacy := filepath.Join(root, sessionComponent)
	if err := validateFilesystemCleanupTarget(cleanupTargetCheckpoint, details.CheckpointPath, expectedLegacy, root); err != nil {
		return filesystemCleanupTarget{}, err
	}
	return filesystemCleanupTarget{kind: cleanupTargetCheckpoint, path: cleanAbsolutePath(details.CheckpointPath), root: root}, nil
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

func cleanAbsoluteRoot(path, label string) (string, error) {
	cleaned := cleanAbsolutePath(path)
	if cleaned == "" {
		return "", fmt.Errorf("%s is required and must be absolute", label)
	}
	return cleaned, nil
}

func cleanAbsolutePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" || !filepath.IsAbs(path) {
		return ""
	}
	return filepath.Clean(path)
}

func safePathComponent(label, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || filepath.Base(value) != value || value == "." || value == ".." {
		return "", fmt.Errorf("%s %q is not a safe path component", label, value)
	}
	return value, nil
}

func pathContainsDotDot(path string) bool {
	for _, part := range strings.Split(path, string(filepath.Separator)) {
		if part == ".." {
			return true
		}
	}
	return false
}

func ensurePathStaysWithinRoot(path, root string) error {
	if !pathWithinRoot(path, root) {
		return fmt.Errorf("path is outside root %q", root)
	}
	resolvedRoot := root
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		resolvedRoot = filepath.Clean(resolved)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("resolve root: %w", err)
	}

	resolvedPath, err := filepath.EvalSymlinks(path)
	if err == nil {
		if !pathWithinRoot(filepath.Clean(resolvedPath), resolvedRoot) {
			return fmt.Errorf("resolved path escapes root %q", resolvedRoot)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("resolve path: %w", err)
	}
	prefix, err := deepestExistingPath(path)
	if err != nil {
		return err
	}
	if prefix == "" {
		return nil
	}
	if !pathWithinRoot(prefix, root) {
		return nil
	}
	resolvedPrefix, err := filepath.EvalSymlinks(prefix)
	if err != nil {
		return fmt.Errorf("resolve existing prefix: %w", err)
	}
	if !pathWithinRoot(filepath.Clean(resolvedPrefix), resolvedRoot) {
		return fmt.Errorf("resolved existing prefix escapes root %q", resolvedRoot)
	}
	return nil
}

func deepestExistingPath(path string) (string, error) {
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		if _, err := os.Lstat(current); err == nil {
			return current, nil
		} else if !os.IsNotExist(err) {
			return "", fmt.Errorf("stat existing prefix %q: %w", current, err)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", nil
		}
	}
}

func pathWithinRoot(path, root string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
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
	if !hasRecordedRunscBinaryPin(details) {
		return "runsc", "", r.deleteRunscContainer(ctx, "runsc", containerID)
	}
	current := r.currentRunscPin(ctx)
	currentBinary := defaultString(current.BinaryPath, "runsc")
	pinned := runscPinFromDetails(details)
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
	compatibleBinary, err := verifiedRecordedRunscBinary(pinned)
	if err != nil {
		return currentBinary, evidence, fmt.Errorf("current runsc pin mismatch; current delete failed: %w; recorded runsc unavailable: %v", currentErr, err)
	}
	recordedErr := r.deleteRunscContainer(ctx, compatibleBinary, containerID)
	if recordedErr != nil {
		return compatibleBinary, cleanupRunscPinMismatchEvidence(current, pinned, "recorded"), fmt.Errorf("current runsc pin mismatch; current delete failed: %w; recorded delete failed: %v", currentErr, recordedErr)
	}
	return compatibleBinary, cleanupRunscPinMismatchEvidence(current, pinned, "recorded"), nil
}

func hasRecordedRunscBinaryPin(details store.RuntimeGenerationDetails) bool {
	return strings.TrimSpace(details.RunscBinaryPath) != "" && strings.TrimSpace(details.RunscBinaryDigest) != ""
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

func verifiedRecordedRunscBinary(pinned runscPin) (string, error) {
	path := strings.TrimSpace(pinned.BinaryPath)
	if path == "" {
		return "", fmt.Errorf("recorded runsc binary path is required")
	}
	if !filepath.IsAbs(path) {
		return "", fmt.Errorf("recorded runsc binary path %q must be absolute", path)
	}
	cleaned := filepath.Clean(path)
	canonical, err := filepath.EvalSymlinks(cleaned)
	if err != nil {
		return "", fmt.Errorf("resolve recorded runsc binary path %q: %w", path, err)
	}
	canonical = filepath.Clean(canonical)
	if canonical != cleaned {
		return "", fmt.Errorf("recorded runsc binary path %q is not canonical, resolves to %q", cleaned, canonical)
	}
	info, err := os.Stat(canonical)
	if err != nil {
		return "", fmt.Errorf("stat recorded runsc binary %q: %w", canonical, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("recorded runsc binary %q is not a regular file", canonical)
	}
	if info.Mode()&0o111 == 0 {
		return "", fmt.Errorf("recorded runsc binary %q is not executable", canonical)
	}
	digest, err := fileSHA256(canonical)
	if err != nil {
		return "", fmt.Errorf("digest recorded runsc binary %q: %w", canonical, err)
	}
	if got, want := "sha256:"+digest, strings.TrimSpace(pinned.BinaryDigest); got != want {
		return "", fmt.Errorf("recorded runsc binary digest %s, want %s", got, want)
	}
	return canonical, nil
}

type runscContainerDeleteResult struct {
	Missing bool
}

func (r *Runtime) deleteRunscContainer(ctx context.Context, runscBinary, containerID string) error {
	_, err := r.deleteRunscContainerDetailed(ctx, runscBinary, containerID)
	return err
}

func (r *Runtime) deleteRunscContainerDetailed(ctx context.Context, runscBinary, containerID string) (runscContainerDeleteResult, error) {
	runscBinary = defaultString(runscBinary, "runsc")
	_, _ = r.runner.CombinedOutput(ctx, runscBinary, "-root", r.cfg.RunscRoot, "kill", containerID, "KILL")
	output, err := r.runner.CombinedOutput(ctx, runscBinary, "-root", r.cfg.RunscRoot, "delete", containerID)
	if err != nil {
		if commandFailureContains(output, err, "does not exist", "not found", "no such container", "no such file") {
			return runscContainerDeleteResult{Missing: true}, nil
		}
		return runscContainerDeleteResult{}, fmt.Errorf("%w: %s", err, strings.TrimSpace(string(output)))
	}
	return runscContainerDeleteResult{}, nil
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
	if err := r.writeNetworkHostsProjection(details); err != nil {
		return GenerationArtifacts{}, err
	}
	materializedDriverConfig, err := r.writeDriverConfigProjection(req)
	if err != nil {
		return GenerationArtifacts{}, err
	}
	spec, specDigest, err := r.renderRuntimeSpec(req)
	if err != nil {
		return GenerationArtifacts{}, err
	}
	if err := writeJSONFileAtomic(details.SpecPath, spec, 0o644); err != nil {
		return GenerationArtifacts{}, fmt.Errorf("write runtime spec: %w", err)
	}
	currentRunsc := r.currentRunscPin(ctx)
	bundleDigest := digestHex(mustCanonicalJSON(map[string]any{
		"bundle_dir":  filepath.Clean(details.BundleDirPath),
		"rootfs":      spec.Root.Path,
		"spec_digest": specDigest,
	}))
	runtimeConfigDigest := digestHex(mustCanonicalJSON(map[string]any{
		"runsc_network":       r.runscNetwork(details),
		"runsc_overlay2":      r.runscOverlay2(details),
		"runsc_platform":      currentRunsc.Platform,
		"runsc_binary_path":   currentRunsc.BinaryPath,
		"runsc_binary_digest": currentRunsc.BinaryDigest,
		"rootfs":              spec.Root.Path,
	}))
	manifest, err := r.buildGenerationManifest(req, currentRunsc.Version, bundleDigest, runtimeConfigDigest, specDigest)
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
		MaterializedDriverConfig: materializedDriverConfig,
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
	return r.prepareRuntimeDataDirs(req)
}

func (r *Runtime) writeNetworkHostsProjection(details store.RuntimeGenerationDetails) error {
	if strings.TrimSpace(details.NetworkHostsPath) == "" {
		return nil
	}
	payload, err := renderNetworkHostsProjection(details)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(details.NetworkHostsPath, payload, 0o644); err != nil {
		return fmt.Errorf("write network hosts projection: %w", err)
	}
	return nil
}

func (r *Runtime) writeDriverConfigProjection(req StartRequest) ([]DriverConfigMaterialization, error) {
	if driverID(req) != string(agents.Pi) {
		return nil, nil
	}
	details := req.Generation
	projection, err := buildPiDriverConfigProjection(details)
	if err != nil {
		return nil, err
	}
	entries := []DriverConfigMaterialization{
		{
			Name:                        "models",
			SourceProjectionPath:        agents.PiModelsConfigPath,
			HostSourcePath:              filepath.Join(details.ControlDirPath, "driver", "pi", "models.json"),
			SandboxDestination:          agents.PiModelsSandboxPath,
			DestinationMutableBySandbox: false,
		},
		{
			Name:                        "settings",
			SourceProjectionPath:        agents.PiSettingsConfigPath,
			HostSourcePath:              filepath.Join(details.ControlDirPath, "driver", "pi", "settings.json"),
			SandboxDestination:          agents.PiSettingsSandboxPath,
			DestinationMutableBySandbox: false,
		},
	}
	payloads := map[string]any{
		"models":   projection.Models,
		"settings": projection.Settings,
	}
	for i := range entries {
		payload, err := canonicalJSON(payloads[entries[i].Name])
		if err != nil {
			return nil, fmt.Errorf("render pi %s config: %w", entries[i].Name, err)
		}
		if err := writeFileAtomic(entries[i].HostSourcePath, payload, 0o644); err != nil {
			return nil, fmt.Errorf("write pi %s config: %w", entries[i].Name, err)
		}
		entries[i].SourceDigest = prefixedSHA256(payload)
	}
	return entries, nil
}

type piDriverConfigProjection struct {
	Models   map[string]any
	Settings map[string]any
}

func buildPiDriverConfigProjection(details store.RuntimeGenerationDetails) (piDriverConfigProjection, error) {
	model := strings.TrimSpace(details.Model)
	if model == "" {
		return piDriverConfigProjection{}, fmt.Errorf("pi model is required")
	}
	baseURL := strings.TrimSpace(details.ManifestAnthropicBaseURL)
	if baseURL == "" {
		return piDriverConfigProjection{}, fmt.Errorf("pi sandbox model proxy base url is required")
	}
	return piDriverConfigProjection{
		Models: map[string]any{
			"providers": map[string]any{
				agents.PiHarnessProxyProvider: map[string]any{
					"baseUrl": baseURL,
					"api":     "anthropic-messages",
					"apiKey":  "harness-model-proxy-dummy-key",
					"models": []map[string]any{
						{
							"id": model,
						},
					},
				},
			},
		},
		Settings: map[string]any{
			"schema_version":      1,
			"coding_agent_dir":    agents.PiCodingAgentDir,
			"session_dir":         agents.PiSessionDir,
			"offline":             true,
			"skip_version_check":  true,
			"telemetry":           false,
			"provider":            agents.PiHarnessProxyProvider,
			"model":               model,
			"production_sessions": true,
		},
	}, nil
}

func renderNetworkHostsProjection(details store.RuntimeGenerationDetails) ([]byte, error) {
	host, err := modelProxyBaseURLHost(details.ManifestAnthropicBaseURL)
	if err != nil {
		return nil, err
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return nil, fmt.Errorf("network hosts projection requires a hostname alias, got %q", host)
	}
	gateway, err := netip.ParseAddr(strings.TrimSpace(details.HostGatewayIP))
	if err != nil {
		return nil, fmt.Errorf("network hosts projection requires host gateway ip: %w", err)
	}
	lines := []string{
		"127.0.0.1 localhost",
		"::1 localhost ip6-localhost ip6-loopback",
		fmt.Sprintf("%s %s", gateway.String(), host),
	}
	return []byte(strings.Join(lines, "\n") + "\n"), nil
}

func modelProxyBaseURLHost(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("invalid model proxy base url: %w", err)
	}
	if parsed.Scheme != "http" {
		return "", fmt.Errorf("model proxy base url must use the local http proxy scheme")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("model proxy base url must not include userinfo, query, or fragment")
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return "", fmt.Errorf("model proxy base url must not include a path")
	}
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	if host == "" || strings.ContainsAny(host, " \t\r\n/") {
		return "", fmt.Errorf("model proxy base url must include a hostname")
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return "", fmt.Errorf("model proxy base url must use a stable hostname alias, not an IP literal")
	}
	if modelProxyHostIsHostLocal(host) {
		return "", fmt.Errorf("model proxy base url must not use a host-local name")
	}
	if modelProxyHostIsProviderUpstream(host) {
		return "", fmt.Errorf("model proxy base url must not point at a provider upstream")
	}
	return host, nil
}

func modelProxyHostIsHostLocal(host string) bool {
	return host == "localhost" ||
		host == "host.docker.internal" ||
		strings.HasSuffix(host, ".localhost") ||
		strings.HasSuffix(host, ".local")
}

func modelProxyHostIsProviderUpstream(host string) bool {
	switch host {
	case "api.anthropic.com", "anthropic.com", "api.openai.com", "openai.com":
		return true
	default:
		return strings.HasSuffix(host, ".anthropic.com") || strings.HasSuffix(host, ".openai.com")
	}
}

func (r *Runtime) buildGenerationManifest(req StartRequest, runscVersion, bundleDigest, runtimeConfigDigest, specDigest string) (controlManifest, error) {
	details := req.Generation
	if !isSandboxIsolatedRequest(req) {
		selectedDriver := driverID(req)
		if selectedDriver == "" {
			return controlManifest{}, fmt.Errorf("agent is required")
		}
		return controlManifest{}, fmt.Errorf("unsupported agent %q", selectedDriver)
	}
	selectedDriver := driverID(req)
	agent, _ := agents.SandboxAgentForDriver(selectedDriver)
	manifest := controlManifest{
		SessionID:                            req.SessionID,
		GenerationID:                         details.GenerationID,
		SandboxContractVersion:               defaultString(details.SandboxContractVersion, store.SandboxContractVersion),
		CreatedAt:                            time.Now().UTC().Format(time.RFC3339Nano),
		AttemptID:                            "attempt-0",
		NetworkProfileID:                     details.NetworkProfileID,
		AgentRuntimeProfileID:                details.AgentRuntimeProfileID,
		Agent:                                agent,
		DriverID:                             selectedDriver,
		BridgeProtocolVersion:                2,
		TurnInputSchema:                      "RunTurn",
		ClaudeSessionUUID:                    req.ClaudeSessionUUID,
		ResumeClaude:                         req.ResumeClaude,
		RunscPlatform:                        effectiveRunscPlatform(details),
		RunscVersion:                         runscVersion,
		SandboxModelProxyBaseURL:             details.ManifestAnthropicBaseURL,
		Model:                                details.Model,
		OutputFormat:                         details.OutputFormat,
		WorkspacePath:                        "/workspace",
		AgentHomePath:                        "/agent-home",
		BundleDigest:                         bundleDigest,
		RuntimeConfigDigest:                  runtimeConfigDigest,
		SpecDigest:                           specDigest,
		EgressPolicyDigest:                   details.EgressPolicyDigest,
		ManifestVersion:                      1,
		ClaudeCodeDisableNonessentialTraffic: details.DisableNonessentialTraffic,
	}
	if selectedDriver == string(agents.Pi) {
		manifest.PiCodingAgentDir = agents.PiCodingAgentDir
		manifest.PiCodingAgentSessionDir = agents.PiSessionDir
		manifest.PiOffline = true
		manifest.PiSkipVersionCheck = true
		manifest.PiTelemetryDisabled = true
	}
	return manifest, nil
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
	selectedDriver := driverID(req)
	if selectedDriver == "" {
		return fmt.Errorf("agent is required")
	}
	if _, ok := agents.Lookup(selectedDriver); !ok {
		return fmt.Errorf("unsupported agent %q", selectedDriver)
	}
	if !isSandboxIsolatedRequest(req) {
		return fmt.Errorf("unsupported agent %q", selectedDriver)
	}
	if platform := effectiveRunscPlatform(details); platform != defaultRunscPlatform {
		return fmt.Errorf("unsupported runsc platform %q", platform)
	}
	if details.RequiresSecretDrop ||
		strings.TrimSpace(details.SecretsDirPath) != "" ||
		strings.TrimSpace(details.AnthropicAPIKeySecretID) != "" ||
		strings.TrimSpace(details.AnthropicAuthTokenSecretID) != "" ||
		strings.TrimSpace(details.SecretVersion) != "" {
		return fmt.Errorf("sandbox_secret_disallowed")
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
	return projectedControlManifestPayloadDigest(manifest)
}

func projectedControlManifestPayloadDigest(payload any) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		return "", err
	}
	strictFields, regenerableFields := controlManifestProjectionFields()
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

func controlManifestProjectionFields() (map[string]struct{}, map[string]struct{}) {
	strictFields := map[string]struct{}{
		"session_id":                   {},
		"generation_id":                {},
		"sandbox_contract_version":     {},
		"network_profile_id":           {},
		"agent_runtime_profile_id":     {},
		"agent":                        {},
		"driver_id":                    {},
		"bridge_protocol_version":      {},
		"turn_input_schema":            {},
		"claude_session_uuid":          {},
		"resume_claude":                {},
		"runsc_platform":               {},
		"runsc_version":                {},
		"sandbox_model_proxy_base_url": {},
		"model":                        {},
		"output_format":                {},
		"workspace_path":               {},
		"agent_home_path":              {},
		"bundle_digest":                {},
		"runtime_config_digest":        {},
		"spec_digest":                  {},
		"egress_policy_digest":         {},
		"manifest_version":             {},
		"claude_code_disable_nonessential_traffic": {},
		"pi_coding_agent_dir":                      {},
		"pi_coding_agent_session_dir":              {},
		"pi_offline":                               {},
		"pi_skip_version_check":                    {},
		"pi_telemetry_disabled":                    {},
	}
	regenerableFields := map[string]struct{}{
		"created_at": {},
		"attempt_id": {},
	}
	return strictFields, regenerableFields
}

func (r *Runtime) renderRuntimeSpec(req StartRequest) (runtimeSpec, string, error) {
	selectedDriver := driverID(req)
	switch selectedDriver {
	case string(agents.ClaudeCode), string(agents.Pi), string(agents.Shell):
		return r.renderSandboxIsolatedRuntimeSpec(req)
	case "":
		return runtimeSpec{}, "", fmt.Errorf("agent is required")
	default:
		return runtimeSpec{}, "", fmt.Errorf("unsupported agent %q", selectedDriver)
	}
}

func (r *Runtime) renderSandboxIsolatedRuntimeSpec(req StartRequest) (runtimeSpec, string, error) {
	var spec runtimeSpec
	details := req.Generation
	selectedDriver := driverID(req)
	agent, ok := agents.SandboxAgentForDriver(selectedDriver)
	if !ok {
		return runtimeSpec{}, "", fmt.Errorf("unsupported agent %q", selectedDriver)
	}
	identity, err := r.requiredSandboxIdentity(details)
	if err != nil {
		return runtimeSpec{}, "", err
	}
	workspaceHostPath, agentHomeHostPath, err := r.sandboxIsolationDataPaths(req)
	if err != nil {
		return runtimeSpec{}, "", err
	}
	plan, err := BuildSandboxMountPlan(SandboxMountPlanInputs{
		Generation:        details,
		WorkspaceHostPath: workspaceHostPath,
		AgentHomeHostPath: agentHomeHostPath,
		NetworkHostsPath:  details.NetworkHostsPath,
	})
	if err != nil {
		return runtimeSpec{}, "", err
	}
	spec.OCIVersion = "1.0.2"
	spec.Process.Terminal = false
	spec.Process.User = specUser{UID: identity.UID, GID: identity.GID, AdditionalGIDs: identity.SupplementalGIDs}
	spec.Process.Args = []string{"/usr/local/bin/harness-agent-entrypoint"}
	spec.Process.Env = []string{
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"LANG=C.UTF-8",
		"MPLCONFIGDIR=/tmp/matplotlib",
		"TMPDIR=/tmp",
		"HOME=/agent-home",
		"USER=harness",
		"LOGNAME=harness",
		"SESSION_WORKSPACE=/workspace",
		"HARNESS_AGENT_HOME=/agent-home",
		"HARNESS_AGENT=" + agent,
		"HARNESS_DRIVER_ID=" + driverID(req),
		"HARNESS_TURN_INPUT_SCHEMA=RunTurn",
		"HARNESS_BRIDGE_PROTOCOL_VERSION=2",
		"HARNESS_EXPECTED_SESSION_ID=" + req.SessionID,
		"HARNESS_EXPECTED_GENERATION_ID=" + details.GenerationID,
		"HARNESS_EXPECTED_NETWORK_PROFILE_ID=" + details.NetworkProfileID,
		"HARNESS_EXPECTED_AGENT_RUNTIME_PROFILE_ID=" + details.AgentRuntimeProfileID,
		"HARNESS_EXPECTED_MANIFEST_VERSION=1",
		fmt.Sprintf("HARNESS_AGENT_UID=%d", identity.UID),
		fmt.Sprintf("HARNESS_AGENT_GID=%d", identity.GID),
		"HARNESS_BRIDGE_DIR=" + bridge.BridgeMountDestination,
		"HARNESS_BRIDGE_MODE=" + defaultString(r.cfg.BridgeMode, "claim-loop"),
		"HARNESS_BRIDGE_HEARTBEAT_INTERVAL=" + formatSeconds(defaultDuration(r.cfg.BridgeHeartbeat, 30*time.Second)),
		"HARNESS_BRIDGE_POLL_INTERVAL=" + formatSeconds(defaultDuration(r.cfg.BridgePollInterval, 5*time.Millisecond)),
		"HARNESS_BRIDGE_IDLE_INTERVAL=" + formatSeconds(defaultDuration(r.cfg.BridgePollInterval, 5*time.Millisecond)),
		"HARNESS_PROBE_HEALTHZ_STATUSES=" + joinInts(defaultIntSlice(r.cfg.ProbeHealthzStatuses, []int{200})),
	}
	if selectedDriver == string(agents.Pi) {
		spec.Process.Env = append(spec.Process.Env,
			"PI_CODING_AGENT_DIR="+agents.PiCodingAgentDir,
			"PI_CODING_AGENT_SESSION_DIR="+agents.PiSessionDir,
			"PI_OFFLINE=1",
			"PI_SKIP_VERSION_CHECK=1",
			"PI_TELEMETRY=0",
		)
	}
	spec.Process.Cwd = "/"
	spec.Process.Capabilities = emptyCapabilities()
	spec.Process.Rlimits = []map[string]any{{"type": "RLIMIT_NOFILE", "hard": 1024, "soft": 1024}}
	spec.Process.NoNewPrivileges = true
	spec.Root = specRoot{Path: r.rootFSPath(), Readonly: true}
	spec.Hostname = "harness-gen-" + shortID(details.GenerationID)
	pseudoMounts := RuntimeAdapterPseudoMounts()
	if err := ValidateRuntimeAdapterPseudoMounts(pseudoMounts); err != nil {
		return runtimeSpec{}, "", err
	}
	spec.Mounts = append(pseudoMounts, plan.SpecMounts()...)
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

func prefixedSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + fmt.Sprintf("%x", sum[:])
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

func (r *Runtime) currentRunscPin(ctx context.Context) runscPin {
	path, digest := runscBinaryMetadata()
	return runscPin{
		Platform:     defaultRunscPlatform,
		Version:      r.runscVersion(ctx),
		BinaryPath:   path,
		BinaryDigest: digest,
	}
}

func effectiveRunscPlatform(details store.RuntimeGenerationDetails) string {
	return defaultString(details.RunscPlatform, defaultRunscPlatform)
}

func runscPinFromArtifacts(details store.RuntimeGenerationDetails, artifacts GenerationArtifacts) runscPin {
	return runscPin{
		Platform:     effectiveRunscPlatform(details),
		Version:      artifacts.RunscVersion,
		BinaryPath:   artifacts.RunscBinaryPath,
		BinaryDigest: artifacts.RunscBinaryDigest,
	}
}

func runscPinFromDetails(details store.RuntimeGenerationDetails) runscPin {
	return runscPin{
		Platform:     effectiveRunscPlatform(details),
		Version:      details.RunscVersion,
		BinaryPath:   details.RunscBinaryPath,
		BinaryDigest: details.RunscBinaryDigest,
	}
}

func (r *Runtime) verifyLaunchRunscPin(ctx context.Context, operation string, details store.RuntimeGenerationDetails, artifacts GenerationArtifacts) (runscPin, error) {
	current := r.currentRunscPin(ctx)
	if err := verifyRequiredRunscPin(operation, "prepared artifacts", current, runscPinFromArtifacts(details, artifacts)); err != nil {
		return current, err
	}
	if err := verifyOptionalRunscPin(operation, "resource instance", current, runscPinFromDetails(details)); err != nil {
		return current, err
	}
	return current, nil
}

func (r *Runtime) verifyGenerationRunscPin(ctx context.Context, operation string, details store.RuntimeGenerationDetails) (runscPin, error) {
	current := r.currentRunscPin(ctx)
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

func verifyOptionalRunscPin(operation, source string, current, pinned runscPin) error {
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
			continue
		}
		if check.current != check.pinned {
			return fmt.Errorf("runsc pin mismatch before %s: %s %s current %q pinned %q", operation, source, check.field, check.current, check.pinned)
		}
	}
	return nil
}

func runscBinaryMetadata() (string, string) {
	path, err := exec.LookPath("runsc")
	if err != nil {
		return "runsc", "unavailable:" + err.Error()
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		canonical = filepath.Clean(path)
	}
	digest, err := fileSHA256(canonical)
	if err != nil {
		return canonical, "unavailable:" + err.Error()
	}
	return canonical, "sha256:" + digest
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

type runtimeSandboxIdentity struct {
	UID              int
	GID              int
	SupplementalGIDs []int
}

func (r *Runtime) requiredSandboxIdentity(details store.RuntimeGenerationDetails) (runtimeSandboxIdentity, error) {
	supplementalGIDs := append([]int(nil), details.SandboxSupplementalGIDs...)
	if len(supplementalGIDs) == 0 {
		supplementalGIDs = append([]int(nil), r.cfg.SandboxSupplementalGIDs...)
	}
	identity := runtimeSandboxIdentity{
		UID:              details.SandboxUID,
		GID:              details.SandboxGID,
		SupplementalGIDs: supplementalGIDs,
	}
	if identity.UID <= 0 {
		identity.UID = r.cfg.SandboxUID
	}
	if identity.GID <= 0 {
		identity.GID = r.cfg.SandboxGID
	}
	sort.Ints(identity.SupplementalGIDs)
	if identity.UID <= 0 {
		return runtimeSandboxIdentity{}, fmt.Errorf("sandbox identity uid must be > 0")
	}
	if identity.GID <= 0 {
		return runtimeSandboxIdentity{}, fmt.Errorf("sandbox identity gid must be > 0")
	}
	seen := map[int]struct{}{}
	for _, gid := range identity.SupplementalGIDs {
		if gid <= 0 {
			return runtimeSandboxIdentity{}, fmt.Errorf("sandbox identity supplemental gids must be positive")
		}
		if _, ok := seen[gid]; ok {
			return runtimeSandboxIdentity{}, fmt.Errorf("sandbox identity supplemental gids contain duplicate gid %d", gid)
		}
		seen[gid] = struct{}{}
	}
	return identity, nil
}

func emptyCapabilities() map[string]any {
	return map[string]any{
		"bounding":    []string{},
		"effective":   []string{},
		"inheritable": []string{},
		"permitted":   []string{},
		"ambient":     []string{},
	}
}

func (r *Runtime) prepareRuntimeDataDirs(req StartRequest) error {
	if isSandboxIsolatedRequest(req) {
		return r.prepareSandboxIsolationDataDirs(req)
	}
	selectedDriver := driverID(req)
	if selectedDriver == "" {
		return fmt.Errorf("agent is required")
	}
	return fmt.Errorf("unsupported agent %q", selectedDriver)
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
	if driverID(req) == string(agents.Pi) {
		piRootDir := filepath.Join(agentHomeHostPath, ".pi")
		piAgentDir := filepath.Join(agentHomeHostPath, ".pi", "agent")
		for _, item := range []struct {
			label string
			path  string
		}{
			{label: "pi root dir", path: piRootDir},
			{label: "pi agent dir", path: piAgentDir},
			{label: "pi session dir", path: filepath.Join(piAgentDir, "sessions")},
		} {
			if err := ensureSandboxOwnedDir(item.path, identity.UID, identity.GID, 0o750); err != nil {
				return fmt.Errorf("%s: %w", item.label, err)
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
	if agent := strings.TrimSpace(req.Agent); agent != "" {
		return agent
	}
	return strings.TrimSpace(req.Generation.Agent)
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

func commandFailureContains(output []byte, err error, needles ...string) bool {
	text := string(output)
	if err != nil {
		text += " " + err.Error()
	}
	return commandOutputContains(text, needles...)
}

func generationNftTableName(details store.RuntimeGenerationDetails) string {
	if tableName := strings.TrimSpace(details.NftTableName); tableName != "" {
		return tableName
	}
	return hostEgressTableName(details.GenerationID)
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
	bundlePath := artifacts.BundleDir
	hub.Publish(OutputEvent{Stream: "runtime", Line: "using per-generation runtime bundle"})
	r.cleanupRunscContainer(ctx, containerID)

	cmdCtx, cancelCmd := context.WithCancel(context.Background())
	cmd := exec.CommandContext(cmdCtx, currentRunsc.BinaryPath,
		"-root", r.cfg.RunscRoot,
		"-platform", currentRunsc.Platform,
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
		SessionID:        req.SessionID,
		GenerationID:     req.GenerationID,
		RunscContainerID: containerID,
		Agent:            req.Agent,
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
	if !req.WaitForTurn {
		return Result{
			ControlManifestDigest: req.PreparedArtifacts.ManifestDigest,
			RunscVersion:          req.PreparedArtifacts.RunscVersion,
			PostStartProof:        &postStartProof,
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
	result.PostStartProof = &postStartProof
	return result
}

func (r *Runtime) resumeFromCheckpoint(ctx context.Context, req StartRequest, output func(Output)) Result {
	hub := NewOutputHub()
	outputCh, cancel := hub.Subscribe()
	defer cancel()

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
	bundlePath := artifacts.BundleDir
	r.cleanupRunscContainer(ctx, containerID)

	cmdCtx, cancelCmd := context.WithCancel(context.Background())
	cmd := exec.CommandContext(cmdCtx, currentRunsc.BinaryPath,
		"-root", r.cfg.RunscRoot,
		"-platform", currentRunsc.Platform,
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
		SessionID:        req.SessionID,
		GenerationID:     req.GenerationID,
		RunscContainerID: containerID,
		Agent:            req.Agent,
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
	if !req.WaitForTurn {
		return Result{
			ControlManifestDigest: req.PreparedArtifacts.ManifestDigest,
			RunscVersion:          req.PreparedArtifacts.RunscVersion,
			PostStartProof:        &postStartProof,
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
	result.PostStartProof = &postStartProof
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
		{"checkpoint_runsc_platform", effectiveRunscPlatform(details), details.CheckpointRunscPlatform},
		{"checkpoint_runsc_version", artifacts.RunscVersion, details.CheckpointRunscVersion},
		{"checkpoint_runsc_binary_path", artifacts.RunscBinaryPath, details.CheckpointRunscBinaryPath},
		{"checkpoint_runsc_binary_digest", artifacts.RunscBinaryDigest, details.CheckpointRunscBinaryDigest},
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

func runscContainerID(details store.RuntimeGenerationDetails) (string, error) {
	containerID := strings.TrimSpace(details.RunscContainerID)
	if containerID == "" {
		return "", fmt.Errorf("runsc container id is required")
	}
	return containerID, nil
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
	if strings.TrimSpace(req.Generation.SessionID) != "" && req.Generation.SessionID != req.SessionID {
		return fmt.Errorf("checkpoint generation session mismatch")
	}
	if strings.TrimSpace(req.Generation.GenerationID) != "" && req.Generation.GenerationID != req.GenerationID {
		return fmt.Errorf("checkpoint generation id mismatch")
	}
	if strings.TrimSpace(req.Generation.RunscContainerID) != "" && req.Generation.RunscContainerID != container.RunscContainerID {
		return fmt.Errorf("checkpoint runsc container mismatch")
	}
	generationCheckpointPath := strings.TrimSpace(req.Generation.CheckpointPath)
	checkpointPath := strings.TrimSpace(req.CheckpointPath)
	if checkpointPath == "" {
		checkpointPath = generationCheckpointPath
	}
	if checkpointPath == "" {
		return errors.New("generation checkpoint path is required")
	}
	if !filepath.IsAbs(checkpointPath) || filepath.Clean(checkpointPath) != checkpointPath {
		return fmt.Errorf("generation checkpoint path %q must be canonical absolute path", checkpointPath)
	}
	if generationCheckpointPath != "" && filepath.Clean(generationCheckpointPath) != checkpointPath {
		return fmt.Errorf("checkpoint path mismatch: got %q want generation path %q", checkpointPath, generationCheckpointPath)
	}
	currentRunsc, err := r.verifyGenerationRunscPin(ctx, "checkpoint", req.Generation)
	if err != nil {
		return err
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
	cmd := exec.CommandContext(ctx, currentRunsc.BinaryPath,
		"-root", r.cfg.RunscRoot,
		"-overlay2", r.runscOverlay2(req.Generation),
		"checkpoint",
		"-image-path", checkpointPath,
		container.RunscContainerID,
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
