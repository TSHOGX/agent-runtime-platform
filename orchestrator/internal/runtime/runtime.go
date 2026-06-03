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

type networkHostsProjection struct {
	Path    string
	Payload []byte
}

type GenerationArtifactProjection struct {
	Artifacts           GenerationArtifacts
	NetworkHosts        networkHostsProjection
	DriverConfig        driverConfigProjection
	RuntimeSpec         runtimeSpec
	ControlManifestFile controlManifestFile
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

func (r *Runtime) writeNetworkHostsProjection(details store.RuntimeGenerationDetails) error {
	rendered, err := renderOptionalNetworkHostsProjection(details)
	if err != nil {
		return err
	}
	if strings.TrimSpace(rendered.Path) == "" {
		return nil
	}
	if err := writeFileAtomic(rendered.Path, rendered.Payload, 0o644); err != nil {
		return fmt.Errorf("write network hosts projection: %w", err)
	}
	return nil
}

func renderOptionalNetworkHostsProjection(details store.RuntimeGenerationDetails) (networkHostsProjection, error) {
	path := strings.TrimSpace(details.NetworkHostsPath)
	if path == "" {
		return networkHostsProjection{}, nil
	}
	if details.NetworkHostsPath != path || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return networkHostsProjection{}, fmt.Errorf("network hosts path %q must be canonical absolute path", details.NetworkHostsPath)
	}
	payload, err := renderNetworkHostsProjection(details)
	if err != nil {
		return networkHostsProjection{}, err
	}
	return networkHostsProjection{Path: path, Payload: payload}, nil
}

type driverConfigProjection struct {
	Entries  []DriverConfigMaterialization
	Payloads map[string][]byte
}

func (r *Runtime) renderDriverConfigProjection(req StartRequest) (driverConfigProjection, error) {
	driver := agents.ID(strings.TrimSpace(driverID(req)))
	specs := agents.DriverConfigMaterializationSpecsFor(driver)
	renderer, ok := driveradapter.ConfigProjectionRendererFor(driver)
	if len(specs) == 0 {
		if ok {
			return driverConfigProjection{}, fmt.Errorf("%s driver config materialization specs are missing", driver)
		}
		return driverConfigProjection{}, nil
	}
	if !ok {
		return driverConfigProjection{}, fmt.Errorf("%s driver config projection renderer is missing", driver)
	}
	details := req.Generation
	if err := validateRuntimeArtifactPathEvidence("driver config", "control dir path", details.ControlDirPath); err != nil {
		return driverConfigProjection{}, err
	}
	payloads, err := renderer(details)
	if err != nil {
		return driverConfigProjection{}, err
	}
	entries := make([]DriverConfigMaterialization, 0, len(specs))
	for _, spec := range specs {
		if _, ok := payloads[spec.Name]; !ok {
			return driverConfigProjection{}, fmt.Errorf("%s %s config renderer is missing", driver, spec.Name)
		}
		entries = append(entries, DriverConfigMaterialization{
			Name:                        spec.Name,
			SourceProjectionPath:        spec.SourceProjectionPath,
			HostSourcePath:              spec.HostSourcePath(details.ControlDirPath),
			SandboxDestination:          spec.SandboxDestination,
			DestinationMutableBySandbox: spec.DestinationMutableBySandbox,
		})
	}
	renderedPayloads := make(map[string][]byte, len(entries))
	for i := range entries {
		payload, err := canonicalJSON(payloads[entries[i].Name])
		if err != nil {
			return driverConfigProjection{}, fmt.Errorf("render %s %s config: %w", driver, entries[i].Name, err)
		}
		entries[i].SourceDigest = prefixedSHA256(payload)
		renderedPayloads[entries[i].Name] = payload
	}
	return driverConfigProjection{Entries: entries, Payloads: renderedPayloads}, nil
}

func (r *Runtime) writeDriverConfigProjection(req StartRequest) ([]DriverConfigMaterialization, error) {
	rendered, err := r.renderDriverConfigProjection(req)
	if err != nil {
		return nil, err
	}
	for _, entry := range rendered.Entries {
		payload := rendered.Payloads[entry.Name]
		if err := writeFileAtomic(entry.HostSourcePath, payload, 0o644); err != nil {
			return nil, fmt.Errorf("write %s %s config: %w", driverID(req), entry.Name, err)
		}
	}
	return rendered.Entries, nil
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

func shortID(id string) (string, error) {
	token := strings.NewReplacer("gen_", "", "-", "").Replace(strings.TrimSpace(id))
	if len(token) > 12 {
		token = token[:12]
	}
	if token == "" || !hasASCIIAlnum(token) {
		return "", fmt.Errorf("short generation id is required")
	}
	return token, nil
}

func hasASCIIAlnum(value string) bool {
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return true
		}
	}
	return false
}

func joinInts(values []int) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.Itoa(value))
	}
	return strings.Join(parts, ",")
}

type bridgeProbeRuntimeConfig struct {
	bridgeMode      string
	heartbeat       time.Duration
	pollInterval    time.Duration
	healthzStatuses []int
}

func (r *Runtime) requiredBridgeProbeConfig() (bridgeProbeRuntimeConfig, error) {
	cfg := bridgeProbeRuntimeConfig{
		bridgeMode:      strings.TrimSpace(r.cfg.BridgeMode),
		heartbeat:       r.cfg.BridgeHeartbeat,
		pollInterval:    r.cfg.BridgePollInterval,
		healthzStatuses: append([]int(nil), r.cfg.ProbeHealthzStatuses...),
	}
	if cfg.bridgeMode == "" {
		return bridgeProbeRuntimeConfig{}, fmt.Errorf("bridge mode is required")
	}
	if cfg.heartbeat <= 0 {
		return bridgeProbeRuntimeConfig{}, fmt.Errorf("bridge heartbeat interval is required")
	}
	if cfg.pollInterval <= 0 {
		return bridgeProbeRuntimeConfig{}, fmt.Errorf("bridge poll interval is required")
	}
	if len(cfg.healthzStatuses) == 0 {
		return bridgeProbeRuntimeConfig{}, fmt.Errorf("probe healthz statuses are required")
	}
	for _, status := range cfg.healthzStatuses {
		if status < 100 || status > 599 {
			return bridgeProbeRuntimeConfig{}, fmt.Errorf("invalid probe healthz status %d", status)
		}
	}
	return cfg, nil
}

func formatSeconds(value time.Duration) string {
	if value%time.Second == 0 {
		return strconv.FormatInt(int64(value/time.Second), 10)
	}
	return strconv.FormatFloat(float64(value)/float64(time.Second), 'f', -1, 64)
}

func (r *Runtime) runscNetwork(details store.RuntimeGenerationDetails) (string, error) {
	network := strings.TrimSpace(details.RunscNetwork)
	if network == "" {
		return "", fmt.Errorf("runsc network is required")
	}
	return network, nil
}

func (r *Runtime) runscOverlay2(details store.RuntimeGenerationDetails) (string, error) {
	overlay2 := strings.TrimSpace(details.RunscOverlay2)
	if overlay2 == "" {
		return "", fmt.Errorf("runsc overlay2 is required")
	}
	return overlay2, nil
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
