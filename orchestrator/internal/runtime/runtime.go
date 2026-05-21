package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"harness-platform/orchestrator/internal/agents"
)

type Config struct {
	RestoreScript   string
	RunscRoot       string
	RunscNetwork    string
	RunscOverlay2   string
	SessionsRoot    string
	AgentHomesRoot  string
	CheckpointsRoot string
	BundleRoot      string
	DefaultAgent    string
}

const (
	controlFileName                         = "session.json"
	runscSandboxNetnsName                   = "phase1-demo"
	runscSandboxNetnsInterface              = "gv-phase1"
	runscSandboxNetnsCIDR                   = "10.200.1.2/24"
	runscSandboxGatewayIP                   = "10.200.1.1"
	controlProxyBindURL                     = "http://0.0.0.0:8082"
	controlClaudeBaseURLSandbox             = "http://" + runscSandboxGatewayIP + ":8082"
	controlClaudeAPIKey                     = "123"
	controlClaudeModel                      = "sonnet"
	controlClaudeOutputFormat               = "stream-json"
	controlClaudeDisableNonessentialTraffic = true
)

type StartRequest struct {
	SessionID         string
	RestoreID         string
	Agent             string
	FirstMessage      string
	ClaudeSessionUUID string
	ResumeClaude      bool
	Done              <-chan struct{}
}

type Output struct {
	Stream string `json:"stream"`
	Line   string `json:"line"`
}

type Result struct {
	RestoreMS *int64
	Err       error
}

type controlManifest struct {
	SessionID                            string `json:"session_id"`
	SessionWorkspace                     string `json:"session_workspace"`
	HarnessAgentHome                     string `json:"harness_agent_home"`
	HarnessAgent                         string `json:"harness_agent"`
	ClaudeSessionUUID                    string `json:"claude_session_uuid,omitempty"`
	ClaudeResume                         bool   `json:"claude_resume"`
	ProxyBindURL                         string `json:"proxy_bind_url"`
	AnthropicBaseURL                     string `json:"anthropic_base_url"`
	AnthropicAPIKey                      string `json:"anthropic_api_key"`
	AnthropicAuthToken                   string `json:"anthropic_auth_token"`
	ClaudeCodeDisableNonessentialTraffic bool   `json:"claude_code_disable_nonessential_traffic"`
	ClaudeModel                          string `json:"claude_model"`
	ClaudeOutputFormat                   string `json:"claude_output_format"`
}

type Runtime struct {
	cfg        Config
	mu         sync.RWMutex
	containers map[string]*Container
}

type Container struct {
	SessionID string
	RestoreID string
	Agent     string
	Cmd       *exec.Cmd
	Stdin     io.WriteCloser
	Stdout    io.ReadCloser
	Stderr    io.ReadCloser
	Cancel    context.CancelFunc
	InputMu   sync.Mutex
	OutputHub *OutputHub // Per-container pub/sub for output events
}

func New(cfg Config) *Runtime {
	return &Runtime{
		cfg:        cfg,
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
		return r.sendMessage(ctx, container, req.FirstMessage, req.Done, output)
	}

	// Check if checkpoint exists (resume path)
	checkpointPath := filepath.Join(r.cfg.CheckpointsRoot, req.SessionID)
	if _, err := os.Stat(checkpointPath); err == nil {
		return r.resumeFromCheckpoint(ctx, req, output)
	}

	// Fresh start (cold path)
	return r.startFresh(ctx, req, output)
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

func buildControlContent(req StartRequest, runscNetwork string) string {
	sessionWorkspace := filepath.Join("/sessions", req.SessionID)
	agentHome := filepath.Join("/agent-homes", req.SessionID)
	manifest := controlManifest{
		SessionID:                            req.SessionID,
		SessionWorkspace:                     sessionWorkspace,
		HarnessAgentHome:                     agentHome,
		HarnessAgent:                         req.Agent,
		ClaudeSessionUUID:                    req.ClaudeSessionUUID,
		ClaudeResume:                         req.ResumeClaude,
		ProxyBindURL:                         controlProxyBindURL,
		AnthropicBaseURL:                     defaultClaudeBaseURL(runscNetwork),
		AnthropicAPIKey:                      controlClaudeAPIKey,
		AnthropicAuthToken:                   controlClaudeAPIKey,
		ClaudeCodeDisableNonessentialTraffic: controlClaudeDisableNonessentialTraffic,
		ClaudeModel:                          controlClaudeModel,
		ClaudeOutputFormat:                   controlClaudeOutputFormat,
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return ""
	}
	return string(data) + "\n"
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

func defaultClaudeBaseURL(runscNetwork string) string {
	if strings.EqualFold(strings.TrimSpace(runscNetwork), "host") {
		return controlProxyBindURL
	}
	return controlClaudeBaseURLSandbox
}

func (r *Runtime) ensureSandboxNetwork(ctx context.Context) error {
	if !strings.EqualFold(strings.TrimSpace(r.cfg.RunscNetwork), "sandbox") {
		return nil
	}

	commands := [][]string{
		{"ip", "netns", "exec", runscSandboxNetnsName, "ip", "addr", "replace", runscSandboxNetnsCIDR, "dev", runscSandboxNetnsInterface},
		{"ip", "netns", "exec", runscSandboxNetnsName, "ip", "link", "set", runscSandboxNetnsInterface, "up"},
		{"ip", "netns", "exec", runscSandboxNetnsName, "ip", "route", "replace", "default", "via", runscSandboxGatewayIP, "dev", runscSandboxNetnsInterface},
	}
	for _, args := range commands {
		cmd := exec.CommandContext(ctx, args[0], args[1:]...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("configure sandbox network %q: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
		}
	}
	return nil
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

	if err := r.prepareSessionDirs(req.SessionID); err != nil {
		return Result{Err: err}
	}
	if err := r.ensureSandboxNetwork(ctx); err != nil {
		return Result{Err: err}
	}

	// Create control file in the shared control directory
	controlDir := "/var/lib/harness/control/phase2-template"
	if err := os.MkdirAll(controlDir, 0o755); err != nil {
		return Result{Err: fmt.Errorf("create control dir: %w", err)}
	}

	controlFile := filepath.Join(controlDir, controlFileName)
	controlContent := buildControlContent(req, r.cfg.RunscNetwork)

	if err := os.WriteFile(controlFile, []byte(controlContent), 0o644); err != nil {
		return Result{Err: fmt.Errorf("write control file: %w", err)}
	}

	// Build runsc run command
	containerID := fmt.Sprintf("phase3-%s", req.SessionID)
	bundlePath := filepath.Join(r.cfg.BundleRoot, "phase2-template-bundle")
	r.cleanupRunscContainer(ctx, containerID)

	cmdCtx, cancelCmd := context.WithCancel(context.Background())
	cmd := exec.CommandContext(cmdCtx, "runsc",
		"-root", r.cfg.RunscRoot,
		"-platform", "systrap",
		"-overlay2", r.cfg.RunscOverlay2,
		"-network", r.cfg.RunscNetwork,
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
		SessionID: req.SessionID,
		RestoreID: containerID,
		Agent:     req.Agent,
		Cmd:       cmd,
		Stdin:     stdin,
		Stdout:    stdout,
		Stderr:    stderr,
		Cancel:    cancelCmd,
		OutputHub: hub,
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
		r.cleanupRunscContainer(context.Background(), containerID)
		hub.Close() // Close hub when container exits
		r.mu.Lock()
		delete(r.containers, req.SessionID)
		r.mu.Unlock()
	}()

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
	return result
}

func (r *Runtime) resumeFromCheckpoint(ctx context.Context, req StartRequest, output func(Output)) Result {
	hub := NewOutputHub()
	outputCh, cancel := hub.Subscribe()
	defer cancel()

	hub.Publish(OutputEvent{Stream: "runtime", Line: "resuming from checkpoint"})

	if err := r.prepareSessionDirs(req.SessionID); err != nil {
		return Result{Err: err}
	}
	if err := r.ensureSandboxNetwork(ctx); err != nil {
		return Result{Err: err}
	}

	// Similar to startFresh but use runsc restore
	checkpointPath := filepath.Join(r.cfg.CheckpointsRoot, req.SessionID)
	containerID := fmt.Sprintf("phase3-%s", req.SessionID)
	bundlePath := filepath.Join(r.cfg.BundleRoot, "phase2-template-bundle")
	r.cleanupRunscContainer(ctx, containerID)

	cmdCtx, cancelCmd := context.WithCancel(context.Background())
	cmd := exec.CommandContext(cmdCtx, "runsc",
		"-root", r.cfg.RunscRoot,
		"-platform", "systrap",
		"-overlay2", r.cfg.RunscOverlay2,
		"-network", r.cfg.RunscNetwork,
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
		SessionID: req.SessionID,
		RestoreID: containerID,
		Agent:     req.Agent,
		Cmd:       cmd,
		Stdin:     stdin,
		Stdout:    stdout,
		Stderr:    stderr,
		Cancel:    cancelCmd,
		OutputHub: hub,
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
		r.cleanupRunscContainer(context.Background(), containerID)
		hub.Close() // Close hub when container exits
		r.mu.Lock()
		delete(r.containers, req.SessionID)
		r.mu.Unlock()
	}()

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
	return result
}

func (r *Runtime) Checkpoint(ctx context.Context, sessionID string) error {
	r.mu.RLock()
	container, exists := r.containers[sessionID]
	r.mu.RUnlock()

	if !exists {
		return errors.New("container not found")
	}

	checkpointPath := filepath.Join(r.cfg.CheckpointsRoot, sessionID)
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

	if err := cmd.Run(); err != nil {
		_ = os.RemoveAll(checkpointPath)
		return fmt.Errorf("runsc checkpoint: %w", err)
	}

	r.mu.Lock()
	if current := r.containers[sessionID]; current == container {
		delete(r.containers, sessionID)
	}
	r.mu.Unlock()

	// Kill and delete container
	container.Cancel()
	_ = container.Cmd.Wait()

	deleteCmd := exec.CommandContext(ctx, "runsc",
		"-root", r.cfg.RunscRoot,
		"-overlay2", r.cfg.RunscOverlay2,
		"delete",
		container.RestoreID,
	)
	_ = deleteCmd.Run()

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
