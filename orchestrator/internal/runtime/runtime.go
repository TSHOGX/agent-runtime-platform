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
)

type Config struct {
	RestoreScript   string
	RunscRoot       string
	SessionsRoot    string
	CheckpointsRoot string
	BundleRoot      string
	DefaultAgent    string
}

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
	OutputHub *OutputHub // Per-container pub/sub for output events
}

func New(cfg Config) *Runtime {
	return &Runtime{
		cfg:        cfg,
		containers: make(map[string]*Container),
	}
}

func (r *Runtime) Start(ctx context.Context, req StartRequest, output func(Output)) Result {
	if req.Agent == "" {
		req.Agent = r.cfg.DefaultAgent
	}

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

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func buildControlContent(req StartRequest, sessionsRoot string) string {
	containerSessionsRoot := getenv("HARNESS_CONTAINER_SESSIONS_ROOT", "/sessions")
	claudeBaseURL := firstNonEmpty(
		os.Getenv("HARNESS_CLAUDE_BASE_URL"),
		os.Getenv("HARNESS_ANTHROPIC_BASE_URL"),
		os.Getenv("ANTHROPIC_BASE_URL"),
		"http://127.0.0.1:8082",
	)
	claudeAPIKey := firstNonEmpty(
		os.Getenv("HARNESS_CLAUDE_API_KEY"),
		os.Getenv("HARNESS_CLAUDE_AUTH_TOKEN"),
		os.Getenv("HARNESS_ANTHROPIC_API_KEY"),
		os.Getenv("HARNESS_ANTHROPIC_AUTH_TOKEN"),
		"123",
	)

	lines := []struct {
		key   string
		value string
	}{
		{"SESSION_ID", req.SessionID},
		{"HARNESS_AGENT", req.Agent},
		{"CLAUDE_SESSION_UUID", req.ClaudeSessionUUID},
		{"CLAUDE_RESUME", boolEnv(req.ResumeClaude)},
		{"SESSION_WORKSPACE", filepath.Join(containerSessionsRoot, req.SessionID)},
		{"HARNESS_AGENT_HOME", getenv("HARNESS_AGENT_HOME", "/var/lib/harness-agent")},
		{"CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", getenv("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1")},
		{"CLAUDE_MODEL", getenv("CLAUDE_MODEL", "sonnet")},
		{"ANTHROPIC_BASE_URL", claudeBaseURL},
		{"ANTHROPIC_API_KEY", claudeAPIKey},
		{"ANTHROPIC_AUTH_TOKEN", claudeAPIKey},
	}

	var b strings.Builder
	for _, line := range lines {
		b.WriteString("export ")
		b.WriteString(line.key)
		b.WriteByte('=')
		b.WriteString(shellQuote(line.value))
		b.WriteByte('\n')
	}
	return b.String()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func boolEnv(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
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

// writeUserTurn delivers a user message to the agent's stdin.
//
// The claude agent runs with `--input-format stream-json`, which expects one
// JSONL frame per turn and keeps stdin open between turns. Other agents (sh,
// demo) consume raw text lines. The server holds the session in running_active
// until the current turn's parser sees a completion event.
func writeUserTurn(stdin io.Writer, agent, message string) error {
	if agent == "claude" {
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
	_, err := fmt.Fprintln(stdin, message)
	return err
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

	if err := writeUserTurn(container.Stdin, container.Agent, message); err != nil {
		r.mu.Lock()
		delete(r.containers, container.SessionID)
		r.mu.Unlock()
		return Result{Err: fmt.Errorf("write to stdin: %w", err)}
	}

	return forwardOutput(ctx, outputCh, done, output)
}

func (r *Runtime) startFresh(ctx context.Context, req StartRequest, output func(Output)) Result {
	hub := NewOutputHub()
	outputCh, cancel := hub.Subscribe()
	defer cancel()

	hub.Publish(OutputEvent{Stream: "runtime", Line: "starting fresh container"})

	// Create control file in the shared control directory
	controlDir := "/var/lib/harness/control/phase2-template"
	if err := os.MkdirAll(controlDir, 0o755); err != nil {
		return Result{Err: fmt.Errorf("create control dir: %w", err)}
	}

	controlFile := filepath.Join(controlDir, "session.env")
	controlContent := buildControlContent(req, r.cfg.SessionsRoot)

	if err := os.WriteFile(controlFile, []byte(controlContent), 0o644); err != nil {
		return Result{Err: fmt.Errorf("write control file: %w", err)}
	}

	// Build runsc run command
	containerID := fmt.Sprintf("phase3-%s", req.SessionID)
	bundlePath := filepath.Join(r.cfg.BundleRoot, "phase2-template-bundle")
	r.cleanupRunscContainer(ctx, containerID)

	cmdCtx, cancelCmd := context.WithCancel(ctx)
	cmd := exec.CommandContext(cmdCtx, "runsc",
		"-root", r.cfg.RunscRoot,
		"-platform", "systrap",
		"-network", "host",
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
		if err := writeUserTurn(stdin, req.Agent, req.FirstMessage); err != nil {
			return Result{Err: fmt.Errorf("write first message: %w", err)}
		}
	}

	return forwardOutput(ctx, outputCh, req.Done, output)
}

func (r *Runtime) resumeFromCheckpoint(ctx context.Context, req StartRequest, output func(Output)) Result {
	hub := NewOutputHub()
	outputCh, cancel := hub.Subscribe()
	defer cancel()

	hub.Publish(OutputEvent{Stream: "runtime", Line: "resuming from checkpoint"})

	// Similar to startFresh but use runsc restore
	checkpointPath := filepath.Join(r.cfg.CheckpointsRoot, req.SessionID)
	containerID := fmt.Sprintf("phase3-%s", req.SessionID)
	bundlePath := filepath.Join(r.cfg.BundleRoot, "phase2-template-bundle")
	r.cleanupRunscContainer(ctx, containerID)

	cmdCtx, cancelCmd := context.WithCancel(ctx)
	cmd := exec.CommandContext(cmdCtx, "runsc",
		"-root", r.cfg.RunscRoot,
		"-platform", "systrap",
		"-network", "host",
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
		if err := writeUserTurn(stdin, req.Agent, req.FirstMessage); err != nil {
			return Result{Err: fmt.Errorf("write first message: %w", err)}
		}
	}

	return forwardOutput(ctx, outputCh, req.Done, output)
}

func (r *Runtime) Checkpoint(ctx context.Context, sessionID string) error {
	r.mu.Lock()
	container, exists := r.containers[sessionID]
	if exists {
		delete(r.containers, sessionID)
	}
	r.mu.Unlock()

	if !exists {
		return errors.New("container not found")
	}

	checkpointPath := filepath.Join(r.cfg.CheckpointsRoot, sessionID)
	if err := os.MkdirAll(filepath.Dir(checkpointPath), 0o755); err != nil {
		return fmt.Errorf("create checkpoint dir: %w", err)
	}

	// Create checkpoint
	cmd := exec.CommandContext(ctx, "runsc",
		"-root", r.cfg.RunscRoot,
		"checkpoint",
		"-image-path", checkpointPath,
		container.RestoreID,
	)

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("runsc checkpoint: %w", err)
	}

	// Kill and delete container
	container.Cancel()
	_ = container.Cmd.Wait()

	deleteCmd := exec.CommandContext(ctx, "runsc",
		"-root", r.cfg.RunscRoot,
		"delete",
		container.RestoreID,
	)
	_ = deleteCmd.Run()

	return nil
}
