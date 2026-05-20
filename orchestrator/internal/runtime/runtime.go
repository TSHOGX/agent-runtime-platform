package runtime

import (
	"bufio"
	"context"
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
	Cmd       *exec.Cmd
	Stdin     io.WriteCloser
	Stdout    io.ReadCloser
	Stderr    io.ReadCloser
	Cancel    context.CancelFunc
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
		return r.sendMessage(ctx, container, req.FirstMessage, output)
	}

	// Check if checkpoint exists (resume path)
	checkpointPath := filepath.Join(r.cfg.CheckpointsRoot, req.SessionID)
	if _, err := os.Stat(checkpointPath); err == nil {
		return r.resumeFromCheckpoint(ctx, req, output)
	}

	// Fresh start (cold path)
	return r.startFresh(ctx, req, output)
}

func scanLines(wg *sync.WaitGroup, r io.Reader, stream string, emit func(Output)) {
	defer wg.Done()
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		emit(Output{Stream: stream, Line: scanner.Text()})
	}
}

func (r *Runtime) Destroy(ctx context.Context, restoreID string) error {
	if restoreID == "" {
		return errors.New("restore id is required")
	}
	kill := exec.CommandContext(ctx, "runsc", "-root", r.cfg.RunscRoot, "kill", restoreID, "KILL")
	deleteCmd := exec.CommandContext(ctx, "runsc", "-root", r.cfg.RunscRoot, "delete", restoreID)
	_ = kill.Run()
	deleteErr := deleteCmd.Run()
	if deleteErr != nil {
		return fmt.Errorf("runsc delete %s: %w", restoreID, deleteErr)
	}
	return nil
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

func (r *Runtime) sendMessage(ctx context.Context, container *Container, message string, output func(Output)) Result {
	// Write message to stdin
	if _, err := fmt.Fprintln(container.Stdin, message); err != nil {
		r.mu.Lock()
		delete(r.containers, container.SessionID)
		r.mu.Unlock()
		return Result{Err: fmt.Errorf("write to stdin: %w", err)}
	}

	// Output is already being streamed by the goroutines started in startFresh/resumeFromCheckpoint
	return Result{}
}

func (r *Runtime) startFresh(ctx context.Context, req StartRequest, output func(Output)) Result {
	emit := func(out Output) {
		if output != nil {
			output(out)
		}
	}
	emit(Output{Stream: "runtime", Line: "starting fresh container"})

	// Create control file in the shared control directory
	controlDir := "/var/lib/harness/control/phase2-template"
	if err := os.MkdirAll(controlDir, 0o755); err != nil {
		return Result{Err: fmt.Errorf("create control dir: %w", err)}
	}

	controlFile := filepath.Join(controlDir, "session.env")
	controlContent := fmt.Sprintf(`SESSION_ID=%s
HARNESS_AGENT=%s
CLAUDE_SESSION_UUID=%s
SESSION_WORKSPACE=%s
`, req.SessionID, req.Agent, req.ClaudeSessionUUID, filepath.Join(r.cfg.SessionsRoot, req.SessionID))

	if err := os.WriteFile(controlFile, []byte(controlContent), 0o644); err != nil {
		return Result{Err: fmt.Errorf("write control file: %w", err)}
	}

	// Build runsc run command
	containerID := fmt.Sprintf("harness-%s", req.SessionID)
	bundlePath := filepath.Join(r.cfg.BundleRoot, "phase2-template-bundle")

	cmdCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(cmdCtx, "runsc",
		"-root", r.cfg.RunscRoot,
		"-platform", "systrap",
		"-network", "sandbox",
		"run",
		"-bundle", bundlePath,
		containerID,
	)

	// Get pipes
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return Result{Err: fmt.Errorf("stdin pipe: %w", err)}
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return Result{Err: fmt.Errorf("stdout pipe: %w", err)}
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return Result{Err: fmt.Errorf("stderr pipe: %w", err)}
	}

	if err := cmd.Start(); err != nil {
		cancel()
		return Result{Err: fmt.Errorf("runsc run start: %w", err)}
	}

	// Store container
	container := &Container{
		SessionID: req.SessionID,
		RestoreID: containerID,
		Cmd:       cmd,
		Stdin:     stdin,
		Stdout:    stdout,
		Stderr:    stderr,
		Cancel:    cancel,
	}

	r.mu.Lock()
	r.containers[req.SessionID] = container
	r.mu.Unlock()

	// Start streaming output
	var wg sync.WaitGroup
	wg.Add(2)
	go scanLines(&wg, stdout, "stdout", emit)
	go scanLines(&wg, stderr, "stderr", emit)

	// Monitor container exit in background
	go func() {
		wg.Wait()
		cmd.Wait()
		r.mu.Lock()
		delete(r.containers, req.SessionID)
		r.mu.Unlock()
	}()

	// Send first message
	if req.FirstMessage != "" {
		if _, err := fmt.Fprintln(stdin, req.FirstMessage); err != nil {
			return Result{Err: fmt.Errorf("write first message: %w", err)}
		}
	}

	return Result{}
}

func (r *Runtime) resumeFromCheckpoint(ctx context.Context, req StartRequest, output func(Output)) Result {
	emit := func(out Output) {
		if output != nil {
			output(out)
		}
	}
	emit(Output{Stream: "runtime", Line: "resuming from checkpoint"})

	// Similar to startFresh but use runsc restore
	checkpointPath := filepath.Join(r.cfg.CheckpointsRoot, req.SessionID)
	containerID := fmt.Sprintf("harness-%s", req.SessionID)
	bundlePath := filepath.Join(r.cfg.BundleRoot, "phase2-template-bundle")

	cmdCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(cmdCtx, "runsc",
		"-root", r.cfg.RunscRoot,
		"-platform", "systrap",
		"-network", "sandbox",
		"restore",
		"-bundle", bundlePath,
		"-image-path", checkpointPath,
		containerID,
	)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return Result{Err: fmt.Errorf("stdin pipe: %w", err)}
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return Result{Err: fmt.Errorf("stdout pipe: %w", err)}
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return Result{Err: fmt.Errorf("stderr pipe: %w", err)}
	}

	if err := cmd.Start(); err != nil {
		cancel()
		return Result{Err: fmt.Errorf("runsc restore start: %w", err)}
	}

	container := &Container{
		SessionID: req.SessionID,
		RestoreID: containerID,
		Cmd:       cmd,
		Stdin:     stdin,
		Stdout:    stdout,
		Stderr:    stderr,
		Cancel:    cancel,
	}

	r.mu.Lock()
	r.containers[req.SessionID] = container
	r.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(2)
	go scanLines(&wg, stdout, "stdout", emit)
	go scanLines(&wg, stderr, "stderr", emit)

	go func() {
		wg.Wait()
		cmd.Wait()
		r.mu.Lock()
		delete(r.containers, req.SessionID)
		r.mu.Unlock()
	}()

	if req.FirstMessage != "" {
		if _, err := fmt.Fprintln(stdin, req.FirstMessage); err != nil {
			return Result{Err: fmt.Errorf("write first message: %w", err)}
		}
	}

	return Result{}
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
