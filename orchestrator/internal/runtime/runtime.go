package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

type Config struct {
	RestoreScript string
	RunscRoot     string
	SessionsRoot  string
	DefaultAgent  string
}

type StartRequest struct {
	SessionID    string
	RestoreID    string
	Agent        string
	FirstMessage string
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
	cfg Config
}

func New(cfg Config) *Runtime {
	return &Runtime{cfg: cfg}
}

func (r *Runtime) Start(ctx context.Context, req StartRequest, output func(Output)) Result {
	if req.Agent == "" {
		req.Agent = r.cfg.DefaultAgent
	}
	if output != nil {
		output(Output{
			Stream: "runtime",
			Line:   "starting restore via phase 2 script",
		})
	}
	cmd := exec.CommandContext(ctx, r.cfg.RestoreScript)
	cmd.Dir = filepath.Dir(filepath.Dir(r.cfg.RestoreScript))
	cmd.Env = append(os.Environ(),
		"SESSION_ID="+req.SessionID,
		"RESTORE_ID="+req.RestoreID,
		"HARNESS_AGENT="+req.Agent,
		"FIRST_MESSAGE="+req.FirstMessage,
		"HARNESS_COMMAND="+req.FirstMessage,
		"DETACH=0",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return Result{Err: fmt.Errorf("restore script failed: %w", err)}
	}

	restoreMS := r.readRestoreMS(req.SessionID)
	return Result{RestoreMS: restoreMS}
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
