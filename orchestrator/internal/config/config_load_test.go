package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckedInHarnessConfigLoads(t *testing.T) {
	cfg, err := loadProjectConfig(filepath.Join("..", "..", "..", "config", "harness.yaml"))
	if err != nil {
		t.Fatalf("load checked-in harness config: %v", err)
	}
	if err := validateHarnessConfig(cfg.Harness); err != nil {
		t.Fatalf("validate checked-in harness config: %v", err)
	}
}

func TestLoadValidatesMergedHarnessConfig(t *testing.T) {
	repo := writeMinimalLoadConfig(t)
	chdirForLoadTest(t, repo)
	unsetEnvForTest(t, "HARNESS_SESSION_TTL")
	t.Setenv("HARNESS_SESSION_RETENTION", "-1s")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "harness.session_retention must be >= 0") {
		t.Fatalf("expected merged validation error, got %v", err)
	}
}

func TestLoadAutoCheckpointEnvOverride(t *testing.T) {
	repo := writeMinimalLoadConfig(t)
	chdirForLoadTest(t, repo)
	t.Setenv("HARNESS_AUTO_CHECKPOINT_ENABLED", "true")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !cfg.Harness.Checkpoint.AutoEnabled {
		t.Fatalf("expected env override to enable checkpoint policy: %+v", cfg.Harness.Checkpoint)
	}
}

func TestLoadRejectsInvalidMaxSessionsEnv(t *testing.T) {
	tests := []string{"many", "0", "-1"}

	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			repo := writeMinimalLoadConfig(t)
			chdirForLoadTest(t, repo)
			t.Setenv("HARNESS_MAX_SESSIONS", value)

			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), "invalid HARNESS_MAX_SESSIONS") {
				t.Fatalf("expected invalid max sessions env error, got %v", err)
			}
		})
	}
}

func TestLoadRejectsInvalidAutoCheckpointEnv(t *testing.T) {
	repo := writeMinimalLoadConfig(t)
	chdirForLoadTest(t, repo)
	t.Setenv("HARNESS_AUTO_CHECKPOINT_ENABLED", "maybe")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "invalid HARNESS_AUTO_CHECKPOINT_ENABLED") {
		t.Fatalf("expected invalid auto checkpoint env error, got %v", err)
	}
}

func TestLoadRejectsShellDefaultAgent(t *testing.T) {
	repo := writeMinimalLoadConfig(t)
	chdirForLoadTest(t, repo)
	unsetEnvForTest(t, "HARNESS_SESSION_TTL")
	t.Setenv("HARNESS_DEFAULT_AGENT", "sh")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "default agent must be an agent-capable driver") {
		t.Fatalf("expected default agent validation error, got %v", err)
	}
}

func TestLoadDefaultDBPathIsOutsideSessionsRoot(t *testing.T) {
	repo := writeMinimalLoadConfig(t)
	chdirForLoadTest(t, repo)
	t.Setenv("HARNESS_SESSIONS_ROOT", filepath.Join(t.TempDir(), "sessions"))
	t.Setenv("HARNESS_DB_PATH", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if isolationPathWithin(cfg.DBPath, cfg.SessionsRoot) {
		t.Fatalf("default DB path %q must not be under sessions root %q", cfg.DBPath, cfg.SessionsRoot)
	}
	if _, err := ValidateIsolationRoots(cfg.IsolationRoots()); err != nil {
		t.Fatalf("default roots should satisfy isolation validation: %v", err)
	}
}

func TestLoadProjectConfigRejectsMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.yaml")
	_, err := loadProjectConfig(path)
	if err == nil || !strings.Contains(err.Error(), "config file is required") {
		t.Fatalf("expected missing config rejection, got %v", err)
	}
}

func TestLoadProjectConfigRejectsEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "harness.yaml")
	if err := os.WriteFile(path, []byte(" \n\t\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	_, err := loadProjectConfig(path)
	if err == nil || !strings.Contains(err.Error(), "config file is empty") {
		t.Fatalf("expected empty config rejection, got %v", err)
	}
}
