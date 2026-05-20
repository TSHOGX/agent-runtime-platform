package config

import (
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type Config struct {
	Addr            string
	SharedSecret    string
	CookieName      string
	SessionTTL      time.Duration
	RepoRoot        string
	RestoreScript   string
	RunscRoot       string
	SessionsRoot    string
	CheckpointsRoot string
	BundleRoot      string
	DBPath          string
	DefaultAgent    string
	MaxSessions     int
}

func Load() (Config, error) {
	repoRoot, err := os.Getwd()
	if err != nil {
		return Config{}, err
	}
	if filepath.Base(repoRoot) == "orchestrator" {
		repoRoot = filepath.Dir(repoRoot)
	}

	sessionsRoot := getenv("HARNESS_SESSIONS_ROOT", "/var/lib/harness/sessions")
	cfg := Config{
		Addr:            getenv("HARNESS_ORCHESTRATOR_ADDR", ":8090"),
		SharedSecret:    os.Getenv("HARNESS_LAB_PASSWORD"),
		CookieName:      getenv("HARNESS_COOKIE_NAME", "harness_auth"),
		SessionTTL:      durationEnv("HARNESS_SESSION_TTL", 2*time.Hour),
		RepoRoot:        getenv("HARNESS_REPO_ROOT", repoRoot),
		RestoreScript:   getenv("HARNESS_RESTORE_SCRIPT", filepath.Join(repoRoot, "bundle", "restore-sandbox.sh")),
		RunscRoot:       getenv("RUNSC_ROOT", "/var/lib/harness/runsc"),
		SessionsRoot:    sessionsRoot,
		CheckpointsRoot: getenv("HARNESS_CHECKPOINTS_ROOT", "/var/lib/harness/checkpoints"),
		BundleRoot:      getenv("HARNESS_BUNDLE_ROOT", filepath.Join(repoRoot, "bundle", "out")),
		DBPath:          getenv("HARNESS_DB_PATH", filepath.Join(sessionsRoot, "orchestrator.db")),
		DefaultAgent:    getenv("HARNESS_DEFAULT_AGENT", "demo"),
		MaxSessions:     intEnv("HARNESS_MAX_SESSIONS", 30),
	}
	return cfg, nil
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func intEnv(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return duration
}
