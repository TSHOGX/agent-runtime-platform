package config

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	AgentHomesRoot  string
	CheckpointsRoot string
	BundleRoot      string
	DBPath          string
	DefaultAgent    string
	MaxSessions     int
	RunscNetwork    string
	RunscOverlay2   string
	Claude          ClaudeConfig
}

type ClaudeConfig struct {
	ProxyBindURL               string
	SandboxBaseURL             string
	APIKey                     string
	AuthToken                  string
	Model                      string
	OutputFormat               string
	DisableNonessentialTraffic bool
}

func Load() (Config, error) {
	repoRoot, err := os.Getwd()
	if err != nil {
		return Config{}, err
	}
	if filepath.Base(repoRoot) == "orchestrator" {
		repoRoot = filepath.Dir(repoRoot)
	}
	projectConfig, err := loadProjectConfig(filepath.Join(repoRoot, "config", "harness.yaml"))
	if err != nil {
		return Config{}, err
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
		AgentHomesRoot:  getenv("HARNESS_AGENT_HOMES_ROOT", "/var/lib/harness/agent-homes"),
		CheckpointsRoot: getenv("HARNESS_CHECKPOINTS_ROOT", "/var/lib/harness/checkpoints"),
		BundleRoot:      getenv("HARNESS_BUNDLE_ROOT", filepath.Join(repoRoot, "bundle", "out")),
		DBPath:          getenv("HARNESS_DB_PATH", filepath.Join(sessionsRoot, "orchestrator.db")),
		DefaultAgent:    getenv("HARNESS_DEFAULT_AGENT", "claude"),
		MaxSessions:     intEnv("HARNESS_MAX_SESSIONS", 30),
		RunscNetwork:    defaultString(projectConfig.Runtime.RunscNetwork, "sandbox"),
		RunscOverlay2:   defaultString(projectConfig.Runtime.RunscOverlay2, "none"),
		Claude: ClaudeConfig{
			ProxyBindURL:               defaultString(projectConfig.Claude.ProxyBindURL, "http://0.0.0.0:8082"),
			SandboxBaseURL:             defaultString(projectConfig.Claude.SandboxBaseURL, "http://10.200.1.1:8082"),
			APIKey:                     defaultString(projectConfig.Claude.APIKey, "123"),
			AuthToken:                  defaultString(projectConfig.Claude.AuthToken, defaultString(projectConfig.Claude.APIKey, "123")),
			Model:                      defaultString(projectConfig.Claude.Model, "sonnet"),
			OutputFormat:               defaultString(projectConfig.Claude.OutputFormat, "stream-json"),
			DisableNonessentialTraffic: projectConfig.Claude.DisableNonessentialTraffic,
		},
	}
	return cfg, nil
}

type projectConfig struct {
	Runtime struct {
		RunscNetwork  string
		RunscOverlay2 string
	}
	Claude ClaudeConfig
}

func loadProjectConfig(path string) (projectConfig, error) {
	cfg := projectConfig{}
	cfg.Claude.DisableNonessentialTraffic = true

	f, err := os.Open(path)
	if err != nil && os.IsNotExist(err) {
		return cfg, nil
	}
	if err != nil {
		return cfg, err
	}
	defer f.Close()

	section := ""
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if idx := strings.Index(line, "#"); idx >= 0 {
			line = line[:idx]
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		if !strings.HasPrefix(line, " ") && strings.HasSuffix(strings.TrimSpace(line), ":") {
			section = strings.TrimSuffix(strings.TrimSpace(line), ":")
			continue
		}
		key, value, ok := parseYAMLKV(line)
		if !ok {
			return cfg, fmt.Errorf("invalid config line in %s: %q", path, scanner.Text())
		}
		switch section + "." + key {
		case "runtime.runsc_network":
			cfg.Runtime.RunscNetwork = value
		case "runtime.runsc_overlay2":
			cfg.Runtime.RunscOverlay2 = value
		case "claude.proxy_bind_url":
			cfg.Claude.ProxyBindURL = value
		case "claude.sandbox_base_url":
			cfg.Claude.SandboxBaseURL = value
		case "claude.api_key":
			cfg.Claude.APIKey = value
		case "claude.auth_token":
			cfg.Claude.AuthToken = value
		case "claude.model":
			cfg.Claude.Model = value
		case "claude.output_format":
			cfg.Claude.OutputFormat = value
		case "claude.disable_nonessential_traffic":
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return cfg, fmt.Errorf("invalid boolean for claude.disable_nonessential_traffic: %q", value)
			}
			cfg.Claude.DisableNonessentialTraffic = parsed
		default:
			return cfg, fmt.Errorf("unknown config key %q in section %q", key, section)
		}
	}
	return cfg, scanner.Err()
}

func parseYAMLKV(line string) (string, string, bool) {
	parts := strings.SplitN(strings.TrimSpace(line), ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	key := strings.TrimSpace(parts[0])
	value := strings.TrimSpace(parts[1])
	value = strings.Trim(value, `"'`)
	return key, value, key != ""
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
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
