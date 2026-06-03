package config

import (
	"strings"
	"testing"
	"time"
)

func TestValidateHarnessConfigAllowsZeroSessionRetention(t *testing.T) {
	cfg := testHarnessConfig()
	cfg.SessionRetention.Duration = 0

	if err := validateHarnessConfig(cfg); err != nil {
		t.Fatalf("zero session retention should be valid: %v", err)
	}
}

func TestSessionRetentionEnvRejectsRemovedSessionTTL(t *testing.T) {
	unsetEnvForTest(t, "HARNESS_SESSION_RETENTION")
	t.Setenv("HARNESS_SESSION_TTL", "2h")

	_, err := sessionRetentionEnv(time.Hour)
	if err == nil || !strings.Contains(err.Error(), "HARNESS_SESSION_TTL has been removed; use HARNESS_SESSION_RETENTION") {
		t.Fatalf("expected removed env error, got %v", err)
	}
}

func TestSessionRetentionEnvRejectsRemovedSessionTTLEvenWithReplacement(t *testing.T) {
	t.Setenv("HARNESS_SESSION_TTL", "2h")
	t.Setenv("HARNESS_SESSION_RETENTION", "720h")

	_, err := sessionRetentionEnv(time.Hour)
	if err == nil || !strings.Contains(err.Error(), "HARNESS_SESSION_TTL has been removed") {
		t.Fatalf("expected removed env error, got %v", err)
	}
}

func TestSessionRetentionEnvStrictParsing(t *testing.T) {
	unsetEnvForTest(t, "HARNESS_SESSION_TTL")
	tests := []struct {
		name  string
		value string
		want  time.Duration
		err   string
	}{
		{name: "zero", value: "0s", want: 0},
		{name: "normal", value: "720h", want: 720 * time.Hour},
		{name: "days rejected", value: "30d", err: "invalid HARNESS_SESSION_RETENTION duration"},
		{name: "typo rejected", value: "forever", err: "invalid HARNESS_SESSION_RETENTION duration"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HARNESS_SESSION_RETENTION", tt.value)
			got, err := sessionRetentionEnv(time.Hour)
			if tt.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("expected %q error, got %v", tt.err, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("session retention env: %v", err)
			}
			if got != tt.want {
				t.Fatalf("retention=%s want %s", got, tt.want)
			}
		})
	}
}

func TestLoadRejectsRemovedSessionTTLEnv(t *testing.T) {
	repo := writeMinimalLoadConfig(t)
	chdirForLoadTest(t, repo)
	t.Setenv("HARNESS_SESSION_TTL", "2h")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "HARNESS_SESSION_TTL has been removed; use HARNESS_SESSION_RETENTION") {
		t.Fatalf("expected removed env load error, got %v", err)
	}
}

func TestLoadRejectsInvalidSessionRetentionEnv(t *testing.T) {
	repo := writeMinimalLoadConfig(t)
	chdirForLoadTest(t, repo)
	unsetEnvForTest(t, "HARNESS_SESSION_TTL")
	t.Setenv("HARNESS_SESSION_RETENTION", "30d")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "invalid HARNESS_SESSION_RETENTION duration") {
		t.Fatalf("expected invalid retention env load error, got %v", err)
	}
}
