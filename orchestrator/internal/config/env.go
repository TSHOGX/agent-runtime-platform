package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func getenv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func maxSessionsEnv(defaultValue int) (int, error) {
	raw, ok := os.LookupEnv("HARNESS_MAX_SESSIONS")
	if !ok {
		return defaultValue, nil
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("invalid HARNESS_MAX_SESSIONS %q: %w", raw, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("invalid HARNESS_MAX_SESSIONS %q: must be > 0", raw)
	}
	return value, nil
}

func boolEnv(key string) (bool, bool, error) {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return false, false, nil
	}
	value := strings.TrimSpace(raw)
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, false, fmt.Errorf("invalid %s %q: %w", key, raw, err)
	}
	return parsed, true, nil
}

func sessionRetentionEnv(defaultValue time.Duration) (time.Duration, error) {
	if _, ok := os.LookupEnv("HARNESS_SESSION_TTL"); ok {
		return 0, fmt.Errorf("HARNESS_SESSION_TTL has been removed; use HARNESS_SESSION_RETENTION")
	}
	value, ok := os.LookupEnv("HARNESS_SESSION_RETENTION")
	if !ok {
		return defaultValue, nil
	}
	duration, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("invalid HARNESS_SESSION_RETENTION duration %q: %w", value, err)
	}
	return duration, nil
}
