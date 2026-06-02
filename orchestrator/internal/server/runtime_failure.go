package server

import (
	"net/http"
	"strings"
)

func runtimeFailureClass(message string) string {
	if strings.Contains(message, "sandbox_secret_disallowed") {
		return "sandbox_secret_disallowed"
	}
	if strings.Contains(message, "shell_secret_disallowed") {
		return "shell_secret_disallowed"
	}
	if strings.Contains(message, "control manifest digest mismatch") ||
		strings.Contains(message, "expected manifest_") ||
		strings.Contains(message, "expected session_id") ||
		strings.Contains(message, "expected generation_id") ||
		strings.Contains(message, "expected network_profile_id") ||
		strings.Contains(message, "expected agent_runtime_profile_id") ||
		strings.Contains(message, "expected anthropic_api_key_secret_id") ||
		strings.Contains(message, "expected anthropic_auth_token_secret_id") ||
		strings.Contains(message, "expected secret_version") ||
		strings.Contains(message, "secret mount") {
		return "manifest_digest_mismatch"
	}
	if strings.Contains(message, "pre-start sandbox network probe") {
		return "probe_failed_pre_start"
	}
	if strings.Contains(message, "harness-bridge-client probe") ||
		strings.Contains(message, "bridge probe") ||
		strings.Contains(message, "bridge startup probe") ||
		strings.Contains(message, "probe GET /healthz") ||
		strings.Contains(message, "probe POST /v1/messages") {
		return "probe_failed_post_start"
	}
	if strings.Contains(message, "configure sandbox network") {
		return "network_setup_failed"
	}
	return "runtime_failed"
}

func runtimeFailureMessage(errorClass, reason string) string {
	switch errorClass {
	case "probe_failed_pre_start":
		return "sandbox network probe failed before start"
	case "probe_failed_post_start":
		return "sandbox network probe failed after start"
	case "manifest_digest_mismatch":
		return "runtime manifest validation failed"
	case "network_setup_failed":
		return "sandbox network setup failed"
	case "sandbox_secret_disallowed":
		return "sandbox generation cannot mount model secrets"
	case "shell_secret_disallowed":
		return "shell agent cannot mount model secrets"
	default:
		if strings.TrimSpace(reason) != "" {
			return reason
		}
		return "runtime failed"
	}
}

func writeRuntimeStartError(w http.ResponseWriter, err error) {
	reason := ""
	if err != nil {
		reason = err.Error()
	}
	errorClass := runtimeFailureClass(reason)
	writeErrorClass(w, http.StatusInternalServerError, errorClass, runtimeFailureMessage(errorClass, reason))
}
