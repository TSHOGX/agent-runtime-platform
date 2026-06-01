package store

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"harness-platform/orchestrator/internal/agents"
)

const (
	piDriverStateKindUninitialized = "pi_uninitialized"
	piDriverStateKindSession       = "pi_session"
)

func bootstrapPiDriverState(driverStateBootstrapContext) (any, error) {
	return map[string]any{
		"schema_version": 1,
		"driver_id":      string(agents.Pi),
		"state_kind":     piDriverStateKindUninitialized,
		"session_dir":    agents.PiSessionDir,
	}, nil
}

func validatePiDriverStatePayload(object map[string]any) error {
	if sessionDir, _ := object["session_dir"].(string); sessionDir != agents.PiSessionDir {
		return fmt.Errorf("pi driver state session_dir = %q", sessionDir)
	}
	switch got, _ := object["state_kind"].(string); got {
	case piDriverStateKindUninitialized:
		return nil
	case piDriverStateKindSession:
		rel := strings.TrimSpace(stringValue(object["selected_session_relpath"]))
		if rel == "" {
			return fmt.Errorf("pi driver state selected_session_relpath is required")
		}
		if strings.HasPrefix(rel, "/") || strings.Contains(rel, "\\") {
			return fmt.Errorf("pi driver state selected_session_relpath must be relative")
		}
		if rel != strings.TrimPrefix(rel, "./") || rel != strings.TrimSpace(rel) {
			return fmt.Errorf("pi driver state selected_session_relpath must be clean")
		}
		parts := strings.Split(rel, "/")
		for _, part := range parts {
			if part == "" || part == "." || part == ".." {
				return fmt.Errorf("pi driver state selected_session_relpath must stay under session_dir")
			}
		}
		selectedFile, _ := object["selected_session_file"].(string)
		if selectedFile != agents.PiSessionDir+"/"+rel {
			return fmt.Errorf("pi driver state selected_session_file = %q, want %q", selectedFile, agents.PiSessionDir+"/"+rel)
		}
		if strings.TrimSpace(stringValue(object["selected_session_id"])) == "" {
			return fmt.Errorf("pi driver state selected_session_id is required")
		}
		if strings.TrimSpace(stringValue(object["last_completed_turn_id"])) == "" {
			return fmt.Errorf("pi driver state last_completed_turn_id is required")
		}
		return nil
	default:
		return fmt.Errorf("pi driver state_kind = %q", got)
	}
}

func validatePiDriverStateUpdateAgainstHostTx(ctx context.Context, tx *sql.Tx, p CompleteTurnParams, canonicalPayload []byte) error {
	record, err := getSandboxContractForGenerationWithGenerationMirror(ctx, tx, p.SessionID, p.GenerationID)
	if err != nil {
		return fmt.Errorf("pi driver state contract validation: %w", err)
	}
	contract, err := decodeSandboxContractObject(record.CanonicalPayload)
	if err != nil {
		return err
	}
	agentHomeHostPath, err := piAgentHomeHostPathFromContract(contract)
	if err != nil {
		return err
	}
	return ValidatePiDriverStatePayloadForHost(canonicalPayload, agentHomeHostPath, fmt.Sprint(p.TurnID))
}

func piAgentHomeHostPathFromContract(contract map[string]any) (string, error) {
	dataVolumes, ok := contract["data_volumes"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("pi driver state validation requires data_volumes")
	}
	agentHome, ok := dataVolumes["agent_home"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("pi driver state validation requires agent_home data volume")
	}
	if driver, _ := agentHome["driver"].(string); driver != string(agents.Pi) {
		return "", fmt.Errorf("pi agent_home data volume driver = %q", driver)
	}
	if key, _ := agentHome["driver_home_key"].(string); key != string(agents.Pi) {
		return "", fmt.Errorf("pi agent_home data volume key = %q", key)
	}
	if destination, _ := agentHome["sandbox_destination"].(string); destination != "/agent-home" {
		return "", fmt.Errorf("pi agent_home sandbox destination = %q", destination)
	}
	hostPath := strings.TrimSpace(stringValue(agentHome["host_path"]))
	if hostPath == "" || !filepath.IsAbs(hostPath) {
		return "", fmt.Errorf("pi agent_home host_path is required")
	}
	return hostPath, nil
}

func ValidatePiDriverStatePayloadForHost(canonicalPayload []byte, agentHomeHostPath, expectedCompletedTurnID string) error {
	return ValidateDriverStatePayloadForHost(string(agents.Pi), canonicalPayload, agentHomeHostPath, expectedCompletedTurnID)
}

func validatePiDriverStatePayloadForHost(canonicalPayload []byte, agentHomeHostPath, expectedCompletedTurnID string) error {
	canonical, _, err := canonicalDriverStatePayload(canonicalPayload, string(agents.Pi))
	if err != nil {
		return err
	}
	if !bytes.Equal(canonical, canonicalPayload) {
		return fmt.Errorf("pi driver state payload is not canonical")
	}
	object, err := decodeSandboxContractObject(canonicalPayload)
	if err != nil {
		return err
	}
	stateKind, _ := object["state_kind"].(string)
	agentHomeHostPath = strings.TrimSpace(agentHomeHostPath)
	if agentHomeHostPath == "" || !filepath.IsAbs(agentHomeHostPath) {
		return fmt.Errorf("pi agent_home host path is required")
	}
	switch stateKind {
	case piDriverStateKindUninitialized:
		if strings.TrimSpace(expectedCompletedTurnID) != "" {
			return fmt.Errorf("pi completed turn must advance to pi_session state")
		}
		return nil
	case piDriverStateKindSession:
		if expected := strings.TrimSpace(expectedCompletedTurnID); expected != "" && strings.TrimSpace(stringValue(object["last_completed_turn_id"])) != expected {
			return fmt.Errorf("pi driver state last_completed_turn_id mismatch")
		}
		return validatePiSessionFileAgainstHost(agentHomeHostPath, object)
	default:
		return fmt.Errorf("pi driver state_kind = %q", stateKind)
	}
}

func validatePiSessionFileAgainstHost(agentHomeHostPath string, object map[string]any) error {
	rel := strings.TrimSpace(stringValue(object["selected_session_relpath"]))
	if rel == "" || strings.HasPrefix(rel, "/") || strings.Contains(rel, "\\") || path.Clean(rel) != rel {
		return fmt.Errorf("pi selected session relpath is invalid")
	}
	if strings.HasPrefix(rel, "../") || rel == ".." {
		return fmt.Errorf("pi selected session relpath escapes session dir")
	}
	selectedFile := strings.TrimSpace(stringValue(object["selected_session_file"]))
	if selectedFile != agents.PiSessionDir+"/"+rel {
		return fmt.Errorf("pi selected session file = %q, want %q", selectedFile, agents.PiSessionDir+"/"+rel)
	}
	hostSessionRoot := filepath.Join(agentHomeHostPath, ".pi", "agent", "sessions")
	hostCandidate := filepath.Join(hostSessionRoot, filepath.FromSlash(rel))
	info, err := os.Lstat(hostCandidate)
	if err != nil {
		return fmt.Errorf("pi selected session host file missing: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("pi selected session host file must not be a symlink")
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("pi selected session host file is not regular")
	}
	realAgentHome, err := filepath.EvalSymlinks(agentHomeHostPath)
	if err != nil {
		return fmt.Errorf("pi agent_home realpath failed: %w", err)
	}
	realRoot, err := filepath.EvalSymlinks(hostSessionRoot)
	if err != nil {
		return fmt.Errorf("pi session root realpath failed: %w", err)
	}
	rootRel, err := filepath.Rel(realAgentHome, realRoot)
	if err != nil {
		return fmt.Errorf("pi session root relative path failed: %w", err)
	}
	if filepath.ToSlash(rootRel) != ".pi/agent/sessions" {
		return fmt.Errorf("pi session root realpath escapes agent_home")
	}
	realCandidate, err := filepath.EvalSymlinks(hostCandidate)
	if err != nil {
		return fmt.Errorf("pi selected session realpath failed: %w", err)
	}
	realRel, err := filepath.Rel(realRoot, realCandidate)
	if err != nil {
		return fmt.Errorf("pi selected session relative path failed: %w", err)
	}
	if realRel == "." || strings.HasPrefix(realRel, ".."+string(filepath.Separator)) || realRel == ".." || filepath.IsAbs(realRel) {
		return fmt.Errorf("pi selected session realpath escapes session dir")
	}
	if filepath.ToSlash(realRel) != rel {
		return fmt.Errorf("pi selected session realpath = %q, want %q", filepath.ToSlash(realRel), rel)
	}
	return nil
}
