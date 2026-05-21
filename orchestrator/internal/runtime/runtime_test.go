package runtime

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestWriteUserTurnClaudeJSONLFraming(t *testing.T) {
	var buf bytes.Buffer
	if err := writeUserTurn(&buf, "claude", "hello world"); err != nil {
		t.Fatalf("writeUserTurn: %v", err)
	}

	out := buf.String()
	if !strings.HasSuffix(out, "\n") {
		t.Fatalf("expected trailing newline for JSONL framing, got %q", out)
	}
	if strings.Count(out, "\n") != 1 {
		t.Fatalf("expected exactly one JSONL frame, got %q", out)
	}

	var frame struct {
		Type    string `json:"type"`
		Message struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSuffix(out, "\n")), &frame); err != nil {
		t.Fatalf("invalid JSON frame %q: %v", out, err)
	}
	if frame.Type != "user" || frame.Message.Role != "user" || frame.Message.Content != "hello world" {
		t.Fatalf("unexpected frame: %+v", frame)
	}
}

func TestWriteUserTurnClaudeEscapesNewlines(t *testing.T) {
	// Multi-line user input must stay on a single JSONL line.
	var buf bytes.Buffer
	if err := writeUserTurn(&buf, "claude", "line1\nline2"); err != nil {
		t.Fatalf("writeUserTurn: %v", err)
	}
	if strings.Count(buf.String(), "\n") != 1 {
		t.Fatalf("multi-line input must produce one JSONL frame, got %q", buf.String())
	}
}

func TestWriteUserTurnNonClaudeRawText(t *testing.T) {
	var buf bytes.Buffer
	if err := writeUserTurn(&buf, "sh", "ls -la"); err != nil {
		t.Fatalf("writeUserTurn: %v", err)
	}
	if buf.String() != "ls -la\n" {
		t.Fatalf("expected raw text passthrough for non-claude agents, got %q", buf.String())
	}
}

func TestBuildControlContentUsesHarnessClaudeKey(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "ikun")
	t.Setenv("HARNESS_CLAUDE_API_KEY", "123")

	content := buildControlContent(StartRequest{
		SessionID:         "sess_1",
		Agent:             "claude",
		ClaudeSessionUUID: "11111111-2222-3333-4444-555555555555",
	})

	if !strings.Contains(content, "export ANTHROPIC_API_KEY='123'\n") {
		t.Fatalf("expected harness Claude API key in control content, got:\n%s", content)
	}
	if strings.Contains(content, "ikun") {
		t.Fatalf("control content must not inherit generic local ANTHROPIC_API_KEY:\n%s", content)
	}
}

func TestBuildControlContentDefaultsToKey123(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "ikun")
	t.Setenv("HARNESS_CLAUDE_API_KEY", "")
	t.Setenv("HARNESS_CLAUDE_AUTH_TOKEN", "")
	t.Setenv("HARNESS_ANTHROPIC_API_KEY", "")
	t.Setenv("HARNESS_ANTHROPIC_AUTH_TOKEN", "")

	content := buildControlContent(StartRequest{
		SessionID:         "sess_1",
		Agent:             "claude",
		ClaudeSessionUUID: "11111111-2222-3333-4444-555555555555",
	})

	if !strings.Contains(content, "export ANTHROPIC_API_KEY='123'\n") {
		t.Fatalf("expected default Claude API key 123 in control content, got:\n%s", content)
	}
}

func TestBuildControlContentUsesContainerWorkspaceAndResumeFlag(t *testing.T) {
	content := buildControlContent(StartRequest{
		SessionID:         "sess_1",
		Agent:             "claude",
		ClaudeSessionUUID: "11111111-2222-3333-4444-555555555555",
		ResumeClaude:      true,
	})

	if !strings.Contains(content, "export SESSION_WORKSPACE='/sessions/sess_1'\n") {
		t.Fatalf("expected container workspace path in control content, got:\n%s", content)
	}
	if strings.Contains(content, "/tmp/host-sessions") {
		t.Fatalf("control content must not expose host session path as in-container workspace:\n%s", content)
	}
	if !strings.Contains(content, "export CLAUDE_RESUME='1'\n") {
		t.Fatalf("expected Claude resume flag in control content, got:\n%s", content)
	}
}

func TestBuildControlContentUsesSessionAgentHome(t *testing.T) {
	t.Setenv("HARNESS_AGENT_HOME", "")

	content := buildControlContent(StartRequest{
		SessionID:         "sess_1",
		Agent:             "claude",
		ClaudeSessionUUID: "11111111-2222-3333-4444-555555555555",
	})

	if !strings.Contains(content, "export HARNESS_AGENT_HOME='/agent-homes/sess_1'\n") {
		t.Fatalf("expected per-session agent home outside workspace in control content, got:\n%s", content)
	}
	if strings.Contains(content, ".agent-home") || strings.Contains(content, "/sessions/sess_1/.") {
		t.Fatalf("agent home must not be under the workspace, got:\n%s", content)
	}
}

func TestBuildControlContentIgnoresUnsafeAgentHomeOverride(t *testing.T) {
	t.Setenv("HARNESS_AGENT_HOME", "/sessions/sess_1/.home")

	content := buildControlContent(StartRequest{
		SessionID:         "sess_1",
		Agent:             "claude",
		ClaudeSessionUUID: "11111111-2222-3333-4444-555555555555",
	})

	if !strings.Contains(content, "export HARNESS_AGENT_HOME='/agent-homes/sess_1'\n") {
		t.Fatalf("expected canonical agent home path, got:\n%s", content)
	}
	if strings.Contains(content, "/sessions/sess_1/.home") {
		t.Fatalf("unsafe workspace agent home override leaked into control content:\n%s", content)
	}
}
