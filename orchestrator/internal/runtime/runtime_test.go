package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestRuntimeStartRejectsUnsupportedAgent(t *testing.T) {
	rt := New(Config{DefaultAgent: "claude"})
	res := rt.Start(context.Background(), StartRequest{
		SessionID: "sess_1",
		Agent:     "opencode",
	}, nil)
	if res.Err == nil {
		t.Fatal("expected unsupported agent error")
	}
	if !strings.Contains(res.Err.Error(), "unsupported agent") {
		t.Fatalf("expected unsupported agent error, got %v", res.Err)
	}
}

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
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSuffix(out, "\n")), &frame); err != nil {
		t.Fatalf("invalid JSON frame %q: %v", out, err)
	}
	if frame.Type != "user" || frame.Message.Role != "user" {
		t.Fatalf("unexpected frame: %+v", frame)
	}
	if len(frame.Message.Content) != 1 {
		t.Fatalf("expected one content block, got %+v", frame.Message.Content)
	}
	if frame.Message.Content[0].Type != "text" || frame.Message.Content[0].Text != "hello world" {
		t.Fatalf("unexpected content block: %+v", frame.Message.Content[0])
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
	var frame struct {
		Message struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSuffix(buf.String(), "\n")), &frame); err != nil {
		t.Fatalf("invalid JSON frame %q: %v", buf.String(), err)
	}
	if len(frame.Message.Content) != 1 || frame.Message.Content[0].Text != "line1\nline2" {
		t.Fatalf("unexpected multi-line content: %+v", frame.Message.Content)
	}
}

func TestWriteUserTurnShellJSONFraming(t *testing.T) {
	var buf bytes.Buffer
	if err := writeUserTurn(&buf, "sh", "ls -la"); err != nil {
		t.Fatalf("writeUserTurn: %v", err)
	}
	out := buf.String()
	if !strings.HasSuffix(out, "\n") {
		t.Fatalf("expected trailing newline for shell JSON framing, got %q", out)
	}
	var frame struct {
		Type    string `json:"type"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSuffix(out, "\n")), &frame); err != nil {
		t.Fatalf("invalid JSON frame %q: %v", out, err)
	}
	if frame.Type != "turn" || frame.Content != "ls -la" {
		t.Fatalf("unexpected shell frame: %+v", frame)
	}
}

func TestWriteInterruptShellJSONFraming(t *testing.T) {
	var buf bytes.Buffer
	if err := writeInterrupt(&buf, "sh"); err != nil {
		t.Fatalf("writeInterrupt: %v", err)
	}
	var frame struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &frame); err != nil {
		t.Fatalf("invalid interrupt frame %q: %v", buf.String(), err)
	}
	if frame.Type != "interrupt" {
		t.Fatalf("unexpected interrupt frame: %+v", frame)
	}
}

func TestWriteUserTurnRejectsUnsupportedAgent(t *testing.T) {
	var buf bytes.Buffer
	err := writeUserTurn(&buf, "opencode", "hello")
	if err == nil {
		t.Fatal("expected unsupported agent error")
	}
	if !strings.Contains(err.Error(), "unsupported agent") {
		t.Fatalf("expected unsupported agent error, got %v", err)
	}
}

func TestBuildControlContentUsesHarnessClaudeKey(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "ikun")
	t.Setenv("HARNESS_CLAUDE_API_KEY", "123")
	t.Setenv("HARNESS_CLAUDE_BASE_URL", "")
	t.Setenv("HARNESS_ANTHROPIC_BASE_URL", "")
	t.Setenv("ANTHROPIC_BASE_URL", "")

	content := buildControlContent(StartRequest{
		SessionID:         "sess_1",
		Agent:             "claude",
		ClaudeSessionUUID: "11111111-2222-3333-4444-555555555555",
	}, "host")

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
	t.Setenv("HARNESS_CLAUDE_BASE_URL", "")
	t.Setenv("HARNESS_ANTHROPIC_BASE_URL", "")
	t.Setenv("ANTHROPIC_BASE_URL", "")

	content := buildControlContent(StartRequest{
		SessionID:         "sess_1",
		Agent:             "claude",
		ClaudeSessionUUID: "11111111-2222-3333-4444-555555555555",
	}, "host")

	if !strings.Contains(content, "export ANTHROPIC_API_KEY='123'\n") {
		t.Fatalf("expected default Claude API key 123 in control content, got:\n%s", content)
	}
}

func TestBuildControlContentUsesContainerWorkspaceAndResumeFlag(t *testing.T) {
	t.Setenv("HARNESS_CLAUDE_BASE_URL", "")
	t.Setenv("HARNESS_ANTHROPIC_BASE_URL", "")
	t.Setenv("ANTHROPIC_BASE_URL", "")

	content := buildControlContent(StartRequest{
		SessionID:         "sess_1",
		Agent:             "claude",
		ClaudeSessionUUID: "11111111-2222-3333-4444-555555555555",
		ResumeClaude:      true,
	}, "sandbox")

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
	t.Setenv("HARNESS_CLAUDE_BASE_URL", "")
	t.Setenv("HARNESS_ANTHROPIC_BASE_URL", "")
	t.Setenv("ANTHROPIC_BASE_URL", "")

	content := buildControlContent(StartRequest{
		SessionID:         "sess_1",
		Agent:             "claude",
		ClaudeSessionUUID: "11111111-2222-3333-4444-555555555555",
	}, "sandbox")

	if !strings.Contains(content, "export HARNESS_AGENT_HOME='/agent-homes/sess_1'\n") {
		t.Fatalf("expected per-session agent home outside workspace in control content, got:\n%s", content)
	}
	if strings.Contains(content, ".agent-home") || strings.Contains(content, "/sessions/sess_1/.") {
		t.Fatalf("agent home must not be under the workspace, got:\n%s", content)
	}
}

func TestBuildControlContentIgnoresUnsafeAgentHomeOverride(t *testing.T) {
	t.Setenv("HARNESS_AGENT_HOME", "/sessions/sess_1/.home")
	t.Setenv("HARNESS_CLAUDE_BASE_URL", "")
	t.Setenv("HARNESS_ANTHROPIC_BASE_URL", "")
	t.Setenv("ANTHROPIC_BASE_URL", "")

	content := buildControlContent(StartRequest{
		SessionID:         "sess_1",
		Agent:             "claude",
		ClaudeSessionUUID: "11111111-2222-3333-4444-555555555555",
	}, "sandbox")

	if !strings.Contains(content, "export HARNESS_AGENT_HOME='/agent-homes/sess_1'\n") {
		t.Fatalf("expected canonical agent home path, got:\n%s", content)
	}
	if strings.Contains(content, "/sessions/sess_1/.home") {
		t.Fatalf("unsafe workspace agent home override leaked into control content:\n%s", content)
	}
}

func TestBuildControlContentUsesNetworkSpecificProxyRoot(t *testing.T) {
	t.Setenv("HARNESS_CLAUDE_BASE_URL", "")
	t.Setenv("HARNESS_ANTHROPIC_BASE_URL", "")
	t.Setenv("ANTHROPIC_BASE_URL", "")

	hostContent := buildControlContent(StartRequest{
		SessionID:         "sess_1",
		Agent:             "claude",
		ClaudeSessionUUID: "11111111-2222-3333-4444-555555555555",
	}, "host")
	if !strings.Contains(hostContent, "export ANTHROPIC_BASE_URL='http://127.0.0.1:8082'\n") {
		t.Fatalf("expected host proxy root in control content, got:\n%s", hostContent)
	}

	sandboxContent := buildControlContent(StartRequest{
		SessionID:         "sess_1",
		Agent:             "claude",
		ClaudeSessionUUID: "11111111-2222-3333-4444-555555555555",
	}, "sandbox")
	if !strings.Contains(sandboxContent, "export ANTHROPIC_BASE_URL='http://10.0.0.1:8082'\n") {
		t.Fatalf("expected sandbox proxy root in control content, got:\n%s", sandboxContent)
	}
}
