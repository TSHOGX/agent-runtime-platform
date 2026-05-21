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
	}, "/tmp/sessions")

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
	}, "/tmp/sessions")

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
	}, "/tmp/host-sessions")

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

func TestBuildControlContentUsesPrivateAgentHome(t *testing.T) {
	t.Setenv("HARNESS_AGENT_HOME", "")

	content := buildControlContent(StartRequest{
		SessionID:         "sess_1",
		Agent:             "claude",
		ClaudeSessionUUID: "11111111-2222-3333-4444-555555555555",
	}, "/tmp/host-sessions")

	if !strings.Contains(content, "export HARNESS_AGENT_HOME='/var/lib/harness-agent'\n") {
		t.Fatalf("expected private agent home in control content, got:\n%s", content)
	}
	if strings.Contains(content, "/tmp/host-sessions/.home") {
		t.Fatalf("control content must not place agent home under the workspace, got:\n%s", content)
	}
}
