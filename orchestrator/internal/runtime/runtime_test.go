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

func TestCleanupExitedContainerDoesNotRemoveReplacement(t *testing.T) {
	rt := New(Config{})
	oldContainer := &Container{SessionID: "sess_1", RestoreID: "phase3-sess_1"}
	newContainer := &Container{SessionID: "sess_1", RestoreID: "phase3-sess_1"}

	rt.containers["sess_1"] = newContainer
	rt.cleanupExitedContainer(oldContainer)

	if got := rt.containers["sess_1"]; got != newContainer {
		t.Fatalf("replacement container was removed: got %+v", got)
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

func TestBuildControlContentUsesExplicitSandboxManifest(t *testing.T) {
	t.Setenv("HARNESS_CLAUDE_BASE_URL", "http://bad.invalid")
	t.Setenv("HARNESS_ANTHROPIC_BASE_URL", "http://bad.invalid")
	t.Setenv("ANTHROPIC_BASE_URL", "http://bad.invalid")
	t.Setenv("HARNESS_CLAUDE_API_KEY", "bad")
	t.Setenv("HARNESS_ANTHROPIC_API_KEY", "bad")

	content := buildControlContent(StartRequest{
		SessionID:         "sess_1",
		Agent:             "claude",
		ClaudeSessionUUID: "11111111-2222-3333-4444-555555555555",
		ResumeClaude:      true,
	}, "sandbox")

	var manifest controlManifest
	if err := json.Unmarshal([]byte(content), &manifest); err != nil {
		t.Fatalf("control content is not valid JSON: %v\n%s", err, content)
	}
	if manifest.SessionID != "sess_1" {
		t.Fatalf("unexpected session id: %+v", manifest)
	}
	if manifest.SessionWorkspace != "/sessions/sess_1" {
		t.Fatalf("unexpected session workspace: %+v", manifest)
	}
	if manifest.HarnessAgentHome != "/agent-homes/sess_1" {
		t.Fatalf("unexpected agent home: %+v", manifest)
	}
	if !manifest.ClaudeResume {
		t.Fatalf("expected resume flag to be set: %+v", manifest)
	}
	if manifest.ProxyBindURL != "http://0.0.0.0:8082" {
		t.Fatalf("unexpected proxy bind URL: %+v", manifest)
	}
	if manifest.AnthropicBaseURL != "http://10.200.1.1:8082" {
		t.Fatalf("unexpected sandbox base URL: %+v", manifest)
	}
	if manifest.AnthropicAPIKey != "123" || manifest.AnthropicAuthToken != "123" {
		t.Fatalf("unexpected proxy credential: %+v", manifest)
	}
	if !manifest.ClaudeCodeDisableNonessentialTraffic {
		t.Fatalf("expected nonessential traffic to be disabled: %+v", manifest)
	}
	if manifest.ClaudeModel != "sonnet" || manifest.ClaudeOutputFormat != "stream-json" {
		t.Fatalf("unexpected Claude defaults: %+v", manifest)
	}
}

func TestBuildControlContentUsesExplicitHostProxyURL(t *testing.T) {
	content := buildControlContent(StartRequest{
		SessionID:         "sess_1",
		Agent:             "claude",
		ClaudeSessionUUID: "11111111-2222-3333-4444-555555555555",
	}, "host")

	var manifest controlManifest
	if err := json.Unmarshal([]byte(content), &manifest); err != nil {
		t.Fatalf("control content is not valid JSON: %v\n%s", err, content)
	}
	if manifest.AnthropicBaseURL != "http://0.0.0.0:8082" {
		t.Fatalf("expected host proxy URL, got %+v", manifest)
	}
}
