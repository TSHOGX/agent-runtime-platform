package server

import (
	"strings"
	"testing"

	"harness-platform/orchestrator/internal/events"
)

func assertPublicSessionJSONOmitsHostFields(t *testing.T, payload []byte) {
	t.Helper()
	body := string(payload)
	for _, field := range []string{
		`"workspace"`,
		`"agent_home_path"`,
		`"agent":`,
		`"active_generation_id":`,
		`"restore_id"`,
		`"checkpoint_path"`,
		`"claude_session_uuid"`,
	} {
		if strings.Contains(body, field) {
			t.Fatalf("public session payload exposed host-only field %s: %s", field, body)
		}
	}
}

func assertContains(t *testing.T, value, want string) {
	t.Helper()
	if !strings.Contains(value, want) {
		t.Fatalf("expected %q to contain %q", value, want)
	}
}

func jsonArrayContainsAll(values []any, want ...string) bool {
	seen := map[string]struct{}{}
	for _, value := range values {
		if text, ok := value.(string); ok {
			seen[text] = struct{}{}
		}
	}
	for _, value := range want {
		if _, ok := seen[value]; !ok {
			return false
		}
	}
	return true
}

func drainHasEvent(ch <-chan events.Event, eventType string) bool {
	for {
		select {
		case event := <-ch:
			if event.Type == eventType {
				return true
			}
		default:
			return false
		}
	}
}
