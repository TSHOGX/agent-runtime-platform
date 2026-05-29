package agents

import (
	"fmt"
	"strings"
)

type ID string

const (
	ClaudeCode ID = "claude_code"
	Shell      ID = "sh"
)

const LegacyClaudeToken = "claude"

type Protocol string

const (
	ProtocolClaudeStreamJSON Protocol = "claude_stream_json"
	ProtocolShellPTY         Protocol = "shell_pty"
)

type Definition struct {
	ID       ID
	Label    string
	Protocol Protocol
}

var supported = map[ID]Definition{
	ClaudeCode: {
		ID:       ClaudeCode,
		Label:    "Claude Code",
		Protocol: ProtocolClaudeStreamJSON,
	},
	Shell: {
		ID:       Shell,
		Label:    "Shell",
		Protocol: ProtocolShellPTY,
	},
}

func Lookup(value string) (Definition, bool) {
	def, ok := supported[ID(value)]
	return def, ok
}

func CanonicalDriverID(value string) (ID, error) {
	trimmed := strings.TrimSpace(value)
	switch ID(trimmed) {
	case ClaudeCode, Shell:
		return ID(trimmed), nil
	case ID(LegacyClaudeToken):
		return ClaudeCode, nil
	default:
		return "", fmt.Errorf("unsupported driver %q", value)
	}
}

func PublicAgentForDriver(value string) (string, bool) {
	switch ID(strings.TrimSpace(value)) {
	case ClaudeCode:
		return LegacyClaudeToken, true
	case Shell:
		return string(Shell), true
	default:
		return "", false
	}
}

func SandboxAgentForDriver(value string) (string, bool) {
	return PublicAgentForDriver(value)
}
