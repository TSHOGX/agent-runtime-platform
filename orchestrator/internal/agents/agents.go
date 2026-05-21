package agents

type ID string

const (
	Claude ID = "claude"
	Shell  ID = "sh"
)

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
	Claude: {
		ID:       Claude,
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
