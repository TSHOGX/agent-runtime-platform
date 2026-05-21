import { Bot, TerminalSquare, type LucideIcon } from "lucide-react";

export type RuntimeAgent = "claude" | "sh";

export type NewSessionMode = "agent" | "shell";

export type NewSessionOption = {
  value: NewSessionMode;
  label: string;
  agent: RuntimeAgent;
  icon: LucideIcon;
};

const AGENT_LABELS: Record<RuntimeAgent, string> = {
  claude: "Claude Code",
  sh: "Shell"
};

export const NEW_SESSION_OPTIONS: readonly NewSessionOption[] = [
  { value: "shell", label: "Shell", agent: "sh", icon: TerminalSquare },
  { value: "agent", label: "Agent", agent: "claude", icon: Bot }
];

export function agentLabel(agent: string) {
  if (agent === "claude" || agent === "sh") {
    return AGENT_LABELS[agent];
  }
  return agent || "Agent";
}
