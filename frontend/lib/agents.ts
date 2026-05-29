import { Bot, TerminalSquare, type LucideIcon } from "lucide-react";

import type { DeploymentCapabilities, DeploymentSessionMode, SessionMode } from "./types";

const MODE_ICONS: Record<SessionMode, LucideIcon> = {
  agent: Bot,
  shell: TerminalSquare
};

export type NewSessionOption = {
  value: SessionMode;
  label: string;
  icon: LucideIcon;
  createEnabled: boolean;
  disabledReason: string | null;
};

export const FALLBACK_DEPLOYMENT_CAPABILITIES: DeploymentCapabilities = {
  schema_version: 1,
  default_mode: "agent",
  session_modes: [
    { mode: "agent", label: "Agent", visible: true, create_enabled: true, disabled_reason: null }
  ]
};

export function sessionModeLabel(mode: SessionMode | string, label?: string) {
  return label || (mode === "shell" ? "Shell" : "Agent");
}

function optionFromCapability(capability: DeploymentSessionMode): NewSessionOption {
  return {
    value: capability.mode,
    label: sessionModeLabel(capability.mode, capability.label),
    icon: MODE_ICONS[capability.mode],
    createEnabled: capability.create_enabled,
    disabledReason: capability.disabled_reason
  };
}

export function newSessionOptions(capabilities: DeploymentCapabilities | null): NewSessionOption[] {
  const source = capabilities ?? FALLBACK_DEPLOYMENT_CAPABILITIES;
  return source.session_modes
    .filter((mode) => mode.visible)
    .map(optionFromCapability)
    .sort((left, right) => (left.value === source.default_mode ? -1 : right.value === source.default_mode ? 1 : 0));
}
