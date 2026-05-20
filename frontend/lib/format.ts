import type { SessionStatus } from "./types";

const SHORT_TIME = new Intl.DateTimeFormat("en-US", { hour: "2-digit", minute: "2-digit" });

export function formatTime(value: string | null | undefined) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return SHORT_TIME.format(date);
}

export function formatRelative(value: string | null | undefined) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  const diff = (Date.now() - date.getTime()) / 1000;
  if (diff < 60) return "just now";
  if (diff < 3600) return `${Math.round(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.round(diff / 3600)}h ago`;
  return `${Math.round(diff / 86400)}d ago`;
}

export function formatBytes(size: number | undefined | null) {
  if (size === null || size === undefined || !Number.isFinite(size) || size < 0) return "-";
  if (size < 1024) return `${size} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let value = size / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value >= 10 || unit === 0 ? Math.round(value) : value.toFixed(1)} ${units[unit]}`;
}

export function statusLabel(status: string) {
  switch (status as SessionStatus) {
    case "created":
      return "Ready";
    case "running":
      return "Running";
    case "idle":
      return "Idle";
    case "completed":
      return "Completed";
    case "failed":
      return "Failed";
    case "destroyed":
      return "Ended";
    default:
      return status || "Unknown";
  }
}

export function agentLabel(agent: string) {
  switch (agent) {
    case "claude":
      return "Claude Code";
    case "opencode":
      return "OpenCode";
    case "sh":
      return "Shell";
    default:
      return agent || "Agent";
  }
}

export function isAcceptingInput(status: string) {
  return status === "created" || status === "idle";
}

export function isTerminal(status: string) {
  return status === "completed" || status === "failed" || status === "destroyed";
}
