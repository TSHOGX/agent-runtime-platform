export type SessionStatus = "created" | "running" | "idle" | "completed" | "failed" | "destroyed";

export type AgentKind = "claude" | "opencode" | "sh";

export type ApiSession = {
  id: string;
  user_id: string;
  status: string;
  agent: string;
  workspace: string;
  restore_id: string;
  restore_ms?: number | null;
  claude_session_uuid?: string;
  created_at: string;
  updated_at: string;
  expires_at?: string | null;
  completed_at?: string | null;
};

export type ApiArtifact = {
  session_id: string;
  path: string;
  size: number;
  mod_time: string;
  created_at: string;
  updated_at: string;
};

export type ApiMessage = {
  id: number;
  session_id: string;
  role: "user" | "assistant" | string;
  content: string;
  created_at: string;
};

export type StreamLineKind = "stdout" | "stderr" | "runtime";

export type StreamLine = {
  id: string;
  session_id: string;
  stream: StreamLineKind | string;
  line: string;
  time: string;
};

export type DeltaPayload = {
  message_id: string;
  text: string;
};

export type HarnessEvent = {
  type: string;
  session_id?: string;
  time?: string;
  payload?: unknown;
};

export type ConnectionStatus = "idle" | "connecting" | "live" | "reconnecting" | "down";
