export type SessionStatus =
  | "created"
  | "running_active"
  | "running_idle"
  | "checkpointing"
  | "checkpointed"
  | "failed"
  | "destroyed";

export type ApiSession = {
  id: string;
  user_id: string;
  status: SessionStatus;
  mode: SessionMode;
  mode_label: string;
  restore_ms?: number | null;
  created_at: string;
  updated_at: string;
  expires_at?: string | null;
  ended_at?: string | null;
  last_activity_at?: string | null;
  auto_checkpoint_enabled?: boolean;
  failure_reason?: string;
  error_class?: string;
};

export type SessionMode = "agent" | "shell";

export type DeploymentSessionMode = {
  mode: SessionMode;
  label: string;
  visible: boolean;
  create_enabled: boolean;
  disabled_reason: string | null;
};

export type DeploymentCapabilities = {
  schema_version: 1;
  default_mode: SessionMode;
  session_modes: DeploymentSessionMode[];
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
  delta_type?: string;
  index?: number;
};

export type HarnessEvent = {
  event_id?: number;
  type: string;
  session_id?: string;
  turn_id?: number;
  output_sequence?: number;
  proxy_request_id?: string;
  stream?: string;
  severity?: string;
  time?: string;
  payload?: unknown;
};

export type ConnectionStatus = "idle" | "connecting" | "live" | "reconnecting" | "down";
