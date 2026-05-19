export type BackendMode = "real" | "mock";

export type ApiSession = {
  id: string;
  user_id: string;
  status: string;
  agent: string;
  workspace: string;
  restore_id: string;
  restore_ms?: number | null;
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

export type ApiErrorResponse = {
  error?: string;
  upstream?: string;
};

export type OutputStreamKind = "thinking" | "tool-call" | "answer" | "runtime" | "system";

export type OutputEntry = {
  id: string;
  time: string;
  stream: string;
  kind: OutputStreamKind;
  label: string;
  line: string;
  source: "real" | "mock";
};

export type HarnessEvent = {
  type: string;
  session_id?: string;
  time?: string;
  payload?: unknown;
};

export type SendMessageResponse = {
  status: string;
  session_id: string;
};

export type RequestResult<T> =
  | {
      ok: true;
      data: T;
      response: Response;
    }
  | {
      ok: false;
      status: number;
      error: string;
      response?: Response;
    };

const SHORT_TIME = new Intl.DateTimeFormat("en-US", {
  hour: "2-digit",
  minute: "2-digit"
});

const FULL_TIME = new Intl.DateTimeFormat("en-US", {
  month: "short",
  day: "numeric",
  hour: "2-digit",
  minute: "2-digit"
});

const DEFAULT_WS_BASE_URL = "ws://127.0.0.1:8090";

export function requestJson<T>(input: RequestInfo | URL, init?: RequestInit) {
  return fetch(input, {
    cache: "no-store",
    ...init
  })
    .then(async (response) => {
      const contentType = response.headers.get("content-type") ?? "";
      const text = await response.text();
      let payload: unknown = text;

      if (contentType.includes("application/json") && text) {
        try {
          payload = JSON.parse(text) as unknown;
        } catch {
          payload = text;
        }
      } else if (contentType.includes("application/json")) {
        payload = undefined;
      }

      if (!response.ok) {
        return {
          ok: false,
          status: response.status,
          error: extractError(payload, response.statusText || "request failed"),
          response
        } satisfies RequestResult<T>;
      }

      return {
        ok: true,
        data: payload as T,
        response
      } satisfies RequestResult<T>;
    })
    .catch((error: unknown) => ({
      ok: false,
      status: 0,
      error: error instanceof Error ? error.message : "network error"
    })) as Promise<RequestResult<T>>;
}

export function buildEventsWebSocketUrl(sessionId: string) {
  const url = new URL("/api/events", getWebSocketBaseUrl());
  url.searchParams.set("session_id", sessionId);
  return url.toString();
}

export function formatTime(value: string | null | undefined) {
  if (!value) {
    return "-";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }

  return SHORT_TIME.format(date);
}

export function formatFullTime(value: string | null | undefined) {
  if (!value) {
    return "-";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }

  return FULL_TIME.format(date);
}

export function formatBytes(size: number) {
  if (!Number.isFinite(size) || size < 0) {
    return "-";
  }

  if (size < 1024) {
    return `${size} B`;
  }

  const units = ["KB", "MB", "GB", "TB"];
  let value = size / 1024;
  let unit = 0;

  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }

  return `${value >= 10 || unit === 0 ? Math.round(value) : value.toFixed(1)} ${units[unit]}`;
}

export function displayAgentName(agent: string) {
  switch (agent) {
    case "claude":
      return "Claude Code";
    case "opencode":
      return "OpenCode";
    case "sh":
      return "Shell";
    default:
      return agent || "Unknown";
  }
}

export function statusLabel(status: string) {
  switch (status) {
    case "created":
      return "Ready";
    case "running":
      return "Running";
    case "completed":
      return "Completed";
    case "failed":
      return "Failed";
    case "destroyed":
      return "Destroyed";
    default:
      return status || "Unknown";
  }
}

export function statusTone(status: string) {
  switch (status) {
    case "running":
      return "running";
    case "completed":
      return "completed";
    case "failed":
      return "failed";
    case "destroyed":
      return "destroyed";
    default:
      return "ready";
  }
}

export function canSendFirstTask(session: ApiSession | null) {
  return session?.status === "created";
}

export function isBackendUnavailable(result: RequestResult<unknown>) {
  if (result.ok) {
    return false;
  }

  return result.status === 0 || result.status >= 500;
}

export function choosePrimarySessionId(sessions: ApiSession[]) {
  const running = sessions.find((session) => session.status === "running");
  if (running) {
    return running.id;
  }

  const created = sessions.find((session) => session.status === "created");
  if (created) {
    return created.id;
  }

  return sessions[0]?.id ?? null;
}

export function buildArtifactHref(sessionId: string, path: string) {
  const encodedPath = path
    .split("/")
    .filter(Boolean)
    .map((segment) => encodeURIComponent(segment))
    .join("/");

  return `/artifacts/${encodeURIComponent(sessionId)}/${encodedPath}`;
}

export function classifyOutputStream(stream: string): OutputStreamKind {
  const normalized = stream.trim().toLowerCase().replace(/[_\s]+/g, "-");

  switch (normalized) {
    case "thinking":
      return "thinking";
    case "tool":
    case "tool-call":
      return "tool-call";
    case "answer":
    case "final":
      return "answer";
    case "system":
      return "system";
    default:
      return "runtime";
  }
}

export function outputKindLabel(kind: OutputStreamKind) {
  switch (kind) {
    case "thinking":
      return "Thinking";
    case "tool-call":
      return "Tool call";
    case "answer":
      return "Answer";
    case "system":
      return "System";
    default:
      return "Runtime";
  }
}

export function createOutputEntry(options: {
  stream: string;
  line: string;
  source: "real" | "mock";
  time?: string;
  id?: string;
}): OutputEntry {
  const kind = classifyOutputStream(options.stream);
  return {
    id:
      options.id ??
      `${options.source}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`,
    time: options.time ?? new Date().toISOString(),
    stream: options.stream,
    kind,
    label: outputKindLabel(kind),
    line: options.line,
    source: options.source
  };
}

export function getWebSocketBaseUrl() {
  const configured = process.env.NEXT_PUBLIC_HARNESS_WS_URL ?? DEFAULT_WS_BASE_URL;
  return normalizeWebSocketBaseUrl(configured);
}

function extractError(payload: unknown, fallback: string) {
  if (payload && typeof payload === "object" && "error" in payload) {
    const error = (payload as ApiErrorResponse).error;
    if (typeof error === "string" && error.trim()) {
      return error;
    }
  }

  return fallback;
}

function normalizeWebSocketBaseUrl(value: string) {
  const trimmed = value.trim().replace(/\/+$/, "");

  if (/^https?:\/\//i.test(trimmed)) {
    return trimmed.replace(/^http/i, "ws");
  }

  if (/^wss?:\/\//i.test(trimmed)) {
    return trimmed;
  }

  return `ws://${trimmed.replace(/^\/+/, "")}`;
}
