const DEFAULT_WS_BASE_URL = "ws://127.0.0.1:8090";

export function getWebSocketBaseUrl() {
  const configured = process.env.NEXT_PUBLIC_HARNESS_WS_URL ?? DEFAULT_WS_BASE_URL;
  return normalize(configured);
}

export function buildEventsWebSocketUrl(sessionId?: string) {
  const url = new URL("/api/events", getWebSocketBaseUrl());
  if (sessionId) {
    url.searchParams.set("session_id", sessionId);
  }
  return url.toString();
}

function normalize(value: string) {
  const trimmed = value.trim().replace(/\/+$/, "");
  if (/^https?:\/\//i.test(trimmed)) {
    return trimmed.replace(/^http/i, "ws");
  }
  if (/^wss?:\/\//i.test(trimmed)) {
    return trimmed;
  }
  return `ws://${trimmed.replace(/^\/+/, "")}`;
}
