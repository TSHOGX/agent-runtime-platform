export function buildEventsStreamUrl(sessionId?: string) {
  const base =
    typeof window !== "undefined"
      ? window.location.origin
      : "http://127.0.0.1:8000";
  const url = new URL("/api/events/stream", base);
  if (sessionId) {
    url.searchParams.set("session_id", sessionId);
  }
  return url.toString();
}

export const buildEventsWebSocketUrl = buildEventsStreamUrl;
