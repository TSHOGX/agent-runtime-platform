import type { ApiArtifact, ApiMessage, ApiSession } from "./types";

export type RequestResult<T> =
  | { ok: true; data: T; response: Response }
  | { ok: false; status: number; error: string; response?: Response };

export async function request<T>(input: RequestInfo | URL, init?: RequestInit): Promise<RequestResult<T>> {
  try {
    const response = await fetch(input, { cache: "no-store", ...init });
    const contentType = response.headers.get("content-type") ?? "";
    const text = await response.text();
    let payload: unknown = text;
    if (contentType.includes("application/json") && text) {
      try {
        payload = JSON.parse(text);
      } catch {
        payload = text;
      }
    }
    if (!response.ok) {
      const error =
        typeof payload === "object" && payload && "error" in payload && typeof (payload as { error?: unknown }).error === "string"
          ? ((payload as { error: string }).error)
          : response.statusText || "request failed";
      return { ok: false, status: response.status, error, response };
    }
    return { ok: true, data: payload as T, response };
  } catch (err) {
    return { ok: false, status: 0, error: err instanceof Error ? err.message : "network error" };
  }
}

export async function fetchHealth() {
  return request<{ status: string }>("/api/healthz");
}

export async function fetchSessions() {
  return request<{ sessions: ApiSession[] }>("/api/sessions");
}

export async function createSession(agent: string) {
  return request<ApiSession>("/api/sessions", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ agent })
  });
}

export async function destroySession(sessionId: string) {
  return request<{ status: string }>(`/api/sessions/${encodeURIComponent(sessionId)}`, { method: "DELETE" });
}

export async function fetchMessages(sessionId: string) {
  return request<{ messages: ApiMessage[] | null }>(`/api/sessions/${encodeURIComponent(sessionId)}/messages`);
}

export async function postMessage(sessionId: string, content: string) {
  return request<{ status: string; session_id: string; message: ApiMessage }>(
    `/api/sessions/${encodeURIComponent(sessionId)}/messages`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ content })
    }
  );
}

export async function fetchArtifacts(sessionId: string) {
  return request<{ artifacts: ApiArtifact[] | null }>(
    `/api/sessions/${encodeURIComponent(sessionId)}/artifacts`
  );
}

export function buildArtifactHref(sessionId: string, path: string) {
  const encodedPath = path
    .split("/")
    .filter(Boolean)
    .map(encodeURIComponent)
    .join("/");
  return `/artifacts/${encodeURIComponent(sessionId)}/${encodedPath}`;
}

export async function fetchArtifactText(sessionId: string, path: string, maxBytes = 2 * 1024 * 1024) {
  const response = await fetch(buildArtifactHref(sessionId, path), { cache: "no-store" });
  if (!response.ok) {
    return { ok: false as const, status: response.status, error: response.statusText || "fetch failed" };
  }
  const sizeHeader = response.headers.get("content-length");
  if (sizeHeader && Number.parseInt(sizeHeader, 10) > maxBytes) {
    return { ok: false as const, status: 413, error: "artifact too large" };
  }
  const text = await response.text();
  if (text.length > maxBytes) {
    return { ok: false as const, status: 413, error: "artifact too large" };
  }
  return { ok: true as const, text };
}
