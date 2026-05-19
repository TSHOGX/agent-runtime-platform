const HOP_BY_HOP_HEADERS = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "content-length",
  "host"
]);

export const DEFAULT_ORCHESTRATOR_URL = "http://127.0.0.1:8090";

export function getOrchestratorBaseUrl() {
  const configured =
    process.env.HARNESS_API_BASE_URL ??
    process.env.ORCHESTRATOR_URL ??
    DEFAULT_ORCHESTRATOR_URL;

  return configured.replace(/\/+$/, "");
}

export function buildUpstreamUrl(pathname: string, search: string) {
  const baseUrl = getOrchestratorBaseUrl();
  const upstreamUrl = new URL(pathname, `${baseUrl}/`);
  upstreamUrl.search = search;
  return upstreamUrl;
}

function copyRequestHeaders(headers: Headers, requestUrl: URL) {
  const nextHeaders = new Headers();

  headers.forEach((value, key) => {
    const lowerKey = key.toLowerCase();
    if (HOP_BY_HOP_HEADERS.has(lowerKey) || lowerKey === "origin" || lowerKey === "referer") {
      return;
    }
    nextHeaders.set(key, value);
  });

  nextHeaders.set("x-forwarded-host", requestUrl.host);
  nextHeaders.set("x-forwarded-proto", requestUrl.protocol.replace(":", ""));

  return nextHeaders;
}

function copyResponseHeaders(headers: Headers) {
  const nextHeaders = new Headers();

  headers.forEach((value, key) => {
    if (HOP_BY_HOP_HEADERS.has(key.toLowerCase())) {
      return;
    }
    nextHeaders.append(key, value);
  });

  nextHeaders.set("cache-control", "no-store");
  return nextHeaders;
}

export async function proxyRequest(request: Request, upstreamPath: string) {
  const requestUrl = new URL(request.url);
  const upstreamUrl = buildUpstreamUrl(upstreamPath, requestUrl.search);
  const headers = copyRequestHeaders(request.headers, requestUrl);
  const body =
    request.method === "GET" || request.method === "HEAD"
      ? undefined
      : new Uint8Array(await request.arrayBuffer());

  return fetch(upstreamUrl, {
    method: request.method,
    headers,
    body,
    cache: "no-store"
  });
}

export async function proxyToResponse(request: Request, upstreamPath: string) {
  try {
    const upstreamResponse = await proxyRequest(request, upstreamPath);
    const body = await upstreamResponse.arrayBuffer();

    return new Response(body, {
      status: upstreamResponse.status,
      headers: copyResponseHeaders(upstreamResponse.headers)
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "upstream unavailable";

    return Response.json(
      { error: message, upstream: getOrchestratorBaseUrl() },
      {
        status: 503,
        headers: {
          "cache-control": "no-store"
        }
      }
    );
  }
}
