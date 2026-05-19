import type { NextRequest } from "next/server";
import { proxyToResponse } from "@/lib/orchestrator-proxy";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type RouteContext = {
  params: Promise<{ path?: string[] }> | { path?: string[] };
};

function buildUpstreamPath(path: string[]) {
  if (path.length === 1 && path[0] === "healthz") {
    return "/healthz";
  }

  return `/api/${path.map((segment) => encodeURIComponent(segment)).join("/")}`;
}

async function handle(request: NextRequest, context: RouteContext) {
  const params = await context.params;
  return proxyToResponse(request, buildUpstreamPath(params.path ?? []));
}

export const GET = handle;
export const POST = handle;
export const PUT = handle;
export const PATCH = handle;
export const DELETE = handle;
export const OPTIONS = handle;
export const HEAD = handle;
