import type { NextRequest } from "next/server";
import { proxyToResponse } from "@/lib/orchestrator-proxy";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

type RouteContext = {
  params: Promise<{
    sessionId: string;
    path?: string[];
  }>;
};

async function handle(request: NextRequest, context: RouteContext) {
  const params = await context.params;
  const encodedPath = params.path?.map((segment) => encodeURIComponent(segment)).join("/");
  const upstreamPath = encodedPath
    ? `/artifacts/${encodeURIComponent(params.sessionId)}/${encodedPath}`
    : `/artifacts/${encodeURIComponent(params.sessionId)}`;

  return proxyToResponse(request, upstreamPath);
}

export const GET = handle;
