import type { ApiArtifact, ApiSession, OutputEntry } from "@/lib/dashboard";
import { createOutputEntry } from "@/lib/dashboard";

const now = Date.now();
const minutesAgo = (minutes: number) => new Date(now - minutes * 60_000).toISOString();

export const mockSessions: ApiSession[] = [
  {
    id: "mock_sess_103",
    user_id: "lab",
    status: "completed",
    agent: "claude",
    workspace: "/workspace/mock_sess_103",
    restore_id: "phase4-mock_sess_103",
    restore_ms: 142,
    created_at: minutesAgo(96),
    updated_at: minutesAgo(82),
    expires_at: minutesAgo(-24 * 60),
    completed_at: minutesAgo(82)
  },
  {
    id: "mock_sess_102",
    user_id: "lab",
    status: "running",
    agent: "sh",
    workspace: "/workspace/mock_sess_102",
    restore_id: "phase4-mock_sess_102",
    created_at: minutesAgo(42),
    updated_at: minutesAgo(6),
    expires_at: minutesAgo(-24 * 60)
  },
  {
    id: "mock_sess_101",
    user_id: "lab",
    status: "created",
    agent: "opencode",
    workspace: "/workspace/mock_sess_101",
    restore_id: "phase4-mock_sess_101",
    created_at: minutesAgo(16),
    updated_at: minutesAgo(16),
    expires_at: minutesAgo(-24 * 60)
  }
];

export const mockArtifactsBySession: Record<string, ApiArtifact[]> = {
  mock_sess_103: [
    {
      session_id: "mock_sess_103",
      path: "summary.csv",
      size: 18_432,
      mod_time: minutesAgo(84),
      created_at: minutesAgo(84),
      updated_at: minutesAgo(84)
    },
    {
      session_id: "mock_sess_103",
      path: "trend.png",
      size: 126_418,
      mod_time: minutesAgo(83),
      created_at: minutesAgo(83),
      updated_at: minutesAgo(83)
    },
    {
      session_id: "mock_sess_103",
      path: "report.md",
      size: 7_208,
      mod_time: minutesAgo(82),
      created_at: minutesAgo(82),
      updated_at: minutesAgo(82)
    }
  ],
  mock_sess_102: [
    {
      session_id: "mock_sess_102",
      path: "workspace.log",
      size: 9_824,
      mod_time: minutesAgo(8),
      created_at: minutesAgo(8),
      updated_at: minutesAgo(8)
    }
  ],
  mock_sess_101: []
};

export const mockOutputBySession: Record<string, OutputEntry[]> = {
  mock_sess_103: [
    createOutputEntry({
      stream: "runtime",
      line: "completed a sample report run",
      source: "mock",
      time: minutesAgo(83)
    }),
    createOutputEntry({
      stream: "answer",
      line: "wrote summary.csv, trend.png, and report.md",
      source: "mock",
      time: minutesAgo(82)
    })
  ],
  mock_sess_102: [
    createOutputEntry({
      stream: "runtime",
      line: "mock session is still running a shell smoke task",
      source: "mock",
      time: minutesAgo(9)
    }),
    createOutputEntry({
      stream: "thinking",
      line: "scanning workspace artifacts",
      source: "mock",
      time: minutesAgo(8)
    }),
    createOutputEntry({
      stream: "tool-call",
      line: "writing workspace.log",
      source: "mock",
      time: minutesAgo(7)
    })
  ],
  mock_sess_101: []
};

export function createMockSession(agent: string): ApiSession {
  const createdAt = new Date().toISOString();
  const id = `mock_sess_${Math.random().toString(36).slice(2, 8)}`;

  return {
    id,
    user_id: "lab",
    status: "created",
    agent,
    workspace: `/workspace/${id}`,
    restore_id: `phase4-${id}`,
    created_at: createdAt,
    updated_at: createdAt,
    expires_at: new Date(Date.now() + 24 * 60 * 60_000).toISOString()
  };
}

export function createMockTaskOutput(content: string) {
  const trimmed = content.trim();
  const firstLine = trimmed.split(/\r?\n/, 1)[0] || "one-shot task";

  return [
    createOutputEntry({
      stream: "runtime",
      line: "mock fallback accepted the one-shot task.",
      source: "mock"
    }),
    createOutputEntry({
      stream: "thinking",
      line: `task: ${firstLine}`,
      source: "mock"
    }),
    createOutputEntry({
      stream: "answer",
      line: "real backend is not connected; no sandbox was started.",
      source: "mock"
    })
  ];
}

export function getMockSessionOutput(sessionId: string | null) {
  if (!sessionId) {
    return [];
  }

  return mockOutputBySession[sessionId] ?? [];
}
