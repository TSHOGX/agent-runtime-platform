import { describe, expect, it } from "vitest";

import { reduceSessionEvent, type SessionEventSlice } from "./session-events";
import type { ApiSession, HarnessEvent } from "./types";

type TestConversation = {
  streaming: Array<{ id: string; text: string }>;
};

function session(patch: Partial<ApiSession> = {}): ApiSession {
  return {
    id: "sess_1",
    user_id: "lab",
    status: "checkpointed",
    agent: "claude",
    active_generation_id: "gen_1",
    restore_ms: 42,
    created_at: "2026-05-26T00:00:00Z",
    updated_at: "2026-05-26T00:00:00Z",
    last_activity_at: "2026-05-26T00:00:00Z",
    ...patch
  };
}

function slice(baseSession = session()): SessionEventSlice<TestConversation> {
  return {
    sessions: [baseSession],
    conversations: {
      sess_1: { streaming: [{ id: "pending", text: "partial" }] }
    }
  };
}

function reduce(base: SessionEventSlice<TestConversation>, event: HarnessEvent) {
  return reduceSessionEvent(base, event, {
    emptyConversation: () => ({ streaming: [] }),
    time: "2026-05-26T01:00:00Z"
  });
}

describe("reduceSessionEvent", () => {
  it("keeps retryable generation errors input-acceptable and clears streaming", () => {
    const next = reduce(slice(session({ status: "running_active" })), {
      type: "generation.error",
      session_id: "sess_1",
      payload: {
        error: "runtime failed",
        session_status: "running_idle",
        session_updated_at: "2026-05-26T01:01:00Z"
      }
    });

    expect(next.sessions[0].status).toBe("running_idle");
    expect(next.sessions[0].updated_at).toBe("2026-05-26T01:01:00Z");
    expect(next.conversations.sess_1.streaming).toEqual([]);
    expect(next.notifications).toEqual([{ level: "error", message: "runtime failed" }]);
  });

  it("clears restore metadata for checkpoint retirement", () => {
    const next = reduce(slice(), {
      type: "session.checkpoint_retired",
      session_id: "sess_1",
      payload: {
        session_status: "running_idle",
        session_updated_at: "2026-05-26T01:02:00Z",
        session_last_activity_at: "2026-05-26T00:30:00Z",
        active_generation_id: "gen_1",
        restore_ms: null
      }
    });

    expect(next.sessions[0]).toMatchObject({
      status: "running_idle",
      updated_at: "2026-05-26T01:02:00Z",
      last_activity_at: "2026-05-26T00:30:00Z",
      restore_ms: null
    });
  });

  it("clears restore metadata for restore fallback retirement", () => {
    const next = reduce(slice(), {
      type: "session.restore_fallback_retired",
      session_id: "sess_1",
      payload: {
        session_status: "running_idle",
        session_updated_at: "2026-05-26T01:03:00Z",
        active_generation_id: "gen_1",
        restore_ms: null
      }
    });

    expect(next.sessions[0].status).toBe("running_idle");
    expect(next.sessions[0].restore_ms).toBeNull();
  });

  it("marks terminal session events failed", () => {
    const failedStatus = reduce(slice(session({ status: "running_idle" })), {
      type: "session.failed",
      session_id: "sess_1",
      time: "2026-05-26T01:04:00Z"
    });
    expect(failedStatus.sessions[0].status).toBe("failed");
    expect(failedStatus.conversations.sess_1.streaming).toEqual([]);

    const terminalError = reduce(slice(session({ status: "running_idle" })), {
      type: "session.error",
      session_id: "sess_1",
      payload: { terminal: true, error: "terminal failure" }
    });
    expect(terminalError.sessions[0].status).toBe("failed");
    expect(terminalError.notifications).toEqual([{ level: "error", message: "terminal failure" }]);
  });

  it("does not fail the session for non-terminal session errors or failed turns", () => {
    const nonTerminal = reduce(slice(session({ status: "running_idle" })), {
      type: "session.error",
      session_id: "sess_1",
      payload: { terminal: false, error: "retryable runtime error" }
    });
    expect(nonTerminal.sessions[0].status).toBe("running_idle");
    expect(nonTerminal.conversations.sess_1.streaming).toEqual([]);

    const failedTurn = reduce(slice(session({ status: "running_active" })), {
      type: "ack_turn_completed",
      session_id: "sess_1",
      payload: {
        status: "failed",
        error: "turn failed",
        session_status: "running_idle",
        session_updated_at: "2026-05-26T01:05:00Z"
      }
    });
    expect(failedTurn.sessions[0].status).toBe("running_idle");
    expect(failedTurn.notifications).toEqual([{ level: "error", message: "turn failed" }]);
  });
});
