import type { ApiSession, HarnessEvent, SessionStatus } from "./types";

export type SessionEventConversation = {
  streaming: unknown[];
};

export type SessionEventSlice<TConversation extends SessionEventConversation> = {
  sessions: ApiSession[];
  conversations: Record<string, TConversation>;
};

export type SessionEventNotification = {
  level: "error";
  message: string;
};

export type SessionEventReduction<TConversation extends SessionEventConversation> =
  SessionEventSlice<TConversation> & {
    handled: boolean;
    notifications: SessionEventNotification[];
  };

const SESSION_EVENT_STATUSES = new Set<SessionStatus>([
  "running_active",
  "running_idle",
  "checkpointing",
  "checkpointed",
  "failed",
  "destroyed"
]);

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function readSessionStatusEvent(type: string): SessionStatus | null {
  if (!type.startsWith("session.")) return null;
  const status = type.slice("session.".length) as SessionStatus;
  return SESSION_EVENT_STATUSES.has(status) ? status : null;
}

function readSessionStatus(value: unknown): SessionStatus | null {
  return typeof value === "string" && SESSION_EVENT_STATUSES.has(value as SessionStatus)
    ? (value as SessionStatus)
    : null;
}

function updateSession(
  sessions: ApiSession[],
  sessionId: string,
  patch: (session: ApiSession) => ApiSession
) {
  return sessions.map((session) => (session.id === sessionId ? patch(session) : session));
}

function clearStreaming<TConversation extends SessionEventConversation>(
  conversations: Record<string, TConversation>,
  sessionId: string,
  emptyConversation: () => TConversation
) {
  const current = conversations[sessionId] ?? emptyConversation();
  return {
    ...conversations,
    [sessionId]: { ...current, streaming: [] }
  };
}

function emptyReduction<TConversation extends SessionEventConversation>(
  slice: SessionEventSlice<TConversation>,
  handled = false
): SessionEventReduction<TConversation> {
  return { ...slice, handled, notifications: [] };
}

export function reduceSessionEvent<TConversation extends SessionEventConversation>(
  slice: SessionEventSlice<TConversation>,
  event: HarnessEvent,
  options: { emptyConversation: () => TConversation; time: string }
): SessionEventReduction<TConversation> {
  const sessionId = event.session_id;
  const time = event.time ?? options.time;

  const status = readSessionStatusEvent(event.type);
  if (status) {
    if (!sessionId) return emptyReduction(slice, true);
    return {
      sessions: updateSession(slice.sessions, sessionId, (session) => ({ ...session, status, updated_at: time })),
      conversations:
        status === "running_active"
          ? slice.conversations
          : clearStreaming(slice.conversations, sessionId, options.emptyConversation),
      handled: true,
      notifications: []
    };
  }

  switch (event.type) {
    case "session.error": {
      if (!isRecord(event.payload)) return emptyReduction(slice, true);
      const message = typeof event.payload.error === "string" ? event.payload.error : "Session failed";
      if (!sessionId) return { ...emptyReduction(slice, true), notifications: [{ level: "error", message }] };
      const terminal = event.payload.terminal === true;
      return {
        sessions: terminal
          ? updateSession(slice.sessions, sessionId, (session) => ({ ...session, status: "failed", updated_at: time }))
          : slice.sessions,
        conversations: clearStreaming(slice.conversations, sessionId, options.emptyConversation),
        handled: true,
        notifications: [{ level: "error", message }]
      };
    }
    case "generation.error": {
      if (!sessionId || !isRecord(event.payload)) return emptyReduction(slice, true);
      const message = typeof event.payload.error === "string" ? event.payload.error : "Runtime start failed";
      const sessionStatus = readSessionStatus(event.payload.session_status);
      const updatedAt =
        typeof event.payload.session_updated_at === "string" ? event.payload.session_updated_at : time;
      return {
        sessions: sessionStatus
          ? updateSession(slice.sessions, sessionId, (session) => ({
              ...session,
              status: sessionStatus,
              updated_at: updatedAt
            }))
          : slice.sessions,
        conversations: clearStreaming(slice.conversations, sessionId, options.emptyConversation),
        handled: true,
        notifications: [{ level: "error", message }]
      };
    }
    case "session.checkpoint_retired":
    case "session.restore_fallback_retired": {
      if (!sessionId || !isRecord(event.payload)) return emptyReduction(slice, true);
      const sessionStatus = readSessionStatus(event.payload.session_status) ?? "running_idle";
      const updatedAt =
        typeof event.payload.session_updated_at === "string" ? event.payload.session_updated_at : time;
      const activeGenerationId =
        typeof event.payload.active_generation_id === "string" ? event.payload.active_generation_id : undefined;
      const lastActivityAt =
        typeof event.payload.session_last_activity_at === "string" ? event.payload.session_last_activity_at : null;
      return {
        sessions: updateSession(slice.sessions, sessionId, (session) => ({
          ...session,
          status: sessionStatus,
          updated_at: updatedAt,
          active_generation_id: activeGenerationId ?? session.active_generation_id,
          last_activity_at: lastActivityAt,
          checkpoint_path: null,
          restore_ms: null
        })),
        conversations: slice.conversations,
        handled: true,
        notifications: []
      };
    }
    case "ack_turn_completed": {
      if (!sessionId || !isRecord(event.payload)) return emptyReduction(slice, true);
      const turnStatus = typeof event.payload.status === "string" ? event.payload.status : "completed";
      const sessionStatus = readSessionStatus(event.payload.session_status);
      const updatedAt =
        typeof event.payload.session_updated_at === "string" ? event.payload.session_updated_at : time;
      const notifications: SessionEventNotification[] = [];
      if (turnStatus === "failed" || turnStatus === "canceled") {
        const message =
          typeof event.payload.error === "string" && event.payload.error
            ? event.payload.error
            : turnStatus === "canceled"
              ? "Turn canceled"
              : "Turn failed";
        notifications.push({ level: "error", message });
      }
      return {
        sessions: sessionStatus
          ? updateSession(slice.sessions, sessionId, (session) => ({
              ...session,
              status: sessionStatus,
              updated_at: updatedAt
            }))
          : slice.sessions,
        conversations: clearStreaming(slice.conversations, sessionId, options.emptyConversation),
        handled: true,
        notifications
      };
    }
    default:
      return emptyReduction(slice);
  }
}
