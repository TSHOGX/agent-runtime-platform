"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState
} from "react";
import { toast } from "sonner";

import {
  createSession as apiCreateSession,
  destroySession as apiDestroySession,
  fetchArtifacts,
  fetchHealth,
  fetchMessages,
  fetchSession,
  fetchSessions,
  interruptSession as apiInterruptSession,
  postMessage as apiPostMessage
} from "@/lib/api";
import type { RuntimeAgent } from "@/lib/agents";
import { reduceSessionEvent } from "@/lib/session-events";
import { buildEventsStreamUrl } from "@/lib/ws";
import type {
  ApiArtifact,
  ApiMessage,
  ApiSession,
  ConnectionStatus,
  HarnessEvent,
  SessionStatus,
  StreamLine
} from "@/lib/types";

type ConversationState = {
  messages: ApiMessage[];
  streaming: Array<{ id: string; text: string; deltaType?: string }>;
  stream: StreamLine[];
  loading: boolean;
};

type HarnessState = {
  ready: boolean;
  bootError: string | null;
  connection: ConnectionStatus;
  sessions: ApiSession[];
  selectedId: string | null;
  conversations: Record<string, ConversationState>;
  artifacts: Record<string, ApiArtifact[]>;
};

type HarnessApi = {
  state: HarnessState;
  selectSession: (id: string | null) => void;
  createSession: (agent: RuntimeAgent) => Promise<{ ok: boolean; error?: string; session?: ApiSession }>;
  destroySession: (id: string) => Promise<void>;
  interruptSession: (id: string) => Promise<{ ok: boolean; error?: string }>;
  sendMessage: (id: string, content: string) => Promise<{ ok: boolean; error?: string }>;
  refresh: () => Promise<void>;
};

const HarnessContext = createContext<HarnessApi | null>(null);

const initialState: HarnessState = {
  ready: false,
  bootError: null,
  connection: "connecting",
  sessions: [],
  selectedId: null,
  conversations: {},
  artifacts: {}
};

const RUNNING_STATUSES = new Set<SessionStatus>(["running_active"]);
const MESSAGE_POLL_INTERVAL_MS = 1000;
const MESSAGE_POLL_TIMEOUT_MS = 120_000;
const SSE_TYPED_EVENT_TYPES = [
  "session.created",
  "session.running_active",
  "session.running_idle",
  "session.checkpointing",
  "session.checkpointed",
  "session.failed",
  "session.destroyed",
  "message.created",
  "agent.message",
  "agent.delta",
  "agent.output",
  "system.status",
  "session.error",
  "generation.error",
  "session.checkpoint_retired",
  "session.restore_fallback_retired",
  "artifact.updated",
  "artifact.deleted",
  "ack_turn_started",
  "emit_output",
  "ack_turn_completed",
  "proxy.request.started",
  "proxy.request.completed",
  "proxy.request.failed",
  "replay_gap",
  "error"
] as const;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function readSession(payload: unknown): ApiSession | null {
  if (!isRecord(payload)) return null;
  const id = typeof payload.id === "string" ? payload.id : null;
  if (!id) return null;
  return payload as unknown as ApiSession;
}

function readArtifact(payload: unknown): ApiArtifact | null {
  if (!isRecord(payload)) return null;
  const sessionId = typeof payload.session_id === "string" ? payload.session_id : null;
  const path = typeof payload.path === "string" ? payload.path : null;
  if (!sessionId || !path) return null;
  return payload as unknown as ApiArtifact;
}

function newId() {
  return `${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

function emptyConvo(): ConversationState {
  return { messages: [], streaming: [], stream: [], loading: false };
}

function delay(ms: number) {
  return new Promise<void>((resolve) => setTimeout(resolve, ms));
}

function isActiveStatus(status: SessionStatus) {
  return RUNNING_STATUSES.has(status);
}

function appendStreaming(
  streaming: ConversationState["streaming"],
  id: string,
  text: string,
  deltaType?: string
): ConversationState["streaming"] {
  const idx = streaming.findIndex((entry) => entry.id === id);
  if (idx === -1) {
    return [...streaming, { id, text, deltaType }];
  }
  return streaming.map((entry, entryIdx) =>
    entryIdx === idx ? { ...entry, text: entry.text + text } : entry
  );
}

function isCommittedStream(streamText: string, messageText: string) {
  const stream = streamText.trim();
  const message = messageText.trim();
  return stream !== "" && message !== "" && (stream === message || message.startsWith(stream));
}

function pruneCommittedStreaming(
  streaming: ConversationState["streaming"],
  messages: ApiMessage[]
): ConversationState["streaming"] {
  let next = streaming;
  for (const message of messages) {
    if (message.role !== "assistant") continue;
    const idx = next.findIndex((entry) => isCommittedStream(entry.text, message.content));
    if (idx !== -1) {
      next = [...next.slice(0, idx), ...next.slice(idx + 1)];
    }
  }
  return next;
}

export function HarnessProvider({ children }: { children: React.ReactNode }) {
  const [state, setState] = useState<HarnessState>(initialState);
  const stateRef = useRef(state);

  const aliveRef = useRef(true);
  const pollingRef = useRef<Set<string>>(new Set());
  const connectedOnceRef = useRef(false);
  const lastEventIDRef = useRef<number | null>(null);

  useEffect(() => {
    stateRef.current = state;
  }, [state]);

  const ensureConvo = useCallback((convos: HarnessState["conversations"], id: string) => {
    if (convos[id]) return convos;
    return { ...convos, [id]: emptyConvo() };
  }, []);

  const upsertConvo = useCallback(
    (id: string, patch: (prev: ConversationState) => ConversationState) => {
      setState((prev) => {
        const current = prev.conversations[id] ?? emptyConvo();
        return { ...prev, conversations: { ...prev.conversations, [id]: patch(current) } };
      });
    },
    []
  );

  const loadSessionDetails = useCallback(
    (id: string, skipLoaded = false) => {
      const convo = stateRef.current.conversations[id];
      if (skipLoaded && convo && convo.messages.length > 0) return;
      upsertConvo(id, (c) => ({ ...c, loading: true }));
      void fetchMessages(id).then((res) => {
        upsertConvo(id, (c) => ({
          ...c,
          loading: false,
          messages: res.ok ? res.data.messages ?? [] : c.messages
        }));
      });
      void fetchArtifacts(id).then((res) => {
        if (!res.ok) return;
        const list = res.data.artifacts ?? [];
        setState((p) => ({ ...p, artifacts: { ...p.artifacts, [id]: list } }));
      });
    },
    [upsertConvo]
  );

  const pollConversation = useCallback(
    async (id: string, afterMessageId?: number) => {
      if (pollingRef.current.has(id)) return;
      pollingRef.current.add(id);
      const deadline = Date.now() + MESSAGE_POLL_TIMEOUT_MS;

      try {
        while (aliveRef.current && Date.now() < deadline) {
          const [messagesRes, sessionRes, artifactsRes] = await Promise.all([
            fetchMessages(id),
            fetchSession(id),
            fetchArtifacts(id)
          ]);
          let sawAssistant = false;
          let active = true;

          if (sessionRes.ok) {
            const session = sessionRes.data;
            active = isActiveStatus(session.status);
            setState((p) => ({
              ...p,
              sessions: p.sessions.some((s) => s.id === session.id)
                ? p.sessions.map((s) => (s.id === session.id ? session : s))
                : [session, ...p.sessions],
              conversations: ensureConvo(p.conversations, session.id)
            }));
            if (!active) {
              upsertConvo(id, (c) => ({ ...c, streaming: [] }));
            }
          }

          if (messagesRes.ok) {
            const messages = messagesRes.data.messages ?? [];
            const committedAssistantMessages = messages.filter(
              (m) => m.role === "assistant" && (afterMessageId === undefined || m.id > afterMessageId)
            );
            sawAssistant = committedAssistantMessages.length > 0;
            upsertConvo(id, (c) => ({
              ...c,
              loading: false,
              streaming: active ? pruneCommittedStreaming(c.streaming, committedAssistantMessages) : [],
              messages
            }));
          }

          if (artifactsRes.ok) {
            const list = artifactsRes.data.artifacts ?? [];
            setState((p) => ({ ...p, artifacts: { ...p.artifacts, [id]: list } }));
          }

          if (!active || (sawAssistant && !sessionRes.ok)) return;
          await delay(MESSAGE_POLL_INTERVAL_MS);
        }
      } finally {
        pollingRef.current.delete(id);
      }
    },
    [ensureConvo, upsertConvo]
  );

  const handleEvent = useCallback(
    (event: HarnessEvent) => {
      const sessionId = event.session_id;
      const time = event.time ?? new Date().toISOString();
      const applySessionEvent = () => {
        const preview = reduceSessionEvent(
          { sessions: stateRef.current.sessions, conversations: stateRef.current.conversations },
          event,
          { emptyConversation: emptyConvo, time }
        );
        if (!preview.handled) return false;
        for (const notification of preview.notifications) {
          if (notification.level === "error") {
            toast.error(notification.message, { duration: 6000 });
          }
        }
        setState((p) => {
          const next = reduceSessionEvent(
            { sessions: p.sessions, conversations: p.conversations },
            event,
            { emptyConversation: emptyConvo, time }
          );
          return { ...p, sessions: next.sessions, conversations: next.conversations };
        });
        return true;
      };
      switch (event.type) {
        case "session.created": {
          const sess = readSession(event.payload);
          if (!sess) return;
          setState((p) => ({
            ...p,
            sessions: [sess, ...p.sessions.filter((s) => s.id !== sess.id)],
            conversations: ensureConvo(p.conversations, sess.id)
          }));
          return;
        }
        case "session.running_active":
        case "session.running_idle":
        case "session.checkpointing":
        case "session.checkpointed":
        case "session.failed":
        case "session.destroyed": {
          applySessionEvent();
          return;
        }
        case "message.created": {
          if (!sessionId) return;
          const msg = event.payload as ApiMessage | undefined;
          if (!msg || typeof msg.id !== "number") return;
          upsertConvo(sessionId, (c) => ({
            ...c,
            messages: c.messages.some((m) => m.id === msg.id) ? c.messages : [...c.messages, msg]
          }));
          return;
        }
        case "agent.message": {
          if (!sessionId) return;
          const msg = event.payload as ApiMessage | undefined;
          if (!msg || typeof msg.id !== "number") return;
          upsertConvo(sessionId, (c) => ({
            ...c,
            streaming: pruneCommittedStreaming(c.streaming, [msg]),
            messages: c.messages.some((m) => m.id === msg.id) ? c.messages : [...c.messages, msg]
          }));
          return;
        }
        case "agent.delta": {
          if (!sessionId || !isRecord(event.payload)) return;
          const text = typeof event.payload.text === "string" ? event.payload.text : "";
          const id = typeof event.payload.message_id === "string" ? event.payload.message_id : "assistant_pending";
          const deltaType = typeof event.payload.delta_type === "string" ? event.payload.delta_type : "text_delta";
          if (deltaType !== "text_delta") return;
          if (!text) return;
          upsertConvo(sessionId, (c) => ({ ...c, streaming: appendStreaming(c.streaming, id, text, deltaType) }));
          return;
        }
        case "agent.output": {
          if (!sessionId || !isRecord(event.payload)) return;
          const stream = typeof event.payload.stream === "string" ? event.payload.stream : "stdout";
          const line = typeof event.payload.line === "string" ? event.payload.line : "";
          if (!line) return;
          const entry: StreamLine = {
            id: newId(),
            session_id: sessionId,
            stream,
            line,
            time
          };
          upsertConvo(sessionId, (c) => ({ ...c, stream: [...c.stream, entry].slice(-400) }));
          return;
        }
        case "system.status": {
          // System status messages: display as Toast notification
          if (!sessionId || !isRecord(event.payload)) return;
          const line = typeof event.payload.line === "string" ? event.payload.line : "";
          if (!line) return;

          // Display Toast notification
          toast.info(line, { duration: 3000 });

          // Also log in development mode
          if (process.env.NODE_ENV === "development") {
            console.log(`[System] ${line}`);
          }
          return;
        }
        case "session.error": {
          applySessionEvent();
          return;
        }
        case "generation.error": {
          applySessionEvent();
          return;
        }
        case "session.checkpoint_retired":
        case "session.restore_fallback_retired": {
          applySessionEvent();
          return;
        }
        case "ack_turn_completed": {
          applySessionEvent();
          return;
        }
        case "artifact.updated": {
          const artifact = readArtifact(event.payload);
          if (!artifact) return;
          setState((p) => {
            const list = p.artifacts[artifact.session_id] ?? [];
            const next = [
              artifact,
              ...list.filter((a) => a.path !== artifact.path)
            ];
            return { ...p, artifacts: { ...p.artifacts, [artifact.session_id]: next } };
          });
          return;
        }
        case "artifact.deleted": {
          if (!isRecord(event.payload)) return;
          const deletedSessionId =
            typeof event.payload.session_id === "string" ? event.payload.session_id : sessionId;
          const path = typeof event.payload.path === "string" ? event.payload.path : null;
          if (!deletedSessionId || !path) return;
          setState((p) => {
            const list = p.artifacts[deletedSessionId] ?? [];
            const next = list.filter((a) => a.path !== path && !a.path.startsWith(`${path}/`));
            return { ...p, artifacts: { ...p.artifacts, [deletedSessionId]: next } };
          });
          return;
        }
        default:
          return;
      }
    },
    [ensureConvo, upsertConvo]
  );

  const handleEventRef = useRef(handleEvent);
  useEffect(() => {
    handleEventRef.current = handleEvent;
  }, [handleEvent]);

  const refresh = useCallback(async () => {
    const health = await fetchHealth();
    if (!health.ok) {
      setState((p) => ({ ...p, ready: true, bootError: health.error, sessions: [] }));
      return;
    }
    const sessions = await fetchSessions();
    if (!sessions.ok) {
      setState((p) => ({ ...p, ready: true, bootError: sessions.error, sessions: [] }));
      return;
    }
    const list = sessions.data.sessions ?? [];
    const currentSelected = stateRef.current.selectedId;
    const selectedId = currentSelected && list.some((s) => s.id === currentSelected) ? currentSelected : list[0]?.id ?? null;
    setState((p) => {
      const conversations = { ...p.conversations };
      for (const s of list) {
        if (!conversations[s.id]) conversations[s.id] = emptyConvo();
      }
      return { ...p, ready: true, bootError: null, sessions: list, selectedId, conversations };
    });
    if (selectedId) {
      loadSessionDetails(selectedId, true);
      const selected = list.find((s) => s.id === selectedId);
      if (selected && isActiveStatus(selected.status)) {
        void pollConversation(selectedId);
      }
    }
  }, [loadSessionDetails, pollConversation]);

  useEffect(() => {
    aliveRef.current = true;
    let cleanedUp = false;
    connectedOnceRef.current = false;

    let source: EventSource;
    try {
      source = new EventSource(buildEventsStreamUrl(undefined, lastEventIDRef.current));
    } catch {
      const failureTimer = setTimeout(() => {
        if (!cleanedUp) {
          setState((p) => ({ ...p, connection: "down" }));
        }
      }, 0);
      return () => {
        cleanedUp = true;
        aliveRef.current = false;
        clearTimeout(failureTimer);
      };
    }

    source.onopen = () => {
      if (cleanedUp) return;
      const wasConnected = connectedOnceRef.current;
      connectedOnceRef.current = true;
      setState((p) => ({ ...p, connection: "live" }));
      if (wasConnected) {
        void refresh();
      }
    };

    const handleSourceEvent = (ev: MessageEvent) => {
      try {
        const data = JSON.parse(typeof ev.data === "string" ? ev.data : "");
        const eventID =
          data && typeof data.event_id === "number"
            ? data.event_id
            : Number.parseInt(ev.lastEventId, 10);
        if (Number.isFinite(eventID) && eventID > 0) {
          lastEventIDRef.current = eventID;
        }
        if (data && data.type === "replay_gap") {
          void refresh();
          return;
        }
        if (data && typeof data.type === "string") {
          handleEventRef.current(data as HarnessEvent);
        }
      } catch {
        // ignore malformed frames
      }
    };
    source.onmessage = handleSourceEvent;
    for (const eventType of SSE_TYPED_EVENT_TYPES) {
      source.addEventListener(eventType, handleSourceEvent);
    }

    source.onerror = () => {
      if (cleanedUp) return;
      if (source.readyState === EventSource.CLOSED) {
        setState((p) => ({ ...p, connection: "down" }));
        return;
      }
      setState((p) => ({ ...p, connection: connectedOnceRef.current ? "reconnecting" : "connecting" }));
    };

    const bootTimer = setTimeout(() => {
      void refresh();
    }, 0);

    return () => {
      cleanedUp = true;
      aliveRef.current = false;
      clearTimeout(bootTimer);
      for (const eventType of SSE_TYPED_EVENT_TYPES) {
        source.removeEventListener(eventType, handleSourceEvent);
      }
      source.close();
    };
  }, [refresh]);

  const selectSession = useCallback(
    (id: string | null) => {
      setState((p) => ({ ...p, selectedId: id }));
      if (!id) return;
      loadSessionDetails(id, true);
      const session = stateRef.current.sessions.find((s) => s.id === id);
      if (session && isActiveStatus(session.status)) {
        void pollConversation(id);
      }
    },
    [loadSessionDetails, pollConversation]
  );

  const createSession = useCallback(
    async (agent: RuntimeAgent) => {
      const res = await apiCreateSession(agent);
      if (!res.ok) return { ok: false as const, error: res.error };
      const session = res.data;
      setState((p) => ({
        ...p,
        sessions: [session, ...p.sessions.filter((s) => s.id !== session.id)],
        selectedId: session.id,
        conversations: { ...p.conversations, [session.id]: emptyConvo() }
      }));
      return { ok: true as const, session };
    },
    []
  );

  const destroySession = useCallback(async (id: string) => {
    await apiDestroySession(id);
  }, []);

  const interruptSession = useCallback(async (id: string) => {
    const res = await apiInterruptSession(id);
    if (!res.ok) return { ok: false as const, error: res.error };
    return { ok: true as const };
  }, []);

  const sendMessage = useCallback(
    async (id: string, content: string) => {
      const res = await apiPostMessage(id, content);
      if (!res.ok) return { ok: false as const, error: res.error };
      // server emits message.created via WS; merge here for instant feedback
      const msg = res.data.message;
      if (msg) {
        upsertConvo(id, (c) => ({
          ...c,
          messages: c.messages.some((m) => m.id === msg.id) ? c.messages : [...c.messages, msg]
        }));
      }
      setState((p) => ({
        ...p,
        sessions: p.sessions.map((s) =>
          s.id === id ? { ...s, status: res.data.status, updated_at: new Date().toISOString() } : s
        )
      }));
      void pollConversation(id, msg?.id);
      return { ok: true as const };
    },
    [pollConversation, upsertConvo]
  );

  const value = useMemo<HarnessApi>(
    () => ({ state, selectSession, createSession, destroySession, interruptSession, sendMessage, refresh }),
    [state, selectSession, createSession, destroySession, interruptSession, sendMessage, refresh]
  );

  return <HarnessContext.Provider value={value}>{children}</HarnessContext.Provider>;
}

export function useHarness() {
  const ctx = useContext(HarnessContext);
  if (!ctx) throw new Error("useHarness must be used inside HarnessProvider");
  return ctx;
}

export function useSelectedSession() {
  const { state } = useHarness();
  if (!state.selectedId) return null;
  return state.sessions.find((s) => s.id === state.selectedId) ?? null;
}

export function useConversation(sessionId: string | null) {
  const { state } = useHarness();
  if (!sessionId) return emptyConvo();
  return state.conversations[sessionId] ?? emptyConvo();
}

export function useArtifacts(sessionId: string | null) {
  const { state } = useHarness();
  if (!sessionId) return [];
  return state.artifacts[sessionId] ?? [];
}
