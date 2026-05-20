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

import {
  createSession as apiCreateSession,
  destroySession as apiDestroySession,
  fetchArtifacts,
  fetchHealth,
  fetchMessages,
  fetchSessions,
  postMessage as apiPostMessage
} from "@/lib/api";
import { buildEventsWebSocketUrl } from "@/lib/ws";
import type {
  AgentKind,
  ApiArtifact,
  ApiMessage,
  ApiSession,
  ConnectionStatus,
  HarnessEvent,
  StreamLine
} from "@/lib/types";

type ConversationState = {
  messages: ApiMessage[];
  streaming: { id: string; text: string } | null;
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
  createSession: (agent: AgentKind) => Promise<{ ok: boolean; error?: string; session?: ApiSession }>;
  destroySession: (id: string) => Promise<void>;
  sendMessage: (id: string, content: string) => Promise<{ ok: boolean; error?: string }>;
  refresh: () => Promise<void>;
};

const HarnessContext = createContext<HarnessApi | null>(null);

const initialState: HarnessState = {
  ready: false,
  bootError: null,
  connection: "idle",
  sessions: [],
  selectedId: null,
  conversations: {},
  artifacts: {}
};

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
  return { messages: [], streaming: null, stream: [], loading: false };
}

export function HarnessProvider({ children }: { children: React.ReactNode }) {
  const [state, setState] = useState<HarnessState>(initialState);
  const stateRef = useRef(state);

  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const backoffRef = useRef(1000);
  const aliveRef = useRef(true);

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

  const handleEvent = useCallback(
    (event: HarnessEvent) => {
      const sessionId = event.session_id;
      const time = event.time ?? new Date().toISOString();
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
        case "session.running":
        case "session.idle":
        case "session.completed":
        case "session.failed":
        case "session.destroyed": {
          if (!sessionId) return;
          const status = event.type.split(".")[1];
          setState((p) => ({
            ...p,
            sessions: p.sessions.map((s) => (s.id === sessionId ? { ...s, status, updated_at: time } : s))
          }));
          if (status !== "running") {
            upsertConvo(sessionId, (c) => ({ ...c, streaming: null }));
          }
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
            streaming: null,
            messages: c.messages.some((m) => m.id === msg.id) ? c.messages : [...c.messages, msg]
          }));
          return;
        }
        case "agent.delta": {
          if (!sessionId || !isRecord(event.payload)) return;
          const text = typeof event.payload.text === "string" ? event.payload.text : "";
          const id = typeof event.payload.message_id === "string" ? event.payload.message_id : "assistant_pending";
          if (!text) return;
          upsertConvo(sessionId, (c) => {
            if (!c.streaming || c.streaming.id !== id) {
              return { ...c, streaming: { id, text } };
            }
            return { ...c, streaming: { id, text: c.streaming.text + text } };
          });
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
    setState((p) => {
      const conversations = { ...p.conversations };
      for (const s of list) {
        if (!conversations[s.id]) conversations[s.id] = emptyConvo();
      }
      const selectedId = p.selectedId && list.some((s) => s.id === p.selectedId) ? p.selectedId : list[0]?.id ?? null;
      return { ...p, ready: true, bootError: null, sessions: list, selectedId, conversations };
    });
  }, []);

  useEffect(() => {
    aliveRef.current = true;
    let cleanedUp = false;

    const connect = () => {
      if (cleanedUp) return;
      if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) return;
      setState((p) => ({ ...p, connection: p.connection === "live" ? "reconnecting" : "connecting" }));
      let ws: WebSocket;
      try {
        ws = new WebSocket(buildEventsWebSocketUrl());
      } catch {
        schedule();
        return;
      }
      wsRef.current = ws;
      ws.onopen = () => {
        backoffRef.current = 1000;
        setState((p) => ({ ...p, connection: "live" }));
      };
      ws.onmessage = (ev) => {
        try {
          const data = JSON.parse(typeof ev.data === "string" ? ev.data : "");
          if (data && typeof data.type === "string") {
            handleEventRef.current(data as HarnessEvent);
          }
        } catch {
          // ignore malformed frames
        }
      };
      ws.onclose = () => {
        wsRef.current = null;
        if (cleanedUp) return;
        setState((p) => ({ ...p, connection: "down" }));
        schedule();
      };
    };
    const schedule = () => {
      if (cleanedUp) return;
      if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
      const delay = Math.min(backoffRef.current, 8000);
      reconnectTimerRef.current = setTimeout(() => {
        backoffRef.current = Math.min(backoffRef.current * 2, 8000);
        connect();
      }, delay);
    };

    const bootTimer = setTimeout(() => {
      void refresh();
      connect();
    }, 0);

    return () => {
      cleanedUp = true;
      aliveRef.current = false;
      clearTimeout(bootTimer);
      if (reconnectTimerRef.current) clearTimeout(reconnectTimerRef.current);
      if (wsRef.current) {
        try { wsRef.current.close(); } catch { /* noop */ }
      }
    };
  }, [refresh]);

  const selectSession = useCallback(
    (id: string | null) => {
      setState((p) => ({ ...p, selectedId: id }));
      if (!id) return;
      const convo = stateRef.current.conversations[id];
      if (convo && convo.messages.length > 0) return;
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

  const createSession = useCallback(
    async (agent: AgentKind) => {
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
      return { ok: true as const };
    },
    [upsertConvo]
  );

  const value = useMemo<HarnessApi>(
    () => ({ state, selectSession, createSession, destroySession, sendMessage, refresh }),
    [state, selectSession, createSession, destroySession, sendMessage, refresh]
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
