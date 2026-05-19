"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  buildArtifactHref,
  buildEventsWebSocketUrl,
  canSendFirstTask,
  choosePrimarySessionId,
  createOutputEntry,
  displayAgentName,
  formatBytes,
  formatFullTime,
  formatTime,
  isBackendUnavailable,
  requestJson,
  statusLabel,
  statusTone,
  type ApiArtifact,
  type ApiSession,
  type BackendMode,
  type HarnessEvent,
  type OutputEntry,
  type SendMessageResponse
} from "@/lib/dashboard";
import {
  createMockSession,
  createMockTaskOutput,
  getMockSessionOutput,
  mockArtifactsBySession,
  mockOutputBySession,
  mockSessions
} from "@/lib/mock";

type BackendState = {
  mode: "loading" | BackendMode;
  title: string;
  detail: string;
  checkedAt: string | null;
};

type ArtifactState = {
  loading: boolean;
  error: string | null;
};

type RunState = {
  submitting: boolean;
  error: string | null;
};

type StreamState = {
  status: "idle" | "connecting" | "live" | "mock" | "failed";
  detail: string;
  error: string | null;
  lastEventAt: string | null;
};

type AgentOption = "claude" | "opencode" | "sh";

const AGENT_OPTIONS: { value: AgentOption; label: string }[] = [
  { value: "claude", label: "Claude Code" },
  { value: "opencode", label: "OpenCode" },
  { value: "sh", label: "Shell smoke" }
];

const DEFAULT_TASK_PROMPT =
  "Summarize the available data, write a concise report, and save artifacts under /workspace.";

function cloneArtifactMap(source: Record<string, ApiArtifact[]>) {
  const next: Record<string, ApiArtifact[]> = {};
  for (const [sessionId, artifacts] of Object.entries(source)) {
    next[sessionId] = [...artifacts];
  }
  return next;
}

function cloneOutputMap(source: Record<string, OutputEntry[]>) {
  const next: Record<string, OutputEntry[]> = {};
  for (const [sessionId, entries] of Object.entries(source)) {
    next[sessionId] = [...entries];
  }
  return next;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function readString(value: unknown) {
  return typeof value === "string" && value.trim() ? value : null;
}

function readOptionalString(value: unknown) {
  return typeof value === "string" ? value : null;
}

function readOptionalNumber(value: unknown) {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function getSessionPillTone(state: BackendState) {
  if (state.mode === "real") {
    return "mode-real";
  }
  if (state.mode === "mock") {
    return "mode-mock";
  }
  return "mode-loading";
}

function getStatusDotTone(state: BackendState) {
  if (state.mode === "real") {
    return "status-real";
  }
  if (state.mode === "mock") {
    return "status-mock";
  }
  return "muted";
}

function getStreamTone(state: StreamState) {
  switch (state.status) {
    case "live":
      return "status-completed";
    case "mock":
      return "status-running";
    case "failed":
      return "status-failed";
    case "connecting":
      return "status-running";
    default:
      return "status-ready";
  }
}

function getStreamLabel(state: StreamState) {
  switch (state.status) {
    case "connecting":
      return "Connecting";
    case "live":
      return "Live";
    case "mock":
      return "Mock";
    case "failed":
      return "Failed";
    default:
      return "Idle";
  }
}

function parseHarnessEvent(raw: string): HarnessEvent | null {
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!isRecord(parsed) || typeof parsed.type !== "string") {
      return null;
    }

    return {
      type: parsed.type,
      session_id: readOptionalString(parsed.session_id) ?? undefined,
      time: readOptionalString(parsed.time) ?? undefined,
      payload: parsed.payload
    };
  } catch {
    return null;
  }
}

function extractSessionPayload(payload: unknown): ApiSession | null {
  if (!isRecord(payload)) {
    return null;
  }

  const id = readString(payload.id);
  const userId = readString(payload.user_id);
  const status = readString(payload.status);
  const agent = readString(payload.agent);
  const workspace = readString(payload.workspace);
  const restoreId = readString(payload.restore_id);
  const createdAt = readString(payload.created_at);
  const updatedAt = readString(payload.updated_at);

  if (!id || !userId || !status || !agent || !workspace || !restoreId || !createdAt || !updatedAt) {
    return null;
  }

  return {
    id,
    user_id: userId,
    status,
    agent,
    workspace,
    restore_id: restoreId,
    restore_ms: readOptionalNumber(payload.restore_ms),
    created_at: createdAt,
    updated_at: updatedAt,
    expires_at: readOptionalString(payload.expires_at),
    completed_at: readOptionalString(payload.completed_at)
  };
}

function extractArtifactPayload(payload: unknown): ApiArtifact | null {
  if (!isRecord(payload)) {
    return null;
  }

  const sessionId = readString(payload.session_id);
  const path = readString(payload.path);
  const size = readOptionalNumber(payload.size);
  const modTime = readString(payload.mod_time);
  const createdAt = readString(payload.created_at);
  const updatedAt = readString(payload.updated_at);

  if (!sessionId || !path || size === null || !modTime || !createdAt || !updatedAt) {
    return null;
  }

  return {
    session_id: sessionId,
    path,
    size,
    mod_time: modTime,
    created_at: createdAt,
    updated_at: updatedAt
  };
}

function extractErrorMessage(payload: unknown) {
  if (isRecord(payload) && typeof payload.error === "string" && payload.error.trim()) {
    return payload.error;
  }

  return null;
}

function extractRestoreMs(payload: unknown) {
  if (!isRecord(payload)) {
    return null;
  }

  return readOptionalNumber(payload.restore_ms);
}

function applySessionPatch(sessions: ApiSession[], sessionId: string, patch: Partial<ApiSession>) {
  return sessions.map((session) => (session.id === sessionId ? { ...session, ...patch } : session));
}

function upsertSessionItem(sessions: ApiSession[], nextSession: ApiSession) {
  return [nextSession, ...sessions.filter((session) => session.id !== nextSession.id)];
}

function upsertArtifact(artifacts: ApiArtifact[], nextArtifact: ApiArtifact) {
  return [
    nextArtifact,
    ...artifacts.filter(
      (artifact) =>
        artifact.session_id !== nextArtifact.session_id || artifact.path !== nextArtifact.path
    )
  ];
}

function outputForSelectedSession(
  backendMode: "loading" | BackendMode,
  selectedSessionId: string | null,
  outputBySession: Record<string, OutputEntry[]>
) {
  if (!selectedSessionId) {
    return [];
  }

  if (backendMode === "mock") {
    return outputBySession[selectedSessionId] ?? getMockSessionOutput(selectedSessionId);
  }

  return outputBySession[selectedSessionId] ?? [];
}

export default function Workbench() {
  const wsRef = useRef<WebSocket | null>(null);
  const wsTokenRef = useRef(0);
  const sessionsRef = useRef<ApiSession[]>([]);

  const [backend, setBackend] = useState<BackendState>({
    mode: "loading",
    title: "Checking real backend",
    detail: "Reading /api/healthz",
    checkedAt: null
  });
  const [sessions, setSessions] = useState<ApiSession[]>([]);
  const [selectedSessionId, setSelectedSessionId] = useState<string | null>(null);
  const [artifactsBySession, setArtifactsBySession] = useState<Record<string, ApiArtifact[]>>({});
  const [outputBySession, setOutputBySession] = useState<Record<string, OutputEntry[]>>({});
  const [artifactState, setArtifactState] = useState<ArtifactState>({
    loading: false,
    error: null
  });
  const [selectedAgent, setSelectedAgent] = useState<AgentOption>("claude");
  const [creatingSession, setCreatingSession] = useState(false);
  const [createSessionError, setCreateSessionError] = useState<string | null>(null);
  const [taskPrompt, setTaskPrompt] = useState(DEFAULT_TASK_PROMPT);
  const [runState, setRunState] = useState<RunState>({
    submitting: false,
    error: null
  });
  const [streamState, setStreamState] = useState<StreamState>({
    status: "idle",
    detail: "Waiting for a selected session.",
    error: null,
    lastEventAt: null
  });
  const [sessionError, setSessionError] = useState<string | null>(null);
  const [booting, setBooting] = useState(true);
  const [retryTick, setRetryTick] = useState(0);

  const selectedSession = useMemo(
    () => sessions.find((session) => session.id === selectedSessionId) ?? null,
    [sessions, selectedSessionId]
  );

  useEffect(() => {
    sessionsRef.current = sessions;
  }, [sessions]);

  const displayedArtifacts = useMemo(() => {
    if (!selectedSessionId) {
      return [];
    }

    if (backend.mode === "mock") {
      return mockArtifactsBySession[selectedSessionId] ?? artifactsBySession[selectedSessionId] ?? [];
    }

    return artifactsBySession[selectedSessionId] ?? [];
  }, [artifactsBySession, backend.mode, selectedSessionId]);

  const displayedOutput = useMemo(
    () => outputForSelectedSession(backend.mode, selectedSessionId, outputBySession),
    [backend.mode, outputBySession, selectedSessionId]
  );

  const closeEventStream = useCallback(() => {
    wsTokenRef.current += 1;
    const socket = wsRef.current;
    wsRef.current = null;

    if (socket && socket.readyState !== WebSocket.CLOSED && socket.readyState !== WebSocket.CLOSING) {
      try {
        socket.close();
      } catch {
        // Ignore stale socket failures.
      }
    }
  }, []);

  const patchSessionInList = useCallback((sessionId: string, patch: Partial<ApiSession>) => {
    setSessions((current) => applySessionPatch(current, sessionId, patch));
  }, []);

  const appendSessionOutput = useCallback((sessionId: string, entry: OutputEntry) => {
    setOutputBySession((current) => ({
      ...current,
      [sessionId]: [...(current[sessionId] ?? []), entry]
    }));
  }, []);

  const replaceSessionOutput = useCallback((sessionId: string, entries: OutputEntry[]) => {
    setOutputBySession((current) => ({
      ...current,
      [sessionId]: entries
    }));
  }, []);

  const upsertArtifactForSession = useCallback((artifact: ApiArtifact) => {
    setArtifactsBySession((current) => ({
      ...current,
      [artifact.session_id]: upsertArtifact(current[artifact.session_id] ?? [], artifact)
    }));
  }, []);

  const enterMockFallback = useCallback(
    (detail: string) => {
      closeEventStream();
      setBackend({
        mode: "mock",
        title: "Mock fallback",
        detail,
        checkedAt: new Date().toISOString()
      });
      setSessions(mockSessions);
      setSelectedSessionId(choosePrimarySessionId(mockSessions));
      setArtifactsBySession(cloneArtifactMap(mockArtifactsBySession));
      setOutputBySession(cloneOutputMap(mockOutputBySession));
      setArtifactState({ loading: false, error: null });
      setCreateSessionError(null);
      setSessionError(null);
      setRunState({ submitting: false, error: null });
      setStreamState({
        status: "mock",
        detail: detail || "Streaming cached mock data.",
        error: null,
        lastEventAt: new Date().toISOString()
      });
      setBooting(false);
    },
    [closeEventStream]
  );

  const handleHarnessEvent = useCallback(
    (event: HarnessEvent, fallbackSessionId: string | null) => {
      const eventSessionId = event.session_id ?? fallbackSessionId;
      const eventTime = event.time ?? new Date().toISOString();

      if (event.type === "agent.output") {
        if (!eventSessionId || !isRecord(event.payload)) {
          return;
        }

        const stream = readString(event.payload.stream) ?? "runtime";
        const line = readString(event.payload.line) ?? "";
        if (!line) {
          return;
        }

        const entry = createOutputEntry({
          stream,
          line,
          source: "real",
          time: eventTime,
          id: `${eventSessionId}_${eventTime}_${Math.random().toString(36).slice(2, 8)}`
        });

        appendSessionOutput(eventSessionId, entry);
        setStreamState({
          status: "live",
          detail: "Streaming agent output.",
          error: null,
          lastEventAt: eventTime
        });
        return;
      }

      if (event.type === "session.created") {
        const session = extractSessionPayload(event.payload);
        if (!session) {
          return;
        }

        setSessions((current) => upsertSessionItem(current, session));
        setBackend((current) => ({
          ...current,
          detail: `Session ${session.id} created`,
          checkedAt: eventTime
        }));
        return;
      }

      if (event.type === "session.running") {
        if (eventSessionId) {
          patchSessionInList(eventSessionId, { status: "running", updated_at: eventTime });
        }
        setStreamState({
          status: "live",
          detail: "Session running.",
          error: null,
          lastEventAt: eventTime
        });
        return;
      }

      if (event.type === "session.completed") {
        const restoreMs = extractRestoreMs(event.payload);
        if (eventSessionId) {
          patchSessionInList(eventSessionId, {
            status: "completed",
            updated_at: eventTime,
            completed_at: eventTime,
            ...(restoreMs === null ? {} : { restore_ms: restoreMs })
          });
        }
        setRunState({ submitting: false, error: null });
        setStreamState({
          status: "idle",
          detail: "Session completed.",
          error: null,
          lastEventAt: eventTime
        });
        setBackend((current) => ({
          ...current,
          detail: "Session completed",
          checkedAt: eventTime
        }));
        return;
      }

      if (event.type === "session.failed") {
        if (eventSessionId) {
          patchSessionInList(eventSessionId, {
            status: "failed",
            updated_at: eventTime,
            completed_at: eventTime
          });
        }
        const error = extractErrorMessage(event.payload) ?? "Session failed.";
        setRunState({ submitting: false, error });
        setStreamState({
          status: "failed",
          detail: error,
          error,
          lastEventAt: eventTime
        });
        setBackend((current) => ({
          ...current,
          detail: "Session failed",
          checkedAt: eventTime
        }));
        return;
      }

      if (event.type === "session.destroyed") {
        if (eventSessionId) {
          patchSessionInList(eventSessionId, { status: "destroyed", updated_at: eventTime });
        }
        setStreamState({
          status: "idle",
          detail: "Session destroyed.",
          error: null,
          lastEventAt: eventTime
        });
        return;
      }

      if (event.type === "session.error") {
        const error = extractErrorMessage(event.payload) ?? "Session error.";
        setRunState({ submitting: false, error });
        setStreamState({
          status: "failed",
          detail: error,
          error,
          lastEventAt: eventTime
        });
        return;
      }

      if (event.type === "artifact.updated") {
        const artifact = extractArtifactPayload(event.payload);
        if (!artifact) {
          return;
        }

        upsertArtifactForSession(artifact);
        setStreamState((current) => ({
          ...current,
          detail: "Artifact updated.",
          lastEventAt: eventTime
        }));
      }
    },
    [appendSessionOutput, patchSessionInList, upsertArtifactForSession]
  );

  const loadRealDashboard = useCallback(async () => {
    closeEventStream();
    setBooting(true);
    setSessionError(null);
    setCreateSessionError(null);
    setRunState({ submitting: false, error: null });
    setArtifactState({ loading: false, error: null });
    setBackend({
      mode: "loading",
      title: "Checking real backend",
      detail: "Reading /api/healthz",
      checkedAt: null
    });

    const healthResult = await requestJson<{ status: string }>("/api/healthz");
    if (!healthResult.ok) {
      if (isBackendUnavailable(healthResult)) {
        enterMockFallback(healthResult.error);
        return;
      }

      setBackend({
        mode: "real",
        title: "Real backend",
        detail: healthResult.error,
        checkedAt: new Date().toISOString()
      });
      setSessionError(healthResult.error);
      setSessions([]);
      setSelectedSessionId(null);
      setArtifactsBySession({});
      setOutputBySession({});
      setStreamState({
        status: "failed",
        detail: healthResult.error,
        error: healthResult.error,
        lastEventAt: new Date().toISOString()
      });
      setBooting(false);
      return;
    }

    const sessionsResult = await requestJson<{ sessions: ApiSession[] }>("/api/sessions");
    if (!sessionsResult.ok) {
      if (isBackendUnavailable(sessionsResult)) {
        enterMockFallback(sessionsResult.error);
        return;
      }

      setBackend({
        mode: "real",
        title: "Real backend",
        detail: sessionsResult.error,
        checkedAt: new Date().toISOString()
      });
      setSessionError(sessionsResult.error);
      setSessions([]);
      setSelectedSessionId(null);
      setArtifactsBySession({});
      setOutputBySession({});
      setStreamState({
        status: "failed",
        detail: sessionsResult.error,
        error: sessionsResult.error,
        lastEventAt: new Date().toISOString()
      });
      setBooting(false);
      return;
    }

    const nextSessions = sessionsResult.data.sessions ?? [];
    const nextSelectedId = choosePrimarySessionId(nextSessions);

    setBackend({
      mode: "real",
      title: "Real backend",
      detail: `${nextSessions.length} session${nextSessions.length === 1 ? "" : "s"} loaded`,
      checkedAt: new Date().toISOString()
    });
    setSessions(nextSessions);
    setSelectedSessionId(nextSelectedId);
    setArtifactsBySession({});
    setOutputBySession({});
    setStreamState({
      status: nextSelectedId ? "connecting" : "idle",
      detail: nextSelectedId ? "Preparing event stream." : "Waiting for a selected session.",
      error: null,
      lastEventAt: null
    });
    setBooting(false);
  }, [closeEventStream, enterMockFallback]);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      void loadRealDashboard();
    }, 0);

    return () => window.clearTimeout(timer);
  }, [loadRealDashboard, retryTick]);

  useEffect(() => {
    if (backend.mode !== "real" || !selectedSessionId) {
      closeEventStream();
      return;
    }

    const sessionId = selectedSessionId;
    const token = wsTokenRef.current + 1;
    wsTokenRef.current = token;

    const socket = new WebSocket(buildEventsWebSocketUrl(sessionId));
    wsRef.current = socket;

    let opened = false;

    socket.onopen = () => {
      if (wsTokenRef.current !== token) {
        return;
      }

      opened = true;
      setStreamState({
        status: "live",
        detail: `Streaming ${sessionId}.`,
        error: null,
        lastEventAt: new Date().toISOString()
      });
    };

    socket.onmessage = (message) => {
      if (wsTokenRef.current !== token || typeof message.data !== "string") {
        return;
      }

      const event = parseHarnessEvent(message.data);
      if (!event) {
        return;
      }

      handleHarnessEvent(event, sessionId);
    };

    socket.onerror = () => {
      if (wsTokenRef.current !== token) {
        return;
      }

      const activeSession = sessionsRef.current.find((session) => session.id === sessionId) ?? null;
      if (activeSession && ["completed", "failed", "destroyed"].includes(activeSession.status)) {
        setStreamState({
          status: "idle",
          detail: `Session ${activeSession.status}.`,
          error: null,
          lastEventAt: new Date().toISOString()
        });
        return;
      }

      enterMockFallback(`WebSocket connection failed for ${sessionId}.`);
    };

    socket.onclose = () => {
      if (wsTokenRef.current !== token) {
        return;
      }

      const activeSession = sessionsRef.current.find((session) => session.id === sessionId) ?? null;
      if (activeSession && ["completed", "failed", "destroyed"].includes(activeSession.status)) {
        setStreamState({
          status: "idle",
          detail: `Session ${activeSession.status}.`,
          error: null,
          lastEventAt: new Date().toISOString()
        });
        return;
      }

      if (!opened) {
        enterMockFallback(`WebSocket connection closed for ${sessionId}.`);
        return;
      }

      enterMockFallback(`WebSocket stream closed for ${sessionId}.`);
    };

    return () => {
      if (wsRef.current === socket) {
        wsRef.current = null;
      }
      wsTokenRef.current += 1;
      try {
        socket.close();
      } catch {
        // Ignore cleanup errors.
      }
    };
  }, [backend.mode, closeEventStream, enterMockFallback, handleHarnessEvent, selectedSessionId]);

  useEffect(() => {
    if (backend.mode !== "real" || !selectedSessionId) {
      return;
    }

    let active = true;

    void (async () => {
      setArtifactState({ loading: true, error: null });

      const artifactResult = await requestJson<{ artifacts: ApiArtifact[] }>(
        `/api/sessions/${encodeURIComponent(selectedSessionId)}/artifacts`
      );

      if (!active) {
        return;
      }

      if (artifactResult.ok) {
        setArtifactsBySession((current) => ({
          ...current,
          [selectedSessionId]: artifactResult.data.artifacts ?? []
        }));
        setArtifactState({ loading: false, error: null });
        return;
      }

      if (artifactResult.status === 404) {
        setArtifactsBySession((current) => ({
          ...current,
          [selectedSessionId]: []
        }));
        setArtifactState({ loading: false, error: null });
        return;
      }

      if (isBackendUnavailable(artifactResult)) {
        enterMockFallback(artifactResult.error);
        return;
      }

      setArtifactsBySession((current) => ({
        ...current,
        [selectedSessionId]: []
      }));
      setArtifactState({ loading: false, error: artifactResult.error });
    })();

    return () => {
      active = false;
    };
  }, [backend.mode, enterMockFallback, selectedSessionId]);

  const handleCreateSession = async () => {
    setCreateSessionError(null);
    setSessionError(null);

    if (backend.mode === "loading") {
      return;
    }

    if (backend.mode === "mock") {
      const session = createMockSession(selectedAgent);
      setSessions((current) => [session, ...current]);
      setSelectedSessionId(session.id);
      setArtifactsBySession((current) => ({
        ...current,
        [session.id]: []
      }));
      setOutputBySession((current) => ({
        ...current,
        [session.id]: []
      }));
      setRunState({ submitting: false, error: null });
      setStreamState({
        status: "mock",
        detail: "Mock session created.",
        error: null,
        lastEventAt: new Date().toISOString()
      });
      return;
    }

    setCreatingSession(true);

    const result = await requestJson<ApiSession>("/api/sessions", {
      method: "POST",
      headers: {
        "content-type": "application/json"
      },
      body: JSON.stringify({ agent: selectedAgent })
    });

    setCreatingSession(false);

    if (!result.ok) {
      if (isBackendUnavailable(result)) {
        enterMockFallback(result.error);
        return;
      }

      setCreateSessionError(result.error);
      return;
    }

    setSessions((current) => upsertSessionItem(current, result.data));
    setSelectedSessionId(result.data.id);
    setArtifactsBySession((current) => ({
      ...current,
      [result.data.id]: []
    }));
    setOutputBySession((current) => ({
      ...current,
      [result.data.id]: []
    }));
    setRunState({ submitting: false, error: null });
    setBackend((current) => ({
      ...current,
      detail: "Session created",
      checkedAt: new Date().toISOString()
    }));
  };

  const handleRunTask = async () => {
    const prompt = taskPrompt.trim();

    if (!selectedSession || !canSendFirstTask(selectedSession) || !prompt || runState.submitting) {
      return;
    }

    if (backend.mode === "mock") {
      const updatedAt = new Date().toISOString();
      const mockOutput = createMockTaskOutput(prompt);

      setSessions((current) =>
        current.map((session) =>
          session.id === selectedSession.id
            ? {
                ...session,
                status: "completed",
                updated_at: updatedAt,
                completed_at: updatedAt
              }
            : session
        )
      );
      replaceSessionOutput(selectedSession.id, mockOutput);
      setRunState({ submitting: false, error: null });
      setStreamState({
        status: "mock",
        detail: "Mock stream finished.",
        error: null,
        lastEventAt: updatedAt
      });
      return;
    }

    if (backend.mode !== "real") {
      setRunState({ submitting: false, error: "Backend is still checking." });
      return;
    }

    setRunState({ submitting: true, error: null });
    appendSessionOutput(
      selectedSession.id,
      createOutputEntry({
        stream: "system",
        line: "Task accepted. Waiting for stream events.",
        source: "real"
      })
    );

    const result = await requestJson<SendMessageResponse>(
      `/api/sessions/${encodeURIComponent(selectedSession.id)}/messages`,
      {
        method: "POST",
        headers: {
          "content-type": "application/json"
        },
        body: JSON.stringify({ content: prompt })
      }
    );

    if (!result.ok) {
      setRunState({ submitting: false, error: result.error });
      if (isBackendUnavailable(result)) {
        enterMockFallback(result.error);
      }
      return;
    }

    const updatedAt = new Date().toISOString();
    setSessions((current) =>
      current.map((session) =>
        session.id === selectedSession.id
          ? { ...session, status: result.data.status || "running", updated_at: updatedAt }
          : session
      )
    );
    setRunState({ submitting: false, error: null });
    setBackend((current) => ({
      ...current,
      detail: "Task accepted by real backend",
      checkedAt: updatedAt
    }));
    setStreamState((current) => ({
      ...current,
      detail: "Waiting for agent.output events.",
      lastEventAt: updatedAt
    }));
  };

  const modeLabel =
    backend.mode === "loading"
      ? "Checking"
      : backend.mode === "real"
        ? "Real backend"
        : "Mock fallback";
  const effectiveStreamState =
    backend.mode === "real" && !selectedSessionId
      ? {
          status: "idle" as const,
          detail: "Waiting for a selected session.",
          error: null,
          lastEventAt: null
        }
      : streamState;

  const artifactsLoading =
    backend.mode === "real" && Boolean(selectedSessionId) && artifactState.loading;
  const artifactError = backend.mode === "real" ? artifactState.error : null;
  const selectedSessionSubtitle = selectedSession
    ? `${displayAgentName(selectedSession.agent)} - ${statusLabel(selectedSession.status)} - updated ${formatTime(selectedSession.updated_at)}`
    : "No session selected";
  const runDisabledReason = !selectedSession
    ? "Create or select a session first."
    : !canSendFirstTask(selectedSession)
      ? "This MVP accepts the first message only."
      : !taskPrompt.trim()
        ? "Enter a task prompt."
        : null;
  const canCreateSession = backend.mode !== "loading" && !creatingSession;
  const canRunTask =
    backend.mode !== "loading" &&
    !runState.submitting &&
    Boolean(selectedSession) &&
    canSendFirstTask(selectedSession) &&
    Boolean(taskPrompt.trim());

  return (
    <main className="workbench-shell">
      <section className="sidebar" aria-label="Sessions">
        <div className="brand-row">
          <div>
            <p className="eyebrow">Harness</p>
            <h1>Workbench</h1>
          </div>
          <span className={`mode-pill ${getSessionPillTone(backend)}`}>{modeLabel}</span>
        </div>

        <div className="status-strip" role="status" aria-live="polite">
          <span className={`status-dot ${getStatusDotTone(backend)}`} aria-hidden="true" />
          <span className="status-copy">
            <strong>{backend.title}</strong>
            <small>{backend.detail}</small>
          </span>
          <button
            className="quiet-button"
            type="button"
            onClick={() => setRetryTick((value) => value + 1)}
            disabled={backend.mode === "loading"}
          >
            Retry real
          </button>
        </div>

        {sessionError ? (
          <div className="notice-strip notice-error" role="alert">
            <strong>Session load error</strong>
            <p>{sessionError}</p>
          </div>
        ) : null}

        <div className="control-group">
          <label htmlFor="agent">Agent</label>
          <select
            id="agent"
            value={selectedAgent}
            onChange={(event) => setSelectedAgent(event.target.value as AgentOption)}
            disabled={backend.mode === "loading" || creatingSession}
          >
            {AGENT_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </div>

        <button
          className="primary-action"
          type="button"
          onClick={handleCreateSession}
          disabled={!canCreateSession}
        >
          {creatingSession ? "Creating..." : "Create session"}
        </button>

        {createSessionError ? (
          <div className="notice-strip notice-error" role="alert">
            <strong>Create failed</strong>
            <p>{createSessionError}</p>
          </div>
        ) : null}

        <div className="section-heading">
          <h2>Sessions</h2>
          <span>{booting ? "Loading" : `${sessions.length}`}</span>
        </div>

        <div className="session-list" aria-busy={booting}>
          {booting ? (
            <div className="session-item" aria-hidden="true">
              <span className="session-main">
                <strong>Loading sessions</strong>
                <span>Waiting for backend state.</span>
              </span>
              <span className="session-meta">
                <span className="status-tag status-running">Loading</span>
              </span>
            </div>
          ) : sessions.length === 0 ? (
            <div className="notice-strip" role="status">
              <strong>No sessions</strong>
              <p>The backend returned an empty list.</p>
            </div>
          ) : (
            sessions.map((session) => {
              const active = session.id === selectedSessionId;
              const tone = statusTone(session.status);

              return (
                <button
                  key={session.id}
                  className={`session-item ${active ? "is-active" : ""}`}
                  type="button"
      onClick={() => {
        setSelectedSessionId(session.id);
        if (backend.mode === "real") {
          setStreamState({
            status: "connecting",
            detail: `Connecting to ${session.id}.`,
            error: null,
            lastEventAt: null
          });
        }
      }}
                  aria-pressed={active}
                >
                  <span className="session-main">
                    <strong>{session.id}</strong>
                    <span>
                      {displayAgentName(session.agent)} - updated {formatTime(session.updated_at)}
                    </span>
                  </span>
                  <span className="session-meta">
                    <span className={`status-tag status-${tone}`}>{statusLabel(session.status)}</span>
                    <span>{session.restore_ms ? `${session.restore_ms} ms` : formatTime(session.created_at)}</span>
                  </span>
                </button>
              );
            })
          )}
        </div>
      </section>

      <section className="task-panel" aria-label="Task run">
        <header className="panel-header">
          <div>
            <p className="eyebrow">Task</p>
            <h2>One-shot run</h2>
          </div>
          <span
            className={`status-tag ${
              selectedSession ? `status-${statusTone(selectedSession.status)}` : "status-ready"
            }`}
          >
            {selectedSession ? statusLabel(selectedSession.status) : "Idle"}
          </span>
        </header>

        <div className="detail-grid" aria-label="Selected session details">
          <div className="detail-item">
            <span className="detail-label">Session</span>
            <strong className="detail-value">{selectedSession?.id ?? "None"}</strong>
          </div>
          <div className="detail-item">
            <span className="detail-label">Agent</span>
            <strong className="detail-value">
              {selectedSession ? displayAgentName(selectedSession.agent) : "-"}
            </strong>
          </div>
          <div className="detail-item">
            <span className="detail-label">Updated</span>
            <strong className="detail-value">
              {selectedSession ? formatFullTime(selectedSession.updated_at) : "-"}
            </strong>
          </div>
          <div className="detail-item">
            <span className="detail-label">Workspace</span>
            <strong className="detail-value">{selectedSession?.workspace ?? "-"}</strong>
          </div>
        </div>

        <form
          className="task-form"
          onSubmit={(event) => {
            event.preventDefault();
            void handleRunTask();
          }}
        >
          <label htmlFor="task">Task prompt</label>
          <textarea
            id="task"
            rows={6}
            placeholder="One-shot task"
            value={taskPrompt}
            onChange={(event) => setTaskPrompt(event.target.value)}
            disabled={backend.mode === "loading" || runState.submitting}
          />
          <div className="form-actions">
            <button className="primary-action" type="submit" disabled={!canRunTask}>
              {runState.submitting ? "Submitting..." : "Run task"}
            </button>
            <button className="secondary-action" type="button" disabled>
              Stop
            </button>
          </div>
        </form>

        {runState.error ? (
          <div className="notice-strip notice-error" role="alert">
            <strong>Run failed</strong>
            <p>{runState.error}</p>
          </div>
        ) : runDisabledReason ? (
          <div className="notice-strip" role="status">
            <strong>Not ready</strong>
            <p>{runDisabledReason}</p>
          </div>
        ) : null}

        <div className="run-state">
          <span className="status-dot muted" aria-hidden="true" />
          <span>{selectedSessionSubtitle}</span>
        </div>

        <div className="stream-strip" aria-live="polite">
          <span className={`status-tag ${getStreamTone(effectiveStreamState)}`}>
            {getStreamLabel(effectiveStreamState)}
          </span>
          <span className="stream-copy">
            <strong>{effectiveStreamState.detail}</strong>
            <small>
              {effectiveStreamState.lastEventAt
                ? `Last event ${formatFullTime(effectiveStreamState.lastEventAt)}`
                : "Waiting for events."}
            </small>
          </span>
        </div>

        {effectiveStreamState.error ? (
          <div className="notice-strip notice-error" role="alert">
            <strong>Stream error</strong>
            <p>{effectiveStreamState.error}</p>
          </div>
        ) : null}

        <div className="output-panel">
          <div className="section-heading">
            <div>
              <h2>agent.output</h2>
              <span>Thinking, tool calls, and answer lines</span>
            </div>
            <span className={`status-tag ${getStreamTone(effectiveStreamState)}`}>
              {getStreamLabel(effectiveStreamState)}
            </span>
          </div>

          {displayedOutput.length > 0 ? (
            <div className="output-list" aria-label="Agent output">
              {displayedOutput.map((entry) => (
                <article className={`output-entry output-${entry.kind}`} key={entry.id}>
                  <div className="output-entry-head">
                    <span className="output-chip">{entry.label}</span>
                    <span className="output-meta">
                      {entry.stream} · {entry.source} · {formatTime(entry.time)}
                    </span>
                  </div>
                  <p>{entry.line}</p>
                </article>
              ))}
            </div>
          ) : (
            <div className="empty-state output-empty" role="status">
              <strong>No output</strong>
              <span>{selectedSession ? "Waiting for live events." : "Select a session to see output."}</span>
            </div>
          )}
        </div>
      </section>

      <section className="artifact-panel" aria-label="Artifacts">
        <header className="panel-header">
          <div>
            <p className="eyebrow">Workspace</p>
            <h2>Artifacts</h2>
          </div>
          <span className="status-tag status-ready">{displayedArtifacts.length} files</span>
        </header>

        {artifactError ? (
          <div className="notice-strip notice-error" role="alert">
            <strong>Artifact load error</strong>
            <p>{artifactError}</p>
          </div>
        ) : null}

        <div className="artifact-list" aria-busy={artifactsLoading}>
          {artifactsLoading ? (
            <div className="artifact-row" aria-hidden="true">
              <span>
                <strong>Loading artifacts</strong>
                <small>Fetching session files.</small>
              </span>
              <span>-</span>
            </div>
          ) : displayedArtifacts.length > 0 ? (
            displayedArtifacts.map((artifact) => (
              <a
                className="artifact-row"
                href={buildArtifactHref(artifact.session_id, artifact.path)}
                key={artifact.path}
              >
                <span>
                  <strong>{artifact.path}</strong>
                  <small>
                    Updated {formatTime(artifact.updated_at)} · {formatBytes(artifact.size)}
                  </small>
                </span>
                <span>Download</span>
              </a>
            ))
          ) : (
            <div className="notice-strip" role="status">
              <strong>No artifacts</strong>
              <p>{selectedSession ? "Nothing has been written yet." : "Select a session."}</p>
            </div>
          )}
        </div>
      </section>
    </main>
  );
}
