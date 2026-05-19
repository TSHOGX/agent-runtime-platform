"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  buildArtifactHref,
  canSendFirstTask,
  choosePrimarySessionId,
  displayAgentName,
  formatBytes,
  formatFullTime,
  formatTime,
  isBackendUnavailable,
  requestJson,
  type SendMessageResponse,
  statusLabel,
  statusTone,
  type ApiArtifact,
  type ApiSession,
  type BackendMode
} from "@/lib/dashboard";
import {
  createMockSession,
  createMockTaskOutput,
  mockArtifactsBySession,
  mockOutputLines,
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
  outputLines: string[];
};

type AgentOption = "claude" | "opencode" | "sh";

const AGENT_OPTIONS: { value: AgentOption; label: string }[] = [
  { value: "claude", label: "Claude Code" },
  { value: "opencode", label: "OpenCode" },
  { value: "sh", label: "Shell smoke" }
];

const DEFAULT_TASK_PROMPT =
  "Summarize the available data, write a concise report, and save artifacts under /workspace.";

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

export default function Workbench() {
  const [backend, setBackend] = useState<BackendState>({
    mode: "loading",
    title: "Checking real backend",
    detail: "Reading /api/healthz",
    checkedAt: null
  });
  const [sessions, setSessions] = useState<ApiSession[]>([]);
  const [selectedSessionId, setSelectedSessionId] = useState<string | null>(null);
  const [realArtifacts, setRealArtifacts] = useState<ApiArtifact[]>([]);
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
    error: null,
    outputLines: []
  });
  const [sessionError, setSessionError] = useState<string | null>(null);
  const [booting, setBooting] = useState(true);
  const [retryTick, setRetryTick] = useState(0);

  const selectedSession = useMemo(
    () => sessions.find((session) => session.id === selectedSessionId) ?? null,
    [sessions, selectedSessionId]
  );
  const displayedArtifacts = useMemo(() => {
    if (!selectedSessionId) {
      return [];
    }

    if (backend.mode === "mock") {
      return mockArtifactsBySession[selectedSessionId] ?? [];
    }

    if (backend.mode === "real") {
      return realArtifacts;
    }

    return [];
  }, [backend.mode, realArtifacts, selectedSessionId]);

  const enterMockFallback = useCallback((detail: string) => {
    setBackend({
      mode: "mock",
      title: "Mock fallback",
      detail,
      checkedAt: new Date().toISOString()
    });
    setSessions(mockSessions);
    setSelectedSessionId(choosePrimarySessionId(mockSessions));
    setRealArtifacts([]);
    setArtifactState({ loading: false, error: null });
    setCreateSessionError(null);
    setRunState({
      submitting: false,
      error: null,
      outputLines: mockOutputLines
    });
    setBooting(false);
  }, []);

  const loadRealDashboard = useCallback(async () => {
    setBooting(true);
    setSessionError(null);
    setCreateSessionError(null);
    setRunState({ submitting: false, error: null, outputLines: [] });
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
      setRealArtifacts([]);
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
      setRealArtifacts([]);
      setBooting(false);
      return;
    }

    const nextSessions = sessionsResult.data.sessions ?? [];

    setBackend({
      mode: "real",
      title: "Real backend",
      detail: `${nextSessions.length} session${nextSessions.length === 1 ? "" : "s"} loaded`,
      checkedAt: new Date().toISOString()
    });
    setSessions(nextSessions);
    setSelectedSessionId(choosePrimarySessionId(nextSessions));
    setRealArtifacts([]);
    setBooting(false);
  }, [enterMockFallback]);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      void loadRealDashboard();
    }, 0);

    return () => window.clearTimeout(timer);
  }, [loadRealDashboard, retryTick]);

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
        setRealArtifacts(artifactResult.data.artifacts ?? []);
        setArtifactState({ loading: false, error: null });
        return;
      }

      if (artifactResult.status === 404) {
        setRealArtifacts([]);
        setArtifactState({ loading: false, error: null });
        return;
      }

      if (isBackendUnavailable(artifactResult)) {
        enterMockFallback(artifactResult.error);
        return;
      }

      setRealArtifacts([]);
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
      setRunState({ submitting: false, error: null, outputLines: [] });
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

    setSessions((current) => [result.data, ...current.filter((session) => session.id !== result.data.id)]);
    setSelectedSessionId(result.data.id);
    setRealArtifacts([]);
    setRunState({ submitting: false, error: null, outputLines: [] });
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

    setRunState({
      submitting: true,
      error: null,
      outputLines: ["agent.output submitting one-shot task."]
    });

    if (backend.mode === "mock") {
      const updatedAt = new Date().toISOString();
      setSessions((current) =>
        current.map((session) =>
          session.id === selectedSession.id
            ? { ...session, status: "running", updated_at: updatedAt }
            : session
        )
      );
      setRunState({
        submitting: false,
        error: null,
        outputLines: createMockTaskOutput(prompt)
      });
      return;
    }

    if (backend.mode !== "real") {
      setRunState({ submitting: false, error: "Backend is still checking.", outputLines: [] });
      return;
    }

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
      if (isBackendUnavailable(result)) {
        enterMockFallback(result.error);
        return;
      }

      setRunState({
        submitting: false,
        error: result.error,
        outputLines: []
      });
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
    setRunState({
      submitting: false,
      error: null,
      outputLines: ["agent.output task accepted; waiting for event stream."]
    });
    setBackend((current) => ({
      ...current,
      detail: "Task accepted by real backend",
      checkedAt: new Date().toISOString()
    }));
  };

  const modeLabel =
    backend.mode === "loading"
      ? "Checking"
      : backend.mode === "real"
        ? "Real backend"
        : "Mock fallback";
  const outputLines = runState.outputLines;
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
                  onClick={() => setSelectedSessionId(session.id)}
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

        <div className="output-panel">
          <div className="section-heading">
            <h2>agent.output</h2>
            <span>{backend.mode === "mock" ? "mock" : "stream"}</span>
          </div>

          {outputLines.length > 0 ? (
            <pre aria-label="Agent output">{outputLines.join("\n")}</pre>
          ) : (
            <div className="empty-state output-empty" role="status">
              <strong>No output</strong>
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
                  <small>Updated {formatTime(artifact.updated_at)}</small>
                </span>
                <span>{formatBytes(artifact.size)}</span>
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
