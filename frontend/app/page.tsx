type SessionStatus = "ready" | "running" | "completed";

type SessionPreview = {
  id: string;
  title: string;
  status: SessionStatus;
  agent: string;
  time: string;
};

type ArtifactPreview = {
  name: string;
  size: string;
  updated: string;
};

const sessions: SessionPreview[] = [
  {
    id: "demo-103",
    title: "Daily revenue check",
    status: "completed",
    agent: "claude",
    time: "14:32"
  },
  {
    id: "demo-102",
    title: "Battery anomaly scan",
    status: "running",
    agent: "sh",
    time: "13:58"
  },
  {
    id: "demo-101",
    title: "Fleet summary draft",
    status: "ready",
    agent: "opencode",
    time: "11:20"
  }
];

const outputLines = [
  "agent.output Waiting for the first task.",
  "agent.output Session will run once when submitted.",
  "agent.output Artifacts appear in the workspace panel."
];

const artifacts: ArtifactPreview[] = [
  { name: "summary.csv", size: "18 KB", updated: "14:34" },
  { name: "trend.png", size: "126 KB", updated: "14:35" },
  { name: "report.md", size: "7 KB", updated: "14:36" }
];

const statusLabel: Record<SessionStatus, string> = {
  ready: "Ready",
  running: "Running",
  completed: "Completed"
};

export default function Home() {
  return (
    <main className="workbench-shell">
      <section className="sidebar" aria-label="Sessions">
        <div className="brand-row">
          <div>
            <p className="eyebrow">Harness</p>
            <h1>Workbench</h1>
          </div>
          <span className="mode-pill mode-real">Real backend</span>
        </div>

        <div className="control-group">
          <label htmlFor="agent">Agent</label>
          <select id="agent" defaultValue="claude">
            <option value="claude">Claude Code</option>
            <option value="opencode">OpenCode</option>
            <option value="sh">Shell smoke</option>
          </select>
        </div>

        <button className="primary-action" type="button">
          Create session
        </button>

        <div className="status-strip" role="status">
          <span className="status-dot" aria-hidden="true" />
          <span>Idle</span>
          <button type="button" className="quiet-button">
            Retry backend
          </button>
        </div>

        <div className="section-heading">
          <h2>Sessions</h2>
          <span>{sessions.length}</span>
        </div>

        <div className="session-list">
          {sessions.map((session) => (
            <button
              key={session.id}
              className={`session-item ${session.status === "running" ? "is-active" : ""}`}
              type="button"
            >
              <span className="session-main">
                <strong>{session.title}</strong>
                <span>{session.id}</span>
              </span>
              <span className="session-meta">
                <span className={`status-tag status-${session.status}`}>
                  {statusLabel[session.status]}
                </span>
                <span>{session.agent}</span>
                <span>{session.time}</span>
              </span>
            </button>
          ))}
        </div>
      </section>

      <section className="task-panel" aria-label="Task run">
        <header className="panel-header">
          <div>
            <p className="eyebrow">Task</p>
            <h2>One-shot run</h2>
          </div>
          <span className="status-tag status-running">Waiting</span>
        </header>

        <form className="task-form">
          <label htmlFor="task">Task prompt</label>
          <textarea
            id="task"
            rows={5}
            placeholder="Ask the agent to inspect Doris data and write artifacts."
          />
          <div className="form-actions">
            <button className="primary-action" type="button">
              Run task
            </button>
            <button className="secondary-action" type="button" disabled>
              Stop
            </button>
          </div>
        </form>

        <div className="run-state">
          <span className="status-dot muted" aria-hidden="true" />
          <span>No task running</span>
        </div>

        <div className="output-panel">
          <div className="section-heading">
            <h2>agent.output</h2>
            <span>stream</span>
          </div>
          <pre aria-label="Agent output">
            {outputLines.map((line) => `${line}\n`).join("")}
          </pre>
        </div>
      </section>

      <section className="artifact-panel" aria-label="Artifacts">
        <header className="panel-header">
          <div>
            <p className="eyebrow">Workspace</p>
            <h2>Artifacts</h2>
          </div>
          <span className="status-tag status-ready">{artifacts.length} files</span>
        </header>

        <div className="artifact-list">
          {artifacts.map((artifact) => (
            <a className="artifact-row" href="#" key={artifact.name}>
              <span>
                <strong>{artifact.name}</strong>
                <small>{artifact.updated}</small>
              </span>
              <span>{artifact.size}</span>
            </a>
          ))}
        </div>

        <div className="empty-state">
          <strong>Preview pending</strong>
          <span>Downloads use the artifact links.</span>
        </div>
      </section>
    </main>
  );
}
