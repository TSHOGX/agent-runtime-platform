"use client";

import { useState } from "react";
import { Download, FileText } from "lucide-react";

import { useArtifacts, useSelectedSession } from "@/components/harness-provider";
import { Tabs } from "@/components/ui/tabs";
import { ArtifactViewer } from "./artifact-viewer";
import { buildArtifactHref } from "@/lib/api";
import { formatBytes, formatRelative } from "@/lib/format";
import { cn } from "@/lib/cn";
import type { ApiArtifact } from "@/lib/types";

const MAX_TABS = 6;

export function ArtifactPane() {
  const session = useSelectedSession();
  const artifacts = useArtifacts(session?.id ?? null);

  if (!session) {
    return (
      <aside className="flex h-full min-w-0 flex-col border-l border-[var(--color-border)] bg-[var(--color-surface)] px-4 py-3 text-xs text-[var(--color-foreground-muted)]">
        <span>Files will appear here once a session is running.</span>
      </aside>
    );
  }
  return <ArtifactPaneInner key={session.id} sessionId={session.id} artifacts={artifacts} />;
}

function ArtifactPaneInner({
  sessionId,
  artifacts
}: {
  sessionId: string;
  artifacts: ApiArtifact[];
}) {
  const [openTabs, setOpenTabs] = useState<string[]>([]);
  const [activeTab, setActiveTab] = useState<string | null>(null);

  const openArtifact = (path: string) => {
    setOpenTabs((prev) => {
      if (prev.includes(path)) return prev;
      const next = [...prev, path];
      return next.length > MAX_TABS ? next.slice(next.length - MAX_TABS) : next;
    });
    setActiveTab(path);
  };

  const closeTab = (path: string) => {
    setOpenTabs((prev) => prev.filter((p) => p !== path));
    setActiveTab((prev) => {
      if (prev !== path) return prev;
      const idx = openTabs.indexOf(path);
      const remaining = openTabs.filter((p) => p !== path);
      return remaining[Math.min(idx, remaining.length - 1)] ?? null;
    });
  };

  return (
    <aside className="flex h-full min-h-0 flex-col border-l border-[var(--color-border)] bg-[var(--color-surface)]">
      <div className="flex items-center justify-between px-4 py-3 border-b border-[var(--color-border)]">
        <span className="text-xs uppercase tracking-wider text-[var(--color-foreground-muted)]">
          Files {artifacts.length > 0 ? `(${artifacts.length})` : ""}
        </span>
      </div>

      <div className="max-h-[40%] min-h-[120px] overflow-y-auto border-b border-[var(--color-border)]">
        {artifacts.length === 0 ? (
          <p className="px-4 py-4 text-xs text-[var(--color-foreground-muted)]">
            No artifacts yet. The agent writes files to <code className="font-mono">/workspace</code>.
          </p>
        ) : (
          <ul className="px-1 py-1">
            {artifacts.map((a) => {
              const active = activeTab === a.path;
              return (
                <li key={a.path}>
                  <button
                    className={cn(
                      "flex w-full items-center gap-2 rounded-[var(--radius)] px-2.5 py-1.5 text-left text-xs hover:bg-[var(--color-surface-muted)]",
                      active && "bg-[var(--color-surface-muted)]"
                    )}
                    onClick={() => openArtifact(a.path)}
                  >
                    <FileText className="h-3.5 w-3.5 shrink-0 text-[var(--color-foreground-muted)]" />
                    <span className="flex-1 truncate font-mono">{a.path}</span>
                    <span className="shrink-0 text-[10px] text-[var(--color-foreground-muted)]">
                      {formatBytes(a.size)} · {formatRelative(a.updated_at)}
                    </span>
                    <a
                      href={buildArtifactHref(a.session_id, a.path)}
                      download
                      onClick={(e) => e.stopPropagation()}
                      className="shrink-0 text-[var(--color-foreground-muted)] hover:text-[var(--color-foreground)]"
                      title="Download"
                    >
                      <Download className="h-3.5 w-3.5" />
                    </a>
                  </button>
                </li>
              );
            })}
          </ul>
        )}
      </div>

      <div className="flex flex-1 min-h-0 flex-col">
        {openTabs.length > 0 ? (
          <Tabs
            value={activeTab}
            onValueChange={setActiveTab}
            items={openTabs.map((path) => ({
              value: path,
              label: path.split("/").pop() ?? path,
              onClose: () => closeTab(path)
            }))}
          />
        ) : null}
        <div className="flex-1 min-h-0 overflow-hidden">
          {activeTab ? (
            <ArtifactViewer key={activeTab} sessionId={sessionId} path={activeTab} />
          ) : (
            <div className="flex h-full items-center justify-center text-center text-xs text-[var(--color-foreground-muted)]">
              <span>Select a file from the list above to preview it here.</span>
            </div>
          )}
        </div>
      </div>
    </aside>
  );
}
