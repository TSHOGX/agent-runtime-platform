"use client";

import { useState } from "react";
import { Loader2, Plus, RotateCw } from "lucide-react";

import { useHarness } from "@/components/harness-provider";
import { Button } from "@/components/ui/button";
import { StatusDot, statusTone } from "@/components/ui/badge";
import { agentLabel, formatRelative, statusLabel } from "@/lib/format";
import { cn } from "@/lib/cn";
import type { AgentKind } from "@/lib/types";

const AGENT_OPTIONS: { value: AgentKind; label: string }[] = [
  { value: "claude", label: "Claude Code" },
  { value: "opencode", label: "OpenCode" },
  { value: "sh", label: "Shell" }
];

export function Sidebar() {
  const { state, selectSession, createSession, refresh } = useHarness();
  const [creating, setCreating] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);
  const [pickerOpen, setPickerOpen] = useState(false);

  const handleCreate = async (agent: AgentKind) => {
    setPickerOpen(false);
    setCreating(true);
    setCreateError(null);
    const res = await createSession(agent);
    setCreating(false);
    if (!res.ok) setCreateError(res.error ?? "Failed to create session");
  };

  return (
    <aside className="flex h-full w-[280px] flex-col border-r border-[var(--color-border)] bg-[var(--color-surface)]">
      <div className="flex items-center justify-between px-4 py-3">
        <div className="flex flex-col">
          <span className="text-sm font-semibold tracking-tight">Harness</span>
          <span className="text-[11px] text-[var(--color-foreground-muted)]">Workbench</span>
        </div>
        <Button
          variant="ghost"
          size="icon"
          onClick={() => void refresh()}
          aria-label="Refresh sessions"
          title="Refresh"
        >
          <RotateCw className="h-4 w-4" />
        </Button>
      </div>

      <div className="px-3 pb-2 relative">
        <Button
          variant="primary"
          size="lg"
          className="w-full justify-start gap-2"
          onClick={() => setPickerOpen((v) => !v)}
          disabled={creating}
        >
          {creating ? <Loader2 className="h-4 w-4 animate-spin" /> : <Plus className="h-4 w-4" />}
          New session
        </Button>
        {pickerOpen ? (
          <div className="absolute left-3 right-3 top-[calc(100%-0.25rem)] z-10 rounded-[var(--radius)] border border-[var(--color-border-strong)] bg-[var(--color-background)] p-1 shadow-lg">
            {AGENT_OPTIONS.map((opt) => (
              <button
                key={opt.value}
                className="w-full rounded-[var(--radius-sm)] px-3 py-2 text-left text-sm hover:bg-[var(--color-surface-muted)]"
                onClick={() => void handleCreate(opt.value)}
              >
                {opt.label}
              </button>
            ))}
          </div>
        ) : null}
        {createError ? (
          <p className="mt-2 text-xs text-[var(--color-danger)]">{createError}</p>
        ) : null}
      </div>

      <div className="flex items-center justify-between px-4 pt-3 pb-2">
        <span className="text-[11px] uppercase tracking-wider text-[var(--color-foreground-muted)]">Sessions</span>
        <span className="text-[11px] text-[var(--color-foreground-muted)]">{state.sessions.length}</span>
      </div>

      <div className="flex-1 overflow-y-auto">
        {state.sessions.length === 0 ? (
          <p className="px-4 py-6 text-center text-xs text-[var(--color-foreground-muted)]">
            {state.ready ? "No sessions yet. Create one to get started." : "Loading…"}
          </p>
        ) : (
          <ul className="px-1.5 pb-3 space-y-0.5">
            {state.sessions.map((s) => {
              const active = s.id === state.selectedId;
              return (
                <li key={s.id}>
                  <button
                    className={cn(
                      "group flex w-full items-start gap-2 rounded-[var(--radius)] px-2.5 py-2 text-left transition-colors",
                      active
                        ? "bg-[var(--color-surface-muted)] border-l-2 border-[var(--color-accent)] pl-[calc(0.625rem-2px)]"
                        : "hover:bg-[var(--color-surface-muted)]"
                    )}
                    onClick={() => selectSession(s.id)}
                  >
                    <StatusDot tone={statusTone(s.status)} />
                    <div className="flex-1 min-w-0">
                      <div className="truncate text-sm leading-tight">
                        {s.id}
                      </div>
                      <div className="mt-0.5 flex items-center gap-1.5 text-[11px] text-[var(--color-foreground-muted)]">
                        <span>{agentLabel(s.agent)}</span>
                        <span aria-hidden>·</span>
                        <span>{statusLabel(s.status)}</span>
                        <span aria-hidden>·</span>
                        <span>{formatRelative(s.updated_at)}</span>
                      </div>
                    </div>
                  </button>
                </li>
              );
            })}
          </ul>
        )}
      </div>

      {state.connection !== "live" && state.connection !== "idle" ? (
        <div className="border-t border-[var(--color-border)] bg-[var(--color-surface-muted)] px-3 py-2 text-[11px] text-[var(--color-foreground-muted)]">
          {state.connection === "connecting"
            ? "Connecting to event stream…"
            : state.connection === "reconnecting"
            ? "Reconnecting…"
            : "Disconnected. Retrying."}
        </div>
      ) : null}
    </aside>
  );
}
