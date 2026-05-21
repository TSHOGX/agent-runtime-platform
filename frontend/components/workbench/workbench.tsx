"use client";

import { HarnessProvider, useHarness } from "@/components/harness-provider";
import { Sidebar } from "@/components/workbench/sidebar";
import { Conversation } from "@/components/workbench/conversation";
import { ArtifactPane } from "@/components/workbench/artifact-pane";

function Shell() {
  const { state } = useHarness();
  if (state.bootError) {
    return (
      <div className="flex h-screen items-center justify-center bg-[var(--color-background)] p-8 text-center">
        <div className="max-w-sm rounded-[var(--radius-lg)] border border-[var(--color-border)] bg-[var(--color-surface)] p-6">
          <h1 className="text-base font-semibold">Backend unreachable</h1>
          <p className="mt-2 text-sm text-[var(--color-foreground-muted)]">{state.bootError}</p>
          <p className="mt-3 text-xs text-[var(--color-foreground-muted)]">
            Start the orchestrator (./orchestrator) and the page will recover automatically.
          </p>
        </div>
      </div>
    );
  }
  return (
    <div className="grid h-screen grid-cols-[280px_minmax(0,1fr)_400px] bg-[var(--color-background)]">
      <Sidebar />
      <Conversation />
      <ArtifactPane />
    </div>
  );
}

export function Workbench() {
  return (
    <HarnessProvider>
      <Shell />
    </HarnessProvider>
  );
}
