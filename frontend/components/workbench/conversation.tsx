"use client";

import { useEffect, useRef } from "react";
import { Bot } from "lucide-react";

import { useConversation, useHarness, useSelectedSession } from "@/components/harness-provider";
import { MessageBubble, StreamingBubble } from "./message-bubble";
import { Composer } from "./composer";
import { StatusDot, statusTone } from "@/components/ui/badge";
import { agentLabel, isAcceptingInput, isTerminal, statusLabel } from "@/lib/format";

export function Conversation() {
  const session = useSelectedSession();
  const convo = useConversation(session?.id ?? null);
  const { sendMessage } = useHarness();
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    el.scrollTop = el.scrollHeight;
  }, [convo.messages.length, convo.streaming?.text, convo.stream.length]);

  if (!session) {
    return (
      <main className="flex flex-col items-center justify-center text-center bg-[var(--color-background)]">
        <div className="flex h-12 w-12 items-center justify-center rounded-full bg-[var(--color-surface-muted)]">
          <Bot className="h-5 w-5 text-[var(--color-foreground-muted)]" />
        </div>
        <h2 className="mt-3 text-sm font-medium">No session selected</h2>
        <p className="mt-1 max-w-xs text-xs text-[var(--color-foreground-muted)]">
          Pick a session from the left, or create a new one to start a conversation.
        </p>
      </main>
    );
  }

  const accepting = isAcceptingInput(session.status);
  const terminal = isTerminal(session.status);
  const placeholder = terminal
    ? "Session ended. Start a new one to continue."
    : accepting
    ? "Send a message…"
    : "Agent is thinking…";

  const showRuntimeFooter = convo.stream.length > 0;
  const lastStreamLine = convo.stream[convo.stream.length - 1];

  return (
    <main className="flex h-full min-h-0 flex-col bg-[var(--color-background)]">
      <header className="flex items-center justify-between border-b border-[var(--color-border)] bg-[var(--color-surface)] px-6 py-3">
        <div className="flex min-w-0 items-center gap-2">
          <StatusDot tone={statusTone(session.status)} />
          <span className="truncate text-sm font-medium">{session.id}</span>
          <span className="text-[11px] text-[var(--color-foreground-muted)]">
            · {agentLabel(session.agent)}
          </span>
        </div>
        <span className="text-[11px] text-[var(--color-foreground-muted)]">{statusLabel(session.status)}</span>
      </header>

      <div ref={scrollRef} className="flex-1 min-h-0 overflow-y-auto">
        {convo.loading && convo.messages.length === 0 ? (
          <p className="px-6 py-8 text-xs text-[var(--color-foreground-muted)]">Loading conversation…</p>
        ) : convo.messages.length === 0 && !convo.streaming ? (
          <div className="flex h-full items-center justify-center px-6 text-center">
            <div className="max-w-md">
              <h3 className="text-base font-semibold">Ready when you are.</h3>
              <p className="mt-1.5 text-sm text-[var(--color-foreground-muted)]">
                Type a prompt below. The agent will work in <code className="font-mono text-xs bg-[var(--color-surface-muted)] px-1 rounded">/workspace</code>{" "}
                inside its sandbox and stream replies back here.
              </p>
            </div>
          </div>
        ) : (
          <div className="divide-y divide-[var(--color-border)]">
            {convo.messages.map((m) => (
              <MessageBubble key={m.id} message={m} />
            ))}
            {convo.streaming ? <StreamingBubble text={convo.streaming.text} /> : null}
          </div>
        )}
      </div>

      {showRuntimeFooter && (session.status === "running" || session.status === "running_active") ? (
        <div className="border-t border-[var(--color-border)] bg-[var(--color-surface-muted)] px-6 py-1.5 font-mono text-[11px] text-[var(--color-foreground-muted)] truncate">
          {lastStreamLine.stream}: {lastStreamLine.line}
        </div>
      ) : null}

      <Composer
        disabled={!accepting}
        placeholder={placeholder}
        onSend={(content) => sendMessage(session.id, content)}
      />
    </main>
  );
}
