"use client";

import { useEffect, useRef, useState, type KeyboardEvent } from "react";
import { Loader2, Send, Square, TerminalSquare } from "lucide-react";

import { useConversation, useHarness, useSelectedSession } from "@/components/harness-provider";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { StatusDot, statusTone } from "@/components/ui/badge";
import { agentLabel } from "@/lib/agents";
import { isAcceptingInput, isTerminal, statusLabel } from "@/lib/format";
import type { ApiMessage } from "@/lib/types";

function ShellEntry({ message }: { message: ApiMessage }) {
  const isUser = message.role === "user";
  return isUser ? (
    <div className="flex gap-2 px-6 py-3">
      <span className="shrink-0 font-mono text-[11px] leading-6 text-[var(--color-foreground-muted)]">$</span>
      <pre className="min-w-0 whitespace-pre-wrap break-words font-mono text-sm leading-6 text-[var(--color-foreground)]">
        {message.content}
      </pre>
    </div>
  ) : (
    <div className="px-6 py-2">
      <pre className="min-w-0 whitespace-pre-wrap break-words font-mono text-[13px] leading-6 text-[var(--color-foreground)]">
        {message.content}
      </pre>
    </div>
  );
}

function ShellComposer({
  disabled,
  busy,
  placeholder,
  onSend,
  onInterrupt
}: {
  disabled: boolean;
  busy: boolean;
  placeholder: string;
  onSend: (content: string) => Promise<{ ok: boolean; error?: string }>;
  onInterrupt: () => Promise<{ ok: boolean; error?: string }>;
}) {
  const [value, setValue] = useState("");
  const [sending, setSending] = useState(false);
  const [stopping, setStopping] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const ref = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    if (!ref.current) return;
    if (!value) {
      ref.current.style.height = "32px";
      return;
    }
    ref.current.style.height = "auto";
    ref.current.style.height = `${Math.max(32, Math.min(ref.current.scrollHeight, 240))}px`;
  }, [value]);

  const submit = async () => {
    const text = value.trim();
    if (!text || sending || disabled) return;
    setSending(true);
    setError(null);
    const res = await onSend(text);
    setSending(false);
    if (res.ok) {
      setValue("");
    } else {
      setError(res.error ?? "Failed to send");
    }
  };

  const interrupt = async () => {
    if (!busy || stopping) return;
    setStopping(true);
    setError(null);
    const res = await onInterrupt();
    setStopping(false);
    if (!res.ok) {
      setError(res.error ?? "Failed to interrupt");
    }
  };

  const onKey = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey && !e.nativeEvent.isComposing) {
      e.preventDefault();
      void submit();
    }
  };

  return (
    <div className="border-t border-[var(--color-border)] bg-[var(--color-background)] px-6 py-3">
      <div className="flex items-center gap-2 rounded-[var(--radius)] border border-[var(--color-border-strong)] bg-[var(--color-surface)] px-3 py-2">
        <span className="shrink-0 font-mono text-sm text-[var(--color-foreground-muted)]">$</span>
        <Textarea
          ref={ref}
          value={value}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={onKey}
          placeholder={placeholder}
          rows={1}
          disabled={disabled || sending || busy}
          className="h-8 min-h-8 border-0 bg-transparent px-0 py-1 font-mono text-sm leading-6 focus:border-0 focus:outline-none focus-visible:outline-none"
        />
        {busy ? (
          <Button
            variant="danger"
            size="icon"
            onClick={() => void interrupt()}
            disabled={stopping}
            aria-label="Stop"
            title="Stop"
          >
            {stopping ? <Loader2 className="h-4 w-4 animate-spin" /> : <Square className="h-4 w-4" />}
          </Button>
        ) : (
          <Button
            variant="primary"
            size="icon"
            onClick={() => void submit()}
            disabled={disabled || sending || !value.trim()}
            aria-label="Run"
          >
            <Send className="h-4 w-4" />
          </Button>
        )}
      </div>
      <div className="mt-1.5 flex items-center justify-between text-[11px] text-[var(--color-foreground-muted)]">
        <span>{busy ? "Command running. Stop to interrupt." : "Enter to run · Shift+Enter for newline"}</span>
        {error ? <span className="text-[var(--color-danger)]">{error}</span> : null}
      </div>
    </div>
  );
}

export function ShellConversation() {
  const session = useSelectedSession();
  const convo = useConversation(session?.id ?? null);
  const { sendMessage, interruptSession } = useHarness();
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    el.scrollTop = el.scrollHeight;
  }, [convo.messages.length]);

  if (!session) {
    return null;
  }

  const accepting = isAcceptingInput(session.status);
  const busy = session.status === "running_active";
  const terminal = isTerminal(session.status);
  const placeholder = terminal
    ? "Session ended. Start a new one to continue."
    : busy
    ? "Command running…"
    : "Run a command…";

  return (
    <main className="flex h-full min-h-0 min-w-0 flex-col bg-[var(--color-background)]">
      <header className="flex items-center justify-between border-b border-[var(--color-border)] bg-[var(--color-surface)] px-6 py-3">
        <div className="flex min-w-0 items-center gap-2">
          <StatusDot tone={statusTone(session.status)} />
          <span className="truncate text-sm font-medium">{session.id}</span>
          <span className="text-[11px] text-[var(--color-foreground-muted)]">· {agentLabel(session.agent)}</span>
        </div>
        <span className="text-[11px] text-[var(--color-foreground-muted)]">{statusLabel(session.status)}</span>
      </header>

      <div ref={scrollRef} className="flex-1 min-h-0 overflow-y-auto">
        {convo.loading && convo.messages.length === 0 ? (
          <p className="px-6 py-8 text-xs text-[var(--color-foreground-muted)]">Loading shell…</p>
        ) : convo.messages.length === 0 ? (
          <div className="flex h-full items-center justify-center px-6 text-center">
            <div className="max-w-md">
              <div className="flex items-center justify-center">
                <div className="flex h-10 w-10 items-center justify-center rounded-full bg-[var(--color-surface-muted)]">
                  <TerminalSquare className="h-5 w-5 text-[var(--color-foreground-muted)]" />
                </div>
              </div>
              <h3 className="mt-3 text-base font-semibold">Ready when you are.</h3>
              <p className="mt-1.5 text-sm text-[var(--color-foreground-muted)]">
                Type a shell command below. The session runs in <code className="font-mono text-xs bg-[var(--color-surface-muted)] px-1 rounded">/workspace</code>.
              </p>
            </div>
          </div>
        ) : (
          <div className="divide-y divide-[var(--color-border)]">
            {convo.messages.map((m) => (
              <ShellEntry key={m.id} message={m} />
            ))}
          </div>
        )}
      </div>

      <ShellComposer
        disabled={!accepting}
        busy={busy}
        placeholder={placeholder}
        onSend={(content) => sendMessage(session.id, content)}
        onInterrupt={() => interruptSession(session.id)}
      />
    </main>
  );
}
