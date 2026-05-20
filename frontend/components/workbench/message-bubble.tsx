"use client";

import { User, Bot } from "lucide-react";

import { MarkdownView } from "./markdown-view";
import type { ApiMessage } from "@/lib/types";
import { cn } from "@/lib/cn";

export function MessageBubble({ message }: { message: ApiMessage }) {
  const isUser = message.role === "user";
  return (
    <div
      className={cn(
        "flex gap-3 px-6 py-4",
        isUser ? "bg-transparent" : "bg-[var(--color-surface)]"
      )}
    >
      <div
        className={cn(
          "flex h-7 w-7 shrink-0 items-center justify-center rounded-full text-[11px] font-medium",
          isUser
            ? "bg-[var(--color-user-bubble)] text-[var(--color-foreground)]"
            : "bg-[var(--color-foreground)] text-[var(--color-accent-foreground)]"
        )}
      >
        {isUser ? <User className="h-3.5 w-3.5" /> : <Bot className="h-3.5 w-3.5" />}
      </div>
      <div className="flex-1 min-w-0">
        <div className="text-[11px] uppercase tracking-wider text-[var(--color-foreground-muted)]">
          {isUser ? "You" : "Assistant"}
        </div>
        <div className="mt-1">
          {isUser ? (
            <div className="whitespace-pre-wrap break-words text-sm leading-relaxed">{message.content}</div>
          ) : (
            <MarkdownView content={message.content} />
          )}
        </div>
      </div>
    </div>
  );
}

export function StreamingBubble({ text }: { text: string }) {
  return (
    <div className="flex gap-3 px-6 py-4 bg-[var(--color-surface)]">
      <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-[var(--color-foreground)] text-[var(--color-accent-foreground)]">
        <Bot className="h-3.5 w-3.5" />
      </div>
      <div className="flex-1 min-w-0">
        <div className="text-[11px] uppercase tracking-wider text-[var(--color-foreground-muted)]">
          Assistant
          <span className="ml-1 inline-block h-1.5 w-1.5 rounded-full bg-[var(--color-accent)] animate-pulse-dot" />
        </div>
        <div className="mt-1">
          <MarkdownView content={text || "…"} />
        </div>
      </div>
    </div>
  );
}
