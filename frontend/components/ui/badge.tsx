import type { HTMLAttributes } from "react";

import { cn } from "@/lib/cn";

export function Badge({ className, ...rest }: HTMLAttributes<HTMLSpanElement>) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full border border-[var(--color-border)] bg-[var(--color-surface)] px-2 py-[1px] text-[11px] font-medium text-[var(--color-foreground-muted)]",
        className
      )}
      {...rest}
    />
  );
}

type DotProps = { tone?: "running" | "idle" | "ready" | "completed" | "failed" | "muted" };

export function StatusDot({ tone = "muted" }: DotProps) {
  const color =
    tone === "running"
      ? "bg-[var(--color-accent)] animate-pulse-dot"
      : tone === "idle"
      ? "bg-[var(--color-success)]"
      : tone === "ready"
      ? "bg-[var(--color-warning)]"
      : tone === "completed"
      ? "bg-[var(--color-success)]"
      : tone === "failed"
      ? "bg-[var(--color-danger)]"
      : "bg-[var(--color-border-strong)]";
  return <span className={cn("inline-block h-2 w-2 rounded-full", color)} />;
}

export function statusTone(status: string): DotProps["tone"] {
  switch (status) {
    case "running":
      return "running";
    case "idle":
      return "idle";
    case "created":
      return "ready";
    case "completed":
      return "completed";
    case "failed":
      return "failed";
    default:
      return "muted";
  }
}
