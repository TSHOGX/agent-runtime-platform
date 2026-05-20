"use client";

import { useEffect, useState } from "react";
import { Loader2 } from "lucide-react";

import { fetchArtifactText } from "@/lib/api";
import { classifyArtifact } from "@/lib/mime";
import { MarkdownView } from "./markdown-view";
import { CodeView } from "./viewers/code-view";

type Props = { sessionId: string; path: string };

type LoadState =
  | { phase: "skip" }
  | { phase: "loading" }
  | { phase: "ok"; text: string }
  | { phase: "error"; error: string };

export function ArtifactViewer({ sessionId, path }: Props) {
  const meta = classifyArtifact(path);
  const skip = meta.kind === "binary" || meta.kind === "image";
  const [load, setLoad] = useState<LoadState>(skip ? { phase: "skip" } : { phase: "loading" });

  useEffect(() => {
    if (skip) return;
    let cancelled = false;
    void fetchArtifactText(sessionId, path).then((res) => {
      if (cancelled) return;
      setLoad(res.ok ? { phase: "ok", text: res.text } : { phase: "error", error: res.error });
    });
    return () => {
      cancelled = true;
    };
  }, [sessionId, path, skip]);

  if (meta.kind === "image") {
    return (
      <div className="flex h-full items-center justify-center bg-[var(--color-surface)] p-4 overflow-auto">
        {/* eslint-disable-next-line @next/next/no-img-element */}
        <img
          src={`/artifacts/${encodeURIComponent(sessionId)}/${path
            .split("/")
            .map(encodeURIComponent)
            .join("/")}`}
          alt={path}
          className="max-h-full max-w-full rounded-[var(--radius)] border border-[var(--color-border)]"
        />
      </div>
    );
  }

  if (meta.kind === "binary") {
    return (
      <div className="flex h-full items-center justify-center px-6 text-center">
        <div>
          <p className="text-sm">Binary file — preview not supported.</p>
          <p className="mt-1 text-xs text-[var(--color-foreground-muted)]">
            Use the Download link in the file row to fetch the raw bytes.
          </p>
        </div>
      </div>
    );
  }

  if (load.phase === "loading") {
    return (
      <div className="flex h-full items-center justify-center text-[var(--color-foreground-muted)]">
        <Loader2 className="h-4 w-4 animate-spin" />
      </div>
    );
  }
  if (load.phase === "error") {
    return <div className="px-6 py-4 text-sm text-[var(--color-danger)]">{load.error}</div>;
  }
  if (load.phase !== "ok") return null;

  if (meta.kind === "markdown") {
    return (
      <div className="overflow-auto px-6 py-4">
        <MarkdownView content={load.text} />
      </div>
    );
  }
  if (meta.kind === "code") {
    return <CodeView code={load.text} language={meta.language} />;
  }
  return (
    <pre className="overflow-auto whitespace-pre-wrap break-words px-6 py-4 font-mono text-xs leading-relaxed">
      {load.text}
    </pre>
  );
}
