"use client";

import { useMemo, useState } from "react";
import {
  Braces,
  ChevronRight,
  Code2,
  Download,
  File,
  FileText,
  Folder,
  FolderOpen,
  Image as ImageIcon,
  Search,
  Table2
} from "lucide-react";

import { useArtifacts, useSelectedSession } from "@/components/harness-provider";
import { Tabs } from "@/components/ui/tabs";
import { ArtifactViewer } from "./artifact-viewer";
import { buildArtifactHref } from "@/lib/api";
import { buildArtifactTree, type ArtifactTreeDirectory, type ArtifactTreeNode } from "@/lib/artifact-tree";
import { formatBytes, formatRelative } from "@/lib/format";
import { classifyArtifact } from "@/lib/mime";
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
  const [collapsedDirs, setCollapsedDirs] = useState<Set<string>>(() => new Set());
  const [query, setQuery] = useState("");

  const artifactPaths = useMemo(() => new Set(artifacts.map((a) => a.path)), [artifacts]);
  const liveOpenTabs = useMemo(
    () => openTabs.filter((path) => artifactPaths.has(path)),
    [artifactPaths, openTabs]
  );
  const liveActiveTab = activeTab && artifactPaths.has(activeTab) ? activeTab : null;
  const visibleArtifacts = useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) return artifacts;
    return artifacts.filter((a) => a.path.toLowerCase().includes(needle));
  }, [artifacts, query]);
  const tree = useMemo(() => buildArtifactTree(visibleArtifacts), [visibleArtifacts]);
  const activeArtifact = liveActiveTab ? artifacts.find((a) => a.path === liveActiveTab) ?? null : null;

  const openArtifact = (path: string) => {
    setOpenTabs((prev) => {
      if (prev.includes(path)) return prev;
      const next = [...prev, path];
      return next.length > MAX_TABS ? next.slice(next.length - MAX_TABS) : next;
    });
    setActiveTab(path);
  };

  const closeTab = (path: string) => {
    const idx = liveOpenTabs.indexOf(path);
    const remaining = liveOpenTabs.filter((p) => p !== path);
    setOpenTabs(remaining);
    setActiveTab((prev) => {
      if (prev !== path) return prev;
      return remaining[Math.min(idx, remaining.length - 1)] ?? null;
    });
  };

  const toggleDirectory = (path: string) => {
    setCollapsedDirs((prev) => {
      const next = new Set(prev);
      if (next.has(path)) {
        next.delete(path);
      } else {
        next.add(path);
      }
      return next;
    });
  };

  return (
    <aside className="flex h-full min-h-0 flex-col border-l border-[var(--color-border)] bg-[var(--color-surface)]">
      <div className="border-b border-[var(--color-border)] px-4 py-3">
        <div className="flex items-center justify-between gap-3">
          <span className="text-xs uppercase tracking-wider text-[var(--color-foreground-muted)]">
            Files {artifacts.length > 0 ? `(${artifacts.length})` : ""}
          </span>
          {artifacts.length > 0 ? (
            <span className="shrink-0 text-[11px] text-[var(--color-foreground-muted)]">
              {formatBytes(tree.size)}
            </span>
          ) : null}
        </div>
        {artifacts.length > 0 ? (
          <label className="mt-2 flex h-8 items-center gap-2 rounded-[var(--radius)] border border-[var(--color-border)] bg-[var(--color-background)] px-2 text-xs">
            <Search className="h-3.5 w-3.5 shrink-0 text-[var(--color-foreground-muted)]" />
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search files"
              className="min-w-0 flex-1 bg-transparent outline-none placeholder:text-[var(--color-foreground-muted)]"
            />
          </label>
        ) : null}
      </div>

      <div className="max-h-[42%] min-h-[144px] overflow-y-auto border-b border-[var(--color-border)]">
        {artifacts.length === 0 ? (
          <p className="px-4 py-4 text-xs text-[var(--color-foreground-muted)]">
            No artifacts yet. The agent writes files to <code className="font-mono">/workspace</code>.
          </p>
        ) : visibleArtifacts.length === 0 ? (
          <p className="px-4 py-4 text-xs text-[var(--color-foreground-muted)]">No matching files.</p>
        ) : (
          <ArtifactTreeView
            root={tree}
            collapsedDirs={collapsedDirs}
            forceExpanded={query.trim().length > 0}
            activePath={liveActiveTab}
            onToggleDirectory={toggleDirectory}
            onOpenArtifact={openArtifact}
          />
        )}
      </div>

      <div className="flex flex-1 min-h-0 flex-col">
        {liveOpenTabs.length > 0 ? (
          <Tabs
            value={liveActiveTab}
            onValueChange={setActiveTab}
            items={liveOpenTabs.map((path) => ({
              value: path,
              label: path.split("/").pop() ?? path,
              onClose: () => closeTab(path)
            }))}
          />
        ) : null}
        <div className="flex-1 min-h-0 overflow-hidden">
          {liveActiveTab && activeArtifact ? (
            <ArtifactViewer
              key={`${liveActiveTab}:${activeArtifact.updated_at}`}
              sessionId={sessionId}
              path={liveActiveTab}
              artifact={activeArtifact}
            />
          ) : (
            <div className="flex h-full items-center justify-center text-center text-xs text-[var(--color-foreground-muted)]">
              <span>Select a file from the tree above to preview it here.</span>
            </div>
          )}
        </div>
      </div>
    </aside>
  );
}

function ArtifactTreeView({
  root,
  collapsedDirs,
  forceExpanded,
  activePath,
  onToggleDirectory,
  onOpenArtifact
}: {
  root: ArtifactTreeDirectory;
  collapsedDirs: Set<string>;
  forceExpanded: boolean;
  activePath: string | null;
  onToggleDirectory: (path: string) => void;
  onOpenArtifact: (path: string) => void;
}) {
  return (
    <ul className="py-1">
      {root.children.map((node) => (
        <TreeNode
          key={`${node.kind}:${node.path}`}
          node={node}
          depth={0}
          collapsedDirs={collapsedDirs}
          forceExpanded={forceExpanded}
          activePath={activePath}
          onToggleDirectory={onToggleDirectory}
          onOpenArtifact={onOpenArtifact}
        />
      ))}
    </ul>
  );
}

function TreeNode({
  node,
  depth,
  collapsedDirs,
  forceExpanded,
  activePath,
  onToggleDirectory,
  onOpenArtifact
}: {
  node: ArtifactTreeNode;
  depth: number;
  collapsedDirs: Set<string>;
  forceExpanded: boolean;
  activePath: string | null;
  onToggleDirectory: (path: string) => void;
  onOpenArtifact: (path: string) => void;
}) {
  const indent = 8 + depth * 14;

  if (node.kind === "directory") {
    const expanded = forceExpanded || !collapsedDirs.has(node.path);
    return (
      <li>
        <button
          className="flex h-7 w-full items-center gap-1.5 px-2 pr-3 text-left text-xs hover:bg-[var(--color-surface-muted)]"
          style={{ paddingLeft: indent }}
          onClick={() => onToggleDirectory(node.path)}
          aria-expanded={expanded}
        >
          <ChevronRight
            className={cn(
              "h-3.5 w-3.5 shrink-0 text-[var(--color-foreground-muted)] transition-transform",
              expanded && "rotate-90"
            )}
          />
          {expanded ? (
            <FolderOpen className="h-3.5 w-3.5 shrink-0 text-[var(--color-foreground-muted)]" />
          ) : (
            <Folder className="h-3.5 w-3.5 shrink-0 text-[var(--color-foreground-muted)]" />
          )}
          <span className="min-w-0 flex-1 truncate font-mono">{node.name}</span>
          <span className="shrink-0 text-[10px] text-[var(--color-foreground-muted)]">
            {node.fileCount} · {formatBytes(node.size)}
          </span>
        </button>
        {expanded ? (
          <ul>
            {node.children.map((child) => (
              <TreeNode
                key={`${child.kind}:${child.path}`}
                node={child}
                depth={depth + 1}
                collapsedDirs={collapsedDirs}
                forceExpanded={forceExpanded}
                activePath={activePath}
                onToggleDirectory={onToggleDirectory}
                onOpenArtifact={onOpenArtifact}
              />
            ))}
          </ul>
        ) : null}
      </li>
    );
  }

  const active = activePath === node.path;
  return (
    <li>
      <div
        className={cn(
          "group flex h-7 items-center gap-1.5 px-2 pr-2 text-xs hover:bg-[var(--color-surface-muted)]",
          active && "bg-[var(--color-surface-muted)]"
        )}
        style={{ paddingLeft: indent + 21 }}
      >
        <button
          className="flex min-w-0 flex-1 items-center gap-1.5 text-left"
          onClick={() => onOpenArtifact(node.path)}
          title={node.path}
        >
          <ArtifactIcon path={node.path} />
          <span className="min-w-0 flex-1 truncate font-mono">{node.name}</span>
          <span className="shrink-0 text-[10px] text-[var(--color-foreground-muted)]">
            {formatBytes(node.artifact.size)} · {formatRelative(node.artifact.updated_at)}
          </span>
        </button>
        <a
          href={buildArtifactHref(node.artifact.session_id, node.path)}
          download
          className="shrink-0 text-[var(--color-foreground-muted)] opacity-70 hover:text-[var(--color-foreground)] group-hover:opacity-100"
          title="Download"
        >
          <Download className="h-3.5 w-3.5" />
        </a>
      </div>
    </li>
  );
}

function ArtifactIcon({ path }: { path: string }) {
  const kind = classifyArtifact(path).kind;
  const className = "h-3.5 w-3.5 shrink-0 text-[var(--color-foreground-muted)]";
  switch (kind) {
    case "image":
      return <ImageIcon className={className} />;
    case "code":
      return <Code2 className={className} />;
    case "json":
      return <Braces className={className} />;
    case "table":
      return <Table2 className={className} />;
    case "markdown":
    case "text":
      return <FileText className={className} />;
    default:
      return <File className={className} />;
  }
}
