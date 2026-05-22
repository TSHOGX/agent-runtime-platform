import type { ApiArtifact } from "./types";

export type ArtifactTreeFile = {
  kind: "file";
  name: string;
  path: string;
  artifact: ApiArtifact;
};

export type ArtifactTreeDirectory = {
  kind: "directory";
  name: string;
  path: string;
  children: ArtifactTreeNode[];
  fileCount: number;
  size: number;
  updatedAt: string | null;
};

export type ArtifactTreeNode = ArtifactTreeDirectory | ArtifactTreeFile;

export function buildArtifactTree(artifacts: ApiArtifact[]): ArtifactTreeDirectory {
  const root: ArtifactTreeDirectory = {
    kind: "directory",
    name: "",
    path: "",
    children: [],
    fileCount: 0,
    size: 0,
    updatedAt: null
  };
  const directories = new Map<string, ArtifactTreeDirectory>([["", root]]);

  for (const artifact of [...artifacts].sort((a, b) => a.path.localeCompare(b.path))) {
    const parts = artifact.path.split("/").filter(Boolean);
    if (parts.length === 0) continue;

    let current = root;
    root.fileCount += 1;
    root.size += artifact.size;
    root.updatedAt = latestTime(root.updatedAt, artifact.updated_at);

    for (const segment of parts.slice(0, -1)) {
      const path = current.path ? `${current.path}/${segment}` : segment;
      let next = directories.get(path);
      if (!next) {
        next = {
          kind: "directory",
          name: segment,
          path,
          children: [],
          fileCount: 0,
          size: 0,
          updatedAt: null
        };
        directories.set(path, next);
        current.children.push(next);
      }
      next.fileCount += 1;
      next.size += artifact.size;
      next.updatedAt = latestTime(next.updatedAt, artifact.updated_at);
      current = next;
    }

    current.children.push({
      kind: "file",
      name: parts[parts.length - 1],
      path: artifact.path,
      artifact
    });
  }

  sortTree(root);
  return root;
}

export function collectDirectoryPaths(root: ArtifactTreeDirectory) {
  const paths: string[] = [];
  const visit = (node: ArtifactTreeNode) => {
    if (node.kind !== "directory") return;
    if (node.path) paths.push(node.path);
    node.children.forEach(visit);
  };
  root.children.forEach(visit);
  return paths;
}

function sortTree(dir: ArtifactTreeDirectory) {
  dir.children.sort((a, b) => {
    if (a.kind !== b.kind) return a.kind === "directory" ? -1 : 1;
    return a.name.localeCompare(b.name);
  });
  for (const child of dir.children) {
    if (child.kind === "directory") sortTree(child);
  }
}

function latestTime(current: string | null, candidate: string) {
  if (!current) return candidate;
  return new Date(candidate).getTime() > new Date(current).getTime() ? candidate : current;
}
