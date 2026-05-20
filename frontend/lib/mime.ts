export type ViewerKind = "markdown" | "code" | "image" | "text" | "binary";

const CODE_EXT: Record<string, string> = {
  js: "javascript",
  jsx: "javascript",
  ts: "typescript",
  tsx: "typescript",
  py: "python",
  go: "go",
  rs: "rust",
  java: "java",
  rb: "ruby",
  php: "php",
  sh: "bash",
  bash: "bash",
  zsh: "bash",
  c: "c",
  h: "c",
  cpp: "cpp",
  hpp: "cpp",
  cs: "csharp",
  swift: "swift",
  kt: "kotlin",
  css: "css",
  scss: "scss",
  html: "xml",
  xml: "xml",
  sql: "sql",
  yaml: "yaml",
  yml: "yaml",
  toml: "ini",
  ini: "ini",
  json: "json"
};

const TEXT_EXT = new Set(["txt", "log", "csv", "tsv", "env", "diff", "patch"]);
const IMAGE_EXT = new Set(["png", "jpg", "jpeg", "gif", "webp", "svg", "bmp", "ico"]);

export function classifyArtifact(path: string): { kind: ViewerKind; language?: string } {
  const ext = (path.split(".").pop() ?? "").toLowerCase();
  const base = (path.split("/").pop() ?? "").toLowerCase();
  if (!ext) {
    if (["dockerfile", "makefile", "rakefile", "readme", "license"].includes(base)) {
      return { kind: "text" };
    }
    return { kind: "binary" };
  }
  if (ext === "md" || ext === "markdown" || ext === "mdx") return { kind: "markdown" };
  if (IMAGE_EXT.has(ext)) return { kind: "image" };
  if (CODE_EXT[ext]) return { kind: "code", language: CODE_EXT[ext] };
  if (TEXT_EXT.has(ext)) return { kind: "text" };
  return { kind: "binary" };
}
