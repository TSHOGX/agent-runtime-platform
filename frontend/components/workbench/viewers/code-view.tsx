"use client";

import { useEffect, useRef } from "react";
import hljs from "highlight.js";

export function CodeView({ code, language }: { code: string; language?: string }) {
  const ref = useRef<HTMLElement>(null);
  useEffect(() => {
    if (!ref.current) return;
    if (language) {
      try {
        const result = hljs.highlight(code, { language, ignoreIllegals: true });
        ref.current.innerHTML = result.value;
        return;
      } catch {
        // fall through
      }
    }
    const auto = hljs.highlightAuto(code);
    ref.current.innerHTML = auto.value;
  }, [code, language]);
  return (
    <pre className="h-full overflow-auto px-6 py-4 font-mono text-xs leading-relaxed">
      <code ref={ref} className={language ? `language-${language}` : undefined}>{code}</code>
    </pre>
  );
}
