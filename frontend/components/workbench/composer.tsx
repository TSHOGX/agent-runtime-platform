"use client";

import { useEffect, useRef, useState, type KeyboardEvent } from "react";
import { Send } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";

type ComposerProps = {
  disabled?: boolean;
  placeholder?: string;
  onSend: (content: string) => Promise<{ ok: boolean; error?: string }>;
};

export function Composer({ disabled, placeholder, onSend }: ComposerProps) {
  const [value, setValue] = useState("");
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const ref = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    if (!ref.current) return;
    ref.current.style.height = "auto";
    ref.current.style.height = `${Math.min(ref.current.scrollHeight, 240)}px`;
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

  const onKey = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey && !e.nativeEvent.isComposing) {
      e.preventDefault();
      void submit();
    }
  };

  return (
    <div className="border-t border-[var(--color-border)] bg-[var(--color-background)] px-6 py-3">
      <div className="relative flex items-end gap-2 rounded-[var(--radius-lg)] border border-[var(--color-border-strong)] bg-[var(--color-surface)] px-3 py-2 focus-within:border-[var(--color-accent)]">
        <Textarea
          ref={ref}
          value={value}
          onChange={(e) => setValue(e.target.value)}
          onKeyDown={onKey}
          placeholder={placeholder ?? "Send a message…"}
          rows={1}
          disabled={disabled || sending}
          className="border-0 bg-transparent p-0 focus:border-0 min-h-[28px]"
        />
        <Button
          variant="primary"
          size="icon"
          onClick={() => void submit()}
          disabled={disabled || sending || !value.trim()}
          aria-label="Send"
        >
          <Send className="h-4 w-4" />
        </Button>
      </div>
      <div className="mt-1.5 flex items-center justify-between text-[11px] text-[var(--color-foreground-muted)]">
        <span>{disabled ? "Session is busy or ended." : "Enter to send · Shift+Enter for newline"}</span>
        {error ? <span className="text-[var(--color-danger)]">{error}</span> : null}
      </div>
    </div>
  );
}
