"use client";

import { forwardRef, type TextareaHTMLAttributes } from "react";

import { cn } from "@/lib/cn";

export const Textarea = forwardRef<HTMLTextAreaElement, TextareaHTMLAttributes<HTMLTextAreaElement>>(
  ({ className, ...rest }, ref) => (
    <textarea
      ref={ref}
      className={cn(
        "w-full resize-none rounded-[var(--radius)] border border-[var(--color-border)] bg-[var(--color-surface)] px-3 py-2 text-sm leading-relaxed",
        "placeholder:text-[var(--color-foreground-muted)] focus:border-[var(--color-accent)] focus:outline-none",
        "disabled:opacity-60 disabled:cursor-not-allowed",
        className
      )}
      {...rest}
    />
  )
);
Textarea.displayName = "Textarea";
