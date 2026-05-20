"use client";

import { type ReactNode } from "react";

import { cn } from "@/lib/cn";

type TabsProps = {
  value: string | null;
  onValueChange: (value: string) => void;
  items: { value: string; label: ReactNode; onClose?: () => void }[];
  className?: string;
};

export function Tabs({ value, onValueChange, items, className }: TabsProps) {
  if (items.length === 0) return null;
  return (
    <div className={cn("flex items-center gap-1 overflow-x-auto border-b border-[var(--color-border)] px-1 bg-[var(--color-surface)]", className)}>
      {items.map((item) => {
        const active = item.value === value;
        return (
          <div
            key={item.value}
            className={cn(
              "group flex items-center gap-1 rounded-t-[var(--radius)] border-b-2 -mb-px px-2.5 py-1.5 text-xs cursor-pointer select-none whitespace-nowrap",
              active
                ? "border-[var(--color-accent)] text-[var(--color-foreground)] bg-[var(--color-background)]"
                : "border-transparent text-[var(--color-foreground-muted)] hover:text-[var(--color-foreground)]"
            )}
            onClick={() => onValueChange(item.value)}
          >
            <span className="truncate max-w-[160px]">{item.label}</span>
            {item.onClose ? (
              <button
                aria-label="Close tab"
                className="opacity-50 hover:opacity-100"
                onClick={(e) => {
                  e.stopPropagation();
                  item.onClose?.();
                }}
              >
                ×
              </button>
            ) : null}
          </div>
        );
      })}
    </div>
  );
}
