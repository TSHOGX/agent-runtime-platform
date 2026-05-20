"use client";

import { forwardRef, type ButtonHTMLAttributes } from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";

const button = cva(
  "inline-flex items-center justify-center gap-2 rounded-[var(--radius)] font-medium transition-colors disabled:opacity-50 disabled:pointer-events-none focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--color-accent)]",
  {
    variants: {
      variant: {
        primary:
          "bg-[var(--color-accent)] text-[var(--color-accent-foreground)] hover:bg-[oklch(0.56_0.13_38)]",
        ghost:
          "bg-transparent text-[var(--color-foreground)] hover:bg-[var(--color-surface-muted)]",
        outline:
          "border border-[var(--color-border-strong)] bg-[var(--color-surface)] text-[var(--color-foreground)] hover:bg-[var(--color-surface-muted)]",
        soft:
          "bg-[var(--color-surface-muted)] text-[var(--color-foreground)] hover:bg-[oklch(0.92_0.01_70)]",
        danger:
          "bg-transparent text-[var(--color-danger)] hover:bg-[oklch(0.95_0.04_25)]"
      },
      size: {
        sm: "h-7 px-2 text-xs",
        md: "h-8 px-3 text-sm",
        lg: "h-9 px-4 text-sm",
        icon: "h-8 w-8 p-0"
      }
    },
    defaultVariants: { variant: "ghost", size: "md" }
  }
);

export type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement> &
  VariantProps<typeof button>;

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, size, ...rest }, ref) => (
    <button ref={ref} className={cn(button({ variant, size }), className)} {...rest} />
  )
);
Button.displayName = "Button";
