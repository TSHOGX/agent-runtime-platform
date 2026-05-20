import type { HTMLAttributes } from "react";

import { cn } from "@/lib/cn";

export function Separator({ className, ...rest }: HTMLAttributes<HTMLDivElement>) {
  return <div role="separator" className={cn("h-px w-full bg-[var(--color-border)]", className)} {...rest} />;
}
