"use client";

import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

interface PanelProps {
  title: string;
  hint?: string;
  children: ReactNode;
  className?: string;
}

export function Panel({ title, hint, children, className }: PanelProps) {
  return (
    <section
      className={cn(
        "min-w-0 rounded-xl border border-white/[0.08] bg-zinc-950/70 p-3 backdrop-blur-xl",
        className,
      )}
    >
      <header className="mb-2 flex items-center justify-between gap-2">
        <h3 className="font-mono text-[10px] uppercase tracking-[0.12em] text-zinc-500">{title}</h3>
        {hint && <span className="font-mono text-[9px] text-zinc-600">{hint}</span>}
      </header>
      {children}
    </section>
  );
}
