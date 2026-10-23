"use client";

import { motion } from "framer-motion";
import type { QuickPrompt } from "@/lib/marketClient";
import { cn } from "@/lib/utils";

const container = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: { staggerChildren: 0.055, delayChildren: 0.05 },
  },
  exit: {
    opacity: 0,
    y: 10,
    transition: { duration: 0.15 },
  },
};

const item = {
  hidden: { opacity: 0, y: 12 },
  visible: {
    opacity: 1,
    y: 0,
    transition: { type: "spring" as const, stiffness: 320, damping: 28 },
  },
};

interface QuickPromptsProps {
  prompts: QuickPrompt[];
  onSelect: (query: string) => void;
  disabled?: boolean;
  className?: string;
}

/**
 * Crisp starter pills: hairline border, no backdrop blur, high-contrast mono text
 * so they stay razor sharp against the void.
 */
export function QuickPrompts({ prompts, onSelect, disabled = false, className }: QuickPromptsProps) {
  return (
    <motion.div
      variants={container}
      initial="hidden"
      animate="visible"
      exit="exit"
      className={cn("pointer-events-auto mt-5 flex max-w-2xl flex-wrap items-center justify-center gap-2.5", className)}
    >
      {prompts.map((prompt) => (
        <motion.button
          key={prompt.label}
          variants={item}
          type="button"
          disabled={disabled}
          onClick={() => onSelect(prompt.query)}
          className={cn(
            "rounded-full border border-white/10 bg-white/[0.03] px-4 py-2 font-mono text-xs text-zinc-300 shadow-sm",
            "transition-all duration-200 hover:border-white/25 hover:bg-white/[0.08] hover:text-white",
            "active:scale-[0.98] disabled:cursor-not-allowed disabled:opacity-40",
          )}
        >
          {prompt.label}
        </motion.button>
      ))}
    </motion.div>
  );
}
