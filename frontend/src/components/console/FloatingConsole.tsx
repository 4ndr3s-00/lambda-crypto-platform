"use client";

import { useCallback, useEffect, useRef, useState, type ChangeEvent, type KeyboardEvent } from "react";
import { motion } from "framer-motion";
import { cn } from "@/lib/utils";

const PLACEHOLDER = "Pregunta lo que sea...";
const MAX_HEIGHT = 168;

interface FloatingConsoleProps {
  value: string;
  onChange: (value: string) => void;
  onSubmit: () => void;
  isGenerating: boolean;
}

/**
 * The single input capsule. It owns no layout of its own — the parent decides
 * whether it sits dead-center (home) or docked at the bottom (active chat) and
 * Framer Motion springs the difference.
 */
export function FloatingConsole({ value, onChange, onSubmit, isGenerating }: FloatingConsoleProps) {
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const [focused, setFocused] = useState(false);

  const resize = useCallback(() => {
    const node = textareaRef.current;
    if (!node) return;
    node.style.height = "auto";
    node.style.height = `${Math.min(node.scrollHeight, MAX_HEIGHT)}px`;
  }, []);

  // The parent clears `value` after a submit; collapse the textarea back to one row.
  useEffect(() => {
    resize();
  }, [value, resize]);

  const ready = value.trim().length > 0 && !isGenerating;

  const submit = useCallback(() => {
    if (!value.trim() || isGenerating) return;
    onSubmit();
  }, [isGenerating, onSubmit, value]);

  const handleChange = useCallback(
    (event: ChangeEvent<HTMLTextAreaElement>) => {
      onChange(event.target.value);
    },
    [onChange],
  );

  const handleKeyDown = useCallback(
    (event: KeyboardEvent<HTMLTextAreaElement>) => {
      if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        submit();
      }
    },
    [submit],
  );

  return (
    <motion.div
      layout
      transition={{ type: "spring", stiffness: 280, damping: 28 }}
      className="pointer-events-auto w-full max-w-2xl"
    >
      <div
        className={cn(
          "flex items-end gap-2 rounded-2xl border bg-zinc-950/70 p-3 shadow-2xl backdrop-blur-2xl transition-all duration-300",
          focused ? "border-white/20" : "border-white/10 hover:border-white/20",
        )}
      >
        <textarea
          ref={textareaRef}
          value={value}
          onChange={handleChange}
          onKeyDown={handleKeyDown}
          onFocus={() => setFocused(true)}
          onBlur={() => setFocused(false)}
          rows={1}
          spellCheck={false}
          aria-label="Consulta en lenguaje natural"
          placeholder={PLACEHOLDER}
          className="min-h-[40px] w-full resize-none bg-transparent px-2 py-2 font-mono text-sm text-zinc-100 outline-none placeholder:text-zinc-600 caret-zinc-300 focus:outline-none"
        />

        <button
          type="button"
          onClick={submit}
          disabled={!ready}
          aria-label="Ejecutar consulta"
          className={cn(
            "mb-0.5 flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-white text-black",
            "transition-all duration-200 active:scale-95",
            ready ? "opacity-100 hover:bg-zinc-200" : "cursor-not-allowed opacity-30",
          )}
        >
          {isGenerating ? (
            <span className="h-3.5 w-3.5 animate-spin rounded-full border-[1.5px] border-zinc-500 border-t-transparent" />
          ) : (
            <svg viewBox="0 0 16 16" className="h-3.5 w-3.5" fill="none" aria-hidden="true">
              <path
                d="M8 13V3M8 3L3.5 7.5M8 3l4.5 4.5"
                stroke="currentColor"
                strokeWidth="1.6"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
          )}
        </button>
      </div>
    </motion.div>
  );
}
