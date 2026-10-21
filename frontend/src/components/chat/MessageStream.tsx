"use client";

import { useCallback, useEffect, useRef, type CSSProperties, type UIEvent } from "react";
import { AnimatePresence, motion } from "framer-motion";
import type { ChatMessage } from "@/lib/types";
import { MessageItem } from "./MessageItem";

interface MessageStreamProps {
  messages: ChatMessage[];
  busy: boolean;
}

/** Off-screen cost is deferred by the browser via content-visibility. */
const VIRTUALIZED: CSSProperties = {
  contentVisibility: "auto",
  containIntrinsicSize: "auto 140px",
};

export function MessageStream({ messages, busy }: MessageStreamProps) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const contentRef = useRef<HTMLDivElement>(null);
  const pinnedRef = useRef(true);

  const handleScroll = useCallback((event: UIEvent<HTMLDivElement>) => {
    const el = event.currentTarget;
    const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
    pinnedRef.current = distanceFromBottom < 96;
  }, []);

  const scrollToBottom = useCallback((force: boolean) => {
    const el = scrollRef.current;
    if (!el) return;
    if (force || pinnedRef.current) {
      el.scrollTop = el.scrollHeight;
    }
  }, []);

  useEffect(() => {
    scrollToBottom(true);
  }, [messages.length, scrollToBottom]);

  /* Artifacts, SQL panels and streamed text change height after mount.
     Keep the tail pinned without fighting a user who scrolled up. */
  useEffect(() => {
    const content = contentRef.current;
    if (!content || typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver(() => scrollToBottom(false));
    observer.observe(content);
    return () => observer.disconnect();
  }, [scrollToBottom]);

  const lastId = messages[messages.length - 1]?.id;

  return (
    <div
      ref={scrollRef}
      onScroll={handleScroll}
      className="pointer-events-auto relative flex-1 overflow-y-auto overscroll-contain"
    >
      <div
        ref={contentRef}
        className="mx-auto flex w-full max-w-3xl flex-col gap-6 px-4 pb-40 pt-28 sm:px-6 sm:pb-44 sm:pt-20"
      >
        {messages.map((message) => (
          <div key={message.id} style={message.id === lastId ? undefined : VIRTUALIZED}>
            <MessageItem message={message} onStreamTick={() => scrollToBottom(false)} />
          </div>
        ))}

        <AnimatePresence>
          {busy && (
            <motion.div
              key="thinking"
              initial={{ opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0 }}
              transition={{ type: "spring", stiffness: 300, damping: 30 }}
              className="flex items-center gap-2.5"
            >
              <span className="flex h-4 w-4 items-center justify-center rounded-full border border-white/15 font-mono text-[8px] text-emerald-400">
                Q
              </span>
              <span className="font-mono text-[11px] text-zinc-500">
                Qwen2.5 sintetizando…
              </span>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}
