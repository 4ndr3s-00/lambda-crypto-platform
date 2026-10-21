"use client";

import { useEffect, useRef, useState } from "react";
import { motion } from "framer-motion";
import type { ChatMessage } from "@/lib/types";
import { cn, formatUtcStamp } from "@/lib/utils";
import { SQLInspector } from "./SQLInspector";
import { DataArtifact } from "./DataArtifact";
import { LineagePanel } from "./LineagePanel";

interface MessageItemProps {
  message: ChatMessage;
  /** Fired on every streamed frame so the list can keep the tail in view. */
  onStreamTick?: () => void;
}

/** Token-by-token cadence: faster for long answers, never instant, never sluggish. */
function streamDuration(totalChars: number): number {
  return Math.min(2600, Math.max(650, totalChars * 3.1));
}

export function MessageItem({ message, onStreamTick }: MessageItemProps) {
  const isUser = message.role === "user";
  const [revealed, setRevealed] = useState(isUser ? message.content.length : 0);
  const [streamDone, setStreamDone] = useState(isUser);

  const tickRef = useRef(onStreamTick);
  tickRef.current = onStreamTick;

  useEffect(() => {
    if (isUser) return;

    const total = message.content.length;
    const duration = streamDuration(total);
    let raf = 0;
    let start = 0;
    let cancelled = false;

    const step = (now: number) => {
      if (cancelled) return;
      if (start === 0) start = now;
      const t = Math.min(1, (now - start) / duration);
      // easeOutQuad — the cadence decelerates as the synthesis lands.
      const eased = 1 - (1 - t) ** 2.2;
      setRevealed(Math.floor(eased * total));
      tickRef.current?.();

      if (t < 1) {
        raf = requestAnimationFrame(step);
      } else {
        setRevealed(total);
        setStreamDone(true);
        tickRef.current?.();
      }
    };

    raf = requestAnimationFrame(step);
    return () => {
      cancelled = true;
      cancelAnimationFrame(raf);
    };
  }, [isUser, message.content]);

  if (isUser) {
    return (
      <motion.div
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ type: "spring", stiffness: 320, damping: 30 }}
        className="flex flex-col items-end gap-1"
      >
        <span className="pr-1 font-mono text-[10px] tabular-nums text-zinc-600">
          TÚ · {message.timestamp.slice(11, 19)}
        </span>
        <div className="max-w-[92%] rounded-2xl rounded-tr-sm border border-white/10 bg-white/[0.03] px-4 py-2.5 text-sm text-zinc-200 backdrop-blur-md">
          {message.content}
        </div>
      </motion.div>
    );
  }

  const visible = message.content.slice(0, revealed);

  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ type: "spring", stiffness: 320, damping: 30 }}
      className="flex flex-col gap-2"
    >
      <div className="flex items-center gap-2">
        <span className="flex h-4 w-4 items-center justify-center rounded-full border border-white/15 font-mono text-[8px] text-emerald-400">
          Q
        </span>
        <span className="font-mono text-[10px] uppercase tracking-wider text-zinc-500">
          IQ ANALYZER · QWEN2.5 · {message.timestamp.slice(11, 19)}
        </span>
      </div>

      <div className="rounded-2xl rounded-tl-sm border border-white/[0.07] bg-white/[0.012] p-4 backdrop-blur-md">
        <pre
          className={cn(
            "whitespace-pre-wrap break-words font-mono text-[12.5px] leading-relaxed text-zinc-100",
          )}
        >
          {visible}
        </pre>

        {streamDone && (message.sqlInspection || message.dataArtifact || message.lineage || message.meta) && (
          <motion.div
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ type: "spring", stiffness: 260, damping: 26 }}
            className="mt-3.5 flex flex-col gap-3"
          >
            {message.dataArtifact && <DataArtifact artifact={message.dataArtifact} />}
            {message.lineage && <LineagePanel lineage={message.lineage} />}
            {message.sqlInspection && <SQLInspector data={message.sqlInspection} />}
            {message.meta && (
              <div className="flex flex-col gap-1.5 border-t border-white/[0.06] pt-2.5 font-mono text-[10px] leading-relaxed text-zinc-600">
                {message.meta.sources && message.meta.sources.length > 0 && (
                  <div className="flex flex-wrap gap-1">
                    {message.meta.sources.map((source) => (
                      <span
                        key={source}
                        className="rounded-full border border-white/10 bg-white/[0.02] px-2 py-0.5 text-[9px] text-zinc-500"
                      >
                        {source}
                      </span>
                    ))}
                  </div>
                )}
                <span>{message.meta.disclaimer}</span>
                <span className="tabular-nums">
                  Fecha de corte: {formatUtcStamp(new Date(message.meta.cutoffDate))}
                </span>
              </div>
            )}
          </motion.div>
        )}
      </div>
    </motion.div>
  );
}
