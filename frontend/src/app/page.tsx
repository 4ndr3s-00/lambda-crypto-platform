"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { AnimatePresence, motion } from "framer-motion";
import { ParticleVoid } from "@/components/canvas/ParticleVoid";
import { BrandMark } from "@/components/chrome/BrandMark";
import { FloatingTicker } from "@/components/chrome/FloatingTicker";
import { ResetButton } from "@/components/chrome/ResetButton";
import { MessageStream } from "@/components/chat/MessageStream";
import { FloatingConsole } from "@/components/console/FloatingConsole";
import { QuickPrompts } from "@/components/console/QuickPrompts";
import { DashboardView } from "@/components/dashboard/DashboardView";
import type {
  ChatMessage,
  DashboardInterval,
  MarketTicker,
  ViewMode,
} from "@/lib/types";
import { QUICK_PROMPTS, mockApi } from "@/lib/mockData";
import { cn } from "@/lib/utils";

const CAPSULE_SPRING = {
  type: "spring" as const,
  stiffness: 280,
  damping: 28,
  mass: 0.8,
};

const TRACK_SPRING = {
  type: "spring" as const,
  stiffness: 220,
  damping: 26,
};

export default function Home() {
  const [view, setView] = useState<ViewMode>("CHAT_CONSOLE");
  const [interval, setInterval] = useState<DashboardInterval>("1d");

  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [isGenerating, setIsGenerating] = useState(false);
  const [tickers, setTickers] = useState<MarketTicker[]>([]);

  const generatingRef = useRef(false);
  const timersRef = useRef<number[]>([]);
  const generationRef = useRef(0);

  const hasMessages = messages.length > 0;
  const showHome = !hasMessages;

  /* Live market ribbon ------------------------------------------------ */
  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const overview = await mockApi.getOverview();
        if (cancelled) return;
        setTickers(
          overview.assets
            .filter((asset) => asset.isLive)
            .map((asset) => ({
              symbol: asset.base,
              pair: `${asset.base}/USDT`,
              price: asset.price,
              changePercent: asset.change24h,
            })),
        );
      } catch {
        // Sin exchange no mostramos precios inventados en el ribbon.
      }
    };
    void load();
    const intervalId = window.setInterval(load, 5000);
    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, []);

  /* Timer cleanup ------------------------------------------------------ */
  useEffect(() => {
    const timers = timersRef.current;
    return () => {
      for (const timer of timers) window.clearTimeout(timer);
    };
  }, []);

  /* Query execution ---------------------------------------------------- */
  const runQuery = useCallback((raw: string) => {
    const query = raw.trim();
    if (query.length === 0 || generatingRef.current) return;

    generatingRef.current = true;
    setIsGenerating(true);
    setInput("");

    const userMessage: ChatMessage = {
      id: `msg_user_${Date.now()}`,
      role: "user",
      timestamp: new Date().toISOString(),
      content: query,
    };
    setMessages((prev) => [...prev, userMessage]);

    const token = generationRef.current;
    const thinkMs = 620 + Math.random() * 540;

    const timer = window.setTimeout(() => {
      void mockApi
        .postChat({ query })
        .then(({ message }) => {
          if (generationRef.current !== token) return;
          setMessages((prev) => [...prev, message]);
          setIsGenerating(false);
          generatingRef.current = false;
        })
        .catch(() => {
          if (generationRef.current !== token) return;
          setIsGenerating(false);
          generatingRef.current = false;
        });
    }, thinkMs);

    timersRef.current.push(timer);
  }, []);

  const handleConsoleSubmit = useCallback(() => {
    runQuery(input);
  }, [input, runQuery]);

  const handlePromptSelect = useCallback(
    (query: string) => {
      runQuery(query);
    },
    [runQuery],
  );

  /* Reset: clear the thread, empty the input, spring the capsule back home */
  const resetChat = useCallback(() => {
    generationRef.current += 1;
    for (const timer of timersRef.current) window.clearTimeout(timer);
    timersRef.current = [];
    generatingRef.current = false;
    setIsGenerating(false);
    setMessages([]);
    setInput("");
  }, []);

  return (
    <div className="relative h-screen w-screen overflow-hidden bg-black">
      <motion.div
        className="flex h-screen w-[200vw] will-change-transform"
        animate={{ x: view === "CHAT_CONSOLE" ? "0vw" : "-100vw" }}
        transition={TRACK_SPRING}
      >
        {/* ---------------------------------------------------------- */}
        {/* VIEW 1 — AI CONSOLE                                        */}
        {/* ---------------------------------------------------------- */}
        <section className="relative flex h-screen w-screen shrink-0 flex-col overflow-hidden">
          <ParticleVoid active={view === "CHAT_CONSOLE"} />

          <BrandMark onReset={resetChat} />
          <FloatingTicker tickers={tickers} />

          <div className="pointer-events-none absolute right-4 top-4 z-40 flex items-center gap-2 md:right-6">
            <ResetButton visible={hasMessages} onClick={resetChat} />
            <button
              type="button"
              onClick={() => setView("STREAMING_DASHBOARD")}
              className="pointer-events-auto whitespace-nowrap rounded-full border border-white/10 px-3 py-1.5 font-mono text-[10px] tracking-wide text-zinc-300 transition-colors duration-200 hover:border-white/25 hover:text-white active:scale-[0.98] sm:text-[11px]"
            >
              <span className="hidden sm:inline"> Dashboard en Vivo →</span>
              <span className="sm:hidden"> Dashboard →</span>
            </button>
          </div>

          <main className="pointer-events-none relative z-10 flex min-h-0 flex-1 flex-col">
            <AnimatePresence>
              {hasMessages && (
                <motion.div
                  key="stream"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0, transition: { duration: 0.18 } }}
                  className="flex min-h-0 flex-1 flex-col"
                >
                  <MessageStream messages={messages} busy={isGenerating} />
                </motion.div>
              )}
            </AnimatePresence>
          </main>

          {/* Content fades under the floating chrome while scrolling. */}
          {hasMessages && (
            <div
              aria-hidden="true"
              className="pointer-events-none absolute inset-x-0 top-0 z-20 h-32 bg-gradient-to-b from-black from-30% via-black/90 via-65% to-transparent md:h-24"
            />
          )}

          {/* Two-state capsule: dead-center at home, docked at bottom in chat. */}
          <div className="pointer-events-none absolute inset-0 z-20 flex flex-col items-center">
            <motion.div
              layout
              transition={CAPSULE_SPRING}
              className={cn(
                "pointer-events-none flex w-full flex-col items-center px-4",
                showHome ? "my-auto" : "mt-auto pb-6",
              )}
            >
              <AnimatePresence>
                {showHome && (
                  <motion.h1
                    key="title"
                    initial={{ opacity: 0, y: 10 }}
                    animate={{ opacity: 1, y: 0 }}
                    exit={{ opacity: 0, y: -10, transition: { duration: 0.16 } }}
                    transition={{ type: "spring", stiffness: 300, damping: 30 }}
                    className="mb-6 select-none font-sans text-4xl font-medium tracking-tight text-white lg:text-5xl"
                  >
                    IQ ANALYZER
                  </motion.h1>
                )}
              </AnimatePresence>

              <FloatingConsole
                value={input}
                onChange={setInput}
                onSubmit={handleConsoleSubmit}
                isGenerating={isGenerating}
              />

              <AnimatePresence>
                {showHome && input.trim().length === 0 && (
                  <QuickPrompts
                    key="prompts"
                    prompts={QUICK_PROMPTS}
                    onSelect={handlePromptSelect}
                    disabled={isGenerating}
                  />
                )}
              </AnimatePresence>
            </motion.div>
          </div>
        </section>

        {/* ---------------------------------------------------------- */}
        {/* VIEW 2 — STREAMING ANALYTICS DASHBOARD                     */}
        {/* ---------------------------------------------------------- */}
        <section className="h-screen w-screen shrink-0 overflow-hidden bg-black">
          <DashboardView
            interval={interval}
            onIntervalChange={setInterval}
            onBack={() => setView("CHAT_CONSOLE")}
          />
        </section>
      </motion.div>
    </div>
  );
}
