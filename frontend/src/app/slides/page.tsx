"use client";

import { useCallback, useEffect, useState, type ReactNode } from "react";
import { AnimatePresence, motion } from "framer-motion";
import { BlurText } from "@/components/slides/BlurText";
import { cn } from "@/lib/utils";

const TOTAL = 10;

const slideVariants = {
  enter: (direction: number) => ({
    x: direction > 0 ? 90 : -90,
    opacity: 0,
    filter: "blur(16px)",
  }),
  center: {
    x: 0,
    opacity: 1,
    filter: "blur(0px)",
  },
  exit: (direction: number) => ({
    x: direction > 0 ? -90 : 90,
    opacity: 0,
    filter: "blur(16px)",
  }),
};

function Reveal({
  children,
  delay = 0,
  className,
}: {
  children: ReactNode;
  delay?: number;
  className?: string;
}) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay, duration: 0.65, ease: [0.22, 1, 0.36, 1] }}
      className={className}
    >
      {children}
    </motion.div>
  );
}

function Kicker({ children }: { children: ReactNode }) {
  return (
    <p className="font-mono text-[10px] uppercase tracking-[0.28em] text-emerald-400/80">
      {children}
    </p>
  );
}

function Panel({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "rounded-2xl border border-white/10 bg-white/[0.025] p-5 backdrop-blur-md",
        className,
      )}
    >
      {children}
    </div>
  );
}

function FlowNode({
  label,
  detail,
  accent = false,
}: {
  label: string;
  detail: string;
  accent?: boolean;
}) {
  return (
    <div
      className={cn(
        "min-w-0 flex-1 rounded-xl border px-4 py-3",
        accent
          ? "border-emerald-400/30 bg-emerald-400/[0.07]"
          : "border-white/10 bg-white/[0.02]",
      )}
    >
      <p className={cn("font-mono text-[10px] uppercase tracking-wider", accent ? "text-emerald-300" : "text-zinc-300")}>
        {label}
      </p>
      <p className="mt-1 font-mono text-[9px] leading-relaxed text-zinc-600">{detail}</p>
    </div>
  );
}

function Arrow() {
  return <span className="hidden shrink-0 font-mono text-lg text-zinc-700 md:block">→</span>;
}

function SlideOne() {
  return (
    <div className="flex h-full flex-col justify-between">
      <div className="pt-16 md:pt-24">
        <Reveal>
          <Kicker>LAMBDA CRYPTO PLATFORM · 2026</Kicker>
        </Reveal>
        <h1 className="mt-5 max-w-6xl text-[clamp(4rem,11vw,11rem)] font-medium leading-[0.82] tracking-[-0.08em] text-white">
          <BlurText text="IQ Analyzer" effect="blur" animateBy="words" delay={120} stepDuration={0.7} />
        </h1>
        <Reveal delay={0.25}>
          <p className="mt-7 max-w-2xl text-[clamp(1.1rem,2vw,1.7rem)] leading-snug text-zinc-400">
            Datos que se entienden.
            <span className="block text-zinc-600">Del evento crudo a la decisión, sin perder el contexto.</span>
          </p>
        </Reveal>
      </div>

      <div className="grid grid-cols-1 gap-3 pb-10 md:grid-cols-3">
        {[
          ["01", "Extracción", "Binance → contrato → Kafka"],
          ["02", "Procesamiento", "Speed + Batch + Serving"],
          ["03", "Lectura", "API → dashboard → asistente"],
        ].map(([number, title, detail], index) => (
          <Reveal key={number} delay={0.35 + index * 0.08}>
            <Panel className="flex items-start gap-4">
              <span className="font-mono text-[11px] text-emerald-400">{number}</span>
              <div>
                <p className="font-mono text-[11px] uppercase tracking-wider text-zinc-200">{title}</p>
                <p className="mt-1 font-mono text-[9px] text-zinc-600">{detail}</p>
              </div>
            </Panel>
          </Reveal>
        ))}
      </div>
    </div>
  );
}

function SlideTwo() {
  return (
    <div className="flex h-full flex-col justify-center">
      <Kicker>01 · EL PROBLEMA</Kicker>
      <h2 className="mt-4 max-w-5xl text-[clamp(2.6rem,6vw,6rem)] font-medium leading-[0.92] tracking-[-0.06em] text-white">
        No falta el dato.
        <span className="block text-zinc-500">Falta la lectura.</span>
      </h2>
      <div className="mt-10 grid grid-cols-1 gap-4 md:grid-cols-3">
        {[
          ["Datos en dos velocidades", "El mercado cambia en segundos. El histórico necesita otra escala."],
          ["Contexto disperso", "Kafka, MinIO, Postgres y documentos no hablan solos."],
          ["Respuestas sin evidencia", "Una cifra sin fuente no es información: es ruido."],
        ].map(([title, body], index) => (
          <Reveal key={title} delay={0.1 + index * 0.1}>
            <Panel className="min-h-[170px]">
              <p className="font-mono text-[11px] uppercase tracking-wider text-zinc-200">{title}</p>
              <p className="mt-4 text-sm leading-relaxed text-zinc-500">{body}</p>
            </Panel>
          </Reveal>
        ))}
      </div>
    </div>
  );
}

function SlideThree() {
  return (
    <div className="flex h-full flex-col justify-center">
      <Kicker>02 · ARQUITECTURA</Kicker>
      <h2 className="mt-4 max-w-5xl text-[clamp(2.5rem,5.5vw,5.7rem)] font-medium leading-[0.92] tracking-[-0.06em] text-white">
        Una arquitectura.
        <span className="block text-zinc-500">Dos relojes.</span>
      </h2>

      <Reveal delay={0.2} className="mt-10">
        <div className="flex flex-col items-stretch gap-2 md:flex-row md:items-center">
          <FlowNode label="Fuente" detail="Binance REST / WebSocket" />
          <Arrow />
          <FlowNode label="Ingesta" detail="Contrato + validación" />
          <Arrow />
          <FlowNode label="Kafka" detail="market.events" />
          <Arrow />
          <FlowNode label="Speed" detail="Spark Streaming" accent />
          <Arrow />
          <FlowNode label="Serving" detail="Postgres" />
          <Arrow />
          <FlowNode label="Lectura" detail="API + frontend" />
        </div>
      </Reveal>

      <div className="mt-5 grid grid-cols-1 gap-4 md:grid-cols-3">
        {[
          ["Speed", "Lo que está pasando.", "Watermark · DLQ · checkpoints"],
          ["Batch", "La memoria que queda.", "MinIO · OHLCV · retornos"],
          ["Serving", "Una sola versión.", "Batch + Speed · reconciliación"],
        ].map(([title, subtitle, detail], index) => (
          <Reveal key={title} delay={0.35 + index * 0.08}>
            <Panel>
              <p className="font-mono text-[11px] uppercase tracking-wider text-emerald-400">{title}</p>
              <p className="mt-2 text-lg text-zinc-200">{subtitle}</p>
              <p className="mt-2 font-mono text-[9px] text-zinc-600">{detail}</p>
            </Panel>
          </Reveal>
        ))}
      </div>
    </div>
  );
}

function SlideFour() {
  return (
    <div className="flex h-full flex-col justify-center">
      <Kicker>03 · SPEED LAYER</Kicker>
      <h2 className="mt-4 text-[clamp(2.5rem,5.5vw,5.7rem)] font-medium leading-[0.92] tracking-[-0.06em] text-white">
        Lo que está pasando
        <span className="block text-emerald-400">ahora.</span>
      </h2>

      <Reveal delay={0.2} className="mt-10">
        <div className="flex flex-col items-stretch gap-2 md:flex-row md:items-center">
          <FlowNode label="Kafka" detail="market.events" />
          <Arrow />
          <FlowNode label="Spark Structured Streaming" detail="event-time · watermark 2 min" accent />
          <Arrow />
          <FlowNode label="Postgres" detail="serving.realtime_metrics" />
        </div>
      </Reveal>

      <div className="mt-6 grid grid-cols-1 gap-4 md:grid-cols-3">
        {[
          ["02 min", "Watermark", "Tolera eventos fuera de orden sin abrir ventanas eternas."],
          ["DLQ", "Aislamiento", "Un evento inválido no contamina la métrica buena."],
          ["Snapshot", "Alcance", "Guarda el último estado; el historial completo vive en Batch."],
        ].map(([value, title, body], index) => (
          <Reveal key={title} delay={0.3 + index * 0.08}>
            <Panel>
              <p className="font-mono text-2xl tracking-tight text-white">{value}</p>
              <p className="mt-2 font-mono text-[10px] uppercase tracking-wider text-emerald-400">{title}</p>
              <p className="mt-2 text-sm leading-relaxed text-zinc-500">{body}</p>
            </Panel>
          </Reveal>
        ))}
      </div>
    </div>
  );
}

function SlideFive() {
  return (
    <div className="flex h-full flex-col justify-center">
      <Kicker>04 · BATCH LAYER</Kicker>
      <h2 className="mt-4 text-[clamp(2.5rem,5.5vw,5.7rem)] font-medium leading-[0.92] tracking-[-0.06em] text-white">
        La memoria que
        <span className="block text-zinc-500">no se pierde.</span>
      </h2>

      <Reveal delay={0.2} className="mt-10">
        <div className="flex flex-col items-stretch gap-2 md:flex-row md:items-center">
          <FlowNode label="MinIO" detail="historical/source=.../date=..." />
          <Arrow />
          <FlowNode label="Spark Batch" detail="normaliza · deduplica · agrega" accent />
          <Arrow />
          <FlowNode label="MinIO" detail="batch-results/metric=ohlcv" />
          <Arrow />
          <FlowNode label="Airflow" detail="batch_recompute · 02:00 UTC" />
        </div>
      </Reveal>

      <div className="mt-6 grid grid-cols-1 gap-4 md:grid-cols-3">
        {[
          ["OHLCV", "Velas", "Open, high, low, close y volumen por intervalo."],
          ["Retornos", "Continuidad", "Series comparables para análisis y gráficos."],
          ["Idempotencia", "Reproceso", "Volver a correr no duplica: reescribe la partición."],
        ].map(([title, subtitle, body], index) => (
          <Reveal key={title} delay={0.3 + index * 0.08}>
            <Panel>
              <p className="font-mono text-[11px] uppercase tracking-wider text-emerald-400">{title}</p>
              <p className="mt-2 text-lg text-zinc-200">{subtitle}</p>
              <p className="mt-2 text-sm leading-relaxed text-zinc-500">{body}</p>
            </Panel>
          </Reveal>
        ))}
      </div>
    </div>
  );
}

function SlideSix() {
  return (
    <div className="flex h-full flex-col justify-center">
      <Kicker>05 · SERVING</Kicker>
      <h2 className="mt-4 max-w-5xl text-[clamp(2.5rem,5.5vw,5.7rem)] font-medium leading-[0.92] tracking-[-0.06em] text-white">
        Una sola versión
        <span className="block text-zinc-500">de la verdad.</span>
      </h2>

      <Reveal delay={0.2} className="mt-10">
        <div className="flex flex-col items-stretch gap-2 md:flex-row md:items-center">
          <FlowNode label="Batch" detail="batch-results" />
          <span className="hidden font-mono text-lg text-zinc-700 md:block">+</span>
          <FlowNode label="Speed" detail="realtime_metrics" />
          <Arrow />
          <FlowNode label="Reconciliación" detail="precedencia Batch" accent />
          <Arrow />
          <FlowNode label="Postgres" detail="serving.ohlcv_reconciled" />
          <Arrow />
          <FlowNode label="API" detail="fuente + frescura" />
        </div>
      </Reveal>

      <div className="mt-6 grid grid-cols-1 gap-4 md:grid-cols-3">
        {[
          ["Batch manda", "Precedencia", "La ventana cerrada no se reescribe con un snapshot."],
          ["Frescura", "Confianza", "Si el dato está viejo, la API lo dice o usa exchange."],
          ["Trazabilidad", "Auditoría", "source, data_source, is_live y cutoff_time."],
        ].map(([title, subtitle, body], index) => (
          <Reveal key={title} delay={0.3 + index * 0.08}>
            <Panel>
              <p className="font-mono text-[11px] uppercase tracking-wider text-emerald-400">{title}</p>
              <p className="mt-2 text-lg text-zinc-200">{subtitle}</p>
              <p className="mt-2 text-sm leading-relaxed text-zinc-500">{body}</p>
            </Panel>
          </Reveal>
        ))}
      </div>
    </div>
  );
}

function SlideSeven() {
  return (
    <div className="flex h-full flex-col justify-center">
      <Kicker>06 · ASISTENTE ANALÍTICO</Kicker>
      <h2 className="mt-4 max-w-5xl text-[clamp(2.5rem,5.5vw,5.7rem)] font-medium leading-[0.92] tracking-[-0.06em] text-white">
        No es un chatbot.
        <span className="block text-zinc-500">Es una cadena de confianza.</span>
      </h2>

      <div className="mt-9 grid grid-cols-1 gap-4 md:grid-cols-3">
        {[
          {
            n: "01",
            title: "Router semántico local",
            body: "bge-m3 compara la intención. Saludos, identidad o fuera de alcance se resuelven sin gastar tokens.",
            detail: "recepcionista",
          },
          {
            n: "02",
            title: "Retrieval-Augmented Generation (RAG)",
            body: "Recupera chunks documentales relevantes. No mete toda la biblioteca en el prompt: reduce ruido y costo.",
            detail: "bibliotecario",
            accent: true,
          },
          {
            n: "03",
            title: "Groq + herramientas",
            body: "Solo para consultas reales. Pide métricas y velas a la base; no inventa cifras.",
            detail: "analista",
          },
        ].map((item, index) => (
          <Reveal key={item.n} delay={0.12 + index * 0.1}>
            <Panel
              className={cn(
                "h-full min-h-[250px]",
                item.accent && "border-emerald-400/25 bg-emerald-400/[0.045]",
              )}
            >
              <div className="flex items-center justify-between">
                <span className="font-mono text-[11px] text-emerald-400">{item.n}</span>
                <span className="font-mono text-[9px] uppercase tracking-wider text-zinc-600">{item.detail}</span>
              </div>
              <p className="mt-6 text-[1.15rem] leading-snug text-zinc-100">{item.title}</p>
              <p className="mt-3 text-sm leading-relaxed text-zinc-500">{item.body}</p>
            </Panel>
          </Reveal>
        ))}
      </div>

      <Reveal delay={0.5}>
        <p className="mt-5 font-mono text-[10px] leading-relaxed text-zinc-600">
          RAG no es un solo chunk: es recuperación selectiva de contexto. El router no reemplaza al RAG; lo protege.
        </p>
      </Reveal>
    </div>
  );
}

function SlideEight() {
  return (
    <div className="flex h-full flex-col justify-center">
      <Kicker>07 · CONFIANZA</Kicker>
      <h2 className="mt-4 text-[clamp(2.5rem,5.5vw,5.7rem)] font-medium leading-[0.92] tracking-[-0.06em] text-white">
        Por qué no
        <span className="block text-emerald-400">inventa.</span>
      </h2>

      <div className="mt-9 grid grid-cols-1 gap-4 md:grid-cols-4">
        {[
          ["Prompt", "Comportamiento", "Define rol, tono, límites y formato."],
          ["RAG", "Evidencia", "Solo responde con contexto recuperado."],
          ["Herramientas", "Cifras", "Métricas y velas desde la base o exchange."],
          ["Router", "Límites", "Bloquea fuera de alcance antes de gastar."],
        ].map(([title, subtitle, body], index) => (
          <Reveal key={title} delay={0.12 + index * 0.08}>
            <Panel className="min-h-[190px]">
              <p className="font-mono text-[11px] uppercase tracking-wider text-emerald-400">{title}</p>
              <p className="mt-3 text-lg text-zinc-200">{subtitle}</p>
              <p className="mt-3 text-sm leading-relaxed text-zinc-500">{body}</p>
            </Panel>
          </Reveal>
        ))}
      </div>

      <Reveal delay={0.5}>
        <div className="mt-7 border-l border-emerald-400/40 pl-5">
          <p className="text-[clamp(1.2rem,2.4vw,2rem)] leading-snug text-zinc-300">
            Si no hay contexto, no hay respuesta.
          </p>
        </div>
      </Reveal>
    </div>
  );
}

function SlideNine() {
  return (
    <div className="flex h-full flex-col justify-center">
      <Kicker>08 · PRODUCTO</Kicker>
      <h2 className="mt-4 text-[clamp(2.5rem,5.5vw,5.7rem)] font-medium leading-[0.92] tracking-[-0.06em] text-white">
        Lo que se ve.
      </h2>

      <div className="mt-7 grid min-h-0 flex-1 grid-cols-1 gap-5 lg:grid-cols-[0.9fr_1.1fr]">
        <div className="flex flex-col justify-between gap-4">
          {[
            ["100 velas diarias", "Scroll horizontal fluido, fuente Postgres."],
            ["Consulta en lenguaje natural", "Respuesta limpia + gráfico real."],
            ["Trazabilidad visible", "Fuente, frescura y fecha de corte."],
          ].map(([title, body], index) => (
            <Reveal key={title} delay={0.15 + index * 0.08}>
              <div className="border-l border-white/10 pl-4">
                <p className="font-mono text-[11px] uppercase tracking-wider text-zinc-200">{title}</p>
                <p className="mt-1 text-sm text-zinc-500">{body}</p>
              </div>
            </Reveal>
          ))}
        </div>

        <Reveal delay={0.2} className="min-h-0">
          <div className="relative h-full min-h-[260px] overflow-hidden rounded-2xl border border-white/10 bg-zinc-950/60">
            <img
              src="/slides/chart.png"
              alt="Chart de velas de IQ Analyzer"
              className="h-full w-full object-cover object-top opacity-90"
            />
            <div className="pointer-events-none absolute inset-0 bg-gradient-to-t from-black/40 via-transparent to-transparent" />
          </div>
        </Reveal>
      </div>
    </div>
  );
}

function SlideTen() {
  return (
    <div className="flex h-full flex-col justify-between">
      <div className="pt-16 md:pt-24">
        <Kicker>09 · CIERRE</Kicker>
        <h2 className="mt-5 max-w-6xl text-[clamp(3rem,8vw,8rem)] font-medium leading-[0.84] tracking-[-0.07em] text-white">
          Datos que
          <span className="block text-zinc-500">se entienden.</span>
        </h2>
      </div>

      <div className="grid grid-cols-1 gap-3 pb-10 md:grid-cols-3">
        {[
          ["Ahora", "Velas, métricas y asistente en una sola lectura."],
          ["Después", "Backfill completo, más activos y observabilidad."],
          ["Siempre", "Fuente visible. Cifra real. Contexto antes de responder."],
        ].map(([title, body], index) => (
          <Reveal key={title} delay={0.2 + index * 0.08}>
            <Panel>
              <p className="font-mono text-[11px] uppercase tracking-wider text-emerald-400">{title}</p>
              <p className="mt-2 text-sm leading-relaxed text-zinc-500">{body}</p>
            </Panel>
          </Reveal>
        ))}
      </div>
    </div>
  );
}

function renderSlide(index: number) {
  switch (index) {
    case 0:
      return <SlideOne />;
    case 1:
      return <SlideTwo />;
    case 2:
      return <SlideThree />;
    case 3:
      return <SlideFour />;
    case 4:
      return <SlideFive />;
    case 5:
      return <SlideSix />;
    case 6:
      return <SlideSeven />;
    case 7:
      return <SlideEight />;
    case 8:
      return <SlideNine />;
    default:
      return <SlideTen />;
  }
}

export default function SlidesPage() {
  const [index, setIndex] = useState(0);
  const [direction, setDirection] = useState(1);

  const go = useCallback(
    (delta: number) => {
      setDirection(delta);
      setIndex((current) => Math.max(0, Math.min(TOTAL - 1, current + delta)));
    },
    [],
  );

  const goTo = useCallback(
    (next: number) => {
      setDirection(next > index ? 1 : -1);
      setIndex(next);
    },
    [index],
  );

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (["ArrowRight", "PageDown", " "].includes(event.key)) {
        event.preventDefault();
        go(1);
      } else if (["ArrowLeft", "PageUp"].includes(event.key)) {
        event.preventDefault();
        go(-1);
      } else if (event.key === "Home") {
        event.preventDefault();
        goTo(0);
      } else if (event.key === "End") {
        event.preventDefault();
        goTo(TOTAL - 1);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [go, goTo]);

  const progress = ((index + 1) / TOTAL) * 100;

  return (
    <main className="relative h-dvh w-screen overflow-hidden bg-black text-zinc-100">
      {/* Ambient background */}
      <div className="pointer-events-none fixed inset-0">
        <div
          className="absolute inset-0 opacity-[0.55]"
          style={{
            backgroundImage:
              "linear-gradient(rgba(255,255,255,0.035) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,0.035) 1px, transparent 1px)",
            backgroundSize: "64px 64px",
            maskImage: "radial-gradient(ellipse at center, black 20%, transparent 78%)",
          }}
        />
        <motion.div
          className="absolute left-[10%] top-[15%] h-[38rem] w-[38rem] rounded-full bg-emerald-500/[0.08] blur-[120px]"
          animate={{ x: [0, 70, -30, 0], y: [0, -40, 30, 0] }}
          transition={{ duration: 22, repeat: Infinity, ease: "easeInOut" }}
        />
        <motion.div
          className="absolute bottom-[-10%] right-[5%] h-[34rem] w-[34rem] rounded-full bg-cyan-400/[0.05] blur-[110px]"
          animate={{ x: [0, -60, 20, 0], y: [0, 30, -20, 0] }}
          transition={{ duration: 26, repeat: Infinity, ease: "easeInOut" }}
        />
        <div className="absolute inset-0 bg-[radial-gradient(circle_at_50%_0%,rgba(255,255,255,0.04),transparent_55%)]" />
      </div>

      {/* Top bar */}
      <header className="relative z-20 flex items-center justify-between px-5 pt-5 md:px-8">
        <div className="flex items-center gap-3">
          <span className="font-mono text-[10px] font-semibold tracking-[0.22em] text-zinc-300">
            IQ ANALYZER
          </span>
          <span className="hidden font-mono text-[9px] uppercase tracking-[0.18em] text-zinc-700 sm:inline">
            / LAMBDA CRYPTO PLATFORM
          </span>
        </div>
        <div className="flex items-center gap-3 font-mono text-[9px] uppercase tracking-[0.18em] text-zinc-600">
          <span>{String(index + 1).padStart(2, "0")} / {String(TOTAL).padStart(2, "0")}</span>
          <span className="h-1 w-24 overflow-hidden rounded-full bg-white/10">
            <motion.span
              className="block h-full bg-emerald-400"
              animate={{ width: `${progress}%` }}
              transition={{ type: "spring", stiffness: 160, damping: 24 }}
            />
          </span>
        </div>
      </header>

      {/* Slide */}
      <div className="relative z-10 mx-auto h-[calc(100dvh-7.5rem)] w-full max-w-[1500px] px-5 pb-4 pt-6 md:px-8">
        <AnimatePresence mode="wait" custom={direction} initial={false}>
          <motion.section
            key={index}
            custom={direction}
            variants={slideVariants}
            initial="enter"
            animate="center"
            exit="exit"
            transition={{ duration: 0.55, ease: [0.22, 1, 0.36, 1] }}
            className="h-full w-full"
          >
            {renderSlide(index)}
          </motion.section>
        </AnimatePresence>
      </div>

      {/* Bottom navigation */}
      <footer className="relative z-20 flex items-center justify-between px-5 pb-5 md:px-8">
        <div className="flex items-center gap-1.5">
          {Array.from({ length: TOTAL }).map((_, dot) => (
            <button
              key={dot}
              type="button"
              onClick={() => goTo(dot)}
              aria-label={`Ir a la diapositiva ${dot + 1}`}
              className={cn(
                "h-1.5 rounded-full transition-all duration-300",
                dot === index ? "w-8 bg-emerald-400" : "w-1.5 bg-white/15 hover:bg-white/30",
              )}
            />
          ))}
        </div>

        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => go(-1)}
            disabled={index === 0}
            className="rounded-full border border-white/10 px-4 py-2 font-mono text-[9px] uppercase tracking-wider text-zinc-400 transition-colors hover:border-white/25 hover:text-white disabled:cursor-not-allowed disabled:opacity-25"
          >
            ← Anterior
          </button>
          <button
            type="button"
            onClick={() => go(1)}
            disabled={index === TOTAL - 1}
            className="rounded-full border border-emerald-400/30 bg-emerald-400/10 px-4 py-2 font-mono text-[9px] uppercase tracking-wider text-emerald-300 transition-colors hover:bg-emerald-400/15 disabled:cursor-not-allowed disabled:opacity-25"
          >
            Siguiente →
          </button>
        </div>
      </footer>
    </main>
  );
}
