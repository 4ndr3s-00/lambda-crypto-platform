"use client";

import { useCallback, useEffect, useState, type ReactNode } from "react";
import { AnimatePresence, motion } from "framer-motion";
import { BlurText } from "@/components/slides/BlurText";
import { cn } from "@/lib/utils";

const TOTAL = 13;

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

function Role({ name, area }: { name: string; area: string }) {
  return (
    <div className="rounded-xl border border-white/10 bg-white/[0.02] px-3 py-2">
      <p className="font-mono text-[8px] uppercase tracking-wider text-zinc-600">{area}</p>
      <p className="mt-1 font-mono text-[10px] text-zinc-300">{name}</p>
    </div>
  );
}

function SlideOne() {
  return (
    <div className="flex h-full flex-col justify-between">
      <div className="pt-10 md:pt-16">
        <Reveal>
          <Kicker>ARQUITECTURA LAMBDA DISTRIBUIDA A GRAN ESCALA</Kicker>
        </Reveal>
        <h1 className="mt-5 max-w-6xl text-[clamp(3.4rem,9vw,9.5rem)] font-medium leading-[0.82] tracking-[-0.08em] text-white">
          <BlurText text="Plataforma de Datos" effect="blur" animateBy="words" delay={100} stepDuration={0.65} />
          <span className="block text-zinc-500">Cripto & Finanzas</span>
        </h1>
        <Reveal delay={0.25}>
          <p className="mt-6 max-w-2xl text-[clamp(1rem,1.7vw,1.4rem)] leading-snug text-zinc-400">
            Lambda pura. Batch para precisión, Speed para latencia y Serving para una sola verdad.
          </p>
        </Reveal>
      </div>

      <div className="grid grid-cols-2 gap-2 pb-7 md:grid-cols-4 xl:grid-cols-7">
        {[
          ["DEVOPS & SERVING API", "Andrés Teherán"],
          ["STREAMING & KAFKA", "Samuel Benavidez"],
          ["STORAGE & CATALOG", "Samuel Roncancio"],
          ["DISTRIBUTED COMPUTE", "Andrés Blanco"],
          ["ORCHESTRATION & UI", "Camilo"],
          ["DATA GOVERNANCE", "Diego"],
          ["ASISTENTE & RAG", "Sebastián"],
        ].map(([area, name], index) => (
          <Reveal key={area} delay={0.35 + index * 0.05}>
            <Role area={area} name={name} />
          </Reveal>
        ))}
      </div>
    </div>
  );
}

function SlideTwo() {
  return (
    <div className="flex h-full flex-col justify-center">
      <Kicker>01 · DECISIÓN ARQUITECTÓNICA</Kicker>
      <h2 className="mt-4 max-w-5xl text-[clamp(2.4rem,5vw,5rem)] font-medium leading-[0.92] tracking-[-0.06em] text-white">
        Lambda pura
        <span className="block text-zinc-500">vs. Medallion.</span>
      </h2>

      <div className="mt-9 grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Reveal delay={0.15}>
          <Panel className="border-emerald-400/25 bg-emerald-400/[0.04]">
            <p className="font-mono text-[11px] uppercase tracking-wider text-emerald-300">Nuestro enfoque</p>
            <div className="mt-5 space-y-4 text-sm">
              {[
                ["Separación operativa", "Batch para precisión, Speed para latencia."],
                ["Almacenamiento", "/historical · /batch-results · /checkpoints."],
                ["Reconciliación", "Resuelta en Serving por event-time."],
                ["Consulta reciente", "Sub-segundo desde memoria/Kafka."],
              ].map(([label, value]) => (
                <div key={label} className="grid grid-cols-[0.8fr_1.2fr] gap-3 border-t border-white/10 pt-3">
                  <span className="font-mono text-[10px] uppercase tracking-wider text-zinc-400">{label}</span>
                  <span className="text-zinc-500">{value}</span>
                </div>
              ))}
            </div>
          </Panel>
        </Reveal>

        <Reveal delay={0.25}>
          <Panel className="opacity-70">
            <p className="font-mono text-[11px] uppercase tracking-wider text-zinc-500">Medallion · descartado</p>
            <div className="mt-5 space-y-4 text-sm">
              {[
                ["Separación", "Progresiva por refinamiento."],
                ["Almacenamiento", "Bronze · Silver · Gold."],
                ["Reconciliación", "Resuelta en la capa Gold."],
                ["Consulta reciente", "Dependiente del micro-batch."],
              ].map(([label, value]) => (
                <div key={label} className="grid grid-cols-[0.8fr_1.2fr] gap-3 border-t border-white/10 pt-3">
                  <span className="font-mono text-[10px] uppercase tracking-wider text-zinc-600">{label}</span>
                  <span className="text-zinc-600">{value}</span>
                </div>
              ))}
            </div>
          </Panel>
        </Reveal>
      </div>
    </div>
  );
}

function SlideThree() {
  return (
    <div className="flex h-full flex-col justify-center">
      <Kicker>02 · FUNDACIÓN E INFRAESTRUCTURA</Kicker>
      <h2 className="mt-4 text-[clamp(2.4rem,5vw,5rem)] font-medium leading-[0.92] tracking-[-0.06em] text-white">
        Infraestructura
        <span className="block text-zinc-500">resiliente.</span>
      </h2>
      <p className="mt-3 font-mono text-[10px] uppercase tracking-wider text-zinc-500">Ingeniería DevOps por Andrés Teherán</p>

      <div className="mt-8 grid grid-cols-1 gap-4 lg:grid-cols-[0.9fr_1.1fr]">
        <div className="space-y-3">
          {[
            ["Orquestación unificada", "Stack completo con un comando. Contenedores aislados en lambda-net."],
            ["Infraestructura desacoplada", "Kafka KRaft, MinIO, Spark, Postgres y Airflow sin dependencias fatales."],
            ["Benchmarking de estrés", "Picos de 1.000.000 de eventos sin degradación del clúster."],
          ].map(([title, body], index) => (
            <Reveal key={title} delay={0.12 + index * 0.08}>
              <Panel>
                <p className="font-mono text-[11px] uppercase tracking-wider text-zinc-200">{title}</p>
                <p className="mt-2 text-sm leading-relaxed text-zinc-500">{body}</p>
              </Panel>
            </Reveal>
          ))}
        </div>

        <Reveal delay={0.2}>
          <Panel className="flex h-full flex-col justify-between">
            <div className="grid grid-cols-2 gap-3">
              {["MinIO", "Kafka", "Spark", "Airflow", "Postgres", "API"].map((service) => (
                <div key={service} className="rounded-xl border border-white/10 bg-black/30 px-4 py-5 text-center">
                  <p className="font-mono text-[11px] uppercase tracking-wider text-zinc-300">{service}</p>
                </div>
              ))}
            </div>
            <div className="mt-5 flex items-end justify-between border-t border-white/10 pt-4">
              <span className="font-mono text-[9px] uppercase tracking-wider text-zinc-600">Carga validada</span>
              <span className="font-mono text-3xl tracking-tight text-emerald-400">1.000.000</span>
            </div>
          </Panel>
        </Reveal>
      </div>
    </div>
  );
}

function SlideFour() {
  return (
    <div className="flex h-full flex-col justify-center">
      <Kicker>03 · INGESTA Y BUS DE MENSAJERÍA</Kicker>
      <h2 className="mt-4 text-[clamp(2.4rem,5vw,5rem)] font-medium leading-[0.92] tracking-[-0.06em] text-white">
        Ingesta
        <span className="block text-zinc-500">resiliente.</span>
      </h2>
      <p className="mt-3 font-mono text-[10px] uppercase tracking-wider text-zinc-500">Streaming por Samuel Benavidez</p>

      <Reveal delay={0.18} className="mt-9">
        <div className="flex flex-col items-stretch gap-2 md:flex-row md:items-center">
          <FlowNode label="REST" detail="Mercado en vivo" />
          <FlowNode label="WebSockets" detail="Trades" />
          <FlowNode label="CSV Loader" detail="Histórico" />
          <Arrow />
          <FlowNode label="Rate limiting" detail="Backoff exponencial" />
          <Arrow />
          <FlowNode label="Kafka KRaft" detail="market.events" accent />
          <Arrow />
          <FlowNode label="DLQ" detail="market.dlq" />
        </div>
      </Reveal>

      <div className="mt-6 grid grid-cols-1 gap-4 md:grid-cols-3">
        {[
          ["Orden", "Partición", "Clave exchange|symbol para conservar secuencia."],
          ["Contrato", "Validación", "El esquema canónico separa lo válido de lo corrupto."],
          ["Aislamiento", "DLQ", "Un evento inválido no detiene el pipeline."],
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

function SlideFive() {
  return (
    <div className="flex h-full flex-col justify-center">
      <Kicker>04 · ALMACENAMIENTO Y CATÁLOGO</Kicker>
      <h2 className="mt-4 text-[clamp(2.4rem,5vw,5rem)] font-medium leading-[0.92] tracking-[-0.06em] text-white">
        Almacenamiento
        <span className="block text-zinc-500">funcional.</span>
      </h2>
      <p className="mt-3 font-mono text-[10px] uppercase tracking-wider text-zinc-500">Arquitectura de Datos por Samuel Roncancio</p>

      <div className="mt-8 grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Reveal delay={0.15}>
          <Panel>
            <p className="font-mono text-[11px] uppercase tracking-wider text-emerald-400">MinIO Data Lake</p>
            <div className="mt-4 space-y-3 font-mono text-[11px] text-zinc-400">
              <p>s3://market-lake/</p>
              <p className="pl-4 text-zinc-500">/historical <span className="text-zinc-700">inmutable</span></p>
              <p className="pl-4 text-zinc-500">/batch-results <span className="text-zinc-700">Parquet</span></p>
              <p>s3://market-checkpoints/ <span className="text-zinc-700">tolerancia a fallos</span></p>
              <p>s3://market-quarantine/ <span className="text-zinc-700">eventos corruptos</span></p>
            </div>
          </Panel>
        </Reveal>

        <Reveal delay={0.25}>
          <Panel>
            <p className="font-mono text-[11px] uppercase tracking-wider text-emerald-400">OpenMetadata & OpenLineage</p>
            <div className="mt-4 space-y-3">
              {[
                ["Esquema válido", "100%"],
                ["Linaje completo", "100%"],
                ["Auditoría automática", "event_id único y no nulo"],
                ["Trazabilidad", "API → consulta"],
              ].map(([label, value]) => (
                <div key={label} className="flex items-center justify-between border-t border-white/10 pt-3">
                  <span className="font-mono text-[10px] uppercase tracking-wider text-zinc-500">{label}</span>
                  <span className="text-sm text-zinc-300">{value}</span>
                </div>
              ))}
            </div>
          </Panel>
        </Reveal>
      </div>
    </div>
  );
}

function SlideSix() {
  return (
    <div className="flex h-full flex-col justify-center">
      <Kicker>05 · SPEED LAYER</Kicker>
      <h2 className="mt-4 text-[clamp(2.4rem,5vw,5rem)] font-medium leading-[0.92] tracking-[-0.06em] text-white">
        Lo que está pasando
        <span className="block text-emerald-400">ahora.</span>
      </h2>
      <p className="mt-3 font-mono text-[10px] uppercase tracking-wider text-zinc-500">Spark Structured Streaming por Andrés Blanco</p>

      <Reveal delay={0.18} className="mt-9">
        <div className="flex flex-col items-stretch gap-2 md:flex-row md:items-center">
          <FlowNode label="Kafka" detail="market.events" />
          <Arrow />
          <FlowNode label="Spark Streaming" detail="event-time · watermark" accent />
          <Arrow />
          <FlowNode label="Ventanas" detail="5 min deslizantes" />
          <Arrow />
          <FlowNode label="Postgres" detail="serving.realtime_metrics" />
        </div>
      </Reveal>

      <div className="mt-6 grid grid-cols-1 gap-4 md:grid-cols-3">
        {[
          ["Baja latencia", "Consumo continuo desde Kafka; métricas provisionales en memoria."],
          ["Event-time windows", "La ventana refleja el mercado, no la latencia de red."],
          ["Gestión de estado", "Watermark descarta datos tardíos y evita desbordar RAM."],
        ].map(([title, body], index) => (
          <Reveal key={title} delay={0.3 + index * 0.08}>
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

function SlideSeven() {
  return (
    <div className="flex h-full flex-col justify-center">
      <Kicker>06 · BATCH LAYER</Kicker>
      <h2 className="mt-4 text-[clamp(2.4rem,5vw,5rem)] font-medium leading-[0.92] tracking-[-0.06em] text-white">
        La memoria que
        <span className="block text-zinc-500">no se pierde.</span>
      </h2>
      <p className="mt-3 font-mono text-[10px] uppercase tracking-wider text-zinc-500">Spark Batch por Andrés Blanco</p>

      <Reveal delay={0.18} className="mt-9">
        <div className="flex flex-col items-stretch gap-2 md:flex-row md:items-center">
          <FlowNode label="Millones de eventos" detail="histórico crudo" />
          <Arrow />
          <FlowNode label="Deduplicación" detail="event_id + event_time" />
          <Arrow />
          <FlowNode label="Agregación" detail="OHLCV + retornos" accent />
          <Arrow />
          <FlowNode label="Parquet" detail="batch-results" />
        </div>
      </Reveal>

      <div className="mt-6 grid grid-cols-1 gap-4 md:grid-cols-3">
        {[
          ["Fuente de verdad", "Procesamiento masivo e inmutable de s3://market-lake/historical/."],
          ["Agregación financiera", "Cálculo preciso de velas OHLCV y retornos porcentuales."],
          ["Idempotencia estricta", "Recalcular meses sobrescribe particiones sin duplicar."],
        ].map(([title, body], index) => (
          <Reveal key={title} delay={0.3 + index * 0.08}>
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

function SlideEight() {
  return (
    <div className="flex h-full flex-col justify-center">
      <Kicker>07 · ORQUESTACIÓN</Kicker>
      <h2 className="mt-4 text-[clamp(2.4rem,5vw,5rem)] font-medium leading-[0.92] tracking-[-0.06em] text-white">
        Flujos
        <span className="block text-zinc-500">automatizados.</span>
      </h2>
      <p className="mt-3 font-mono text-[10px] uppercase tracking-wider text-zinc-500">Apache Airflow DAGs por Camilo</p>

      <div className="mt-8 grid grid-cols-1 gap-3 md:grid-cols-2">
        {[
          ["ingest_historical", "Extracción periódica de mercado hacia MinIO."],
          ["batch_recompute", "Recálculo Spark Batch. Depende de la ingesta."],
          ["compact_files", "Fusión semanal de small files Parquet."],
          ["replay_dlq", "Reinyección manual de eventos corregidos."],
        ].map(([name, body], index) => (
          <Reveal key={name} delay={0.12 + index * 0.08}>
            <Panel className={index === 3 ? "border-rose-400/20 bg-rose-400/[0.035]" : ""}>
              <p className="font-mono text-[11px] text-emerald-300">{name}</p>
              <p className="mt-2 text-sm leading-relaxed text-zinc-500">{body}</p>
            </Panel>
          </Reveal>
        ))}
      </div>
    </div>
  );
}

function SlideNine() {
  return (
    <div className="flex h-full flex-col justify-center">
      <Kicker>08 · SERVING LAYER</Kicker>
      <h2 className="mt-4 text-[clamp(2.4rem,5vw,5rem)] font-medium leading-[0.92] tracking-[-0.06em] text-white">
        Query final =
        <span className="block text-zinc-500">Batch + Speed.</span>
      </h2>
      <p className="mt-3 font-mono text-[10px] uppercase tracking-wider text-zinc-500">FastAPI Backend por Andrés Teherán</p>

      <Reveal delay={0.18} className="mt-8">
        <Panel>
          <div className="flex items-center gap-3">
            <div className="h-3 flex-1 rounded-full bg-zinc-700" />
            <div className="h-3 flex-1 rounded-full bg-emerald-400/70" />
          </div>
          <div className="mt-2 flex justify-between font-mono text-[9px] uppercase tracking-wider text-zinc-600">
            <span>Batch · histórico</span>
            <span>Speed · provisional</span>
          </div>
          <p className="mt-4 text-center font-mono text-[10px] text-emerald-400">batch_cutoff_timestamp</p>
        </Panel>
      </Reveal>

      <div className="mt-5 grid grid-cols-1 gap-4 md:grid-cols-2">
        {[
          ["Regla de precedencia", "Si event_time <= cutoff, Batch reemplaza al Speed."],
          ["Cero doble conteo", "Deduplicación por UUID y cierre del último job."],
          ["Postgres", "serving.ohlcv_reconciled como una sola versión."],
          ["API", "source, frescura y cutoff visibles para el consumidor."],
        ].map(([title, body], index) => (
          <Reveal key={title} delay={0.3 + index * 0.07}>
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

function SlideTen() {
  return (
    <div className="flex h-full flex-col justify-center">
      <Kicker>09 · GOBIERNO Y TAXONOMÍA</Kicker>
      <h2 className="mt-4 text-[clamp(2.4rem,5vw,5rem)] font-medium leading-[0.92] tracking-[-0.06em] text-white">
        Metadatos con
        <span className="block text-zinc-500">significado.</span>
      </h2>
      <p className="mt-3 font-mono text-[10px] uppercase tracking-wider text-zinc-500">Apache Atlas por Diego</p>

      <div className="mt-8 grid grid-cols-1 gap-4 lg:grid-cols-[0.8fr_1.2fr]">
        <div className="space-y-3">
          {["market.ohlcv", "event_time", "spark.watermark"].map((item, index) => (
            <Reveal key={item} delay={0.12 + index * 0.08}>
              <Panel className="py-3">
                <p className="font-mono text-[11px] text-zinc-300">{item}</p>
              </Panel>
            </Reveal>
          ))}
        </div>
        <div className="space-y-3">
          {[
            ["Datos públicos de mercado", "Clasificación"],
            ["Precio de cierre", "Término de negocio"],
            ["Latencia permitida", "Regla operativa"],
          ].map(([title, subtitle], index) => (
            <Reveal key={title} delay={0.18 + index * 0.08}>
              <Panel>
                <p className="text-sm text-zinc-300">{title}</p>
                <p className="mt-1 font-mono text-[9px] uppercase tracking-wider text-zinc-600">{subtitle}</p>
              </Panel>
            </Reveal>
          ))}
        </div>
      </div>

      <Reveal delay={0.45}>
        <p className="mt-5 font-mono text-[9px] leading-relaxed text-zinc-600">
          OpenMetadata para auditoría técnica; Atlas para cumplimiento y reglas de negocio.
        </p>
      </Reveal>
    </div>
  );
}

function SlideEleven() {
  return (
    <div className="flex h-full flex-col justify-center">
      <Kicker>10 · ASISTENTE ANALÍTICO</Kicker>
      <h2 className="mt-4 max-w-5xl text-[clamp(2.4rem,5vw,5rem)] font-medium leading-[0.92] tracking-[-0.06em] text-white">
        No es un chatbot.
        <span className="block text-zinc-500">Es una cadena de confianza.</span>
      </h2>
      <p className="mt-3 font-mono text-[10px] uppercase tracking-wider text-zinc-500">Groq + RAG por Sebastián</p>

      <div className="mt-7 grid grid-cols-1 gap-3 md:grid-cols-3">
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
          <Reveal key={item.n} delay={0.1 + index * 0.08}>
            <Panel
              className={cn(
                "h-full min-h-[230px]",
                item.accent && "border-emerald-400/25 bg-emerald-400/[0.045]",
              )}
            >
              <div className="flex items-center justify-between">
                <span className="font-mono text-[11px] text-emerald-400">{item.n}</span>
                <span className="font-mono text-[9px] uppercase tracking-wider text-zinc-600">{item.detail}</span>
              </div>
              <p className="mt-5 text-[1.1rem] leading-snug text-zinc-100">{item.title}</p>
              <p className="mt-3 text-sm leading-relaxed text-zinc-500">{item.body}</p>
            </Panel>
          </Reveal>
        ))}
      </div>

      <Reveal delay={0.45}>
        <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-3">
          {[
            ["Prompt", "Comportamiento"],
            ["RAG", "Evidencia"],
            ["Guardrails", "Límites"],
          ].map(([title, body]) => (
            <div key={title} className="border-l border-white/10 pl-4">
              <p className="font-mono text-[10px] uppercase tracking-wider text-zinc-300">{title}</p>
              <p className="mt-1 text-sm text-zinc-600">{body}</p>
            </div>
          ))}
        </div>
      </Reveal>
    </div>
  );
}

function SlideTwelve() {
  return (
    <div className="flex h-full flex-col justify-center">
      <Kicker>11 · SÍNTESIS</Kicker>
      <h2 className="mt-4 max-w-5xl text-[clamp(2.4rem,5vw,5rem)] font-medium leading-[0.92] tracking-[-0.06em] text-white">
        7 ingenieros.
        <span className="block text-zinc-500">1 arquitectura Lambda pura.</span>
      </h2>

      <div className="mt-8 grid grid-cols-1 gap-3 md:grid-cols-2">
        {[
          ["Samuel Benavidez", "Ingesta resiliente → Kafka"],
          ["Andrés Blanco", "Spark Speed + Batch"],
          ["Samuel Roncancio", "MinIO + catálogo"],
          ["Camilo", "Airflow + orquestación"],
          ["Andrés Teherán", "API + Serving"],
          ["Diego", "Gobierno + Atlas"],
          ["Sebastián", "Asistente + RAG"],
        ].map(([name, role], index) => (
          <Reveal key={name} delay={0.1 + index * 0.06}>
            <div className="flex items-center justify-between border-b border-white/10 py-3">
              <span className="font-mono text-[11px] text-zinc-300">{name}</span>
              <span className="font-mono text-[9px] uppercase tracking-wider text-zinc-600">{role}</span>
            </div>
          </Reveal>
        ))}
      </div>

      <Reveal delay={0.55}>
        <p className="mt-6 max-w-4xl text-sm leading-relaxed text-zinc-500">
          La ingesta de Samuel B. alimenta el procesamiento de Andrés B. Los DAGs de Camilo
          orquestan el almacenamiento de Samuel R. La API de Andrés T. reconcilia los mundos,
          gobernados por las reglas de Diego, permitiendo al asistente de Sebastián consultar
          la verdad sin riesgos.
        </p>
      </Reveal>
    </div>
  );
}

function SlideThirteen() {
  return (
    <div className="flex h-full flex-col justify-between">
      <div className="pt-14 md:pt-20">
        <Kicker>12 · CÓDIGO ABIERTO Y DESPLIEGUE</Kicker>
        <h2 className="mt-5 max-w-6xl text-[clamp(3rem,7vw,7.5rem)] font-medium leading-[0.84] tracking-[-0.07em] text-white">
          Reproducible
          <span className="block text-zinc-500">desde el primer comando.</span>
        </h2>
      </div>

      <div className="grid grid-cols-1 gap-4 pb-10 lg:grid-cols-[1.1fr_0.9fr]">
        <Reveal delay={0.2}>
          <Panel>
            <p className="font-mono text-[9px] uppercase tracking-wider text-zinc-600">Clonar</p>
            <p className="mt-3 break-all font-mono text-[11px] text-emerald-300">
              git clone https://github.com/4ndr3s-00/lambda-crypto-platform.git
            </p>
            <p className="mt-4 font-mono text-[9px] uppercase tracking-wider text-zinc-600">Levantar</p>
            <p className="mt-2 font-mono text-[11px] text-zinc-300">
              docker compose -f infra/docker-compose.yml up -d
            </p>
          </Panel>
        </Reveal>

        <div className="grid grid-cols-1 gap-3">
          {[
            ["MIT", "Arquitectura documentada"],
            ["Docker", "Despliegue local completo"],
            ["33 HU", "Pipeline financiero resiliente"],
          ].map(([title, body], index) => (
            <Reveal key={title} delay={0.3 + index * 0.08}>
              <Panel className="py-4">
                <p className="font-mono text-[11px] uppercase tracking-wider text-emerald-400">{title}</p>
                <p className="mt-1 text-sm text-zinc-500">{body}</p>
              </Panel>
            </Reveal>
          ))}
        </div>
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
    case 9:
      return <SlideTen />;
    case 10:
      return <SlideEleven />;
    case 11:
      return <SlideTwelve />;
    default:
      return <SlideThirteen />;
  }
}

export default function SlidesPage() {
  const [index, setIndex] = useState(0);
  const [direction, setDirection] = useState(1);

  const go = useCallback((delta: number) => {
    setDirection(delta);
    setIndex((current) => Math.max(0, Math.min(TOTAL - 1, current + delta)));
  }, []);

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
