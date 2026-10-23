"use client";

import { Fragment } from "react";
import type { LineageInfo } from "@/lib/types";

interface LineagePanelProps {
  lineage: LineageInfo;
}

/**
 * Steward view: the full provenance chain, ownership and schema contract.
 * Hairline surfaces only — no heavy panel, no decorative chrome.
 */
export function LineagePanel({ lineage }: LineagePanelProps) {
  return (
    <div className="rounded-xl border border-white/[0.08] bg-white/[0.015] p-3.5">
      <div className="mb-2.5 flex items-center justify-between gap-3">
        <span className="font-mono text-[11px] uppercase tracking-wider text-zinc-400">
          Linaje & gobierno del dato
        </span>
        <span className="rounded-full border border-white/10 px-2 py-0.5 font-mono text-[10px] text-zinc-500">
          {lineage.schemaVersion}
        </span>
      </div>

      <div className="flex flex-wrap items-center gap-x-2 gap-y-1 font-mono text-[10px] text-zinc-400">
        {lineage.chain.map((step, index) => (
          <Fragment key={step}>
            {index > 0 && <span className="text-zinc-700">→</span>}
            <span className="rounded border border-white/[0.06] bg-white/[0.02] px-2 py-1">{step}</span>
          </Fragment>
        ))}
      </div>

      <div className="mt-3 grid grid-cols-1 gap-px overflow-hidden rounded-lg border border-white/[0.06] bg-white/[0.04] sm:grid-cols-2">
        <Field label="Dataset" value={lineage.dataset} mono />
        <Field label="Owner" value={lineage.owner} />
        <Field label="Schema version" value={lineage.schemaVersion} mono />
        <Field label="Retención" value={lineage.retention} />
        <Field label="Particiones" value={lineage.partitions} mono />
        <Field label="Contrato" value="Idempotente · deduplicado · event-time UTC" />
      </div>
    </div>
  );
}

function Field({ label, value, mono = false }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="flex flex-col gap-0.5 bg-zinc-950/60 px-3 py-2">
      <span className="font-mono text-[9px] uppercase tracking-wider text-zinc-600">{label}</span>
      <span className={mono ? "break-words font-mono text-[11px] text-zinc-300" : "text-[11px] text-zinc-300"}>
        {value}
      </span>
    </div>
  );
}
