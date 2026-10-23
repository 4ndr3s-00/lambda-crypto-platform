"use client";

interface BrandMarkProps {
  onReset: () => void;
}

/**
 * Top-left pure typographic wordmark. No dot, no badge, no border —
 * clicking it is the primary reset gesture.
 */
export function BrandMark({ onReset }: BrandMarkProps) {
  return (
    <button
      type="button"
      onClick={onReset}
      aria-label="IQ ANALYZER — volver al inicio"
      className="pointer-events-auto absolute left-4 top-4 z-40 cursor-pointer font-mono text-xs font-semibold tracking-[0.2em] text-zinc-300 transition-colors duration-200 hover:text-white md:left-6"
    >
      IQ ANALYZER
    </button>
  );
}
