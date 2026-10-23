export function cn(...classes: Array<string | false | null | undefined>): string {
  return classes.filter(Boolean).join(" ");
}

export function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

export function formatNumber(value: number, digits = 2): string {
  if (!Number.isFinite(value)) return "—";
  return value.toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

export function formatUsd(value: number): string {
  if (!Number.isFinite(value)) return "—";
  return `$${formatNumber(value, value >= 1000 ? 0 : 2)}`;
}

export function formatPercent(value: number): string {
  if (!Number.isFinite(value)) return "—";
  const sign = value >= 0 ? "+" : "";
  return `${sign}${value.toFixed(2)}%`;
}

export function formatCompact(value: number): string {
  if (!Number.isFinite(value)) return "—";
  return new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 2 }).format(value);
}

/** Colombia (America/Bogota) is a fixed UTC-5 offset, no daylight saving — a plain shift is exact. */
const BOGOTA_OFFSET_MS = -5 * 60 * 60 * 1000;

/** Shifts a Date so its `getUTC*` getters read as Bogota wall-clock time. */
export function toBogota(date: Date): Date {
  return new Date(date.getTime() + BOGOTA_OFFSET_MS);
}

/** `YYYY-MM-DD HH:MM:SS UTC`, used for data cutoffs (the source stores UTC). */
export function formatUtcStamp(date: Date): string {
  return `${date.toISOString().slice(0, 19).replace("T", " ")} UTC`;
}

/** `YYYY-MM-DD HH:MM:SS COT`, only for intraday wall-clock display. */
export function formatBogotaStamp(date: Date): string {
  return `${toBogota(date).toISOString().slice(0, 19).replace("T", " ")} COT`;
}
