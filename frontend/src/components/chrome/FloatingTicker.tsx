"use client";

import { Fragment } from "react";
import type { MarketTicker } from "@/lib/types";
import { cn, formatNumber, formatPercent } from "@/lib/utils";

interface FloatingTickerProps {
  tickers: MarketTicker[];
}

/**
 * Borderless ticker floating top-center. No container, no blur, no rule line —
 * the values sit directly in the void above the particle field.
 */
export function FloatingTicker({ tickers }: FloatingTickerProps) {
  return (
    <div className="pointer-events-auto absolute left-1/2 top-14 z-30 flex w-full -translate-x-1/2 select-none flex-wrap items-center justify-center gap-x-3 gap-y-1 px-4 font-mono text-[10px] text-zinc-400 md:top-4 md:w-auto md:flex-nowrap md:gap-6 md:px-0 md:text-[11px]">
      {tickers.map((ticker, index) => {
        const up = ticker.changePercent >= 0;
        return (
          <Fragment key={ticker.symbol}>
            {index > 0 && <span className="hidden text-zinc-700 md:inline">/</span>}
            <span className="whitespace-nowrap transition-colors duration-200 hover:text-white">
              {ticker.symbol}{" "}
              <span className="text-white">${formatNumber(ticker.price, 2)}</span>{" "}
              <span className={cn("font-medium", up ? "text-emerald-400" : "text-rose-400")}>
                {formatPercent(ticker.changePercent)}
              </span>
            </span>
          </Fragment>
        );
      })}
    </div>
  );
}
