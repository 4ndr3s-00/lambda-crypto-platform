"use client";

import { AnimatePresence, motion } from "framer-motion";

interface ResetButtonProps {
  visible: boolean;
  onClick: () => void;
}

/** Discreet reset control. Lives inside the chat top-right action group. */
export function ResetButton({ visible, onClick }: ResetButtonProps) {
  return (
    <AnimatePresence>
      {visible && (
        <motion.button
          type="button"
          onClick={onClick}
          initial={{ opacity: 0, y: -6 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -6 }}
          transition={{ type: "spring", stiffness: 360, damping: 30 }}
          className="pointer-events-auto hidden items-center whitespace-nowrap rounded-full border border-white/10 px-3 py-1.5 font-mono text-[10px] tracking-wide text-zinc-400 transition-colors duration-200 hover:border-white/25 hover:text-white active:scale-[0.98] sm:inline-flex md:text-[11px]"
        >
          + Nueva Consulta
        </motion.button>
      )}
    </AnimatePresence>
  );
}
