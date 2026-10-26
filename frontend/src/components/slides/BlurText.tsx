"use client";

import { motion, useReducedMotion } from "framer-motion";
import type { CSSProperties } from "react";

type Effect = "blur" | "zoom" | "tilt" | "focus" | "drift";
type Direction = "top" | "bottom" | "left" | "right";

interface BlurTextProps {
  text: string;
  className?: string;
  effect?: Effect;
  animateBy?: "words" | "letters";
  direction?: Direction;
  delay?: number;
  stepDuration?: number;
  blur?: number;
  distance?: number;
  trigger?: "view" | "load";
  color?: string;
  fromColor?: string;
  style?: CSSProperties;
}

function initialFor(
  effect: Effect,
  direction: Direction,
  blur: number,
  distance: number,
) {
  const axis = direction === "left" || direction === "right" ? "x" : "y";
  const sign = direction === "top" || direction === "left" ? -1 : 1;

  if (effect === "zoom") {
    return { opacity: 0, filter: `blur(${blur * 1.2}px)`, scale: 1.35, [axis]: sign * distance * 0.35 };
  }
  if (effect === "tilt") {
    return {
      opacity: 0,
      filter: `blur(${blur}px)`,
      rotateX: axis === "y" ? sign * 45 : 0,
      rotateY: axis === "x" ? sign * 45 : 0,
      [axis]: sign * distance * 0.5,
    };
  }
  if (effect === "focus") {
    return { opacity: 0.2, filter: `blur(${blur * 1.4}px)`, scale: 1.08 };
  }
  if (effect === "drift") {
    return { opacity: 0, filter: `blur(${blur}px)`, x: sign * distance * 1.4, skewX: sign * 18 };
  }
  return { opacity: 0, filter: `blur(${blur}px)`, [axis]: sign * distance };
}

export function BlurText({
  text,
  className,
  effect = "blur",
  animateBy = "words",
  direction = "top",
  delay = 120,
  stepDuration = 0.55,
  blur = 10,
  distance = 34,
  trigger = "view",
  color,
  fromColor,
  style,
}: BlurTextProps) {
  const reduceMotion = useReducedMotion();
  const pieces =
    animateBy === "letters"
      ? Array.from(text)
      : text.split(" ").map((word, index, words) => (index < words.length - 1 ? `${word}\u00A0` : word));

  if (reduceMotion) {
    return (
      <span className={className} style={{ ...style, color }}>
        {text}
      </span>
    );
  }

  return (
    <span
      className={className}
      style={{ display: "inline-flex", flexWrap: "wrap", color, ...style }}
      aria-label={text}
    >
      {pieces.map((piece, index) => (
        <motion.span
          key={`${piece}-${index}`}
          aria-hidden="true"
          initial={{ ...initialFor(effect, direction, blur, distance), color: fromColor }}
          {...(trigger === "load"
            ? { animate: { opacity: 1, filter: "blur(0px)", x: 0, y: 0, scale: 1, rotateX: 0, rotateY: 0, skewX: 0, color } }
            : {
                whileInView: { opacity: 1, filter: "blur(0px)", x: 0, y: 0, scale: 1, rotateX: 0, rotateY: 0, skewX: 0, color },
                viewport: { once: true, amount: 0.25 },
              })}
          transition={{
            delay: delay / 1000 + index * stepDuration * 0.18,
            duration: stepDuration,
            ease: [0.22, 1, 0.36, 1],
          }}
          style={{ display: "inline-block", willChange: "transform, filter, opacity" }}
        >
          {piece}
        </motion.span>
      ))}
    </span>
  );
}
