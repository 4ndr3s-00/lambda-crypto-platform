"use client";

import { useRef } from "react";
import { useParticlePhysics, type ParticlePhysicsConfig } from "./useParticlePhysics";

interface ParticleVoidProps {
  config?: Partial<ParticlePhysicsConfig>;
  active?: boolean;
}

/**
 * The Grok-Imagine void. A full-viewport, retina-crisp canvas that renders the
 * 3D particle field. It sits at z-0 behind the chat view and owns wheel + pointer
 * input, so the UI above it must opt into `pointer-events-auto` explicitly.
 *
 * Absolute (not fixed) so it travels with the dual-view track.
 */
export function ParticleVoid({ config, active = true }: ParticleVoidProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  useParticlePhysics(canvasRef, config, active);

  return (
    <canvas
      ref={canvasRef}
      id="grok-canvas"
      aria-hidden="true"
      className="pointer-events-auto absolute inset-0 z-0 h-full w-full touch-none bg-black"
    />
  );
}
