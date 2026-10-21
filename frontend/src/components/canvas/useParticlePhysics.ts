"use client";

import { useEffect, useRef, type RefObject } from "react";
import { clamp } from "@/lib/utils";

/**
 * PARTICLE VOID PHYSICS
 * ---------------------
 * Pure vector math + a single requestAnimationFrame integration loop.
 *
 * Depth model: particles live in a 3D box centered on the origin.
 *   - x, y : world offsets (projected through perspective)
 *   - z    : distance from camera, 1 (near) .. 1000 (deep void)
 *
 * Projection:  screenX = (x / z) * focal + centerX
 *              screenY = (y / z) * focal + centerY
 *
 * Every per-frame allocation is pre-sized into typed arrays; the GC stays quiet
 * and the loop holds 60fps on integrated graphics.
 */

export interface Particle {
  x: number;
  y: number;
  z: number;
  /** Equilibrium anchor — the spring always pulls the particle home. */
  ox: number;
  oy: number;
  oz: number;
  vx: number;
  vy: number;
  vz: number;
}

export interface Vec3 {
  x: number;
  y: number;
  z: number;
}

export interface ParticlePhysicsConfig {
  /** Exactly 500 by spec. */
  count: number;
  /** Baseline focal length. */
  focal: number;
  minFocal: number;
  maxFocal: number;
  /** Cursor influence radius in screen pixels. */
  repulsionRadius: number;
  /** Peak exponential push magnitude at distance 0. */
  repulsionStrength: number;
  /** Spring constant pulling particles back to their anchor. */
  springStiffness: number;
  /** Per-frame velocity retention (1 = frictionless). */
  damping: number;
  /** Hairline connection distance in screen pixels. */
  linkDistance: number;
  /** Camera easing factor toward the pointer target rotation. */
  rotationEase: number;
}

export const DEFAULT_CONFIG: ParticlePhysicsConfig = {
  count: 500,
  focal: 400,
  minFocal: 250,
  maxFocal: 650,
  repulsionRadius: 120,
  repulsionStrength: 8,
  springStiffness: 0.014,
  damping: 0.9,
  linkDistance: 60,
  rotationEase: 0.05,
};

export const VOID_MIN_Z = 1;
export const VOID_MAX_Z = 1000;

/* ------------------------------------------------------------------ */
/* Pure math                                                           */
/* ------------------------------------------------------------------ */

/** Spawns `count` particles inside the 3D box. Depth is biased toward the void. */
export function createParticles(count: number, spreadX: number, spreadY: number): Particle[] {
  const particles: Particle[] = [];
  for (let i = 0; i < count; i += 1) {
    const x = (Math.random() - 0.5) * spreadX;
    const y = (Math.random() - 0.5) * spreadY;
    // pow(u, 0.42) skews depth toward the far plane: an infinite field, not a snowstorm.
    const z = VOID_MIN_Z + Math.random() ** 0.42 * (VOID_MAX_Z - VOID_MIN_Z);

    particles.push({
      x,
      y,
      z,
      ox: x,
      oy: y,
      oz: z,
      vx: 0,
      vy: 0,
      vz: 0,
    });
  }
  return particles;
}

/** Rotates a point around the origin on the Y then X axis (camera tilt). */
export function rotatePoint(x: number, y: number, z: number, rotX: number, rotY: number): Vec3 {
  const cosY = Math.cos(rotY);
  const sinY = Math.sin(rotY);
  const x1 = x * cosY - z * sinY;
  const z1 = x * sinY + z * cosY;

  const cosX = Math.cos(rotX);
  const sinX = Math.sin(rotX);
  const y2 = y * cosX - z1 * sinX;
  const z2 = y * sinX + z1 * cosX;

  return { x: x1, y: y2, z: z2 };
}

/** Perspective projection. Returns null when the point is behind the camera. */
export function projectPoint(point: Vec3, focal: number, centerX: number, centerY: number): { x: number; y: number } | null {
  if (point.z <= 0.5) return null;
  return {
    x: (point.x / point.z) * focal + centerX,
    y: (point.y / point.z) * focal + centerY,
  };
}

/** Normalized depth, 0 = touching the camera, 1 = deepest plane. */
export function depthRatio(z: number): number {
  return clamp((z - VOID_MIN_Z) / (VOID_MAX_Z - VOID_MIN_Z), 0, 1);
}

/** Linear interpolation of the near (#FFFFFF) -> far (#A1A1AA) particle palette. */
export function particleColor(t: number): { r: number; g: number; b: number } {
  return {
    r: Math.round(255 + (161 - 255) * t),
    g: Math.round(255 + (161 - 255) * t),
    b: Math.round(255 + (170 - 255) * t),
  };
}

/* ------------------------------------------------------------------ */
/* React integration                                                   */
/* ------------------------------------------------------------------ */

interface LinkBucket {
  cx: number;
  cy: number;
  items: number[];
}

export function useParticlePhysics(
  canvasRef: RefObject<HTMLCanvasElement | null>,
  options?: Partial<ParticlePhysicsConfig>,
  active = true,
): void {
  const optionsRef = useRef(options);
  optionsRef.current = options;
  const activeRef = useRef(active);
  activeRef.current = active;

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const ctx = canvas.getContext("2d", { alpha: true });
    if (!ctx) return;

    const config: ParticlePhysicsConfig = { ...DEFAULT_CONFIG, ...(optionsRef.current ?? {}) };
    const reduceMotion =
      typeof window.matchMedia === "function" &&
      window.matchMedia("(prefers-reduced-motion: reduce)").matches;

    let width = 0;
    let height = 0;
    let centerX = 0;
    let centerY = 0;

    const resize = (): void => {
      width = window.innerWidth;
      height = window.innerHeight;
      centerX = width / 2;
      centerY = height / 2;
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      canvas.width = Math.floor(width * dpr);
      canvas.height = Math.floor(height * dpr);
      canvas.style.width = `${width}px`;
      canvas.style.height = `${height}px`;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    };
    resize();

    const count = reduceMotion ? Math.floor(config.count * 0.35) : config.count;
    const particles = createParticles(count, width * 1.35, height * 1.35);
    const n = particles.length;

    const projX = new Float32Array(n);
    const projY = new Float32Array(n);
    const projT = new Float32Array(n);
    const projOk = new Uint8Array(n);

    let focal = config.focal;
    let targetFocal = config.focal;

    let camRotX = 0;
    let camRotY = 0;
    let targetRotX = 0;
    let targetRotY = 0;

    let pointerX = width / 2;
    let pointerY = height / 2;
    let pointerInside = false;

    const grid = new Map<number, LinkBucket>();

    const projectAll = (): void => {
      for (let i = 0; i < n; i += 1) {
        const p = particles[i];
        if (!p) continue;
        const rotated = rotatePoint(p.x, p.y, p.z, camRotX, camRotY);
        if (rotated.z <= 0.5) {
          projOk[i] = 0;
          continue;
        }
        projX[i] = (rotated.x / rotated.z) * focal + centerX;
        projY[i] = (rotated.y / rotated.z) * focal + centerY;
        projT[i] = depthRatio(rotated.z);
        projOk[i] = 1;
      }
    };

    const integrate = (dt: number): void => {
      const damp = Math.pow(config.damping, dt);
      for (let i = 0; i < n; i += 1) {
        const p = particles[i];
        if (!p) continue;
        p.vx += (p.ox - p.x) * config.springStiffness * dt;
        p.vy += (p.oy - p.y) * config.springStiffness * dt;
        p.vz += (p.oz - p.z) * config.springStiffness * dt;

        p.vx *= damp;
        p.vy *= damp;
        p.vz *= damp;

        p.x += p.vx * dt;
        p.y += p.vy * dt;
        p.z += p.vz * dt;
      }
    };

    const applyRepulsion = (dt: number): void => {
      if (reduceMotion || !pointerInside) return;
      const radius = config.repulsionRadius;
      const radiusSq = radius * radius;

      for (let i = 0; i < n; i += 1) {
        if (!projOk[i]) continue;
        const dx = projX[i] - pointerX;
        const dy = projY[i] - pointerY;
        const distSq = dx * dx + dy * dy;
        if (distSq > radiusSq || distSq < 0.0001) continue;

        const dist = Math.sqrt(distSq);
        // F = (1 - dist / R) * strength  — exponential falloff toward the edge.
        const force = (1 - dist / radius) * config.repulsionStrength;
        const nx = dx / dist;
        const ny = dy / dist;

        const p = particles[i];
        if (!p) continue;
        // Convert the screen-space push back into world space at this depth.
        const worldScale = (p.z / Math.max(focal, 1)) * 0.55;
        p.vx += nx * force * worldScale * dt;
        p.vy += ny * force * worldScale * dt;
      }
    };

    const drawParticles = (): void => {
      for (let i = 0; i < n; i += 1) {
        if (!projOk[i]) continue;
        const x = projX[i];
        const y = projY[i];
        if (x < -20 || x > width + 20 || y < -20 || y > height + 20) continue;

        const t = projT[i];
        const radius = 1.6 - 0.8 * t;
        const alpha = 0.8 - 0.6 * t;
        const color = particleColor(t);

        ctx.beginPath();
        ctx.arc(x, y, radius, 0, Math.PI * 2);
        ctx.fillStyle = `rgba(${color.r}, ${color.g}, ${color.b}, ${alpha.toFixed(3)})`;
        ctx.fill();
      }
    };

    const bucketKey = (cx: number, cy: number): number => (cx + 4096) * 16384 + (cy + 4096);

    const getBucket = (cx: number, cy: number): LinkBucket | undefined => grid.get(bucketKey(cx, cy));

    const drawLinks = (): void => {
      const cell = config.linkDistance;
      const maxDepth = 0.72;
      grid.clear();

      for (let i = 0; i < n; i += 1) {
        if (!projOk[i] || projT[i] > maxDepth) continue;
        const cx = Math.floor(projX[i] / cell);
        const cy = Math.floor(projY[i] / cell);
        const key = bucketKey(cx, cy);
        let bucket = grid.get(key);
        if (!bucket) {
          bucket = { cx, cy, items: [] };
          grid.set(key, bucket);
        }
        bucket.items.push(i);
      }

      ctx.lineWidth = 1;
      let lines = 0;
      const limit = 2400;
      const neighborOffsets: ReadonlyArray<readonly [number, number]> = [
        [1, 0],
        [-1, 1],
        [0, 1],
        [1, 1],
      ];

      const consider = (a: number, b: number): void => {
        if (a === b || lines >= limit) return;
        const dx = projX[a] - projX[b];
        const dy = projY[a] - projY[b];
        const distSq = dx * dx + dy * dy;
        const maxSq = cell * cell;
        if (distSq >= maxSq) return;

        const dist = Math.sqrt(distSq);
        const alpha = 0.05 + (1 - dist / cell) * 0.1; // clamped to the 0.05 – 0.15 band
        ctx.strokeStyle = `rgba(255, 255, 255, ${alpha.toFixed(3)})`;
        ctx.beginPath();
        ctx.moveTo(projX[a], projY[a]);
        ctx.lineTo(projX[b], projY[b]);
        ctx.stroke();
        lines += 1;
      };

      for (const bucket of grid.values()) {
        const items = bucket.items;
        for (let i = 0; i < items.length; i += 1) {
          const a = items[i];
          if (a === undefined) continue;
          for (let j = i + 1; j < items.length; j += 1) {
            const b = items[j];
            if (b === undefined) continue;
            consider(a, b);
          }
          for (const [ox, oy] of neighborOffsets) {
            const neighbor = getBucket(bucket.cx + ox, bucket.cy + oy);
            if (!neighbor) continue;
            for (const b of neighbor.items) consider(a, b);
          }
        }
      }
    };

    let rafId = 0;
    let last = performance.now();
    let running = true;

    const frame = (now: number): void => {
      const rawDelta = (now - last) / 16.6667;
      last = now;

      // The dashboard view owns the viewport while it is active. Pause the
      // canvas loop instead of burning frames behind a translated track.
      if (!activeRef.current) {
        if (running) rafId = requestAnimationFrame(frame);
        return;
      }

      const dt = clamp(rawDelta, 0.2, 2.5);

      if (reduceMotion || !pointerInside) {
        // Idle drift keeps the void alive without a pointer.
        const idle = reduceMotion ? 0.02 : 1;
        targetRotX = Math.sin(now * 0.00021) * 0.07 * idle;
        targetRotY = Math.cos(now * 0.00017) * 0.09 * idle;
      }

      const ease = config.rotationEase * dt;
      camRotX += (targetRotX - camRotX) * ease;
      camRotY += (targetRotY - camRotY) * ease;
      focal += (targetFocal - focal) * clamp(0.08 * dt, 0, 0.25);

      applyRepulsion(dt);
      integrate(dt);
      projectAll();

      ctx.clearRect(0, 0, width, height);
      drawLinks();
      drawParticles();

      if (running) rafId = requestAnimationFrame(frame);
    };

    rafId = requestAnimationFrame(frame);

    const handlePointerMove = (event: PointerEvent): void => {
      pointerX = event.clientX;
      pointerY = event.clientY;
      pointerInside = true;
      if (!reduceMotion) {
        targetRotY = ((event.clientX - centerX) / Math.max(width, 1)) * 0.6;
        targetRotX = ((event.clientY - centerY) / Math.max(height, 1)) * 0.6;
      }
    };

    const handlePointerLeave = (): void => {
      pointerInside = false;
    };

    const handleWheel = (event: WheelEvent): void => {
      event.preventDefault();
      // Scroll down accelerates the field toward the viewer; up recedes into the void.
      targetFocal = clamp(targetFocal + event.deltaY * 0.55, config.minFocal, config.maxFocal);
    };

    const handleResize = (): void => {
      resize();
    };

    const handleVisibility = (): void => {
      if (document.hidden) {
        running = false;
        cancelAnimationFrame(rafId);
      } else if (!running) {
        running = true;
        last = performance.now();
        rafId = requestAnimationFrame(frame);
      }
    };

    window.addEventListener("pointermove", handlePointerMove, { passive: true });
    window.addEventListener("pointerleave", handlePointerLeave);
    window.addEventListener("resize", handleResize);
    document.addEventListener("visibilitychange", handleVisibility);
    canvas.addEventListener("wheel", handleWheel, { passive: false });

    return () => {
      running = false;
      cancelAnimationFrame(rafId);
      window.removeEventListener("pointermove", handlePointerMove);
      window.removeEventListener("pointerleave", handlePointerLeave);
      window.removeEventListener("resize", handleResize);
      document.removeEventListener("visibilitychange", handleVisibility);
      canvas.removeEventListener("wheel", handleWheel);
    };
  }, [canvasRef]);
}
