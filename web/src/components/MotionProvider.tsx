"use client";

import { MotionConfig } from "motion/react";

/** Every motion animation honours prefers-reduced-motion. */
export function MotionProvider({ children }: { children: React.ReactNode }) {
  return <MotionConfig reducedMotion="user">{children}</MotionConfig>;
}
