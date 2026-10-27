"use client";

import { useState } from "react";

/**
 * Pauses the hero's drifting tape. It loops for longer than five seconds next to the page's
 * content, so it needs a control (WCAG 2.2.2); under reduced motion it never moves at all.
 */
export function TapeToggle() {
  const [paused, setPaused] = useState(false);
  return (
    <button
      type="button"
      className="btn absolute right-4 bottom-4 z-10 text-xs sm:right-6"
      aria-pressed={paused}
      onClick={() => {
        const next = !paused;
        setPaused(next);
        document.documentElement.toggleAttribute("data-tape-paused", next);
      }}
    >
      {paused ? "Play tape" : "Pause tape"}
    </button>
  );
}
