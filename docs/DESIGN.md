# LogForge site: design

Source: spec Phase 9 and the author's global website rules. The `design-taste-frontend` skill named in the spec is not installed on the build machine (DECISIONS.md P3), so this file is the design authority.

## Creative direction

**Concept: the operations console of a commit log.** The page is an instrument panel, not a landing page. Every section is a panel bolted to the same rack: thin hairline frames, labelled readouts, monospace figures that line up in columns. Live data is the hero; prose is short and plain.

**Memorable idea: the tape.** A partition is drawn as a horizontal tape of fixed-width cells (offsets). It appears in the hero (record batches drifting along it), in the topology (replicas as parallel tapes whose ends show log end offset and high watermark), in the replay (tapes that stall, truncate and catch up) and in the segment explorer (the tape cut into segment files). One visual vocabulary, four uses.

**Signature interaction: scrubbing the crash.** The crash-recovery replay is a recorded chaos run on a timeline. Dragging the playhead (or pressing arrow keys) moves the whole cluster diagram through time: acknowledgements counting up, a broker draining to red, leadership jumping, a follower's tape catching up. The measured recovery time is the number on the panel.

## Tokens

One warm light theme, "Buttercream and Espresso" (changed 2026-11-13 at the author's request, replacing the dark-first teal theme): 60 % cream background, 30 % espresso structure and text, 10 % caramel accent. Muted natural tones instead of a cold dark console.

| Token | Value | Use |
|---|---|---|
| `--bg` | `#faf6eb` | page (soft vanilla cream) |
| `--panel` | `#fffcf4` | panels |
| `--panel-2` | `#f3ecdb` | insets, tape cells, buttons |
| `--line` | `#e2d7c2` | hairlines, frames |
| `--text` | `#2c1e16` | body text (espresso, 14.9:1 on `--bg`) |
| `--muted` | `#6b5a4e` | labels (6.1:1 on `--bg`) |
| `--accent` | `#c88a36` | caramel: leaders, live data, fills, borders, primary button (espresso text on it, 5.5:1) |
| `--accent-ink` | `#8a5a1c` | accent used as text and links (5.5:1 on `--bg`); caramel itself is 2.7:1, so never body-size text |
| `--accent-dim` | `#e6cfa4` | secondary series |
| `--fail` | `#b23a2e` | **only** failure, offline and under-replicated states (5.5:1) |
| `--warn-line` | `#9c8b7c` | out-of-sync follower outline (neutral, 3.0:1) |

Rules: caramel is the only accent. Red appears only when something is broken. No gradients. No glassmorphism, no purple, no rainbow, no emoji icons, no stock photos, no fabricated numbers, no "made with" badges, no em dashes in copy.

## Type

- Headings and prose: **Geist** (via `next/font/google`, self-hosted at build time).
- Every number, offset, id, unit and table: **Geist Mono**, tabular figures.
- Fluid scale with `clamp()`: body `clamp(1rem, 0.96rem + 0.2vw, 1.0625rem)`, h1 `clamp(2.4rem, 1.6rem + 3.2vw, 4.75rem)`, h2 `clamp(1.6rem, 1.3rem + 1.2vw, 2.4rem)`, small labels `clamp(0.75rem, 0.72rem + 0.1vw, 0.8125rem)` uppercase with letter spacing. No fixed px font sizes.

## Layout

- Content max width 1360 px, centred, 16 px side gutter on phones. The topology panel may extend to 1600 px on very wide screens; background tape may be full-bleed.
- Sections in spec order: Hero, Live topology, Crash replay, Segment explorer, Benchmarks, Architecture walkthrough, Honest limitations, Footer.
- Touch targets at least 44 x 44 px. No information available only on hover: everything shown on hover is also in a visible panel or a data table.

## Motion

- Animate `transform`, `opacity` and SVG stroke only. Never width, height, top, left or box-shadow.
- UI feedback 150 to 400 ms; scroll reveals up to 900 ms. Easing `cubic-bezier(0.22, 1, 0.36, 1)` (out-quint-like) for entrances and `cubic-bezier(0.65, 0, 0.35, 1)` for state changes. Never linear (except continuous drift, which has no start or end) and never the default `ease`.
- One signature effect per section: hero drift; topology replication pulses; replay scrub; explorer lookup walk; chart bars rising once; architecture path drawing (SVG stroke).
- No entrance animation on hero text; nothing blocks first paint.
- `prefers-reduced-motion: reduce`: drift and pulses stop, reveals become instant, the replay does not auto-play (manual scrub only).
- Live polling: every 3 s, paused while the tab is hidden; the route handler caches for 2 s so all visitors together cost one admin request per interval.
- No parallax, no scroll-jacking, no cursor-following effects.

## Icons

Inline SVG, 24 x 24 grid, 1.5 px stroke, round caps, `currentColor`. A small set drawn for this site: broker, leader crown-free marker (a filled square), follower (outlined square), pulse, warning (only in red contexts), github, external link, play, pause, step.

## Accessibility

Skip link, landmarks (`header`, `nav`, `main`, `section` with headings, `footer`), visible 2 px caramel focus ring with offset on every interactive element, `aria-live="polite"` on the live topology status and the replay readout, a data table for every chart and for the topology, keyboard control of the replay scrubber (arrow keys, Home, End, Space).

## Replication lab and hero console (added 2026-11-13)

- **Replication lab** (`#lab`, after the live topology): the tape vocabulary again, one tape per broker, offsets aligned in a fixed 28-slot grid so the three logs read as columns. Committed cells are filled caramel, written-but-uncommitted cells are caramel outlines, cells that disagree with the leader are dashed neutral. The HW is a single text-coloured bar. Red only for killed or fenced brokers and for acknowledged records lost.
- **Hero console**: the same model, unattended, kills its own leader every 16 s so the first screen shows a real failover by the broker's rules. It has a pause control (WCAG 2.2.2) and does not run under reduced motion.
- Both run on `requestAnimationFrame` only while on screen and while the tab is visible; React repaints at most every 66 to 90 ms; the model steps in fixed 10 ms increments so behaviour does not depend on frame rate.
- No inline `style` attributes in server-rendered markup (the CSP is nonce-based); positions come from CSS grid and data attributes.
