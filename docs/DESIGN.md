# LogForge site: design

Source: spec Phase 9 and the author's global website rules. The `design-taste-frontend` skill named in the spec is not installed on the build machine (DECISIONS.md P3), so this file is the design authority.

## Creative direction

**Concept: the operations console of a commit log.** The page is an instrument panel, not a landing page. Every section is a panel bolted to the same rack: thin hairline frames, labelled readouts, monospace figures that line up in columns. Live data is the hero; prose is short and plain.

**Memorable idea: the tape.** A partition is drawn as a horizontal tape of fixed-width cells (offsets). It appears in the hero (record batches drifting along it), in the topology (replicas as parallel tapes whose ends show log end offset and high watermark), in the replay (tapes that stall, truncate and catch up) and in the segment explorer (the tape cut into segment files). One visual vocabulary, four uses.

**Signature interaction: scrubbing the crash.** The crash-recovery replay is a recorded chaos run on a timeline. Dragging the playhead (or pressing arrow keys) moves the whole cluster diagram through time: acknowledgements counting up, a broker draining to red, leadership jumping, a follower's tape catching up. The measured recovery time is the number on the panel.

## Tokens

Dark-first; a light theme exists for `prefers-color-scheme: light` but the design is tuned for dark.

| Token | Dark | Light | Use |
|---|---|---|---|
| `--bg` | `#0a0d10` | `#f6f7f8` | page |
| `--panel` | `#0f1418` | `#ffffff` | panels |
| `--panel-2` | `#141b21` | `#eef1f3` | insets, tape cells |
| `--line` | `#26313a` | `#d4dade` | hairlines, frames |
| `--text` | `#e6edf3` | `#11171c` | body text |
| `--muted` | `#93a1ad` | `#4c5963` | labels (contrast >= 4.5:1 on `--bg` and `--panel`) |
| `--accent` | `#3ddbc4` (teal) | `#0b7f71` | leaders, live data, focus, links |
| `--accent-dim` | `#1f6f64` | `#9ad9cf` | followers in sync, secondary series |
| `--fail` | `#ff5c57` | `#c42b26` | **only** failure, offline and under-replicated states |
| `--warn-line` | `#5a6670` | `#9aa6ae` | out-of-sync follower outline (neutral, not a colour signal) |

Rules: teal is the only accent. Red appears only when something is broken. No gradients except a 1-px hairline glow on the live indicator. No glassmorphism, no purple, no rainbow, no emoji icons, no stock photos, no fabricated numbers, no "made with" badges, no em dashes in copy.

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

Skip link, landmarks (`header`, `nav`, `main`, `section` with headings, `footer`), visible 2 px teal focus ring with offset on every interactive element, `aria-live="polite"` on the live topology status and the replay readout, a data table for every chart and for the topology, keyboard control of the replay scrubber (arrow keys, Home, End, Space).
