# Demo report: the showcase site in a real browser

Checked on 2026-10-26 with `playwright-cli` 0.1.22 (Chromium, headless) against `pnpm build && next start` on `127.0.0.1:3100`, on the i7-1255U laptop recorded in `bench/results`. The broker behind the site was the production image in hosted mode (`render.yaml` settings, 512 MB, `-Xmx300m`, seeded `demo-orders`) on `127.0.0.1:10001`, reached directly or through `web/scripts/coldstart-proxy.mjs`, which holds requests for N seconds (a waking free-tier service) or drops them (a suspended one). The deployed site is checked separately in the Phase 10 entry of `RUN-REPORT.md`.

## Verified

### Screenshots at nine widths (dark, broker live)

Full-page JPEGs of production (`https://logforge-five.vercel.app`, live hosted broker), retaken on 2026-10-27 from the final build on `main` (`bd6e791`), after scrolling the whole page once so scroll reveals have played. Horizontal overflow measured as `scrollWidth > clientWidth`. The reduced-motion pages below were retaken from production at the same time.

| Width | File | Page height | Horizontal overflow |
|---|---|---|---|
| 375 | [home-375.jpg](docs/demo/home-375.jpg) | 12,836 px | none |
| 414 | [home-414.jpg](docs/demo/home-414.jpg) | 12,139 px | none |
| 768 | [home-768.jpg](docs/demo/home-768.jpg) | 9,968 px | none |
| 1024 | [home-1024.jpg](docs/demo/home-1024.jpg) | 8,370 px | none |
| 1280 | [home-1280.jpg](docs/demo/home-1280.jpg) | 8,270 px | none |
| 1440 | [home-1440.jpg](docs/demo/home-1440.jpg) | 8,226 px | none |
| 1920 | [home-1920.jpg](docs/demo/home-1920.jpg) | 8,235 px | none |
| 2560 | [home-2560.jpg](docs/demo/home-2560.jpg) | 8,235 px | none |
| 3440 | [home-3440.jpg](docs/demo/home-3440.jpg) | 8,235 px | none |

The first pass found 375 and 414 px overflowing to 528 px (the end-to-end latency table widened its grid cell); fixed in `9ab0a42` and re-shot above.

### Cold-start indicator, every state

Phase changes were recorded by a MutationObserver injected before page load (`data-phase` on the pill, which view is shown); layout shift by a `layout-shift` PerformanceObserver. Times are ms since navigation.

| Scenario | Sequence observed | Layout shift |
|---|---|---|
| Warm broker | checking 135, live 1,175, live view 1,218, pill faded 3,181. "Waking" never shown. | 0.0014 |
| Waking (proxy holds requests 30 s; it started a few seconds before the page) | checking 115, waking 2,418, live 25,356, live view 25,430, pill faded 27,371. First contentful paint 160 ms with hero, replay, explorer and benchmarks already in the DOM. Elapsed counter read "15s" mid-wait. | 0.0009 |
| Suspended (proxy drops every connection) | checking 164, waking 3,013, asleep 91,016 (90 s after the first ping), persists. 0 cluster polls. The topology shows the recorded snapshot labelled "Recorded, not live". No failure red besides the legend swatch. | 0.0015 |
| Suspended, reduced motion | checking 85, waking 2,737, asleep 90,735 | 0.0014 |
| No broker configured | asleep at once ("not-configured"), recorded snapshot shown | n/a |

Screenshots: [waking 1440](docs/demo/state-waking-1440-light.jpg), [asleep 1440](docs/demo/state-asleep-1440-light.jpg), [live 1440](docs/demo/state-live-1440-light.jpg) (light scheme), and the header at [375](docs/demo/state-waking-375-dark.jpg), [768](docs/demo/state-waking-768-dark.jpg), [1024](docs/demo/state-waking-1024-dark.jpg) (dark) showing the pill's reserved box. A first 375 px check found the heading squeezed beside the pill; it now wraps above it.

Pulse: computed style of the waking dot is `animation: broker-pulse 1.6s infinite` normally and `animation-name: none` under `prefers-reduced-motion: reduce`.

Hidden tab: with the broker live, cluster polls at 996 and 6,004 ms; tab hidden at 10,163; no request until shown again at 27,707; polls at 27,709 and 32,720.

### Reduced motion

Full pages with `prefers-reduced-motion: reduce`: [375](docs/demo/reduced-motion-375.jpg), [1440](docs/demo/reduced-motion-1440.jpg). Content is complete and nothing waits on an animation.

### Keyboard

A Tab walk from the top of the page reached 33 stops in reading order (skip link, header links, hero link, topology topic button, internal-topics checkbox, table disclosure, replay controls and scrubber, explorer input and presets, benchmark dimension and metric buttons, result links, footer links). Every stop had a visible 2 px solid outline; none was off screen.

### Contrast

Every visible text node's colour against its composited background, WCAG thresholds (4.5:1, 3:1 for large text), with the broker live:

| Scheme | Text elements | Below threshold |
|---|---|---|
| dark | 313 | 0 |
| light | 313 | 0 (3 before the light accent moved from `#0b7f71` to `#0a776a`: pressed buttons at 4.32:1) |

### Console

`/`, `/privacy`, `/terms` and a missing page, in dark and light, with and without reduced motion (16 loads): no errors or warnings on any real page. The missing page logs only the browser's own "status of 404" line for the document. Two problems found and fixed on the way: static `/privacy` and `/terms` had all 17 scripts blocked by the CSP nonce (`c36354c`), and Next.js's default 404 page used an inline style without a nonce (custom `not-found.tsx`).

### Production (https://logforge-five.vercel.app, 2026-10-27)

- Broker live: checking 1,202 ms, live 2,631, live view 3,170, pill faded 4,642; layout shift 0.0000; polls every 5 s.
- Render service suspended: every section painted with the recorded snapshot; checking 2,275, waking 5,191, asleep 93,185 ([waking](docs/demo/prod-suspended-waking-1440.jpg), [asleep](docs/demo/prod-suspended-asleep-1440.jpg)); layout shift 0.0016; console clean.
- Console sweep of all pages, both schemes, with and without reduced motion: clean (404 page logs only its own status).
- Lighthouse mobile (PageSpeed Insights, 02:36): Performance 98, Accessibility 100, Best Practices 100, SEO 100.
- Correction: the local contrast scan above skipped text under full opacity. Lighthouse found pending replay events at 40% opacity below 4.5:1; they are now muted by colour, and an opacity-aware rescan of production found 0 of 326 below threshold in each scheme.

### Final pass on main (2026-10-27)

- **Beyond Chromium**: desktop Chromium, Firefox 156 and WebKit 26.6 at 1440×900, plus iPhone 15 (WebKit) and Pixel 7 (Chromium) emulation with touch, against production: live topology within 1.2 to 1.7 s, topology table, replay step (now pauses playback) and scrub to end, segment explorer lookup, benchmark switches, tape toggle; no console errors, no horizontal overflow, no touch target under 24 px in any of the five.
- **Found and fixed by those runs**: replay stepping did not pause autoplay; header links and the replay scrubber were under 24 px; WebKit on plain-HTTP localhost loaded no assets because of `upgrade-insecure-requests` (now only over HTTPS); the new comparison column widened the page to 391 px at 375 (caught by the production screenshot pass).
- **Lighthouse** (PageSpeed Insights, mobile, production, 1:31:58 PM): Performance 100, Accessibility 100, Best Practices 100, SEO 100.
- **Broker suspended** (final build): first paint 1,024 ms with every section; checking, waking 3,524 ms, asleep 91,519 ms; layout shift 0; console clean.
- **Console sweep** of production (16 loads): clean on every real page.

## Not verified / assumed

- Real phones were not tested; iPhone and Android were Playwright emulation on a desktop. Safari itself was not run; WebKit is its engine.
- Render's real cold start after 15 idle minutes was not timed; suspension and resume were.
- The light-scheme state screenshots predate two copy changes (hero note, the "one-minute moving average" label); layout is unchanged.
- The web-design-guidelines and animation reviews named in the Phase 9 gate are not part of this report yet.
