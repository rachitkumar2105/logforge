// Copies measured results from the repository into the site before every build, so every number
// on the page comes from bench/results and chaos/results rather than being typed into the code.
// If a source is missing (for example a build that only has web/), the committed copy is kept.
import { copyFileSync, existsSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const web = join(dirname(fileURLToPath(import.meta.url)), "..");
const repo = join(web, "..");
const data = join(web, "src", "data");
mkdirSync(data, { recursive: true });

const copies = [
  ["bench/results/summary.json", "bench.json"],
  ["chaos/results/summary.json", "chaos.json"],
  ["chaos/results/leader-kill-replay.json", "replay.json"],
];

for (const [from, to] of copies) {
  const src = join(repo, from);
  if (existsSync(src)) {
    copyFileSync(src, join(data, to));
    console.log(`sync-data: ${from} -> src/data/${to}`);
  } else if (existsSync(join(data, to))) {
    console.log(`sync-data: ${from} not found, keeping committed src/data/${to}`);
  } else {
    console.error(`sync-data: missing ${from} and no committed copy`);
    process.exit(1);
  }
}
