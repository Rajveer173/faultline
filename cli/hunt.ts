import { BUGS, type BugId } from "../src/bugs.js";
import { hunt } from "../src/hunt.js";
import { describeFault, encodeScenario } from "../src/sim/scenario.js";

const args = process.argv.slice(2);
const flag = (name: string, fallback: string) => {
  const i = args.indexOf("--" + name);
  return i >= 0 ? args[i + 1] : fallback;
};

const bug = flag("bug", "none");
const seeds = Number(flag("seeds", "2000"));
const from = Number(flag("from", "1"));
if (bug !== "none" && !BUGS.some((b) => b.id === bug)) {
  console.error(`unknown bug "${bug}". one of: none, ${BUGS.map((b) => b.id).join(", ")}`);
  process.exit(2);
}

const started = performance.now();
const found = hunt({
  bugs: bug === "none" ? [] : [bug as BugId],
  firstSeed: from,
  maxSeeds: seeds,
  onProgress: (p) => {
    process.stdout.write(`\r  ${p.seedsTried} seeds  ${(p.seedsTried / (p.elapsedMs / 1000)).toFixed(0)} seeds/s`);
  },
});
process.stdout.write("\n");
const secs = ((performance.now() - started) / 1000).toFixed(2);

if (!found) {
  console.log(`no violation in ${seeds} seeds (${secs}s)`);
  process.exit(0);
}
const v = found.violation;
console.log(`${v.kind} at seed ${found.original.seed} after ${found.seedsTried} seeds (${secs}s, ${found.shrinkRuns} shrink runs)`);
console.log(`  ${v.message}`);
console.log(`  faults ${found.original.faults.length} -> ${found.minimal.faults.length}, duration ${found.original.duration} -> ${found.minimal.duration} ms`);
for (const f of found.minimal.faults) console.log(`    @${f.at}ms  ${describeFault(f)}`);
console.log(`  replay: #s=${encodeScenario(found.minimal)}`);
process.exit(bug === "none" ? 1 : 0);
