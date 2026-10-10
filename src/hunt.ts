import type { BugId } from "./bugs.js";
import type { Violation } from "./check/invariants.js";
import { runScenario, type RunResult } from "./sim/run.js";
import { scenarioFromSeed, type Scenario } from "./sim/scenario.js";

export interface HuntProgress {
  seedsTried: number;
  events: number;
  elapsedMs: number;
}

export interface Finding {
  original: Scenario;
  minimal: Scenario;
  violation: Violation;
  seedsTried: number;
  shrinkRuns: number;
}

/**
 * Try seeds in order until one breaks a checker. Each seed expands into its
 * own fault schedule, network jitter and client workload.
 */
export function hunt(opts: {
  bugs: BugId[];
  firstSeed?: number;
  maxSeeds: number;
  onProgress?: (p: HuntProgress) => boolean | void;
  shrink?: boolean;
  clock?: () => number;
}): Finding | null {
  const clock = opts.clock ?? (() => performance.now());
  const start = clock();
  let events = 0;
  const first = opts.firstSeed ?? 1;
  for (let i = 0; i < opts.maxSeeds; i++) {
    const scenario = scenarioFromSeed(first + i, opts.bugs);
    const r = runScenario(scenario);
    events += r.stats.events;
    if (r.violation) {
      const shrunk = opts.shrink === false ? { scenario, runs: 0, violation: r.violation } : shrink(scenario, r);
      return {
        original: scenario,
        minimal: shrunk.scenario,
        violation: shrunk.violation,
        seedsTried: i + 1,
        shrinkRuns: shrunk.runs,
      };
    }
    if (opts.onProgress && (i & 15) === 15) {
      if (opts.onProgress({ seedsTried: i + 1, events, elapsedMs: clock() - start }) === false) return null;
    }
  }
  return null;
}

/**
 * Make a failing scenario as small as possible while it still fails the same
 * way: cut the run off just after the violation, then remove faults with
 * delta debugging (Zeller's ddmin), then try a calmer network.
 */
export function shrink(scenario: Scenario, first?: RunResult): { scenario: Scenario; violation: Violation; runs: number } {
  let runs = 0;
  const base = first ?? runScenario(scenario);
  runs += first ? 0 : 1;
  if (!base.violation) throw new Error("shrink() needs a failing scenario");
  const kind = base.violation.kind;

  let best = scenario;
  let violation = base.violation;
  const fails = (s: Scenario): boolean => {
    runs++;
    const r = runScenario(s);
    if (r.violation?.kind === kind) {
      violation = r.violation;
      return true;
    }
    return false;
  };

  // 1. Stop the run shortly after it broke.
  const cut = Math.min(best.duration, Math.ceil((violation.at + 400) / 100) * 100);
  const trimmed = { ...best, duration: cut, faults: best.faults.filter((f) => f.at < cut) };
  if (fails(trimmed)) best = trimmed;

  // 2. ddmin over the fault schedule.
  let faults = best.faults;
  let n = 2;
  while (faults.length >= 2) {
    const size = Math.ceil(faults.length / n);
    let reduced = false;
    for (let i = 0; i < n && !reduced; i++) {
      const complement = [...faults.slice(0, i * size), ...faults.slice((i + 1) * size)];
      if (complement.length < faults.length && fails({ ...best, faults: complement })) {
        faults = complement;
        n = Math.max(n - 1, 2);
        reduced = true;
      }
    }
    if (!reduced) {
      if (n >= faults.length) break;
      n = Math.min(n * 2, faults.length);
    }
  }
  if (faults.length === 1 && fails({ ...best, faults: [] })) faults = [];
  best = { ...best, faults };

  // 3. A clean network makes the replay easier to read.
  const calm = { ...best, net: { ...best.net, dropRate: 0, dupRate: 0 } };
  if (fails(calm)) best = calm;

  // Re-run the final scenario so the reported violation matches it exactly.
  runs++;
  const final = runScenario(best);
  return { scenario: best, violation: final.violation ?? violation, runs };
}
