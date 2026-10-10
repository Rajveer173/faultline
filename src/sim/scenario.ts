import { Rng } from "../core/rng.js";
import type { BugId } from "../bugs.js";

export type Fault =
  | { at: number; kind: "crash"; node: number }
  | { at: number; kind: "restart"; node: number }
  | { at: number; kind: "partition"; side: number[] }
  | { at: number; kind: "heal" }
  // Resolved against the live cluster when they fire, the way Jepsen's
  // nemeses target "the primary". Still deterministic: the leader at time t
  // is a function of the scenario.
  | { at: number; kind: "crash-leader"; downFor: number }
  | { at: number; kind: "isolate-leader" }
  // A crash point: arm at `at`, then crash the next node that does `when`,
  // `delay` ms after it does it, and bring it back after `downFor` ms. The ack
  // it just sent still goes out -- which is exactly the moment that shows
  // whether state was made durable before it was acknowledged.
  | { at: number; kind: "trigger"; when: Trigger; delay: number; downFor: number };

export type Trigger = "vote-granted" | "append-acked" | "leader-elected";
export const TRIGGERS: readonly Trigger[] = ["vote-granted", "append-acked", "leader-elected"];

export interface NetConfig {
  minLatency: number;
  maxLatency: number;
  dropRate: number;
  dupRate: number;
}

/**
 * Everything a run depends on. Two runs of the same scenario produce the same
 * trace, byte for byte, which is what makes a failure replayable from a link.
 */
export interface Scenario {
  seed: number;
  nodes: number;
  clients: number;
  keys: number;
  /** Virtual milliseconds. */
  duration: number;
  net: NetConfig;
  bugs: BugId[];
  faults: Fault[];
}

export const DEFAULT_NET: NetConfig = { minLatency: 2, maxLatency: 40, dropRate: 0.02, dupRate: 0.01 };

/** Build the scenario a hunt uses for a given seed: the fault schedule is itself drawn from the seed. */
export function scenarioFromSeed(seed: number, bugs: BugId[] = [], overrides: Partial<Scenario> = {}): Scenario {
  const base: Scenario = {
    seed,
    nodes: 5,
    clients: 3,
    keys: 2,
    duration: 8000,
    net: { ...DEFAULT_NET },
    bugs,
    faults: [],
    ...overrides,
  };
  if (!overrides.faults) base.faults = generateFaults(new Rng(seed ^ 0x5eed_f417), base.nodes, base.duration);
  return base;
}

/**
 * A nemesis schedule. Each step draws one fault from a weighted menu, then
 * waits 80-700 ms. Weights lean towards the leader, because that is where
 * Raft's hard cases live: most interesting bugs need a leader to change hands
 * at an awkward moment. At most a minority is ever down by explicit crash,
 * and the last second is fault-free so the cluster can settle.
 */
export function generateFaults(rng: Rng, nodes: number, duration: number): Fault[] {
  const faults: Fault[] = [];
  const down = new Set<number>();
  const maxDown = Math.floor((nodes - 1) / 2);
  const quietFrom = duration - 1000;
  let partitioned = false;

  type Pick = { weight: number; ok: () => boolean; make: (t: number) => Fault[] };
  const menu: Pick[] = [
    {
      // kill -9 and straight back up, as a supervisor would restart it.
      weight: 3,
      ok: () => down.size < nodes,
      make: (t) => {
        const node = rng.pick(range(nodes).filter((n) => !down.has(n)));
        return [{ at: t, kind: "crash", node }, { at: t + rng.int(5, 80), kind: "restart", node }];
      },
    },
    {
      weight: 3,
      ok: () => true,
      make: (t) => [{ at: t, kind: "crash-leader", downFor: rng.chance(0.5) ? rng.int(5, 150) : rng.int(150, 1200) }],
    },
    {
      weight: 3,
      ok: () => true,
      make: (t) => {
        partitioned = true;
        return [{ at: t, kind: "isolate-leader" }];
      },
    },
    {
      weight: 4,
      ok: () => true,
      make: (t) => [
        {
          at: t,
          kind: "trigger",
          when: rng.pick(TRIGGERS),
          delay: rng.int(0, 40),
          downFor: rng.chance(0.6) ? rng.int(1, 30) : rng.int(30, 400),
        },
      ],
    },
    {
      weight: 2,
      ok: () => down.size < maxDown,
      make: (t) => {
        const node = rng.pick(range(nodes).filter((n) => !down.has(n)));
        down.add(node);
        return [{ at: t, kind: "crash", node }];
      },
    },
    {
      weight: 3,
      ok: () => down.size > 0,
      make: (t) => {
        const node = rng.pick([...down].sort((a, b) => a - b));
        down.delete(node);
        return [{ at: t, kind: "restart", node }];
      },
    },
    {
      weight: 2,
      ok: () => true,
      make: (t) => {
        const size = rng.int(1, Math.floor(nodes / 2) + 1);
        const side = shuffle(rng, range(nodes)).slice(0, size).sort((a, b) => a - b);
        partitioned = true;
        return [{ at: t, kind: "partition", side }];
      },
    },
    {
      weight: 4,
      ok: () => partitioned,
      make: (t) => {
        partitioned = false;
        return [{ at: t, kind: "heal" }];
      },
    },
  ];

  for (let t = rng.int(200, 700); t < quietFrom; t += rng.int(80, 700)) {
    const open = menu.filter((m) => m.ok());
    let r = rng.float() * open.reduce((sum, m) => sum + m.weight, 0);
    const choice = open.find((m) => (r -= m.weight) < 0) ?? open[open.length - 1];
    faults.push(...choice.make(t));
  }

  if (partitioned) faults.push({ at: quietFrom, kind: "heal" });
  for (const node of [...down].sort((a, b) => a - b)) faults.push({ at: quietFrom, kind: "restart", node });
  return faults.sort((a, b) => a.at - b.at);
}

export function describeFault(f: Fault): string {
  switch (f.kind) {
    case "crash":
      return `crash n${f.node}`;
    case "restart":
      return `restart n${f.node}`;
    case "partition":
      return `partition {${f.side.map((n) => "n" + n).join(" ")}} | rest`;
    case "heal":
      return "heal network";
    case "crash-leader":
      return `crash the leader for ${f.downFor} ms`;
    case "isolate-leader":
      return "isolate the leader";
    case "trigger":
      return `crash the next node to ${f.when.replace("-", " ")} after ${f.delay} ms, down ${f.downFor} ms`;
  }
}

// Scenarios travel in URLs, so keep the encoding compact and stable.
export function encodeScenario(s: Scenario): string {
  const json = JSON.stringify(s);
  const bytes = new TextEncoder().encode(json);
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export function decodeScenario(token: string): Scenario {
  const b64 = token.replace(/-/g, "+").replace(/_/g, "/");
  const bin = atob(b64 + "=".repeat((4 - (b64.length % 4)) % 4));
  const bytes = Uint8Array.from(bin, (c) => c.charCodeAt(0));
  return JSON.parse(new TextDecoder().decode(bytes)) as Scenario;
}

function range(n: number): number[] {
  return Array.from({ length: n }, (_, i) => i);
}

function shuffle<T>(rng: Rng, xs: T[]): T[] {
  for (let i = xs.length - 1; i > 0; i--) {
    const j = rng.int(0, i + 1);
    [xs[i], xs[j]] = [xs[j], xs[i]];
  }
  return xs;
}
