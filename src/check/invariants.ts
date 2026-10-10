import type { Command, Observation } from "../raft/types.js";

export interface Violation {
  kind: "election-safety" | "state-machine-safety" | "linearizability";
  at: number;
  message: string;
  /** Nodes or keys the UI should point at. */
  nodes?: number[];
  key?: string;
  opIds?: number[];
}

function sameCmd(a: Command, b: Command): boolean {
  return a.client === b.client && a.seq === b.seq;
}

function describeCmd(c: Command): string {
  return c.op.kind === "put" ? `put ${c.op.key}=${c.op.value}` : `get ${c.op.key}`;
}

/**
 * Safety properties from Figure 3 of the paper, checked as the run happens:
 *  - Election Safety: at most one leader per term.
 *  - State Machine Safety: no two nodes apply different commands at the same index.
 * Log Matching and Leader Completeness are what make the second one hold, so a
 * break in either surfaces here as soon as it reaches a state machine.
 */
export class Invariants {
  private leaders = new Map<number, number>();
  private applied: { node: number; term: number; cmd: Command }[] = [];
  violation: Violation | null = null;

  observe(node: number, o: Observation, at: number): void {
    if (this.violation) return;

    if (o.type === "role" && o.role === "leader") {
      const other = this.leaders.get(o.term);
      if (other !== undefined && other !== node) {
        this.violation = {
          kind: "election-safety",
          at,
          message: `n${other} and n${node} were both leader in term ${o.term}.`,
          nodes: [other, node],
        };
      } else this.leaders.set(o.term, node);
    }

    if (o.type === "apply") {
      const prev = this.applied[o.index];
      if (!prev) this.applied[o.index] = { node, term: o.term, cmd: o.cmd };
      else if (!sameCmd(prev.cmd, o.cmd)) {
        this.violation = {
          kind: "state-machine-safety",
          at,
          message:
            `Index ${o.index} was applied as "${describeCmd(prev.cmd)}" (term ${prev.term}) on n${prev.node} ` +
            `but as "${describeCmd(o.cmd)}" (term ${o.term}) on n${node}. A committed entry was overwritten.`,
          nodes: [prev.node, node],
        };
      }
    }
  }
}
