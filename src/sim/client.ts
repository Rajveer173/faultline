import type { Rng } from "../core/rng.js";
import type { HistoryOp } from "../check/linearizability.js";
import type { Msg, Op } from "../raft/types.js";

export const CLIENT_BASE = 100;
const REQUEST_TIMEOUT = 300;

export interface ClientEnv {
  now(): number;
  send(to: number, msg: Msg): void;
  setTimer(kind: string, delay: number): void;
}

/**
 * A closed-loop client: one operation in flight at a time. It records every
 * operation into the shared history the linearizability checker reads.
 *
 * On timeout a client does not retry the same operation; it records it as
 * indeterminate and moves on. A write that timed out may still be applied
 * later, so its return time is Infinity. A read that timed out told us
 * nothing, so it is dropped from the history.
 */
export class Client {
  private seq = 0;
  private target: number;
  private current: { seq: number; op: Op; entry: HistoryOp } | null = null;

  constructor(
    readonly id: number,
    private readonly env: ClientEnv,
    private readonly rng: Rng,
    private readonly nodes: number,
    private readonly keys: number,
    private readonly stopAt: number,
    private readonly history: HistoryOp[],
    private readonly nextOpId: () => number,
  ) {
    this.target = rng.int(0, nodes);
  }

  start(): void {
    this.env.setTimer("next", this.rng.int(0, 50));
  }

  onTimer(kind: string): void {
    if (kind === "next") this.issue();
    else if (kind === "timeout" && this.current) this.giveUp();
    else if (kind === "retry" && this.current) this.env.send(this.target, { type: "request", seq: this.current.seq, op: this.current.op });
  }

  onMessage(_from: number, msg: Msg): void {
    const cur = this.current;
    if (!cur) return;
    if (msg.type === "response" && msg.seq === cur.seq) {
      cur.entry.ret = this.env.now();
      if (cur.op.kind === "get") cur.entry.value = msg.value;
      this.current = null;
      this.env.setTimer("next", this.rng.int(5, 40));
    } else if (msg.type === "redirect" && msg.seq === cur.seq) {
      this.target = msg.leader ?? this.otherNode();
      this.env.setTimer("retry", msg.leader === null ? 30 : 1);
    }
  }

  private issue(): void {
    if (this.env.now() >= this.stopAt) return;
    this.seq++;
    const key = "k" + this.rng.int(0, this.keys);
    const op: Op = this.rng.chance(0.5)
      ? { kind: "put", key, value: (this.id - CLIENT_BASE + 1) * 100000 + this.seq }
      : { kind: "get", key };
    const entry: HistoryOp = {
      id: this.nextOpId(),
      client: this.id - CLIENT_BASE,
      key,
      kind: op.kind,
      value: op.kind === "put" ? op.value : null,
      invoke: this.env.now(),
      ret: Infinity,
    };
    this.history.push(entry);
    this.current = { seq: this.seq, op, entry };
    this.env.send(this.target, { type: "request", seq: this.seq, op });
    this.env.setTimer("timeout", REQUEST_TIMEOUT);
  }

  private giveUp(): void {
    const cur = this.current!;
    if (cur.op.kind === "get") {
      const i = this.history.indexOf(cur.entry);
      if (i >= 0) this.history.splice(i, 1);
    }
    this.current = null;
    this.target = this.otherNode();
    this.env.setTimer("next", this.rng.int(5, 40));
  }

  private otherNode(): number {
    const n = this.rng.int(0, this.nodes - 1);
    return n >= this.target ? n + 1 : n;
  }
}
