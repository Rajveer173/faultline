import { EventHeap, type Timed } from "../core/heap.js";
import { Rng } from "../core/rng.js";
import { Invariants, type Violation } from "../check/invariants.js";
import { checkHistory, type HistoryOp } from "../check/linearizability.js";
import { RaftNode } from "../raft/node.js";
import type { Durable, Env, Msg, Observation, Role } from "../raft/types.js";
import type { Bugs } from "../bugs.js";
import { Client, CLIENT_BASE } from "./client.js";
import type { Fault, Scenario, Trigger } from "./scenario.js";

export interface MsgRecord {
  id: number;
  from: number;
  to: number;
  type: Msg["type"];
  sent: number;
  /** Delivery time, or null if the message never arrived. */
  recv: number | null;
  drop: "loss" | "partition" | "down" | null;
  dup: boolean;
}

export interface Frame {
  t: number;
  up: boolean;
  role: Role;
  term: number;
  commit: number;
  /** Term of each log entry. */
  log: number[];
}

export interface Trace {
  messages: MsgRecord[];
  /** frames[node] is that node's state over time, one frame per change. */
  frames: Frame[][];
  faults: Fault[];
  history: HistoryOp[];
}

export interface RunStats {
  events: number;
  messages: number;
  elections: number;
  committed: number;
  opsCompleted: number;
  opsTotal: number;
}

export interface RunResult {
  scenario: Scenario;
  violation: Violation | null;
  stats: RunStats;
  /** A hash of every event in order; equal fingerprints mean identical runs. */
  fingerprint: string;
  trace?: Trace;
}

type EvBody =
  | { k: "deliver"; from: number; to: number; msg: Msg; mid: number }
  | { k: "timer"; actor: number; kind: string; token: number }
  | { k: "fault"; fault: Fault };
type Ev = Timed & EvBody;

export function runScenario(scenario: Scenario, opts: { trace?: boolean } = {}): RunResult {
  return new Run(scenario, !!opts.trace).execute();
}

class Run {
  private now = 0;
  private seq = 0;
  private mid = 0;
  private token = 0;
  private readonly heap = new EventHeap<Ev>();
  private readonly timers = new Map<string, number>();

  private readonly master: Rng;
  // One stream per directed link, so a fault on one link does not shift the
  // latencies of every other message in the run. That locality is what lets
  // the shrinker delete a fault without scrambling everything after it.
  private readonly links = new Map<number, Rng>();
  private readonly nodeRngs: Rng[];

  private readonly nodes: (RaftNode | null)[];
  private readonly disks: Durable[];
  private readonly clients: Client[] = [];
  private side: Set<number> | null = null;
  private armed: Extract<Fault, { kind: "trigger" }>[] = [];

  private readonly bugs: Bugs;
  private readonly invariants = new Invariants();
  private readonly history: HistoryOp[] = [];
  private opId = 0;

  private readonly stats: RunStats = { events: 0, messages: 0, elections: 0, committed: 0, opsCompleted: 0, opsTotal: 0 };
  private hash = 0x811c9dc5;

  private readonly trace: Trace | null;
  private readonly dirty: boolean[];
  private readonly msgIndex = new Map<number, MsgRecord>();

  constructor(
    private readonly s: Scenario,
    tracing: boolean,
  ) {
    this.master = new Rng(s.seed);
    this.nodeRngs = Array.from({ length: s.nodes }, () => this.master.fork());
    this.bugs = Object.fromEntries(s.bugs.map((b) => [b, true]));
    this.nodes = Array.from({ length: s.nodes }, () => null);
    this.disks = Array.from({ length: s.nodes }, () => ({ term: 0, votedFor: null, log: [] }));
    this.dirty = Array.from({ length: s.nodes }, () => false);
    this.trace = tracing
      ? { messages: [], frames: Array.from({ length: s.nodes }, () => []), faults: [], history: this.history }
      : null;
  }

  execute(): RunResult {
    const s = this.s;
    for (let i = 0; i < s.nodes; i++) this.boot(i);
    this.flushFrames();
    for (let c = 0; c < s.clients; c++) {
      const id = CLIENT_BASE + c;
      const client = new Client(
        id,
        {
          now: () => this.now,
          send: (to, msg) => this.send(id, to, msg),
          setTimer: (kind, delay) => this.setTimer(id, kind, delay),
        },
        this.master.fork(),
        s.nodes,
        s.keys,
        s.duration - 300,
        this.history,
        () => this.opId++,
      );
      this.clients.push(client);
      client.start();
    }
    for (const fault of s.faults) this.schedule(fault.at, { k: "fault", fault });

    for (;;) {
      const ev = this.heap.pop();
      if (!ev || ev.at > s.duration) break;
      this.now = ev.at;
      this.stats.events++;
      this.dispatch(ev);
      this.flushFrames();
      if (this.invariants.violation) break;
    }

    let violation = this.invariants.violation;
    if (!violation) {
      const lin = checkHistory(this.history);
      if (lin.ok === false) {
        violation = {
          kind: "linearizability",
          at: lin.op.ret === Infinity ? this.now : lin.op.ret,
          message: lin.explanation,
          key: lin.key,
          opIds: lin.related,
        };
      }
    }

    this.stats.opsTotal = this.history.length;
    this.stats.opsCompleted = this.history.filter((o) => o.ret !== Infinity).length;
    return {
      scenario: s,
      violation,
      stats: this.stats,
      fingerprint: (this.hash >>> 0).toString(16).padStart(8, "0"),
      trace: this.trace ?? undefined,
    };
  }

  // ---- scheduling ----------------------------------------------------------

  private schedule(at: number, ev: EvBody): void {
    this.heap.push({ ...ev, at, seq: this.seq++ });
  }

  private setTimer(actor: number, kind: string, delay: number): void {
    const token = ++this.token;
    this.timers.set(actor + ":" + kind, token);
    this.schedule(this.now + delay, { k: "timer", actor, kind, token });
  }

  private mix(...xs: number[]): void {
    let h = this.hash;
    for (const x of xs) {
      h ^= x & 0xffff;
      h = Math.imul(h, 0x01000193);
      h ^= x >>> 16;
      h = Math.imul(h, 0x01000193);
    }
    this.hash = h;
  }

  private dispatch(ev: Ev): void {
    switch (ev.k) {
      case "timer": {
        if (this.timers.get(ev.actor + ":" + ev.kind) !== ev.token) return;
        this.mix(1, ev.at, ev.actor);
        if (ev.actor >= CLIENT_BASE) this.clients[ev.actor - CLIENT_BASE].onTimer(ev.kind);
        else this.nodes[ev.actor]?.onTimer(ev.kind);
        return;
      }
      case "deliver":
        return this.deliver(ev.from, ev.to, ev.msg, ev.mid);
      case "fault":
        this.mix(3, ev.at);
        return this.applyFault(ev.fault);
    }
  }

  // ---- network ------------------------------------------------------------

  private isNode(id: number): boolean {
    return id < CLIENT_BASE;
  }

  /** Partitions cut node-to-node links only; clients can reach every live node. */
  private connected(a: number, b: number): boolean {
    if (!this.side || !this.isNode(a) || !this.isNode(b)) return true;
    return this.side.has(a) === this.side.has(b);
  }

  private send(from: number, to: number, msg: Msg): void {
    this.stats.messages++;
    if (msg.type === "voteReply" && msg.granted) this.fire("vote-granted", from);
    else if (msg.type === "appendReply" && msg.success) this.fire("append-acked", from);
    const { minLatency, maxLatency, dropRate, dupRate } = this.s.net;
    const rng = this.link(from, to);
    const copies = rng.chance(dupRate) ? 2 : 1;
    for (let c = 0; c < copies; c++) {
      const id = this.mid++;
      const rec = this.record(id, from, to, msg, c > 0);
      if (!this.connected(from, to)) {
        if (rec) rec.drop = "partition";
        continue;
      }
      if (rng.chance(dropRate)) {
        if (rec) rec.drop = "loss";
        continue;
      }
      const latency = rng.int(minLatency, maxLatency + 1);
      this.schedule(this.now + latency, { k: "deliver", from, to, msg, mid: id });
    }
  }

  private link(from: number, to: number): Rng {
    const key = from * 1024 + to;
    let rng = this.links.get(key);
    if (!rng) {
      rng = new Rng((this.s.seed * 0x9e3779b1) ^ Math.imul(key + 1, 0x85ebca6b));
      this.links.set(key, rng);
    }
    return rng;
  }

  private record(id: number, from: number, to: number, msg: Msg, dup: boolean): MsgRecord | null {
    if (!this.trace) return null;
    const rec: MsgRecord = { id, from, to, type: msg.type, sent: this.now, recv: null, drop: null, dup };
    this.trace.messages.push(rec);
    this.msgIndex.set(id, rec);
    return rec;
  }

  private deliver(from: number, to: number, msg: Msg, mid: number): void {
    const rec = this.trace ? this.msgIndex.get(mid) ?? null : null;
    if (this.trace) this.msgIndex.delete(mid);
    if (this.isNode(to) && !this.nodes[to]) {
      if (rec) rec.drop = "down";
      return;
    }
    if (!this.connected(from, to)) {
      if (rec) rec.drop = "partition";
      return;
    }
    if (rec) rec.recv = this.now;
    this.mix(2, this.now, from, to);
    if (this.isNode(to)) this.nodes[to]!.onMessage(from, msg);
    else this.clients[to - CLIENT_BASE].onMessage(from, msg);
  }

  // ---- nodes and faults ----------------------------------------------------

  private boot(id: number): void {
    const peers = Array.from({ length: this.s.nodes }, (_, i) => i).filter((i) => i !== id);
    const rng = this.nodeRngs[id];
    const env: Env = {
      id,
      peers,
      now: () => this.now,
      random: () => rng.float(),
      send: (to, msg) => this.send(id, to, msg),
      setTimer: (kind, delay) => this.setTimer(id, kind, delay),
      observe: (o) => this.observe(id, o),
    };
    const node = new RaftNode(env, this.bugs, this.disks[id]);
    this.nodes[id] = node;
    this.dirty[id] = true;
    node.start();
  }

  private observe(node: number, o: Observation): void {
    this.dirty[node] = true;
    if (o.type === "role" && o.role === "leader") {
      this.stats.elections++;
      this.fire("leader-elected", node);
    }
    if (o.type === "commit" && o.index > this.stats.committed) this.stats.committed = o.index;
    this.invariants.observe(node, o, this.now);
  }

  private applyFault(f: Fault): void {
    this.trace?.faults.push(f);
    switch (f.kind) {
      case "crash": {
        const node = this.nodes[f.node];
        if (!node) return;
        this.disks[f.node] = node.durable();
        this.nodes[f.node] = null;
        for (const key of [...this.timers.keys()]) if (key.startsWith(f.node + ":")) this.timers.delete(key);
        this.dirty[f.node] = true;
        return;
      }
      case "restart":
        if (!this.nodes[f.node]) this.boot(f.node);
        return;
      case "crash-leader": {
        const leader = this.currentLeader();
        if (leader === null) return;
        this.applyFault({ at: f.at, kind: "crash", node: leader });
        this.schedule(this.now + f.downFor, { k: "fault", fault: { at: this.now + f.downFor, kind: "restart", node: leader } });
        return;
      }
      case "isolate-leader": {
        const leader = this.currentLeader();
        if (leader !== null) this.side = new Set([leader]);
        return;
      }
      case "trigger":
        this.armed.push(f);
        return;
      case "partition":
        this.side = new Set(f.side);
        return;
      case "heal":
        this.side = null;
        return;
    }
  }

  private fire(when: Trigger, node: number): void {
    const i = this.armed.findIndex((t) => t.when === when);
    if (i < 0) return;
    const t = this.armed.splice(i, 1)[0];
    const crashAt = this.now + t.delay;
    this.schedule(crashAt, { k: "fault", fault: { at: crashAt, kind: "crash", node } });
    this.schedule(crashAt + t.downFor, { k: "fault", fault: { at: crashAt + t.downFor, kind: "restart", node } });
  }

  /** The live leader with the highest term, if any. */
  private currentLeader(): number | null {
    let best: number | null = null;
    for (let i = 0; i < this.nodes.length; i++) {
      const n = this.nodes[i];
      if (n?.role === "leader" && (best === null || n.term > this.nodes[best]!.term)) best = i;
    }
    return best;
  }

  private flushFrames(): void {
    if (!this.trace) return;
    for (let i = 0; i < this.s.nodes; i++) {
      if (!this.dirty[i]) continue;
      this.dirty[i] = false;
      const node = this.nodes[i];
      const frames = this.trace.frames[i];
      const prev = frames[frames.length - 1];
      if (!node) {
        frames.push({ t: this.now, up: false, role: "follower", term: prev?.term ?? 0, commit: 0, log: prev?.log ?? [] });
        continue;
      }
      frames.push({
        t: this.now,
        up: true,
        role: node.role,
        term: node.term,
        commit: node.commitIndex,
        log: node.log.map((e) => e.term),
      });
    }
  }
}
