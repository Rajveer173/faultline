import type { Bugs } from "../bugs.js";
import type { Command, Durable, Entry, Env, Msg, Op, RaftMsg, Role } from "./types.js";

export const ELECTION_MIN = 150;
export const ELECTION_MAX = 300;
export const HEARTBEAT = 50;
/** Entries per AppendEntries. Small on purpose: real systems batch, and batching is where Figure 8 lives. */
export const MAX_BATCH = 4;

interface Session {
  seq: number;
  value: number | null;
}

/**
 * A Raft node, following Figure 2 of the paper. Log indices are 1-based;
 * entry i lives at log[i - 1] and index 0 has term 0.
 *
 * Durability model: every handler finishes its state changes before it sends
 * anything, and the simulator snapshots durable() at the moment of a crash.
 * That is equivalent to an fsync before every send.
 */
export class RaftNode {
  role: Role = "follower";
  term = 0;
  votedFor: number | null = null;
  log: Entry[] = [];
  leader: number | null = null;
  commitIndex = 0;
  lastApplied = 0;

  // State machine: a key-value map plus per-client sessions, so a retried
  // request is applied once however many times it lands in the log.
  readonly kv = new Map<string, number>();
  private readonly sessions = new Map<number, Session>();

  private votes = new Set<number>();
  private nextIndex = new Map<number, number>();
  private matchIndex = new Map<number, number>();
  /** Log index -> client waiting on it. Only meaningful on the leader. */
  private waiting = new Map<number, Command>();

  constructor(
    private readonly env: Env,
    private readonly bugs: Bugs,
    durable?: Durable,
  ) {
    if (durable) {
      this.term = durable.term;
      this.votedFor = durable.votedFor;
      this.log = durable.log.slice();
    }
  }

  start(): void {
    this.env.observe({ type: "role", role: this.role, term: this.term });
    this.resetElectionTimer();
  }

  durable(): Durable {
    return {
      term: this.term,
      votedFor: this.bugs["volatile-vote"] ? null : this.votedFor,
      log: this.log.slice(),
    };
  }

  // ---- log helpers -------------------------------------------------------

  get lastIndex(): number {
    return this.log.length;
  }

  termAt(i: number): number {
    return i === 0 ? 0 : this.log[i - 1]?.term ?? -1;
  }

  private get majority(): number {
    return Math.floor((this.env.peers.length + 1) / 2) + 1;
  }

  // ---- inputs ------------------------------------------------------------

  onTimer(kind: string): void {
    if (kind === "election" && this.role !== "leader") this.startElection();
    else if (kind === "heartbeat" && this.role === "leader") {
      this.broadcastAppend();
      this.env.setTimer("heartbeat", HEARTBEAT);
    }
  }

  onMessage(from: number, msg: Msg): void {
    if (msg.type === "request") return this.onClientRequest(from, msg.seq, msg.op);
    if (msg.type === "response" || msg.type === "redirect") return;

    if (msg.term > this.term) this.becomeFollower(msg.term, null);

    switch (msg.type) {
      case "vote":
        return this.onVote(from, msg);
      case "voteReply":
        return this.onVoteReply(from, msg);
      case "append":
        return this.onAppend(from, msg);
      case "appendReply":
        return this.onAppendReply(from, msg);
    }
  }

  // ---- elections -----------------------------------------------------------

  private resetElectionTimer(): void {
    const spread = ELECTION_MAX - ELECTION_MIN;
    this.env.setTimer("election", ELECTION_MIN + Math.floor(this.env.random() * spread));
  }

  private startElection(): void {
    this.term += 1;
    this.role = "candidate";
    this.votedFor = this.env.id;
    this.leader = null;
    this.votes = new Set([this.env.id]);
    this.env.observe({ type: "role", role: this.role, term: this.term });
    this.resetElectionTimer();
    const lastIndex = this.lastIndex;
    const lastTerm = this.termAt(lastIndex);
    for (const p of this.env.peers) this.env.send(p, { type: "vote", term: this.term, lastIndex, lastTerm });
  }

  private onVote(from: number, m: Extract<RaftMsg, { type: "vote" }>): void {
    let granted = false;
    if (m.term === this.term) {
      const myLastTerm = this.termAt(this.lastIndex);
      const upToDate =
        this.bugs["vote-ignores-log"] ||
        m.lastTerm > myLastTerm ||
        (m.lastTerm === myLastTerm && m.lastIndex >= this.lastIndex);
      if (upToDate && (this.votedFor === null || this.votedFor === from)) {
        this.votedFor = from;
        granted = true;
        this.resetElectionTimer();
      }
    }
    this.env.send(from, { type: "voteReply", term: this.term, granted });
  }

  private onVoteReply(from: number, m: Extract<RaftMsg, { type: "voteReply" }>): void {
    if (this.role !== "candidate" || m.term !== this.term || !m.granted) return;
    this.votes.add(from);
    if (this.votes.size >= this.majority) this.becomeLeader();
  }

  private becomeFollower(term: number, leader: number | null): void {
    const changed = this.role !== "follower" || term !== this.term;
    if (term !== this.term) {
      this.term = term;
      this.votedFor = null;
    }
    this.role = "follower";
    this.leader = leader;
    this.waiting.clear();
    if (changed) this.env.observe({ type: "role", role: this.role, term: this.term });
  }

  private becomeLeader(): void {
    this.role = "leader";
    this.leader = this.env.id;
    for (const p of this.env.peers) {
      this.nextIndex.set(p, this.lastIndex + 1);
      this.matchIndex.set(p, 0);
    }
    this.env.observe({ type: "role", role: this.role, term: this.term });
    this.broadcastAppend();
    this.env.setTimer("heartbeat", HEARTBEAT);
  }

  // ---- replication ---------------------------------------------------------

  private broadcastAppend(): void {
    for (const p of this.env.peers) this.sendAppend(p);
  }

  private sendAppend(to: number): void {
    const next = this.nextIndex.get(to) ?? this.lastIndex + 1;
    const prevIndex = next - 1;
    this.env.send(to, {
      type: "append",
      term: this.term,
      prevIndex,
      prevTerm: this.termAt(prevIndex),
      entries: this.log.slice(prevIndex, prevIndex + MAX_BATCH),
      leaderCommit: this.commitIndex,
    });
  }

  private onAppend(from: number, m: Extract<RaftMsg, { type: "append" }>): void {
    const reply = (success: boolean, match: number, conflict: number) =>
      this.env.send(from, { type: "appendReply", term: this.term, success, match, conflict });

    if (m.term < this.term) return reply(false, 0, 0);
    if (this.role !== "follower" || this.leader !== from) this.becomeFollower(m.term, from);
    this.resetElectionTimer();

    if (m.prevIndex > this.lastIndex) return reply(false, 0, this.lastIndex + 1);
    if (this.termAt(m.prevIndex) !== m.prevTerm) {
      // Skip back over the whole conflicting term in one round trip.
      const bad = this.termAt(m.prevIndex);
      let i = m.prevIndex;
      while (i > 1 && this.termAt(i - 1) === bad) i--;
      return reply(false, 0, i);
    }

    if (this.bugs["truncate-on-append"]) {
      this.log.length = m.prevIndex;
      this.log.push(...m.entries);
    } else {
      for (let k = 0; k < m.entries.length; k++) {
        const idx = m.prevIndex + 1 + k;
        if (idx <= this.lastIndex && this.termAt(idx) !== m.entries[k].term) this.log.length = idx - 1;
        if (idx > this.lastIndex) this.log.push(m.entries[k]);
      }
    }
    this.env.observe({ type: "log", length: this.lastIndex });

    const match = m.prevIndex + m.entries.length;
    if (m.leaderCommit > this.commitIndex) {
      this.commitIndex = Math.max(this.commitIndex, Math.min(m.leaderCommit, match));
      this.env.observe({ type: "commit", index: this.commitIndex });
      this.applyCommitted();
    }
    reply(true, match, 0);
  }

  private onAppendReply(from: number, m: Extract<RaftMsg, { type: "appendReply" }>): void {
    if (this.role !== "leader" || m.term !== this.term) return;
    if (m.success) {
      if (m.match > (this.matchIndex.get(from) ?? 0)) this.matchIndex.set(from, m.match);
      this.nextIndex.set(from, (this.matchIndex.get(from) ?? 0) + 1);
      this.advanceCommit();
      if ((this.nextIndex.get(from) ?? 0) <= this.lastIndex) this.sendAppend(from);
    } else {
      const next = this.nextIndex.get(from) ?? this.lastIndex + 1;
      this.nextIndex.set(from, Math.max(1, Math.min(next - 1, m.conflict || next - 1)));
      this.sendAppend(from);
    }
  }

  private advanceCommit(): void {
    for (let n = this.lastIndex; n > this.commitIndex; n--) {
      // The rule this guards is Figure 8: only entries from the current term
      // are committed by counting; earlier ones commit along with them.
      if (this.termAt(n) !== this.term && !this.bugs["commit-old-term"]) continue;
      let count = 1;
      for (const p of this.env.peers) if ((this.matchIndex.get(p) ?? 0) >= n) count++;
      if (count >= this.majority) {
        this.commitIndex = n;
        this.env.observe({ type: "commit", index: n });
        this.applyCommitted();
        return;
      }
    }
  }

  // ---- state machine -------------------------------------------------------

  private applyCommitted(): void {
    while (this.lastApplied < this.commitIndex) {
      this.lastApplied++;
      const e = this.log[this.lastApplied - 1];
      const value = this.execute(e.cmd);
      this.env.observe({ type: "apply", index: this.lastApplied, term: e.term, cmd: e.cmd });
      const w = this.waiting.get(this.lastApplied);
      if (w) {
        this.waiting.delete(this.lastApplied);
        if (w.client === e.cmd.client && w.seq === e.cmd.seq) {
          this.env.send(w.client, { type: "response", seq: w.seq, value });
        }
      }
    }
  }

  private execute(cmd: Command): number | null {
    const s = this.sessions.get(cmd.client);
    if (s && cmd.seq <= s.seq) return s.value;
    let value: number | null = null;
    if (cmd.op.kind === "put") this.kv.set(cmd.op.key, cmd.op.value);
    else value = this.kv.get(cmd.op.key) ?? null;
    this.sessions.set(cmd.client, { seq: cmd.seq, value });
    return value;
  }

  private onClientRequest(client: number, seq: number, op: Op): void {
    if (this.role !== "leader") {
      this.env.send(client, { type: "redirect", seq, leader: this.leader });
      return;
    }
    const s = this.sessions.get(client);
    if (s && s.seq === seq) {
      this.env.send(client, { type: "response", seq, value: s.value });
      return;
    }
    if (op.kind === "get" && this.bugs["stale-read"]) {
      this.env.send(client, { type: "response", seq, value: this.kv.get(op.key) ?? null });
      return;
    }
    const cmd: Command = { client, seq, op };
    this.log.push({ term: this.term, cmd });
    this.waiting.set(this.lastIndex, cmd);
    this.env.observe({ type: "log", length: this.lastIndex });
    this.broadcastAppend();
  }
}
