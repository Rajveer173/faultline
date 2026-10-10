/**
 * Linearizability checking for a key-value store, one key at a time.
 *
 * Linearizability is compositional (Herlihy & Wing): a history is
 * linearizable iff the sub-history of every object is. So the map is split
 * into independent registers, which keeps each search small.
 *
 * The search is the Wing & Gong algorithm with Lowe's memoisation -- the same
 * approach Porcupine and Knossos use. Walk the history in time order; at each
 * call, try to linearize that operation now; at a return whose call has not
 * been linearized yet, backtrack. A (set of linearized ops, register value)
 * pair that has been seen before is never explored twice.
 */

export interface HistoryOp {
  id: number;
  client: number;
  key: string;
  kind: "put" | "get";
  /** Put: the value written. Get: the value read (null for missing). */
  value: number | null;
  invoke: number;
  /** Infinity when the client gave up and the write may or may not have happened. */
  ret: number;
}

export type LinResult =
  | { ok: true }
  | { ok: false; key: string; op: HistoryOp; explanation: string; related: number[] }
  | { ok: "unknown"; key: string };

interface Ev {
  op: HistoryOp;
  index: number;
  call: boolean;
  match: Ev | null;
  prev: Ev | null;
  next: Ev | null;
}

const STEP_BUDGET = 2_000_000;

export function checkHistory(history: readonly HistoryOp[]): LinResult {
  const byKey = new Map<string, HistoryOp[]>();
  for (const op of history) {
    if (!byKey.has(op.key)) byKey.set(op.key, []);
    byKey.get(op.key)!.push(op);
  }
  for (const key of [...byKey.keys()].sort()) {
    const r = checkRegister(key, byKey.get(key)!);
    if (r.ok !== true) return r;
  }
  return { ok: true };
}

export function checkRegister(key: string, all: readonly HistoryOp[]): LinResult {
  // A write that never returned and whose value nobody read can always be
  // linearized last (its return is at infinity), where it changes nothing.
  // Dropping it is exact, and it is the single biggest cut to the search:
  // every such write could otherwise be placed at any point after its call.
  // Writes carry unique values, so "nobody read it" is a set lookup.
  const read = new Set(all.filter((o) => o.kind === "get").map((o) => o.value));
  const ops = all.filter((o) => o.ret !== Infinity || o.kind !== "put" || read.has(o.value));
  const head = buildList(ops);
  const n = ops.length;
  const linearized = new Uint32Array((n + 31) >>> 5);
  const seen = new Set<string>();
  const stack: { ev: Ev; state: number | null }[] = [];
  let state: number | null = null;
  let ev = head.next;
  let steps = 0;
  let deepest = -1;
  let blocking: HistoryOp | null = null;

  while (head.next) {
    if (++steps > STEP_BUDGET) return { ok: "unknown", key };
    if (!ev) break;
    if (ev.call) {
      const op = ev.op;
      const ok = op.kind === "put" || op.value === state;
      if (ok) {
        const next: number | null = op.kind === "put" ? op.value : state;
        setBit(linearized, ev.index);
        const memo = bitsKey(linearized) + "|" + next;
        if (!seen.has(memo)) {
          seen.add(memo);
          stack.push({ ev, state });
          state = next;
          lift(ev);
          ev = head.next;
          continue;
        }
        clearBit(linearized, ev.index);
      }
      ev = ev.next;
    } else {
      // A return whose call is still unlinearized: everything before it has
      // been tried, so the last choice was wrong.
      if (stack.length > deepest) {
        deepest = stack.length;
        blocking = ev.op;
      }
      const top = stack.pop();
      if (!top) break;
      state = top.state;
      clearBit(linearized, top.ev.index);
      unlift(top.ev);
      ev = top.ev.next;
    }
  }

  if (!head.next) return { ok: true };
  const op = blocking ?? ops[0];
  return { ok: false, key, op, ...explain(op, ops) };
}

function explain(op: HistoryOp, ops: readonly HistoryOp[]): { explanation: string; related: number[] } {
  if (op.kind === "get") {
    // Find the newest write that had fully finished before this read started.
    const before = ops
      .filter((o) => o.kind === "put" && o.ret < op.invoke)
      .sort((a, b) => b.ret - a.ret)[0];
    const source = ops.find((o) => o.kind === "put" && o.value === op.value);
    if (before && before.value !== op.value) {
      const read = op.value === null ? "nothing" : String(op.value);
      return {
        explanation:
          `Client c${op.client} read ${op.key} = ${read} during [${op.invoke}, ${op.ret}] ms, ` +
          `but put ${op.key}=${before.value} had already completed at ${before.ret} ms. ` +
          `The read returned a value that was already overwritten.`,
        related: [op.id, before.id, ...(source ? [source.id] : [])],
      };
    }
  }
  return {
    explanation: `No ordering of the operations on ${op.key} explains what client c${op.client} saw at ${op.ret} ms.`,
    related: [op.id],
  };
}

function buildList(ops: readonly HistoryOp[]): Ev {
  const evs: Ev[] = [];
  ops.forEach((op, index) => {
    const call: Ev = { op, index, call: true, match: null, prev: null, next: null };
    const ret: Ev = { op, index, call: false, match: null, prev: null, next: null };
    call.match = ret;
    evs.push(call, ret);
  });
  const time = (e: Ev) => (e.call ? e.op.invoke : e.op.ret);
  // Ties put calls first, which treats touching operations as concurrent --
  // the lenient reading, so a reported violation is never a timing artefact.
  evs.sort((a, b) => time(a) - time(b) || Number(b.call) - Number(a.call) || a.op.id - b.op.id);
  const head: Ev = { op: null as never, index: -1, call: false, match: null, prev: null, next: null };
  let prev = head;
  for (const e of evs) {
    prev.next = e;
    e.prev = prev;
    prev = e;
  }
  return head;
}

// Remove a call and its matching return from the list; unlift puts them back.
function lift(call: Ev): void {
  const ret = call.match!;
  call.prev!.next = call.next;
  if (call.next) call.next.prev = call.prev;
  ret.prev!.next = ret.next;
  if (ret.next) ret.next.prev = ret.prev;
}

function unlift(call: Ev): void {
  const ret = call.match!;
  ret.prev!.next = ret;
  if (ret.next) ret.next.prev = ret;
  call.prev!.next = call;
  if (call.next) call.next.prev = call;
}

function setBit(b: Uint32Array, i: number): void {
  b[i >>> 5] |= 1 << (i & 31);
}

function clearBit(b: Uint32Array, i: number): void {
  b[i >>> 5] &= ~(1 << (i & 31));
}

function bitsKey(b: Uint32Array): string {
  let s = "";
  for (let i = 0; i < b.length; i++) s += b[i].toString(36) + ",";
  return s;
}
