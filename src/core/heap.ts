// Binary min-heap ordered by (at, seq). seq is a global insertion counter, so
// two events scheduled for the same millisecond always fire in the order they
// were scheduled -- ties never fall back to anything nondeterministic.
export interface Timed {
  at: number;
  seq: number;
}

export class EventHeap<T extends Timed> {
  private items: T[] = [];

  get size(): number {
    return this.items.length;
  }

  push(item: T): void {
    const xs = this.items;
    xs.push(item);
    let i = xs.length - 1;
    while (i > 0) {
      const p = (i - 1) >> 1;
      if (!less(xs[i], xs[p])) break;
      [xs[i], xs[p]] = [xs[p], xs[i]];
      i = p;
    }
  }

  pop(): T | undefined {
    const xs = this.items;
    if (xs.length === 0) return undefined;
    const top = xs[0];
    const last = xs.pop()!;
    if (xs.length > 0) {
      xs[0] = last;
      let i = 0;
      for (;;) {
        const l = 2 * i + 1;
        const r = l + 1;
        let m = i;
        if (l < xs.length && less(xs[l], xs[m])) m = l;
        if (r < xs.length && less(xs[r], xs[m])) m = r;
        if (m === i) break;
        [xs[i], xs[m]] = [xs[m], xs[i]];
        i = m;
      }
    }
    return top;
  }

  peek(): T | undefined {
    return this.items[0];
  }
}

function less(a: Timed, b: Timed): boolean {
  return a.at < b.at || (a.at === b.at && a.seq < b.seq);
}
