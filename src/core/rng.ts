// sfc32, seeded through splitmix32 so that adjacent seeds (1, 2, 3, ...) give
// unrelated streams. Every source of randomness in a run comes from one of
// these, which is what makes a run a pure function of its scenario.
export class Rng {
  private a: number;
  private b: number;
  private c: number;
  private d: number;

  constructor(seed: number) {
    let s = seed >>> 0;
    const mix = () => {
      s = (s + 0x9e3779b9) >>> 0;
      let z = s;
      z = Math.imul(z ^ (z >>> 16), 0x85ebca6b);
      z = Math.imul(z ^ (z >>> 13), 0xc2b2ae35);
      return (z ^ (z >>> 16)) >>> 0;
    };
    this.a = mix();
    this.b = mix();
    this.c = mix();
    this.d = mix();
    for (let i = 0; i < 12; i++) this.u32();
  }

  u32(): number {
    const t = (((this.a + this.b) >>> 0) + this.d) >>> 0;
    this.d = (this.d + 1) >>> 0;
    this.a = this.b ^ (this.b >>> 9);
    this.b = (this.c + (this.c << 3)) >>> 0;
    this.c = ((this.c << 21) | (this.c >>> 11)) >>> 0;
    this.c = (this.c + t) >>> 0;
    return t;
  }

  /** Uniform in [0, 1). */
  float(): number {
    return this.u32() / 4294967296;
  }

  /** Uniform integer in [lo, hi). */
  int(lo: number, hi: number): number {
    return lo + Math.floor(this.float() * (hi - lo));
  }

  chance(p: number): boolean {
    return this.float() < p;
  }

  pick<T>(xs: readonly T[]): T {
    return xs[this.int(0, xs.length)];
  }

  /** An independent child stream. */
  fork(): Rng {
    return new Rng(this.u32());
  }
}
