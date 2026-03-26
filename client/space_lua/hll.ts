/**
 * HyperLogLog for Number of Distinct Values (NDV) estimation
 */
const P = 6;
const M = 1 << P; // 64 registers
const ALPHA = 0.7213 / (1 + 1.079 / M);

function hash32(key: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < key.length; i++) {
    h ^= key.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

function rho(hash: number): number {
  const w = hash >>> P;
  if (w === 0) return 32 - P + 1;
  return Math.clz32(w) + 1 - P;
}

export class HyperLogLog {
  private registers: Uint8Array;

  constructor() {
    this.registers = new Uint8Array(M);
  }

  add(value: string): void {
    const h = hash32(value);
    const idx = h & (M - 1);
    const r = rho(h);
    if (r > this.registers[idx]) {
      this.registers[idx] = r;
    }
  }

  estimate(): number {
    let sum = 0;
    let zeros = 0;
    for (let i = 0; i < M; i++) {
      sum += 2 ** -this.registers[i];
      if (this.registers[i] === 0) zeros++;
    }
    let est = ALPHA * M * M / sum;
    // Small range correction
    if (est <= 2.5 * M && zeros > 0) {
      est = M * Math.log(M / zeros);
    }
    return Math.round(est);
  }

  clear(): void {
    this.registers.fill(0);
  }

  merge(other: HyperLogLog): void {
    for (let i = 0; i < M; i++) {
      if (other.registers[i] > this.registers[i]) {
        this.registers[i] = other.registers[i];
      }
    }
  }
}
