/**
 * Most Common Values (MCV) list for join selectivity estimation.
 *
 * Tracks the top-k most frequent values in a column.
 */

const DEFAULT_MCV_CAPACITY = 128;

export interface MCVConfig {
  capacity?: number;
}

export type MCVEntry = {
  value: string;
  count: number;
};

export class MCVList {
  private counts: Map<string, number>;
  private remainderCount: number;
  public readonly capacity: number;

  constructor(config?: MCVConfig) {
    this.capacity = config?.capacity ?? DEFAULT_MCV_CAPACITY;
    this.counts = new Map();
    this.remainderCount = 0;
  }

  insert(value: string): void {
    const existing = this.counts.get(value);
    if (existing !== undefined) {
      this.counts.set(value, existing + 1);
      return;
    }

    if (this.counts.size < this.capacity) {
      this.counts.set(value, 1);
      return;
    }

    // Full — check if new value should evict the minimum
    let minKey: string | undefined;
    let minCount = Infinity;
    for (const [k, c] of this.counts) {
      if (c < minCount) {
        minCount = c;
        minKey = k;
      }
    }

    this.remainderCount++;

    if (
      minKey !== undefined &&
      minCount <= 1 &&
      this.remainderCount > this.capacity
    ) {
      this.counts.delete(minKey);
      this.remainderCount += minCount; // demote to remainder
      this.counts.set(value, 1);
      this.remainderCount--; // promote from remainder
    }
  }

  delete(value: string): void {
    const existing = this.counts.get(value);
    if (existing !== undefined) {
      if (existing <= 1) {
        this.counts.delete(value);
      } else {
        this.counts.set(value, existing - 1);
      }
      return;
    }
    if (this.remainderCount > 0) {
      this.remainderCount--;
    }
  }

  merge(other: MCVList): void {
    for (const [value, count] of other.counts) {
      const existing = this.counts.get(value);
      if (existing !== undefined) {
        this.counts.set(value, existing + count);
      } else if (this.counts.size < this.capacity) {
        this.counts.set(value, count);
      } else {
        // Try to promote if count is larger than current min
        let minKey: string | undefined;
        let minCount = Infinity;
        for (const [k, c] of this.counts) {
          if (c < minCount) {
            minCount = c;
            minKey = k;
          }
        }
        if (minKey !== undefined && count > minCount) {
          this.remainderCount += minCount;
          this.counts.delete(minKey);
          this.counts.set(value, count);
        } else {
          this.remainderCount += count;
        }
      }
    }
    this.remainderCount += other.remainderCount;
  }

  subtract(other: MCVList): void {
    for (const [value, count] of other.counts) {
      const existing = this.counts.get(value);
      if (existing !== undefined) {
        const newCount = existing - count;
        if (newCount <= 0) {
          this.counts.delete(value);
        } else {
          this.counts.set(value, newCount);
        }
      } else {
        // Was in our remainder
        this.remainderCount = Math.max(0, this.remainderCount - count);
      }
    }
    this.remainderCount = Math.max(
      0,
      this.remainderCount - other.remainderCount,
    );
  }

  totalCount(): number {
    let total = this.remainderCount;
    for (const c of this.counts.values()) {
      total += c;
    }
    return total;
  }

  trackedSize(): number {
    return this.counts.size;
  }

  // Get the top-k entries sorted by count descending
  entries(): MCVEntry[] {
    const result: MCVEntry[] = [];
    for (const [value, count] of this.counts) {
      result.push({ value, count });
    }
    result.sort((a, b) => b.count - a.count);
    return result;
  }

  // Get count for a specific value (0 if not tracked)
  getCount(value: string): number {
    return this.counts.get(value) ?? 0;
  }

  getFrequency(value: string, totalRows: number): number {
    if (totalRows <= 0) return 0;
    return this.getCount(value) / totalRows;
  }

  /**
   * Estimate the fraction of left rows that match any right-side key,
   * given the right side MCV list and total row counts.
   *
   * For keys tracked in both MCVs, use exact counts.
   * For keys tracked only on one side, estimate frequency from the
   * other side remainder using uniform distribution over untracked keys.
   */
  static estimateMatchFraction(
    leftMcv: MCVList | undefined,
    rightMcv: MCVList | undefined,
    leftRows: number,
    rightRows: number,
    leftNdv: number,
    rightNdv: number,
  ): { matchedLeftFraction: number; avgRightRowsPerKey: number } {
    // No MCV available — fall back to uniform
    if (
      !leftMcv ||
      !rightMcv ||
      leftMcv.trackedSize() === 0 ||
      rightMcv.trackedSize() === 0
    ) {
      const matchedLeftFraction =
        leftNdv > 0 ? Math.min(1, rightNdv / leftNdv) : 1;
      const avgRightRowsPerKey =
        rightNdv > 0 ? Math.max(1, rightRows / rightNdv) : 1;
      return { matchedLeftFraction, avgRightRowsPerKey };
    }

    // Average frequency of an untracked value on each side
    const leftUntrackedNdv = Math.max(1, leftNdv - leftMcv.trackedSize());
    const leftUntrackedRows = Math.max(0, leftRows - leftMcv.trackedRowCount());
    const leftAvgPerUntracked =
      leftUntrackedNdv > 0 ? leftUntrackedRows / leftUntrackedNdv : 0;

    const rightUntrackedNdv = Math.max(1, rightNdv - rightMcv.trackedSize());
    const rightUntrackedRows = Math.max(
      0,
      rightRows - rightMcv.trackedRowCount(),
    );
    const rightAvgPerUntracked =
      rightUntrackedNdv > 0 ? rightUntrackedRows / rightUntrackedNdv : 0;

    let matchedLeftRows = 0;
    let matchedOutputRows = 0;

    // Keys tracked on the right side
    for (const rEntry of rightMcv.entries()) {
      const leftCount = leftMcv.getCount(rEntry.value);
      if (leftCount > 0) {
        // Both sides track this value — use exact counts
        matchedLeftRows += leftCount;
        matchedOutputRows += leftCount * rEntry.count;
      } else {
        // Right tracks it, left doesn't — estimate left frequency from remainder
        matchedLeftRows += leftAvgPerUntracked;
        matchedOutputRows += leftAvgPerUntracked * rEntry.count;
      }
    }

    // Keys tracked on the left but not on the right — estimate right frequency
    for (const lEntry of leftMcv.entries()) {
      const rightCount = rightMcv.getCount(lEntry.value);
      if (rightCount > 0) {
        // Already counted above
        continue;
      }
      // Left tracks it, right doesn't — estimate right frequency from remainder
      // But only if this left key plausibly exists on the right side
      // Use probability: rightUntrackedNdv / leftUntrackedNdv as match chance
      const matchProb = Math.min(1, rightUntrackedNdv / leftUntrackedNdv);
      matchedLeftRows += lEntry.count * matchProb;
      matchedOutputRows += lEntry.count * matchProb * rightAvgPerUntracked;
    }

    // Remainder-to-remainder: untracked on both sides
    const remainderMatchFrac = Math.min(
      1,
      rightUntrackedNdv / leftUntrackedNdv,
    );
    matchedLeftRows += leftUntrackedRows * remainderMatchFrac;
    matchedOutputRows +=
      leftUntrackedRows * remainderMatchFrac * rightAvgPerUntracked;

    const matchedLeftFraction =
      leftRows > 0 ? Math.min(1, matchedLeftRows / leftRows) : 0;
    const avgRightRowsPerKey =
      matchedLeftRows > 0
        ? Math.max(1, matchedOutputRows / matchedLeftRows)
        : 1;

    return { matchedLeftFraction, avgRightRowsPerKey };
  }

  trackedRowCount(): number {
    let total = 0;
    for (const c of this.counts.values()) total += c;
    return total;
  }

  remainderForEstimation(): number {
    return this.remainderCount;
  }

  clear(): void {
    this.counts.clear();
    this.remainderCount = 0;
  }

  serialize(): string {
    const entries: [string, number][] = [];
    for (const [k, v] of this.counts) {
      entries.push([k, v]);
    }
    return JSON.stringify({
      c: this.capacity,
      e: entries,
      r: this.remainderCount,
    });
  }

  static deserialize(serialized: string): MCVList {
    const obj = JSON.parse(serialized);
    const mcv = new MCVList({ capacity: obj.c ?? DEFAULT_MCV_CAPACITY });
    for (const [k, v] of obj.e ?? []) {
      mcv.counts.set(k, v);
    }
    mcv.remainderCount = obj.r ?? 0;
    return mcv;
  }
}
