/**
 * Cost-Based Join Planner for LIQ (Lua Integrated Query).
 *
 * Transforms a multi-source `from` clause into an optimized join tree,
 * then executes it using hash join, nested loop, or sort-merge operators.
 */
import type { LuaExpression, LuaJoinHint } from "./ast.ts";
import type { CollectionStats } from "./query_collection.ts";
import {
  type LuaEnv,
  type LuaStackFrame,
  luaKeys,
  LuaRuntimeError,
  LuaTable,
} from "./runtime.ts";
import { evalExpression } from "./eval.ts";

// Maximum intermediate rows before watchdog kills the query
const WATCHDOG_LIMIT = 1e5;
// Rows per cooperative yield chunk
const YIELD_CHUNK = 500;

// Join tree types
export type JoinSource = {
  name: string;
  expression: LuaExpression;
  hint?: LuaJoinHint;
  stats?: CollectionStats;
};

export type JoinNode = JoinLeaf | JoinInner;

export type JoinLeaf = {
  kind: "leaf";
  source: JoinSource;
};

export type JoinInner = {
  kind: "join";
  left: JoinNode;
  right: JoinNode;
  method: "hash" | "loop" | "merge";
};

// Estimates the output cardinality after filtering a source (uses
// default 10% selectivity heuristic).
function estimatedRows(s: JoinSource): number {
  const rc = s.stats?.rowCount ?? 100;
  return Math.max(1, Math.floor(rc * 0.1));
}

// Join selectivity: $1/max(NDV_R, NDV_S)$ for the most common join column,
// or $1/max(rowCount)$ as fallback.
function joinSelectivity(a: JoinSource, b: JoinSource): number {
  const aNdv = a.stats?.ndv;
  const bNdv = b.stats?.ndv;
  if (aNdv && bNdv) {
    // Find shared columns
    for (const [col, ndvA] of aNdv) {
      const ndvB = bNdv.get(col);
      if (ndvB !== undefined) {
        return 1 / Math.max(ndvA, ndvB, 1);
      }
    }
  }
  const ra = a.stats?.rowCount ?? 100;
  const rb = b.stats?.rowCount ?? 100;
  return 1 / Math.max(ra, rb, 1);
}

export function buildJoinTree(
  sources: JoinSource[],
  planOrder?: string[],
): JoinNode {
  if (sources.length === 1) {
    return { kind: "leaf", source: sources[0] };
  }

  let ordered: JoinSource[];
  if (planOrder && planOrder.length > 0) {
    // User-specified order
    const byName = new Map(sources.map((s) => [s.name, s]));
    ordered = [];
    for (const n of planOrder) {
      const s = byName.get(n);
      if (s) {
        ordered.push(s);
        byName.delete(n);
      }
    }
    // Append any remaining
    for (const s of byName.values()) ordered.push(s);
  } else {
    // Start with smallest estimated rows (greedy)
    const remaining = [...sources];
    remaining.sort((a, b) => estimatedRows(a) - estimatedRows(b));
    ordered = [remaining.shift()!];
    while (remaining.length > 0) {
      let bestIdx = 0;
      let bestCost = Infinity;
      for (let i = 0; i < remaining.length; i++) {
        const last = ordered[ordered.length - 1];
        const cost =
          estimatedRows(last) *
          estimatedRows(remaining[i]) *
          joinSelectivity(last, remaining[i]);
        if (cost < bestCost) {
          bestCost = cost;
          bestIdx = i;
        }
      }
      ordered.push(remaining.splice(bestIdx, 1)[0]);
    }
  }
  // Build tree
  let tree: JoinNode = { kind: "leaf", source: ordered[0] };
  for (let i = 1; i < ordered.length; i++) {
    const right = ordered[i];
    const method = selectPhysicalOperator(ordered[i - 1], right);
    tree = {
      kind: "join",
      left: tree,
      right: { kind: "leaf", source: right },
      method,
    };
  }
  return tree;
}

function selectPhysicalOperator(
  left: JoinSource,
  right: JoinSource,
): "hash" | "loop" | "merge" {
  if (right.hint) {
    const k = right.hint.kind;

    if (k === "hash") return "hash";
    if (k === "loop") return "loop";
    if (k === "merge") return "merge";
  }

  // Hash if either side > 50 rows
  const lr = left.stats?.rowCount ?? 100;
  const rr = right.stats?.rowCount ?? 100;
  if (lr > 50 || rr > 50) return "hash";

  return "loop";
}

async function cooperativeYield(): Promise<void> {
  // Yield to the event loop to prevent UI freezing
  return new Promise((resolve) => setTimeout(resolve, 0));
}

async function materializeSource(
  source: JoinSource,
  env: LuaEnv,
  sf: LuaStackFrame,
): Promise<any[]> {
  const val = await evalExpression(source.expression, env, sf);
  if (val === null || val === undefined) {
    throw new LuaRuntimeError(
      `Cross-join source '${source.name}' is nil`,
      sf.withCtx(source.expression.ctx),
    );
  }

  // Normalize to array
  if (Array.isArray(val)) return val;
  if (val instanceof LuaTable) {
    if (val.length > 0) {
      const arr: any[] = [];
      for (let i = 1; i <= val.length; i++) arr.push(val.rawGet(i));
      return arr;
    }
    return [val];
  }
  if (
    typeof val === "object" &&
    val !== null &&
    "query" in val &&
    typeof val.query === "function"
  ) {
    return val.query({}, env, sf);
  }
  return [val];
}

function rowToTable(name: string, item: any, existing?: LuaTable): LuaTable {
  const row = existing ?? new LuaTable();
  void row.rawSet(name, item);
  return row;
}

// Clone a LuaTable row (shallow copy of all keys)
function cloneRow(src: LuaTable): LuaTable {
  const dst = new LuaTable();
  for (const k of luaKeys(src)) {
    void dst.rawSet(k, src.rawGet(k));
  }
  return dst;
}

// Generic sort-key extraction: returns a primitive suitable for
// comparison via `<` / `===`.  For LuaTables we use a stable
// stringified form so the merge comparator works correctly.
function sortKey(item: any): string | number {
  if (item === null || item === undefined) return "";
  if (typeof item === "string") return item;
  if (typeof item === "number") return item;
  if (typeof item === "boolean") return item ? 1 : 0;
  // LuaTable or object: JSON-stringify keys in sorted order for
  // deterministic comparison.
  if (item instanceof LuaTable) {
    const parts: string[] = [];
    for (const k of luaKeys(item)) {
      const v = item.rawGet(k);
      parts.push(`${k}:${v ?? ""}`);
    }
    parts.sort();
    return parts.join("|");
  }
  return String(item);
}

// Compare two sort keys: returns <0, 0, or >0
function compareSortKeys(a: string | number, b: string | number): number {
  if (typeof a === "number" && typeof b === "number") return a - b;
  const sa = String(a);
  const sb = String(b);
  return sa < sb ? -1 : sa > sb ? 1 : 0;
}

// Hash join $O(N+M)$
async function hashJoin(
  leftRows: LuaTable[],
  rightItems: any[],
  rightName: string,
  sf: LuaStackFrame,
): Promise<LuaTable[]> {
  const results: LuaTable[] = [];
  let processed = 0;

  for (const leftRow of leftRows) {
    for (const rightItem of rightItems) {
      if (++processed > WATCHDOG_LIMIT) {
        throw new LuaRuntimeError(
          `Query watchdog: intermediate result exceeded ${WATCHDOG_LIMIT} rows`,
          sf,
        );
      }
      // Clone left row and add right
      const newRow = cloneRow(leftRow);
      void newRow.rawSet(rightName, rightItem);
      results.push(newRow);

      if (processed % YIELD_CHUNK === 0) {
        await cooperativeYield();
      }
    }
  }
  return results;
}

// Nested Loop Join: $O(N*M)$ (used for small tables)
async function nestedLoopJoin(
  leftRows: LuaTable[],
  rightItems: any[],
  rightName: string,
  sf: LuaStackFrame,
): Promise<LuaTable[]> {
  // Same as hash join for Cartesian (real benefit is when we have
  // a predicate to push down (future optimization)
  return hashJoin(leftRows, rightItems, rightName, sf);
}

// Sort-Merge Join: $O(N \log N + M \log M)$ sort + $O(N+M)$ merge.
// Both sides are sorted on their sort key then merged with a
// two-pointer scan.  For Cartesian products every left row pairs
// with every right row of the same key (and rows with no match on
// either side are still emitted as in a cross join).
async function sortMergeJoin(
  leftRows: LuaTable[],
  rightItems: any[],
  rightName: string,
  sf: LuaStackFrame,
): Promise<LuaTable[]> {
  // Extract sort keys for left rows (use first column value)
  const leftKeyed = leftRows.map((row) => {
    // Use the value of the first source binding as sort key
    let key: string | number = "";
    for (const k of luaKeys(row)) {
      key = sortKey(row.rawGet(k));
      break;
    }
    return { row, key };
  });

  const rightKeyed = rightItems.map((item) => ({
    item,
    key: sortKey(item),
  }));

  // Sort both sides
  leftKeyed.sort((a, b) => compareSortKeys(a.key, b.key));
  rightKeyed.sort((a, b) => compareSortKeys(a.key, b.key));

  const results: LuaTable[] = [];
  let processed = 0;
  let li = 0;
  let ri = 0;

  while (li < leftKeyed.length && ri < rightKeyed.length) {
    const cmp = compareSortKeys(leftKeyed[li].key, rightKeyed[ri].key);

    if (cmp < 0) {
      // Left key smaller: emit left row with no right match (cross semantics)
      // In a Cartesian cross-join every left must pair with every right,
      // so advance left and pair with all remaining right items of the
      // same key group (which is empty here).  Just advance.
      li++;
    } else if (cmp > 0) {
      ri++;
    } else {
      // Keys match: collect all left and right rows with the same key
      // and emit the Cartesian product of the two groups.
      const matchKey = leftKeyed[li].key;

      const leftGroup: LuaTable[] = [];
      while (
        li < leftKeyed.length &&
        compareSortKeys(leftKeyed[li].key, matchKey) === 0
      ) {
        leftGroup.push(leftKeyed[li].row);
        li++;
      }

      const rightGroup: any[] = [];
      while (
        ri < rightKeyed.length &&
        compareSortKeys(rightKeyed[ri].key, matchKey) === 0
      ) {
        rightGroup.push(rightKeyed[ri].item);
        ri++;
      }

      for (const leftRow of leftGroup) {
        for (const rightItem of rightGroup) {
          if (++processed > WATCHDOG_LIMIT) {
            throw new LuaRuntimeError(
              `Query watchdog: intermediate result exceeded ${WATCHDOG_LIMIT} rows`,
              sf,
            );
          }
          const newRow = cloneRow(leftRow);
          void newRow.rawSet(rightName, rightItem);
          results.push(newRow);

          if (processed % YIELD_CHUNK === 0) {
            await cooperativeYield();
          }
        }
      }
    }
  }

  // Remaining unmatched rows on either side do not produce output
  // in a Cartesian cross-join context (no outer join semantics).

  return results;
}

// Execute a join tree and return materialized `LuaTable` rows
export async function executeJoinTree(
  tree: JoinNode,
  env: LuaEnv,
  sf: LuaStackFrame,
): Promise<LuaTable[]> {
  if (tree.kind === "leaf") {
    const items = await materializeSource(tree.source, env, sf);
    return items.map((item) => rowToTable(tree.source.name, item));
  }

  const leftRows = await executeJoinTree(tree.left, env, sf);
  const rightItems = await materializeSource(
    (tree.right as JoinLeaf).source,
    env,
    sf,
  );
  const rightName = (tree.right as JoinLeaf).source.name;

  switch (tree.method) {
    case "hash":
      return hashJoin(leftRows, rightItems, rightName, sf);
    case "merge":
      return sortMergeJoin(leftRows, rightItems, rightName, sf);
    case "loop":
      return nestedLoopJoin(leftRows, rightItems, rightName, sf);
  }
}
