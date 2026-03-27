/**
 * Cost-Based Join Planner for LIQ (Lua Integrated Query).
 *
 * Transforms a multi-source `from` clause into an optimized join tree,
 * then executes it using hash join, nested loop, or sort-merge operators.
 */
import type { LuaExpression, LuaFunctionBody, LuaJoinHint } from "./ast.ts";
import type { CollectionStats } from "./query_collection.ts";
import {
  type LuaEnv,
  LuaFunction,
  type LuaStackFrame,
  type LuaValue,
  luaCall,
  luaKeys,
  LuaRuntimeError,
  LuaTable,
  luaTruthy,
  singleResult,
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

// Estimated output cardinality (default 10 % selectivity heuristic)
function estimatedRows(s: JoinSource): number {
  const rc = s.stats?.rowCount ?? 100;
  return Math.max(1, Math.floor(rc * 0.1));
}

// Join selectivity:
// * $1/\max(\text{NDV}_R, \text{NDV}_S)$ for the first shared column, or
// * $1/\max(\text{rowCount}_R, \text{rowCount}_S)$ as fallback.
function joinSelectivity(a: JoinSource, b: JoinSource): number {
  const aNdv = a.stats?.ndv;
  const bNdv = b.stats?.ndv;
  if (aNdv && bNdv) {
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

// Build join tree
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
    for (const s of byName.values()) ordered.push(s);
  } else {
    // Greedy: start with smallest, pick cheapest next
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

  // Hints are only valid on the right side of a join (i.e. sources at
  // index >= 1 in the ordered list).  If reordering moved a hinted
  // source to position 0, strip the hint (it cannot be honoured as
  // the driving table).
  if (ordered[0].hint) {
    ordered[0] = { ...ordered[0], hint: undefined };
  }

  // Build left-deep tree, tracking accumulated row estimate for the
  // cost-based physical-operator selection.
  let tree: JoinNode = { kind: "leaf", source: ordered[0] };
  let accRows = estimatedRows(ordered[0]);

  for (let i = 1; i < ordered.length; i++) {
    const right = ordered[i];
    const method = selectPhysicalOperator(accRows, right);
    tree = {
      kind: "join",
      left: tree,
      right: { kind: "leaf", source: right },
      method,
    };
    // Cartesian product
    accRows = accRows * estimatedRows(right);
  }
  return tree;
}

// Physical operator selection
function selectPhysicalOperator(
  leftRowCount: number,
  right: JoinSource,
): "hash" | "loop" | "merge" {
  if (right.hint) {
    const k = right.hint.kind;
    if (k === "merge") {
      throw new Error("Merge join requires an equi-join predicate");
    }
    if (k === "hash") {
      if (right.hint.using) {
        throw new Error("'using' clause is only valid with 'loop' join hint");
      }
      return "hash";
    }
    if (k === "loop") return "loop";
  }

  // Default: hash if either side is large
  const rr = right.stats?.rowCount ?? 100;
  if (leftRowCount > 50 || rr > 50) return "hash";
  return "loop";
}

// Cooperative yield
async function cooperativeYield(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

// Materialize a source expression into an array
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

  if (Array.isArray(val)) return val;
  if (val instanceof LuaTable) {
    if (val.length > 0) {
      const arr: any[] = [];
      for (let i = 1; i <= val.length; i++) arr.push(val.rawGet(i));
      return arr;
    }
    if (val.empty()) return [];
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

function rowToTable(name: string, item: any): LuaTable {
  const row = new LuaTable();
  void row.rawSet(name, item);
  return row;
}

function cloneRow(src: LuaTable): LuaTable {
  const dst = new LuaTable();
  for (const k of luaKeys(src)) {
    void dst.rawSet(k, src.rawGet(k));
  }
  return dst;
}

function sortKey(item: any): string | number {
  if (item === null || item === undefined) return "";
  if (typeof item === "string") return item;
  if (typeof item === "number") return item;
  if (typeof item === "boolean") return item ? 1 : 0;
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

function compareSortKeys(a: string | number, b: string | number): number {
  if (typeof a === "number" && typeof b === "number") return a - b;
  const sa = String(a);
  const sb = String(b);
  return sa < sb ? -1 : sa > sb ? 1 : 0;
}

// Cross join (used by both `hash` and plain `loop`)
async function crossJoin(
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

// Predicate loop join: `f(L, R) -> Boolean`
async function predicateLoopJoin(
  leftRows: LuaTable[],
  rightItems: any[],
  rightName: string,
  leftRow2item: (row: LuaTable) => LuaValue,
  predicate: LuaValue,
  sf: LuaStackFrame,
): Promise<LuaTable[]> {
  const results: LuaTable[] = [];
  let processed = 0;

  for (const leftRow of leftRows) {
    const leftItem = leftRow2item(leftRow);
    for (const rightItem of rightItems) {
      const res = singleResult(
        await luaCall(predicate, [leftItem, rightItem], sf.astCtx ?? {}, sf),
      );
      if (!luaTruthy(res)) continue;

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
  return results;
}

// Sort-Merge Join
async function sortMergeJoin(
  leftRows: LuaTable[],
  rightItems: any[],
  rightName: string,
  sf: LuaStackFrame,
): Promise<LuaTable[]> {
  const leftKeyed = leftRows.map((row) => {
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

  leftKeyed.sort((a, b) => compareSortKeys(a.key, b.key));
  rightKeyed.sort((a, b) => compareSortKeys(a.key, b.key));

  const results: LuaTable[] = [];
  let processed = 0;
  let li = 0;
  let ri = 0;

  while (li < leftKeyed.length && ri < rightKeyed.length) {
    const cmp = compareSortKeys(leftKeyed[li].key, rightKeyed[ri].key);

    if (cmp < 0) {
      li++;
    } else if (cmp > 0) {
      ri++;
    } else {
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

  return results;
}

// Resolve `using` predicate from a join hint
function resolveUsingPredicate(
  hint: LuaJoinHint | undefined,
  env: LuaEnv,
  sf: LuaStackFrame,
): LuaValue | null {
  if (!hint || hint.kind !== "loop" || !hint.using) return null;
  if (typeof hint.using === "string") {
    const fn = env.get(hint.using);
    if (!fn) {
      throw new LuaRuntimeError(
        `Join predicate '${hint.using}' is not defined`,
        sf,
      );
    }
    return fn;
  }
  return new LuaFunction(hint.using as LuaFunctionBody, env);
}

// Collect the name of the left-most leaf source
function leftMostSourceName(node: JoinNode): string {
  if (node.kind === "leaf") return node.source.name;
  return leftMostSourceName(node.left);
}

// Execute a join tree
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
  const rightSource = (tree.right as JoinLeaf).source;
  const rightItems = await materializeSource(rightSource, env, sf);
  const rightName = rightSource.name;

  // Loop with `using` predicate
  if (tree.method === "loop") {
    const predicate = resolveUsingPredicate(rightSource.hint, env, sf);
    if (predicate) {
      const leftName = leftMostSourceName(tree.left);
      return predicateLoopJoin(
        leftRows,
        rightItems,
        rightName,
        (row) => row.rawGet(leftName),
        predicate,
        sf,
      );
    }
  }

  switch (tree.method) {
    case "hash":
    case "loop":
      return crossJoin(leftRows, rightItems, rightName, sf);
    case "merge":
      return sortMergeJoin(leftRows, rightItems, rightName, sf);
  }
}
