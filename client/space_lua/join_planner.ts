/**
 * Cost-Based Join Planner for LIQ (Lua Integrated Query).
 *
 * Transforms a multi-source `from` clause into an optimized join tree,
 * then executes it using hash join, nested loop, or sort-merge operators.
 *
 * Layout:
 *   1. Types & constants
 *   2. Cardinality & selectivity estimation
 *   3. Join tree construction
 *   4. Physical operator selection
 *   5. Row helpers & materialization
 *   6. Join operators (inner / semi / anti)
 *   7. Join tree execution
 *   8. Equi-predicate extraction
 *   9. EXPLAIN infrastructure
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

// ─── 1. Types & constants ───────────────────────────────────────────

// Defaults tuned for typical SilverBullet workloads:
// hundreds to low thousands of pages, tens of tags,
// objects-per-tag ranging from tens to low thousands.
const DEFAULT_WATCHDOG_LIMIT = 5e5;
const DEFAULT_YIELD_CHUNK = 5000;
const DEFAULT_SELECTIVITY = 0.01;
const SMALL_TABLE_THRESHOLD = 20;

export type JoinPlannerConfig = {
  watchdogLimit?: number;
  yieldChunk?: number;
};

function getWatchdogLimit(config?: JoinPlannerConfig): number {
  return config?.watchdogLimit ?? DEFAULT_WATCHDOG_LIMIT;
}

function getYieldChunk(config?: JoinPlannerConfig): number {
  return config?.yieldChunk ?? DEFAULT_YIELD_CHUNK;
}

export type JoinType = "inner" | "semi" | "anti";

export type JoinSource = {
  name: string;
  expression: LuaExpression;
  hint?: LuaJoinHint;
  stats?: CollectionStats;
  joinType?: JoinType;
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
  joinType: JoinType;
  equiPred?: EquiPredicate;
};

export type EquiPredicate = {
  leftSource: string;
  leftColumn: string;
  rightSource: string;
  rightColumn: string;
};

export type OpStats = {
  actualRows: number;
  loops: number;
  rebinds: number;
  startTimeMs: number;
  endTimeMs: number;
  peakMemoryRows: number;
};

// ─── Explain types ──────────────────────────────────────────────────

export type ExplainNodeType =
  | "Scan"
  | "HashJoin"
  | "NestedLoop"
  | "MergeJoin"
  | "Sort"
  | "Limit"
  | "GroupAggregate"
  | "Unique";

export type ExplainNode = {
  nodeType: ExplainNodeType;
  joinType?: JoinType;
  source?: string;
  method?: "hash" | "loop" | "merge";
  hintUsed?: string;
  startupCost: number;
  estimatedCost: number;
  estimatedRows: number;
  estimatedWidth: number;
  actualRows?: number;
  actualLoops?: number;
  actualStartupTimeMs?: number;
  actualTimeMs?: number;
  memoryRows?: number;
  rowsRemovedByFilter?: number;
  equiPred?: EquiPredicate;
  filterExpr?: string;
  sortKeys?: string[];
  limitCount?: number;
  offsetCount?: number;
  children: ExplainNode[];
};

export type ExplainOptions = {
  analyze: boolean;
  costs: boolean;
  timing: boolean;
};

export type ExplainResult = {
  plan: ExplainNode;
  planningTimeMs: number;
  executionTimeMs?: number;
};

// ─── 2. Cardinality & selectivity estimation ────────────────────────

function estimatedRows(s: JoinSource): number {
  return s.stats?.rowCount ?? 100;
}

function estimateJoinCardinality(
  leftCard: number,
  rightCard: number,
  joinType: JoinType,
  selectivity: number,
): number {
  switch (joinType) {
    case "inner":
      return leftCard * rightCard * selectivity;
    case "semi":
      return Math.min(
        leftCard,
        leftCard * Math.min(1, rightCard * selectivity),
      );
    case "anti":
      return leftCard * Math.max(0, 1 - Math.min(1, rightCard * selectivity));
  }
}

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

function collectSourceNames(node: JoinNode): Set<string> {
  const names = new Set<string>();
  const walk = (n: JoinNode) => {
    if (n.kind === "leaf") names.add(n.source.name);
    else {
      walk(n.left);
      walk(n.right);
    }
  };
  walk(node);
  return names;
}

// ─── 3. Join tree construction ──────────────────────────────────────

export function buildJoinTree(
  sources: JoinSource[],
  planOrder?: string[],
  equiPreds?: EquiPredicate[],
): JoinNode {
  if (sources.length === 1) {
    return { kind: "leaf", source: sources[0] };
  }

  const ordered = orderSources(sources, planOrder);

  // Strip hint from driving table (position 0)
  if (ordered[0].hint) {
    ordered[0] = { ...ordered[0], hint: undefined };
  }

  let tree: JoinNode = { kind: "leaf", source: ordered[0] };
  let accRows = estimatedRows(ordered[0]);

  for (let i = 1; i < ordered.length; i++) {
    const right = ordered[i];
    const jt = right.hint?.joinType ?? "inner";
    const sel = joinSelectivity(ordered[i - 1], right);
    const method = selectPhysicalOperator(accRows, right, jt);
    const equiPred = findEquiPred(tree, right.name, equiPreds);

    tree = {
      kind: "join",
      left: tree,
      right: { kind: "leaf", source: right },
      method,
      joinType: jt,
      equiPred,
    };
    accRows = estimateJoinCardinality(accRows, estimatedRows(right), jt, sel);
  }
  return tree;
}

function orderSources(
  sources: JoinSource[],
  planOrder?: string[],
): JoinSource[] {
  if (planOrder && planOrder.length > 0) {
    const byName = new Map(sources.map((s) => [s.name, s]));
    const ordered: JoinSource[] = [];
    for (const n of planOrder) {
      const s = byName.get(n);
      if (s) {
        ordered.push(s);
        byName.delete(n);
      }
    }
    for (const s of byName.values()) ordered.push(s);
    return ordered;
  }

  // Greedy: start with smallest, pick cheapest next
  const remaining = [...sources];
  remaining.sort((a, b) => estimatedRows(a) - estimatedRows(b));
  const ordered = [remaining.shift()!];
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
  return ordered;
}

function findEquiPred(
  tree: JoinNode,
  rightName: string,
  equiPreds?: EquiPredicate[],
): EquiPredicate | undefined {
  if (!equiPreds) return undefined;
  const joinedNames = collectSourceNames(tree);
  const pred = equiPreds.find(
    (ep) =>
      (joinedNames.has(ep.leftSource) && ep.rightSource === rightName) ||
      (joinedNames.has(ep.rightSource) && ep.leftSource === rightName),
  );
  if (!pred) return undefined;
  if (joinedNames.has(pred.leftSource)) return pred;
  return {
    leftSource: pred.rightSource,
    leftColumn: pred.rightColumn,
    rightSource: pred.leftSource,
    rightColumn: pred.leftColumn,
  };
}

// ─── 4. Physical operator selection ─────────────────────────────────

function selectPhysicalOperator(
  leftRowCount: number,
  right: JoinSource,
  joinType: JoinType = "inner",
): "hash" | "loop" | "merge" {
  if (right.hint) {
    const k = right.hint.kind;
    if (k === "merge") throw new Error("Merge join requires equi-predicate");
    if (k === "hash") {
      if (right.hint.using)
        throw new Error("'using' only valid with 'loop' hint");
      return "hash";
    }
    if (k === "loop") return "loop";
  }

  const rr = right.stats?.rowCount ?? 100;
  if (rr <= SMALL_TABLE_THRESHOLD) return "loop";

  const hashCost = rr + leftRowCount;
  const discount = joinType === "inner" ? 1.0 : 0.5;
  const nljCost = leftRowCount * rr * discount;
  return hashCost < nljCost ? "hash" : "loop";
}

// ─── 5. Row helpers & materialization ───────────────────────────────

async function cooperativeYield(): Promise<void> {
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

function extractField(obj: any, column: string): any {
  if (obj === null || obj === undefined) return null;
  if (obj instanceof LuaTable) return obj.rawGet(column);
  if (typeof obj === "object") return obj[column];
  return null;
}

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

function leftMostSourceName(node: JoinNode): string {
  if (node.kind === "leaf") return node.source.name;
  return leftMostSourceName(node.left);
}

// ─── 6. Join operators ──────────────────────────────────────────────

// --- Semi / Anti ---

async function hashSemiAntiJoin(
  leftRows: LuaTable[],
  rightItems: any[],
  _rightName: string,
  joinType: "semi" | "anti",
  equiPred: EquiPredicate | undefined,
  _sf: LuaStackFrame,
): Promise<LuaTable[]> {
  if (!equiPred) {
    return nestedLoopSemiAntiJoin(leftRows, rightItems, joinType);
  }

  // BUILD: existence set on right join key
  const buildSet = new Set<string>();
  for (const rItem of rightItems) {
    const val = extractField(rItem, equiPred.rightColumn);
    if (val !== null && val !== undefined) {
      buildSet.add(String(val));
    }
  }

  // PROBE
  const results: LuaTable[] = [];
  for (const lRow of leftRows) {
    const leftObj = lRow.rawGet(equiPred.leftSource);
    const val = extractField(leftObj, equiPred.leftColumn);
    const hasMatch =
      val !== null && val !== undefined && buildSet.has(String(val));

    if (joinType === "semi" && hasMatch) results.push(lRow);
    else if (joinType === "anti" && !hasMatch) results.push(lRow);
  }
  return results;
}

function nestedLoopSemiAntiJoin(
  leftRows: LuaTable[],
  rightItems: any[],
  joinType: "semi" | "anti",
): LuaTable[] {
  const results: LuaTable[] = [];
  const hasRight = rightItems.length > 0;
  for (const lRow of leftRows) {
    if (joinType === "semi" && hasRight) results.push(lRow);
    else if (joinType === "anti" && !hasRight) results.push(lRow);
  }
  return results;
}

async function predicateLoopSemiAntiJoin(
  leftRows: LuaTable[],
  rightItems: any[],
  _rightName: string,
  leftRow2item: (row: LuaTable) => LuaValue,
  predicate: LuaValue,
  joinType: "semi" | "anti",
  sf: LuaStackFrame,
): Promise<LuaTable[]> {
  const results: LuaTable[] = [];
  for (const leftRow of leftRows) {
    const leftItem = leftRow2item(leftRow);
    let found = false;
    for (const rightItem of rightItems) {
      const res = singleResult(
        await luaCall(predicate, [leftItem, rightItem], sf.astCtx ?? {}, sf),
      );
      if (luaTruthy(res)) {
        found = true;
        break;
      }
    }
    if (joinType === "semi" && found) results.push(leftRow);
    else if (joinType === "anti" && !found) results.push(leftRow);
  }
  return results;
}

// --- Inner ---

async function crossJoin(
  leftRows: LuaTable[],
  rightItems: any[],
  rightName: string,
  sf: LuaStackFrame,
  config?: JoinPlannerConfig,
): Promise<LuaTable[]> {
  const limit = getWatchdogLimit(config);
  const chunk = getYieldChunk(config);
  const results: LuaTable[] = [];
  let processed = 0;
  for (const leftRow of leftRows) {
    for (const rightItem of rightItems) {
      if (++processed > limit) {
        throw new LuaRuntimeError(
          `Query watchdog: intermediate result exceeded ${limit} rows`,
          sf,
        );
      }
      const newRow = cloneRow(leftRow);
      void newRow.rawSet(rightName, rightItem);
      results.push(newRow);
      if (processed % chunk === 0) await cooperativeYield();
    }
  }
  return results;
}

async function hashInnerJoin(
  leftRows: LuaTable[],
  rightItems: any[],
  rightName: string,
  equiPred: EquiPredicate,
  sf: LuaStackFrame,
  config?: JoinPlannerConfig,
): Promise<LuaTable[]> {
  const limit = getWatchdogLimit(config);
  const chunk = getYieldChunk(config);
  // BUILD: multi-map on right key
  const buildMap = new Map<string, any[]>();
  for (const rItem of rightItems) {
    const val = extractField(rItem, equiPred.rightColumn);
    if (val === null || val === undefined) continue;
    const key = String(val);
    let bucket = buildMap.get(key);
    if (!bucket) {
      bucket = [];
      buildMap.set(key, bucket);
    }
    bucket.push(rItem);
  }

  // PROBE
  const results: LuaTable[] = [];
  let processed = 0;
  for (const lRow of leftRows) {
    const leftObj = lRow.rawGet(equiPred.leftSource);
    const val = extractField(leftObj, equiPred.leftColumn);
    if (val === null || val === undefined) continue;
    const bucket = buildMap.get(String(val));
    if (!bucket) continue;
    for (const rItem of bucket) {
      if (++processed > limit) {
        throw new LuaRuntimeError(
          `Query watchdog: intermediate result exceeded ${limit} rows`,
          sf,
        );
      }
      const newRow = cloneRow(lRow);
      void newRow.rawSet(rightName, rItem);
      results.push(newRow);
      if (processed % chunk === 0) await cooperativeYield();
    }
  }
  return results;
}

async function predicateLoopJoin(
  leftRows: LuaTable[],
  rightItems: any[],
  rightName: string,
  leftRow2item: (row: LuaTable) => LuaValue,
  predicate: LuaValue,
  sf: LuaStackFrame,
  config?: JoinPlannerConfig,
): Promise<LuaTable[]> {
  const limit = getWatchdogLimit(config);
  const chunk = getYieldChunk(config);
  const results: LuaTable[] = [];
  let processed = 0;
  for (const leftRow of leftRows) {
    const leftItem = leftRow2item(leftRow);
    for (const rightItem of rightItems) {
      const res = singleResult(
        await luaCall(predicate, [leftItem, rightItem], sf.astCtx ?? {}, sf),
      );
      if (!luaTruthy(res)) continue;
      if (++processed > limit) {
        throw new LuaRuntimeError(
          `Query watchdog: intermediate result exceeded ${limit} rows`,
          sf,
        );
      }
      const newRow = cloneRow(leftRow);
      void newRow.rawSet(rightName, rightItem);
      results.push(newRow);
      if (processed % chunk === 0) await cooperativeYield();
    }
  }
  return results;
}

async function sortMergeJoin(
  leftRows: LuaTable[],
  rightItems: any[],
  rightName: string,
  sf: LuaStackFrame,
  config?: JoinPlannerConfig,
): Promise<LuaTable[]> {
  const limit = getWatchdogLimit(config);
  const chunk = getYieldChunk(config);
  const leftKeyed = leftRows.map((row) => {
    let key: string | number = "";
    for (const k of luaKeys(row)) {
      key = sortKey(row.rawGet(k));
      break;
    }
    return { row, key };
  });
  const rightKeyed = rightItems.map((item) => ({ item, key: sortKey(item) }));

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
      continue;
    }
    if (cmp > 0) {
      ri++;
      continue;
    }

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
        if (++processed > limit) {
          throw new LuaRuntimeError(
            `Query watchdog: intermediate result exceeded ${limit} rows`,
            sf,
          );
        }
        const newRow = cloneRow(leftRow);
        void newRow.rawSet(rightName, rightItem);
        results.push(newRow);
        if (processed % chunk === 0) await cooperativeYield();
      }
    }
  }
  return results;
}

// ─── Dispatch (routes to the correct operator) ──────────────────────

async function dispatchJoin(
  tree: JoinInner,
  leftRows: LuaTable[],
  rightItems: any[],
  rightSource: JoinSource,
  env: LuaEnv,
  sf: LuaStackFrame,
  config?: JoinPlannerConfig,
): Promise<LuaTable[]> {
  const rightName = rightSource.name;
  const joinType = tree.joinType ?? "inner";

  // Semi/Anti
  if (joinType === "semi" || joinType === "anti") {
    if (tree.method === "loop") {
      const predicate = resolveUsingPredicate(rightSource.hint, env, sf);
      if (predicate) {
        const leftName = leftMostSourceName(tree.left);
        return predicateLoopSemiAntiJoin(
          leftRows,
          rightItems,
          rightName,
          (row) => row.rawGet(leftName),
          predicate,
          joinType,
          sf,
        );
      }
    }
    return hashSemiAntiJoin(
      leftRows,
      rightItems,
      rightName,
      joinType,
      tree.equiPred,
      sf,
    );
  }

  // Inner with using predicate
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
        config,
      );
    }
  }

  // Inner by method
  switch (tree.method) {
    case "hash":
      if (tree.equiPred) {
        return hashInnerJoin(
          leftRows,
          rightItems,
          rightName,
          tree.equiPred,
          sf,
          config,
        );
      }
      return crossJoin(leftRows, rightItems, rightName, sf, config);
    case "loop":
      return crossJoin(leftRows, rightItems, rightName, sf, config);
    case "merge":
      return sortMergeJoin(leftRows, rightItems, rightName, sf, config);
  }
}

// ─── 7. Join tree execution ─────────────────────────────────────────

export async function executeJoinTree(
  tree: JoinNode,
  env: LuaEnv,
  sf: LuaStackFrame,
  config?: JoinPlannerConfig,
): Promise<LuaTable[]> {
  if (tree.kind === "leaf") {
    const items = await materializeSource(tree.source, env, sf);
    return items.map((item) => rowToTable(tree.source.name, item));
  }
  const leftRows = await executeJoinTree(tree.left, env, sf, config);
  const rightSource = (tree.right as JoinLeaf).source;
  const rightItems = await materializeSource(rightSource, env, sf);
  return dispatchJoin(tree, leftRows, rightItems, rightSource, env, sf, config);
}

// ─── 8. Equi-predicate extraction ──────────────────────────────────

export function extractEquiPredicates(
  expr: LuaExpression | undefined,
  sourceNames: Set<string>,
): EquiPredicate[] {
  if (!expr) return [];
  const result: EquiPredicate[] = [];
  collectEquiJoins(expr, sourceNames, result);
  return result;
}

function collectEquiJoins(
  expr: LuaExpression,
  sourceNames: Set<string>,
  out: EquiPredicate[],
): void {
  if (expr.type !== "Binary") return;
  if (expr.operator === "and") {
    collectEquiJoins(expr.left, sourceNames, out);
    collectEquiJoins(expr.right, sourceNames, out);
    return;
  }
  if (expr.operator === "==") {
    const left = parseSourceColumn(expr.left, sourceNames);
    const right = parseSourceColumn(expr.right, sourceNames);
    if (left && right && left.source !== right.source) {
      out.push({
        leftSource: left.source,
        leftColumn: left.column,
        rightSource: right.source,
        rightColumn: right.column,
      });
    }
  }
}

function parseSourceColumn(
  expr: LuaExpression,
  sourceNames: Set<string>,
): { source: string; column: string } | null {
  if (expr.type !== "PropertyAccess") return null;
  if (expr.object.type !== "Variable") return null;
  const source = expr.object.name;
  if (!sourceNames.has(source)) return null;
  return { source, column: expr.property };
}

// ─── 9. EXPLAIN infrastructure ──────────────────────────────────────

// --- Plan construction ---

export function explainJoinTree(
  tree: JoinNode,
  _opts: ExplainOptions,
): ExplainNode {
  if (tree.kind === "leaf") {
    const rows = estimatedRows(tree.source);
    const width = tree.source.stats?.avgColumnCount ?? 5;
    return {
      nodeType: "Scan",
      source: tree.source.name,
      hintUsed: tree.source.hint
        ? formatHintLabel(tree.source.hint)
        : undefined,
      startupCost: 0,
      estimatedCost: rows,
      estimatedRows: rows,
      estimatedWidth: width,
      children: [],
    };
  }

  const leftPlan = explainJoinTree(tree.left, _opts);
  const rightPlan = explainJoinTree(tree.right, _opts);
  const jt = tree.joinType ?? "inner";

  const nodeType: ExplainNodeType =
    tree.method === "hash"
      ? "HashJoin"
      : tree.method === "merge"
        ? "MergeJoin"
        : "NestedLoop";

  const sel = DEFAULT_SELECTIVITY;
  const estRows = estimateJoinCardinality(
    leftPlan.estimatedRows,
    rightPlan.estimatedRows,
    jt,
    sel,
  );

  let startupCost: number;
  let totalCost: number;
  if (tree.method === "hash") {
    startupCost = rightPlan.estimatedCost + rightPlan.estimatedRows;
    totalCost = startupCost + leftPlan.estimatedCost + leftPlan.estimatedRows;
  } else {
    startupCost = leftPlan.startupCost;
    const discount = jt === "inner" ? 1.0 : 0.5;
    totalCost =
      leftPlan.estimatedCost +
      leftPlan.estimatedRows * rightPlan.estimatedRows * discount;
  }

  const rightSource =
    tree.right.kind === "leaf" ? tree.right.source : undefined;
  const hintLabel = rightSource?.hint
    ? formatHintLabel(rightSource.hint)
    : undefined;

  const width = leftPlan.estimatedWidth + rightPlan.estimatedWidth;

  return {
    nodeType,
    joinType: jt,
    method: tree.method,
    hintUsed: hintLabel,
    equiPred: tree.equiPred,
    startupCost: Math.round(startupCost),
    estimatedCost: Math.round(totalCost),
    estimatedRows: Math.max(1, Math.round(estRows)),
    estimatedWidth: width,
    children: [leftPlan, rightPlan],
  };
}

/**
 * Wrap a join plan with Sort / Limit / GroupAggregate nodes
 * based on the query clauses, mirroring Postgres-style plans.
 */
export function wrapPlanWithQueryOps(
  plan: ExplainNode,
  query: {
    orderBy?: { expr: LuaExpression; desc: boolean }[];
    limit?: number;
    offset?: number;
    groupBy?: { expr: LuaExpression; alias?: string }[];
    where?: LuaExpression;
    having?: LuaExpression;
    distinct?: boolean;
  },
): ExplainNode {
  let root = plan;

  // WHERE filter annotation — strip predicates already used as join conditions
  if (query.where) {
    const usedPreds = collectUsedEquiPreds(root);
    const residual = stripEquiPreds(query.where, usedPreds);
    if (residual) {
      root.filterExpr = exprToString(residual);
    }
  }

  // GroupAggregate
  if (query.groupBy && query.groupBy.length > 0) {
    const keys = query.groupBy.map((g) => g.alias ?? exprToString(g.expr));
    root = {
      nodeType: "GroupAggregate",
      startupCost: root.estimatedCost,
      estimatedCost: root.estimatedCost + root.estimatedRows,
      estimatedRows: Math.max(1, Math.round(root.estimatedRows * 0.5)),
      estimatedWidth: root.estimatedWidth,
      sortKeys: keys,
      children: [root],
    };
  }

  // HAVING → annotate as filter on GroupAggregate node
  if (query.having) {
    root.filterExpr = exprToString(query.having);
  }

  // Distinct → Unique node
  if (query.distinct) {
    root = {
      nodeType: "Unique",
      startupCost: root.startupCost,
      estimatedCost: root.estimatedCost,
      estimatedRows: Math.max(1, Math.round(root.estimatedRows * 0.8)),
      estimatedWidth: root.estimatedWidth,
      children: [root],
    };
  }

  // Sort (ORDER BY)
  if (query.orderBy && query.orderBy.length > 0) {
    const keys = query.orderBy.map(
      (o) => exprToString(o.expr) + (o.desc ? " desc" : ""),
    );
    const nLogN =
      root.estimatedRows *
      Math.ceil(Math.log2(Math.max(2, root.estimatedRows)));
    root = {
      nodeType: "Sort",
      startupCost: root.estimatedCost,
      estimatedCost: root.estimatedCost + nLogN,
      estimatedRows: root.estimatedRows,
      estimatedWidth: root.estimatedWidth,
      sortKeys: keys,
      children: [root],
    };
  }

  // Limit / Offset
  if (query.limit !== undefined || query.offset !== undefined) {
    const offset = query.offset ?? 0;
    const limit = query.limit ?? root.estimatedRows;
    const startupFraction =
      offset > 0
        ? root.startupCost +
          (root.estimatedCost - root.startupCost) *
            (offset / Math.max(1, root.estimatedRows))
        : root.startupCost;
    root = {
      nodeType: "Limit",
      startupCost: Math.round(startupFraction),
      estimatedCost: root.estimatedCost,
      estimatedRows: Math.min(limit, Math.max(0, root.estimatedRows - offset)),
      estimatedWidth: root.estimatedWidth,
      limitCount: query.limit,
      offsetCount: query.offset,
      children: [root],
    };
  }

  return root;
}

// --- Helpers ---

function formatHintLabel(hint: LuaJoinHint): string {
  const parts: string[] = [];
  if (hint.joinType) parts.push(hint.joinType);
  parts.push(hint.kind);
  if (hint.using) parts.push("using");
  return parts.join(" ");
}

/** Best-effort textual representation of an expression for EXPLAIN. */
export function exprToString(expr: LuaExpression): string {
  switch (expr.type) {
    case "Binary":
      return `(${exprToString(expr.left)} ${expr.operator} ${exprToString(expr.right)})`;
    case "Unary":
      return `${expr.operator} ${exprToString(expr.argument)}`;
    case "PropertyAccess":
      return `${exprToString(expr.object)}.${expr.property}`;
    case "Variable":
      return expr.name;
    case "String":
      return `'${expr.value}'`;
    case "Number":
      return String(expr.value);
    case "Boolean":
      return String(expr.value);
    case "Nil":
      return "nil";
    case "FunctionCall": {
      const prefix = exprToString(expr.prefix);
      const args = expr.args.map(exprToString).join(", ");
      return `${prefix}(${args})`;
    }
    case "TableAccess":
      return `${exprToString(expr.object)}[${exprToString(expr.key)}]`;
    default:
      return "?";
  }
}

/** Collect all equi-predicates used in the plan tree. */
function collectUsedEquiPreds(node: ExplainNode): EquiPredicate[] {
  const result: EquiPredicate[] = [];
  if (node.equiPred) result.push(node.equiPred);
  for (const child of node.children) {
    result.push(...collectUsedEquiPreds(child));
  }
  return result;
}

/** Check if an expression matches a known equi-predicate. */
function exprMatchesEquiPred(expr: LuaExpression, preds: EquiPredicate[]): boolean {
  if (expr.type !== "Binary" || expr.operator !== "==") return false;
  const left = parseSourceColumnFromExpr(expr.left);
  const right = parseSourceColumnFromExpr(expr.right);
  if (!left || !right) return false;
  return preds.some((ep) =>
    (ep.leftSource === left.source && ep.leftColumn === left.column &&
     ep.rightSource === right.source && ep.rightColumn === right.column) ||
    (ep.leftSource === right.source && ep.leftColumn === right.column &&
     ep.rightSource === left.source && ep.rightColumn === left.column)
  );
}

/** Parse source.column from an expression (no sourceNames filter). */
function parseSourceColumnFromExpr(
  expr: LuaExpression,
): { source: string; column: string } | null {
  if (expr.type !== "PropertyAccess") return null;
  if (expr.object.type !== "Variable") return null;
  return { source: expr.object.name, column: expr.property };
}

/**
 * Strip equi-predicate terms from a WHERE expression,
 * returning the residual expression or undefined if nothing remains.
 */
function stripEquiPreds(
  expr: LuaExpression,
  preds: EquiPredicate[],
): LuaExpression | undefined {
  if (expr.type === "Binary" && expr.operator === "and") {
    const left = stripEquiPreds(expr.left, preds);
    const right = stripEquiPreds(expr.right, preds);
    if (!left && !right) return undefined;
    if (!left) return right;
    if (!right) return left;
    return { ...expr, left, right };
  }
  if (exprMatchesEquiPred(expr, preds)) return undefined;
  return expr;
}

// --- EXPLAIN ANALYZE execution ---

export async function executeAndInstrument(
  tree: JoinNode,
  plan: ExplainNode,
  env: LuaEnv,
  sf: LuaStackFrame,
  opts: ExplainOptions,
  config?: JoinPlannerConfig,
): Promise<LuaTable[]> {
  const t0 = opts.timing ? performance.now() : 0;

  if (tree.kind === "leaf") {
    const items = await materializeSource(tree.source, env, sf);
    const rows = items.map((item) => rowToTable(tree.source.name, item));
    plan.actualRows = rows.length;
    plan.actualLoops = 1;
    if (opts.timing) {
      const elapsed = Math.round((performance.now() - t0) * 1000) / 1000;
      plan.actualStartupTimeMs = elapsed;
      plan.actualTimeMs = elapsed;
    }
    return rows;
  }

  // Recurse left
  const leftRows = await executeAndInstrument(
    tree.left,
    plan.children[0],
    env,
    sf,
    opts,
    config,
  );

  // Recurse right (materialize)
  const rightSource = (tree.right as JoinLeaf).source;
  const rightT0 = opts.timing ? performance.now() : 0;
  const rightItems = await materializeSource(rightSource, env, sf);
  plan.children[1].actualRows = rightItems.length;
  plan.children[1].actualLoops = 1;
  if (opts.timing) {
    const rightElapsed =
      Math.round((performance.now() - rightT0) * 1000) / 1000;
    plan.children[1].actualStartupTimeMs = rightElapsed;
    plan.children[1].actualTimeMs = rightElapsed;
  }

  // Startup time = everything up to this point (child materialization)
  const joinT0 = opts.timing ? performance.now() : 0;

  // Execute join operator
  const joinResult = await dispatchJoin(
    tree,
    leftRows,
    rightItems,
    rightSource,
    env,
    sf,
    config,
  );

  plan.actualRows = joinResult.length;
  plan.actualLoops = 1;
  if (tree.method === "hash") {
    plan.memoryRows = rightItems.length;
  }

  // Rows removed: input rows that did not survive the join condition.
  // For hash/merge with equi-pred: left probes that found no match.
  // For NLJ: full Cartesian minus output.
  if (tree.equiPred || plan.filterExpr) {
    const jt = tree.joinType ?? "inner";
    let inputRows: number;
    if (jt === "semi" || jt === "anti") {
      inputRows = leftRows.length;
    } else if (tree.method === "loop" && !tree.equiPred) {
      inputRows = leftRows.length * rightItems.length;
    } else {
      // Hash/merge: input is left side probes
      inputRows = leftRows.length;
    }
    const removed = inputRows - joinResult.length;
    if (removed > 0) {
      plan.rowsRemovedByFilter = removed;
    }
  }

  if (opts.timing) {
    const startupElapsed = Math.round((joinT0 - t0) * 1000) / 1000;
    const totalElapsed = Math.round((performance.now() - t0) * 1000) / 1000;
    plan.actualStartupTimeMs = startupElapsed;
    plan.actualTimeMs = totalElapsed;
  }

  return joinResult;
}

// --- EXPLAIN output formatting ---

export function formatExplainOutput(
  result: ExplainResult,
  opts: ExplainOptions,
): string {
  const lines: string[] = [];
  formatNode(result.plan, opts, 0, lines);
  lines.push(`Planning Time: ${result.planningTimeMs.toFixed(3)} ms`);
  if (opts.analyze && result.executionTimeMs !== undefined) {
    lines.push(`Execution Time: ${result.executionTimeMs.toFixed(3)} ms`);
  }
  return `\`\`\`\n${lines.join("\n")}\n\`\`\``
}

function formatNode(
  node: ExplainNode,
  opts: ExplainOptions,
  indent: number,
  lines: string[],
): void {
  const isRoot = indent === 0;
  const pad = " ".repeat(indent);
  const prefix = isRoot ? "" : "->  ";
  const label = formatNodeLabel(node);

  // Cost block: (cost=startup..total rows=N width=N)
  let estBlock = "";
  if (opts.costs) {
    const s = (node.startupCost ?? 0).toFixed(2);
    const t = (node.estimatedCost ?? 0).toFixed(2);
    estBlock = `  (cost=${s}..${t} rows=${node.estimatedRows} width=${node.estimatedWidth})`;
  }

  // Actual block: (actual time=startup..total rows=N loops=N)
  let actBlock = "";
  if (opts.analyze && node.actualRows !== undefined) {
    let timeStr = "";
    if (opts.timing && node.actualTimeMs !== undefined) {
      const startup = (node.actualStartupTimeMs ?? node.actualTimeMs).toFixed(
        3,
      );
      const total = node.actualTimeMs.toFixed(3);
      timeStr = ` time=${startup}..${total}`;
    }
    const loops = node.actualLoops ?? 1;
    actBlock = ` (actual${timeStr} rows=${node.actualRows} loops=${loops})`;
  }

  lines.push(`${pad}${prefix}${label}${estBlock}${actBlock}`);

  // Detail lines indented past the arrow
  const detailIndent = indent + prefix.length + 6;
  const detailPad = " ".repeat(detailIndent);

  // 1. Join condition (Hash Cond / Merge Cond / Join Filter)
  if (node.equiPred) {
    const condLabel =
      node.method === "hash"
        ? "Hash Cond"
        : node.method === "merge"
          ? "Merge Cond"
          : "Join Filter";
    const ep = node.equiPred;
    lines.push(
      `${detailPad}${condLabel}: (${ep.leftSource}.${ep.leftColumn} == ${ep.rightSource}.${ep.rightColumn})`,
    );
  }

  // 2. Sort / Group keys (before filter for GroupAggregate — groups are
  //    formed first, then HAVING filters; Sort key describes the operation)
  if (node.sortKeys && node.sortKeys.length > 0) {
    const keyLabel =
      node.nodeType === "GroupAggregate" ? "Group Key" : "Sort Key";
    lines.push(`${detailPad}${keyLabel}: ${node.sortKeys.join(", ")}`);
  }

  // 3. Filter (residual WHERE on joins, HAVING on GroupAggregate)
  if (node.filterExpr) {
    lines.push(`${detailPad}Filter: ${node.filterExpr}`);
  }

  // 4. Limit / Offset
  if (node.nodeType === "Limit") {
    if (node.offsetCount !== undefined) {
      lines.push(`${detailPad}Offset: ${node.offsetCount}`);
    }
    if (node.limitCount !== undefined) {
      lines.push(`${detailPad}Count: ${node.limitCount}`);
    }
  }

  // 5. Join hint (advisory annotation)
  if (node.hintUsed && node.nodeType !== "Scan") {
    lines.push(`${detailPad}Join Hint: ${node.hintUsed}`);
  }

  // 6. Rows removed by filter/condition (analyze only)
  if (opts.analyze && node.rowsRemovedByFilter !== undefined && node.rowsRemovedByFilter > 0) {
    lines.push(
      `${detailPad}Rows Removed by Filter: ${node.rowsRemovedByFilter}`,
    );
  }

  // 7. Memory usage — hash table or sort buffer (analyze only)
  if (node.memoryRows !== undefined && opts.analyze) {
    lines.push(`${detailPad}Memory: ${node.memoryRows} rows`);
  }

  // Recurse into children
  const childIndent = indent + prefix.length + 2;
  for (const child of node.children) {
    formatNode(child, opts, childIndent, lines);
  }
}

function formatNodeLabel(node: ExplainNode): string {
  const jtSuffix = (jt: JoinType | undefined) =>
    jt && jt !== "inner" ? ` ${jt.charAt(0).toUpperCase()}${jt.slice(1)}` : "";

  switch (node.nodeType) {
    case "Scan":
      return `Seq Scan on ${node.source}`;
    case "HashJoin":
      return `Hash${jtSuffix(node.joinType)} Join`;
    case "NestedLoop":
      return `Nested Loop${jtSuffix(node.joinType)}`;
    case "MergeJoin":
      return `Merge${jtSuffix(node.joinType)} Join`;
    case "Sort":
      return "Sort";
    case "Limit":
      return "Limit";
    case "GroupAggregate":
      return "GroupAggregate";
    case "Unique":
      return "Unique";
    default:
      return node.nodeType;
  }
}
