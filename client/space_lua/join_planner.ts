/**
 * Cost-Based Join Planner for LIQ (Lua Integrated Query).
 *
 * Transforms a multi-source `from` clause into an optimized join tree,
 * then executes it using hash join, nested loop, or sort-merge operators.
 */
import type { LuaExpression, LuaFunctionBody, LuaJoinHint } from "./ast.ts";
import { evalExpression } from "./eval.ts";
import type { CollectionStats } from "./query_collection.ts";
import {
  type LuaEnv,
  LuaFunction,
  LuaRuntimeError,
  type LuaStackFrame,
  LuaTable,
  type LuaValue,
  luaCall,
  luaKeys,
  luaTruthy,
  singleResult,
} from "./runtime.ts";

// 1. Types and constants

// Config

const DEFAULT_WATCHDOG_LIMIT = 5e5;
const DEFAULT_YIELD_CHUNK = 5000;
const DEFAULT_SELECTIVITY = 0.01;
const DEFAULT_SMALL_TABLE_THRESHOLD = 20;
const DEFAULT_RANGE_SELECTIVITY = 0.33;
const DEFAULT_MERGE_JOIN_THRESHOLD = 200;
const DEFAULT_WIDTH_WEIGHT = 1;
const DEFAULT_CANDIDATE_WIDTH_WEIGHT = 2;

export type MaterializedSourceOverrides = Map<string, any[]>;

export type JoinPlannerConfig = {
  watchdogLimit?: number;
  yieldChunk?: number;
  smallTableThreshold?: number;
  mergeJoinThreshold?: number;
  widthWeight?: number;
  candidateWidthWeight?: number;
};

function getWatchdogLimit(config?: JoinPlannerConfig): number {
  return config?.watchdogLimit ?? DEFAULT_WATCHDOG_LIMIT;
}

function getYieldChunk(config?: JoinPlannerConfig): number {
  return config?.yieldChunk ?? DEFAULT_YIELD_CHUNK;
}

function finiteNumberOrDefault(
  value: number | undefined,
  fallback: number,
): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function getSmallTableThreshold(config?: JoinPlannerConfig): number {
  return finiteNumberOrDefault(
    config?.smallTableThreshold,
    DEFAULT_SMALL_TABLE_THRESHOLD,
  );
}

function getMergeJoinThreshold(config?: JoinPlannerConfig): number {
  return finiteNumberOrDefault(
    config?.mergeJoinThreshold,
    DEFAULT_MERGE_JOIN_THRESHOLD,
  );
}

function getWidthWeight(config?: JoinPlannerConfig): number {
  return finiteNumberOrDefault(config?.widthWeight, DEFAULT_WIDTH_WEIGHT);
}

function getCandidateWidthWeight(config?: JoinPlannerConfig): number {
  return finiteNumberOrDefault(
    config?.candidateWidthWeight,
    DEFAULT_CANDIDATE_WIDTH_WEIGHT,
  );
}

// Join

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
  estimatedSelectivity?: number;
  estimatedRows?: number;
};

// Predicate

export type EquiPredicate = {
  leftSource: string;
  leftColumn: string;
  rightSource: string;
  rightColumn: string;
};

export type RangePredicate = {
  leftSource: string;
  leftColumn: string;
  operator: ">" | "<" | ">=" | "<=";
  rightSource: string;
  rightColumn: string;
};

// Stats

export type OpStats = {
  actualRows: number;
  loops: number;
  rebinds: number;
  startTimeMs: number;
  endTimeMs: number;
  peakMemoryRows: number;
};

// Explain

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

// 2. Cardinality and selectivity estimation

function estimatedRows(s: JoinSource): number {
  return s.stats?.rowCount ?? 100;
}

function estimatedWidth(s: JoinSource): number {
  return s.stats?.avgColumnCount ?? 5;
}

function clampWidth(width: number): number {
  return Math.max(1, width);
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

/**
 * Compute join selectivity using the actual equi-predicate columns and null
 * fraction. If stats are missing, fall back to a conservative heuristic.
 */
function joinSelectivity(
  left: JoinSource,
  right: JoinSource,
  equiPred?: EquiPredicate,
): number {
  if (equiPred) {
    const leftNdv = left.stats?.ndv?.get(equiPred.leftColumn);
    const rightNdv = right.stats?.ndv?.get(equiPred.rightColumn);
    if (leftNdv !== undefined && rightNdv !== undefined) {
      return 1 / Math.max(leftNdv, rightNdv, 1);
    }
  }

  const ra = left.stats?.rowCount ?? 100;
  const rb = right.stats?.rowCount ?? 100;
  return 1 / Math.max(ra, rb, 1);
}

/**
 * Estimate selectivity for range predicates
 */
function estimateRangeSelectivity(
  rangePredicates: RangePredicate[],
  leftNames: Set<string>,
  rightName: string,
): number {
  // Each range predicate reduces cardinality by DEFAULT_RANGE_SELECTIVITY
  let sel = 1.0;
  for (const rp of rangePredicates) {
    if (
      (leftNames.has(rp.leftSource) && rp.rightSource === rightName) ||
      (leftNames.has(rp.rightSource) && rp.leftSource === rightName)
    ) {
      sel *= DEFAULT_RANGE_SELECTIVITY;
    }
  }
  return sel;
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

function sourceByName(
  sources: JoinSource[],
): Map<string, JoinSource> {
  return new Map(sources.map((s) => [s.name, s]));
}

function findEquiPredBetweenSets(
  leftNames: Set<string>,
  rightName: string,
  equiPreds?: EquiPredicate[],
): EquiPredicate | undefined {
  if (!equiPreds) return undefined;
  const pred = equiPreds.find(
    (ep) =>
      (leftNames.has(ep.leftSource) && ep.rightSource === rightName) ||
      (leftNames.has(ep.rightSource) && ep.leftSource === rightName),
  );
  if (!pred) return undefined;
  if (leftNames.has(pred.leftSource)) return pred;
  return {
    leftSource: pred.rightSource,
    leftColumn: pred.rightColumn,
    rightSource: pred.leftSource,
    rightColumn: pred.leftColumn,
  };
}

function estimateJoinWithCandidate(
  joinedNames: Set<string>,
  joinedRows: number,
  candidate: JoinSource,
  allSources: Map<string, JoinSource>,
  equiPreds?: EquiPredicate[],
  rangePreds?: RangePredicate[],
  joinType: JoinType = "inner",
): { selectivity: number; equiPred?: EquiPredicate; outputRows: number } {
  const equiPred = findEquiPredBetweenSets(
    joinedNames,
    candidate.name,
    equiPreds,
  );

  let equiSel = 1.0;
  if (equiPred) {
    const leftSource = allSources.get(equiPred.leftSource);
    const rightSource = allSources.get(equiPred.rightSource);
    if (leftSource && rightSource) {
      equiSel = joinSelectivity(leftSource, rightSource, equiPred);
    }
  } else {
    equiSel = 1 / Math.max(joinedRows, estimatedRows(candidate), 1);
  }

  const rangeSel = rangePreds
    ? estimateRangeSelectivity(rangePreds, joinedNames, candidate.name)
    : 1.0;

  const combinedSel = equiSel * rangeSel;
  const outputRows = estimateJoinCardinality(
    joinedRows,
    estimatedRows(candidate),
    joinType,
    combinedSel,
  );
  return { selectivity: combinedSel, equiPred, outputRows };
}

// 3. Join tree construction

export function buildJoinTree(
  sources: JoinSource[],
  planOrder?: string[],
  equiPreds?: EquiPredicate[],
  rangePreds?: RangePredicate[],
  config?: JoinPlannerConfig,
): JoinNode {
  if (sources.length === 1) {
    return { kind: "leaf", source: sources[0] };
  }

  const ordered = orderSources(
    sources,
    planOrder,
    equiPreds,
    rangePreds,
    config,
  );

  // Strip hint from driving table
  if (ordered[0].hint) {
    ordered[0] = { ...ordered[0], hint: undefined };
  }

  const allSources = sourceByName(ordered);

  let tree: JoinNode = { kind: "leaf", source: ordered[0] };
  let accRows = estimatedRows(ordered[0]);
  let accWidth = estimatedWidth(ordered[0]);

  for (let i = 1; i < ordered.length; i++) {
    const right = ordered[i];
    const jt = right.hint?.joinType ?? "inner";
    const leftNames = collectSourceNames(tree);
    const { selectivity, equiPred } = estimateJoinWithCandidate(
      leftNames,
      accRows,
      right,
      allSources,
      equiPreds,
      rangePreds,
      jt,
    );

    const method = selectPhysicalOperator(
      accRows,
      right,
      jt,
      !!equiPred,
      accWidth,
      config,
    );

    const joinRows = estimateJoinCardinality(
      accRows,
      estimatedRows(right),
      jt,
      selectivity,
    );

    tree = {
      kind: "join",
      left: tree,
      right: { kind: "leaf", source: right },
      method,
      joinType: jt,
      equiPred,
      estimatedSelectivity: selectivity,
      estimatedRows: joinRows,
    };
    accRows = joinRows;
    accWidth += estimatedWidth(right);
  }
  return tree;
}

function orderSources(
  sources: JoinSource[],
  planOrder?: string[],
  equiPreds?: EquiPredicate[],
  rangePreds?: RangePredicate[],
  config?: JoinPlannerConfig,
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

  // Any explicit join hint is attached to a specific source position in the
  // query and must remain there. Reordering hinted sources can change both
  // semantics and predicate orientation.
  const hasExplicitJoinHint = sources.some((s) => !!s.hint);
  if (hasExplicitJoinHint) {
    return [...sources];
  }

  // Greedy: start with smallest, then pick the cheapest source against the
  // current joined set (not just the previously-added source).
  const remaining = [...sources];
  remaining.sort((a, b) => estimatedRows(a) - estimatedRows(b));

  const ordered = [remaining.shift()!];
  const allSources = sourceByName(sources);
  let joinedNames = new Set<string>([ordered[0].name]);
  let joinedRows = estimatedRows(ordered[0]);
  let joinedWidth = estimatedWidth(ordered[0]);

  while (remaining.length > 0) {
    let bestIdx = 0;
    let bestCost = Infinity;
    let bestOutRows = Infinity;
    let bestCandidateWidth = Infinity;

    for (let i = 0; i < remaining.length; i++) {
      const candidate = remaining[i];
      const joinType = candidate.hint?.joinType ?? "inner";
      const { outputRows } = estimateJoinWithCandidate(
        joinedNames,
        joinedRows,
        candidate,
        allSources,
        equiPreds,
        rangePreds,
        joinType,
      );
      const candidateWidth = clampWidth(estimatedWidth(candidate));
      const cost =
        joinedRows *
        estimatedRows(candidate) *
        (
          getWidthWeight(config) * clampWidth(joinedWidth) +
          getCandidateWidthWeight(config) * candidateWidth
        );

      if (
        cost < bestCost ||
        (cost === bestCost && outputRows < bestOutRows) ||
        (
          cost === bestCost &&
          outputRows === bestOutRows &&
          candidateWidth < bestCandidateWidth
        )
      ) {
        bestCost = cost;
        bestOutRows = outputRows;
        bestCandidateWidth = candidateWidth;
        bestIdx = i;
      }
    }

    const chosen = remaining.splice(bestIdx, 1)[0];
    ordered.push(chosen);
    joinedNames = new Set([...joinedNames, chosen.name]);
    joinedRows = bestOutRows;
    joinedWidth += estimatedWidth(chosen);
  }

  return ordered;
}

// 4. Physical operator selection

function selectPhysicalOperator(
  leftRowCount: number,
  right: JoinSource,
  joinType: JoinType = "inner",
  hasEquiPred: boolean = false,
  leftWidth: number = 5,
  config?: JoinPlannerConfig,
): "hash" | "loop" | "merge" {
  if (right.hint) {
    const k = right.hint.kind;
    if (k === "merge") {
      if (!hasEquiPred) {
        throw new Error("Merge join requires equi-predicate");
      }
      return "merge";
    }
    if (k === "hash") {
      if (right.hint.using) {
        throw new Error("'using' only valid with 'loop' hint");
      }
      return "hash";
    }
    if (k === "loop") return "loop";
  }

  const rr = right.stats?.rowCount ?? 100;
  const rw = clampWidth(estimatedWidth(right));
  const lw = clampWidth(leftWidth);
  const widthWeight = getWidthWeight(config);
  const candidateWidthWeight = getCandidateWidthWeight(config);

  if (rr <= getSmallTableThreshold(config)) return "loop";

  // Width-aware heuristics:
  // - hash builds/copies the right side, so weight build width
  // - nested loop copies combined rows, so weight both sides
  // - merge sorts both sides, so weight both widths
  const hashCost = rr * candidateWidthWeight * rw + leftRowCount;
  const discount = joinType === "inner" ? 1.0 : 0.5;
  const nljCost =
    leftRowCount *
    rr *
    discount *
    (widthWeight * lw + candidateWidthWeight * rw);

  if (
    hasEquiPred &&
    leftRowCount > getMergeJoinThreshold(config) &&
    rr > getMergeJoinThreshold(config)
  ) {
    const leftSort =
      leftRowCount *
      Math.ceil(Math.log2(Math.max(2, leftRowCount))) *
      widthWeight *
      lw;
    const rightSort =
      rr *
      Math.ceil(Math.log2(Math.max(2, rr))) *
      candidateWidthWeight *
      rw;
    const mergeCost =
      leftSort +
      rightSort +
      leftRowCount * widthWeight * lw +
      rr * candidateWidthWeight * rw;
    if (mergeCost < hashCost && mergeCost < nljCost) {
      return "merge";
    }
  }

  return hashCost < nljCost ? "hash" : "loop";
}

// 5. Row helpers and materialization

async function cooperativeYield(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

async function materializeSource(
  source: JoinSource,
  env: LuaEnv,
  sf: LuaStackFrame,
  overrides?: MaterializedSourceOverrides,
): Promise<any[]> {
  const overridden = overrides?.get(source.name);
  if (overridden) {
    return overridden;
  }
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

function hashJoinKey(item: any): string | null {
  if (item === null || item === undefined) return null;
  if (typeof item === "string") return `s:${item}`;
  if (typeof item === "number") {
    return Object.is(item, -0) ? "n:-0" : `n:${item}`;
  }
  if (typeof item === "boolean") {
    return item ? "b:1" : "b:0";
  }
  return null;
}

function normalizeEquiPredicateForJoin(
  equiPred: EquiPredicate,
  leftNode: JoinNode,
  rightSource: JoinSource,
): EquiPredicate {
  const leftNames = collectSourceNames(leftNode);
  const rightName = rightSource.name;

  if (
    leftNames.has(equiPred.leftSource) &&
    equiPred.rightSource === rightName
  ) {
    return equiPred;
  }

  if (
    leftNames.has(equiPred.rightSource) &&
    equiPred.leftSource === rightName
  ) {
    return {
      leftSource: equiPred.rightSource,
      leftColumn: equiPred.rightColumn,
      rightSource: equiPred.leftSource,
      rightColumn: equiPred.leftColumn,
    };
  }

  throw new Error(
    `Equi-predicate does not match join sides: left={${[...leftNames].join(",")}} right=${rightName} pred=${equiPred.leftSource}.${equiPred.leftColumn}==${equiPred.rightSource}.${equiPred.rightColumn}`,
  );
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

function isLeafNode(node: JoinNode): node is JoinLeaf {
  return node.kind === "leaf";
}

function loopPredicateLeftArg(
  leftNode: JoinNode,
  row: LuaTable,
): LuaValue {
  if (isLeafNode(leftNode)) {
    return row.rawGet(leftNode.source.name);
  }

  // For two-source joins the logical left side may still be represented as a
  // single-binding row table. In that case, preserve existing Lua semantics
  // for `loop using function(left, right)` by passing the bound item directly.
  const keys = [...luaKeys(row)];
  if (keys.length === 1) {
    return row.rawGet(keys[0]);
  }

  return row;
}

// 6. Join operators

// Semi/Anti

async function hashSemiAntiJoin(
  leftRows: LuaTable[],
  rightItems: any[],
  joinType: "semi" | "anti",
  equiPred: EquiPredicate,
): Promise<LuaTable[]> {
  // BUILD: existence set on right join key
  const buildSet = new Set<string>();
  for (const rItem of rightItems) {
    const val = extractField(rItem, equiPred.rightColumn);
    const key = hashJoinKey(val);
    if (key !== null) {
      buildSet.add(key);
    }
  }

  const results: LuaTable[] = [];
  for (const lRow of leftRows) {
    const leftObj = lRow.rawGet(equiPred.leftSource);
    const val = extractField(leftObj, equiPred.leftColumn);
    const key = hashJoinKey(val);
    const hasMatch = key !== null && buildSet.has(key);

    if (joinType === "semi" && hasMatch) results.push(lRow);
    else if (joinType === "anti" && !hasMatch) results.push(lRow);
  }
  return results;
}

async function nestedLoopSemiAntiJoin(
  leftRows: LuaTable[],
  rightItems: any[],
  leftNode: JoinNode,
  predicate: LuaValue,
  joinType: "semi" | "anti",
  sf: LuaStackFrame,
): Promise<LuaTable[]> {
  const results: LuaTable[] = [];
  for (const leftRow of leftRows) {
    const leftArg = loopPredicateLeftArg(leftNode, leftRow);
    let found = false;
    for (const rightItem of rightItems) {
      const res = singleResult(
        await luaCall(predicate, [leftArg, rightItem], sf.astCtx ?? {}, sf),
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

// Inner

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
  const buildMap = new Map<string, any[]>();
  for (const rItem of rightItems) {
    const val = extractField(rItem, equiPred.rightColumn);
    const key = hashJoinKey(val);
    if (key === null) continue;
    let bucket = buildMap.get(key);
    if (!bucket) {
      bucket = [];
      buildMap.set(key, bucket);
    }
    bucket.push(rItem);
  }

  const results: LuaTable[] = [];
  let processed = 0;
  for (const lRow of leftRows) {
    const leftObj = lRow.rawGet(equiPred.leftSource);
    const val = extractField(leftObj, equiPred.leftColumn);
    const key = hashJoinKey(val);
    if (key === null) continue;
    const bucket = buildMap.get(key);
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
  leftNode: JoinNode,
  predicate: LuaValue,
  sf: LuaStackFrame,
  config?: JoinPlannerConfig,
): Promise<LuaTable[]> {
  const limit = getWatchdogLimit(config);
  const chunk = getYieldChunk(config);
  const results: LuaTable[] = [];
  let processed = 0;
  for (const leftRow of leftRows) {
    const leftArg = loopPredicateLeftArg(leftNode, leftRow);
    for (const rightItem of rightItems) {
      const res = singleResult(
        await luaCall(predicate, [leftArg, rightItem], sf.astCtx ?? {}, sf),
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

/**
 * Sort-merge join. When an equi-predicate is available, uses the
 * equi-pred columns for key extraction. Otherwise falls back to
 * the first field of each row.
 */
async function sortMergeJoin(
  leftRows: LuaTable[],
  rightItems: any[],
  rightName: string,
  sf: LuaStackFrame,
  config?: JoinPlannerConfig,
  equiPred?: EquiPredicate,
): Promise<LuaTable[]> {
  const limit = getWatchdogLimit(config);
  const chunk = getYieldChunk(config);

  // Key extractors
  const leftKeyFn = equiPred
    ? (row: LuaTable) => {
        const obj = row.rawGet(equiPred.leftSource);
        return sortKey(extractField(obj, equiPred.leftColumn));
      }
    : (row: LuaTable) => {
        let key: string | number = "";
        for (const k of luaKeys(row)) {
          key = sortKey(row.rawGet(k));
          break;
        }
        return key;
      };

  const rightKeyFn = equiPred
    ? (item: any) => sortKey(extractField(item, equiPred.rightColumn))
    : (item: any) => sortKey(item);

  const leftKeyed = leftRows.map((row) => ({ row, key: leftKeyFn(row) }));
  const rightKeyed = rightItems.map((item) => ({
    item,
    key: rightKeyFn(item),
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

// Dispatch (routes to the correct operator)

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
  const predicate = resolveUsingPredicate(rightSource.hint, env, sf);
  const equiPred = tree.equiPred
    ? normalizeEquiPredicateForJoin(tree.equiPred, tree.left, rightSource)
    : undefined;

  // Semi/Anti
  if (joinType === "semi" || joinType === "anti") {
    if (tree.method === "loop") {
      if (!predicate) {
        throw new LuaRuntimeError(
          `${joinType} loop join requires using predicate`,
          sf,
        );
      }
      return nestedLoopSemiAntiJoin(
        leftRows,
        rightItems,
        tree.left,
        predicate,
        joinType,
        sf,
      );
    }

    if (equiPred) {
      return hashSemiAntiJoin(
        leftRows,
        rightItems,
        joinType,
        equiPred,
      );
    }

    throw new LuaRuntimeError(
      `${joinType} join requires equi-predicate or loop using predicate`,
      sf,
    );
  }

  // Inner with using predicate
  if (tree.method === "loop" && predicate) {
    return predicateLoopJoin(
      leftRows,
      rightItems,
      rightName,
      tree.left,
      predicate,
      sf,
      config,
    );
  }

  // Inner by method
  switch (tree.method) {
    case "hash":
      if (equiPred) {
        return hashInnerJoin(
          leftRows,
          rightItems,
          rightName,
          equiPred,
          sf,
          config,
        );
      }
      return crossJoin(leftRows, rightItems, rightName, sf, config);
    case "loop":
      return crossJoin(leftRows, rightItems, rightName, sf, config);
    case "merge":
      return sortMergeJoin(
        leftRows,
        rightItems,
        rightName,
        sf,
        config,
        equiPred,
      );
  }
}

// 7. Join tree execution

export async function executeJoinTree(
  tree: JoinNode,
  env: LuaEnv,
  sf: LuaStackFrame,
  config?: JoinPlannerConfig,
  overrides?: MaterializedSourceOverrides,
): Promise<LuaTable[]> {
  if (tree.kind === "leaf") {
    const items = await materializeSource(tree.source, env, sf, overrides);
    return items.map((item) => rowToTable(tree.source.name, item));
  }
  const leftRows = await executeJoinTree(tree.left, env, sf, config, overrides);
  const rightSource = (tree.right as JoinLeaf).source;
  const rightItems = await materializeSource(rightSource, env, sf, overrides);
  return dispatchJoin(tree, leftRows, rightItems, rightSource, env, sf, config);
}

// 8. Equi-predicate and range predicate extraction

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

/**
 * Extract range predicates (`>`, `<`, `>=`, `<=`)
 */
export function extractRangePredicates(
  expr: LuaExpression | undefined,
  sourceNames: Set<string>,
): RangePredicate[] {
  if (!expr) return [];
  const result: RangePredicate[] = [];
  collectRangeJoins(expr, sourceNames, result);
  return result;
}

function collectRangeJoins(
  expr: LuaExpression,
  sourceNames: Set<string>,
  out: RangePredicate[],
): void {
  if (expr.type !== "Binary") return;
  if (expr.operator === "and") {
    collectRangeJoins(expr.left, sourceNames, out);
    collectRangeJoins(expr.right, sourceNames, out);
    return;
  }
  const op = expr.operator;
  if (op === ">" || op === "<" || op === ">=" || op === "<=") {
    const left = parseSourceColumn(expr.left, sourceNames);
    const right = parseSourceColumn(expr.right, sourceNames);
    if (left && right && left.source !== right.source) {
      out.push({
        leftSource: left.source,
        leftColumn: left.column,
        operator: op as RangePredicate["operator"],
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

// 9. Single-source filter pushdown

/**
 * A filter that references exactly one source from the `from` clause.
 * Can be pushed down to filter that source before the join.
 */
export type SingleSourceFilter = {
  sourceName: string;
  expression: LuaExpression;
};

/**
 * Extract single-source filter conjuncts from a `where` expression.
 *
 * We only push down filters that are syntactically safe:
 * - they reference exactly one source, and
 * - every source-dependent access is explicitly rooted at that source name.
 *
 * This conservative rule keeps pushdown semantics provable.
 */
export function extractSingleSourceFilters(
  expr: LuaExpression | undefined,
  sourceNames: Set<string>,
): { pushed: SingleSourceFilter[]; residual: LuaExpression | undefined } {
  if (!expr) return { pushed: [], residual: undefined };

  const conjuncts = flattenAnd(expr);
  const pushed: SingleSourceFilter[] = [];
  const remaining: LuaExpression[] = [];

  for (const conjunct of conjuncts) {
    const refs = collectReferencedSources(conjunct, sourceNames);
    const explicit = isExplicitlySingleSourceExpr(conjunct, sourceNames);

    if (refs.size === 1 && explicit) {
      const [sourceName] = refs;
      pushed.push({ sourceName, expression: conjunct });
    } else {
      remaining.push(conjunct);
    }
  }

  const residual =
    remaining.length > 0
      ? remaining.reduce((acc, e) => ({
          type: "Binary" as const,
          operator: "and",
          left: acc,
          right: e,
          ctx: expr.ctx,
        }))
      : undefined;

  return { pushed, residual };
}

/**
 * Flatten an `and` chain into individual conjuncts
 */
function flattenAnd(expr: LuaExpression): LuaExpression[] {
  if (expr.type === "Binary" && expr.operator === "and") {
    return [...flattenAnd(expr.left), ...flattenAnd(expr.right)];
  }
  return [expr];
}

/**
 * Collect all source names referenced in an expression
 */
function collectReferencedSources(
  expr: LuaExpression,
  sourceNames: Set<string>,
): Set<string> {
  const refs = new Set<string>();
  walkExprForSources(expr, sourceNames, refs);
  return refs;
}

function walkExprForSources(
  expr: LuaExpression,
  sourceNames: Set<string>,
  refs: Set<string>,
): void {
  switch (expr.type) {
    case "PropertyAccess":
      if (
        expr.object.type === "Variable" &&
        sourceNames.has(expr.object.name)
      ) {
        refs.add(expr.object.name);
      } else {
        walkExprForSources(expr.object, sourceNames, refs);
      }
      break;
    case "Variable":
      if (sourceNames.has(expr.name)) {
        refs.add(expr.name);
      }
      break;
    case "Binary":
      walkExprForSources(expr.left, sourceNames, refs);
      walkExprForSources(expr.right, sourceNames, refs);
      break;
    case "Unary":
      walkExprForSources(expr.argument, sourceNames, refs);
      break;
    case "FunctionCall":
      walkExprForSources(expr.prefix, sourceNames, refs);
      for (const arg of expr.args) {
        walkExprForSources(arg, sourceNames, refs);
      }
      break;
    case "Parenthesized":
      walkExprForSources(expr.expression, sourceNames, refs);
      break;
    case "TableAccess":
      walkExprForSources(expr.object, sourceNames, refs);
      walkExprForSources(expr.key, sourceNames, refs);
      break;
    // Literals and nil reference no sources
    default:
      break;
  }
}

function isExplicitlySingleSourceExpr(
  expr: LuaExpression,
  sourceNames: Set<string>,
): boolean {
  switch (expr.type) {
    case "Nil":
    case "Boolean":
    case "Number":
    case "String":
      return true;
    case "Variable":
      // Bare source variable itself is allowed, but bare field names are not.
      return sourceNames.has(expr.name);
    case "PropertyAccess":
      if (
        expr.object.type === "Variable" &&
        sourceNames.has(expr.object.name)
      ) {
        return true;
      }
      return isExplicitlySingleSourceExpr(expr.object, sourceNames);
    case "TableAccess":
      return (
        isExplicitlySingleSourceExpr(expr.object, sourceNames) &&
        isExplicitlySingleSourceExpr(expr.key, sourceNames)
      );
    case "Binary":
      return (
        isExplicitlySingleSourceExpr(expr.left, sourceNames) &&
        isExplicitlySingleSourceExpr(expr.right, sourceNames)
      );
    case "Unary":
      return isExplicitlySingleSourceExpr(expr.argument, sourceNames);
    case "Parenthesized":
      return isExplicitlySingleSourceExpr(expr.expression, sourceNames);
    case "FunctionCall":
      return (
        isExplicitlySingleSourceExpr(expr.prefix, sourceNames) &&
        expr.args.every((arg) => isExplicitlySingleSourceExpr(arg, sourceNames))
      );
    default:
      return false;
  }
}

/**
 * Apply pushed-down filters to materialized source items
 */
export async function applyPushedFilters(
  items: any[],
  sourceName: string,
  filters: SingleSourceFilter[],
  env: LuaEnv,
  sf: LuaStackFrame,
): Promise<any[]> {
  if (filters.length === 0) return items;

  // Combine all filters for this source into a single `and` expression
  const relevant = filters.filter((f) => f.sourceName === sourceName);
  if (relevant.length === 0) return items;

  let result = items;
  for (const filter of relevant) {
    const filtered: any[] = [];
    for (const item of result) {
      const filterEnv = new (await import("./runtime.ts")).LuaEnv(env);
      filterEnv.setLocal(sourceName, item);
      const val = await evalExpression(filter.expression, filterEnv, sf);
      if (luaTruthy(val)) {
        filtered.push(item);
      }
    }
    result = filtered;
  }
  return result;
}

// 10. Equi-predicate stripping for residual where

export function stripUsedJoinPredicates(
  expr: LuaExpression | undefined,
  tree: JoinNode,
): LuaExpression | undefined {
  if (!expr) return undefined;
  const usedPreds = collectUsedEquiPredsFromJoinTree(tree);
  return stripEquiPreds(expr, usedPreds);
}

function collectUsedEquiPredsFromJoinTree(node: JoinNode): EquiPredicate[] {
  if (node.kind === "leaf") return [];
  const result: EquiPredicate[] = [];
  if (node.equiPred) result.push(node.equiPred);
  result.push(...collectUsedEquiPredsFromJoinTree(node.left));
  result.push(...collectUsedEquiPredsFromJoinTree(node.right));
  return result;
}

// 11. Explain infrastructure

// Plan construction

export function explainJoinTree(
  tree: JoinNode,
  _opts: ExplainOptions,
): ExplainNode {
  if (tree.kind === "leaf") {
    const rows = estimatedRows(tree.source);
    const width = estimatedWidth(tree.source);
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

  const estRows =
    tree.estimatedRows ??
    estimateJoinCardinality(
      leftPlan.estimatedRows,
      rightPlan.estimatedRows,
      jt,
      DEFAULT_SELECTIVITY,
    );

  let startupCost: number;
  let totalCost: number;
  if (tree.method === "hash") {
    startupCost = rightPlan.estimatedCost + rightPlan.estimatedRows;
    totalCost = startupCost + leftPlan.estimatedCost + leftPlan.estimatedRows;
  } else if (tree.method === "merge") {
    // Sort both sides + linear merge
    const leftSort =
      leftPlan.estimatedRows *
      Math.ceil(Math.log2(Math.max(2, leftPlan.estimatedRows)));
    const rightSort =
      rightPlan.estimatedRows *
      Math.ceil(Math.log2(Math.max(2, rightPlan.estimatedRows)));
    startupCost =
      leftPlan.estimatedCost + rightPlan.estimatedCost + leftSort + rightSort;
    totalCost = startupCost + leftPlan.estimatedRows + rightPlan.estimatedRows;
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
 * Wrap a join plan with `Sort`/`Limit`/`GroupAggregate` nodes based on
 * the query clauses
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

  if (query.where) {
    const usedPreds = collectUsedEquiPreds(root);
    const residual = stripEquiPreds(query.where, usedPreds);
    if (residual) {
      root.filterExpr = exprToString(residual);
    }
  }

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

  if (query.having) {
    root.filterExpr = exprToString(query.having);
  }

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

// Helpers

function formatHintLabel(hint: LuaJoinHint): string {
  const parts: string[] = [];
  if (hint.joinType) parts.push(hint.joinType);
  parts.push(hint.kind);
  if (hint.using) parts.push("using");
  return parts.join(" ");
}

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

function collectUsedEquiPreds(node: ExplainNode): EquiPredicate[] {
  const result: EquiPredicate[] = [];
  if (node.equiPred) result.push(node.equiPred);
  for (const child of node.children) {
    result.push(...collectUsedEquiPreds(child));
  }
  return result;
}

function exprMatchesEquiPred(
  expr: LuaExpression,
  preds: EquiPredicate[],
): boolean {
  if (expr.type !== "Binary" || expr.operator !== "==") return false;
  const left = parseSourceColumnFromExpr(expr.left);
  const right = parseSourceColumnFromExpr(expr.right);
  if (!left || !right) return false;
  return preds.some(
    (ep) =>
      (ep.leftSource === left.source &&
        ep.leftColumn === left.column &&
        ep.rightSource === right.source &&
        ep.rightColumn === right.column) ||
      (ep.leftSource === right.source &&
        ep.leftColumn === right.column &&
        ep.rightSource === left.source &&
        ep.rightColumn === left.column),
  );
}

function parseSourceColumnFromExpr(
  expr: LuaExpression,
): { source: string; column: string } | null {
  if (expr.type !== "PropertyAccess") return null;
  if (expr.object.type !== "Variable") return null;
  return { source: expr.object.name, column: expr.property };
}

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

// Explain analyze execution

export async function executeAndInstrument(
  tree: JoinNode,
  plan: ExplainNode,
  env: LuaEnv,
  sf: LuaStackFrame,
  opts: ExplainOptions,
  config?: JoinPlannerConfig,
  overrides?: MaterializedSourceOverrides,
): Promise<LuaTable[]> {
  const t0 = opts.timing ? performance.now() : 0;

  if (tree.kind === "leaf") {
    const items = await materializeSource(tree.source, env, sf, overrides);
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
    overrides,
  );

  // Recurse right (materialize)
  const rightSource = (tree.right as JoinLeaf).source;
  const rightT0 = opts.timing ? performance.now() : 0;
  const rightItems = await materializeSource(rightSource, env, sf, overrides);
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
  if (opts.timing) {
    const startupElapsed = Math.round((joinT0 - t0) * 1000) / 1000;
    const totalElapsed = Math.round((performance.now() - t0) * 1000) / 1000;
    plan.actualStartupTimeMs = startupElapsed;
    plan.actualTimeMs = totalElapsed;
  }

  return joinResult;
}

// Explain output formatting

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
  return `\`\`\`\n${lines.join("\n")}\n\`\`\``;
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

  // Cost block
  const showCosts = opts.analyze || opts.costs;
  let estBlock = "";
  if (showCosts) {
    const s = (node.startupCost ?? 0).toFixed(2);
    const t = (node.estimatedCost ?? 0).toFixed(2);
    estBlock = `  (cost=${s}..${t} rows=${node.estimatedRows} width=${node.estimatedWidth})`;
  }

  // Actual block
  let actBlock = "";
  if (opts.analyze && node.actualRows !== undefined) {
    let timeStr = "";
    if (opts.timing && node.actualTimeMs !== undefined) {
      const st = (node.actualStartupTimeMs ?? 0).toFixed(3);
      const tt = node.actualTimeMs.toFixed(3);
      timeStr = ` time=${st}..${tt}`;
    }
    actBlock = ` (actual${timeStr} rows=${node.actualRows} loops=${node.actualLoops ?? 1})`;
  }

  lines.push(`${pad}${prefix}${label}${estBlock}${actBlock}`);

  // Detail lines
  const detailPad = pad + (isRoot ? "  " : "      ");

  // Equi-predicate (Hash Cond / Merge Cond / Join Filter)
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

  // Sort/group keys
  if (node.sortKeys) {
    const keyLabel =
      node.nodeType === "GroupAggregate" ? "Group Key" : "Sort Key";
    lines.push(`${detailPad}${keyLabel}: ${node.sortKeys.join(", ")}`);
  }

  // Join hint
  if (node.hintUsed) {
    lines.push(`${detailPad}Join Hint: ${node.hintUsed}`);
  }

  // Filter expression
  if (node.filterExpr) {
    lines.push(`${detailPad}Filter: ${node.filterExpr}`);
  }

  // Rows removed by filter (only available with analyze)
  if (
    opts.analyze &&
    node.rowsRemovedByFilter !== undefined &&
    node.rowsRemovedByFilter > 0
  ) {
    lines.push(
      `${detailPad}Rows Removed by Filter: ${node.rowsRemovedByFilter}`,
    );
  }

  // Limit/offset
  if (node.limitCount !== undefined) {
    lines.push(`${detailPad}Count: ${node.limitCount}`);
  }
  if (node.offsetCount !== undefined) {
    lines.push(`${detailPad}Offset: ${node.offsetCount}`);
  }

  // memory usage
  if (node.memoryRows !== undefined) {
    lines.push(`${detailPad}Memory: ${node.memoryRows} rows`);
  }

  // Source
  if (node.source) {
    lines.push(`${detailPad}Source: ${node.source}`);
  }

  // Children
  for (const child of node.children) {
    formatNode(child, opts, indent + (isRoot ? 2 : 6), lines);
  }
}

function formatNodeLabel(node: ExplainNode): string {
  switch (node.nodeType) {
    case "Scan":
      return `Scan on ${node.source}`;
    case "HashJoin":
      return node.joinType && node.joinType !== "inner"
        ? `Hash ${node.joinType.charAt(0).toUpperCase() + node.joinType.slice(1)} Join`
        : "Hash Join";
    case "NestedLoop":
      return node.joinType && node.joinType !== "inner"
        ? `Nested Loop ${node.joinType.charAt(0).toUpperCase() + node.joinType.slice(1)} Join`
        : "Nested Loop";
    case "MergeJoin":
      return node.joinType && node.joinType !== "inner"
        ? `Merge ${node.joinType.charAt(0).toUpperCase() + node.joinType.slice(1)} Join`
        : "Merge Join";
    case "Sort":
      return "Sort";
    case "Limit":
      return "Limit";
    case "GroupAggregate":
      return "GroupAggregate";
    case "Unique":
      return "Unique";
  }
}
