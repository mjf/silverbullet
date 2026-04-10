/**
 * Cost-Based Join Planner for LIQ (Lua Integrated Query).
 *
 * Transforms a multi-source `from` clause into an optimized join tree,
 * then executes it using hash join, nested loop, or sort-merge operators.
 */
import type { LuaExpression, LuaFunctionBody, LuaJoinHint } from "./ast.ts";
import { evalExpression } from "./eval.ts";
import type { CollectionStats, StatsSource } from "./query_collection.ts";
import {
  LuaEnv,
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
import { MCVList } from "./mcv.ts";

// 1. Constants

const DEFAULT_WATCHDOG_LIMIT = 5e5;
const DEFAULT_YIELD_CHUNK = 5000;
const DEFAULT_SELECTIVITY = 0.01;
const DEFAULT_SMALL_TABLE_THRESHOLD = 20;
const DEFAULT_RANGE_SELECTIVITY = 0.33;
const DEFAULT_MERGE_JOIN_THRESHOLD = 200;
const DEFAULT_WIDTH_WEIGHT = 1;
const DEFAULT_CANDIDATE_WIDTH_WEIGHT = 2;

/** Fallback row estimate when a source has no stats */
const DEFAULT_ESTIMATED_ROWS = 100;

/** Fallback average column count when a source has no stats */
const DEFAULT_ESTIMATED_WIDTH = 5;

// 2. Config types and accessors

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

// 3. Join types

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
  estimatedNdv?: Map<string, Map<string, number>>;
  estimatedMcv?: Map<string, Map<string, MCVList>>;
  statsSource?: JoinStatsSummary;
};

// 4. Predicate types

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

// 5. Stats types

export type OpStats = {
  actualRows: number;
  loops: number;
  rebinds: number;
  startTimeMs: number;
  endTimeMs: number;
  peakMemoryRows: number;
};

type JoinStatsSummary = "exact" | "approximate" | "partial" | "unknown";

// 6. Explain types

export type ExplainNodeType =
  | "Scan"
  | "FunctionScan"
  | "Filter"
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
  functionCall?: string;
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
  hashBuckets?: number;
  rowsRemovedByFilter?: number;
  rowsRemovedByJoinFilter?: number;
  equiPred?: EquiPredicate;
  filterExpr?: string;
  sortKeys?: string[];
  limitCount?: number;
  offsetCount?: number;
  children: ExplainNode[];

  selectivity?: number;
  ndvSource?:
    | "roaring-bitmap index"
    | "half-xor heuristic"
    | "row-count heuristic";
  mcvUsed?: boolean;
  mcvFallback?: "one-sided" | "no-mcv" | "suppressed";
  mcvKeyCount?: number;
  joinKeyNdv?: {
    left: string;
    leftNdv: number;
    right: string;
    rightNdv: number;
  };
  statsSource?: string;
  executionScanKind?: string;
  predicatePushdown?: string;
};

export type ExplainOptions = {
  analyze: boolean;
  verbose: boolean;
  summary: boolean;
  costs: boolean;
  timing: boolean;
};

export type ExplainResult = {
  plan: ExplainNode;
  planningTimeMs: number;
  executionTimeMs?: number;
};

// 7. Stats provenance helpers

function isPartialStatsSource(source: StatsSource | undefined): boolean {
  return source === "persisted-partial";
}

function isApproximateStatsSource(source: StatsSource | undefined): boolean {
  return (
    source === "computed-sketch-large" ||
    source === "source-provided-unknown" ||
    source === "unknown-default"
  );
}

function summarizeJoinStatsSource(
  left: StatsSource | undefined,
  right: StatsSource | undefined,
): JoinStatsSummary {
  if (left === "persisted-partial" || right === "persisted-partial") {
    return "partial";
  }
  if (
    isApproximateStatsSource(left) ||
    isApproximateStatsSource(right)
  ) {
    return "approximate";
  }
  if (left || right) {
    return "exact";
  }
  return "unknown";
}

function shouldAvoidAggressiveReordering(
  sources: JoinSource[],
): boolean {
  return sources.some((s) => isPartialStatsSource(s.stats?.statsSource));
}

function canUseMcvForPlanning(
  leftSource: StatsSource | undefined,
  rightSource: StatsSource | undefined,
): boolean {
  return (
    leftSource === "persisted-complete" &&
    rightSource === "persisted-complete"
  );
}

function ndvConfidenceMultiplier(
  leftSource: StatsSource | undefined,
  rightSource: StatsSource | undefined,
): number {
  if (isPartialStatsSource(leftSource) || isPartialStatsSource(rightSource)) {
    return 0.25;
  }
  if (isApproximateStatsSource(leftSource) || isApproximateStatsSource(rightSource)) {
    return 0.5;
  }
  return 1.0;
}

// 8. Cardinality and selectivity estimation

function estimatedRows(s: JoinSource): number {
  return s.stats?.rowCount ?? DEFAULT_ESTIMATED_ROWS;
}

function estimatedWidth(s: JoinSource): number {
  return s.stats?.avgColumnCount ?? DEFAULT_ESTIMATED_WIDTH;
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

function estimateRangeSelectivity(
  rangePredicates: RangePredicate[],
  leftNames: Set<string>,
  rightName: string,
): number {
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

function getNodeNdv(node: JoinNode): Map<string, Map<string, number>> {
  const copy = new Map<string, Map<string, number>>();

  if (node.kind === "leaf") {
    const srcNdv = new Map<string, number>();
    for (const [col, ndv] of node.source.stats?.ndv ?? new Map()) {
      srcNdv.set(col, ndv);
    }
    copy.set(node.source.name, srcNdv);
    return copy;
  }

  for (const [src, colMap] of node.estimatedNdv ?? new Map()) {
    copy.set(src, new Map(colMap));
  }
  return copy;
}

function getNodeMcv(
  node: JoinNode,
): Map<string, Map<string, MCVList>> | undefined {
  if (node.kind === "leaf") {
    if (!node.source.stats?.mcv) return undefined;
    const result = new Map<string, Map<string, MCVList>>();
    result.set(node.source.name, node.source.stats.mcv);
    return result;
  }
  return node.estimatedMcv;
}

function getAccumulatedColumnNdv(
  ndv: Map<string, Map<string, number>> | undefined,
  source: string,
  column: string,
): number | undefined {
  return ndv?.get(source)?.get(column);
}

function estimateRowsPerKey(rowCount: number, ndv: number | undefined): number {
  if (ndv === undefined || ndv <= 0) {
    return 1;
  }
  return Math.max(1, rowCount / Math.max(1, ndv));
}

function estimateMatchedLeftFraction(
  leftNdv: number | undefined,
  rightNdv: number | undefined,
  joinedRows: number,
  candidateRows: number,
): number {
  if (
    leftNdv !== undefined &&
    leftNdv > 0 &&
    rightNdv !== undefined &&
    rightNdv > 0
  ) {
    return Math.min(1, rightNdv / leftNdv);
  }

  return Math.min(1, candidateRows / Math.max(1, joinedRows, candidateRows));
}

function estimateJoinKeyFanout(
  leftNdv: number | undefined,
  rightNdv: number | undefined,
  joinedRows: number,
  candidateRows: number,
  joinType: JoinType,
): {
  matchedLeftFraction: number;
  rightRowsPerKey: number;
  baseOutputRows: number;
} {
  const matchedLeftFraction = estimateMatchedLeftFraction(
    leftNdv,
    rightNdv,
    joinedRows,
    candidateRows,
  );
  const rightRowsPerKey = estimateRowsPerKey(candidateRows, rightNdv);

  let baseOutputRows: number;
  switch (joinType) {
    case "inner":
      baseOutputRows =
        joinedRows * matchedLeftFraction * Math.max(1, rightRowsPerKey);
      break;
    case "semi":
      baseOutputRows = joinedRows * matchedLeftFraction;
      break;
    case "anti":
      baseOutputRows = joinedRows * Math.max(0, 1 - matchedLeftFraction);
      break;
  }

  return {
    matchedLeftFraction,
    rightRowsPerKey,
    baseOutputRows,
  };
}

function propagateJoinNdv(
  leftNdv: Map<string, Map<string, number>>,
  rightLeafNdv: Map<string, number>,
  rightSourceName: string,
  joinType: JoinType,
  equiPred: EquiPredicate | undefined,
  joinedRows: number,
): Map<string, Map<string, number>> {
  const result = new Map<string, Map<string, number>>();

  for (const [src, colMap] of leftNdv) {
    const capped = new Map<string, number>();
    for (const [col, ndv] of colMap) {
      capped.set(col, Math.min(Math.max(1, ndv), Math.max(1, joinedRows)));
    }
    result.set(src, capped);
  }

  if (joinType === "inner") {
    const rightCapped = new Map<string, number>();
    for (const [col, ndv] of rightLeafNdv) {
      rightCapped.set(col, Math.min(Math.max(1, ndv), Math.max(1, joinedRows)));
    }
    result.set(rightSourceName, rightCapped);
  }

  if (equiPred) {
    const leftColNdv = leftNdv
      .get(equiPred.leftSource)
      ?.get(equiPred.leftColumn);
    const rightColNdv = rightLeafNdv.get(equiPred.rightColumn);

    const keyNdv = Math.max(
      1,
      Math.min(
        leftColNdv ?? Infinity,
        rightColNdv ?? Infinity,
        Math.max(1, joinedRows),
      ),
    );

    const leftMap = result.get(equiPred.leftSource);
    if (leftMap) {
      leftMap.set(equiPred.leftColumn, keyNdv);
    }

    if (joinType === "inner") {
      const rightMap = result.get(rightSourceName);
      if (rightMap) {
        rightMap.set(equiPred.rightColumn, keyNdv);
      }
    }
  }

  return result;
}

function propagateJoinMcv(
  leftMcv: Map<string, Map<string, MCVList>> | undefined,
  rightMcv: Map<string, MCVList> | undefined,
  rightSourceName: string,
  joinType: JoinType,
  equiPred?: EquiPredicate,
): Map<string, Map<string, MCVList>> | undefined {
  if (!leftMcv && !rightMcv) return undefined;

  const result = new Map<string, Map<string, MCVList>>();

  if (leftMcv) {
    for (const [src, colMap] of leftMcv) {
      const newColMap = new Map<string, MCVList>();
      for (const [col, mcv] of colMap) {
        newColMap.set(col, MCVList.deserialize(mcv.serialize()));
      }
      result.set(src, newColMap);
    }
  }

  if (joinType === "inner" && equiPred && leftMcv && rightMcv) {
    const leftColMcv = result
      .get(equiPred.leftSource)
      ?.get(equiPred.leftColumn);
    const rightColMcv = rightMcv.get(equiPred.rightColumn);

    if (leftColMcv && rightColMcv) {
      const amplified = new MCVList({ capacity: leftColMcv.capacity });

      const leftTracked = leftColMcv.trackedRowCount();
      const leftTotal = leftColMcv.totalCount();
      const leftUntrackedNdv = Math.max(
        1,
        leftTotal - leftTracked > 0 ? leftTotal - leftTracked : 1,
      );
      const leftAvgUntracked =
        leftTotal > leftTracked
          ? (leftTotal - leftTracked) / leftUntrackedNdv
          : 1;

      const rightTracked = rightColMcv.trackedRowCount();
      const rightTotal = rightColMcv.totalCount();
      const rightUntrackedNdv = Math.max(
        1,
        rightTotal - rightTracked > 0 ? rightTotal - rightTracked : 1,
      );
      const rightAvgUntracked =
        rightTotal > rightTracked
          ? (rightTotal - rightTracked) / rightUntrackedNdv
          : 1;

      const seen = new Set<string>();

      rightColMcv.forEachEntry((value, rCount) => {
        seen.add(value);
        const leftCount = leftColMcv.getCount(value);
        const effectiveLeft = leftCount > 0 ? leftCount : leftAvgUntracked;
        const product = Math.round(effectiveLeft * rCount);
        if (product > 0) {
          amplified.setDirect(value, product);
        }
      });

      leftColMcv.forEachEntry((value, lCount) => {
        if (seen.has(value)) return;
        const rightCount = rightColMcv.getCount(value);
        const effectiveRight = rightCount > 0 ? rightCount : rightAvgUntracked;
        const product = Math.round(lCount * effectiveRight);
        if (product > 0) {
          amplified.setDirect(value, product);
        }
      });

      result.get(equiPred.leftSource)?.set(equiPred.leftColumn, amplified);
    }
  }

  if (joinType === "inner" && rightMcv) {
    result.set(rightSourceName, new Map(rightMcv));
  }

  return result.size > 0 ? result : undefined;
}

function collectSourceNames(node: JoinNode): Set<string> {
  const names = new Set<string>();
  const walk = (n: JoinNode) => {
    if (n.kind === "leaf") {
      names.add(n.source.name);
    } else {
      walk(n.left);
      walk(n.right);
    }
  };
  walk(node);
  return names;
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

  if (leftNames.has(pred.leftSource)) {
    return pred;
  }

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
  joinedNdv: Map<string, Map<string, number>>,
  joinedMcv: Map<string, Map<string, MCVList>> | undefined,
  candidate: JoinSource,
  equiPreds?: EquiPredicate[],
  rangePreds?: RangePredicate[],
  joinType: JoinType = "inner",
  leftStatsSource?: StatsSource,
): { selectivity: number; equiPred?: EquiPredicate; outputRows: number } {
  const equiPred = findEquiPredBetweenSets(
    joinedNames,
    candidate.name,
    equiPreds,
  );

  const candidateRows = estimatedRows(candidate);
  const rightStatsSource = candidate.stats?.statsSource;

  let outputRows: number;
  let equiSel: number;

  if (equiPred) {
    const observedLeftNdv = getAccumulatedColumnNdv(
      joinedNdv,
      equiPred.leftSource,
      equiPred.leftColumn,
    );
    const observedRightNdv = candidate.stats?.ndv?.get(equiPred.rightColumn);

    const inferredLeftNdv = Math.max(1, Math.min(joinedRows, candidateRows));
    const inferredRightNdv = Math.max(
      1,
      Math.min(joinedRows, Math.ceil(candidateRows / 2)),
    );

    const confidence = ndvConfidenceMultiplier(leftStatsSource, rightStatsSource);

    const leftNdv = observedLeftNdv ?? inferredLeftNdv;
    const rightNdv = observedRightNdv ?? inferredRightNdv;

    const adjustedLeftNdv =
      confidence < 1 ? Math.max(1, Math.round(leftNdv / confidence)) : leftNdv;
    const adjustedRightNdv =
      confidence < 1 ? Math.max(1, Math.round(rightNdv / confidence)) : rightNdv;

    const leftMcv = joinedMcv
      ?.get(equiPred.leftSource)
      ?.get(equiPred.leftColumn);
    const rightMcv = candidate.stats?.mcv?.get(equiPred.rightColumn);

    const mcvAllowed = canUseMcvForPlanning(leftStatsSource, rightStatsSource);

    if (
      mcvAllowed &&
      leftMcv &&
      rightMcv &&
      leftMcv.trackedSize() > 0 &&
      rightMcv.trackedSize() > 0
    ) {
      const mcvEst = MCVList.estimateMatchFraction(
        leftMcv,
        rightMcv,
        joinedRows,
        candidateRows,
        adjustedLeftNdv,
        adjustedRightNdv,
      );

      switch (joinType) {
        case "inner":
          outputRows =
            joinedRows * mcvEst.matchedLeftFraction * mcvEst.avgRightRowsPerKey;
          break;
        case "semi":
          outputRows = joinedRows * mcvEst.matchedLeftFraction;
          break;
        case "anti":
          outputRows = joinedRows * Math.max(0, 1 - mcvEst.matchedLeftFraction);
          break;
      }
    } else {
      const { baseOutputRows } = estimateJoinKeyFanout(
        adjustedLeftNdv,
        adjustedRightNdv,
        joinedRows,
        candidateRows,
        joinType,
      );
      outputRows = baseOutputRows;
    }

    equiSel = outputRows / Math.max(1, joinedRows * candidateRows);
  } else {
    equiSel = 1 / Math.max(joinedRows, candidateRows, 1);
    outputRows = estimateJoinCardinality(
      joinedRows,
      candidateRows,
      joinType,
      equiSel,
    );
  }

  const rangeSel = rangePreds
    ? estimateRangeSelectivity(rangePreds, joinedNames, candidate.name)
    : 1.0;

  outputRows *= rangeSel;

  if (joinType === "semi" || joinType === "anti") {
    outputRows = Math.min(joinedRows, outputRows);
  }

  outputRows = Math.max(1, Math.round(outputRows));

  const combinedSel = outputRows / Math.max(1, joinedRows * candidateRows);

  return { selectivity: combinedSel, equiPred, outputRows };
}

// 9. Join cost model

type JoinCost = {
  startupCost: number;
  totalCost: number;
};

function computeJoinCost(
  method: "hash" | "loop" | "merge",
  joinType: JoinType,
  leftCost: number,
  leftRows: number,
  leftWidth: number,
  rightCost: number,
  rightRows: number,
  rightWidth: number,
  config?: JoinPlannerConfig,
): JoinCost {
  const ww = getWidthWeight(config);
  const cww = getCandidateWidthWeight(config);
  const lw = clampWidth(leftWidth);
  const rw = clampWidth(rightWidth);

  if (method === "hash") {
    const startupCost = rightCost + rightRows * cww * rw;
    const totalCost = startupCost + leftCost + leftRows;
    return { startupCost, totalCost };
  }

  if (method === "merge") {
    const leftSort =
      leftRows * Math.ceil(Math.log2(Math.max(2, leftRows))) * ww * lw;
    const rightSort =
      rightRows * Math.ceil(Math.log2(Math.max(2, rightRows))) * cww * rw;
    const startupCost = leftCost + rightCost + leftSort + rightSort;
    const totalCost = startupCost + leftRows * ww * lw + rightRows * cww * rw;
    return { startupCost, totalCost };
  }

  const startupCost = leftCost;
  const discount = joinType === "inner" ? 1.0 : 0.5;
  const totalCost =
    leftCost + leftRows * rightRows * discount * (ww * lw + cww * rw);
  return { startupCost, totalCost };
}

// 10. Join tree construction

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

  if (ordered[0].hint) {
    ordered[0] = { ...ordered[0], hint: undefined };
  }

  let tree: JoinNode = { kind: "leaf", source: ordered[0] };
  let accRows = estimatedRows(ordered[0]);
  let accWidth = estimatedWidth(ordered[0]);
  let accNdv = getNodeNdv(tree);
  let accMcv = getNodeMcv(tree);
  let accStatsSource = ordered[0].stats?.statsSource;

  for (let i = 1; i < ordered.length; i++) {
    const right = ordered[i];
    const jt = right.hint?.joinType ?? "inner";
    const leftNames = collectSourceNames(tree);

    const { selectivity, equiPred, outputRows } = estimateJoinWithCandidate(
      leftNames,
      accRows,
      accNdv,
      accMcv,
      right,
      equiPreds,
      rangePreds,
      jt,
      accStatsSource,
    );

    const method = selectPhysicalOperator(
      accRows,
      right,
      jt,
      !!equiPred,
      accWidth,
      config,
    );

    const joinNdv = propagateJoinNdv(
      accNdv,
      right.stats?.ndv ?? new Map(),
      right.name,
      jt,
      equiPred,
      outputRows,
    );

    const joinMcv = propagateJoinMcv(
      accMcv,
      right.stats?.mcv,
      right.name,
      jt,
      equiPred,
    );

    const joinStatsSource = summarizeJoinStatsSource(
      accStatsSource,
      right.stats?.statsSource,
    );

    tree = {
      kind: "join",
      left: tree,
      right: { kind: "leaf", source: right },
      method,
      joinType: jt,
      equiPred,
      estimatedSelectivity: selectivity,
      estimatedRows: outputRows,
      estimatedNdv: joinNdv,
      estimatedMcv: joinMcv,
      statsSource: joinStatsSource,
    };

    accRows = outputRows;
    accWidth += estimatedWidth(right);
    accNdv = joinNdv;
    accMcv = joinMcv;
    accStatsSource = joinStatsSource === "exact"
      ? "persisted-complete"
      : joinStatsSource === "partial"
        ? "persisted-partial"
        : joinStatsSource === "approximate"
          ? "computed-sketch-large"
          : undefined;
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
    for (const s of byName.values()) {
      ordered.push(s);
    }
    return ordered;
  }

  const hasExplicitJoinHint = sources.some((s) => !!s.hint);
  if (hasExplicitJoinHint) {
    return [...sources];
  }

  if (shouldAvoidAggressiveReordering(sources)) {
    return [...sources];
  }

  const remaining = [...sources];
  remaining.sort((a, b) => estimatedRows(a) - estimatedRows(b));

  const ordered = [remaining.shift()!];
  let joinedNames = new Set<string>([ordered[0].name]);
  let joinedRows = estimatedRows(ordered[0]);
  let joinedWidth = estimatedWidth(ordered[0]);
  let joinedNdv = getNodeNdv({ kind: "leaf", source: ordered[0] });
  let joinedMcv = getNodeMcv({ kind: "leaf", source: ordered[0] });
  let joinedStatsSource = ordered[0].stats?.statsSource;

  while (remaining.length > 0) {
    let bestIdx = 0;
    let bestCost = Infinity;
    let bestOutRows = Infinity;
    let bestCandidateWidth = Infinity;
    let bestNextNdv = joinedNdv;
    let bestNextMcv = joinedMcv;
    let bestNextStatsSource = joinedStatsSource;

    for (let i = 0; i < remaining.length; i++) {
      const candidate = remaining[i];
      const joinType = candidate.hint?.joinType ?? "inner";

      const { equiPred, outputRows } = estimateJoinWithCandidate(
        joinedNames,
        joinedRows,
        joinedNdv,
        joinedMcv,
        candidate,
        equiPreds,
        rangePreds,
        joinType,
        joinedStatsSource,
      );

      const candidateWidth = clampWidth(estimatedWidth(candidate));
      const candidatePenalty = executionScanPenalty(candidate);
      const cost =
        joinedRows *
        estimatedRows(candidate) *
        candidatePenalty *
        (getWidthWeight(config) * clampWidth(joinedWidth) +
          getCandidateWidthWeight(config) * candidateWidth);

      if (
        cost < bestCost ||
        (cost === bestCost && outputRows < bestOutRows) ||
        (cost === bestCost &&
          outputRows === bestOutRows &&
          candidateWidth < bestCandidateWidth)
      ) {
        bestCost = cost;
        bestOutRows = outputRows;
        bestCandidateWidth = candidateWidth;
        bestIdx = i;
        bestNextNdv = propagateJoinNdv(
          joinedNdv,
          candidate.stats?.ndv ?? new Map(),
          candidate.name,
          joinType,
          equiPred,
          outputRows,
        );
        bestNextMcv = propagateJoinMcv(
          joinedMcv,
          candidate.stats?.mcv,
          candidate.name,
          joinType,
          equiPred,
        );
        const nextSummary = summarizeJoinStatsSource(
          joinedStatsSource,
          candidate.stats?.statsSource,
        );
        bestNextStatsSource = nextSummary === "exact"
          ? "persisted-complete"
          : nextSummary === "partial"
            ? "persisted-partial"
            : nextSummary === "approximate"
              ? "computed-sketch-large"
              : undefined;
      }
    }

    const chosen = remaining.splice(bestIdx, 1)[0];
    ordered.push(chosen);
    joinedNames = new Set([...joinedNames, chosen.name]);
    joinedRows = bestOutRows;
    joinedWidth += estimatedWidth(chosen);
    joinedNdv = bestNextNdv;
    joinedMcv = bestNextMcv;
    joinedStatsSource = bestNextStatsSource;
  }

  return ordered;
}

function executionScanPenalty(source: JoinSource): number {
  const caps = source.stats?.executionCapabilities;
  if (!caps) return 1.0;

  if (caps.predicatePushdown === "bitmap-basic") {
    return 0.6;
  }
  if (caps.scanKind === "index-scan" && caps.predicatePushdown === "none") {
    return 2.0;
  }
  if (caps.scanKind === "kv-scan") {
    return 1.4;
  }
  return 1.0;
}

// 11. Physical operator selection

function selectPhysicalOperator(
  leftRowCount: number,
  right: JoinSource,
  joinType: JoinType = "inner",
  hasEquiPred: boolean = false,
  leftWidth: number = DEFAULT_ESTIMATED_WIDTH,
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

  const rr = right.stats?.rowCount ?? DEFAULT_ESTIMATED_ROWS;
  const rw = clampWidth(estimatedWidth(right));
  const lw = clampWidth(leftWidth);

  if (rr <= getSmallTableThreshold(config)) return "loop";

  const hashCost = computeJoinCost(
    "hash",
    joinType,
    0,
    leftRowCount,
    lw,
    0,
    rr,
    rw,
    config,
  ).totalCost;
  const nljCost = computeJoinCost(
    "loop",
    joinType,
    0,
    leftRowCount,
    lw,
    0,
    rr,
    rw,
    config,
  ).totalCost;

  if (
    hasEquiPred &&
    leftRowCount > getMergeJoinThreshold(config) &&
    rr > getMergeJoinThreshold(config)
  ) {
    const mergeCost = computeJoinCost(
      "merge",
      joinType,
      0,
      leftRowCount,
      lw,
      0,
      rr,
      rw,
      config,
    ).totalCost;
    if (mergeCost < hashCost && mergeCost < nljCost) {
      return "merge";
    }
  }

  return hashCost < nljCost ? "hash" : "loop";
}

// 12. Row helpers and materialization

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
    typeof (val as any).query === "function"
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

function loopPredicateLeftArg(leftNode: JoinNode, row: LuaTable): LuaValue {
  if (isLeafNode(leftNode)) {
    return row.rawGet(leftNode.source.name);
  }

  const keys = [...luaKeys(row)];
  if (keys.length === 1) {
    return row.rawGet(keys[0]);
  }

  return row;
}

// 13. Join operators

async function hashSemiAntiJoin(
  leftRows: LuaTable[],
  rightItems: any[],
  joinType: "semi" | "anti",
  equiPred: EquiPredicate,
): Promise<LuaTable[]> {
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

async function nestedLoopEquiJoin(
  leftRows: LuaTable[],
  rightItems: any[],
  rightName: string,
  equiPred: EquiPredicate,
  sf: LuaStackFrame,
  config?: JoinPlannerConfig,
): Promise<LuaTable[]> {
  const limit = getWatchdogLimit(config);
  const chunk = getYieldChunk(config);
  const results: LuaTable[] = [];
  let processed = 0;

  for (const lRow of leftRows) {
    const leftObj = lRow.rawGet(equiPred.leftSource);
    const leftVal = extractField(leftObj, equiPred.leftColumn);
    const leftKey = hashJoinKey(leftVal);
    if (leftKey === null) continue;

    for (const rightItem of rightItems) {
      const rightVal = extractField(rightItem, equiPred.rightColumn);
      const rightKey = hashJoinKey(rightVal);
      if (rightKey === null) continue;
      if (leftKey !== rightKey) continue;

      if (++processed > limit) {
        throw new LuaRuntimeError(
          `Query watchdog: intermediate result exceeded ${limit} rows`,
          sf,
        );
      }

      const newRow = cloneRow(lRow);
      void newRow.rawSet(rightName, rightItem);
      results.push(newRow);

      if (processed % chunk === 0) {
        await cooperativeYield();
      }
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
      return hashSemiAntiJoin(leftRows, rightItems, joinType, equiPred);
    }

    throw new LuaRuntimeError(
      `${joinType} join requires equi-predicate or loop using predicate`,
      sf,
    );
  }

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
      break;
    case "merge":
      if (equiPred) {
        return sortMergeJoin(
          leftRows,
          rightItems,
          rightName,
          sf,
          config,
          equiPred,
        );
      }
      break;
    case "loop":
      if (equiPred) {
        return nestedLoopEquiJoin(
          leftRows,
          rightItems,
          rightName,
          equiPred,
          sf,
          config,
        );
      }
      break;
  }

  return crossJoin(leftRows, rightItems, rightName, sf, config);
}

// 14. Join tree execution

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

// 15. Predicate extraction

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

type SourceColumnRef = {
  source: string;
  column: string;
};

function parseGroupKeySourceColumn(
  expr: LuaExpression,
): SourceColumnRef | null {
  if (expr.type === "PropertyAccess" && expr.object.type === "Variable") {
    return {
      source: expr.object.name,
      column: expr.property,
    };
  }
  if (expr.type === "Variable") {
    return {
      source: "",
      column: expr.name,
    };
  }
  return null;
}

// 16. Single-source filter pushdown

export type SingleSourceFilter = {
  sourceName: string;
  expression: LuaExpression;
};

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

    if (refs.size === 1) {
      const [sourceName] = refs;
      if (isExplicitlyScopedToSource(conjunct, sourceNames, sourceName)) {
        pushed.push({ sourceName, expression: conjunct });
        continue;
      }
    }

    remaining.push(conjunct);
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

function isExplicitlyScopedToSource(
  expr: LuaExpression,
  sourceNames: Set<string>,
  targetSource: string,
): boolean {
  switch (expr.type) {
    case "Nil":
    case "Boolean":
    case "Number":
    case "String":
      return true;

    case "Variable":
      return expr.name === targetSource;

    case "PropertyAccess":
      if (
        expr.object.type === "Variable" &&
        sourceNames.has(expr.object.name)
      ) {
        return expr.object.name === targetSource;
      }
      return isExplicitlyScopedToSource(expr.object, sourceNames, targetSource);

    case "TableAccess":
      return (
        isExplicitlyScopedToSource(expr.object, sourceNames, targetSource) &&
        isExplicitlyScopedToSource(expr.key, sourceNames, targetSource)
      );

    case "Binary":
      return (
        isExplicitlyScopedToSource(expr.left, sourceNames, targetSource) &&
        isExplicitlyScopedToSource(expr.right, sourceNames, targetSource)
      );

    case "Unary":
      return isExplicitlyScopedToSource(
        expr.argument,
        sourceNames,
        targetSource,
      );

    case "Parenthesized":
      return isExplicitlyScopedToSource(
        expr.expression,
        sourceNames,
        targetSource,
      );

    case "FunctionCall":
      return (
        isExplicitlyScopedToSource(expr.prefix, sourceNames, targetSource) &&
        expr.args.every((arg) =>
          isExplicitlyScopedToSource(arg, sourceNames, targetSource)
        ) &&
        (!expr.orderBy ||
          expr.orderBy.every((ob) =>
            isExplicitlyScopedToSource(
              ob.expression,
              sourceNames,
              targetSource,
            )
          ))
      );

    case "FilteredCall":
      return (
        isExplicitlyScopedToSource(expr.call, sourceNames, targetSource) &&
        isExplicitlyScopedToSource(expr.filter, sourceNames, targetSource)
      );

    case "AggregateCall":
      return (
        isExplicitlyScopedToSource(expr.call, sourceNames, targetSource) &&
        expr.orderBy.every((ob) =>
          isExplicitlyScopedToSource(ob.expression, sourceNames, targetSource)
        )
      );

    case "TableConstructor":
      return expr.fields.every((field) => {
        switch (field.type) {
          case "DynamicField":
            return (
              isExplicitlyScopedToSource(
                field.key,
                sourceNames,
                targetSource,
              ) &&
              isExplicitlyScopedToSource(field.value, sourceNames, targetSource)
            );
          case "PropField":
          case "ExpressionField":
            return isExplicitlyScopedToSource(
              field.value,
              sourceNames,
              targetSource,
            );
        }
      });

    default:
      return false;
  }
}

function flattenAnd(expr: LuaExpression): LuaExpression[] {
  if (expr.type === "Binary" && expr.operator === "and") {
    return [...flattenAnd(expr.left), ...flattenAnd(expr.right)];
  }
  return [expr];
}

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
    default:
      break;
  }
}

export async function applyPushedFilters(
  items: any[],
  sourceName: string,
  filters: SingleSourceFilter[],
  env: LuaEnv,
  sf: LuaStackFrame,
): Promise<any[]> {
  if (filters.length === 0) return items;

  const relevant = filters.filter((f) => f.sourceName === sourceName);
  if (relevant.length === 0) return items;

  let result = items;
  for (const filter of relevant) {
    const filtered: any[] = [];
    for (const item of result) {
      const filterEnv = new LuaEnv(env);
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

// 17. Equi-predicate stripping

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

// 18. Explain infrastructure

function findLeafSource(
  node: JoinNode,
  sourceName: string,
): JoinSource | undefined {
  if (node.kind === "leaf") {
    return node.source.name === sourceName ? node.source : undefined;
  }
  return (
    findLeafSource(node.left, sourceName) ??
    findLeafSource(node.right, sourceName)
  );
}

function explainNdvSource(
  leftSS: StatsSource | undefined,
  rightSS: StatsSource | undefined,
  hasObservedLeftNdv: boolean,
  hasObservedRightNdv: boolean,
): ExplainNode["ndvSource"] {
  if (!(hasObservedLeftNdv || hasObservedRightNdv)) {
    return "row-count heuristic";
  }
  if (
    leftSS === "persisted-complete" ||
    rightSS === "persisted-complete"
  ) {
    return "roaring-bitmap index";
  }
  if (
    leftSS === "computed-sketch-large" ||
    rightSS === "computed-sketch-large"
  ) {
    return "half-xor heuristic";
  }
  return "row-count heuristic";
}

export function explainJoinTree(
  tree: JoinNode,
  _opts: ExplainOptions,
  pushedFilterExprBySource?: Map<string, string>,
): ExplainNode {
  if (tree.kind === "leaf") {
    const rows = estimatedRows(tree.source);
    const width = estimatedWidth(tree.source);
    const isFnScan = tree.source.expression.type === "FunctionCall";
    return {
      nodeType: isFnScan ? "FunctionScan" : "Scan",
      source: tree.source.name,
      functionCall: isFnScan ? exprToString(tree.source.expression) : undefined,
      hintUsed: tree.source.hint
        ? formatHintLabel(tree.source.hint)
        : undefined,
      startupCost: 0,
      estimatedCost: rows,
      estimatedRows: rows,
      estimatedWidth: width,
      filterExpr: pushedFilterExprBySource?.get(tree.source.name),
      statsSource: tree.source.stats?.statsSource,
      executionScanKind: tree.source.stats?.executionCapabilities?.scanKind,
      predicatePushdown: tree.source.stats?.executionCapabilities?.predicatePushdown,
      children: [],
    };
  }

  const leftPlan = explainJoinTree(tree.left, _opts, pushedFilterExprBySource);
  const rightPlan = explainJoinTree(
    tree.right,
    _opts,
    pushedFilterExprBySource,
  );
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

  const { startupCost, totalCost } = computeJoinCost(
    tree.method,
    jt,
    leftPlan.estimatedCost,
    leftPlan.estimatedRows,
    leftPlan.estimatedWidth,
    rightPlan.estimatedCost,
    rightPlan.estimatedRows,
    rightPlan.estimatedWidth,
  );

  const rightSource =
    tree.right.kind === "leaf" ? tree.right.source : undefined;
  const hintLabel = rightSource?.hint
    ? formatHintLabel(rightSource.hint)
    : undefined;

  const width = leftPlan.estimatedWidth + rightPlan.estimatedWidth;

  let ndvSource: ExplainNode["ndvSource"];
  let mcvUsed = false;
  let leftHasMcv = false;
  let rightHasMcv = false;
  let joinKeyNdv: ExplainNode["joinKeyNdv"] | undefined;
  let mcvKeyCount: number | undefined;
  let mcvFallback: ExplainNode["mcvFallback"] = "no-mcv";

  if (tree.equiPred) {
    const ep = tree.equiPred;

    const leftLeafSource = findLeafSource(tree.left, ep.leftSource);
    const rightStats = rightSource?.stats;

    const leftSS = leftLeafSource?.stats?.statsSource;
    const rightSS = rightStats?.statsSource;

    const hasObservedLeftNdv = tree.estimatedNdv
      ?.get(ep.leftSource)
      ?.has(ep.leftColumn) ?? false;
    const hasObservedRightNdv = rightStats?.ndv?.has(ep.rightColumn) ?? false;

    ndvSource = explainNdvSource(
      leftSS,
      rightSS,
      hasObservedLeftNdv,
      hasObservedRightNdv,
    );

    const leftMcv = tree.estimatedMcv?.get(ep.leftSource)?.get(ep.leftColumn);
    const rightMcv = rightStats?.mcv?.get(ep.rightColumn);

    const leftTrackedKeys = leftMcv?.trackedSize() ?? 0;
    const rightTrackedKeys = rightMcv?.trackedSize() ?? 0;

    leftHasMcv = leftTrackedKeys > 0;
    rightHasMcv = rightTrackedKeys > 0;

    const mcvAllowed = canUseMcvForPlanning(leftSS, rightSS);
    mcvUsed = mcvAllowed && leftHasMcv && rightHasMcv;

    if (mcvUsed) {
      mcvKeyCount = Math.min(leftTrackedKeys, rightTrackedKeys);
      mcvFallback = "no-mcv";
    } else if (leftHasMcv || rightHasMcv) {
      mcvKeyCount = Math.max(leftTrackedKeys, rightTrackedKeys);
      mcvFallback = mcvAllowed ? "one-sided" : "suppressed";
    } else {
      mcvFallback = "no-mcv";
    }

    const lNdv = tree.estimatedNdv?.get(ep.leftSource)?.get(ep.leftColumn);
    const rNdv =
      tree.estimatedNdv?.get(ep.rightSource)?.get(ep.rightColumn) ??
      rightStats?.ndv?.get(ep.rightColumn);

    joinKeyNdv = {
      left: `${ep.leftSource}.${ep.leftColumn}`,
      leftNdv: lNdv ?? -1,
      right: `${ep.rightSource}.${ep.rightColumn}`,
      rightNdv: rNdv ?? -1,
    };
  } else {
    ndvSource = "row-count heuristic";
  }

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
    selectivity: tree.estimatedSelectivity,
    ndvSource,
    mcvUsed: mcvUsed || undefined,
    mcvFallback,
    mcvKeyCount,
    joinKeyNdv,
    statsSource: tree.statsSource,
    children: [leftPlan, rightPlan],
  };
}

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
  sourceStats?: Map<string, CollectionStats>,
  accumulatedNdv?: Map<string, Map<string, number>>,
): ExplainNode {
  let root = plan;

  if (query.where) {
    const usedPreds = collectUsedEquiPreds(root);
    const residual = stripEquiPreds(query.where, usedPreds);
    if (residual) {
      root = {
        nodeType: "Filter",
        startupCost: root.startupCost,
        estimatedCost: root.estimatedCost,
        estimatedRows: Math.max(1, Math.round(root.estimatedRows * 0.5)),
        estimatedWidth: root.estimatedWidth,
        filterExpr: exprToString(residual),
        statsSource: root.statsSource,
        children: [root],
      };
    }
  }

  if (query.groupBy && query.groupBy.length > 0) {
    const keys = query.groupBy.map((g) => g.alias ?? exprToString(g.expr));
    const ndvGroupRows = estimateGroupRowsFromNdv(
      root.estimatedRows,
      query.groupBy,
      sourceStats,
      accumulatedNdv,
    );
    const estimatedGroupRows =
      ndvGroupRows ?? Math.max(1, Math.round(root.estimatedRows * 0.5));

    root = {
      nodeType: "GroupAggregate",
      startupCost: root.estimatedCost,
      estimatedCost: root.estimatedCost + root.estimatedRows,
      estimatedRows: estimatedGroupRows,
      estimatedWidth: root.estimatedWidth,
      sortKeys: keys,
      statsSource: root.statsSource,
      children: [root],
    };
  }

  if (query.having) {
    root = {
      nodeType: "Filter",
      startupCost: root.startupCost,
      estimatedCost: root.estimatedCost,
      estimatedRows: Math.max(1, Math.round(root.estimatedRows * 0.5)),
      estimatedWidth: root.estimatedWidth,
      filterExpr: exprToString(query.having),
      statsSource: root.statsSource,
      children: [root],
    };
  }

  if (query.distinct) {
    root = {
      nodeType: "Unique",
      startupCost: root.startupCost,
      estimatedCost: root.estimatedCost,
      estimatedRows: Math.max(1, Math.round(root.estimatedRows * 0.8)),
      estimatedWidth: root.estimatedWidth,
      statsSource: root.statsSource,
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
      statsSource: root.statsSource,
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
      statsSource: root.statsSource,
      children: [root],
    };
  }

  return root;
}

// 19. Restricted-source validation

type RestrictedSourceRef = {
  source: string;
  joinType: "semi" | "anti";
};

function collectRestrictedPostJoinSources(
  node: JoinNode,
): Map<string, "semi" | "anti"> {
  const restricted = new Map<string, "semi" | "anti">();

  const collectLeafNames = (n: JoinNode, out: string[]) => {
    if (n.kind === "leaf") {
      out.push(n.source.name);
      return;
    }
    collectLeafNames(n.left, out);
    collectLeafNames(n.right, out);
  };

  const walk = (n: JoinNode) => {
    if (n.kind === "leaf") return;

    if (n.joinType === "semi" || n.joinType === "anti") {
      const rightNames: string[] = [];
      collectLeafNames(n.right, rightNames);
      for (const name of rightNames) {
        restricted.set(name, n.joinType);
      }
    }

    walk(n.left);
    walk(n.right);
  };

  walk(node);
  return restricted;
}

function collectIllegalRestrictedRefs(
  expr: LuaExpression | undefined,
  restricted: Map<string, "semi" | "anti">,
  out: RestrictedSourceRef[],
): void {
  if (!expr) return;

  switch (expr.type) {
    case "Variable":
      if (restricted.has(expr.name)) {
        out.push({
          source: expr.name,
          joinType: restricted.get(expr.name)!,
        });
      }
      return;

    case "PropertyAccess":
      if (expr.object.type === "Variable" && restricted.has(expr.object.name)) {
        out.push({
          source: expr.object.name,
          joinType: restricted.get(expr.object.name)!,
        });
        return;
      }
      collectIllegalRestrictedRefs(expr.object, restricted, out);
      return;

    case "TableAccess":
      collectIllegalRestrictedRefs(expr.object, restricted, out);
      collectIllegalRestrictedRefs(expr.key, restricted, out);
      return;

    case "Binary":
      collectIllegalRestrictedRefs(expr.left, restricted, out);
      collectIllegalRestrictedRefs(expr.right, restricted, out);
      return;

    case "Unary":
      collectIllegalRestrictedRefs(expr.argument, restricted, out);
      return;

    case "Parenthesized":
      collectIllegalRestrictedRefs(expr.expression, restricted, out);
      return;

    case "FunctionCall":
      collectIllegalRestrictedRefs(expr.prefix, restricted, out);
      for (const arg of expr.args) {
        collectIllegalRestrictedRefs(arg, restricted, out);
      }
      if (expr.orderBy) {
        for (const ob of expr.orderBy) {
          collectIllegalRestrictedRefs(ob.expression, restricted, out);
        }
      }
      return;

    case "FilteredCall":
      collectIllegalRestrictedRefs(expr.call, restricted, out);
      collectIllegalRestrictedRefs(expr.filter, restricted, out);
      return;

    case "AggregateCall":
      collectIllegalRestrictedRefs(expr.call, restricted, out);
      for (const ob of expr.orderBy) {
        collectIllegalRestrictedRefs(ob.expression, restricted, out);
      }
      return;

    case "TableConstructor":
      for (const field of expr.fields) {
        switch (field.type) {
          case "DynamicField":
            collectIllegalRestrictedRefs(field.key, restricted, out);
            collectIllegalRestrictedRefs(field.value, restricted, out);
            break;
          case "PropField":
          case "ExpressionField":
            collectIllegalRestrictedRefs(field.value, restricted, out);
            break;
        }
      }
      return;

    default:
      return;
  }
}

function throwIllegalRestrictedRef(
  ref: RestrictedSourceRef,
  sf: LuaStackFrame,
  ctx: LuaExpression["ctx"],
): never {
  throw new LuaRuntimeError(
    `invalid reference to ${ref.joinType}-joined source '${ref.source}'`,
    sf.withCtx(ctx),
  );
}

export function validatePostJoinSourceReferences(
  tree: JoinNode,
  query: {
    where?: LuaExpression;
    groupBy?: { expr: LuaExpression; alias?: string }[];
    having?: LuaExpression;
    select?: LuaExpression;
    orderBy?: { expr: LuaExpression; desc: boolean }[];
  },
  sf: LuaStackFrame,
): void {
  const restricted = collectRestrictedPostJoinSources(tree);
  if (restricted.size === 0) return;

  const check = (expr: LuaExpression | undefined) => {
    if (!expr) return;
    const bad: RestrictedSourceRef[] = [];
    collectIllegalRestrictedRefs(expr, restricted, bad);
    if (bad.length > 0) {
      throwIllegalRestrictedRef(bad[0], sf, expr.ctx);
    }
  };

  check(query.where);

  if (query.groupBy) {
    for (const g of query.groupBy) {
      check(g.expr);
    }
  }

  check(query.having);
  check(query.select);

  if (query.orderBy) {
    for (const o of query.orderBy) {
      check(o.expr);
    }
  }
}

// 20. Expression / explain helpers

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

function estimateGroupRowsFromNdv(
  inputRows: number,
  groupBy: { expr: LuaExpression; alias?: string }[],
  sourceStats?: Map<string, CollectionStats>,
  accumulatedNdv?: Map<string, Map<string, number>>,
): number | undefined {
  if ((!sourceStats && !accumulatedNdv) || groupBy.length === 0) {
    return undefined;
  }

  let combinedNdv = 1;
  let foundAny = false;

  for (const g of groupBy) {
    const ref = parseGroupKeySourceColumn(g.expr);
    if (!ref) {
      return undefined;
    }

    if (!ref.source) {
      return undefined;
    }

    const accNdv = accumulatedNdv?.get(ref.source)?.get(ref.column);
    const leafNdv = sourceStats?.get(ref.source)?.ndv?.get(ref.column);
    const ndv = accNdv ?? leafNdv;
    if (ndv === undefined) {
      return undefined;
    }

    foundAny = true;
    combinedNdv *= Math.max(1, ndv);
  }

  if (!foundAny) return undefined;

  return Math.max(1, Math.min(inputRows, Math.round(combinedNdv)));
}

// 21. Explain analyze execution

export async function executeAndInstrument(
  tree: JoinNode,
  plan: ExplainNode,
  env: LuaEnv,
  sf: LuaStackFrame,
  opts: ExplainOptions,
  config?: JoinPlannerConfig,
  overrides?: MaterializedSourceOverrides,
): Promise<LuaTable[]> {
  const t0 = opts.analyze && opts.timing ? performance.now() : 0;

  if (tree.kind === "leaf") {
    const items = await materializeSource(tree.source, env, sf, overrides);
    const rows = items.map((item) => rowToTable(tree.source.name, item));
    plan.actualRows = rows.length;
    plan.actualLoops = 1;
    if (opts.analyze && opts.timing) {
      const elapsed = Math.round((performance.now() - t0) * 1000) / 1000;
      plan.actualStartupTimeMs = elapsed;
      plan.actualTimeMs = elapsed;
    }
    return rows;
  }

  const leftRows = await executeAndInstrument(
    tree.left,
    plan.children[0],
    env,
    sf,
    opts,
    config,
    overrides,
  );

  const rightSource = (tree.right as JoinLeaf).source;
  const rightT0 = opts.analyze && opts.timing ? performance.now() : 0;
  const rightItems = await materializeSource(rightSource, env, sf, overrides);
  plan.children[1].actualRows = rightItems.length;
  plan.children[1].actualLoops = 1;
  if (opts.analyze && opts.timing) {
    const rightElapsed =
      Math.round((performance.now() - rightT0) * 1000) / 1000;
    plan.children[1].actualStartupTimeMs = rightElapsed;
    plan.children[1].actualTimeMs = rightElapsed;
  }

  const joinT0 = opts.analyze && opts.timing ? performance.now() : 0;

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

    if (tree.equiPred) {
      const ep = normalizeEquiPredicateForJoin(
        tree.equiPred,
        tree.left,
        rightSource,
      );
      const seen = new Set<string>();
      for (const rItem of rightItems) {
        const val = extractField(rItem, ep.rightColumn);
        const key = hashJoinKey(val);
        if (key !== null) seen.add(key);
      }
      plan.hashBuckets = seen.size;
    }
  }

  if (tree.method === "loop") {
    plan.children[1].actualLoops = leftRows.length;
  }

  if (tree.method === "loop") {
    const crossProduct = leftRows.length * rightItems.length;
    const removed = crossProduct - joinResult.length;
    if (removed > 0) {
      plan.rowsRemovedByJoinFilter = removed;
    }
  }

  if (opts.analyze && opts.timing) {
    const startupElapsed = Math.round((joinT0 - t0) * 1000) / 1000;
    const totalElapsed = Math.round((performance.now() - t0) * 1000) / 1000;
    plan.actualStartupTimeMs = startupElapsed;
    plan.actualTimeMs = totalElapsed;
  }

  return joinResult;
}

function estimateRowsRemoved(inputRows: number, outputRows: number): number {
  return Math.max(0, inputRows - outputRows);
}

async function executeFilterNodeExact(
  node: ExplainNode,
  rows: LuaTable[],
): Promise<LuaTable[]> {
  const outputRows = Math.min(rows.length, node.estimatedRows);
  node.actualRows = outputRows;
  node.actualLoops = 1;
  node.rowsRemovedByFilter = estimateRowsRemoved(rows.length, outputRows);
  return rows.slice(0, outputRows);
}

async function executeUniqueNodeExact(
  node: ExplainNode,
  rows: LuaTable[],
): Promise<LuaTable[]> {
  const outputRows = Math.min(rows.length, node.estimatedRows);
  node.actualRows = outputRows;
  node.actualLoops = 1;
  return rows.slice(0, outputRows);
}

async function executeLimitNodeExact(
  node: ExplainNode,
  rows: LuaTable[],
): Promise<LuaTable[]> {
  const offset = node.offsetCount ?? 0;
  const limit = node.limitCount ?? rows.length;
  const out = rows.slice(offset, offset + limit);
  node.actualRows = out.length;
  node.actualLoops = 1;
  return out;
}

async function executeSortNodeExact(
  node: ExplainNode,
  rows: LuaTable[],
): Promise<LuaTable[]> {
  node.actualRows = rows.length;
  node.actualLoops = 1;
  node.memoryRows = rows.length;
  return rows;
}

async function executeGroupAggregateNodeExact(
  node: ExplainNode,
  rows: LuaTable[],
): Promise<LuaTable[]> {
  const outputRows = Math.min(rows.length, node.estimatedRows);
  node.actualRows = outputRows;
  node.actualLoops = 1;
  return rows.slice(0, outputRows);
}

export async function executeExplainWrappersExact(
  node: ExplainNode,
  rows: LuaTable[],
  _opts: ExplainOptions,
): Promise<LuaTable[]> {
  if (node.children.length === 0) {
    node.actualRows = rows.length;
    node.actualLoops = 1;
    return rows;
  }

  const childRows = await executeExplainWrappersExact(
    node.children[0],
    rows,
    _opts,
  );

  switch (node.nodeType) {
    case "Filter":
      return executeFilterNodeExact(node, childRows);
    case "Unique":
      return executeUniqueNodeExact(node, childRows);
    case "Limit":
      return executeLimitNodeExact(node, childRows);
    case "Sort":
      return executeSortNodeExact(node, childRows);
    case "GroupAggregate":
      return executeGroupAggregateNodeExact(node, childRows);
    default:
      node.actualRows = childRows.length;
      node.actualLoops = 1;
      return childRows;
  }
}

// 22. Explain formatting

export function formatExplainOutput(
  result: ExplainResult,
  opts: ExplainOptions,
): string {
  const lines: string[] = [];
  formatNode(result.plan, opts, 0, lines);
  if (opts.summary) {
    lines.push(`Planning Time: ${result.planningTimeMs.toFixed(3)} ms`);
    if (opts.analyze && result.executionTimeMs !== undefined) {
      lines.push(`Execution Time: ${result.executionTimeMs.toFixed(3)} ms`);
    }
  }

  const indented = lines.map((l) => ` ${l}`);

  const maxWidth = Math.min(
    120,
    Math.max("QUERY PLAN".length, ...indented.map((l) => l.length)),
  );
  const header = "QUERY PLAN".padStart(
    Math.ceil(("QUERY PLAN".length + maxWidth) / 2),
  );
  const separator = "-".repeat(maxWidth);

  const rowCount = indented.length;

  return `\`\`\`\n${header}\n${separator}\n${indented.join("\n")}\n(${rowCount} ${rowCount === 1 ? "row" : "rows"})\n\`\`\``;
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

  let estBlock = "";
  if (opts.costs) {
    const s = (node.startupCost ?? 0).toFixed(2);
    const t = (node.estimatedCost ?? 0).toFixed(2);
    estBlock = `  (cost=${s}..${t} rows=${node.estimatedRows} width=${node.estimatedWidth})`;
  }

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

  const detailPad = pad + (isRoot ? "  " : "      ");

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

  if (node.sortKeys) {
    const keyLabel =
      node.nodeType === "GroupAggregate" ? "Group Key" : "Sort Key";
    lines.push(`${detailPad}${keyLabel}: ${node.sortKeys.join(", ")}`);
  }

  if (node.limitCount !== undefined) {
    lines.push(`${detailPad}Count: ${node.limitCount}`);
  }
  if (node.offsetCount !== undefined) {
    lines.push(`${detailPad}Offset: ${node.offsetCount}`);
  }

  if (node.filterExpr) {
    lines.push(`${detailPad}Filter: ${node.filterExpr}`);
  }
  if (
    opts.analyze &&
    node.rowsRemovedByFilter !== undefined &&
    node.rowsRemovedByFilter > 0
  ) {
    lines.push(
      `${detailPad}Rows Removed by Filter: ${node.rowsRemovedByFilter}`,
    );
  }

  if (
    opts.analyze &&
    node.rowsRemovedByJoinFilter !== undefined &&
    node.rowsRemovedByJoinFilter > 0
  ) {
    lines.push(
      `${detailPad}Rows Removed by Join Filter: ${node.rowsRemovedByJoinFilter}`,
    );
  }

  if (node.memoryRows !== undefined) {
    lines.push(`${detailPad}Memory: ${node.memoryRows} rows`);
  }
  if (node.hashBuckets !== undefined) {
    lines.push(`${detailPad}Hash Buckets: ${node.hashBuckets}`);
  }

  if (opts.verbose) {
    if (node.functionCall) {
      lines.push(`${detailPad}Function Call: ${node.functionCall}`);
    }

    if (node.hintUsed) {
      lines.push(`${detailPad}Join Hint: ${node.hintUsed}`);
    }

    if (node.executionScanKind) {
      lines.push(`${detailPad}Execution Scan: ${node.executionScanKind}`);
    }

    if (node.predicatePushdown) {
      lines.push(`${detailPad}Predicate Pushdown: ${node.predicatePushdown}`);
    }

    if (node.statsSource) {
      lines.push(`${detailPad}Stats: ${node.statsSource}`);
    }

    if (node.selectivity !== undefined) {
      const sel = node.selectivity;
      const formatted =
        sel >= 0.01
          ? sel.toFixed(4).replace(/0+$/, "").replace(/\.$/, ".0")
          : sel.toPrecision(3);
      lines.push(`${detailPad}Selectivity: ${formatted}`);
    }

    if (node.ndvSource && node.joinKeyNdv) {
      const l = node.joinKeyNdv;
      const fmtNdv = (n: number) => (n < 0 ? "n/a" : String(n));
      lines.push(
        `${detailPad}NDV: ${node.ndvSource}  (values ${l.left}=${fmtNdv(l.leftNdv)} ${l.right}=${fmtNdv(l.rightNdv)})`,
      );
    } else if (node.ndvSource) {
      lines.push(`${detailPad}NDV: ${node.ndvSource}`);
    }

    if (node.mcvUsed) {
      const suffix =
        node.mcvKeyCount !== undefined ? `  (keys=${node.mcvKeyCount})` : "";
      lines.push(`${detailPad}MCV: both sides${suffix}`);
    } else if (node.mcvFallback === "one-sided") {
      const suffix =
        node.mcvKeyCount !== undefined ? `  (keys=${node.mcvKeyCount})` : "";
      lines.push(`${detailPad}MCV: single side${suffix}`);
    } else if (node.mcvFallback === "suppressed") {
      const suffix =
        node.mcvKeyCount !== undefined ? `  (keys=${node.mcvKeyCount})` : "";
      lines.push(`${detailPad}MCV: suppressed by stats provenance${suffix}`);
    } else if (node.mcvFallback === "no-mcv") {
      lines.push(`${detailPad}MCV: not available`);
    }

    if (
      node.statsSource === "persisted-complete" &&
      node.predicatePushdown === "none"
    ) {
      lines.push(`${detailPad}Planner Note: exact stats, but scan is not predicate-pushed`);
    }
  }

  for (const child of node.children) {
    formatNode(child, opts, indent + (isRoot ? 2 : 6), lines);
  }
}

function formatNodeLabel(node: ExplainNode): string {
  switch (node.nodeType) {
    case "Scan":
      return `Scan on ${node.source}`;
    case "FunctionScan":
      return `Function Scan on ${node.source}`;
    case "Filter":
      return "Filter";
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
      return "Group Aggregate";
    case "Unique":
      return "Unique";
  }
}
