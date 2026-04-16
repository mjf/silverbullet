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
import type { Config } from "../config.ts";

// 1. Constants

const DEFAULT_WATCHDOG_LIMIT = 5e5;
const DEFAULT_YIELD_CHUNK = 5000;
const DEFAULT_SMALL_TABLE_THRESHOLD = 20;
const DEFAULT_RANGE_SELECTIVITY = 0.33;
const DEFAULT_MERGE_JOIN_THRESHOLD = 200;
const DEFAULT_WIDTH_WEIGHT = 1;
const DEFAULT_CANDIDATE_WIDTH_WEIGHT = 2;
const DEFAULT_ESTIMATED_ROWS = 100;
const DEFAULT_ESTIMATED_WIDTH = 5;
const DEFAULT_SEMI_ANTI_LOOP_DISCOUNT = 0.5;
const DEFAULT_PARTIAL_STATS_CONFIDENCE = 0.25;
const DEFAULT_APPROXIMATE_STATS_CONFIDENCE = 0.5;
const DEFAULT_BITMAP_SCAN_PENALTY = 0.6;
const DEFAULT_INDEX_SCAN_NO_PUSHDOWN_PENALTY = 2.0;
const DEFAULT_KV_SCAN_PENALTY = 1.4;
const DEFAULT_FILTER_SELECTIVITY = 0.5;
const DEFAULT_DISTINCT_SURVIVAL_RATIO = 0.8;
const DEFAULT_INFERRED_NDV_DIVISOR = 2;

// 2. Config types and accessors

export type MaterializedSourceOverrides = Map<string, any[]>;

export type JoinPlannerConfig = {
  watchdogLimit?: number;
  yieldChunk?: number;
  smallTableThreshold?: number;
  mergeJoinThreshold?: number;
  widthWeight?: number;
  candidateWidthWeight?: number;
  semiAntiLoopDiscount?: number;
  partialStatsConfidence?: number;
  approximateStatsConfidence?: number;
  bitmapScanPenalty?: number;
  indexScanNoPushdownPenalty?: number;
  kvScanPenalty?: number;
  defaultFilterSelectivity?: number;
  defaultDistinctSurvivalRatio?: number;
  defaultRangeSelectivity?: number;
  inferredNdvDivisor?: number;
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

function getSemiAntiLoopDiscount(config?: JoinPlannerConfig): number {
  return finiteNumberOrDefault(
    config?.semiAntiLoopDiscount,
    DEFAULT_SEMI_ANTI_LOOP_DISCOUNT,
  );
}

function getPartialStatsConfidence(config?: JoinPlannerConfig): number {
  return finiteNumberOrDefault(
    config?.partialStatsConfidence,
    DEFAULT_PARTIAL_STATS_CONFIDENCE,
  );
}

function getApproximateStatsConfidence(config?: JoinPlannerConfig): number {
  return finiteNumberOrDefault(
    config?.approximateStatsConfidence,
    DEFAULT_APPROXIMATE_STATS_CONFIDENCE,
  );
}

function getBitmapScanPenalty(config?: JoinPlannerConfig): number {
  return finiteNumberOrDefault(
    config?.bitmapScanPenalty,
    DEFAULT_BITMAP_SCAN_PENALTY,
  );
}

function getIndexScanNoPushdownPenalty(config?: JoinPlannerConfig): number {
  return finiteNumberOrDefault(
    config?.indexScanNoPushdownPenalty,
    DEFAULT_INDEX_SCAN_NO_PUSHDOWN_PENALTY,
  );
}

function getKvScanPenalty(config?: JoinPlannerConfig): number {
  return finiteNumberOrDefault(config?.kvScanPenalty, DEFAULT_KV_SCAN_PENALTY);
}

function getDefaultFilterSelectivity(config?: JoinPlannerConfig): number {
  return finiteNumberOrDefault(
    config?.defaultFilterSelectivity,
    DEFAULT_FILTER_SELECTIVITY,
  );
}

function getDefaultDistinctSurvivalRatio(config?: JoinPlannerConfig): number {
  return finiteNumberOrDefault(
    config?.defaultDistinctSurvivalRatio,
    DEFAULT_DISTINCT_SURVIVAL_RATIO,
  );
}

function getDefaultRangeSelectivity(config?: JoinPlannerConfig): number {
  return finiteNumberOrDefault(
    config?.defaultRangeSelectivity,
    DEFAULT_RANGE_SELECTIVITY,
  );
}

function getInferredNdvDivisor(config?: JoinPlannerConfig): number {
  return finiteNumberOrDefault(
    config?.inferredNdvDivisor,
    DEFAULT_INFERRED_NDV_DIVISOR,
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
  joinResiduals?: LuaExpression[];
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

// 6. Shared query-clause types

export type OrderByEntry = {
  expr: LuaExpression;
  desc: boolean;
  nulls?: "first" | "last";
  using?: unknown;
};

// 7. Explain types

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
  | "Project"
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
  rowsRemovedByUnique?: number;
  equiPred?: EquiPredicate;
  joinResidualExprs?: string[];
  filterExpr?: string;
  sortKeys?: string[];
  limitCount?: number;
  offsetCount?: number;
  children: ExplainNode[];

  whereExpr?: LuaExpression;
  havingExpr?: LuaExpression;
  orderBySpec?: {
    expr: LuaExpression;
    desc: boolean;
    nulls?: "first" | "last";
    using?: string;
  }[];
  groupBySpec?: { expr: LuaExpression; alias?: string }[];
  distinctSpec?: boolean;

  outputColumns?: string[];
  aggregates?: AggregateDescription[];
  implicitGroup?: boolean;
  filterType?: "where" | "having";
  pushedDownFilter?: boolean;
  joinFilterType?: "join" | "join-residual";

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

export type AggregateDescription = {
  name: string;
  args: string;
  filter?: string;
  orderBy?: string;
  rowsFiltered?: number;
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

// 8. Stats provenance helpers

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
  if (isApproximateStatsSource(left) || isApproximateStatsSource(right)) {
    return "approximate";
  }
  if (left || right) {
    return "exact";
  }
  return "unknown";
}

function shouldAvoidAggressiveReordering(sources: JoinSource[]): boolean {
  return sources.some((s) => isPartialStatsSource(s.stats?.statsSource));
}

function canUseMcvForPlanning(
  leftSource: StatsSource | undefined,
  rightSource: StatsSource | undefined,
): boolean {
  return (
    leftSource === "persisted-complete" && rightSource === "persisted-complete"
  );
}

function ndvConfidenceMultiplier(
  leftSource: StatsSource | undefined,
  rightSource: StatsSource | undefined,
  config?: JoinPlannerConfig,
): number {
  if (isPartialStatsSource(leftSource) || isPartialStatsSource(rightSource)) {
    return getPartialStatsConfidence(config);
  }
  if (
    isApproximateStatsSource(leftSource) ||
    isApproximateStatsSource(rightSource)
  ) {
    return getApproximateStatsConfidence(config);
  }
  return 1.0;
}

// 9. Cardinality and selectivity estimation

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
  config?: JoinPlannerConfig,
): number {
  let sel = 1.0;
  const rangeSel = getDefaultRangeSelectivity(config);
  for (const rp of rangePredicates) {
    if (
      (leftNames.has(rp.leftSource) && rp.rightSource === rightName) ||
      (leftNames.has(rp.rightSource) && rp.leftSource === rightName)
    ) {
      sel *= rangeSel;
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
  leftNdvMap?: Map<string, Map<string, number>>,
  rightNdvMap?: Map<string, number>,
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
      const leftColNdv =
        leftNdvMap?.get(equiPred.leftSource)?.get(equiPred.leftColumn) ??
        leftColMcv.trackedSize();
      const leftUntrackedNdv = Math.max(
        1,
        leftColNdv - leftColMcv.trackedSize(),
      );
      const leftUntrackedRows = Math.max(0, leftTotal - leftTracked);
      const leftAvgUntracked =
        leftUntrackedRows > 0 ? leftUntrackedRows / leftUntrackedNdv : 1;

      const rightTracked = rightColMcv.trackedRowCount();
      const rightTotal = rightColMcv.totalCount();
      const rightColNdv =
        rightNdvMap?.get(equiPred.rightColumn) ?? rightColMcv.trackedSize();
      const rightUntrackedNdv = Math.max(
        1,
        rightColNdv - rightColMcv.trackedSize(),
      );
      const rightUntrackedRows = Math.max(0, rightTotal - rightTracked);
      const rightAvgUntracked =
        rightUntrackedRows > 0 ? rightUntrackedRows / rightUntrackedNdv : 1;

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
  config?: JoinPlannerConfig,
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

    const ndvDivisor = getInferredNdvDivisor(config);
    const inferredLeftNdv = Math.max(1, Math.min(joinedRows, candidateRows));
    const inferredRightNdv = Math.max(
      1,
      Math.min(joinedRows, Math.ceil(candidateRows / ndvDivisor)),
    );

    const confidence = ndvConfidenceMultiplier(
      leftStatsSource,
      rightStatsSource,
      config,
    );

    const leftNdv = observedLeftNdv ?? inferredLeftNdv;
    const rightNdv = observedRightNdv ?? inferredRightNdv;

    const adjustedLeftNdv =
      confidence < 1 ? Math.max(1, Math.round(leftNdv / confidence)) : leftNdv;
    const adjustedRightNdv =
      confidence < 1
        ? Math.max(1, Math.round(rightNdv / confidence))
        : rightNdv;

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
    ? estimateRangeSelectivity(rangePreds, joinedNames, candidate.name, config)
    : 1.0;

  outputRows *= rangeSel;

  if (joinType === "semi" || joinType === "anti") {
    outputRows = Math.min(joinedRows, outputRows);
  }

  outputRows = Math.max(1, Math.round(outputRows));

  const combinedSel = outputRows / Math.max(1, joinedRows * candidateRows);

  return { selectivity: combinedSel, equiPred, outputRows };
}

// 10. Join cost model

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
  const discount = joinType === "inner" ? 1.0 : getSemiAntiLoopDiscount(config);
  const totalCost =
    leftCost + leftRows * rightRows * discount * (ww * lw + cww * rw);
  return { startupCost, totalCost };
}

// 11. Join tree construction

export function buildJoinTree(
  sources: JoinSource[],
  planOrder?: string[],
  equiPreds?: EquiPredicate[],
  rangePreds?: RangePredicate[],
  residualWhere?: LuaExpression,
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
      config,
    );

    let method = selectPhysicalOperator(
      accRows,
      right,
      jt,
      !!equiPred,
      accWidth,
      config,
    );

    if (!equiPred && method !== "loop") {
      method = "loop";
    }

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
      accNdv,
      right.stats?.ndv ?? new Map(),
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
    accStatsSource =
      joinStatsSource === "exact"
        ? "persisted-complete"
        : joinStatsSource === "partial"
          ? "persisted-partial"
          : joinStatsSource === "approximate"
            ? "computed-sketch-large"
            : undefined;
  }

  if (residualWhere) {
    assignResidualPredicatesToLowestCoveringJoin(
      tree,
      residualWhere,
      equiPreds,
    );
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
        config,
      );

      const candidateWidth = clampWidth(estimatedWidth(candidate));
      const candidatePenalty = executionScanPenalty(candidate, config);

      const cost =
        (outputRows + estimatedRows(candidate)) *
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
          joinedNdv,
          candidate.stats?.ndv ?? new Map(),
        );
        const nextSummary = summarizeJoinStatsSource(
          joinedStatsSource,
          candidate.stats?.statsSource,
        );
        bestNextStatsSource =
          nextSummary === "exact"
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

function executionScanPenalty(
  source: JoinSource,
  config?: JoinPlannerConfig,
): number {
  const caps = source.stats?.executionCapabilities;
  if (!caps) return 1.0;

  if (caps.predicatePushdown === "bitmap-basic") {
    return getBitmapScanPenalty(config);
  }
  if (caps.scanKind === "index-scan" && caps.predicatePushdown === "none") {
    return getIndexScanNoPushdownPenalty(config);
  }
  if (caps.scanKind === "kv-scan") {
    return getKvScanPenalty(config);
  }
  return 1.0;
}

// 12. Physical operator selection

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
        throw new Error("merge join requires equi-predicate");
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

  if (rr <= getSmallTableThreshold(config)) {
    if (
      (joinType === "semi" || joinType === "anti") &&
      hasEquiPred &&
      !right.hint?.using
    ) {
      return "hash";
    }
    return "loop";
  }

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

// 13. Row helpers and materialization

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
    `Equi-predicate does not match join sides: left={${[...leftNames].join(",")}} right=${rightName}`,
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

async function evaluateJoinResiduals(
  leftRow: LuaTable,
  rightName: string,
  rightItem: any,
  residuals: LuaExpression[] | undefined,
  env: LuaEnv,
  sf: LuaStackFrame,
): Promise<boolean> {
  if (!residuals || residuals.length === 0) return true;

  const rowEnv = new LuaEnv(env);
  for (const k of luaKeys(leftRow)) {
    rowEnv.setLocal(String(k), leftRow.rawGet(k));
  }
  rowEnv.setLocal(rightName, rightItem);

  for (const residual of residuals) {
    const val = await evalExpression(residual, rowEnv, sf);
    if (!luaTruthy(val)) {
      return false;
    }
  }
  return true;
}

// 14. Join operators

async function hashSemiAntiJoin(
  leftRows: LuaTable[],
  rightItems: any[],
  joinType: "semi" | "anti",
  equiPred: EquiPredicate,
  rightName: string,
  residuals: LuaExpression[] | undefined,
  env: LuaEnv,
  sf: LuaStackFrame,
): Promise<LuaTable[]> {
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
  for (const lRow of leftRows) {
    const leftObj = lRow.rawGet(equiPred.leftSource);
    const val = extractField(leftObj, equiPred.leftColumn);
    const key = hashJoinKey(val);

    let found = false;
    if (key !== null) {
      const bucket = buildMap.get(key) ?? [];
      for (const rItem of bucket) {
        if (
          await evaluateJoinResiduals(
            lRow,
            rightName,
            rItem,
            residuals,
            env,
            sf,
          )
        ) {
          found = true;
          break;
        }
      }
    }

    if (joinType === "semi" && found) results.push(lRow);
    else if (joinType === "anti" && !found) results.push(lRow);
  }
  return results;
}

async function nestedLoopSemiAntiJoin(
  leftRows: LuaTable[],
  rightItems: any[],
  leftNode: JoinNode,
  predicate: LuaValue,
  joinType: "semi" | "anti",
  rightName: string,
  residuals: LuaExpression[] | undefined,
  env: LuaEnv,
  sf: LuaStackFrame,
  equiPred?: EquiPredicate,
): Promise<LuaTable[]> {
  const results: LuaTable[] = [];
  for (const leftRow of leftRows) {
    if (equiPred) {
      const leftObj = leftRow.rawGet(equiPred.leftSource);
      const val = extractField(leftObj, equiPred.leftColumn);
      const key = hashJoinKey(val);
      if (key === null) {
        if (joinType === "anti") results.push(leftRow);
        continue;
      }
    }

    const leftArg = loopPredicateLeftArg(leftNode, leftRow);
    let found = false;
    for (const rightItem of rightItems) {
      const res = singleResult(
        await luaCall(predicate, [leftArg, rightItem], sf.astCtx ?? {}, sf),
      );
      if (!luaTruthy(res)) continue;
      if (
        !(await evaluateJoinResiduals(
          leftRow,
          rightName,
          rightItem,
          residuals,
          env,
          sf,
        ))
      ) {
        continue;
      }
      found = true;
      break;
    }
    if (joinType === "semi" && found) results.push(leftRow);
    else if (joinType === "anti" && !found) results.push(leftRow);
  }
  return results;
}

async function residualLoopJoin(
  leftRows: LuaTable[],
  rightItems: any[],
  rightName: string,
  residuals: LuaExpression[],
  env: LuaEnv,
  sf: LuaStackFrame,
  config?: JoinPlannerConfig,
): Promise<LuaTable[]> {
  const limit = getWatchdogLimit(config);
  const chunk = getYieldChunk(config);
  const results: LuaTable[] = [];
  let processed = 0;

  for (const leftRow of leftRows) {
    for (const rightItem of rightItems) {
      if (
        !(await evaluateJoinResiduals(
          leftRow,
          rightName,
          rightItem,
          residuals,
          env,
          sf,
        ))
      ) {
        continue;
      }

      if (++processed > limit) {
        throw new LuaRuntimeError(
          `Query watchdog: intermediate result exceeded ${limit} rows`,
          sf,
        );
      }

      const newRow = cloneRow(leftRow);
      void newRow.rawSet(rightName, rightItem);
      results.push(newRow);

      if (processed % chunk === 0) {
        await cooperativeYield();
      }
    }
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
  residuals: LuaExpression[] | undefined,
  env: LuaEnv,
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
      if (
        !(await evaluateJoinResiduals(
          lRow,
          rightName,
          rItem,
          residuals,
          env,
          sf,
        ))
      ) {
        continue;
      }
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
  residuals: LuaExpression[] | undefined,
  env: LuaEnv,
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
      if (
        !(await evaluateJoinResiduals(
          lRow,
          rightName,
          rightItem,
          residuals,
          env,
          sf,
        ))
      ) {
        continue;
      }

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
  residuals: LuaExpression[] | undefined,
  env: LuaEnv,
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
      if (
        !(await evaluateJoinResiduals(
          leftRow,
          rightName,
          rightItem,
          residuals,
          env,
          sf,
        ))
      ) {
        continue;
      }
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
  residuals: LuaExpression[] | undefined,
  env: LuaEnv,
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

  if (equiPred) {
    while (
      leftKeyed.length > 0 &&
      hashJoinKey(
        extractField(
          leftKeyed[0].row.rawGet(equiPred.leftSource),
          equiPred.leftColumn,
        ),
      ) === null
    ) {
      leftKeyed.shift();
    }
    while (
      rightKeyed.length > 0 &&
      hashJoinKey(extractField(rightKeyed[0].item, equiPred.rightColumn)) ===
        null
    ) {
      rightKeyed.shift();
    }
  }

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
        if (
          !(await evaluateJoinResiduals(
            leftRow,
            rightName,
            rightItem,
            residuals,
            env,
            sf,
          ))
        ) {
          continue;
        }
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
  const residuals = tree.joinResiduals;

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
        rightName,
        residuals,
        env,
        sf,
        equiPred,
      );
    }

    if (equiPred) {
      return hashSemiAntiJoin(
        leftRows,
        rightItems,
        joinType,
        equiPred,
        rightName,
        residuals,
        env,
        sf,
      );
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
      residuals,
      env,
      sf,
      config,
    );
  }

  if (equiPred) {
    switch (tree.method) {
      case "hash":
        return hashInnerJoin(
          leftRows,
          rightItems,
          rightName,
          equiPred,
          residuals,
          env,
          sf,
          config,
        );
      case "merge":
        return sortMergeJoin(
          leftRows,
          rightItems,
          rightName,
          residuals,
          env,
          sf,
          config,
          equiPred,
        );
      case "loop":
        return nestedLoopEquiJoin(
          leftRows,
          rightItems,
          rightName,
          equiPred,
          residuals,
          env,
          sf,
          config,
        );
    }
  }

  if (tree.method !== "loop") {
    (tree as JoinInner).method = "loop";
  }

  if (residuals && residuals.length > 0) {
    return residualLoopJoin(
      leftRows,
      rightItems,
      rightName,
      residuals,
      env,
      sf,
      config,
    );
  }

  return crossJoin(leftRows, rightItems, rightName, sf, config);
}

// 15. Join tree execution

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
  if (tree.right.kind !== "leaf") {
    throw new Error(
      "Join planner: right child must be a leaf node (left-deep trees only)",
    );
  }
  const rightSource = tree.right.source;
  const rightItems = await materializeSource(rightSource, env, sf, overrides);
  return dispatchJoin(tree, leftRows, rightItems, rightSource, env, sf, config);
}

// 16. Predicate extraction

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

// 17. Single-source filter pushdown

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
          isExplicitlyScopedToSource(arg, sourceNames, targetSource),
        ) &&
        (!expr.orderBy ||
          expr.orderBy.every((ob) =>
            isExplicitlyScopedToSource(
              ob.expression,
              sourceNames,
              targetSource,
            ),
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
          isExplicitlyScopedToSource(ob.expression, sourceNames, targetSource),
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
          default:
            return false;
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

export async function applyPushedFiltersWithStats(
  items: any[],
  sourceName: string,
  filters: SingleSourceFilter[],
  env: LuaEnv,
  sf: LuaStackFrame,
): Promise<{ result: any[]; removedCount: number }> {
  if (filters.length === 0) return { result: items, removedCount: 0 };

  const relevant = filters.filter((f) => f.sourceName === sourceName);
  if (relevant.length === 0) return { result: items, removedCount: 0 };

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
  return { result, removedCount: items.length - result.length };
}

// 18. Predicate stripping

export function stripUsedJoinPredicates(
  expr: LuaExpression | undefined,
  tree: JoinNode,
): LuaExpression | undefined {
  if (!expr) return undefined;
  const usedPreds = collectUsedEquiPredsFromJoinTree(tree);
  const usedResiduals = collectUsedJoinResidualsFromJoinTree(tree);
  return stripJoinPredicates(expr, usedPreds, usedResiduals);
}

function collectUsedEquiPredsFromJoinTree(node: JoinNode): EquiPredicate[] {
  if (node.kind === "leaf") return [];
  const result: EquiPredicate[] = [];
  if (node.equiPred) result.push(node.equiPred);
  result.push(...collectUsedEquiPredsFromJoinTree(node.left));
  result.push(...collectUsedEquiPredsFromJoinTree(node.right));
  return result;
}

function collectUsedJoinResidualsFromJoinTree(node: JoinNode): LuaExpression[] {
  if (node.kind === "leaf") return [];
  const result: LuaExpression[] = [];
  if (node.joinResiduals) {
    result.push(...node.joinResiduals);
  }
  result.push(...collectUsedJoinResidualsFromJoinTree(node.left));
  result.push(...collectUsedJoinResidualsFromJoinTree(node.right));
  return result;
}

// 19. Explain infrastructure

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
  if (leftSS === "persisted-complete" || rightSS === "persisted-complete") {
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
    const pushedFilterExpr = pushedFilterExprBySource?.get(tree.source.name);
    const predicatePushdownKind =
      tree.source.stats?.executionCapabilities?.predicatePushdown;

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
      filterExpr: pushedFilterExpr,
      pushedDownFilter: !!pushedFilterExpr,
      statsSource: tree.source.stats?.statsSource,
      executionScanKind: tree.source.stats?.executionCapabilities?.scanKind,
      predicatePushdown: predicatePushdownKind,
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
      1 / Math.max(leftPlan.estimatedRows, rightPlan.estimatedRows, 1),
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

    const hasObservedLeftNdv =
      tree.estimatedNdv?.get(ep.leftSource)?.has(ep.leftColumn) ?? false;
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
    joinResidualExprs: tree.joinResiduals?.map(exprToString),
    joinFilterType:
      tree.joinResiduals && tree.joinResiduals.length > 0
        ? "join-residual"
        : "join",
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

function collectOutputColumns(expr: LuaExpression): string[] {
  if (expr.type !== "TableConstructor") {
    return [exprToString(expr)];
  }

  const outputs: string[] = [];

  for (const field of expr.fields) {
    switch (field.type) {
      case "PropField": {
        outputs.push(`${field.key} = ${exprToString(field.value)}`);
        break;
      }
      case "DynamicField": {
        const keyStr = exprToString(field.key);
        const valStr = exprToString(field.value);
        outputs.push(`[${keyStr}] = ${valStr}`);
        break;
      }
      case "ExpressionField": {
        outputs.push(exprToString(field.value));
        break;
      }
    }
  }

  return outputs;
}

const KNOWN_AGGREGATE_NAMES = new Set([
  "count",
  "sum",
  "avg",
  "min",
  "max",
  "array_agg",
  "string_agg",
  "group_concat",
  "bool_and",
  "bool_or",
  "every",
  "percentile_cont",
  "percentile_disc",
  "json_agg",
  "json_object_agg",
]);

function selectContainsAggregate(expr: LuaExpression): boolean {
  switch (expr.type) {
    case "FilteredCall":
      return true;
    case "AggregateCall":
      return true;
    case "FunctionCall":
      if (
        expr.prefix.type === "Variable" &&
        KNOWN_AGGREGATE_NAMES.has(expr.prefix.name)
      ) {
        return true;
      }
      return expr.args.some(selectContainsAggregate);
    case "TableConstructor":
      return expr.fields.some((f) => {
        switch (f.type) {
          case "PropField":
          case "ExpressionField":
            return selectContainsAggregate(f.value);
          case "DynamicField":
            return (
              selectContainsAggregate(f.key) || selectContainsAggregate(f.value)
            );
        }
      });
    case "Binary":
      return (
        selectContainsAggregate(expr.left) ||
        selectContainsAggregate(expr.right)
      );
    case "Unary":
      return selectContainsAggregate(expr.argument);
    case "Parenthesized":
      return selectContainsAggregate(expr.expression);
    default:
      return false;
  }
}

export function wrapPlanWithQueryOps(
  plan: ExplainNode,
  query: {
    orderBy?: OrderByEntry[];
    limit?: number;
    offset?: number;
    groupBy?: { expr: LuaExpression; alias?: string }[];
    where?: LuaExpression;
    having?: LuaExpression;
    select?: LuaExpression;
    distinct?: boolean;
  },
  sourceStats?: Map<string, CollectionStats>,
  accumulatedNdv?: Map<string, Map<string, number>>,
  config?: JoinPlannerConfig,
): ExplainNode {
  let root = plan;
  const filterSel = getDefaultFilterSelectivity(config);

  if (query.where) {
    root = {
      nodeType: "Filter",
      startupCost: root.startupCost,
      estimatedCost: root.estimatedCost,
      estimatedRows: Math.max(1, Math.round(root.estimatedRows * filterSel)),
      estimatedWidth: root.estimatedWidth,
      filterExpr: exprToString(query.where),
      whereExpr: query.where,
      filterType: "where",
      statsSource: root.statsSource,
      children: [root],
    };
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
      ndvGroupRows ?? Math.max(1, Math.round(root.estimatedRows * filterSel));

    const aggDescs: AggregateDescription[] = [];
    if (query.select) {
      aggDescs.push(...collectAggregateDescriptions(query.select));
    }
    if (query.having) {
      aggDescs.push(...collectAggregateDescriptions(query.having));
    }

    const seen = new Set<string>();
    const uniqueAggs: AggregateDescription[] = [];
    for (const agg of aggDescs) {
      let sig = `${agg.name}(${agg.args})`;
      if (agg.filter) sig += ` filter(${agg.filter})`;
      if (agg.orderBy) sig += ` order by ${agg.orderBy}`;
      if (!seen.has(sig)) {
        seen.add(sig);
        uniqueAggs.push(agg);
      }
    }

    root = {
      nodeType: "GroupAggregate",
      startupCost: root.estimatedCost,
      estimatedCost: root.estimatedCost + root.estimatedRows,
      estimatedRows: estimatedGroupRows,
      estimatedWidth: root.estimatedWidth,
      sortKeys: keys,
      groupBySpec: query.groupBy,
      aggregates: uniqueAggs.length > 0 ? uniqueAggs : undefined,
      implicitGroup: query.groupBy.length === 0 ? true : undefined,
      statsSource: root.statsSource,
      children: [root],
    };
  }

  if (query.having) {
    root = {
      nodeType: "Filter",
      startupCost: root.startupCost,
      estimatedCost: root.estimatedCost,
      estimatedRows: Math.max(1, Math.round(root.estimatedRows * filterSel)),
      estimatedWidth: root.estimatedWidth,
      filterExpr: exprToString(query.having),
      havingExpr: query.having,
      filterType: "having",
      statsSource: root.statsSource,
      children: [root],
    };
  }

  const hasExplicitGroupBy = query.groupBy && query.groupBy.length > 0;
  const isImplicitAggregate =
    query.select &&
    !hasExplicitGroupBy &&
    selectContainsAggregate(query.select);

  if (isImplicitAggregate) {
    const cols = collectOutputColumns(query.select!);
    const aggDescs = collectAggregateDescriptions(query.select!);
    root = {
      nodeType: "GroupAggregate",
      startupCost: root.estimatedCost,
      estimatedCost: root.estimatedCost + root.estimatedRows,
      estimatedRows: 1,
      estimatedWidth: cols.length > 0 ? cols.length : root.estimatedWidth,
      sortKeys: [],
      outputColumns: cols,
      aggregates: aggDescs.length > 0 ? aggDescs : undefined,
      implicitGroup: true,
      statsSource: root.statsSource,
      children: [root],
    };
  }

  if (query.select) {
    const cols = collectOutputColumns(query.select);
    root = {
      nodeType: "Project",
      startupCost: root.startupCost,
      estimatedCost: root.estimatedCost + root.estimatedRows,
      estimatedRows: root.estimatedRows,
      estimatedWidth: cols.length > 0 ? cols.length : root.estimatedWidth,
      outputColumns: cols,
      statsSource: root.statsSource,
      children: [root],
    };
  } else {
    let implicitCols: string[] = ["*"];
    if (sourceStats && sourceStats.size > 0) {
      const allColumns = new Set<string>();
      for (const stats of sourceStats.values()) {
        for (const col of stats.ndv.keys()) {
          allColumns.add(col);
        }
      }
      if (allColumns.size > 0) {
        implicitCols = [...allColumns].sort();
      }
    }
    root = {
      nodeType: "Project",
      startupCost: root.startupCost,
      estimatedCost: root.estimatedCost,
      estimatedRows: root.estimatedRows,
      estimatedWidth: root.estimatedWidth,
      outputColumns: implicitCols,
      implicitGroup: false,
      statsSource: root.statsSource,
      children: [root],
    };
  }

  if (query.orderBy && query.orderBy.length > 0) {
    const keys = query.orderBy.map((o) => {
      let s = exprToString(o.expr);
      if (o.desc) s += " desc";
      if (o.nulls) s += ` nulls ${o.nulls}`;
      if (o.using) {
        s += ` using ${typeof o.using === "string" ? o.using : "<function>"}`;
      }
      return s;
    });
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
      orderBySpec: query.orderBy.map((o) => ({
        expr: o.expr,
        desc: o.desc,
        nulls: o.nulls,
        using:
          typeof o.using === "string"
            ? o.using
            : o.using
              ? "<function>"
              : undefined,
      })),
      statsSource: root.statsSource,
      children: [root],
    };
  }

  if (query.distinct) {
    root = {
      nodeType: "Unique",
      startupCost: root.startupCost,
      estimatedCost: root.estimatedCost,
      estimatedRows: Math.max(
        1,
        Math.round(
          root.estimatedRows * getDefaultDistinctSurvivalRatio(config),
        ),
      ),
      estimatedWidth: root.estimatedWidth,
      distinctSpec: true,
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

// 20. Restricted-source validation

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
    orderBy?: OrderByEntry[];
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

// 21. Expression / explain helpers

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
    case "Parenthesized":
      return exprToString(expr.expression);
    case "FunctionDefinition":
      return "<anonymous>";
    default:
      return "?";
  }
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

function stripJoinPredicates(
  expr: LuaExpression,
  preds: EquiPredicate[],
  residuals: LuaExpression[],
): LuaExpression | undefined {
  if (expr.type === "Binary" && expr.operator === "and") {
    const left = stripJoinPredicates(expr.left, preds, residuals);
    const right = stripJoinPredicates(expr.right, preds, residuals);
    if (!left && !right) return undefined;
    if (!left) return right;
    if (!right) return left;
    return { ...expr, left, right };
  }
  if (exprMatchesEquiPred(expr, preds)) return undefined;
  if (residuals.some((r) => exprStructurallyEquals(expr, r))) return undefined;
  return expr;
}

function exprStructurallyEquals(a: LuaExpression, b: LuaExpression): boolean {
  if (a.type !== b.type) return false;

  switch (a.type) {
    case "Nil":
      return true;
    case "Boolean":
      return a.value === (b as typeof a).value;
    case "Number":
      return (
        a.value === (b as typeof a).value &&
        a.numericType === (b as typeof a).numericType
      );
    case "String":
      return a.value === (b as typeof a).value;
    case "Variable":
      return a.name === (b as typeof a).name;
    case "PropertyAccess":
      return (
        a.property === (b as typeof a).property &&
        exprStructurallyEquals(a.object, (b as typeof a).object)
      );
    case "TableAccess":
      return (
        exprStructurallyEquals(a.object, (b as typeof a).object) &&
        exprStructurallyEquals(a.key, (b as typeof a).key)
      );
    case "Unary":
      return (
        a.operator === (b as typeof a).operator &&
        exprStructurallyEquals(a.argument, (b as typeof a).argument)
      );
    case "Binary":
      return (
        a.operator === (b as typeof a).operator &&
        exprStructurallyEquals(a.left, (b as typeof a).left) &&
        exprStructurallyEquals(a.right, (b as typeof a).right)
      );
    case "Parenthesized":
      return exprStructurallyEquals(a.expression, (b as typeof a).expression);
    case "FunctionCall": {
      const bb = b as typeof a;
      return (
        exprStructurallyEquals(a.prefix, bb.prefix) &&
        a.name === bb.name &&
        a.args.length === bb.args.length &&
        a.args.every((arg, i) => exprStructurallyEquals(arg, bb.args[i])) &&
        (a.orderBy?.length ?? 0) === (bb.orderBy?.length ?? 0) &&
        (a.orderBy ?? []).every((ob, i) => {
          const other = bb.orderBy![i];
          return (
            ob.direction === other.direction &&
            ob.nulls === other.nulls &&
            exprStructurallyEquals(ob.expression, other.expression)
          );
        })
      );
    }
    case "FilteredCall":
      return (
        exprStructurallyEquals(a.call, (b as typeof a).call) &&
        exprStructurallyEquals(a.filter, (b as typeof a).filter)
      );
    case "AggregateCall": {
      const bb = b as typeof a;
      return (
        exprStructurallyEquals(a.call, bb.call) &&
        a.orderBy.length === bb.orderBy.length &&
        a.orderBy.every((ob, i) => {
          const other = bb.orderBy[i];
          return (
            ob.direction === other.direction &&
            ob.nulls === other.nulls &&
            exprStructurallyEquals(ob.expression, other.expression)
          );
        })
      );
    }
    case "TableConstructor": {
      const bb = b as typeof a;
      return (
        a.fields.length === bb.fields.length &&
        a.fields.every((field, i) => {
          const other = bb.fields[i];
          if (field.type !== other.type) return false;
          switch (field.type) {
            case "PropField":
              return (
                other.type === "PropField" &&
                field.key === other.key &&
                exprStructurallyEquals(field.value, other.value)
              );
            case "ExpressionField":
              return (
                other.type === "ExpressionField" &&
                exprStructurallyEquals(field.value, other.value)
              );
            case "DynamicField":
              return (
                other.type === "DynamicField" &&
                exprStructurallyEquals(field.key, other.key) &&
                exprStructurallyEquals(field.value, other.value)
              );
          }
        })
      );
    }
    case "FunctionDefinition":
      return a === b;
    default:
      return false;
  }
}

function collectAggregateDescriptions(
  expr: LuaExpression | undefined,
): AggregateDescription[] {
  if (!expr) return [];
  const result: AggregateDescription[] = [];
  walkAggregates(expr, result);
  return result;
}

function walkAggregates(
  expr: LuaExpression,
  out: AggregateDescription[],
): void {
  switch (expr.type) {
    case "FilteredCall": {
      const fc = expr.call;
      if (fc.prefix.type === "Variable") {
        const args = fc.args.map(exprToString).join(", ");
        out.push({
          name: fc.prefix.name,
          args,
          filter: exprToString(expr.filter),
        });
        return;
      }
      walkAggregates(fc, out);
      walkAggregates(expr.filter, out);
      return;
    }
    case "AggregateCall": {
      const fc = expr.call;
      if (fc.prefix.type === "Variable") {
        const args = fc.args.map(exprToString).join(", ");
        const ob = expr.orderBy
          .map(
            (o) =>
              exprToString(o.expression) +
              (o.direction === "desc" ? " desc" : ""),
          )
          .join(", ");
        out.push({
          name: fc.prefix.name,
          args,
          orderBy: ob,
        });
        return;
      }
      walkAggregates(fc, out);
      return;
    }
    case "FunctionCall": {
      if (
        expr.prefix.type === "Variable" &&
        KNOWN_AGGREGATE_NAMES.has(expr.prefix.name)
      ) {
        const args = expr.args.map(exprToString).join(", ");
        const desc: AggregateDescription = { name: expr.prefix.name, args };
        if (expr.orderBy && expr.orderBy.length > 0) {
          desc.orderBy = expr.orderBy
            .map(
              (o) =>
                exprToString(o.expression) +
                (o.direction === "desc" ? " desc" : ""),
            )
            .join(", ");
        }
        out.push(desc);
        return;
      }
      walkAggregates(expr.prefix, out);
      for (const arg of expr.args) {
        walkAggregates(arg, out);
      }
      return;
    }
    case "Binary":
      walkAggregates(expr.left, out);
      walkAggregates(expr.right, out);
      return;
    case "Unary":
      walkAggregates(expr.argument, out);
      return;
    case "Parenthesized":
      walkAggregates(expr.expression, out);
      return;
    case "TableConstructor":
      for (const field of expr.fields) {
        switch (field.type) {
          case "DynamicField":
            walkAggregates(field.key, out);
            walkAggregates(field.value, out);
            break;
          case "PropField":
          case "ExpressionField":
            walkAggregates(field.value, out);
            break;
        }
      }
      return;
    default:
      return;
  }
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

// 22. Explain analyze execution

export async function executeAndInstrument(
  tree: JoinNode,
  plan: ExplainNode,
  env: LuaEnv,
  sf: LuaStackFrame,
  opts: ExplainOptions,
  config?: JoinPlannerConfig,
  overrides?: MaterializedSourceOverrides,
  originMs?: number,
  pushedFilters?: SingleSourceFilter[],
): Promise<LuaTable[]> {
  const t0 = originMs ?? (opts.analyze && opts.timing ? performance.now() : 0);

  if (tree.kind === "leaf") {
    let items = await materializeSource(tree.source, env, sf, overrides);
    const unfilteredRowCount =
      tree.source.stats?.unfilteredRowCount ?? tree.source.stats?.rowCount;

    let jsRemovedCount = 0;
    if (pushedFilters && pushedFilters.length > 0) {
      const { result, removedCount } = await applyPushedFiltersWithStats(
        items,
        tree.source.name,
        pushedFilters,
        env,
        sf,
      );
      items = result;
      jsRemovedCount = removedCount;
    }

    const rows = items.map((item) => rowToTable(tree.source.name, item));
    plan.actualRows = rows.length;
    plan.actualLoops = 1;

    const sourceLevelRemoved =
      unfilteredRowCount !== undefined && unfilteredRowCount > items.length
        ? unfilteredRowCount - items.length
        : 0;
    const totalRemoved = sourceLevelRemoved + jsRemovedCount;
    if (totalRemoved > 0) {
      plan.rowsRemovedByFilter = totalRemoved;
    }

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
    t0,
    pushedFilters,
  );
  if (tree.right.kind !== "leaf") {
    throw new Error(
      "Join planner: right child must be a leaf node (left-deep trees only)",
    );
  }
  const rightSource = tree.right.source;
  const rightUnfilteredRowCount =
    rightSource.stats?.unfilteredRowCount ?? rightSource.stats?.rowCount;
  const rightT0 = opts.analyze && opts.timing ? performance.now() : 0;
  let rightItems = await materializeSource(rightSource, env, sf, overrides);

  let rightJsRemoved = 0;
  if (pushedFilters && pushedFilters.length > 0) {
    const { result, removedCount } = await applyPushedFiltersWithStats(
      rightItems,
      rightSource.name,
      pushedFilters,
      env,
      sf,
    );
    rightItems = result;
    rightJsRemoved = removedCount;
  }

  plan.children[1].actualRows = rightItems.length;
  plan.children[1].actualLoops = 1;

  const rightSourceLevelRemoved =
    rightUnfilteredRowCount !== undefined &&
    rightUnfilteredRowCount > rightItems.length
      ? rightUnfilteredRowCount - rightItems.length
      : 0;
  const rightTotalRemoved = rightSourceLevelRemoved + rightJsRemoved;
  if (rightTotalRemoved > 0) {
    plan.children[1].rowsRemovedByFilter = rightTotalRemoved;
  }

  if (opts.analyze && opts.timing) {
    plan.children[1].actualStartupTimeMs =
      Math.round((rightT0 - t0) * 1000) / 1000;
    plan.children[1].actualTimeMs =
      Math.round((performance.now() - t0) * 1000) / 1000;
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

// 23. Explain formatting

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
        ? "Hash Condition"
        : node.method === "merge"
          ? "Merge Condition"
          : "Join Filter";
    const ep = node.equiPred;
    lines.push(
      `${detailPad}${condLabel}: (${ep.leftSource}.${ep.leftColumn} == ${ep.rightSource}.${ep.rightColumn})`,
    );
  }

  if (node.joinResidualExprs && node.joinResidualExprs.length > 0) {
    const residualLabel =
      node.joinFilterType === "join-residual"
        ? "Residual Join Filter"
        : "Join Filter";
    for (const expr of node.joinResidualExprs) {
      lines.push(`${detailPad}${residualLabel}: ${expr}`);
    }
  }

  if (node.sortKeys && node.sortKeys.length > 0) {
    const keyLabel =
      node.nodeType === "GroupAggregate" ? "Group Key" : "Sort Key";
    lines.push(`${detailPad}${keyLabel}: ${node.sortKeys.join(", ")}`);
  }

  if (node.outputColumns && node.outputColumns.length > 0) {
    lines.push(`${detailPad}Output: ${node.outputColumns.join(", ")}`);
  }

  if (node.implicitGroup) {
    lines.push(`${detailPad}Grouping: whole-table aggregate`);
  }

  if (opts.verbose && node.aggregates && node.aggregates.length > 0) {
    for (const agg of node.aggregates) {
      let desc = `${agg.name}(${agg.args})`;
      if (agg.filter) desc += ` filter(${agg.filter})`;
      if (agg.orderBy) desc += ` order by ${agg.orderBy}`;
      lines.push(`${detailPad}Aggregate: ${desc}`);
    }
  }

  if (node.limitCount !== undefined) {
    lines.push(`${detailPad}Count: ${node.limitCount}`);
  }
  if (node.offsetCount !== undefined) {
    lines.push(`${detailPad}Offset: ${node.offsetCount}`);
  }

  if (node.filterExpr) {
    let filterLabel: string;
    if (node.nodeType === "Scan" || node.nodeType === "FunctionScan") {
      filterLabel = node.pushedDownFilter ? "Pushdown Filter" : "Filter";
    } else if (node.filterType === "having") {
      filterLabel = "Having Condition";
    } else {
      filterLabel = "Filter";
    }
    lines.push(`${detailPad}${filterLabel}: ${node.filterExpr}`);
  }
  if (
    opts.analyze &&
    node.rowsRemovedByFilter !== undefined &&
    node.rowsRemovedByFilter > 0
  ) {
    const removedByFilterLabel =
      (node.nodeType === "Scan" || node.nodeType === "FunctionScan") &&
      node.pushedDownFilter
        ? "Rows Removed by Pushdown Filter"
        : "Rows Removed by Filter";

    lines.push(
      `${detailPad}${removedByFilterLabel}: ${node.rowsRemovedByFilter}`,
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

  if (
    opts.analyze &&
    node.rowsRemovedByUnique !== undefined &&
    node.rowsRemovedByUnique > 0
  ) {
    lines.push(
      `${detailPad}Rows Removed by Unique: ${node.rowsRemovedByUnique}`,
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
      lines.push(`${detailPad}Pushdown Capability: ${node.predicatePushdown}`);
    }

    if (node.selectivity !== undefined) {
      const sel = node.selectivity;
      const formatted =
        sel >= 0.01
          ? sel.toFixed(4).replace(/0+$/, "").replace(/\.$/, ".0")
          : sel.toPrecision(3);
      lines.push(`${detailPad}Selectivity: ${formatted}`);
    }

    if (node.statsSource) {
      lines.push(`${detailPad}Stats: ${node.statsSource}`);
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
      lines.push(
        `${detailPad}MCV: suppressed${suffix}`,
      );
    } else if (node.mcvFallback === "no-mcv") {
      lines.push(`${detailPad}MCV: not available`);
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
      return node.filterType === "having" ? "Filter (Having)" : "Filter";
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
      return node.implicitGroup
        ? "Implicit Group Aggregate"
        : "Group Aggregate";
    case "Unique":
      return "Unique";
    case "Project": {
      const isImplicit = node.implicitGroup === false;
      return isImplicit ? "Implicit Project" : "Project";
    }
    default:
      return node.nodeType;
  }
}

// 24. Join residual helpers

function assignResidualPredicatesToLowestCoveringJoin(
  tree: JoinNode,
  expr: LuaExpression,
  equiPreds?: EquiPredicate[],
): void {
  const allSourceNames = collectSourceNames(tree);
  const conjuncts = flattenAnd(expr);

  for (const conjunct of conjuncts) {
    if (exprMatchesEquiPred(conjunct, equiPreds ?? [])) {
      continue;
    }

    const refs = collectReferencedSources(conjunct, allSourceNames);

    // Residual join predicates must reference at least two sources.
    if (refs.size < 2) {
      continue;
    }

    assignResidualPredicateToLowestCoveringJoin(tree, conjunct, refs);
  }
}

function assignResidualPredicateToLowestCoveringJoin(
  node: JoinNode,
  predicate: LuaExpression,
  refs: Set<string>,
): boolean {
  if (node.kind === "leaf") {
    return false;
  }

  const leftSources = collectSourceNames(node.left);
  const rightSources = collectSourceNames(node.right);

  const leftCoversAll = isSubsetOf(refs, leftSources);
  const rightCoversAll = isSubsetOf(refs, rightSources);

  // Prefer the lowest covering join node.
  if (leftCoversAll) {
    return assignResidualPredicateToLowestCoveringJoin(
      node.left,
      predicate,
      refs,
    );
  }
  if (rightCoversAll) {
    return assignResidualPredicateToLowestCoveringJoin(
      node.right,
      predicate,
      refs,
    );
  }

  // This is the lowest join node whose source-set covers the predicate.
  if (!node.joinResiduals) {
    node.joinResiduals = [];
  }

  if (!node.joinResiduals.some((r) => exprStructurallyEquals(r, predicate))) {
    node.joinResiduals.push(predicate);
  }

  return true;
}

function isSubsetOf(
  values: Set<string>,
  candidateSuperset: Set<string>,
): boolean {
  for (const value of values) {
    if (!candidateSuperset.has(value)) {
      return false;
    }
  }
  return true;
}

// 25. Config bridge

export function joinPlannerConfigFromConfig(config: Config): JoinPlannerConfig {
  return {
    watchdogLimit:
      config.get("queryPlanner.watchdogLimit", undefined) ?? undefined,
    yieldChunk: config.get("queryPlanner.yieldChunk", undefined) ?? undefined,
    smallTableThreshold:
      config.get("queryPlanner.smallTableThreshold", undefined) ?? undefined,
    mergeJoinThreshold:
      config.get("queryPlanner.mergeJoinThreshold", undefined) ?? undefined,
    widthWeight: config.get("queryPlanner.widthWeight", undefined) ?? undefined,
    candidateWidthWeight:
      config.get("queryPlanner.candidateWidthWeight", undefined) ?? undefined,
    semiAntiLoopDiscount:
      config.get("queryPlanner.semiAntiLoopDiscount", undefined) ?? undefined,
    partialStatsConfidence:
      config.get("queryPlanner.partialStatsConfidence", undefined) ?? undefined,
    approximateStatsConfidence:
      config.get("queryPlanner.approximateStatsConfidence", undefined) ??
      undefined,
    bitmapScanPenalty:
      config.get("queryPlanner.bitmapScanPenalty", undefined) ?? undefined,
    indexScanNoPushdownPenalty:
      config.get("queryPlanner.indexScanNoPushdownPenalty", undefined) ??
      undefined,
    kvScanPenalty:
      config.get("queryPlanner.kvScanPenalty", undefined) ?? undefined,
    defaultFilterSelectivity:
      config.get("queryPlanner.defaultFilterSelectivity", undefined) ??
      undefined,
    defaultDistinctSurvivalRatio:
      config.get("queryPlanner.defaultDistinctSurvivalRatio", undefined) ??
      undefined,
    defaultRangeSelectivity:
      config.get("queryPlanner.defaultRangeSelectivity", undefined) ??
      undefined,
    inferredNdvDivisor:
      config.get("queryPlanner.inferredNdvDivisor", undefined) ?? undefined,
  };
}
