import type { QueryCollationConfig } from "../../plug-api/types/config.ts";
import type { KvKey } from "../../plug-api/types/datastore.ts";
import { Config } from "../config.ts";
import type { DataStore } from "../data/datastore.ts";
import type { KvPrimitives } from "../data/kv_primitives.ts";
import {
  executeAggregate,
  getAggregateSpec,
  type AggregateResult,
} from "./aggregates.ts";
import type {
  LuaAggregateCallExpression,
  LuaBinaryExpression,
  LuaDynamicField,
  LuaExpression,
  LuaExpressionField,
  LuaFilteredCallExpression,
  LuaFunctionBody,
  LuaFunctionCallExpression,
  LuaOrderBySelectKeyExpression,
  LuaParenthesizedExpression,
  LuaPropField,
  LuaUnaryExpression,
} from "./ast.ts";
import { evalExpression, luaOp } from "./eval.ts";
import { HalfXorSketch, type SketchConfig } from "./half_xor.ts";
import { MCVList, type MCVConfig } from "./mcv.ts";
import { isSqlNull, LIQ_NULL } from "./liq_null.ts";
import {
  jsToLuaValue,
  LuaEnv,
  LuaFunction,
  LuaRuntimeError,
  LuaStackFrame,
  LuaTable,
  type LuaValue,
  luaCall,
  luaGet,
  luaKeys,
  luaTruthy,
  singleResult,
} from "./runtime.ts";
import { asyncMergeSort } from "./util.ts";

// Constants
const KV_ENGINE_BASE_COST_WEIGHT = 1.4;
const MATERIALIZED_ENGINE_BASE_COST_WEIGHT = 1.0;

/**
 * Provenance / confidence class for collection statistics.
 *
 * The planner must not treat all stats equally:
 * - persisted-complete: exact stats from a fully built persisted index
 * - persisted-partial: exact-on-visible-subset stats from an incomplete index
 * - computed-exact-small: exact stats computed from a small materialized source
 * - computed-sketch-large: approximate NDV computed from a large materialized source
 * - recomputed-filtered-exact: exact stats recomputed after pushdown filtering
 * - source-provided-exact: exact stats supplied by a trusted custom source
 * - source-provided-unknown: source supplied stats but without exactness guarantee
 * - computed-empty: exact empty-source stats
 * - unknown-default: conservative fallback with little or no information
 */
export type StatsSource =
  | "persisted-complete"
  | "persisted-partial"
  | "computed-exact-small"
  | "computed-sketch-large"
  | "recomputed-filtered-exact"
  | "recomputed-materialized-exact"
  | "source-provided-exact"
  | "source-provided-unknown"
  | "computed-empty"
  | "unknown-default";

export type PlannerCapability =
  | "pred-eq"
  | "pred-neq"
  | "pred-lt"
  | "pred-lte"
  | "pred-gt"
  | "pred-gte"
  | "pred-in"
  | "bool-and"
  | "bool-or"
  | "bool-not"
  | "expr-literal"
  | "expr-column-qualified"
  | "expr-column-unqualified"
  | "stage-where"
  | "stage-order-by"
  | "stage-limit"
  | "stage-offset"
  | "scan-kv"
  | "scan-index"
  | "scan-bitmap"
  | "scan-materialized"
  | "stats-row-count"
  | "stats-ndv"
  | "stats-mcv";

export type QueryEngineKind =
  | "scan"
  | "kv"
  | "bitmap"
  | "index"
  | "custom";

export type QueryEngineCapability = {
  id: string;
  name: string;
  kind: QueryEngineKind;
  capabilities: PlannerCapability[];
  baseCostWeight?: number;
  capabilityCosts?: Partial<Record<PlannerCapability, number>>;
  priority?: number;
  metadata?: Record<string, string | number | boolean>;
};

export type CollectionExecutionCapabilities = {
  engines: QueryEngineCapability[];
};

// Collection statistics for the cost-based planner
export type CollectionStats = {
  rowCount: number;
  // Row count set when `statsSource` is `recomputed-filtered-exact`
  unfilteredRowCount?: number;
  ndv: Map<string, number>;
  avgColumnCount?: number;
  statsSource?: StatsSource;
  mcv?: Map<string, MCVList>;
  executionCapabilities?: CollectionExecutionCapabilities;
};

export type QueryStageName =
  | "where"
  | "groupBy"
  | "having"
  | "orderBy"
  | "select"
  | "distinct"
  | "limit";

export type QueryStageStat = {
  stage: QueryStageName;
  inputRows: number;
  outputRows: number;
  startTimeMs: number;
  endTimeMs: number;
  elapsedMs: number;
  rowsRemoved?: number;
  memoryRows?: number;
  inlineFilteredRows?: number;
};

export type QueryInstrumentation = {
  onStage?: (stat: QueryStageStat) => void;
};

export type AggregateRuntimeStats = {
  rowsRemovedByAggregateFilter: number;
};

export type AggregateRuntimeInstrumentation = {
  stats: AggregateRuntimeStats;
};

export interface LuaQueryCollection {
  query(
    query: LuaCollectionQuery,
    env: LuaEnv,
    sf: LuaStackFrame,
    config?: Config,
    instrumentation?: QueryInstrumentation,
    aggregateInstrumentation?: AggregateRuntimeInstrumentation,
  ): Promise<any[]>;
}

export interface LuaQueryCollectionWithStats extends LuaQueryCollection {
  getStats?():
    | CollectionStats
    | Promise<CollectionStats | undefined>
    | undefined;

  isTagIndexTrusted?(): Promise<boolean> | boolean;
}

export class StatsTracker {
  rowCount = 0;
  private totalColumnCount = 0;
  private sketchMap = new Map<string, HalfXorSketch>();
  private mcvMap = new Map<string, MCVList>();
  private sketchConfig: SketchConfig;
  private mcvConfig: MCVConfig;

  constructor(sketchConfig?: SketchConfig, mcvConfig?: MCVConfig) {
    this.sketchConfig = sketchConfig ?? {};
    this.mcvConfig = mcvConfig ?? {};
  }

  index(item: Record<string, any>, contextTag: string = "Unknown"): void {
    this.rowCount++;
    const keys = Object.keys(item);
    this.totalColumnCount += keys.length;
    for (const key of keys) {
      const val = item[key];
      if (val === null || val === undefined) continue;
      const strVal = String(val);

      let sketch = this.sketchMap.get(key);
      if (!sketch) {
        sketch = new HalfXorSketch(this.sketchConfig);
        this.sketchMap.set(key, sketch);
      }
      sketch.add(strVal, `${contextTag}.${key}`);

      let mcv = this.mcvMap.get(key);
      if (!mcv) {
        mcv = new MCVList(this.mcvConfig);
        this.mcvMap.set(key, mcv);
      }
      mcv.insert(strVal);
    }
  }

  unindex(item: Record<string, any>): void {
    if (this.rowCount > 0) this.rowCount--;
    const keys = Object.keys(item);
    this.totalColumnCount = Math.max(0, this.totalColumnCount - keys.length);
    for (const key of keys) {
      const val = item[key];
      if (val === null || val === undefined) continue;
      const strVal = String(val);
      const sketch = this.sketchMap.get(key);
      if (sketch) sketch.remove(strVal);
      const mcv = this.mcvMap.get(key);
      if (mcv) mcv.delete(strVal);
    }
  }

  getStats(): CollectionStats {
    const ndv = new Map<string, number>();
    for (const [col, sketch] of this.sketchMap) {
      ndv.set(col, sketch.estimate());
    }
    const mcv = new Map<string, MCVList>();
    for (const [col, m] of this.mcvMap) {
      mcv.set(col, m);
    }
    const avgColumnCount =
      this.rowCount > 0 ? Math.round(this.totalColumnCount / this.rowCount) : 0;
    return {
      rowCount: this.rowCount,
      ndv,
      avgColumnCount,
      mcv,
      statsSource:
        this.rowCount === 0 ? "computed-empty" : "computed-sketch-large",
    };
  }

  getSerializedSketches(): Record<string, string> {
    const sketches: Record<string, string> = {};
    for (const [col, sketch] of this.sketchMap) {
      sketches[col] = sketch.serialize();
    }
    return sketches;
  }

  getSerializedMCVs(): Record<string, string> {
    const mcvs: Record<string, string> = {};
    for (const [col, mcv] of this.mcvMap) {
      if (mcv.trackedSize() > 0) {
        mcvs[col] = mcv.serialize();
      }
    }
    return mcvs;
  }

  clear(): void {
    this.rowCount = 0;
    this.totalColumnCount = 0;
    this.sketchMap.clear();
    this.mcvMap.clear();
  }
}

// Implicit single group map key (aggregates without `group by`)
const IMPLICIT_GROUP_KEY: unique symbol = Symbol("implicit-group");

function nowMs(): number {
  return performance.now();
}

function emitStageStat(
  instrumentation: QueryInstrumentation | undefined,
  stage: QueryStageName,
  inputRows: number,
  outputRows: number,
  startTimeMs: number,
  extra: {
    rowsRemoved?: number;
    memoryRows?: number;
    inlineFilteredRows?: number;
  } = {},
): void {
  if (!instrumentation?.onStage) return;
  const endTimeMs = nowMs();
  instrumentation.onStage({
    stage,
    inputRows,
    outputRows,
    startTimeMs,
    endTimeMs,
    elapsedMs: Math.round((endTimeMs - startTimeMs) * 1000) / 1000,
    rowsRemoved: extra.rowsRemoved,
    memoryRows: extra.memoryRows,
    inlineFilteredRows: extra.inlineFilteredRows,
  });
}

// Build environment for post-`group by` clauses. Injects `key` and `group`
// as top-level variables. Unpacks first group item fields and group-by key
// fields as locals so that bare field access works after grouping.
function buildGroupItemEnv(
  objectVariable: string | undefined,
  groupByNames: string[] | undefined,
  item: any,
  parentGlobals: LuaEnv,
  sf: LuaStackFrame,
): LuaEnv {
  const itemEnv = new LuaEnv(parentGlobals);
  itemEnv.setLocal("_", item);
  if (item instanceof LuaTable) {
    const keyVal = item.rawGet("key");
    const groupVal = item.rawGet("group");
    const firstItem =
      groupVal instanceof LuaTable ? groupVal.rawGet(1) : undefined;

    if (firstItem) {
      for (const k of luaKeys(firstItem)) {
        if (typeof k !== "string") continue;
        itemEnv.setLocal(k, luaGet(firstItem, k, sf.astCtx ?? null, sf));
      }
    }

    if (objectVariable) {
      itemEnv.setLocal(objectVariable, firstItem ?? item);
    }
    if (keyVal !== undefined) {
      itemEnv.setLocal("key", keyVal);
    }
    if (groupVal !== undefined) {
      itemEnv.setLocal("group", groupVal);
    }

    // Unpack named fields from multi-key LuaTable keys
    if (keyVal instanceof LuaTable) {
      for (const k of luaKeys(keyVal)) {
        if (typeof k !== "string") continue;
        itemEnv.setLocal(k, luaGet(keyVal, k, sf.astCtx ?? null, sf));
      }
    }

    // Bind all `group by` aliases/names to their key values.  For
    // single key bind the name to the scalar `keyVal`.  For multi-key
    // bind each name to the field from the key table.
    if (groupByNames && groupByNames.length > 0) {
      if (!(keyVal instanceof LuaTable)) {
        // Bind all names to scalar
        for (const gbn of groupByNames) {
          itemEnv.setLocal(gbn, keyVal);
        }
      } else {
        // Ensure every alias is bound even if `luaKeys` missed it
        for (const gbn of groupByNames) {
          const v = keyVal.rawGet(gbn);
          if (v !== undefined) {
            itemEnv.setLocal(gbn, v);
          }
        }
      }
    }
  }
  return itemEnv;
}

/**
 * Build an environment for evaluating per-item expressions in queries.
 *
 * When `objectVariable` is NOT set: item fields are unpacked as locals
 * and shadow any globals. The item is also bound to `_`.
 *
 * When `objectVariable` IS set: only the object variable is bound.
 * Item fields are NOT unpacked - the user opted into qualified access
 * (e.g. `p.name`) and bare names must resolve from the parent env.
 */
function buildItemEnvLocal(
  objectVariable: string | undefined,
  item: any,
  env: LuaEnv,
  sf: LuaStackFrame,
): LuaEnv {
  const itemEnv = new LuaEnv(env);
  if (objectVariable) {
    itemEnv.setLocal(objectVariable, item);
  } else {
    // Unpack item fields as locals so unqualified access works
    itemEnv.setLocal("_", item);
    if (item instanceof LuaTable) {
      for (const key of luaKeys(item)) {
        itemEnv.setLocal(key, luaGet(item, key, sf.astCtx ?? null, sf));
      }
    } else if (typeof item === "object" && item !== null) {
      for (const key of luaKeys(item)) {
        itemEnv.setLocal(key, luaGet(item, key, sf.astCtx ?? null, sf));
      }
    }
  }
  return itemEnv;
}

export { buildItemEnvLocal as buildItemEnv };

export type LuaOrderBy = {
  expr: LuaExpression;
  desc: boolean;
  nulls?: "first" | "last";
  using?: string | LuaFunctionBody;
};

export type LuaGroupByEntry = {
  expr: LuaExpression;
  alias?: string;
};

/**
 * Represents a query for a collection
 */
export type LuaCollectionQuery = {
  objectVariable?: string;
  // The filter expression evaluated with Lua
  where?: LuaExpression;
  // The order by expression evaluated with Lua
  orderBy?: LuaOrderBy[];
  // The select expression evaluated with Lua
  select?: LuaExpression;
  // The limit of the query
  limit?: number;
  // The offset of the query
  offset?: number;
  // Whether to return only distinct values
  distinct?: boolean;
  // The group by entries evaluated with Lua
  groupBy?: LuaGroupByEntry[];
  // The having expression evaluated with Lua
  having?: LuaExpression;
};

/**
 * Compute CollectionStats from a plain JavaScript array.
 * Used by the join planner for inline array/table sources.
 */
export function computeStatsFromArray(
  items: any[],
  sketchConfig?: SketchConfig,
): CollectionStats {
  const EXACT_THRESHOLD = 10_000;
  const ndv = new Map<string, number>();
  let totalColumnCount = 0;

  const materializedScanEngine: QueryEngineCapability = {
    id: "materialized-array-scan",
    name: "Materialized array scan",
    kind: "scan",
    capabilities: ["scan-materialized"],
    baseCostWeight: MATERIALIZED_ENGINE_BASE_COST_WEIGHT,
  };

  if (items.length === 0) {
    return {
      rowCount: 0,
      ndv,
      avgColumnCount: 0,
      statsSource: "computed-empty",
      executionCapabilities: {
        engines: [materializedScanEngine],
      },
    };
  }

  if (items.length <= EXACT_THRESHOLD) {
    const seen = new Map<string, Set<string>>();
    for (const item of items) {
      if (typeof item === "object" && item !== null) {
        const keys =
          item instanceof LuaTable ? luaKeys(item) : Object.keys(item);
        totalColumnCount += keys.length;
        for (const key of keys) {
          if (typeof key !== "string") continue;
          const val = item instanceof LuaTable ? item.rawGet(key) : item[key];
          if (val === null || val === undefined) continue;
          let s = seen.get(key);
          if (!s) {
            s = new Set();
            seen.set(key, s);
          }
          s.add(String(val));
        }
      }
    }

    for (const [k, s] of seen) {
      ndv.set(k, s.size);
    }

    const avgColumnCount = Math.round(totalColumnCount / items.length);

    return {
      rowCount: items.length,
      ndv,
      avgColumnCount,
      statsSource: "computed-exact-small",
      executionCapabilities: {
        engines: [
          {
            ...materializedScanEngine,
            capabilities: [
              "scan-materialized",
              "stats-row-count",
              "stats-ndv",
            ],
          },
        ],
      },
    };
  }

  const sketches = new Map<string, HalfXorSketch>();
  for (const item of items) {
    if (typeof item === "object" && item !== null) {
      const keys = item instanceof LuaTable ? luaKeys(item) : Object.keys(item);
      totalColumnCount += keys.length;
      for (const key of keys) {
        if (typeof key !== "string") continue;
        const val = item instanceof LuaTable ? item.rawGet(key) : item[key];
        if (val === null || val === undefined) continue;
        let sketch = sketches.get(key);
        if (!sketch) {
          sketch = new HalfXorSketch(sketchConfig);
          sketches.set(key, sketch);
        }
        sketch.add(String(val));
      }
    }
  }

  for (const [k, sketch] of sketches) {
    ndv.set(k, sketch.estimate());
  }

  const avgColumnCount = Math.round(totalColumnCount / items.length);

  return {
    rowCount: items.length,
    ndv,
    avgColumnCount,
    statsSource: "computed-sketch-large",
    executionCapabilities: {
      engines: [
        {
          ...materializedScanEngine,
          capabilities: [
            "scan-materialized",
            "stats-row-count",
            "stats-ndv",
          ],
        },
      ],
    },
  };
}

/**
 * Implements a query collection for a regular JavaScript array
 */
export class ArrayQueryCollection<T> implements LuaQueryCollection {
  constructor(private readonly array: T[]) {}

  query(
    query: LuaCollectionQuery,
    env: LuaEnv,
    sf: LuaStackFrame,
    config?: Config,
    instrumentation?: QueryInstrumentation,
    aggregateInstrumentation?: AggregateRuntimeInstrumentation,
  ): Promise<any[]> {
    return applyQuery(
      this.array,
      query,
      env,
      sf,
      config,
      instrumentation,
      aggregateInstrumentation,
    );
  }

  getStats(): CollectionStats {
    return computeStatsFromArray(this.array);
  }
}

// Wrap any object, array, or LuaQueryCollection as a queryable collection
export function toCollection(obj: any): LuaQueryCollection {
  if (
    obj instanceof ArrayQueryCollection ||
    obj instanceof DataStoreQueryCollection
  ) {
    return obj;
  }
  if (Array.isArray(obj)) {
    return new ArrayQueryCollection(obj);
  }
  return new ArrayQueryCollection([obj]);
}

function containsAggregate(expr: LuaExpression, config?: Config): boolean {
  switch (expr.type) {
    case "FilteredCall": {
      const fc = (expr as LuaFilteredCallExpression).call;
      if (
        fc.prefix.type === "Variable" &&
        getAggregateSpec(fc.prefix.name, config)
      ) {
        return true;
      }
      return (
        containsAggregate(fc, config) ||
        containsAggregate((expr as LuaFilteredCallExpression).filter, config)
      );
    }
    case "AggregateCall": {
      const ac = expr as LuaAggregateCallExpression;
      const fc = ac.call;
      if (
        fc.prefix.type === "Variable" &&
        getAggregateSpec(fc.prefix.name, config)
      ) {
        return true;
      }
      return containsAggregate(fc, config);
    }
    case "FunctionCall": {
      const fc = expr as LuaFunctionCallExpression;
      if (
        fc.prefix.type === "Variable" &&
        getAggregateSpec(fc.prefix.name, config)
      ) {
        return true;
      }
      return fc.args.some((a) => containsAggregate(a, config));
    }
    case "Binary": {
      const bin = expr as LuaBinaryExpression;
      return (
        containsAggregate(bin.left, config) ||
        containsAggregate(bin.right, config)
      );
    }
    case "Unary": {
      const un = expr as LuaUnaryExpression;
      return containsAggregate(un.argument, config);
    }
    case "Parenthesized": {
      const p = expr as LuaParenthesizedExpression;
      return containsAggregate(p.expression, config);
    }
    case "TableConstructor":
      return expr.fields.some((f) => {
        switch (f.type) {
          case "PropField":
            return containsAggregate((f as LuaPropField).value, config);
          case "DynamicField": {
            const df = f as LuaDynamicField;
            return (
              containsAggregate(df.key, config) ||
              containsAggregate(df.value, config)
            );
          }
          case "ExpressionField":
            return containsAggregate((f as LuaExpressionField).value, config);
          default:
            return false;
        }
      });
    case "QueryIn":
      return (
        containsAggregate(expr.left, config) ||
        containsAggregate(expr.right, config)
      );
    default:
      return false;
  }
}

type AggregateEvalEntry = {
  key: string;
  value: LuaValue;
  rowsFiltered: number;
  counted: boolean;
};

type AggregateEvalContext = {
  entries: AggregateEvalEntry[];
  cursor: number;
  local: Map<string, AggregateEvalEntry>;
};

type GroupedValue = {
  item: LuaTable;
  aggregateCtx: AggregateEvalContext;
};

function isGroupedValue(item: any): item is GroupedValue {
  return (
    !!item &&
    typeof item === "object" &&
    "item" in item &&
    item.item instanceof LuaTable &&
    "aggregateCtx" in item
  );
}

function unwrapGroupedItem(item: any): LuaTable {
  return isGroupedValue(item) ? item.item : item;
}

function getGroupedAggregateContext(
  item: any,
): AggregateEvalContext | undefined {
  return isGroupedValue(item) ? item.aggregateCtx : undefined;
}

function resetAggregateEvalContext(
  ctx: AggregateEvalContext | undefined,
): void {
  if (ctx) {
    ctx.cursor = 0;
    ctx.local.clear();
  }
}

function reuseAggregateEvalEntry(
  ctx: AggregateEvalContext | undefined,
  key: string,
): AggregateEvalEntry | undefined {
  if (!ctx) return undefined;
  const entry = ctx.entries[ctx.cursor];
  if (!entry || entry.key !== key) return undefined;
  ctx.cursor++;
  return entry;
}

function reuseAggregateEvalEntryLocal(
  ctx: AggregateEvalContext | undefined,
  key: string,
): AggregateEvalEntry | undefined {
  return ctx?.local.get(key);
}

function storeAggregateEvalEntry(
  ctx: AggregateEvalContext | undefined,
  entry: AggregateEvalEntry,
): void {
  if (!ctx) return;
  if (ctx.cursor < ctx.entries.length) {
    ctx.entries[ctx.cursor] = entry;
  } else {
    ctx.entries.push(entry);
  }
  ctx.cursor++;
}

function rememberAggregateEvalEntryLocal(
  ctx: AggregateEvalContext | undefined,
  entry: AggregateEvalEntry,
): void {
  if (!ctx) return;
  ctx.local.set(entry.key, entry);
}

function aggregateCallKey(expr: LuaExpression): string | null {
  switch (expr.type) {
    case "FilteredCall": {
      const fc = expr.call;
      if (fc.prefix.type !== "Variable") return null;
      return [
        "filtered",
        fc.prefix.name,
        fc.args.map(exprToString).join(","),
        exprToString(expr.filter),
        (fc.orderBy ?? [])
          .map((o) => {
            let s = exprToString(o.expression);
            if (o.direction === "desc") s += " desc";
            if (o.nulls) s += ` nulls ${o.nulls}`;
            return s;
          })
          .join("|"),
      ].join("::");
    }
    case "AggregateCall": {
      const fc = expr.call;
      if (fc.prefix.type !== "Variable") return null;
      return [
        "aggregate",
        fc.prefix.name,
        fc.args.map(exprToString).join(","),
        expr.orderBy
          .map((o) => {
            let s = exprToString(o.expression);
            if (o.direction === "desc") s += " desc";
            if (o.nulls) s += ` nulls ${o.nulls}`;
            return s;
          })
          .join("|"),
      ].join("::");
    }
    case "FunctionCall": {
      if (expr.prefix.type !== "Variable") return null;
      return [
        "function",
        expr.prefix.name,
        expr.args.map(exprToString).join(","),
        (expr.orderBy ?? [])
          .map((o) => {
            let s = exprToString(o.expression);
            if (o.direction === "desc") s += " desc";
            if (o.nulls) s += ` nulls ${o.nulls}`;
            return s;
          })
          .join("|"),
      ].join("::");
    }
    default:
      return null;
  }
}

function exprToString(expr: LuaExpression): string {
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
      let s = `${prefix}(${args})`;
      if (expr.orderBy && expr.orderBy.length > 0) {
        const orderBy = expr.orderBy
          .map((o) => {
            let part = exprToString(o.expression);
            if (o.direction === "desc") part += " desc";
            if (o.nulls) part += ` nulls ${o.nulls}`;
            return part;
          })
          .join(", ");
        s += ` order by ${orderBy}`;
      }
      return s;
    }
    case "FilteredCall":
      return `${exprToString(expr.call)} filter((${exprToString(expr.filter)}))`;
    case "AggregateCall": {
      let s = exprToString(expr.call);
      if (expr.orderBy.length > 0) {
        const orderBy = expr.orderBy
          .map((o) => {
            let part = exprToString(o.expression);
            if (o.direction === "desc") part += " desc";
            if (o.nulls) part += ` nulls ${o.nulls}`;
            return part;
          })
          .join(", ");
        s += ` order by ${orderBy}`;
      }
      return s;
    }
    case "TableAccess":
      return `${exprToString(expr.object)}[${exprToString(expr.key)}]`;
    case "Parenthesized":
      return exprToString(expr.expression);
    case "FunctionDefinition":
      return "<anonymous>";
    case "TableConstructor": {
      const parts: string[] = [];
      for (const field of expr.fields) {
        switch (field.type) {
          case "PropField":
            parts.push(`${field.key} = ${exprToString(field.value)}`);
            break;
          case "DynamicField":
            parts.push(`[${exprToString(field.key)}] = ${exprToString(field.value)}`);
            break;
          case "ExpressionField":
            parts.push(exprToString(field.value));
            break;
        }
      }
      return `{ ${parts.join(", ")} }`;
    }
    default:
      return "?";
  }
}

// Wrap a value for select result tables so that the column key survives
// in the `LuaTable`
function selectVal(v: LuaValue): LuaValue {
  return v === null || v === undefined ? LIQ_NULL : v;
}

function unwrapAggregateValue(v: AggregateResult): LuaValue {
  return v.value;
}

function mergeAggregateRuntimeStatsOnce(
  target: AggregateRuntimeInstrumentation | undefined,
  entry: AggregateEvalEntry,
): void {
  if (!target || entry.counted) return;
  target.stats.rowsRemovedByAggregateFilter += entry.rowsFiltered ?? 0;
  entry.counted = true;
}

/**
 * Evaluate an expression in aggregate-aware mode.
 *
 * When a FunctionCall matches a registered aggregate name, the aggregate
 * protocol is executed instead of normal call semantics.  All other
 * expressions fall through to normal evalExpression.
 */
export async function evalExpressionWithAggregates(
  expr: LuaExpression,
  env: LuaEnv,
  sf: LuaStackFrame,
  groupItems: LuaTable,
  objectVariable: string | undefined,
  outerEnv: LuaEnv,
  config: Config,
  aggregateInstrumentation?: AggregateRuntimeInstrumentation,
  aggregateEvalContext?: AggregateEvalContext,
): Promise<LuaValue> {
  if (!containsAggregate(expr, config)) {
    return evalExpression(expr, env, sf);
  }
  const recurse = (e: LuaExpression) =>
    evalExpressionWithAggregates(
      e,
      env,
      sf,
      groupItems,
      objectVariable,
      outerEnv,
      config,
      aggregateInstrumentation,
      aggregateEvalContext,
    );

  if (expr.type === "FilteredCall") {
    const filtered = expr as LuaFilteredCallExpression;
    const fc = filtered.call;
    if (fc.prefix.type === "Variable") {
      const name = fc.prefix.name;
      const spec = getAggregateSpec(name, config);
      if (spec) {
        const key = aggregateCallKey(expr) ?? "<aggregate>";

        const localCached = reuseAggregateEvalEntryLocal(
          aggregateEvalContext,
          key,
        );
        if (localCached) {
          mergeAggregateRuntimeStatsOnce(aggregateInstrumentation, localCached);
          return localCached.value;
        }

        const cached = reuseAggregateEvalEntry(aggregateEvalContext, key);
        if (cached) {
          rememberAggregateEvalEntryLocal(aggregateEvalContext, cached);
          mergeAggregateRuntimeStatsOnce(aggregateInstrumentation, cached);
          return cached.value;
        }

        const valueExpr = fc.args.length > 0 ? fc.args[0] : null;
        const extraArgExprs = fc.args.length > 1 ? fc.args.slice(1) : [];
        const result = await executeAggregate(
          spec,
          groupItems,
          valueExpr,
          extraArgExprs,
          objectVariable,
          outerEnv,
          sf,
          evalExpression,
          config,
          filtered.filter,
          fc.orderBy,
        );

        const entry: AggregateEvalEntry = {
          key,
          value: unwrapAggregateValue(result),
          rowsFiltered: result.rowsFiltered ?? 0,
          counted: false,
        };
        storeAggregateEvalEntry(aggregateEvalContext, entry);
        rememberAggregateEvalEntryLocal(aggregateEvalContext, entry);
        mergeAggregateRuntimeStatsOnce(aggregateInstrumentation, entry);
        return entry.value;
      }
    }

    return evalExpression(expr, env, sf);
  }

  if (expr.type === "AggregateCall") {
    const agg = expr as LuaAggregateCallExpression;
    const fc = agg.call;
    if (fc.prefix.type === "Variable") {
      const name = fc.prefix.name;
      const spec = getAggregateSpec(name, config);
      if (spec) {
        const key = aggregateCallKey(expr) ?? "<aggregate>";

        const localCached = reuseAggregateEvalEntryLocal(
          aggregateEvalContext,
          key,
        );
        if (localCached) {
          mergeAggregateRuntimeStatsOnce(aggregateInstrumentation, localCached);
          return localCached.value;
        }

        const cached = reuseAggregateEvalEntry(aggregateEvalContext, key);
        if (cached) {
          rememberAggregateEvalEntryLocal(aggregateEvalContext, cached);
          mergeAggregateRuntimeStatsOnce(aggregateInstrumentation, cached);
          return cached.value;
        }

        const valueExpr = fc.args.length > 0 ? fc.args[0] : null;
        const extraArgExprs = fc.args.length > 1 ? fc.args.slice(1) : [];
        const result = await executeAggregate(
          spec,
          groupItems,
          valueExpr,
          extraArgExprs,
          objectVariable,
          outerEnv,
          sf,
          evalExpression,
          config,
          undefined,
          agg.orderBy,
        );

        const entry: AggregateEvalEntry = {
          key,
          value: unwrapAggregateValue(result),
          rowsFiltered: result.rowsFiltered ?? 0,
          counted: false,
        };
        storeAggregateEvalEntry(aggregateEvalContext, entry);
        rememberAggregateEvalEntryLocal(aggregateEvalContext, entry);
        mergeAggregateRuntimeStatsOnce(aggregateInstrumentation, entry);
        return entry.value;
      }
    }

    return evalExpression(expr, env, sf);
  }

  if (expr.type === "FunctionCall") {
    const fc = expr as LuaFunctionCallExpression;
    if (fc.prefix.type === "Variable") {
      const name = fc.prefix.name;
      const spec = getAggregateSpec(name, config);
      if (spec) {
        const key = aggregateCallKey(expr) ?? "<aggregate>";

        const localCached = reuseAggregateEvalEntryLocal(
          aggregateEvalContext,
          key,
        );
        if (localCached) {
          mergeAggregateRuntimeStatsOnce(aggregateInstrumentation, localCached);
          return localCached.value;
        }

        const cached = reuseAggregateEvalEntry(aggregateEvalContext, key);
        if (cached) {
          rememberAggregateEvalEntryLocal(aggregateEvalContext, cached);
          mergeAggregateRuntimeStatsOnce(aggregateInstrumentation, cached);
          return cached.value;
        }

        const valueExpr = fc.args.length > 0 ? fc.args[0] : null;
        const extraArgExprs = fc.args.length > 1 ? fc.args.slice(1) : [];
        const result = await executeAggregate(
          spec,
          groupItems,
          valueExpr,
          extraArgExprs,
          objectVariable,
          outerEnv,
          sf,
          evalExpression,
          config,
          undefined,
          fc.orderBy,
        );

        const entry: AggregateEvalEntry = {
          key,
          value: unwrapAggregateValue(result),
          rowsFiltered: result.rowsFiltered ?? 0,
          counted: false,
        };
        storeAggregateEvalEntry(aggregateEvalContext, entry);
        rememberAggregateEvalEntryLocal(aggregateEvalContext, entry);
        mergeAggregateRuntimeStatsOnce(aggregateInstrumentation, entry);
        return entry.value;
      }
    }
  }
  if (expr.type === "TableConstructor") {
    const table = new LuaTable();
    let nextArrayIndex = 1;
    for (const field of expr.fields) {
      switch (field.type) {
        case "PropField": {
          const pf = field as LuaPropField;
          const value = await recurse(pf.value);
          void table.set(pf.key, selectVal(value), sf);
          break;
        }
        case "DynamicField": {
          const df = field as LuaDynamicField;
          const key = await evalExpression(df.key, env, sf);
          const value = await recurse(df.value);
          void table.set(key, selectVal(value), sf);
          break;
        }
        case "ExpressionField": {
          const ef = field as LuaExpressionField;
          const value = await recurse(ef.value);
          table.rawSetArrayIndex(nextArrayIndex, selectVal(value));
          nextArrayIndex++;
          break;
        }
      }
    }
    return table;
  }
  if (expr.type === "Binary") {
    const bin = expr as LuaBinaryExpression;
    if (bin.operator === "and") {
      const left = singleResult(await recurse(bin.left));
      if (!luaTruthy(left)) return left;
      return singleResult(await recurse(bin.right));
    }
    if (bin.operator === "or") {
      const left = singleResult(await recurse(bin.left));
      if (luaTruthy(left)) return left;
      return singleResult(await recurse(bin.right));
    }
    const left = singleResult(await recurse(bin.left));
    const right = singleResult(await recurse(bin.right));
    return luaOp(bin.operator, left, right, undefined, undefined, expr.ctx, sf);
  }
  if (expr.type === "Unary") {
    const un = expr as LuaUnaryExpression;
    const arg = singleResult(await recurse(un.argument));
    switch (un.operator) {
      case "-":
        return typeof arg === "number"
          ? -arg
          : luaOp("-", 0, arg, undefined, undefined, expr.ctx, sf);
      case "not":
        return !luaTruthy(arg);
      case "#":
        return evalExpression(expr, env, sf);
      case "~":
        if (typeof arg === "number") return ~arg;
        throw new Error("attempt to perform bitwise operation on a non-number");
      default:
        return evalExpression(expr, env, sf);
    }
  }
  if (expr.type === "Parenthesized") {
    const paren = expr as LuaParenthesizedExpression;
    return singleResult(await recurse(paren.expression));
  }
  if (expr.type === "QueryIn") {
    const left = singleResult(await recurse(expr.left));
    const right = singleResult(await recurse(expr.right));

    if (right instanceof LuaTable) {
      for (let i = 1; i <= right.length; i++) {
        const candidate = right.rawGet(i);
        if (candidate === left) {
          return true;
        }
      }

      for (const key of luaKeys(right)) {
        if (typeof key === "number" && Number.isInteger(key)) {
          continue;
        }
        const candidate = right.rawGet(key);
        if (candidate === left) {
          return true;
        }
      }

      return false;
    }

    if (Array.isArray(right)) {
      return right.some((candidate) => candidate === left);
    }

    throw new LuaRuntimeError(
      "'in' right-hand side must be a table or array",
      sf.withCtx(expr.ctx),
    );
  }
  return evalExpression(expr, env, sf);
}

/**
 * Collect the canonical key order from an array of select results.
 * Finds the first LuaTable that has the maximum number of string keys
 * and returns its keys in insertion order.  This represents the
 * "complete" column set in the order the user wrote in `select { ... }`.
 */
function collectCanonicalKeyOrder(results: any[]): string[] | null {
  let best: string[] | null = null;
  for (const item of results) {
    if (item instanceof LuaTable) {
      const keys = luaKeys(item).filter(
        (k): k is string => typeof k === "string",
      );
      if (!best || keys.length > best.length) {
        best = keys;
      }
    }
  }
  return best;
}

function normalizeSelectResults(results: any[]): any[] {
  if (results.length === 0) return results;
  const canonicalKeys = collectCanonicalKeyOrder(results);
  if (!canonicalKeys || canonicalKeys.length === 0) return results;
  for (let i = 0; i < results.length; i++) {
    const item = results[i];
    if (!(item instanceof LuaTable)) continue;
    let needsRebuild = false;
    for (const k of canonicalKeys) {
      const v = item.rawGet(k);
      if (v === undefined || v === null) {
        needsRebuild = true;
        break;
      }
    }
    if (!needsRebuild) continue;
    const rebuilt = new LuaTable();
    for (const k of canonicalKeys) {
      const v = item.rawGet(k);
      void rebuilt.rawSet(k, v === undefined || v === null ? LIQ_NULL : v);
    }
    for (const k of luaKeys(item)) {
      if (typeof k !== "string") {
        void rebuilt.rawSet(k, item.rawGet(k));
      }
    }
    results[i] = rebuilt;
  }
  return results;
}

function resolveUsing(
  using: string | LuaFunctionBody | undefined,
  env: LuaEnv,
  _sf: LuaStackFrame,
): LuaValue | null {
  if (using === undefined) return null;
  if (typeof using === "string") {
    return env.get(using) ?? null;
  }
  return new LuaFunction(using, env);
}

// Compare values using a custom comparator with SWO violation detection
async function usingCompare(
  luaCmp: LuaValue,
  aVal: any,
  bVal: any,
  originalA: number,
  originalB: number,
  desc: boolean,
  sf: LuaStackFrame,
  violated: boolean[],
  keyIdx: number,
): Promise<number> {
  const res = luaTruthy(
    singleResult(await luaCall(luaCmp, [aVal, bVal], sf.astCtx ?? {}, sf)),
  );
  const reverseRes = luaTruthy(
    singleResult(await luaCall(luaCmp, [bVal, aVal], sf.astCtx ?? {}, sf)),
  );

  // both true means SWO violation
  if (res && reverseRes) {
    violated[keyIdx] = true;
    return originalA < originalB ? -1 : 1;
  }

  if (res) return desc ? 1 : -1;
  if (reverseRes) return desc ? -1 : 1;
  return 0;
}

function orderByNeedsSelectKeys(orderBy: LuaOrderBy[] | undefined): boolean {
  if (!orderBy) return false;
  return orderBy.some((o) => o.expr.type === "OrderBySelectKey");
}

async function evalOrderByExpr(
  expr: LuaExpression,
  itemEnv: LuaEnv,
  sf: LuaStackFrame,
  grouped: boolean,
  groupTable: LuaTable | undefined,
  objectVariable: string | undefined,
  outerEnv: LuaEnv,
  config: Config,
  selectRow: any,
  aggregateInstrumentation?: AggregateRuntimeInstrumentation,
  aggregateCtx?: AggregateEvalContext,
): Promise<LuaValue> {
  if (expr.type === "OrderBySelectKey") {
    const ob = expr as LuaOrderBySelectKeyExpression;

    if (ob.key.type !== "String") {
      throw new LuaRuntimeError(
        "order by projected key must use a string literal",
        sf.withCtx(ob.ctx),
      );
    }

    if (!(selectRow instanceof LuaTable)) {
      return null;
    }

    const v = luaGet(selectRow, ob.key.value, sf.astCtx ?? null, sf);
    return isSqlNull(v) ? null : v;
  }

  if (grouped) {
    return evalExpressionWithAggregates(
      expr,
      itemEnv,
      sf,
      groupTable!,
      objectVariable,
      outerEnv,
      config,
      aggregateInstrumentation,
      aggregateCtx,
    );
  }

  return evalExpression(expr, itemEnv, sf);
}

/**
 * Pre-compute all sort keys for each result item (Schwartzian transform)
 * and evaluate each `order by` expression exactly once per item
 */
async function precomputeSortKeys(
  results: any[],
  orderBy: LuaOrderBy[],
  mkEnv: (
    ov: string | undefined,
    item: any,
    e: LuaEnv,
    s: LuaStackFrame,
  ) => LuaEnv,
  objectVariable: string | undefined,
  env: LuaEnv,
  sf: LuaStackFrame,
  grouped: boolean,
  selectResults: any[] | undefined,
  config: Config,
  aggregateInstrumentation?: AggregateRuntimeInstrumentation,
): Promise<any[][]> {
  const allKeys: any[][] = new Array(results.length);
  for (let i = 0; i < results.length; i++) {
    const item = results[i];
    const luaItem = unwrapGroupedItem(item);
    const itemEnv = mkEnv(objectVariable, luaItem, env, sf);
    const selectRow = selectResults?.[i];

    if (selectRow instanceof LuaTable) {
      for (const k of luaKeys(selectRow)) {
        const v = luaGet(selectRow, k, sf.astCtx ?? null, sf);
        itemEnv.setLocal(k, isSqlNull(v) ? null : v);
      }
    }

    const keys: any[] = new Array(orderBy.length);
    const aggregateCtx = grouped
      ? getGroupedAggregateContext(item)
      : undefined;
    if (aggregateCtx) {
      resetAggregateEvalContext(aggregateCtx);
    }
    const groupTable = grouped ? luaItem.rawGet("group") : undefined;

    for (let j = 0; j < orderBy.length; j++) {
      keys[j] = await evalOrderByExpr(
        orderBy[j].expr,
        itemEnv,
        sf,
        grouped,
        groupTable,
        objectVariable,
        env,
        config,
        selectRow,
        aggregateInstrumentation,
        aggregateCtx,
      );
    }

    allKeys[i] = keys;
  }
  return allKeys;
}

/**
 * Compare two items by their pre-computed sort keys without Lua
 * expressions evaluation.
 */
async function sortKeyCompare(
  a: { val: any; idx: number },
  b: { val: any; idx: number },
  orderBy: LuaOrderBy[],
  aKeys: any[],
  bKeys: any[],
  collation: QueryCollationConfig | undefined,
  collator: Intl.Collator,
  resolvedUsing: (LuaValue | null)[],
  violated: boolean[],
  sf: LuaStackFrame,
): Promise<number> {
  for (let idx = 0; idx < orderBy.length; idx++) {
    const { desc, nulls } = orderBy[idx];
    const aVal = aKeys[idx];
    const bVal = bKeys[idx];

    // Handle nulls positioning
    const aIsNull = aVal === null || aVal === undefined || isSqlNull(aVal);
    const bIsNull = bVal === null || bVal === undefined || isSqlNull(bVal);
    if (aIsNull || bIsNull) {
      if (aIsNull && bIsNull) continue;
      // Default: nulls last for asc, nulls first for desc
      const nullsLast = nulls === "last" || (nulls === undefined && !desc);
      if (aIsNull) return nullsLast ? 1 : -1;
      return nullsLast ? -1 : 1;
    }

    const usingFn = resolvedUsing[idx];
    if (usingFn) {
      const cmp = await usingCompare(
        usingFn,
        aVal,
        bVal,
        a.idx,
        b.idx,
        desc,
        sf,
        violated,
        idx,
      );
      if (cmp !== 0) return cmp;
    } else if (
      collation?.enabled &&
      typeof aVal === "string" &&
      typeof bVal === "string"
    ) {
      const order = collator.compare(aVal, bVal);
      if (order !== 0) {
        return desc ? -order : order;
      }
    } else if (aVal < bVal) {
      return desc ? 1 : -1;
    } else if (aVal > bVal) {
      return desc ? -1 : 1;
    }
  }
  return 0;
}

// Build a select-result table from a non-aggregate select expression
async function evalSelectExpression(
  selectExpr: LuaExpression,
  itemEnv: LuaEnv,
  sf: LuaStackFrame,
): Promise<LuaValue> {
  const result = await evalExpression(selectExpr, itemEnv, sf);
  if (!(result instanceof LuaTable)) return result;
  for (const k of luaKeys(result)) {
    const v = result.rawGet(k);
    if (v === null || v === undefined) {
      void result.rawSet(k, LIQ_NULL);
    }
  }
  return result;
}

export async function applyQuery(
  results: any[],
  query: LuaCollectionQuery,
  env: LuaEnv,
  sf: LuaStackFrame,
  config: Config = new Config(),
  instrumentation?: QueryInstrumentation,
  aggregateInstrumentation?: AggregateRuntimeInstrumentation,
): Promise<any[]> {
  results = results.slice();

  if (query.where) {
    const stageStart = nowMs();
    const inputRows = results.length;

    // P2: Extract simple column-vs-literal comparisons that can be
    // evaluated with fast JS property access — no LuaEnv allocation
    // or evalExpression overhead per row.
    const { filters: inlineFilters, residual: residualWhere } =
      extractKvInlineFilters(query.where, query.objectVariable);

    // Fast pre-filter pass: pure JS comparisons, no Lua overhead
    let inlineFilteredRows = 0;
    if (inlineFilters.length > 0) {
      const preFiltered = [];
      for (const value of results) {
        if (matchesAllKvInlineFilters(value, inlineFilters)) {
          preFiltered.push(value);
        }
      }
      inlineFilteredRows = results.length - preFiltered.length;
      results = preFiltered;
    }

    // Residual pass: complex predicates still need full Lua eval
    if (residualWhere) {
      const filteredResults = [];
      for (const value of results) {
        const itemEnv = buildItemEnvLocal(query.objectVariable, value, env, sf);
        const whereResult = await evalExpression(residualWhere, itemEnv, sf);
        if (luaTruthy(whereResult)) {
          filteredResults.push(value);
        }
      }
      results = filteredResults;
    }

    emitStageStat(
      instrumentation,
      "where",
      inputRows,
      results.length,
      stageStart,
      {
        rowsRemoved: Math.max(0, inputRows - results.length),
        inlineFilteredRows:
          inlineFilteredRows > 0 ? inlineFilteredRows : undefined,
      },
    );
  }

  // Implicit single group
  if (
    !query.groupBy &&
    ((query.select && containsAggregate(query.select, config)) ||
      (query.having && containsAggregate(query.having, config)))
  ) {
    query = { ...query, groupBy: [] };
  }

  const grouped = !!query.groupBy;

  // Collect group-by key names for unpacking into the post-group environment.
  let groupByNames: string[] | undefined;

  if (query.groupBy) {
    const stageStart = nowMs();
    const inputRows = results.length;

    // Extract expressions and names from `group by` entries
    const groupByEntries = query.groupBy;

    // Derive canonical name (explicit alias first, or from expression)
    groupByNames = groupByEntries
      .map((entry) => {
        if (entry.alias) return entry.alias;
        if (entry.expr.type === "Variable") return entry.expr.name;
        if (entry.expr.type === "PropertyAccess") return entry.expr.property;
        return undefined as unknown as string;
      })
      .filter(Boolean);

    const groups = new Map<string | symbol, { key: any; items: any[] }>();

    for (const item of results) {
      const itemEnv = buildItemEnvLocal(query.objectVariable, item, env, sf);

      const keyParts: any[] = [];
      const keyRecord: Record<string, any> = {};

      for (let ei = 0; ei < groupByEntries.length; ei++) {
        const entry = groupByEntries[ei];
        const v = await evalExpression(entry.expr, itemEnv, sf);
        keyParts.push(v);
        // Use alias if provided, or from expression
        const name =
          entry.alias ??
          (entry.expr.type === "Variable" ? entry.expr.name : undefined) ??
          (entry.expr.type === "PropertyAccess"
            ? entry.expr.property
            : undefined);
        if (name) {
          keyRecord[name] = v;
        }
      }

      // Implicit single group uses a symbol key
      const compositeKey: string | symbol =
        keyParts.length === 0
          ? IMPLICIT_GROUP_KEY
          : keyParts.length === 1
            ? generateKey(keyParts[0])
            : JSON.stringify(keyParts.map(generateKey));
      let entry = groups.get(compositeKey);
      if (!entry) {
        let keyVal: any;
        if (keyParts.length === 0) {
          // Implicit single group — key is `nil`
          keyVal = null;
        } else if (keyParts.length === 1) {
          keyVal = keyParts[0];
        } else {
          const kt = new LuaTable();
          // Always populate array indices from keyParts
          for (let i = 0; i < keyParts.length; i++) {
            kt.rawSetArrayIndex(i + 1, keyParts[i]);
          }
          // Additionally set named fields for Variable/PropertyAccess exprs
          for (const name in keyRecord) {
            void kt.rawSet(name, keyRecord[name]);
          }
          keyVal = kt;
        }
        entry = {
          key: keyVal,
          items: [],
        };
        groups.set(compositeKey, entry);
      }
      entry.items.push(item);
    }

    const groupedResults: GroupedValue[] = [];
    for (const { key, items } of groups.values()) {
      const groupTable = new LuaTable();
      for (let i = 0; i < items.length; i++) {
        const item = items[i];
        groupTable.rawSetArrayIndex(
          i + 1,
          item instanceof LuaTable || typeof item !== "object" || item === null
            ? item
            : jsToLuaValue(item),
        );
      }
      const row = new LuaTable();
      void row.rawSet("key", key);
      void row.rawSet("group", groupTable);
      groupedResults.push({
        item: row,
        aggregateCtx: { entries: [], cursor: 0, local: new Map() },
      });
    }
    results = groupedResults as any[];

    emitStageStat(
      instrumentation,
      "groupBy",
      inputRows,
      results.length,
      stageStart,
    );
  }

  if (query.having) {
    const stageStart = nowMs();
    const inputRows = results.length;
    const filteredResults = [];
    for (const value of results) {
      let condResult;
      if (grouped) {
        const luaValue = unwrapGroupedItem(value);
        const aggregateCtx = getGroupedAggregateContext(value);
        resetAggregateEvalContext(aggregateCtx);
        const itemEnv = buildGroupItemEnv(
          query.objectVariable,
          groupByNames,
          luaValue,
          env,
          sf,
        );
        const groupTable = luaValue.rawGet("group");
        condResult = await evalExpressionWithAggregates(
          query.having,
          itemEnv,
          sf,
          groupTable,
          query.objectVariable,
          env,
          config,
          aggregateInstrumentation,
          aggregateCtx,
        );
      } else {
        const itemEnv = buildItemEnvLocal(query.objectVariable, value, env, sf);
        condResult = await evalExpression(query.having, itemEnv, sf);
      }
      if (luaTruthy(condResult)) {
        filteredResults.push(value);
      }
    }
    results = filteredResults;
    emitStageStat(
      instrumentation,
      "having",
      inputRows,
      results.length,
      stageStart,
      { rowsRemoved: Math.max(0, inputRows - results.length) },
    );
  }

  const mkEnv = grouped
    ? (ov: string | undefined, item: any, e: LuaEnv, s: LuaStackFrame) =>
        buildGroupItemEnv(ov, groupByNames, unwrapGroupedItem(item), e, s)
    : buildItemEnvLocal;

  let selectResults: any[] | undefined;

  const needsSelectResultsForOrderBy =
    !!query.select &&
    !!query.orderBy &&
    (grouped || orderByNeedsSelectKeys(query.orderBy));

  // Pre-compute select results when ORDER BY depends on projected values.
  if (needsSelectResultsForOrderBy) {
    const stageStart = nowMs();
    const inputRows = results.length;
    const selectExpr = query.select!;
    selectResults = [];
    for (const item of results) {
      const itemEnv = mkEnv(query.objectVariable, item, env, sf);

      if (grouped) {
        const luaItem = unwrapGroupedItem(item);
        const aggregateCtx = getGroupedAggregateContext(item);
        resetAggregateEvalContext(aggregateCtx);
        const groupTable = luaItem.rawGet("group");
        const selected = await evalExpressionWithAggregates(
          selectExpr,
          itemEnv,
          sf,
          groupTable,
          query.objectVariable,
          env,
          config,
          aggregateInstrumentation,
          aggregateCtx,
        );
        selectResults.push(selected);
      } else {
        selectResults.push(await evalSelectExpression(selectExpr, itemEnv, sf));
      }
    }
    selectResults = normalizeSelectResults(selectResults);
    emitStageStat(
      instrumentation,
      "select",
      inputRows,
      selectResults.length,
      stageStart,
    );
  }

  if (query.orderBy) {
    const stageStart = nowMs();
    const inputRows = results.length;

    const collation = config.get<QueryCollationConfig>("queryCollation", {});
    const collator = Intl.Collator(collation?.locale, collation?.options);

    const resolvedUsing: (LuaValue | null)[] = [];
    const violated: boolean[] = [];
    for (const ob of query.orderBy) {
      resolvedUsing.push(resolveUsing(ob.using, env, sf));
      violated.push(false);
    }

    // Decorate: pre-compute all sort keys once (Schwartzian transform)
    const sortKeys = await precomputeSortKeys(
      results,
      query.orderBy,
      mkEnv,
      query.objectVariable,
      env,
      sf,
      grouped,
      selectResults,
      config,
      aggregateInstrumentation,
    );

    // Tag each result with its original index for stable sorting
    const tagged = results.map((val, idx) => ({ val, idx }));

    // Sort: compare cached keys only, no Lua eval in comparator
    await asyncMergeSort(tagged, (a, b) =>
      sortKeyCompare(
        a,
        b,
        query.orderBy!,
        sortKeys[a.idx],
        sortKeys[b.idx],
        collation,
        collator,
        resolvedUsing,
        violated,
        sf,
      ),
    );

    // Check for SWO violations in comparators
    for (let i = 0; i < violated.length; i++) {
      if (violated[i]) {
        throw new LuaRuntimeError(
          `order by #${
            i + 1
          }: 'using' comparator violates strict weak ordering`,
          sf,
        );
      }
    }

    if (selectResults) {
      const reorderedResults: any[] = new Array(tagged.length);
      const reorderedSelect: any[] = new Array(tagged.length);
      for (let i = 0; i < tagged.length; i++) {
        reorderedResults[i] = tagged[i].val;
        reorderedSelect[i] = selectResults[tagged[i].idx];
      }
      results = reorderedResults;
      selectResults = reorderedSelect;
    } else {
      results = tagged.map((t) => t.val);
    }

    emitStageStat(
      instrumentation,
      "orderBy",
      inputRows,
      results.length,
      stageStart,
      { memoryRows: inputRows },
    );
  }

  if (query.select) {
    if (!selectResults) {
      const stageStart = nowMs();
      const inputRows = results.length;
      const selectExpr = query.select;
      const newResult = [];
      for (const item of results) {
        const itemEnv = mkEnv(query.objectVariable, item, env, sf);
        if (grouped) {
          const luaItem = unwrapGroupedItem(item);
          const aggregateCtx = getGroupedAggregateContext(item);
          resetAggregateEvalContext(aggregateCtx);
          const groupTable = luaItem.rawGet("group");
          newResult.push(
            await evalExpressionWithAggregates(
              selectExpr,
              itemEnv,
              sf,
              groupTable,
              query.objectVariable,
              env,
              config,
              aggregateInstrumentation,
              aggregateCtx,
            ),
          );
        } else {
          newResult.push(await evalSelectExpression(query.select, itemEnv, sf));
        }
      }
      results = normalizeSelectResults(newResult);
      emitStageStat(
        instrumentation,
        "select",
        inputRows,
        results.length,
        stageStart,
      );
    } else {
      results = selectResults;
    }
  }

  if (query.distinct) {
    const stageStart = nowMs();
    const inputRows = results.length;
    const seen = new Set();
    const distinctResult = [];
    for (const item of results) {
      const key = generateKey(unwrapGroupedItem(item));
      if (!seen.has(key)) {
        seen.add(key);
        distinctResult.push(item);
      }
    }
    results = distinctResult;
    emitStageStat(
      instrumentation,
      "distinct",
      inputRows,
      results.length,
      stageStart,
      { rowsRemoved: Math.max(0, inputRows - results.length) },
    );
  }

  if (query.limit !== undefined && query.offset !== undefined) {
    const stageStart = nowMs();
    const inputRows = results.length;
    results = results.slice(query.offset, query.offset + query.limit);
    emitStageStat(
      instrumentation,
      "limit",
      inputRows,
      results.length,
      stageStart,
    );
  } else if (query.limit !== undefined) {
    const stageStart = nowMs();
    const inputRows = results.length;
    results = results.slice(0, query.limit);
    emitStageStat(
      instrumentation,
      "limit",
      inputRows,
      results.length,
      stageStart,
    );
  } else if (query.offset !== undefined) {
    const stageStart = nowMs();
    const inputRows = results.length;
    results = results.slice(query.offset);
    emitStageStat(
      instrumentation,
      "limit",
      inputRows,
      results.length,
      stageStart,
    );
  }

  return grouped ? results.map(unwrapGroupedItem) : results;
}

/**
 * A simple scalar filter that can be applied inline during a KV scan
 * without building a full Lua environment per row.
 */
export type KvInlineFilter = {
  column: string;
  op: "==" | "~=" | "!=" | "<" | "<=" | ">" | ">=";
  value: string | number | boolean | null;
};

/**
 * Extract simple column-vs-literal comparisons from a WHERE expression
 * that can be evaluated directly on raw KV values without Lua eval.
 *
 * Returns the pushable filters and a residual WHERE expression for
 * anything that couldn't be pushed.
 */
export function extractKvInlineFilters(
  where: LuaExpression | undefined,
  objectVariable: string | undefined,
): { filters: KvInlineFilter[]; residual: LuaExpression | undefined } {
  if (!where) return { filters: [], residual: undefined };

  const conjuncts = flattenAndConjuncts(where);
  const pushed: KvInlineFilter[] = [];
  const remaining: LuaExpression[] = [];

  for (const conjunct of conjuncts) {
    const filter = tryExtractInlineFilter(conjunct, objectVariable);
    if (filter) {
      pushed.push(filter);
    } else {
      remaining.push(conjunct);
    }
  }

  const residual =
    remaining.length > 0
      ? remaining.reduce((acc, e) => ({
          type: "Binary" as const,
          operator: "and" as const,
          left: acc,
          right: e,
          ctx: where.ctx,
        }))
      : undefined;

  return { filters: pushed, residual };
}

/** Flatten top-level AND conjuncts from a WHERE expression. */
function flattenAndConjuncts(expr: LuaExpression): LuaExpression[] {
  if (expr.type === "Binary" && expr.operator === "and") {
    return [
      ...flattenAndConjuncts((expr as LuaBinaryExpression).left),
      ...flattenAndConjuncts((expr as LuaBinaryExpression).right),
    ];
  }
  return [expr];
}

/**
 * Try to convert a single conjunct into a KvInlineFilter.
 * Returns null if the conjunct is too complex for inline evaluation.
 */
function tryExtractInlineFilter(
  expr: LuaExpression,
  objectVariable: string | undefined,
): KvInlineFilter | null {
  if (expr.type !== "Binary") return null;
  const bin = expr as LuaBinaryExpression;
  const op = bin.operator;

  // Only comparison operators
  if (
    op !== "==" &&
    op !== "~=" &&
    op !== "!=" &&
    op !== "<" &&
    op !== "<=" &&
    op !== ">" &&
    op !== ">="
  ) {
    return null;
  }

  // Try column op literal (left = column, right = literal)
  const leftCol = tryExtractColumn(bin.left, objectVariable);
  const rightLit = tryExtractLiteral(bin.right);
  if (leftCol !== null && rightLit !== undefined) {
    return { column: leftCol, op: op as KvInlineFilter["op"], value: rightLit };
  }

  // Try literal op column (left = literal, right = column) — flip operator
  const rightCol = tryExtractColumn(bin.right, objectVariable);
  const leftLit = tryExtractLiteral(bin.left);
  if (rightCol !== null && leftLit !== undefined) {
    const flipped = flipComparisonOp(op);
    if (flipped) {
      return { column: rightCol, op: flipped, value: leftLit };
    }
  }

  return null;
}

/** Extract a column name from a simple variable or property access. */
function tryExtractColumn(
  expr: LuaExpression,
  objectVariable: string | undefined,
): string | null {
  // Without object variable: bare variable name is the column
  if (!objectVariable && expr.type === "Variable") {
    return expr.name;
  }
  // With object variable: `obj.column` pattern
  if (
    objectVariable &&
    expr.type === "PropertyAccess" &&
    expr.object.type === "Variable" &&
    expr.object.name === objectVariable
  ) {
    return expr.property;
  }
  return null;
}

/** Extract a constant literal value from an expression. */
function tryExtractLiteral(
  expr: LuaExpression,
): string | number | boolean | null | undefined {
  switch (expr.type) {
    case "String":
      return expr.value;
    case "Number":
      return expr.value;
    case "Boolean":
      return expr.value;
    case "Nil":
      return null;
    default:
      return undefined; // signal: not a literal
  }
}

/** Flip a comparison operator for when operands are swapped. */
function flipComparisonOp(op: string): KvInlineFilter["op"] | null {
  switch (op) {
    case "==":
      return "==";
    case "~=":
      return "~=";
    case "!=":
      return "!=";
    case "<":
      return ">";
    case "<=":
      return ">=";
    case ">":
      return "<";
    case ">=":
      return "<=";
    default:
      return null;
  }
}

/**
 * Evaluate a KvInlineFilter against a raw item (JS object or LuaTable).
 * Returns true if the item passes the filter.
 */
function matchesKvInlineFilter(item: any, filter: KvInlineFilter): boolean {
  let fieldVal: any;
  if (item instanceof LuaTable) {
    fieldVal = item.rawGet(filter.column);
  } else if (typeof item === "object" && item !== null) {
    fieldVal = item[filter.column];
  } else {
    return false;
  }

  // Normalize undefined to null for comparison
  if (fieldVal === undefined) fieldVal = null;

  const lit = filter.value;

  switch (filter.op) {
    case "==":
      return fieldVal === lit;
    case "~=":
    case "!=":
      return fieldVal !== lit;
    case "<":
      if (fieldVal === null || lit === null) return false;
      return fieldVal < lit;
    case "<=":
      if (fieldVal === null || lit === null) return false;
      return fieldVal <= lit;
    case ">":
      if (fieldVal === null || lit === null) return false;
      return fieldVal > lit;
    case ">=":
      if (fieldVal === null || lit === null) return false;
      return fieldVal >= lit;
    default:
      return true;
  }
}

/**
 * Apply an array of KvInlineFilters to a single item.
 * Returns true only if ALL filters pass (conjunction).
 */
function matchesAllKvInlineFilters(
  item: any,
  filters: KvInlineFilter[],
): boolean {
  for (let i = 0; i < filters.length; i++) {
    if (!matchesKvInlineFilter(item, filters[i])) return false;
  }
  return true;
}

export async function queryLua<T = any>(
  kv: KvPrimitives,
  prefix: KvKey,
  query: LuaCollectionQuery,
  env: LuaEnv,
  sf: LuaStackFrame = LuaStackFrame.lostFrame,
  enricher?: (key: KvKey, item: any) => any,
  config?: Config,
  instrumentation?: QueryInstrumentation,
  aggregateInstrumentation?: AggregateRuntimeInstrumentation,
): Promise<T[]> {
  // P2: Extract simple column comparisons that can be evaluated inline
  // during the KV scan, avoiding object allocation and Lua eval overhead
  // for rows that will be filtered out anyway.
  const { filters: inlineFilters, residual: residualWhere } =
    extractKvInlineFilters(query.where, query.objectVariable);

  const results: T[] = [];
  for await (let { key, value } of kv.query({ prefix })) {
    if (enricher) {
      value = enricher(key, value);
    }
    // Apply inline filters before pushing into results array
    if (
      inlineFilters.length > 0 &&
      !matchesAllKvInlineFilters(value, inlineFilters)
    ) {
      continue;
    }
    results.push(value);
  }

  // Pass residual WHERE (complex predicates) to applyQuery
  const residualQuery: LuaCollectionQuery =
    inlineFilters.length > 0 ? { ...query, where: residualWhere } : query;

  return applyQuery(
    results,
    residualQuery,
    env,
    sf,
    config,
    instrumentation,
    aggregateInstrumentation,
  );
}

function generateKey(value: any) {
  if (isSqlNull(value)) {
    return "__SQL_NULL__";
  }
  if (value instanceof LuaTable) {
    return JSON.stringify(luaTableToJSWithNulls(value));
  }
  return typeof value === "object" && value !== null
    ? JSON.stringify(value)
    : value;
}

function luaTableToJSWithNulls(
  table: LuaTable,
  sf = LuaStackFrame.lostFrame,
): any {
  if (table.length > 0) {
    const arr: any[] = [];
    for (let i = 1; i <= table.length; i++) {
      const v = table.rawGet(i);
      arr.push(
        isSqlNull(v)
          ? "__SQL_NULL__"
          : v instanceof LuaTable
            ? luaTableToJSWithNulls(v, sf)
            : v,
      );
    }
    return arr;
  }
  const obj: Record<string, any> = {};
  for (const key of luaKeys(table)) {
    const v = table.rawGet(key);
    obj[key] = isSqlNull(v)
      ? "__SQL_NULL__"
      : v instanceof LuaTable
        ? luaTableToJSWithNulls(v, sf)
        : v;
  }
  return obj;
}

export class DataStoreQueryCollection implements LuaQueryCollection {
  constructor(
    private readonly dataStore: DataStore,
    readonly prefix: string[],
  ) {}

  query(
    query: LuaCollectionQuery,
    env: LuaEnv,
    sf: LuaStackFrame,
    config?: Config,
    instrumentation?: QueryInstrumentation,
    aggregateInstrumentation?: AggregateRuntimeInstrumentation,
  ): Promise<any[]> {
    return queryLua(
      this.dataStore.kv,
      this.prefix,
      query,
      env,
      sf,
      undefined,
      config,
      instrumentation,
      aggregateInstrumentation,
    );
  }

  /** O(n) count via KV scan — avoids materializing all rows for planning */
  async getStats(): Promise<CollectionStats> {
    const rowCount = await this.dataStore.kv.countQuery({
      prefix: this.prefix,
    });

    return {
      rowCount,
      ndv: new Map(),
      statsSource: rowCount === 0 ? "computed-empty" : "unknown-default",
      executionCapabilities: {
        engines: [
          {
            id: "kv-basic-filter",
            name: "KV basic filter scan",
            kind: "kv",
            capabilities: [
              "scan-kv",
              "stage-where",
              "pred-eq",
              "pred-neq",
              "pred-lt",
              "pred-lte",
              "pred-gt",
              "pred-gte",
              "bool-and",
              "expr-literal",
              "expr-column-qualified",
              "expr-column-unqualified",
              "stats-row-count",
            ],
            baseCostWeight: KV_ENGINE_BASE_COST_WEIGHT,
            capabilityCosts: {
              "pred-eq": 0.9,
              "pred-neq": 1.0,
              "pred-lt": 1.0,
              "pred-lte": 1.0,
              "pred-gt": 1.0,
              "pred-gte": 1.0,
              "bool-and": 0.8,
            },
          },
        ],
      },
    };
  }
}

export function normalizeExecutionEngines(
  caps: CollectionExecutionCapabilities | undefined,
): QueryEngineCapability[] {
  return caps?.engines ?? [];
}

export function makeQueryEngineCapability(
  kind: QueryEngineKind,
  capabilities: PlannerCapability[],
  overrides: Partial<QueryEngineCapability> = {},
): QueryEngineCapability {
  return {
    id: overrides.id ?? kind,
    name: overrides.name ?? kind,
    kind,
    capabilities,
    baseCostWeight: overrides.baseCostWeight,
    capabilityCosts: overrides.capabilityCosts,
    priority: overrides.priority,
    metadata: overrides.metadata,
  };
}

export function makeExecutionCapabilities(
  kind: QueryEngineKind,
  capabilities: PlannerCapability[],
  overrides: Partial<QueryEngineCapability> = {},
): CollectionExecutionCapabilities {
  return {
    engines: [makeQueryEngineCapability(kind, capabilities, overrides)],
  };
}

export function collectionHasPlannerCapability(
  stats: CollectionStats | undefined,
  capability: PlannerCapability,
): boolean {
  const engines = stats?.executionCapabilities?.engines ?? [];
  return engines.some((engine) => engine.capabilities.includes(capability));
}

export function collectionHasEngineKind(
  stats: CollectionStats | undefined,
  kind: QueryEngineKind,
): boolean {
  const engines = stats?.executionCapabilities?.engines ?? [];
  return engines.some((engine) => engine.kind === kind);
}

export function collectionPrimaryEngine(
  stats: CollectionStats | undefined,
): QueryEngineCapability | undefined {
  const engines = stats?.executionCapabilities?.engines ?? [];
  if (engines.length === 0) return undefined;
  return [...engines].sort((a, b) => (b.priority ?? 0) - (a.priority ?? 0))[0];
}
