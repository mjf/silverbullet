import type { ObjectValue } from "@silverbulletmd/silverbullet/type/index";
import type { Config } from "../config.ts";
import {
  ArrayQueryCollection,
  applyQuery,
  type LuaCollectionQuery,
  type LuaQueryCollection,
  type LuaQueryCollectionWithStats,
  type CollectionStats,
} from "../space_lua/query_collection.ts";
import {
  jsToLuaValue,
  LuaEnv,
  LuaStackFrame,
  LuaTable,
} from "../space_lua/runtime.ts";
import { parseExpressionString } from "../space_lua/parse.ts";
import type { DataStore } from "./datastore.ts";
import type { KV, KvKey } from "@silverbulletmd/silverbullet/type/datastore";
import type { EventHook } from "../plugos/hooks/event.ts";
import type { DataStoreMQ } from "./mq.datastore.ts";
import type { Space } from "../space.ts";
import { validateObject } from "../plugos/syscalls/jsonschema.ts";
import {
  getAggregateSpec,
  getBuiltinAggregateEntries,
} from "../space_lua/aggregates.ts";
import {
  BitmapIndex,
  type BitmapIndexConfig,
  type EncodedObject,
} from "./bitmap/bitmap_index.ts";
import { MCVList } from "../space_lua/mcv.ts";

// KV key prefixes
const indexKey = "idx";
// Reverse key: [reverseKey, page, tag, refKey] → objectId
const reverseKey = "ridx";

const indexVersionKey = ["$indexVersion"];

// Bump this every time a full reindex is needed
// 14: $enc rename + reverse key restructure
const desiredIndexVersion = 14;

const textEncoder = new TextEncoder();

type TagDefinition = {
  tagPage?: string;
  metatable?: any;
  mustValidate?: boolean;
  schema?: any;
  validate?: (o: ObjectValue) => Promise<string | null | undefined>;
  transform?: (
    o: ObjectValue,
  ) =>
    | Promise<ObjectValue[] | ObjectValue>
    | ObjectValue[]
    | ObjectValue
    | null;
};

type BitmapPredicate =
  | {
      kind: "eq";
      column: string;
      value: string | number | boolean;
    }
  | {
      kind: "neq";
      column: string;
      value: string | number | boolean;
    };

type IndexStorageStats = {
  bitmapBytes: number;
  metaBytes: number;
  dictionaryBytes: number;
  objectBytes: number;
  indexBytes: number;
  totalBytes: number;
};

type StorageStatsRow = {
  scope: "tag" | "global";
  tag: string | null;
  rowCount: number | null;
  bitmapBytes: number;
  metaBytes: number;
  dictionaryBytes: number | null;
  objectBytes: number;
  indexBytes: number;
  totalBytes: number;
};

function literalValueFromExpr(
  expr: any,
): string | number | boolean | undefined {
  switch (expr?.type) {
    case "String":
      return expr.value;
    case "Number":
      return expr.value;
    case "Boolean":
      return expr.value;
    default:
      return undefined;
  }
}

function propertyNameForSourceExpr(
  expr: any,
  sourceName: string | undefined,
): string | undefined {
  if (!expr) return undefined;

  if (expr.type === "PropertyAccess") {
    if (expr.object?.type === "Variable") {
      if (!sourceName || expr.object.name === sourceName) {
        return expr.property;
      }
    }
  }

  if (!sourceName && expr.type === "Variable") {
    return expr.name;
  }

  return undefined;
}

function extractBitmapPredicate(
  expr: any,
  sourceName: string | undefined,
): BitmapPredicate | undefined {
  if (!expr || expr.type !== "Binary") return undefined;

  const leftCol = propertyNameForSourceExpr(expr.left, sourceName);
  const rightCol = propertyNameForSourceExpr(expr.right, sourceName);
  const leftLit = literalValueFromExpr(expr.left);
  const rightLit = literalValueFromExpr(expr.right);

  if (expr.operator === "==") {
    if (leftCol && rightLit !== undefined) {
      return { kind: "eq", column: leftCol, value: rightLit };
    }
    if (rightCol && leftLit !== undefined) {
      return { kind: "eq", column: rightCol, value: leftLit };
    }
  }

  if (expr.operator === "~=" || expr.operator === "!=") {
    if (leftCol && rightLit !== undefined) {
      return { kind: "neq", column: leftCol, value: rightLit };
    }
    if (rightCol && leftLit !== undefined) {
      return { kind: "neq", column: rightCol, value: leftLit };
    }
  }

  return undefined;
}

export class ObjectValidationError extends Error {
  constructor(
    message: string,
    readonly object: ObjectValue,
  ) {
    super(message);
  }
}

export class ObjectIndex {
  private bitmapIndex: BitmapIndex;

  constructor(
    private ds: DataStore,
    private config: Config,
    private eventHook: EventHook,
    private mq: DataStoreMQ,
    bitmapConfig?: Partial<BitmapIndexConfig>,
  ) {
    this.bitmapIndex = new BitmapIndex(bitmapConfig);

    // Clear any entries for deleted files
    this.eventHook.addLocalListener("file:deleted", (path: string) => {
      return this.clearFileIndex(path);
    });

    let indexStarted = false;
    this.eventHook.addLocalListener("file:listed", () => {
      indexStarted = true;
    });

    // Handle initial index completion
    void this.hasFullIndexCompleted().then((hasCompleted) => {
      if (!hasCompleted) {
        const emptyQueueHandler = async () => {
          console.log("Index queue empty, checking if index is complete");
          if (indexStarted) {
            console.info("Initial index complete, reloading editor state");
            await this.markFullIndexComplete();
            this.eventHook.removeLocalListener(
              "mq:emptyQueue:indexQueue",
              emptyQueueHandler,
            );
            void this.eventHook.dispatchEvent("editor:reloadState");
          }
        };
        this.eventHook.addLocalListener(
          "mq:emptyQueue:indexQueue",
          emptyQueueHandler,
        );
      }
    });
  }

  private enrichValue(tagName: string, value: any): any {
    const mt = this.config.get<LuaTable | undefined>(
      ["tags", tagName, "metatable"],
      undefined,
    );
    if (!mt) return value;
    value = jsToLuaValue(value);
    value.metatable = mt;
    return value;
  }

  private allKnownTags(): string[] {
    const tags: string[] = [];
    for (const tagId of this.bitmapIndex.allTagIds()) {
      const decoded = this.bitmapIndex.getDictionary().decodeValue(tagId);
      if (typeof decoded === "string") {
        tags.push(decoded);
      }
    }
    tags.sort();
    return tags;
  }

  private estimateStoredValueSize(value: unknown): number {
    if (value === null || value === undefined) {
      return 0;
    }

    if (value instanceof Uint8Array) {
      return value.byteLength;
    }

    if (typeof value === "string") {
      return textEncoder.encode(value).byteLength;
    }

    if (typeof value === "number" || typeof value === "boolean") {
      return textEncoder.encode(String(value)).byteLength;
    }

    try {
      return textEncoder.encode(JSON.stringify(value)).byteLength;
    } catch {
      return 0;
    }
  }

  private async computeIndexStorageStats(
    tagName?: string,
  ): Promise<IndexStorageStats> {
    let bitmapBytes = 0;
    let metaBytes = 0;
    let dictionaryBytes = 0;
    let objectBytes = 0;

    if (tagName) {
      for await (const { value } of this.ds.query({
        prefix: [indexKey, tagName],
      })) {
        objectBytes += this.estimateStoredValueSize(value);
      }

      const tagId = this.bitmapIndex.getDictionary().tryEncode(tagName);
      if (tagId !== undefined) {
        for await (const { value } of this.ds.query({
          prefix: ["b", String(tagId)],
        })) {
          bitmapBytes += this.estimateStoredValueSize(value);
        }

        const meta = await this.ds.get(["m", String(tagId)]);
        if (meta !== undefined && meta !== null) {
          metaBytes += this.estimateStoredValueSize(meta);
        }
      }
    } else {
      for await (const { value } of this.ds.query({
        prefix: [indexKey],
      })) {
        objectBytes += this.estimateStoredValueSize(value);
      }

      for await (const { value } of this.ds.query({
        prefix: ["b"],
      })) {
        bitmapBytes += this.estimateStoredValueSize(value);
      }

      for await (const { value } of this.ds.query({
        prefix: ["m"],
      })) {
        metaBytes += this.estimateStoredValueSize(value);
      }
    }

    const dictSnapshot = await this.ds.get(["$dict"]);
    if (dictSnapshot !== undefined && dictSnapshot !== null) {
      dictionaryBytes = this.estimateStoredValueSize(dictSnapshot);
    }

    const indexBytes = bitmapBytes + metaBytes + dictionaryBytes;
    const totalBytes = indexBytes + objectBytes;

    return {
      bitmapBytes,
      metaBytes,
      dictionaryBytes,
      objectBytes,
      indexBytes,
      totalBytes,
    };
  }

  tag(tagName: string): LuaQueryCollectionWithStats {
    if (!tagName) {
      throw new Error("Tag name is required");
    }

    const self = this;

    return {
      async query(
        query: LuaCollectionQuery,
        env: LuaEnv,
        sf: LuaStackFrame,
        config?: Config,
      ): Promise<ObjectValue<any>[]> {
        const bitmapPredicate = extractBitmapPredicate(
          query.where,
          query.objectVariable,
        );

        if (bitmapPredicate) {
          const objectIds = await self.bitmapMatchObjectIds(
            tagName,
            bitmapPredicate,
          );
          if (objectIds) {
            const prefetched = await self.loadObjectsByObjectIds(
              tagName,
              objectIds,
            );
            return applyQuery(prefetched, query, env, sf, config);
          }
        }

        const results: ObjectValue<any>[] = [];
        for await (const { value } of self.ds.query({
          prefix: [indexKey, tagName],
        })) {
          const decoded = self.bitmapIndex.decodeObject(
            value as EncodedObject,
          ) as ObjectValue<any>;
          results.push(self.enrichValue(tagName, decoded));
        }
        return applyQuery(results, query, env, sf, config);
      },

      async isTagIndexTrusted(): Promise<boolean> {
        return self.isTagIndexTrusted(tagName);
      },

      async getStats(): Promise<CollectionStats> {
        const tagId = self.bitmapIndex.getDictionary().tryEncode(tagName);
        const indexTrusted = await self.isTagIndexTrusted(tagName);

        if (tagId === undefined) {
          return {
            rowCount: 0,
            ndv: new Map(),
            avgColumnCount: 0,
            statsSource: "computed-empty",
            executionCapabilities: {
              predicatePushdown: "none",
              scanKind: "index-scan",
            },
          };
        }
        const rowCount = self.bitmapIndex.getRowCount(tagId);
        const meta = self.bitmapIndex.getTagMetaById(tagId);
        const ndv = new Map<string, number>();
        const mcv = new Map<string, MCVList>();

        if (meta) {
          for (const [col, colMeta] of Object.entries(meta.columns)) {
            ndv.set(col, colMeta.ndv);

            const topValues = self.bitmapIndex.getColumnMCV(tagId, col);
            if (topValues.length > 0) {
              const list = new MCVList();
              for (const { value, count } of topValues) {
                list.setDirect(value, count);
              }
              mcv.set(col, list);
            }
          }
        }

        const indexComplete = await self.hasFullIndexCompleted();
        return {
          rowCount,
          ndv,
          avgColumnCount:
            rowCount > 0 && meta
              ? Math.round(meta.totalColumnCount / rowCount)
              : 0,
          mcv: mcv.size > 0 ? mcv : undefined,
          statsSource: indexComplete
            ? "persisted-complete"
            : "persisted-partial",
          executionCapabilities: {
            predicatePushdown: indexTrusted ? "bitmap-basic" : "none",
            scanKind: "index-scan",
          },
        };
      },
    };
  }

  async stats(tagName?: string): Promise<LuaQueryCollection> {
    if (tagName === "") {
      throw new Error("Tag name is required");
    }

    const tags = tagName === undefined ? this.allKnownTags() : [tagName];
    const rows: Record<string, any>[] = [];
    const indexComplete = await this.hasFullIndexCompleted();

    for (const tag of tags) {
      const tagId = this.bitmapIndex.getDictionary().tryEncode(tag);
      const indexTrusted = await this.isTagIndexTrusted(tag);

      if (tagId === undefined) {
        rows.push({
          tag,
          column: null,
          rowCount: 0,
          avgColumnCount: 0,
          ndv: null,
          indexed: null,
          statsSource: "computed-empty",
          predicatePushdown: "none",
          scanKind: "index-scan",
          trackedMcvValues: 0,
        });
        continue;
      }

      const meta = this.bitmapIndex.getTagMetaById(tagId);
      const rowCount = this.bitmapIndex.getRowCount(tagId);
      const avgColumnCount =
        rowCount > 0 && meta ? Math.round(meta.totalColumnCount / rowCount) : 0;
      const statsSource = indexComplete
        ? "persisted-complete"
        : "persisted-partial";
      const predicatePushdown = indexTrusted ? "bitmap-basic" : "none";
      const scanKind = "index-scan";

      rows.push({
        tag,
        column: null,
        rowCount,
        avgColumnCount,
        ndv: null,
        indexed: null,
        statsSource,
        predicatePushdown,
        scanKind,
        trackedMcvValues: 0,
      });

      if (!meta || Object.keys(meta.columns).length === 0) {
        continue;
      }

      const columns = Object.keys(meta.columns).sort();
      for (const column of columns) {
        const colMeta = meta.columns[column];
        const trackedMcvValues = this.bitmapIndex.getColumnMCV(
          tagId,
          column,
        ).length;

        rows.push({
          tag,
          column,
          rowCount,
          avgColumnCount,
          ndv: colMeta.ndv,
          indexed: colMeta.indexed,
          statsSource,
          predicatePushdown,
          scanKind,
          trackedMcvValues,
        });
      }
    }

    return new ArrayQueryCollection(rows);
  }

  async storageStats(tagName?: string): Promise<LuaQueryCollection> {
    if (tagName === "") {
      throw new Error("Tag name is required");
    }

    const rows: StorageStatsRow[] = [];
    const tags = tagName === undefined ? this.allKnownTags() : [tagName];

    if (tagName === undefined) {
      const globalStorage = await this.computeIndexStorageStats();
      rows.push({
        scope: "global",
        tag: null,
        rowCount: null,
        bitmapBytes: globalStorage.bitmapBytes,
        metaBytes: globalStorage.metaBytes,
        dictionaryBytes: globalStorage.dictionaryBytes,
        objectBytes: globalStorage.objectBytes,
        indexBytes: globalStorage.indexBytes,
        totalBytes: globalStorage.totalBytes,
      });
    }

    for (const tag of tags) {
      const tagId = this.bitmapIndex.getDictionary().tryEncode(tag);
      const storage = await this.computeIndexStorageStats(tag);

      rows.push({
        scope: "tag",
        tag,
        rowCount: tagId === undefined ? 0 : this.bitmapIndex.getRowCount(tagId),
        bitmapBytes: storage.bitmapBytes,
        metaBytes: storage.metaBytes,
        dictionaryBytes: null,
        objectBytes: storage.objectBytes,
        indexBytes: storage.bitmapBytes + storage.metaBytes,
        totalBytes:
          storage.objectBytes + storage.bitmapBytes + storage.metaBytes,
      });
    }

    return new ArrayQueryCollection(rows);
  }

  contentPages(): LuaQueryCollection {
    return this.filteredTag(
      "page",
      (varName) =>
        `not table.find(${varName}.tags, function(tag) return tag == "meta" or string.startsWith(tag, "meta/") end)`,
    );
  }

  metaPages(): LuaQueryCollection {
    return this.filteredTag(
      "page",
      (varName) =>
        `table.find(${varName}.tags, function(tag) return tag == "meta" or string.startsWith(tag, "meta/") end)`,
    );
  }

  private filteredTag(
    tagName: string,
    buildFilterExpr: (varName: string) => string,
  ): LuaQueryCollection {
    const self = this;
    return {
      async query(
        query: LuaCollectionQuery,
        env: LuaEnv,
        sf: LuaStackFrame,
        config?: Config,
      ): Promise<any[]> {
        const varName = query.objectVariable || "_";
        const filter = parseExpressionString(buildFilterExpr(varName));
        const where = query.where
          ? {
              type: "Binary" as const,
              operator: "and",
              left: filter,
              right: query.where,
              ctx: {},
            }
          : filter;

        const results: any[] = [];
        for await (const { value } of self.ds.query({
          prefix: [indexKey, tagName],
        })) {
          const decoded = self.bitmapIndex.decodeObject(value as EncodedObject);
          results.push(self.enrichValue(tagName, decoded));
        }
        return applyQuery(results, { ...query, where }, env, sf, config);
      },
    };
  }

  /**
   * Returns a queryable collection of all aggregate functions.
   */
  aggregates(): LuaQueryCollection {
    const entries: Record<string, any>[] = [];

    for (const entry of getBuiltinAggregateEntries()) {
      entries.push({
        builtin: true,
        name: entry.name,
        description: entry.description,
        initialize: true,
        iterate: true,
        finish: entry.hasFinish,
        target: null,
      });
    }

    const userAggs: Record<string, any> = this.config.get("aggregates", {});
    for (const [key, spec] of Object.entries(userAggs)) {
      const aliasTarget =
        spec instanceof LuaTable ? spec.rawGet("alias") : (spec?.alias ?? null);
      if (typeof aliasTarget === "string") {
        const resolved = getAggregateSpec(aliasTarget, this.config);
        entries.push({
          builtin: false,
          name: key,
          description:
            spec instanceof LuaTable
              ? (spec.rawGet("description") ?? resolved?.description ?? "")
              : (spec?.description ?? resolved?.description ?? ""),
          initialize: resolved ? !!resolved.initialize : false,
          iterate: resolved ? !!resolved.iterate : false,
          finish: resolved ? !!resolved.finish : false,
          target: aliasTarget,
        });
      } else {
        let hasInit = false;
        let hasIter = false;
        let hasFin = false;
        let desc = "";
        if (spec instanceof LuaTable) {
          hasInit = !!spec.rawGet("initialize");
          hasIter = !!spec.rawGet("iterate");
          hasFin = !!spec.rawGet("finish");
          desc = spec.rawGet("description") ?? "";
        } else if (spec) {
          hasInit = !!spec.initialize;
          hasIter = !!spec.iterate;
          hasFin = !!spec.finish;
          desc = spec.description ?? "";
        }
        entries.push({
          builtin: false,
          name: key,
          description: desc,
          initialize: hasInit,
          iterate: hasIter,
          finish: hasFin,
          target: null,
        });
      }
    }
    return new ArrayQueryCollection(entries);
  }

  async getObjectByRef(
    page: string,
    tag: string,
    ref: string,
  ): Promise<any | null> {
    const refKey = this.cleanKey(ref, page);
    // Reverse key: [reverseKey, page, tag, refKey]
    const objectId = await this.ds.get<number>([reverseKey, page, tag, refKey]);
    if (objectId === null || objectId === undefined) return null;

    const encoded = await this.ds.get<EncodedObject>([
      indexKey,
      tag,
      String(objectId),
    ]);
    if (!encoded) return null;
    return this.bitmapIndex.decodeObject(encoded);
  }

  async deleteObject(page: string, tag: string, ref: string): Promise<void> {
    const refKey = this.cleanKey(ref, page);
    const objectId = await this.ds.get<number>([reverseKey, page, tag, refKey]);
    if (objectId === null || objectId === undefined) return;

    const encoded = await this.ds.get<EncodedObject>([
      indexKey,
      tag,
      String(objectId),
    ]);

    if (encoded) {
      const tagId = this.bitmapIndex.getDictionary().tryEncode(tag);
      if (tagId !== undefined) {
        const meta = this.bitmapIndex.getTagMetaById(tagId);
        if (meta) {
          this.bitmapIndex.unindexObject(tagId, objectId, encoded, meta);
        }
      }
    }

    await this.ds.batchDelete([
      [indexKey, tag, String(objectId)],
      [reverseKey, page, tag, refKey],
    ]);
    await this.flushBitmapState();
  }

  public async indexObjects<T>(
    page: string,
    objects: ObjectValue<T>[],
  ): Promise<void> {
    const kvs = await this.processObjectsToKVs<T>(page, objects, false);
    if (kvs.length > 0) {
      await this.batchSet(page, kvs);
    }
  }

  public async validateObjects<T>(page: string, objects: ObjectValue<T>[]) {
    await this.processObjectsToKVs(page, objects, true);
  }

  queryLuaObjects<T>(
    globalEnv: LuaEnv,
    tag: string,
    query: LuaCollectionQuery,
    scopedVariables?: Record<string, any>,
  ): Promise<ObjectValue<T>[]> {
    const sf = LuaStackFrame.createWithGlobalEnv(globalEnv);
    let env = globalEnv;
    if (scopedVariables) {
      env = new LuaEnv(globalEnv);
      for (const [key, value] of Object.entries(scopedVariables)) {
        env.setLocal(key, jsToLuaValue(value));
      }
    }
    return this.tag(tag).query(query, env, sf) as Promise<ObjectValue<T>[]>;
  }

  private async batchSet(page: string, kvs: KV[]): Promise<void> {
    // Phase 1: batch-read all reverse keys to find existing objectIds
    const reverseKeys: KvKey[] = kvs.map(({ key }) => [
      reverseKey,
      page,
      key[0] as string,
      key[1] as string,
    ]);
    const existingObjectIds = await this.ds.batchGet<number>(reverseKeys);

    // Phase 2: batch-read all existing encoded objects for those that exist
    const encodedReadKeys: (KvKey | null)[] = existingObjectIds.map(
      (objId, i) => {
        if (objId !== null && objId !== undefined) {
          return [indexKey, kvs[i].key[0] as string, String(objId)];
        }
        return null;
      },
    );
    const nonNullEncodedKeys = encodedReadKeys.filter(
      (k): k is KvKey => k !== null,
    );
    const nonNullIndices: number[] = [];
    for (let i = 0; i < encodedReadKeys.length; i++) {
      if (encodedReadKeys[i] !== null) nonNullIndices.push(i);
    }
    const fetchedEncoded =
      nonNullEncodedKeys.length > 0
        ? await this.ds.batchGet<EncodedObject>(nonNullEncodedKeys)
        : [];

    // Map fetched results back to their indices
    const oldEncodedByIndex = new Map<number, EncodedObject>();
    for (let j = 0; j < nonNullIndices.length; j++) {
      const enc = fetchedEncoded[j];
      if (enc) {
        oldEncodedByIndex.set(nonNullIndices[j], enc);
      }
    }

    // Phase 3: process all objects
    const writes: KV[] = [];
    const deletes: KvKey[] = [];

    for (let i = 0; i < kvs.length; i++) {
      const { key, value } = kvs[i];
      const tag = key[0] as string;
      const refKey = key[1] as string;
      const existingObjectId = existingObjectIds[i];

      // Unindex old object if it exists
      if (existingObjectId !== null && existingObjectId !== undefined) {
        const oldEncoded = oldEncodedByIndex.get(i);
        if (oldEncoded) {
          const tagId = this.bitmapIndex.getDictionary().tryEncode(tag);
          if (tagId !== undefined) {
            const meta = this.bitmapIndex.getTagMetaById(tagId);
            if (meta) {
              this.bitmapIndex.unindexObject(
                tagId,
                existingObjectId,
                oldEncoded,
                meta,
              );
            }
          }
        }
        deletes.push([indexKey, tag, String(existingObjectId)]);
      }

      // Encode and index new object
      const encoded = this.bitmapIndex.encodeObject(
        value as Record<string, unknown>,
      );
      const { tagId, meta } = this.bitmapIndex.getTagMeta(tag);
      const objectId =
        existingObjectId ?? this.bitmapIndex.allocateObjectId(tagId);

      if (existingObjectId !== null && existingObjectId !== undefined) {
        // allocateObjectId was not called, but unindex decremented count
        meta.count++;
      }

      this.bitmapIndex.indexObject(tagId, objectId, encoded, meta);

      writes.push({
        key: [indexKey, tag, String(objectId)],
        value: encoded,
      });
      writes.push({
        key: [reverseKey, page, tag, refKey],
        value: objectId,
      });
    }

    const bitmapFlush = this.bitmapIndex.flushToKVs();
    writes.push(...bitmapFlush.writes);
    deletes.push(...bitmapFlush.deletes);

    if (deletes.length > 0) {
      await this.ds.batchDelete(deletes);
    }
    if (writes.length > 0) {
      await this.ds.batchSet(writes);
    }
  }

  private async flushBitmapState(): Promise<void> {
    const { writes, deletes } = this.bitmapIndex.flushToKVs();
    if (deletes.length > 0) {
      await this.ds.batchDelete(deletes);
    }
    if (writes.length > 0) {
      await this.ds.batchSet(writes);
    }
  }

  public async clearFileIndex(file: string): Promise<void> {
    const normalizedPage = this.normalizePageName(file);

    // Prefix scan on [reverseKey, normalizedPage] — only this page's entries
    const allDeletes: KvKey[] = [];
    for await (const { key, value } of this.ds.query<number>({
      prefix: [reverseKey, normalizedPage],
    })) {
      const tag = key[2] as string;
      const objectId = value;

      const encoded = await this.ds.get<EncodedObject>([
        indexKey,
        tag,
        String(objectId),
      ]);
      if (encoded) {
        const tagId = this.bitmapIndex.getDictionary().tryEncode(tag);
        if (tagId !== undefined) {
          const meta = this.bitmapIndex.getTagMetaById(tagId);
          if (meta) {
            this.bitmapIndex.unindexObject(tagId, objectId, encoded, meta);
          }
        }
      }

      allDeletes.push(key);
      allDeletes.push([indexKey, tag, String(objectId)]);
    }

    if (allDeletes.length > 0) {
      await this.ds.batchDelete(allDeletes);
    }
    await this.flushBitmapState();
  }

  public async clearIndex(): Promise<void> {
    this.bitmapIndex.clear();
    const allKeys: KvKey[] = [];
    for await (const { key } of this.ds.query({ prefix: [indexKey] })) {
      allKeys.push(key);
    }
    for await (const { key } of this.ds.query({ prefix: [reverseKey] })) {
      allKeys.push(key);
    }
    for await (const { key } of this.ds.query({ prefix: ["b"] })) {
      allKeys.push(key);
    }
    for await (const { key } of this.ds.query({ prefix: ["m"] })) {
      allKeys.push(key);
    }
    for await (const { key } of this.ds.query({ prefix: ["$dict"] })) {
      allKeys.push(key);
    }
    for await (const { key } of this.ds.query({ prefix: ["$indexStats"] })) {
      allKeys.push(key);
    }
    for await (const { key } of this.ds.query({ prefix: ["$tagSketch"] })) {
      allKeys.push(key);
    }
    await this.ds.batchDelete(allKeys);
    console.log("Deleted", allKeys.length, "keys from the index");
  }

  async ensureFullIndex(space: Space) {
    const currentIndexVersion = await this.getCurrentIndexVersion();

    if (
      currentIndexVersion === undefined ||
      currentIndexVersion === null ||
      currentIndexVersion < desiredIndexVersion
    ) {
      if (await this.mq.isQueueEmpty("indexQueue")) {
        console.info(
          "[index]",
          "Performing a full space reindex, this could take a while...",
          currentIndexVersion,
          desiredIndexVersion,
        );
        await this.reindexSpace(space);
      } else {
        console.info(
          "[index]",
          "Index incomplete, waiting for existing index queue work to settle before full reindex",
        );
      }
    }
  }

  async reindexSpace(space: Space) {
    await this.markFullIndexInComplete();

    await this.mq.awaitEmptyQueue("indexQueue");

    console.log("Clearing page index...");
    await this.clearIndex();

    const files = await space.deduplicatedFileList();

    console.log("Queing", files.length, "pages to be indexed.");
    const startTime = Date.now();
    await this.mq.batchSend(
      "indexQueue",
      files.map((file) => file.name),
    );
    await this.mq.awaitEmptyQueue("indexQueue");

    // Recompute NDV for all tags after full reindex to correct any drift
    this.bitmapIndex.recomputeAllNDV();
    await this.flushBitmapState();

    await this.markFullIndexComplete();
    void this.eventHook.dispatchEvent("editor:reloadState");
    console.log("Full index completed after", Date.now() - startTime, "ms");
  }

  public async hasFullIndexCompleted() {
    return (await this.ds.get(indexVersionKey)) >= desiredIndexVersion;
  }

  private getCurrentIndexVersion() {
    return this.ds.get(indexVersionKey);
  }

  async loadPersistedBitmapState(): Promise<void> {
    this.bitmapIndex.clear();

    const dictSnapshot = await this.ds.get(["$dict"]);
    if (dictSnapshot) {
      this.bitmapIndex.loadDictionary(dictSnapshot);
    }

    for await (const { key, value } of this.ds.query({
      prefix: ["m"],
    })) {
      const tagId = Number(key[1]);
      if (Number.isFinite(tagId)) {
        this.bitmapIndex.loadTagMeta(tagId, value as any);
      }
    }

    for await (const { key, value } of this.ds.query<Uint8Array>({
      prefix: ["b"],
    })) {
      const tagId = Number(key[1]);
      const column = String(key[2]);
      const valueId = Number(key[3]);
      if (
        Number.isFinite(tagId) &&
        Number.isFinite(valueId) &&
        value instanceof Uint8Array
      ) {
        this.bitmapIndex.loadBitmap(tagId, column, valueId, value);
      }
    }
  }

  async awaitIndexQueueDrain(): Promise<void> {
    await this.mq.awaitEmptyQueue("indexQueue");
  }

  async markFullIndexComplete() {
    await this.ds.set(indexVersionKey, desiredIndexVersion);
  }

  async markFullIndexInComplete() {
    await this.ds.delete(indexVersionKey);
  }

  private normalizePageName(page: string): string {
    return page.endsWith(".md") ? page.replace(/\.md$/, "") : page;
  }

  cleanKey(ref: string, page: string) {
    if (ref.startsWith(`${page}@`)) {
      return ref.substring(page.length + 1);
    } else {
      return ref;
    }
  }

  private async isBitmapPushdownTrusted(
    tagName: string,
    column: string,
  ): Promise<boolean> {
    const tagId = this.bitmapIndex.getDictionary().tryEncode(tagName);
    if (tagId === undefined) {
      return false;
    }

    const meta = this.bitmapIndex.getTagMetaById(tagId);
    if (!meta) {
      return false;
    }

    const columnMeta = meta.columns[column];
    if (!columnMeta?.indexed) {
      return false;
    }

    if (!(await this.hasFullIndexCompleted())) {
      return false;
    }
    if (!(await this.mq.isQueueEmpty("indexQueue"))) {
      return false;
    }

    return true;
  }

  async isTagIndexTrusted(tagName: string): Promise<boolean> {
    const tagId = this.bitmapIndex.getDictionary().tryEncode(tagName);
    if (tagId === undefined) {
      return false;
    }

    if (!(await this.hasFullIndexCompleted())) {
      return false;
    }
    if (!(await this.mq.isQueueEmpty("indexQueue"))) {
      return false;
    }

    return this.bitmapIndex.getTagMetaById(tagId) !== undefined;
  }

  private async loadObjectsByObjectIds(
    tagName: string,
    objectIds: number[],
  ): Promise<ObjectValue<any>[]> {
    if (objectIds.length === 0) {
      return [];
    }

    // Batch-read all encoded objects in one call
    const keys: KvKey[] = objectIds.map((id) => [
      indexKey,
      tagName,
      String(id),
    ]);
    const encodedObjects = await this.ds.batchGet<EncodedObject>(keys);

    const results: ObjectValue<any>[] = [];
    for (const encoded of encodedObjects) {
      if (!encoded) continue;
      const decoded = this.bitmapIndex.decodeObject(
        encoded,
      ) as ObjectValue<any>;
      results.push(this.enrichValue(tagName, decoded));
    }

    return results;
  }

  private async bitmapMatchObjectIds(
    tagName: string,
    predicate: BitmapPredicate,
  ): Promise<number[] | undefined> {
    const dict = this.bitmapIndex.getDictionary();
    const tagId = dict.tryEncode(tagName);
    if (tagId === undefined) {
      return [];
    }

    if (!(await this.isBitmapPushdownTrusted(tagName, predicate.column))) {
      return undefined;
    }

    const valueId = dict.tryEncode(predicate.value);

    if (predicate.kind === "eq") {
      if (valueId === undefined) {
        return undefined;
      }

      const bm = this.bitmapIndex.getBitmap(tagId, predicate.column, valueId);
      if (!bm) {
        return undefined;
      }

      return [...bm.toArray()].sort((a, b) => a - b);
    }

    if (valueId === undefined) {
      return undefined;
    }

    const bitmaps = this.bitmapIndex.getColumnBitmaps(tagId, predicate.column);
    if (bitmaps.length === 0) {
      return undefined;
    }

    const excluded = this.bitmapIndex.getBitmap(
      tagId,
      predicate.column,
      valueId,
    );
    const ids = new Set<number>();
    for (const bm of bitmaps) {
      for (const id of bm.toArray()) {
        ids.add(id);
      }
    }
    if (excluded) {
      for (const id of excluded.toArray()) {
        ids.delete(id);
      }
    }
    return [...ids].sort((a, b) => a - b);
  }

  private async processObjectsToKVs<T>(
    page: string,
    objects: ObjectValue<T>[],
    throwOnValidationErrors: boolean,
  ): Promise<KV<T>[]> {
    const kvs: KV<T>[] = [];
    const tagDefinitions: Record<string, TagDefinition> = this.config.get(
      "tags",
      {},
    );
    while (objects.length > 0) {
      const obj = objects.shift()!;
      if (!obj.tag) {
        console.error("Object has no tag", obj, "this shouldn't happen");
        continue;
      }
      const allTags = [obj.tag, ...(obj.tags || [])];
      for (const tag of allTags) {
        const tagDefinition = tagDefinitions[tag];
        if (
          tagDefinition?.schema &&
          (tagDefinition?.mustValidate || throwOnValidationErrors)
        ) {
          const validationError = validateObject(tagDefinition?.schema, obj);
          if (validationError) {
            if (!throwOnValidationErrors) {
              console.warn(
                `Object failed ${tag} validation so won't be indexed:`,
                obj,
                "Validation error:",
                validationError,
              );
              continue;
            } else {
              throw new ObjectValidationError(validationError, obj);
            }
          }
        }
        if (
          tagDefinition?.validate &&
          (tagDefinition?.mustValidate || throwOnValidationErrors)
        ) {
          const validationError = await tagDefinition.validate(obj);
          if (validationError) {
            if (!throwOnValidationErrors) {
              console.warn(
                `Object failed ${tag} validation so won't be indexed:`,
                obj,
                "Validation error:",
                validationError,
              );
              continue;
            } else {
              throw new ObjectValidationError(validationError, obj);
            }
          }
        }
        if (tagDefinition?.transform) {
          let newObjects;
          try {
            newObjects = await tagDefinition.transform(obj);
          } catch (e: any) {
            throw new ObjectValidationError(e.message, obj);
          }

          if (!newObjects) {
            kvs.push({
              key: [tag, this.cleanKey(obj.ref, page)],
              value: obj,
            });
            continue;
          }

          if (!Array.isArray(newObjects)) {
            newObjects = [newObjects];
          }
          let foundAssignedRef = false;
          for (const newObj of newObjects) {
            if (!newObj.ref) {
              console.error(
                "transform result object did not contain ref",
                newObj,
              );
              continue;
            }
            if (newObj.ref === obj.ref) {
              kvs.push({
                key: [tag, this.cleanKey(newObj.ref, page)],
                value: newObj,
              });
              foundAssignedRef = true;
            } else {
              objects.push(newObj);
            }
          }
          if (!foundAssignedRef && newObjects.length) {
            throw new Error(
              `transform() result objects for ${tag} did not contain result with original ref.`,
            );
          }
        } else {
          kvs.push({
            key: [tag, this.cleanKey(obj.ref, page)],
            value: obj,
          });
        }
      }
    }
    return kvs;
  }
}
