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
import {
  MCVList,
} from "../space_lua/mcv.ts";

// KV key prefixes
const indexKey = "idx";
const reverseKey = "ridx";

const indexVersionKey = ["$indexVersion"];

// Bump this every time a full reindex is needed
const desiredIndexVersion = 12;

type TagDefinition = {
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

  // --- Enricher: applies metatables to query results ---

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

  // --- Query collections ---

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
      ): Promise<any[]> {
        // Load all objects for this tag via KV prefix scan, decode, enrich
        const results: any[] = [];
        for await (const { value } of self.ds.query({
          prefix: [indexKey, tagName],
        })) {
          const decoded = self.bitmapIndex.decodeObject(value as EncodedObject);
          results.push(self.enrichValue(tagName, decoded));
        }
        return applyQuery(results, query, env, sf, config);
      },

      async getStats(): Promise<CollectionStats> {
        const tagId = self.bitmapIndex.getDictionary().tryEncode(tagName);
        if (tagId === undefined) {
          return { rowCount: 0, ndv: new Map(), avgColumnCount: 0 };
        }
        const rowCount = self.bitmapIndex.getRowCount(tagId);
        const meta = self.bitmapIndex.getTagMetaById(tagId);
        const ndv = new Map<string, number>();
        const mcv = new Map<string, MCVList>();

        if (meta) {
          for (const [col, colMeta] of Object.entries(meta.columns)) {
            ndv.set(col, colMeta.ndv);

            // Build MCVList from exact bitmap cardinalities
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
          avgColumnCount: ndv.size,
          mcv: mcv.size > 0 ? mcv : undefined,
          statsSource: indexComplete ? "persisted" : "partial",
        };
      },
    };
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

  // --- Object CRUD ---

  async getObjectByRef(
    page: string,
    tag: string,
    ref: string,
  ): Promise<any | null> {
    const refKey = this.cleanKey(ref, page);
    const objectId = await this.ds.get<number>([reverseKey, tag, refKey, page]);
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
    const objectId = await this.ds.get<number>([reverseKey, tag, refKey, page]);
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
      [reverseKey, tag, refKey, page],
    ]);
    await this.flushBitmapState();
  }

  // --- Indexing ---

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
    // Use the tag() collection's query method
    return this.tag(tag).query(query, env, sf) as Promise<ObjectValue<T>[]>;
  }

  // --- Batch write: encode objects, store, update bitmaps ---

  private async batchSet(page: string, kvs: KV[]): Promise<void> {
    const writes: KV[] = [];
    const deletes: KvKey[] = [];

    for (const { key, value } of kvs) {
      const tag = key[0] as string;
      const refKey = key[1] as string;

      // Check if this ref already exists (re-index case)
      const existingObjectId = await this.ds.get<number>([
        reverseKey,
        tag,
        refKey,
        page,
      ]);

      if (existingObjectId !== null && existingObjectId !== undefined) {
        // Remove old object from bitmap index
        const oldEncoded = await this.ds.get<EncodedObject>([
          indexKey,
          tag,
          String(existingObjectId),
        ]);
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

      // Encode and store new object
      const encoded = this.bitmapIndex.encodeObject(
        value as Record<string, unknown>,
      );
      const { tagId, meta } = this.bitmapIndex.getTagMeta(tag);
      const objectId =
        existingObjectId ?? this.bitmapIndex.allocateObjectId(tagId);

      // If reusing an existing objectId, don't increment count (unindex decremented it)
      if (existingObjectId !== null && existingObjectId !== undefined) {
        meta.count++;
        // dirtyMeta already set by unindexObject
      }

      this.bitmapIndex.indexObject(tagId, objectId, encoded, meta);

      writes.push({
        key: [indexKey, tag, String(objectId)],
        value: encoded,
      });
      writes.push({
        key: [reverseKey, tag, refKey, page],
        value: objectId,
      });
    }

    // Flush bitmap state to KV writes
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

  // --- File clearing ---

  public async clearFileIndex(file: string): Promise<void> {
    const normalizedPage = this.normalizePageName(file);

    // Find all reverse-index entries for this page
    const allDeletes: KvKey[] = [];
    for await (const { key, value } of this.ds.query<number>({
      prefix: [reverseKey],
    })) {
      // key: [reverseKey, tag, refKey, page]
      if (key[3] === normalizedPage) {
        const tag = key[1] as string;
        const objectId = value;

        // Remove from bitmap index
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

        allDeletes.push(key); // reverse key
        allDeletes.push([indexKey, tag, String(objectId)]); // object key
      }
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
    // Clean up bitmap storage keys
    for await (const { key } of this.ds.query({ prefix: ["b"] })) {
      allKeys.push(key);
    }
    for await (const { key } of this.ds.query({ prefix: ["m"] })) {
      allKeys.push(key);
    }
    for await (const { key } of this.ds.query({ prefix: ["$dict"] })) {
      allKeys.push(key);
    }
    // Clean up legacy keys from old index format
    for await (const { key } of this.ds.query({ prefix: ["$indexStats"] })) {
      allKeys.push(key);
    }
    for await (const { key } of this.ds.query({ prefix: ["$tagSketch"] })) {
      allKeys.push(key);
    }
    await this.ds.batchDelete(allKeys);
    console.log("Deleted", allKeys.length, "keys from the index");
  }

  // --- Reindex ---

  async ensureFullIndex(space: Space) {
    const currentIndexVersion = await this.getCurrentIndexVersion();

    if (!currentIndexVersion) {
      console.log("No index version found, assuming fresh install");
      return;
    }

    if (
      currentIndexVersion < desiredIndexVersion &&
      (await this.mq.isQueueEmpty("indexQueue"))
    ) {
      console.info(
        "[index]",
        "Performing a full space reindex, this could take a while...",
        currentIndexVersion,
        desiredIndexVersion,
      );
      await this.reindexSpace(space);
      void this.eventHook.dispatchEvent("editor:reloadState");
    }
  }

  async reindexSpace(space: Space) {
    console.log("Clearing page index...");
    await this.clearIndex();
    await this.markFullIndexInComplete();

    const files = await space.deduplicatedFileList();

    console.log("Queing", files.length, "pages to be indexed.");
    const startTime = Date.now();
    await this.mq.batchSend(
      "indexQueue",
      files.map((file) => file.name),
    );
    await this.mq.awaitEmptyQueue("indexQueue");
    await this.markFullIndexComplete();
    console.log("Full index completed after", Date.now() - startTime, "ms");
  }

  public async hasFullIndexCompleted() {
    return (await this.ds.get(indexVersionKey)) >= desiredIndexVersion;
  }

  private getCurrentIndexVersion() {
    return this.ds.get(indexVersionKey);
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

  // --- Helpers ---

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

  // --- Validation / transform pipeline (unchanged logic) ---

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
