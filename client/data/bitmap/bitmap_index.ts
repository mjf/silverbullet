/**
 * Manages per-tag, per-column bitmap indices backed by RoaringBitmaps.
 * Uses the Dictionary for value to ID mapping.
 *
 * Storage keys (in the underlying KV store):
 *
 * - `b\0{tagId}\0{columnName}\0{valueId}` -> serialized `RoaringBitmap`
 * - `m\0{tagId}` -> `TagMeta`
 * - `$dict` -> `DictionarySnapshot`
 *
 * Object keys remain per-tag:
 *
 * - `o\0{tagId}\0{objectId}` -> encoded object
 */

import { RoaringBitmap } from "./roaring_bitmap.ts";
import {
  Dictionary,
  canonicalize,
  type DictionarySnapshot,
} from "./dictionary.ts";
import type { KV, KvKey } from "../../../plug-api/types/datastore.ts";

// Storage key prefixes
const BITMAP_PREFIX = "b";
const META_PREFIX = "m";
const OBJECT_PREFIX = "o";
const DICT_KEY: KvKey = ["$dict"];

// Configuration

export type BitmapIndexConfig = {
  /** Max selectivity (NDV/rowCount) for bitmap indexing. Default: 0.5 */
  maxSelectivity: number;
  /** Min rows before bitmap indices activate. Default: 50 */
  minRowsForIndex: number;
  /** Max encoded value length in bytes. Default: 256 */
  maxValueBytes: number;
  /** Max dictionary entries (safety cap). Default: 100000 */
  maxDictionarySize: number;
  /** Max bitmap keys per column (safety cap). Default: 10000 */
  maxBitmapsPerColumn: number;
  /** Columns to always index regardless of selectivity. Default: ["page", "tag"] */
  alwaysIndexColumns: string[];
};

const DEFAULT_CONFIG: BitmapIndexConfig = {
  maxSelectivity: 0.5,
  minRowsForIndex: 50,
  maxValueBytes: 256,
  maxDictionarySize: 100000,
  maxBitmapsPerColumn: 10000,
  alwaysIndexColumns: ["page", "tag"],
};

// Tag metadata

export type ColumnMeta = {
  ndv: number;
  indexed: boolean;
};

export type TagMeta = {
  count: number;
  nextObjectId: number;
  totalColumnCount: number;
  columns: Record<string, ColumnMeta>;
};

function emptyTagMeta(): TagMeta {
  return { count: 0, nextObjectId: 0, totalColumnCount: 0, columns: {} };
}

// Encoded object

/**
 * An encoded object has dictionary IDs.
 *
 * Non-encoded fields (long strings, numbers, booleans, arrays, objects)
 * are stored as-is.
 *
 * The `_enc` field lists which fields were dictionary-encoded,
 * so we know what to decode on read.
 */
export type EncodedObject = {
  _enc: string[];
  [key: string]: unknown;
};

// BitmapIndex

export class BitmapIndex {
  private dict: Dictionary;
  private config: BitmapIndexConfig;
  // In-memory cache of tag metadata
  private metaCache: Map<number, TagMeta> = new Map();
  // In-memory cache of bitmaps: `${tagId}\0${column}\0${valueId}` -> bitmap
  private bitmapCache: Map<string, RoaringBitmap> = new Map();
  // Track dirty bitmaps for batch flush
  private dirtyBitmaps: Set<string> = new Set();
  private dirtyMeta: Set<number> = new Set();

  constructor(config?: Partial<BitmapIndexConfig>, dict?: Dictionary) {
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.dict = dict ?? new Dictionary();
  }

  getDictionary(): Dictionary {
    return this.dict;
  }

  getConfig(): BitmapIndexConfig {
    return this.config;
  }

  // Encoding

  /**
   * Determine if a field value should be dictionary-encoded.
   */
  shouldEncode(value: unknown): boolean {
    if (value === null || value === undefined) return false;
    if (this.dict.size >= this.config.maxDictionarySize) return false;
    const canonical = canonicalize(value);
    // Already in dictionary — always encode
    if (this.dict.tryEncode(value) !== undefined) return true;
    // Check byte length
    return canonical.length <= this.config.maxValueBytes;
  }

  /**
   * Encode an object: replace short-string fields with dictionary IDs.
   * Returns the encoded object and the list of encoded field names.
   */
  encodeObject(obj: Record<string, unknown>): EncodedObject {
    const encoded: EncodedObject = { _enc: [] };
    const encFields: string[] = [];

    for (const [key, value] of Object.entries(obj)) {
      if (key === "_enc") continue; // reserved

      if (Array.isArray(value)) {
        // Encode array elements individually
        const encodedArr: unknown[] = [];
        let anyEncoded = false;
        for (const elem of value) {
          if (this.shouldEncode(elem)) {
            encodedArr.push(this.dict.encode(elem));
            anyEncoded = true;
          } else {
            encodedArr.push(elem);
          }
        }
        encoded[key] = anyEncoded ? encodedArr : value;
        if (anyEncoded) encFields.push(key);
      } else if (this.shouldEncode(value)) {
        encoded[key] = this.dict.encode(value);
        encFields.push(key);
      } else {
        encoded[key] = value;
      }
    }

    encoded._enc = encFields;
    return encoded;
  }

  /**
   * Decode an encoded object back to the original form.
   */
  decodeObject(encoded: EncodedObject): Record<string, unknown> {
    const result: Record<string, unknown> = {};
    const encFields = new Set(encoded._enc);

    for (const [key, value] of Object.entries(encoded)) {
      if (key === "_enc") continue;

      if (encFields.has(key)) {
        if (Array.isArray(value)) {
          result[key] = value.map((v) =>
            typeof v === "number" ? this.dict.decodeValue(v) : v,
          );
        } else if (typeof value === "number") {
          result[key] = this.dict.decodeValue(value);
        } else {
          result[key] = value;
        }
      } else {
        result[key] = value;
      }
    }

    return result;
  }

  // Bitmap operations

  private bitmapCacheKey(
    tagId: number,
    column: string,
    valueId: number,
  ): string {
    return `${tagId}\0${column}\0${valueId}`;
  }

  private bitmapStorageKey(
    tagId: number,
    column: string,
    valueId: number,
  ): KvKey {
    return [BITMAP_PREFIX, String(tagId), column, String(valueId)];
  }

  private metaStorageKey(tagId: number): KvKey {
    return [META_PREFIX, String(tagId)];
  }

  objectStorageKey(tagId: number, objectId: number): KvKey {
    return [OBJECT_PREFIX, String(tagId), String(objectId)];
  }

  /**
   * Check if a column should have bitmap indexing for a given tag.
   */
  shouldIndexColumn(column: string, tagMeta: TagMeta): boolean {
    if (this.config.alwaysIndexColumns.includes(column)) return true;
    if (tagMeta.count < this.config.minRowsForIndex) return false;

    const colMeta = tagMeta.columns[column];
    if (!colMeta) return true; // new column, index by default

    if (colMeta.ndv > this.config.maxBitmapsPerColumn) return false;
    if (
      tagMeta.count > 0 &&
      colMeta.ndv / tagMeta.count > this.config.maxSelectivity
    ) {
      return false;
    }
    return true;
  }

  /**
   * Get or create tag metadata.
   */
  getTagMeta(tag: string): { tagId: number; meta: TagMeta } {
    const tagId = this.dict.encode(tag);
    let meta = this.metaCache.get(tagId);
    if (!meta) {
      meta = emptyTagMeta();
      this.metaCache.set(tagId, meta);
    }
    return { tagId, meta };
  }

  /**
   * Allocate a new object ID for a tag.
   */
  allocateObjectId(tagId: number): number {
    const meta = this.metaCache.get(tagId)!;
    const id = meta.nextObjectId;
    meta.nextObjectId++;
    meta.count++;
    this.dirtyMeta.add(tagId);
    return id;
  }

  /**
   * Set a bit in a bitmap index.
   */
  setBit(
    tagId: number,
    column: string,
    valueId: number,
    objectId: number,
  ): void {
    const cacheKey = this.bitmapCacheKey(tagId, column, valueId);
    let bm = this.bitmapCache.get(cacheKey);
    if (!bm) {
      bm = new RoaringBitmap();
      this.bitmapCache.set(cacheKey, bm);
    }
    bm.add(objectId);
    this.dirtyBitmaps.add(cacheKey);
  }

  /**
   * Clear a bit from a bitmap index.
   */
  clearBit(
    tagId: number,
    column: string,
    valueId: number,
    objectId: number,
  ): void {
    const cacheKey = this.bitmapCacheKey(tagId, column, valueId);
    const bm = this.bitmapCache.get(cacheKey);
    if (!bm) return;
    bm.remove(objectId);
    this.dirtyBitmaps.add(cacheKey);
  }

  /**
   * Get bitmap for a specific tag/column/value.
   * Returns undefined if no bitmap exists.
   */
  getBitmap(
    tagId: number,
    column: string,
    valueId: number,
  ): RoaringBitmap | undefined {
    return this.bitmapCache.get(this.bitmapCacheKey(tagId, column, valueId));
  }

  /**
   * Index an encoded object: set bits in all relevant column bitmaps.
   */
  indexObject(
    tagId: number,
    objectId: number,
    encoded: EncodedObject,
    meta: TagMeta,
  ): void {
    let objectColumnCount = 0;
    const encodedFields = new Set(encoded._enc);

    for (const [key, value] of Object.entries(encoded)) {
      if (key === "_enc") continue;
      objectColumnCount++;
      if (!this.shouldIndexColumn(key, meta)) continue;

      if (!meta.columns[key]) {
        meta.columns[key] = { ndv: 0, indexed: true };
      }

      const isEncodedField = encodedFields.has(key);

      if (Array.isArray(value)) {
        for (const elem of value) {
          const valueId =
            typeof elem === "number" && isEncodedField
              ? elem
              : this.dict.encode(elem);
          const wasEmpty = this.setBitAndCheck(tagId, key, valueId, objectId);
          if (wasEmpty) meta.columns[key].ndv++;
        }
      } else {
        const valueId =
          typeof value === "number" && isEncodedField
            ? value
            : this.dict.encode(value);
        const wasEmpty = this.setBitAndCheck(tagId, key, valueId, objectId);
        if (wasEmpty) meta.columns[key].ndv++;
      }
    }

    meta.totalColumnCount += objectColumnCount;
    this.dirtyMeta.add(tagId);
  }

  /**
   * Remove an object from all bitmap indices.
   */
  unindexObject(
    tagId: number,
    objectId: number,
    encoded: EncodedObject,
    meta: TagMeta,
  ): void {
    let objectColumnCount = 0;
    const encodedFields = new Set(encoded._enc);

    for (const [key, value] of Object.entries(encoded)) {
      if (key === "_enc") continue;
      objectColumnCount++;
      if (!meta.columns[key]?.indexed) continue;

      const isEncodedField = encodedFields.has(key);

      if (Array.isArray(value)) {
        for (const elem of value) {
          const valueId =
            typeof elem === "number" && isEncodedField
              ? elem
              : this.dict.tryEncode(elem);
          if (valueId !== undefined) {
            const nowEmpty = this.clearBitAndCheck(
              tagId,
              key,
              valueId,
              objectId,
            );
            if (nowEmpty && meta.columns[key]) {
              meta.columns[key].ndv = Math.max(0, meta.columns[key].ndv - 1);
            }
          }
        }
      } else {
        const valueId =
          typeof value === "number" && isEncodedField
            ? value
            : this.dict.tryEncode(value);
        if (valueId !== undefined) {
          const nowEmpty = this.clearBitAndCheck(tagId, key, valueId, objectId);
          if (nowEmpty && meta.columns[key]) {
            meta.columns[key].ndv = Math.max(0, meta.columns[key].ndv - 1);
          }
        }
      }
    }

    meta.totalColumnCount = Math.max(
      0,
      meta.totalColumnCount - objectColumnCount,
    );
    meta.count = Math.max(0, meta.count - 1);
    this.dirtyMeta.add(tagId);
  }

  /**
   * Recompute NDV for all columns of a tag from the bitmap cache.
   */
  recomputeNDV(tagId: number, meta: TagMeta): void {
    // Reset all NDVs
    for (const col of Object.keys(meta.columns)) {
      meta.columns[col].ndv = 0;
    }

    // Count non-empty bitmaps per column
    const prefix = `${tagId}\0`;
    for (const [cacheKey, bm] of this.bitmapCache) {
      if (!cacheKey.startsWith(prefix)) continue;
      if (bm.isEmpty()) continue;
      // Parse column name from cache key: "tagId\0column\0valueId"
      const parts = cacheKey.split("\0");
      const column = parts[1];
      if (meta.columns[column]) {
        meta.columns[column].ndv++;
      }
    }

    // Update indexed status based on thresholds
    for (const [col, colMeta] of Object.entries(meta.columns)) {
      colMeta.indexed = this.shouldIndexColumn(col, meta);
    }
  }

  // Stats (derived from bitmaps — exact, no sketches)

  /**
   * Get exact NDV for a column.
   */
  getColumnNDV(tagId: number, column: string): number {
    return this.metaCache.get(tagId)?.columns[column]?.ndv ?? 0;
  }

  /**
   * Compute Most Common Values for a column from bitmap cardinalities.
   * Returns the top-k (value, count) pairs sorted by count descending.
   * Exact — no sketches needed.
   */
  getColumnMCV(
    tagId: number,
    column: string,
    topK: number = 10,
  ): { value: string; count: number }[] {
    const prefix = `${tagId}\0${column}\0`;
    const entries: { valueId: number; count: number }[] = [];

    for (const [cacheKey, bm] of this.bitmapCache) {
      if (!cacheKey.startsWith(prefix) || bm.isEmpty()) continue;
      const valueId = parseInt(cacheKey.substring(prefix.length), 10);
      entries.push({ valueId, count: bm.cardinality() });
    }

    // Sort descending by count, take top-k
    entries.sort((a, b) => b.count - a.count);
    const topEntries = entries.slice(0, topK);

    return topEntries.map(({ valueId, count }) => ({
      value: String(this.dict.decodeValue(valueId) ?? valueId),
      count,
    }));
  }

  /**
   * Get row count for a tag.
   */
  getRowCount(tagId: number): number {
    return this.metaCache.get(tagId)?.count ?? 0;
  }

  // Persistence helpers

  /**
   * Collect all dirty data as KV writes for a batch flush.
   * Returns writes and deletes to apply to the KV store.
   */
  flushToKVs(): { writes: KV[]; deletes: KvKey[] } {
    const writes: KV[] = [];
    const deletes: KvKey[] = [];

    // Flush dirty bitmaps
    for (const cacheKey of this.dirtyBitmaps) {
      const parts = cacheKey.split("\0");
      const tagId = parseInt(parts[0], 10);
      const column = parts[1];
      const valueId = parseInt(parts[2], 10);
      const storageKey = this.bitmapStorageKey(tagId, column, valueId);
      const bm = this.bitmapCache.get(cacheKey);

      if (!bm || bm.isEmpty()) {
        deletes.push(storageKey);
        this.bitmapCache.delete(cacheKey);
      } else {
        writes.push({ key: storageKey, value: bm.serialize() });
      }
    }
    this.dirtyBitmaps.clear();

    // Flush dirty metadata
    for (const tagId of this.dirtyMeta) {
      const meta = this.metaCache.get(tagId);
      if (meta) {
        writes.push({ key: this.metaStorageKey(tagId), value: meta });
      }
    }
    this.dirtyMeta.clear();

    // Flush dictionary if dirty
    if (this.dict.dirty) {
      writes.push({ key: DICT_KEY, value: this.dict.toSnapshot() });
      this.dict.clearDirty();
    }

    return { writes, deletes };
  }

  /**
   * Load state from KV entries (called at startup).
   */
  loadDictionary(snapshot: DictionarySnapshot): void {
    this.dict = new Dictionary(snapshot);
  }

  loadTagMeta(tagId: number, meta: TagMeta): void {
    this.metaCache.set(tagId, meta);
  }

  loadBitmap(
    tagId: number,
    column: string,
    valueId: number,
    data: Uint8Array,
  ): void {
    const cacheKey = this.bitmapCacheKey(tagId, column, valueId);
    this.bitmapCache.set(cacheKey, RoaringBitmap.deserialize(data));
  }

  /**
   * Clear all in-memory state.
   */
  clear(): void {
    this.dict = new Dictionary();
    this.metaCache.clear();
    this.bitmapCache.clear();
    this.dirtyBitmaps.clear();
    this.dirtyMeta.clear();
  }

  /**
   * Get all tag IDs known to this index.
   */
  allTagIds(): number[] {
    return [...this.metaCache.keys()];
  }

  /**
   * Get tag metadata by tagId (for external consumers that already have the ID).
   */
  getTagMetaById(tagId: number): TagMeta | undefined {
    return this.metaCache.get(tagId);
  }

  /**
   * Get all non-empty bitmaps for a (tag, column) pair — one per distinct value.
   * Used by bitmap_query for ~= (inequality) pre-filtering.
   */
  getColumnBitmaps(tagId: number, column: string): RoaringBitmap[] {
    const prefix = `${tagId}\0${column}\0`;
    const results: RoaringBitmap[] = [];
    for (const [cacheKey, bm] of this.bitmapCache) {
      if (cacheKey.startsWith(prefix) && !bm.isEmpty()) {
        results.push(bm);
      }
    }
    return results;
  }

  /**
   * Set a bit. Returns true if the bitmap was empty before this set.
   */
  private setBitAndCheck(
    tagId: number,
    column: string,
    valueId: number,
    objectId: number,
  ): boolean {
    const cacheKey = this.bitmapCacheKey(tagId, column, valueId);
    let bm = this.bitmapCache.get(cacheKey);
    const wasEmpty = !bm || bm.isEmpty();
    if (!bm) {
      bm = new RoaringBitmap();
      this.bitmapCache.set(cacheKey, bm);
    }
    bm.add(objectId);
    this.dirtyBitmaps.add(cacheKey);
    return wasEmpty;
  }

  /**
   * Clear a bit. Returns true if the bitmap is now empty after this clear.
   */
  private clearBitAndCheck(
    tagId: number,
    column: string,
    valueId: number,
    objectId: number,
  ): boolean {
    const cacheKey = this.bitmapCacheKey(tagId, column, valueId);
    const bm = this.bitmapCache.get(cacheKey);
    if (!bm) return false;
    bm.remove(objectId);
    this.dirtyBitmaps.add(cacheKey);
    return bm.isEmpty();
  }
}
