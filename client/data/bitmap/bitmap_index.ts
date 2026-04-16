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
import { Dictionary, type DictionarySnapshot } from "./dictionary.ts";
import type { KV, KvKey } from "../../../plug-api/types/datastore.ts";

// Storage key prefixes
const BITMAP_PREFIX = "b";
const META_PREFIX = "m";
const OBJECT_PREFIX = "o";
const DICT_KEY: KvKey = ["$dict"];

// Internal metadata field — uses $ prefix to avoid collisions with user data
const ENC_FIELD = "$enc";

// Add after existing constants (around line 28):
const DEFAULT_MCV_TOP_K = 10;

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
 * The `$enc` field lists which fields were dictionary-encoded,
 * so we know what to decode on read.
 */
export type EncodedObject = {
  $enc: string[];
  [key: string]: unknown;
};

// Two-level bitmap cache: tagId → column → valueId → RoaringBitmap

type ColumnBitmaps = Map<number, RoaringBitmap>;
type TagBitmaps = Map<string, ColumnBitmaps>;

// BitmapIndex

export class BitmapIndex {
  private dict: Dictionary;
  private config: BitmapIndexConfig;
  // In-memory cache of tag metadata
  private metaCache: Map<number, TagMeta> = new Map();
  // Two-level bitmap cache: tagId → (column → (valueId → bitmap))
  private bitmapsByTag: Map<number, TagBitmaps> = new Map();
  // Track dirty bitmaps for batch flush: "tagId\0column\0valueId"
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

  // Bitmap cache helpers

  private getOrCreateTagBitmaps(tagId: number): TagBitmaps {
    let tag = this.bitmapsByTag.get(tagId);
    if (!tag) {
      tag = new Map();
      this.bitmapsByTag.set(tagId, tag);
    }
    return tag;
  }

  private getOrCreateColumnBitmaps(
    tagBitmaps: TagBitmaps,
    column: string,
  ): ColumnBitmaps {
    let col = tagBitmaps.get(column);
    if (!col) {
      col = new Map();
      tagBitmaps.set(column, col);
    }
    return col;
  }

  private dirtyKey(tagId: number, column: string, valueId: number): string {
    return `${tagId}\0${column}\0${valueId}`;
  }

  // Encoding

  /**
   * Encode an object: replace short-string fields with dictionary IDs.
   * Returns the encoded object and the list of encoded field names.
   * Uses a single canonicalize call per value via Dictionary.encodeIfFits.
   */
  encodeObject(obj: Record<string, unknown>): EncodedObject {
    const encoded: EncodedObject = { $enc: [] };
    const encFields: string[] = [];
    const maxBytes = this.config.maxValueBytes;
    const maxSize = this.config.maxDictionarySize;

    for (const [key, value] of Object.entries(obj)) {
      if (key === ENC_FIELD) continue;

      if (Array.isArray(value)) {
        const encodedArr: unknown[] = [];
        let anyEncoded = false;
        for (const elem of value) {
          const id = this.dict.encodeIfFits(elem, maxBytes, maxSize);
          if (id !== undefined) {
            encodedArr.push(id);
            anyEncoded = true;
          } else {
            encodedArr.push(elem);
          }
        }
        encoded[key] = anyEncoded ? encodedArr : value;
        if (anyEncoded) encFields.push(key);
      } else {
        const id = this.dict.encodeIfFits(value, maxBytes, maxSize);
        if (id !== undefined) {
          encoded[key] = id;
          encFields.push(key);
        } else {
          encoded[key] = value;
        }
      }
    }

    encoded.$enc = encFields;
    return encoded;
  }

  /**
   * Decode an encoded object back to the original form.
   */
  decodeObject(encoded: EncodedObject): Record<string, unknown> {
    const result: Record<string, unknown> = {};
    const encFields = new Set(encoded.$enc);

    for (const [key, value] of Object.entries(encoded)) {
      if (key === ENC_FIELD) continue;

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
    if (!colMeta) return true;

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
    const tagBitmaps = this.getOrCreateTagBitmaps(tagId);
    const colBitmaps = this.getOrCreateColumnBitmaps(tagBitmaps, column);
    let bm = colBitmaps.get(valueId);
    if (!bm) {
      bm = new RoaringBitmap();
      colBitmaps.set(valueId, bm);
    }
    bm.add(objectId);
    this.dirtyBitmaps.add(this.dirtyKey(tagId, column, valueId));
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
    const colBitmaps = this.bitmapsByTag.get(tagId)?.get(column);
    if (!colBitmaps) return;
    const bm = colBitmaps.get(valueId);
    if (!bm) return;
    bm.remove(objectId);
    this.dirtyBitmaps.add(this.dirtyKey(tagId, column, valueId));
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
    return this.bitmapsByTag.get(tagId)?.get(column)?.get(valueId);
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
    const encodedFields = new Set(encoded.$enc);

    for (const [key, value] of Object.entries(encoded)) {
      if (key === ENC_FIELD) continue;
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
    const encodedFields = new Set(encoded.$enc);

    for (const [key, value] of Object.entries(encoded)) {
      if (key === ENC_FIELD) continue;
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
    for (const col of Object.keys(meta.columns)) {
      meta.columns[col].ndv = 0;
    }

    const tagBitmaps = this.bitmapsByTag.get(tagId);
    if (tagBitmaps) {
      for (const [column, colBitmaps] of tagBitmaps) {
        if (!meta.columns[column]) continue;
        let count = 0;
        for (const bm of colBitmaps.values()) {
          if (!bm.isEmpty()) count++;
        }
        meta.columns[column].ndv = count;
      }
    }

    for (const [col, colMeta] of Object.entries(meta.columns)) {
      colMeta.indexed = this.shouldIndexColumn(col, meta);
    }

    this.dirtyMeta.add(tagId);
  }

  /**
   * Recompute NDV for all known tags.
   */
  recomputeAllNDV(): void {
    for (const [tagId, meta] of this.metaCache) {
      this.recomputeNDV(tagId, meta);
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
   */
  getColumnMCV(
    tagId: number,
    column: string,
    topK: number = DEFAULT_MCV_TOP_K,
  ): { value: string; count: number }[] {
    const colBitmaps = this.bitmapsByTag.get(tagId)?.get(column);
    if (!colBitmaps) return [];

    const entries: { valueId: number; count: number }[] = [];
    for (const [valueId, bm] of colBitmaps) {
      if (!bm.isEmpty()) {
        entries.push({ valueId, count: bm.cardinality() });
      }
    }

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

    for (const dirtyStr of this.dirtyBitmaps) {
      const parts = dirtyStr.split("\0");
      const tagId = parseInt(parts[0], 10);
      const column = parts[1];
      const valueId = parseInt(parts[2], 10);
      const storageKey = this.bitmapStorageKey(tagId, column, valueId);
      const bm = this.bitmapsByTag.get(tagId)?.get(column)?.get(valueId);

      if (!bm || bm.isEmpty()) {
        deletes.push(storageKey);
        this.bitmapsByTag.get(tagId)?.get(column)?.delete(valueId);
      } else {
        writes.push({ key: storageKey, value: bm.serialize() });
      }
    }
    this.dirtyBitmaps.clear();

    for (const tagId of this.dirtyMeta) {
      const meta = this.metaCache.get(tagId);
      if (meta) {
        writes.push({ key: this.metaStorageKey(tagId), value: meta });
      }
    }
    this.dirtyMeta.clear();

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
    const tagBitmaps = this.getOrCreateTagBitmaps(tagId);
    const colBitmaps = this.getOrCreateColumnBitmaps(tagBitmaps, column);
    colBitmaps.set(valueId, RoaringBitmap.deserialize(data));
  }

  /**
   * Clear all in-memory state.
   */
  clear(): void {
    this.dict = new Dictionary();
    this.metaCache.clear();
    this.bitmapsByTag.clear();
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
   */
  getColumnBitmaps(tagId: number, column: string): RoaringBitmap[] {
    const colBitmaps = this.bitmapsByTag.get(tagId)?.get(column);
    if (!colBitmaps) return [];
    const results: RoaringBitmap[] = [];
    for (const bm of colBitmaps.values()) {
      if (!bm.isEmpty()) {
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
    const tagBitmaps = this.getOrCreateTagBitmaps(tagId);
    const colBitmaps = this.getOrCreateColumnBitmaps(tagBitmaps, column);
    let bm = colBitmaps.get(valueId);
    const wasEmpty = !bm || bm.isEmpty();
    if (!bm) {
      bm = new RoaringBitmap();
      colBitmaps.set(valueId, bm);
    }
    bm.add(objectId);
    this.dirtyBitmaps.add(this.dirtyKey(tagId, column, valueId));
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
    const colBitmaps = this.bitmapsByTag.get(tagId)?.get(column);
    if (!colBitmaps) return false;
    const bm = colBitmaps.get(valueId);
    if (!bm) return false;
    bm.remove(objectId);
    this.dirtyBitmaps.add(this.dirtyKey(tagId, column, valueId));
    return bm.isEmpty();
  }
}
