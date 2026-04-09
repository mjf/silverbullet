/**
 * Universal value dictionary for Roaring Bitmap indexing.
 * Maps arbitrary JS values to dense 32-bit integer IDs.
 */

// Type prefix bytes prevent collisions: "true" (string) vs true (boolean)
const PREFIX_NULL = 0;
const PREFIX_BOOL = 1;
const PREFIX_NUMBER = 2;
const PREFIX_STRING = 3;
const PREFIX_COMPLEX = 4;

/**
 * Produce a deterministic string key for any JS value.
 * Values that are === equal produce identical keys.
 */
export function canonicalize(value: unknown): string {
  if (value === null || value === undefined) {
    return String.fromCharCode(PREFIX_NULL);
  }
  if (typeof value === "boolean") {
    return String.fromCharCode(PREFIX_BOOL) + (value ? "1" : "0");
  }
  if (typeof value === "number") {
    return String.fromCharCode(PREFIX_NUMBER) + numToKey(value);
  }
  if (typeof value === "string") {
    return String.fromCharCode(PREFIX_STRING) + value;
  }
  // Arrays, objects — sorted-key JSON for determinism
  return String.fromCharCode(PREFIX_COMPLEX) + stableStringify(value);
}

/**
 * Deterministic number encoding that preserves sort order for common cases.
 * Uses IEEE 754 hex for exact roundtrip; special-cases NaN/±Infinity.
 */
function numToKey(n: number): string {
  if (Object.is(n, -0)) return "-0";
  if (Number.isNaN(n)) return "NaN";
  if (n === Infinity) return "+Inf";
  if (n === -Infinity) return "-Inf";
  // toString() is exact for all finite doubles
  return n.toString();
}

function stableStringify(value: unknown): string {
  if (value === null || value === undefined) return "null";
  if (typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) {
    return `[${value.map(stableStringify).join(",")}]`;
  }
  const keys = Object.keys(value as Record<string, unknown>).sort();
  return (
    "{" +
    keys
      .map(
        (k) =>
          JSON.stringify(k) +
          ":" +
          stableStringify((value as Record<string, unknown>)[k]),
      )
      .join(",") +
    "}"
  );
}

export type DictionarySnapshot = {
  entries: [string, number][]; // canonical → id
  nextId: number;
};

export class Dictionary {
  private forward: Map<string, number>;
  private reverse: string[]; // id → canonical key
  private original: Map<string, unknown>; // canonical key → original value
  private _nextId: number;
  private _dirty: boolean;

  constructor(snapshot?: DictionarySnapshot) {
    this.forward = new Map();
    this.reverse = [];
    this.original = new Map();
    this._nextId = 0;
    this._dirty = false;

    if (snapshot) {
      this._nextId = snapshot.nextId;
      // Pre-allocate reverse array
      this.reverse = new Array(snapshot.nextId);
      for (const [canonical, id] of snapshot.entries) {
        this.forward.set(canonical, id);
        this.reverse[id] = canonical;
      }
    }
  }

  /**
   * Get or assign an integer ID for a value.
   * Returns the existing ID if already known.
   */
  encode(value: unknown): number {
    const key = canonicalize(value);
    const existing = this.forward.get(key);
    if (existing !== undefined) return existing;

    const id = this._nextId++;
    this.forward.set(key, id);
    this.reverse[id] = key;
    this.original.set(key, value);
    this._dirty = true;
    return id;
  }

  /**
   * Look up an existing ID without assigning a new one.
   * Returns undefined if the value is not in the dictionary.
   */
  tryEncode(value: unknown): number | undefined {
    return this.forward.get(canonicalize(value));
  }

  /**
   * Decode an ID back to the canonical key string.
   * For structural decode back to original value, use decodeValue().
   */
  decode(id: number): string | undefined {
    return this.reverse[id];
  }

  /**
   * Decode an ID back to the original JS value.
   * Falls back to the canonical key if the original is not cached
   * (e.g., after deserialization from snapshot).
   */
  decodeValue(id: number): unknown {
    const key = this.reverse[id];
    if (key === undefined) return undefined;

    // Check original cache first
    const orig = this.original.get(key);
    if (orig !== undefined) return orig;

    // Reconstruct from canonical key
    return decodeCanonical(key);
  }

  get size(): number {
    return this.forward.size;
  }

  get nextId(): number {
    return this._nextId;
  }

  get dirty(): boolean {
    return this._dirty;
  }

  clearDirty(): void {
    this._dirty = false;
  }

  /**
   * Serialize to a snapshot for persistence.
   */
  toSnapshot(): DictionarySnapshot {
    const entries: [string, number][] = [];
    for (const [key, id] of this.forward) {
      entries.push([key, id]);
    }
    return { entries, nextId: this._nextId };
  }
}

/**
 * Reconstruct a JS value from a canonical key.
 */
function decodeCanonical(key: string): unknown {
  if (key.length === 0) return undefined;
  const prefix = key.charCodeAt(0);
  const rest = key.substring(1);

  switch (prefix) {
    case PREFIX_NULL:
      return null;
    case PREFIX_BOOL:
      return rest === "1";
    case PREFIX_NUMBER:
      return decodeNumber(rest);
    case PREFIX_STRING:
      return rest;
    case PREFIX_COMPLEX:
      try {
        return JSON.parse(rest);
      } catch {
        return rest;
      }
    default:
      return key;
  }
}

function decodeNumber(s: string): number {
  if (s === "-0") return -0;
  if (s === "NaN") return NaN;
  if (s === "+Inf") return Infinity;
  if (s === "-Inf") return -Infinity;
  return Number(s);
}
