import {
  bytesToHex,
  decodeCanonicalValue,
  encodeCanonicalValue,
  hexToBytes,
} from "./value_codec.ts";

export type DictionaryValue =
  | null
  | boolean
  | number
  | string
  | DictionaryValue[]
  | { [key: string]: DictionaryValue };

export type DictionarySnapshot = {
  nextId: number;
  values: Record<number, string>;
};

function normalizeValue(value: unknown): DictionaryValue {
  if (value === undefined) {
    return null;
  }
  return value as DictionaryValue;
}

function bytesToKey(bytes: Uint8Array): string {
  let out = "";
  for (const b of bytes) {
    out += String.fromCharCode(b);
  }
  return out;
}

export function canonicalize(value: unknown): string {
  return bytesToKey(encodeCanonicalValue(normalizeValue(value)));
}

export class Dictionary {
  private valueToId = new Map<string, number>();
  private idToValue = new Map<number, DictionaryValue>();

  nextId = 0;
  dirty = false;

  constructor(snapshot?: DictionarySnapshot) {
    if (snapshot) {
      this.loadSnapshot(snapshot);
    }
  }

  get size(): number {
    return this.idToValue.size;
  }

  clear() {
    this.valueToId.clear();
    this.idToValue.clear();
    this.nextId = 0;
    this.dirty = false;
  }

  clearDirty() {
    this.dirty = false;
  }

  encode(value: unknown): number {
    const normalized = normalizeValue(value);
    const key = canonicalize(normalized);
    const existing = this.valueToId.get(key);
    if (existing !== undefined) {
      return existing;
    }

    const id = this.nextId++;
    this.valueToId.set(key, id);
    this.idToValue.set(id, normalized);
    this.dirty = true;
    return id;
  }

  tryEncode(value: unknown): number | undefined {
    return this.valueToId.get(canonicalize(value));
  }

  decode(id: number): DictionaryValue | undefined {
    return this.idToValue.get(id);
  }

  decodeValue(id: number): DictionaryValue | undefined {
    return this.idToValue.get(id);
  }

  hasId(id: number): boolean {
    return this.idToValue.has(id);
  }

  toSnapshot(): DictionarySnapshot {
    const values: Record<number, string> = {};
    for (const [id, value] of this.idToValue.entries()) {
      values[id] = bytesToHex(encodeCanonicalValue(value));
    }
    return {
      nextId: this.nextId,
      values,
    };
  }

  load(snapshot: DictionarySnapshot) {
    this.loadSnapshot(snapshot);
  }

  private loadSnapshot(snapshot: DictionarySnapshot) {
    this.valueToId.clear();
    this.idToValue.clear();
    this.nextId = snapshot.nextId;

    for (const [idStr, hex] of Object.entries(snapshot.values)) {
      const id = Number(idStr);
      const value = decodeCanonicalValue(hexToBytes(hex)) as DictionaryValue;
      this.idToValue.set(id, value);
      this.valueToId.set(canonicalize(value), id);
    }

    this.dirty = false;
  }
}