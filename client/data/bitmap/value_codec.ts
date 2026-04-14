const TAG_NULL = 0x00;
const TAG_FALSE = 0x01;
const TAG_TRUE = 0x02;
const TAG_NUMBER = 0x03;
const TAG_STRING = 0x04;
const TAG_ARRAY = 0x05;
const TAG_OBJECT = 0x06;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

type SupportedValue =
  | null
  | boolean
  | number
  | string
  | SupportedValue[]
  | { [key: string]: SupportedValue };

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object") return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function normalizeNumber(value: number): number {
  if (Number.isNaN(value)) {
    return NaN;
  }
  return value;
}

function ensureSupportedValue(value: unknown): asserts value is SupportedValue {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "string"
  ) {
    return;
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      ensureSupportedValue(item);
    }
    return;
  }
  if (isPlainObject(value)) {
    for (const [k, v] of Object.entries(value)) {
      if (typeof k !== "string") {
        throw new Error("Only string object keys are supported");
      }
      ensureSupportedValue(v);
    }
    return;
  }
  throw new Error(`Unsupported value type for value codec: ${typeof value}`);
}

function u32ToBytes(n: number): Uint8Array {
  const buf = new Uint8Array(4);
  const view = new DataView(buf.buffer);
  view.setUint32(0, n, false);
  return buf;
}

function bytesToU32(bytes: Uint8Array, offset: number): number {
  const view = new DataView(bytes.buffer, bytes.byteOffset + offset, 4);
  return view.getUint32(0, false);
}

function f64ToBytes(n: number): Uint8Array {
  const buf = new Uint8Array(8);
  const view = new DataView(buf.buffer);
  if (Number.isNaN(n)) {
    view.setUint32(0, 0x7ff80000, false);
    view.setUint32(4, 0x00000000, false);
  } else {
    view.setFloat64(0, n, false);
  }
  return buf;
}

function bytesToF64(bytes: Uint8Array, offset: number): number {
  const view = new DataView(bytes.buffer, bytes.byteOffset + offset, 8);
  return view.getFloat64(0, false);
}

function concatBytes(parts: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const p of parts) total += p.length;
  const out = new Uint8Array(total);
  let offset = 0;
  for (const p of parts) {
    out.set(p, offset);
    offset += p.length;
  }
  return out;
}

function encodeStringPayload(value: string): Uint8Array {
  const data = textEncoder.encode(value);
  return concatBytes([u32ToBytes(data.length), data]);
}

function decodeStringPayload(
  bytes: Uint8Array,
  offset: number,
): { value: string; nextOffset: number } {
  if (offset + 4 > bytes.length) {
    throw new Error("Truncated string length");
  }
  const len = bytesToU32(bytes, offset);
  offset += 4;
  if (offset + len > bytes.length) {
    throw new Error("Truncated string payload");
  }
  const value = textDecoder.decode(bytes.subarray(offset, offset + len));
  return { value, nextOffset: offset + len };
}

function encodeValueInternal(value: SupportedValue): Uint8Array {
  if (value === null) {
    return Uint8Array.of(TAG_NULL);
  }

  if (typeof value === "boolean") {
    return Uint8Array.of(value ? TAG_TRUE : TAG_FALSE);
  }

  if (typeof value === "number") {
    return concatBytes([
      Uint8Array.of(TAG_NUMBER),
      f64ToBytes(normalizeNumber(value)),
    ]);
  }

  if (typeof value === "string") {
    return concatBytes([Uint8Array.of(TAG_STRING), encodeStringPayload(value)]);
  }

  if (Array.isArray(value)) {
    const parts: Uint8Array[] = [
      Uint8Array.of(TAG_ARRAY),
      u32ToBytes(value.length),
    ];
    for (const item of value) {
      parts.push(encodeValueInternal(item));
    }
    return concatBytes(parts);
  }

  const keys = Object.keys(value).sort();
  const parts: Uint8Array[] = [
    Uint8Array.of(TAG_OBJECT),
    u32ToBytes(keys.length),
  ];
  for (const key of keys) {
    parts.push(encodeStringPayload(key));
    parts.push(encodeValueInternal(value[key]));
  }
  return concatBytes(parts);
}

function decodeValueInternal(
  bytes: Uint8Array,
  offset: number,
): { value: SupportedValue; nextOffset: number } {
  if (offset >= bytes.length) {
    throw new Error("Unexpected end of input");
  }

  const tag = bytes[offset++];

  switch (tag) {
    case TAG_NULL:
      return { value: null, nextOffset: offset };

    case TAG_FALSE:
      return { value: false, nextOffset: offset };

    case TAG_TRUE:
      return { value: true, nextOffset: offset };

    case TAG_NUMBER: {
      if (offset + 8 > bytes.length) {
        throw new Error("Truncated number payload");
      }
      const value = bytesToF64(bytes, offset);
      return { value, nextOffset: offset + 8 };
    }

    case TAG_STRING: {
      const { value, nextOffset } = decodeStringPayload(bytes, offset);
      return { value, nextOffset };
    }

    case TAG_ARRAY: {
      if (offset + 4 > bytes.length) {
        throw new Error("Truncated array length");
      }
      const count = bytesToU32(bytes, offset);
      offset += 4;
      const arr: SupportedValue[] = [];
      for (let i = 0; i < count; i++) {
        const decoded = decodeValueInternal(bytes, offset);
        arr.push(decoded.value);
        offset = decoded.nextOffset;
      }
      return { value: arr, nextOffset: offset };
    }

    case TAG_OBJECT: {
      if (offset + 4 > bytes.length) {
        throw new Error("Truncated object length");
      }
      const count = bytesToU32(bytes, offset);
      offset += 4;
      const obj: Record<string, SupportedValue> = {};
      for (let i = 0; i < count; i++) {
        const keyDecoded = decodeStringPayload(bytes, offset);
        offset = keyDecoded.nextOffset;
        const valueDecoded = decodeValueInternal(bytes, offset);
        offset = valueDecoded.nextOffset;
        obj[keyDecoded.value] = valueDecoded.value;
      }
      return { value: obj, nextOffset: offset };
    }

    default:
      throw new Error(`Unknown value tag: ${tag}`);
  }
}

export function encodeCanonicalValue(value: unknown): Uint8Array {
  ensureSupportedValue(value);
  return encodeValueInternal(value);
}

export function decodeCanonicalValue(bytes: Uint8Array): SupportedValue {
  const { value, nextOffset } = decodeValueInternal(bytes, 0);
  if (nextOffset !== bytes.length) {
    throw new Error("Trailing bytes after canonical value");
  }
  return value;
}

export function canonicalValueToHex(value: unknown): string {
  return bytesToHex(encodeCanonicalValue(value));
}

export function bytesToHex(bytes: Uint8Array): string {
  let out = "";
  for (const b of bytes) {
    out += b.toString(16).padStart(2, "0");
  }
  return out;
}

export function hexToBytes(hex: string): Uint8Array {
  if (hex.length % 2 !== 0) {
    throw new Error("Invalid hex length");
  }
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) {
    const byte = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
    if (Number.isNaN(byte)) {
      throw new Error("Invalid hex digit");
    }
    out[i] = byte;
  }
  return out;
}
