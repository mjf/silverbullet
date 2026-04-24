import { describe, expect, test } from "vitest";
import { ObjectIndex } from "./object_index.ts";
import { DataStore } from "./datastore.ts";
import { MemoryKvPrimitives } from "./memory_kv_primitives.ts";
import { DataStoreMQ } from "./mq.datastore.ts";
import { EventHook } from "../plugos/hooks/event.ts";
import { Config } from "../config.ts";
import { LuaEnv, LuaStackFrame } from "../space_lua/runtime.ts";
import { parseExpressionString } from "../space_lua/parse.ts";

function names(results: any[]): string[] {
  return results.map((r: any) => r.name ?? r.rawGet?.("name")).sort();
}

// --- Test helpers ---

function createTestIndex(config?: Config) {
  const kv = new MemoryKvPrimitives();
  const ds = new DataStore(kv);
  const cfg = config ?? new Config();
  const eventHook = new EventHook(cfg);
  const mq = new DataStoreMQ(ds, eventHook);
  const index = new ObjectIndex(ds, cfg, eventHook, mq, {
    minRowsForIndex: 0,
  });
  return { index, ds, kv, config: cfg };
}

// --- Basic indexing ---

describe("ObjectIndex indexObjects", () => {
  test("index and retrieve objects", async () => {
    const { index } = createTestIndex();

    await index.indexObjects("TestPage", [
      { tag: "item", ref: "TestPage@1", name: "Buy groceries" },
      { tag: "item", ref: "TestPage@2", name: "Write tests" },
    ]);

    const result = await index.getObjectByRef("TestPage", "item", "TestPage@1");
    expect(result).toBeTruthy();
    expect(result.name).toBe("Buy groceries");

    const result2 = await index.getObjectByRef(
      "TestPage",
      "item",
      "TestPage@2",
    );
    expect(result2).toBeTruthy();
    expect(result2.name).toBe("Write tests");
  });

  test("getObjectByRef returns null for nonexistent", async () => {
    const { index } = createTestIndex();
    const result = await index.getObjectByRef("X", "item", "X@999");
    expect(result).toBeNull();
  });

  test("index with multiple tags", async () => {
    const { index } = createTestIndex();

    await index.indexObjects("P", [
      { tag: "task", tags: ["item"], ref: "P@1", name: "A task" },
    ]);

    // Should be retrievable under both tags
    const asTask = await index.getObjectByRef("P", "task", "P@1");
    expect(asTask).toBeTruthy();
    expect(asTask.name).toBe("A task");

    const asItem = await index.getObjectByRef("P", "item", "P@1");
    expect(asItem).toBeTruthy();
    expect(asItem.name).toBe("A task");
  });
});

// --- Re-indexing (overwrite) ---

describe("ObjectIndex re-indexing", () => {
  test("re-indexing same ref overwrites", async () => {
    const { index } = createTestIndex();

    await index.indexObjects("P", [
      { tag: "item", ref: "P@1", name: "Version 1" },
    ]);

    await index.indexObjects("P", [
      { tag: "item", ref: "P@1", name: "Version 2" },
    ]);

    const result = await index.getObjectByRef("P", "item", "P@1");
    expect(result).toBeTruthy();
    expect(result.name).toBe("Version 2");
  });
});

// --- Delete ---

describe("ObjectIndex deleteObject", () => {
  test("delete removes object", async () => {
    const { index } = createTestIndex();

    await index.indexObjects("P", [
      { tag: "item", ref: "P@1", name: "To delete" },
    ]);

    await index.deleteObject("P", "item", "P@1");

    const result = await index.getObjectByRef("P", "item", "P@1");
    expect(result).toBeNull();
  });

  test("delete nonexistent is no-op", async () => {
    const { index } = createTestIndex();
    // Should not throw
    await index.deleteObject("P", "item", "P@999");
  });
});

// --- Clear file index ---

describe("ObjectIndex clearFileIndex", () => {
  test("clears all objects for a page", async () => {
    const { index } = createTestIndex();

    await index.indexObjects("Page1", [
      { tag: "item", ref: "Page1@1", name: "A" },
      { tag: "item", ref: "Page1@2", name: "B" },
    ]);
    await index.indexObjects("Page2", [
      { tag: "item", ref: "Page2@1", name: "C" },
    ]);

    await index.clearFileIndex("Page1");

    expect(await index.getObjectByRef("Page1", "item", "Page1@1")).toBeNull();
    expect(await index.getObjectByRef("Page1", "item", "Page1@2")).toBeNull();
    // Page2 should still be there
    const page2Obj = await index.getObjectByRef("Page2", "item", "Page2@1");
    expect(page2Obj).toBeTruthy();
    expect(page2Obj.name).toBe("C");
  });

  test("clears .md extension pages", async () => {
    const { index } = createTestIndex();

    await index.indexObjects("Notes", [
      { tag: "item", ref: "Notes@1", name: "Note" },
    ]);

    await index.clearFileIndex("Notes.md");

    expect(await index.getObjectByRef("Notes", "item", "Notes@1")).toBeNull();
  });
});

// --- Clear entire index ---

describe("ObjectIndex clearIndex", () => {
  test("clears everything", async () => {
    const { index } = createTestIndex();

    await index.indexObjects("P1", [{ tag: "item", ref: "P1@1", name: "X" }]);
    await index.indexObjects("P2", [{ tag: "page", ref: "P2", name: "P2" }]);

    await index.clearIndex();

    expect(await index.getObjectByRef("P1", "item", "P1@1")).toBeNull();
    expect(await index.getObjectByRef("P2", "page", "P2")).toBeNull();
  });
});

// --- Tag query ---

describe("ObjectIndex tag query", () => {
  test("tag().query returns all objects", async () => {
    const { index } = createTestIndex();

    await index.indexObjects("P", [
      { tag: "item", ref: "P@1", name: "A" },
      { tag: "item", ref: "P@2", name: "B" },
      { tag: "item", ref: "P@3", name: "C" },
    ]);

    const collection = index.tag("item");
    const env = new LuaEnv();
    const sf = LuaStackFrame.lostFrame;
    const results = await collection.query({}, env, sf);

    expect(results).toHaveLength(3);
    const names = results.map((r: any) => r.name ?? r.rawGet?.("name"));
    expect(names.sort()).toEqual(["A", "B", "C"]);
  });

  test("tag().query with limit", async () => {
    const { index } = createTestIndex();

    await index.indexObjects("P", [
      { tag: "item", ref: "P@1", name: "A" },
      { tag: "item", ref: "P@2", name: "B" },
      { tag: "item", ref: "P@3", name: "C" },
    ]);

    const collection = index.tag("item");
    const env = new LuaEnv();
    const sf = LuaStackFrame.lostFrame;
    const results = await collection.query({ limit: 2 }, env, sf);

    expect(results).toHaveLength(2);
  });

  test("tag() throws on empty name", () => {
    const { index } = createTestIndex();
    expect(() => index.tag("")).toThrow("Tag name is required");
  });
});

// --- Stats ---

describe("ObjectIndex stats", () => {
  test("getStats returns row count and NDV after full index completion", async () => {
    const { index } = createTestIndex();

    await index.indexObjects("P", [
      { tag: "item", ref: "P@1", name: "A", page: "P" },
      { tag: "item", ref: "P@2", name: "B", page: "P" },
      { tag: "item", ref: "P@3", name: "C", page: "Q" },
    ]);

    await index.markFullIndexComplete();

    const stats = await index.tag("item").getStats!();
    expect(stats).toBeDefined();
    expect(stats!.rowCount).toBe(3);
    // page column has 2 distinct values: P and Q
    expect(stats!.ndv.get("page")).toBe(2);
    // The raw data has 3 distinct names, but persisted NDV comes from
    // bitmap-indexed metadata. Because `name` is highly selective and not
    // always-indexed, indexing is disabled once the selectivity threshold
    // is exceeded, so the persisted NDV observed here is 2.
    expect(stats!.ndv.get("name")).toBe(2);
  });

  test("getStats for unknown tag returns zero", async () => {
    const { index } = createTestIndex();
    const stats = await index.tag("nonexistent").getStats!();
    expect(stats!.rowCount).toBe(0);
  });

  test("getStats advertises bitmap-extended capabilities when trusted", async () => {
    const { index } = createTestIndex();

    await index.indexObjects("P", [
      { tag: "item", ref: "P@1", page: "X" },
      { tag: "item", ref: "P@2", page: "Y" },
      { tag: "item", ref: "P@3", page: "Z" },
    ]);
    await index.markFullIndexComplete();

    const stats = await index.tag("item").getStats!();
    const caps = stats!.executionCapabilities!.engines[0].capabilities;
    expect(caps).toContain("pred-in");
    expect(caps).toContain("bool-or");
    expect(caps).toContain("bool-not");
    expect(caps).toContain("scan-bitmap");
    expect(caps).toContain("stage-where");
  });

  test("per-tag stats row reports bitmap-extended pushdown label", async () => {
    const { index } = createTestIndex();

    await index.indexObjects("P", [
      { tag: "item", ref: "P@1", page: "X" },
      { tag: "item", ref: "P@2", page: "Y" },
    ]);
    await index.markFullIndexComplete();

    const env = new LuaEnv();
    const sf = LuaStackFrame.lostFrame;
    const rows = await (await index.stats("item")).query({}, env, sf);
    const tagRow = rows.find((r: any) => r.column === null);
    expect(tagRow!.predicatePushdown).toBe("bitmap-extended");
  });
});

// --- Validation ---

describe("ObjectIndex validation", () => {
  test("validates objects against schema", async () => {
    const cfg = new Config();
    cfg.set(["tags", "strict"], {
      mustValidate: true,
      schema: {
        type: "object",
        properties: {
          name: { type: "string" },
          count: { type: "number" },
        },
        required: ["name"],
      },
    });

    const { index } = createTestIndex(cfg);

    // This should succeed (valid)
    await index.indexObjects("P", [
      { tag: "strict", ref: "P@1", name: "Valid", count: 5 },
    ]);

    const result = await index.getObjectByRef("P", "strict", "P@1");
    expect(result).toBeTruthy();
  });

  test("validateObjects throws on invalid", async () => {
    const cfg = new Config();
    cfg.set(["tags", "strict"], {
      mustValidate: true,
      schema: {
        type: "object",
        required: ["name"],
      },
    });

    const { index } = createTestIndex(cfg);

    await expect(
      index.validateObjects("P", [{ tag: "strict", ref: "P@1" } as any]),
    ).rejects.toThrow();
  });
});

// --- Bitmap predicate pushdown ---

describe("ObjectIndex bitmap pushdown", () => {
  async function seedPages(index: ObjectIndex) {
    await index.indexObjects("P", [
      { tag: "item", ref: "P@1", name: "A", page: "X" },
      { tag: "item", ref: "P@2", name: "B", page: "Y" },
      { tag: "item", ref: "P@3", name: "C", page: "Z" },
      { tag: "item", ref: "P@4", name: "D", page: "X" },
      { tag: "item", ref: "P@5", name: "E", page: "W" },
    ]);
    await index.markFullIndexComplete();
  }

  test("pred-in: `page in {X, Y}` is answered via bitmap union", async () => {
    const { index } = createTestIndex();
    await seedPages(index);

    const env = new LuaEnv();
    const sf = LuaStackFrame.lostFrame;
    const where = parseExpressionString("o.page in {'X', 'Y'}");
    const results = await index
      .tag("item")
      .query({ where, objectVariable: "o" }, env, sf);
    expect(names(results)).toEqual(["A", "B", "D"]);
  });

  test("bool-or: same-column equality chain collapses to bitmap IN", async () => {
    const { index } = createTestIndex();
    await seedPages(index);

    const env = new LuaEnv();
    const sf = LuaStackFrame.lostFrame;
    const where = parseExpressionString(
      "o.page == 'X' or o.page == 'Y' or o.page == 'Z'",
    );
    const results = await index
      .tag("item")
      .query({ where, objectVariable: "o" }, env, sf);
    expect(names(results)).toEqual(["A", "B", "C", "D"]);
  });

  test("bool-not: `not (page in {X, Y})` rewrites to neq conjunction", async () => {
    const { index } = createTestIndex();
    await seedPages(index);

    const env = new LuaEnv();
    const sf = LuaStackFrame.lostFrame;
    const where = parseExpressionString("not (o.page in {'X', 'Y'})");
    const results = await index
      .tag("item")
      .query({ where, objectVariable: "o" }, env, sf);
    expect(names(results)).toEqual(["C", "E"]);
  });

  test("pred-in combined with AND of range predicate", async () => {
    const { index } = createTestIndex();
    await index.indexObjects("P", [
      { tag: "item", ref: "P@1", name: "A", page: "X", count: 10 },
      { tag: "item", ref: "P@2", name: "B", page: "Y", count: 20 },
      { tag: "item", ref: "P@3", name: "C", page: "Z", count: 30 },
      { tag: "item", ref: "P@4", name: "D", page: "X", count: 40 },
      { tag: "item", ref: "P@5", name: "E", page: "W", count: 50 },
    ]);
    await index.markFullIndexComplete();

    const env = new LuaEnv();
    const sf = LuaStackFrame.lostFrame;
    const where = parseExpressionString(
      "o.page in {'X', 'Y'} and o.count >= 20",
    );
    const results = await index
      .tag("item")
      .query({ where, objectVariable: "o" }, env, sf);
    expect(names(results)).toEqual(["B", "D"]);
  });

  test("pred-in with single value behaves like equality", async () => {
    const { index } = createTestIndex();
    await seedPages(index);

    const env = new LuaEnv();
    const sf = LuaStackFrame.lostFrame;
    const where = parseExpressionString("o.page in {'Y'}");
    const results = await index
      .tag("item")
      .query({ where, objectVariable: "o" }, env, sf);
    expect(names(results)).toEqual(["B"]);
  });

  test("pred-in with value not in dictionary returns empty set", async () => {
    const { index } = createTestIndex();
    await seedPages(index);

    const env = new LuaEnv();
    const sf = LuaStackFrame.lostFrame;
    const where = parseExpressionString("o.page in {'nope', 'also-nope'}");
    const results = await index
      .tag("item")
      .query({ where, objectVariable: "o" }, env, sf);
    expect(results).toHaveLength(0);
  });

  test("bool-or on different columns falls back to full scan (no bitmap push)", async () => {
    const { index } = createTestIndex();
    await seedPages(index);

    const env = new LuaEnv();
    const sf = LuaStackFrame.lostFrame;
    const where = parseExpressionString("o.page == 'X' or o.name == 'C'");
    const results = await index
      .tag("item")
      .query({ where, objectVariable: "o" }, env, sf);
    expect(names(results)).toEqual(["A", "C", "D"]);
  });
});

// --- cleanKey ---

describe("ObjectIndex cleanKey", () => {
  test("strips page prefix from ref", () => {
    const { index } = createTestIndex();
    expect(index.cleanKey("MyPage@42", "MyPage")).toBe("42");
  });

  test("leaves ref without page prefix unchanged", () => {
    const { index } = createTestIndex();
    expect(index.cleanKey("other@42", "MyPage")).toBe("other@42");
  });
});
