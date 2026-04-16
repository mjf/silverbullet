import { describe, expect, it } from "vitest";
import {
  buildJoinTree,
  executeJoinTree,
  explainJoinTree,
  formatExplainOutput,
  stripUsedJoinPredicates,
  wrapPlanWithQueryOps,
  type ExplainNode,
  type JoinSource,
} from "./join_planner.ts";
import { parseExpressionString } from "./parse.ts";
import { LuaEnv, LuaStackFrame, LuaTable } from "./runtime.ts";

describe("wrapPlanWithQueryOps group NDV", () => {
  it("prefers accumulated post-join NDV over leaf source NDV", () => {
    const plan: ExplainNode = {
      nodeType: "HashJoin",
      startupCost: 0,
      estimatedCost: 100,
      estimatedRows: 1000,
      estimatedWidth: 10,
      children: [
        {
          nodeType: "Scan",
          source: "p",
          startupCost: 0,
          estimatedCost: 10,
          estimatedRows: 100,
          estimatedWidth: 5,
          children: [],
        },
        {
          nodeType: "Scan",
          source: "para",
          startupCost: 0,
          estimatedCost: 20,
          estimatedRows: 500,
          estimatedWidth: 5,
          children: [],
        },
      ],
    };

    const sourceStats = new Map([
      [
        "p",
        {
          rowCount: 100,
          avgColumnCount: 5,
          ndv: new Map([["name", 100]]),
        },
      ],
      [
        "para",
        {
          rowCount: 500,
          avgColumnCount: 5,
          ndv: new Map([["page", 100]]),
        },
      ],
    ]);

    const accumulatedNdv = new Map([
      ["p", new Map([["name", 37]])],
      ["para", new Map([["page", 37]])],
    ]);

    const wrapped = wrapPlanWithQueryOps(
      plan,
      {
        groupBy: [
          {
            expr: {
              type: "PropertyAccess",
              object: {
                type: "Variable",
                name: "p",
                ctx: {} as any,
              },
              property: "name",
              ctx: {} as any,
            },
          },
        ],
      },
      sourceStats as any,
      accumulatedNdv,
    );

    expect(wrapped.nodeType).toBe("Project");
    expect(wrapped.children.length).toBe(1);
    const groupNode = wrapped.children[0];
    expect(groupNode.nodeType).toBe("GroupAggregate");
    expect(groupNode.estimatedRows).toBe(37);
  });

  it("falls back to leaf source NDV when accumulated NDV is absent", () => {
    const plan: ExplainNode = {
      nodeType: "Scan",
      source: "p",
      startupCost: 0,
      estimatedCost: 10,
      estimatedRows: 100,
      estimatedWidth: 5,
      children: [],
    };

    const sourceStats = new Map([
      [
        "p",
        {
          rowCount: 100,
          avgColumnCount: 5,
          ndv: new Map([["name", 42]]),
        },
      ],
    ]);

    const wrapped = wrapPlanWithQueryOps(
      plan,
      {
        groupBy: [
          {
            expr: {
              type: "PropertyAccess",
              object: {
                type: "Variable",
                name: "p",
                ctx: {} as any,
              },
              property: "name",
              ctx: {} as any,
            },
          },
        ],
      },
      sourceStats as any,
      undefined,
    );

    expect(wrapped.nodeType).toBe("Project");
    expect(wrapped.children.length).toBe(1);
    const groupNode = wrapped.children[0];
    expect(groupNode.nodeType).toBe("GroupAggregate");
    expect(groupNode.estimatedRows).toBe(42);
  });
});

describe("wrapPlanWithQueryOps Sort key annotations", () => {
  const basePlan: ExplainNode = {
    nodeType: "Scan",
    source: "t",
    startupCost: 0,
    estimatedCost: 100,
    estimatedRows: 100,
    estimatedWidth: 5,
    children: [],
  };

  it("annotates plain asc sort key", () => {
    const wrapped = wrapPlanWithQueryOps(basePlan, {
      orderBy: [
        {
          expr: { type: "Variable", name: "name", ctx: {} as any },
          desc: false,
        },
      ],
    });
    expect(wrapped.nodeType).toBe("Sort");
    expect(wrapped.sortKeys).toEqual(["name"]);
  });

  it("annotates desc sort key", () => {
    const wrapped = wrapPlanWithQueryOps(basePlan, {
      orderBy: [
        {
          expr: { type: "Variable", name: "age", ctx: {} as any },
          desc: true,
        },
      ],
    });
    expect(wrapped.nodeType).toBe("Sort");
    expect(wrapped.sortKeys).toEqual(["age desc"]);
  });

  it("annotates nulls first", () => {
    const wrapped = wrapPlanWithQueryOps(basePlan, {
      orderBy: [
        {
          expr: { type: "Variable", name: "priority", ctx: {} as any },
          desc: false,
          nulls: "first",
        },
      ],
    });
    expect(wrapped.nodeType).toBe("Sort");
    expect(wrapped.sortKeys).toEqual(["priority nulls first"]);
  });

  it("annotates desc nulls last", () => {
    const wrapped = wrapPlanWithQueryOps(basePlan, {
      orderBy: [
        {
          expr: { type: "Variable", name: "score", ctx: {} as any },
          desc: true,
          nulls: "last",
        },
      ],
    });
    expect(wrapped.nodeType).toBe("Sort");
    expect(wrapped.sortKeys).toEqual(["score desc nulls last"]);
  });

  it("annotates using with function name", () => {
    const wrapped = wrapPlanWithQueryOps(basePlan, {
      orderBy: [
        {
          expr: { type: "Variable", name: "val", ctx: {} as any },
          desc: false,
          using: "my_cmp",
        },
      ],
    });
    expect(wrapped.nodeType).toBe("Sort");
    expect(wrapped.sortKeys).toEqual(["val using my_cmp"]);
  });

  it("annotates multiple keys with mixed options", () => {
    const wrapped = wrapPlanWithQueryOps(basePlan, {
      orderBy: [
        {
          expr: {
            type: "PropertyAccess",
            object: { type: "Variable", name: "p", ctx: {} as any },
            property: "name",
            ctx: {} as any,
          },
          desc: false,
        },
        {
          expr: {
            type: "PropertyAccess",
            object: { type: "Variable", name: "p", ctx: {} as any },
            property: "age",
            ctx: {} as any,
          },
          desc: true,
          nulls: "first",
        },
      ],
    });
    expect(wrapped.nodeType).toBe("Sort");
    expect(wrapped.sortKeys).toEqual(["p.name", "p.age desc nulls first"]);
  });

  it("stores orderBySpec with nulls and using", () => {
    const wrapped = wrapPlanWithQueryOps(basePlan, {
      orderBy: [
        {
          expr: { type: "Variable", name: "x", ctx: {} as any },
          desc: true,
          nulls: "last",
          using: "cmp_fn",
        },
      ],
    });
    expect(wrapped.orderBySpec).toEqual([
      {
        expr: { type: "Variable", name: "x", ctx: {} as any },
        desc: true,
        nulls: "last",
        using: "cmp_fn",
      },
    ]);
  });
});

function makeSource(
  name: string,
  items?: any[],
  extraNdv?: [string, number][],
): JoinSource {
  return {
    name,
    expression: parseExpressionString(name),
    stats: {
      rowCount: items?.length ?? 100,
      ndv: new Map<string, number>([
        ["id", 100],
        ["x", 100],
        ["y", 100],
        ["z", 100],
        ["price", 100],
        ["min_price", 100],
        ["max_price", 100],
        ["keep", 2],
        ["flag", 2],
        ...(extraNdv ?? []),
      ]),
      avgColumnCount: 4,
      statsSource: "computed-exact-small",
      executionCapabilities: {
        predicatePushdown: "none",
        scanKind: "materialized",
      },
    },
  };
}

function testEnvWithSources(bindings: Record<string, any[]>): LuaEnv {
  const env = new LuaEnv();
  for (const [name, items] of Object.entries(bindings)) {
    env.setLocal(name, items);
  }
  return env;
}

describe("join residual predicate stripping and explain", () => {
  it("strips consumed equi and cross-source residual predicates from WHERE", () => {
    const sources: JoinSource[] = [makeSource("a"), makeSource("b")];

    const where = parseExpressionString(
      "a.id == b.id and a.price > b.min_price and a.keep == 1",
    );

    const joinTree = buildJoinTree(
      sources,
      undefined,
      [
        {
          leftSource: "a",
          leftColumn: "id",
          rightSource: "b",
          rightColumn: "id",
        },
      ],
      [
        {
          leftSource: "a",
          leftColumn: "price",
          operator: ">",
          rightSource: "b",
          rightColumn: "min_price",
        },
      ],
      where,
    );

    const residual = stripUsedJoinPredicates(where, joinTree);
    expect(residual).toBeDefined();
    expect(JSON.stringify(residual)).toContain('"property":"keep"');
    expect(JSON.stringify(residual)).toContain('"value":1');
    expect(JSON.stringify(residual)).not.toContain('"min_price"');
    expect(JSON.stringify(residual)).not.toContain('"operator":">"');
  });

  it("does not strip single-source predicates from WHERE", () => {
    const sources: JoinSource[] = [makeSource("a"), makeSource("b")];

    const where = parseExpressionString(
      "a.id == b.id and a.price > b.min_price and a.keep == 1 and b.flag == true",
    );

    const joinTree = buildJoinTree(
      sources,
      undefined,
      [
        {
          leftSource: "a",
          leftColumn: "id",
          rightSource: "b",
          rightColumn: "id",
        },
      ],
      [
        {
          leftSource: "a",
          leftColumn: "price",
          operator: ">",
          rightSource: "b",
          rightColumn: "min_price",
        },
      ],
      where,
    );

    const residual = stripUsedJoinPredicates(where, joinTree);
    const residualText = JSON.stringify(residual);

    expect(residualText.includes('"keep"')).toBe(true);
    expect(residualText.includes('"flag"')).toBe(true);
    expect(residualText.includes('"min_price"')).toBe(false);
  });

  it("explain exposes join residual filter", () => {
    const sources: JoinSource[] = [makeSource("a"), makeSource("b")];

    const where = parseExpressionString(
      "a.id == b.id and a.price > b.min_price and a.keep == 1",
    );

    const joinTree = buildJoinTree(
      sources,
      undefined,
      [
        {
          leftSource: "a",
          leftColumn: "id",
          rightSource: "b",
          rightColumn: "id",
        },
      ],
      [
        {
          leftSource: "a",
          leftColumn: "price",
          operator: ">",
          rightSource: "b",
          rightColumn: "min_price",
        },
      ],
      where,
    );

    const explain = explainJoinTree(joinTree, {
      analyze: false,
      verbose: true,
      summary: false,
      costs: true,
      timing: false,
    });

    expect(explain.joinResidualExprs).toEqual(["(a.price > b.min_price)"]);

    const rendered = formatExplainOutput(
      {
        plan: explain,
        planningTimeMs: 0,
      },
      {
        analyze: false,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
      },
    );

    expect(
      rendered.includes("Residual Join Filter: (a.price > b.min_price)"),
    ).toBe(true);
    expect(rendered.includes("Hash Condition: (a.id == b.id)")).toBe(true);
  });

  it("multiple consumed residual conjuncts are all stripped from post-join WHERE", () => {
    const sources: JoinSource[] = [
      makeSource("a"),
      makeSource("b", undefined, [
        ["id", 100],
        ["min_price", 100],
        ["max_price", 100],
      ]),
    ];

    const where = parseExpressionString(
      "a.id == b.id and a.price > b.min_price and a.price <= b.max_price and a.keep == 1",
    );

    const joinTree = buildJoinTree(
      sources,
      undefined,
      [
        {
          leftSource: "a",
          leftColumn: "id",
          rightSource: "b",
          rightColumn: "id",
        },
      ],
      [
        {
          leftSource: "a",
          leftColumn: "price",
          operator: ">",
          rightSource: "b",
          rightColumn: "min_price",
        },
        {
          leftSource: "a",
          leftColumn: "price",
          operator: "<=",
          rightSource: "b",
          rightColumn: "max_price",
        },
      ],
      where,
    );

    const residual = stripUsedJoinPredicates(where, joinTree);
    expect(residual).toBeDefined();
    expect(JSON.stringify(residual)).toContain('"property":"keep"');
    expect(JSON.stringify(residual)).toContain('"value":1');
    expect(JSON.stringify(residual)).not.toContain('"min_price"');
    expect(JSON.stringify(residual)).not.toContain('"max_price"');

    const explain = explainJoinTree(joinTree, {
      analyze: false,
      verbose: true,
      summary: false,
      costs: true,
      timing: false,
    });

    expect(explain.joinResidualExprs).toEqual([
      "(a.price > b.min_price)",
      "(a.price <= b.max_price)",
    ]);
  });

  it("assigns a residual predicate to the lowest covering join node in a three-source query", () => {
    const sources: JoinSource[] = [
      makeSource("a"),
      makeSource("b"),
      makeSource("c"),
    ];

    const where = parseExpressionString("a.x + b.y > 15");

    const joinTree = buildJoinTree(
      sources,
      undefined,
      undefined,
      undefined,
      where,
    );

    expect(joinTree.kind).toBe("join");
    if (joinTree.kind !== "join") {
      throw new Error("expected join root");
    }

    expect(joinTree.joinResiduals).toBeUndefined();

    expect(joinTree.left.kind).toBe("join");
    if (joinTree.left.kind !== "join") {
      throw new Error("expected lower join");
    }

    expect(joinTree.left.joinResiduals?.map((e) => JSON.stringify(e))).toEqual([
      JSON.stringify(where),
    ]);
  });
});

describe("join residual execution", () => {
  it("hash join applies residual predicate during execution", async () => {
    const aItems = [
      { id: 1, price: 5, keep: 1 },
      { id: 2, price: 20, keep: 1 },
    ];
    const bItems = [
      { id: 1, min_price: 10 },
      { id: 2, min_price: 10 },
    ];

    const sources: JoinSource[] = [
      makeSource("a", aItems),
      makeSource("b", bItems),
    ];

    const where = parseExpressionString(
      "a.id == b.id and a.price > b.min_price",
    );

    const joinTree = buildJoinTree(
      sources,
      undefined,
      [
        {
          leftSource: "a",
          leftColumn: "id",
          rightSource: "b",
          rightColumn: "id",
        },
      ],
      [
        {
          leftSource: "a",
          leftColumn: "price",
          operator: ">",
          rightSource: "b",
          rightColumn: "min_price",
        },
      ],
      where,
    );

    const env = testEnvWithSources({ a: aItems, b: bItems });
    const rows = await executeJoinTree(joinTree, env, LuaStackFrame.lostFrame);

    expect(rows.length).toBe(1);
    expect((rows[0].rawGet("a") as any).id).toBe(2);
    expect((rows[0].rawGet("b") as any).id).toBe(2);
  });

  it("semi join respects residual predicate during execution", async () => {
    const aItems = [
      { id: 1, price: 5 },
      { id: 2, price: 20 },
    ];
    const bItems = [
      { id: 1, min_price: 10 },
      { id: 2, min_price: 10 },
    ];

    const sources: JoinSource[] = [
      makeSource("a", aItems),
      {
        ...makeSource("b", bItems),
        hint: {
          type: "JoinHint",
          kind: "hash",
          joinType: "semi",
          ctx: {} as any,
        },
      },
    ];

    const where = parseExpressionString(
      "a.id == b.id and a.price > b.min_price",
    );

    const joinTree = buildJoinTree(
      sources,
      undefined,
      [
        {
          leftSource: "a",
          leftColumn: "id",
          rightSource: "b",
          rightColumn: "id",
        },
      ],
      [
        {
          leftSource: "a",
          leftColumn: "price",
          operator: ">",
          rightSource: "b",
          rightColumn: "min_price",
        },
      ],
      where,
    );

    const env = testEnvWithSources({ a: aItems, b: bItems });
    const rows = await executeJoinTree(joinTree, env, LuaStackFrame.lostFrame);

    expect(rows.length).toBe(1);
    expect((rows[0].rawGet("a") as any).id).toBe(2);
  });

  it("anti join respects residual predicate during execution", async () => {
    const aItems = [
      { id: 1, price: 5 },
      { id: 2, price: 20 },
    ];
    const bItems = [
      { id: 1, min_price: 10 },
      { id: 2, min_price: 10 },
    ];

    const sources: JoinSource[] = [
      makeSource("a", aItems),
      {
        ...makeSource("b", bItems),
        hint: {
          type: "JoinHint",
          kind: "hash",
          joinType: "anti",
          ctx: {} as any,
        },
      },
    ];

    const where = parseExpressionString(
      "a.id == b.id and a.price > b.min_price",
    );

    const joinTree = buildJoinTree(
      sources,
      undefined,
      [
        {
          leftSource: "a",
          leftColumn: "id",
          rightSource: "b",
          rightColumn: "id",
        },
      ],
      [
        {
          leftSource: "a",
          leftColumn: "price",
          operator: ">",
          rightSource: "b",
          rightColumn: "min_price",
        },
      ],
      where,
    );

    const env = testEnvWithSources({ a: aItems, b: bItems });
    const rows = await executeJoinTree(joinTree, env, LuaStackFrame.lostFrame);

    expect(rows.length).toBe(1);
    expect((rows[0].rawGet("a") as any).id).toBe(1);
  });

  it("post-join WHERE wrapper only keeps true residual after stripping consumed join predicates", () => {
    const sources: JoinSource[] = [makeSource("a"), makeSource("b")];

    const where = parseExpressionString(
      "a.id == b.id and a.price > b.min_price and a.keep == 1",
    );

    const joinTree = buildJoinTree(
      sources,
      undefined,
      [
        {
          leftSource: "a",
          leftColumn: "id",
          rightSource: "b",
          rightColumn: "id",
        },
      ],
      [
        {
          leftSource: "a",
          leftColumn: "price",
          operator: ">",
          rightSource: "b",
          rightColumn: "min_price",
        },
      ],
      where,
    );

    const residualWhere = stripUsedJoinPredicates(where, joinTree);
    expect(residualWhere).toBeDefined();
    expect(JSON.stringify(residualWhere)).toContain('"property":"keep"');
    expect(JSON.stringify(residualWhere)).toContain('"value":1');
    expect(JSON.stringify(residualWhere)).not.toContain('"min_price"');
    expect(JSON.stringify(residualWhere)).not.toContain('"operator":">"');

    const explain = wrapPlanWithQueryOps(
      explainJoinTree(joinTree, {
        analyze: false,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
      }),
      {
        where: residualWhere,
      },
      new Map(),
    );

    expect(explain.nodeType).toBe("Project");
    expect(explain.children[0].nodeType).toBe("Filter");
    expect(explain.children[0].filterExpr).toBe("(a.keep == 1)");
    expect(explain.children[0].children[0].joinResidualExprs).toEqual([
      "(a.price > b.min_price)",
    ]);
  });

  it("executeJoinTree with multiple residual conjuncts keeps only rows matching all", async () => {
    const aItems = [
      { id: 1, price: 15, keep: 1 },
      { id: 2, price: 25, keep: 1 },
      { id: 3, price: 40, keep: 1 },
    ];
    const bItems = [
      { id: 1, min_price: 10, max_price: 20 },
      { id: 2, min_price: 10, max_price: 20 },
      { id: 3, min_price: 10, max_price: 50 },
    ];

    const sources: JoinSource[] = [
      makeSource("a", aItems),
      makeSource("b", bItems),
    ];

    const where = parseExpressionString(
      "a.id == b.id and a.price > b.min_price and a.price <= b.max_price",
    );

    const joinTree = buildJoinTree(
      sources,
      undefined,
      [
        {
          leftSource: "a",
          leftColumn: "id",
          rightSource: "b",
          rightColumn: "id",
        },
      ],
      [
        {
          leftSource: "a",
          leftColumn: "price",
          operator: ">",
          rightSource: "b",
          rightColumn: "min_price",
        },
        {
          leftSource: "a",
          leftColumn: "price",
          operator: "<=",
          rightSource: "b",
          rightColumn: "max_price",
        },
      ],
      where,
    );

    const env = testEnvWithSources({ a: aItems, b: bItems });
    const rows = await executeJoinTree(joinTree, env, LuaStackFrame.lostFrame);

    expect(rows.length).toBe(2);
    expect((rows[0].rawGet("a") as any).id).toBe(1);
    expect((rows[1].rawGet("a") as any).id).toBe(3);
  });

  it("join residual evaluation works when row values are LuaTable instances", async () => {
    const a1 = new LuaTable();
    void a1.rawSet("id", 1);
    void a1.rawSet("price", 5);

    const a2 = new LuaTable();
    void a2.rawSet("id", 2);
    void a2.rawSet("price", 20);

    const b1 = new LuaTable();
    void b1.rawSet("id", 1);
    void b1.rawSet("min_price", 10);

    const b2 = new LuaTable();
    void b2.rawSet("id", 2);
    void b2.rawSet("min_price", 10);

    const aItems = [a1, a2];
    const bItems = [b1, b2];

    const sources: JoinSource[] = [
      makeSource("a", aItems),
      makeSource("b", bItems),
    ];

    const where = parseExpressionString(
      "a.id == b.id and a.price > b.min_price",
    );

    const joinTree = buildJoinTree(
      sources,
      undefined,
      [
        {
          leftSource: "a",
          leftColumn: "id",
          rightSource: "b",
          rightColumn: "id",
        },
      ],
      [
        {
          leftSource: "a",
          leftColumn: "price",
          operator: ">",
          rightSource: "b",
          rightColumn: "min_price",
        },
      ],
      where,
    );

    const env = testEnvWithSources({ a: aItems, b: bItems });
    const rows = await executeJoinTree(joinTree, env, LuaStackFrame.lostFrame);

    expect(rows.length).toBe(1);
    const a = rows[0].rawGet("a") as LuaTable;
    const b = rows[0].rawGet("b") as LuaTable;
    expect(a.rawGet("id")).toBe(2);
    expect(b.rawGet("id")).toBe(2);
  });

  it("residual-only three-source join filters at the lowest covering join", async () => {
    const aItems = [{ x: 1 }, { x: 2 }, { x: 3 }];
    const bItems = [{ y: 10 }, { y: 20 }];
    const cItems = [{ z: 100 }];

    const sources: JoinSource[] = [
      makeSource("a", aItems),
      makeSource("b", bItems),
      makeSource("c", cItems),
    ];

    const where = parseExpressionString("a.x + b.y > 15");

    const joinTree = buildJoinTree(
      sources,
      undefined,
      undefined,
      undefined,
      where,
    );

    const env = testEnvWithSources({ a: aItems, b: bItems, c: cItems });
    const rows = await executeJoinTree(joinTree, env, LuaStackFrame.lostFrame);

    expect(rows.length).toBe(3);
    const totals = rows.map((row) => {
      const a = row.rawGet("a") as any;
      const b = row.rawGet("b") as any;
      const c = row.rawGet("c") as any;
      return a.x + b.y + c.z;
    });
    expect(totals).toEqual([121, 122, 123]);

    const residualWhere = stripUsedJoinPredicates(where, joinTree);
    expect(residualWhere).toBeUndefined();
  });
});

describe("aggregate detection uses configured aggregate registry", () => {
  it("treats config-defined aggregate functions as aggregates in explain planning", () => {
    const plan: ExplainNode = {
      nodeType: "Scan",
      source: "t",
      startupCost: 0,
      estimatedCost: 100,
      estimatedRows: 100,
      estimatedWidth: 5,
      children: [],
    };

    const query = {
      select: parseExpressionString("myagg(t.value)"),
    };

    const config = {
      get(path: string, defaultValue?: any) {
        if (path === "aggregates.myagg") {
          return {
            name: "myagg",
            initialize: () => 0,
            iterate: () => 0,
          };
        }
        return defaultValue;
      },
    };

    const wrapped = wrapPlanWithQueryOps(
      plan,
      query,
      undefined,
      undefined,
      undefined,
      config as any,
    );

    expect(wrapped.nodeType).toBe("Project");
    expect(wrapped.children).toHaveLength(1);

    const aggNode = wrapped.children[0];
    expect(aggNode.nodeType).toBe("GroupAggregate");
    expect(aggNode.implicitGroup).toBe(true);
    expect(aggNode.estimatedRows).toBe(1);
    expect(aggNode.outputColumns).toEqual(["myagg(t.value)"]);
    expect(aggNode.aggregates).toEqual([
      {
        name: "myagg",
        args: "t.value",
      },
    ]);
  });

  it("treats configured aggregate aliases as aggregates in explain planning", () => {
    const plan: ExplainNode = {
      nodeType: "Scan",
      source: "t",
      startupCost: 0,
      estimatedCost: 100,
      estimatedRows: 100,
      estimatedWidth: 5,
      children: [],
    };

    const query = {
      select: parseExpressionString("aliasagg(t.value)"),
    };

    const config = {
      get(path: string, defaultValue?: any) {
        if (path === "aggregates.aliasagg") {
          return {
            alias: "sum",
          };
        }
        return defaultValue;
      },
    };

    const wrapped = wrapPlanWithQueryOps(
      plan,
      query,
      undefined,
      undefined,
      undefined,
      config as any,
    );

    expect(wrapped.nodeType).toBe("Project");
    expect(wrapped.children).toHaveLength(1);

    const aggNode = wrapped.children[0];
    expect(aggNode.nodeType).toBe("GroupAggregate");
    expect(aggNode.implicitGroup).toBe(true);
    expect(aggNode.estimatedRows).toBe(1);
    expect(aggNode.aggregates).toEqual([
      {
        name: "aliasagg",
        args: "t.value",
      },
    ]);
  });

  it("does not classify unknown functions as aggregates", () => {
    const plan: ExplainNode = {
      nodeType: "Scan",
      source: "t",
      startupCost: 0,
      estimatedCost: 100,
      estimatedRows: 100,
      estimatedWidth: 5,
      children: [],
    };

    const query = {
      select: parseExpressionString("unknown_fn(t.value)"),
    };

    const config = {
      get(_path: string, defaultValue?: any) {
        return defaultValue;
      },
    };

    const wrapped = wrapPlanWithQueryOps(
      plan,
      query,
      undefined,
      undefined,
      undefined,
      config as any,
    );

    expect(wrapped.nodeType).toBe("Project");
    expect(wrapped.children).toHaveLength(1);

    const child = wrapped.children[0];
    expect(child.nodeType).not.toBe("GroupAggregate");
  });
});
