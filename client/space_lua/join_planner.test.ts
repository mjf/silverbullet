import { describe, expect, it } from "vitest";
import {
  attachAnalyzeQueryOpStats,
  buildJoinTree,
  buildLeadingHintInfo,
  buildNormalizationInfoBySource,
  collectScanSourceOrder,
  executeAndInstrument,
  executeJoinTree,
  explainJoinTree,
  exprToDisplayString,
  exprToString,
  extractSingleSourceFilters,
  formatExplainOutput,
  normalizePushdownExpression,
  stripOuterParens,
  stripUsedJoinPredicates,
  wrapPlanWithQueryOps,
  type ExplainNode,
  type JoinSource,
} from "./join_planner.ts";
import { parseExpressionString } from "./parse.ts";
import { LuaEnv, LuaStackFrame, LuaTable } from "./runtime.ts";
import { Config } from "../config.ts";

function analyzeOpts() {
  return {
    analyze: true,
    verbose: true,
    summary: false,
    costs: true,
    timing: false,
    hints: false,
  } as const;
}

function leafNamesInOrder(tree: any): string[] {
  const out: string[] = [];
  const walk = (n: any) => {
    if (n.kind === "leaf") {
      out.push(n.source.name);
      return;
    }
    walk(n.left);
    walk(n.right);
  };
  walk(tree);
  return out;
}

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
  // No `executionCapabilities`: emulates a materialised source with no engine
  // pushdown support, which is what most tests in this suite want.  The
  // planner treats a missing `engines` array as "no capabilities advertised"
  // (same as `engines: []`), which exactly matches the old
  // `predicatePushdown: "none"` intent.
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

describe("leading join order hints", () => {
  it("leading forces prefix while the remaining suffix is still optimized", () => {
    const sources: JoinSource[] = [
      {
        ...makeSource("a"),
        stats: {
          ...makeSource("a").stats!,
          rowCount: 1000,
        },
      },
      {
        ...makeSource("b"),
        stats: {
          ...makeSource("b").stats!,
          rowCount: 5,
        },
      },
      {
        ...makeSource("c"),
        stats: {
          ...makeSource("c").stats!,
          rowCount: 900,
        },
      },
      {
        ...makeSource("d"),
        stats: {
          ...makeSource("d").stats!,
          rowCount: 10,
        },
      },
    ];

    const tree = buildJoinTree(sources, ["a", "c"]);
    const order = leafNamesInOrder(tree);

    expect(order.slice(0, 2)).toEqual(["a", "c"]);
    expect(order.slice(2)).toEqual(["b", "d"]);
  });

  it("leading full list fixes the complete join order", () => {
    const sources: JoinSource[] = [
      {
        ...makeSource("a"),
        stats: {
          ...makeSource("a").stats!,
          rowCount: 1000,
        },
      },
      {
        ...makeSource("b"),
        stats: {
          ...makeSource("b").stats!,
          rowCount: 5,
        },
      },
      {
        ...makeSource("c"),
        stats: {
          ...makeSource("c").stats!,
          rowCount: 900,
        },
      },
    ];

    const tree = buildJoinTree(sources, ["c", "a", "b"]);
    const order = leafNamesInOrder(tree);

    expect(order).toEqual(["c", "a", "b"]);
  });

  it("leading preserves prefix and still allows hinted suffix choice", () => {
    const sources: JoinSource[] = [
      {
        ...makeSource("a"),
        stats: {
          ...makeSource("a").stats!,
          rowCount: 100,
        },
      },
      {
        ...makeSource("b"),
        stats: {
          ...makeSource("b").stats!,
          rowCount: 1000,
        },
      },
      {
        ...makeSource("c"),
        hint: {
          type: "JoinHint",
          kind: "loop",
          ctx: {} as any,
        },
        stats: {
          ...makeSource("c").stats!,
          rowCount: 2,
        },
      },
    ];

    const tree = buildJoinTree(sources, ["a"]);
    const order = leafNamesInOrder(tree);

    expect(order[0]).toBe("a");
    expect(order.slice(1)).toEqual(["c", "b"]);

    const joins: any[] = [];
    const collect = (n: any) => {
      if (n.kind === "join") {
        joins.push(n);
        collect(n.left);
        collect(n.right);
      }
    };
    collect(tree);

    const joinWithC = joins.find(
      (j) => j.right.kind === "leaf" && j.right.source.name === "c",
    );
    expect(joinWithC?.method).toBe("loop");
  });
});

describe("leading hint in explain output", () => {
  function sourcesForLeadingTest(): JoinSource[] {
    return [
      {
        ...makeSource("a"),
        stats: { ...makeSource("a").stats!, rowCount: 50 },
      },
      {
        ...makeSource("b"),
        stats: { ...makeSource("b").stats!, rowCount: 80 },
      },
      {
        ...makeSource("c"),
        stats: { ...makeSource("c").stats!, rowCount: 70 },
      },
      {
        ...makeSource("d"),
        stats: { ...makeSource("d").stats!, rowCount: 60 },
      },
      {
        ...makeSource("e"),
        stats: { ...makeSource("e").stats!, rowCount: 30 },
      },
      {
        ...makeSource("f"),
        stats: { ...makeSource("f").stats!, rowCount: 40 },
      },
    ];
  }

  function hintOpts() {
    return {
      analyze: false,
      verbose: true,
      summary: false,
      costs: true,
      timing: false,
      hints: true,
    } as const;
  }

  it("collectScanSourceOrder returns leaves in join-tree execution order", () => {
    const sources = sourcesForLeadingTest();
    const tree = buildJoinTree(sources, ["a", "c", "b"]);
    const plan = explainJoinTree(tree, hintOpts());
    const order = collectScanSourceOrder(plan);
    expect(order.slice(0, 3)).toEqual(["a", "c", "b"]);
    expect(order.slice(3).sort()).toEqual(["d", "e", "f"]);
  });

  it("buildLeadingHintInfo reports fixed prefix and planner-chosen suffix", () => {
    const sources = sourcesForLeadingTest();
    const tree = buildJoinTree(sources, ["a", "c", "b"]);
    const plan = explainJoinTree(tree, hintOpts());

    const info = buildLeadingHintInfo(["a", "c", "b"], plan);
    expect(info).toBeDefined();
    expect(info!.requested).toEqual(["a", "c", "b"]);
    expect(info!.fixed).toEqual(["a", "c", "b"]);
    expect(info!.plannerChosen.length).toBe(3);
    expect(info!.plannerChosen.sort()).toEqual(["d", "e", "f"]);
    expect(info!.finalOrder.length).toBe(6);
    expect(info!.finalOrder.slice(0, 3)).toEqual(["a", "c", "b"]);
  });

  it("buildLeadingHintInfo returns undefined when no leading clause is given", () => {
    const sources = sourcesForLeadingTest();
    const tree = buildJoinTree(sources);
    const plan = explainJoinTree(tree, hintOpts());
    expect(buildLeadingHintInfo(undefined, plan)).toBeUndefined();
    expect(buildLeadingHintInfo([], plan)).toBeUndefined();
  });

  it("formatExplainOutput renders leading hint preamble when hints enabled", () => {
    const sources = sourcesForLeadingTest();
    const tree = buildJoinTree(sources, ["a", "c", "b"]);
    const plan = explainJoinTree(tree, hintOpts());
    const rendered = formatExplainOutput(
      {
        plan,
        planningTimeMs: 0,
        leadingHint: buildLeadingHintInfo(["a", "c", "b"], plan),
      },
      hintOpts(),
    );

    expect(rendered.includes("Leading Hint")).toBe(true);
    expect(rendered.includes("Requested:      a, c, b")).toBe(true);
    expect(rendered.includes("Fixed by hint:  a, c, b")).toBe(true);
    // Final order begins with the fixed prefix.
    expect(rendered).toMatch(/Final order:\s+a, c, b, /);
    // Planner-chosen suffix contains exactly d, e, f (any order).
    expect(rendered).toMatch(/Planner-chosen: (?:[def], ?){2}[def]/);
  });

  it("leading hint preamble is omitted when hints option is disabled", () => {
    const sources = sourcesForLeadingTest();
    const tree = buildJoinTree(sources, ["a", "c", "b"]);
    const plan = explainJoinTree(tree, {
      ...hintOpts(),
      hints: false,
    });
    const rendered = formatExplainOutput(
      {
        plan,
        planningTimeMs: 0,
        leadingHint: buildLeadingHintInfo(["a", "c", "b"], plan),
      },
      {
        ...hintOpts(),
        hints: false,
      },
    );

    expect(rendered.includes("Leading Hint")).toBe(false);
    expect(rendered.includes("Requested:")).toBe(false);
  });

  it("leading hint preamble is omitted when no hint was given even if hints enabled", () => {
    const sources = sourcesForLeadingTest();
    const tree = buildJoinTree(sources);
    const plan = explainJoinTree(tree, hintOpts());
    const rendered = formatExplainOutput(
      {
        plan,
        planningTimeMs: 0,
        leadingHint: buildLeadingHintInfo(undefined, plan),
      },
      hintOpts(),
    );

    expect(rendered.includes("Leading Hint")).toBe(false);
  });

  it("leading hint no longer annotates the root node itself", () => {
    const sources = sourcesForLeadingTest();
    const tree = buildJoinTree(sources, ["a", "c", "b"]);
    const plan = wrapPlanWithQueryOps(explainJoinTree(tree, hintOpts()), {
      leading: ["a", "c", "b"],
    });

    const walk = (n: ExplainNode): boolean => {
      if ((n as any).leadingHint !== undefined) return true;
      return n.children.some(walk);
    };
    expect(walk(plan)).toBe(false);
  });
});

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
      hints: false,
    });

    expect(explain.joinResidualExprs).toEqual(["a.price > b.min_price"]);

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
        hints: false,
      },
    );

    expect(
      rendered.includes("Residual Join Filter: a.price > b.min_price"),
    ).toBe(true);
    expect(rendered.includes("Hash Condition: a.id == b.id")).toBe(true);
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
      hints: false,
    });

    expect(explain.joinResidualExprs).toEqual([
      "a.price > b.min_price",
      "a.price <= b.max_price",
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

describe("single-source normalization metadata", () => {
  it("builds complete normalization info when all source-local conjuncts are pushable", () => {
    const expr = parseExpressionString("a.x == 1 and a.y == 2");
    const info = buildNormalizationInfoBySource(expr, new Set(["a", "b"]));

    expect(info.get("a")).toEqual({
      state: "complete",
      originalExpr: "(a.x == 1) and (a.y == 2)",
      normalizedExpr: "(a.x == 1) and (a.y == 2)",
      pushdownExpr: "(a.x == 1) and (a.y == 2)",
      leftoverExpr: "none",
    });
    expect(info.has("b")).toBe(false);
  });

  it("builds partial normalization info when a source-local leftover remains", () => {
    const expr = parseExpressionString("a.x == 1 and unknown_fn(a.y)");
    const info = buildNormalizationInfoBySource(expr, new Set(["a", "b"]));

    expect(info.get("a")).toEqual({
      state: "partial",
      originalExpr: "(a.x == 1) and unknown_fn(a.y)",
      normalizedExpr: "(a.x == 1) and unknown_fn(a.y)",
      pushdownExpr: "a.x == 1",
      leftoverExpr: "unknown_fn(a.y)",
    });
  });

  it("preserves user's original predicate when normalization rewrites it", () => {
    const expr = parseExpressionString("not (a.x in {1, 2}) and a.y == 3");
    const info = buildNormalizationInfoBySource(expr, new Set(["a"]));

    const entry = info.get("a");
    expect(entry).toBeDefined();
    expect(entry!.state).toBe("complete");
    expect(entry!.originalExpr).toContain("not");
    expect(entry!.originalExpr).toContain(" in ");
    expect(entry!.pushdownExpr).toContain("a.x ~= 1");
    expect(entry!.pushdownExpr).toContain("a.x ~= 2");
    expect(entry!.pushdownExpr).toContain("a.y == 3");
    expect(entry!.normalizedExpr).toContain("a.x ~= 1");
    expect(entry!.normalizedExpr).toContain("a.x ~= 2");
    expect(entry!.leftoverExpr).toBe("none");
  });

  it("explain leaf renders partial normalization lines", () => {
    const source = makeSource("a");
    const plan = explainJoinTree(
      { kind: "leaf", source },
      {
        analyze: false,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: false,
      },
      new Map([["a", "(a.x == 1)"]]),
      new Map([
        [
          "a",
          {
            state: "partial",
            originalExpr: "(a.x == 1) and unknown_fn(a.y)",
            normalizedExpr: "((a.x == 1) and unknown_fn(a.y))",
            pushdownExpr: "(a.x == 1)",
            leftoverExpr: "unknown_fn(a.y)",
          },
        ],
      ]),
    );

    // Node fields preserve whatever the caller supplied; display-time
    // stripping is applied only when rendering.
    expect(plan.normalizationState).toBe("partial");
    expect(plan.originalPredicateExpr).toBe("(a.x == 1) and unknown_fn(a.y)");
    expect(plan.normalizedPredicateExpr).toBe(
      "((a.x == 1) and unknown_fn(a.y))",
    );
    expect(plan.normalizedPushdownExpr).toBe("(a.x == 1)");
    expect(plan.normalizedLeftoverExpr).toBe("unknown_fn(a.y)");

    const rendered = formatExplainOutput(
      {
        plan,
        planningTimeMs: 0,
      },
      {
        analyze: false,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: false,
      },
    );

    expect(rendered.includes("Normalization: partial")).toBe(true);
    expect(
      rendered.includes("Original Predicate: (a.x == 1) and unknown_fn(a.y)"),
    ).toBe(true);
    expect(
      rendered.includes("Normalized Predicate: (a.x == 1) and unknown_fn(a.y)"),
    ).toBe(true);
    expect(rendered.includes("Normalized Pushdown: a.x == 1")).toBe(true);
    expect(rendered.includes("Normalized Leftover: unknown_fn(a.y)")).toBe(
      true,
    );
  });

  it("does not render normalization lines when no normalization metadata exists", () => {
    const source = makeSource("a");
    const plan = explainJoinTree(
      { kind: "leaf", source },
      {
        analyze: false,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: false,
      },
    );

    const rendered = formatExplainOutput(
      {
        plan,
        planningTimeMs: 0,
      },
      {
        analyze: false,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: false,
      },
    );

    expect(rendered.includes("Normalization:")).toBe(false);
    expect(rendered.includes("Pushdown:")).toBe(false);
    expect(rendered.includes("Leftover:")).toBe(false);
  });

  it("cross-source explain threads normalization metadata to matching scan leaves", () => {
    const sources: JoinSource[] = [makeSource("a"), makeSource("b")];

    const joinTree = buildJoinTree(sources);
    const normalizationInfo = buildNormalizationInfoBySource(
      parseExpressionString("a.x == 1 and unknown_fn(a.y) and b.flag == true"),
      new Set(["a", "b"]),
    );

    const explain = explainJoinTree(
      joinTree,
      {
        analyze: false,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: false,
      },
      new Map([
        ["a", "(a.x == 1)"],
        ["b", "(b.flag == true)"],
      ]),
      normalizationInfo,
    );

    const leaves: ExplainNode[] = [];
    const walk = (node: ExplainNode) => {
      if (node.nodeType === "Scan" || node.nodeType === "FunctionScan") {
        leaves.push(node);
      }
      for (const child of node.children) {
        walk(child);
      }
    };
    walk(explain);

    const aLeaf = leaves.find((l) => l.source === "a");
    const bLeaf = leaves.find((l) => l.source === "b");

    // Node data is the build-time (post-normalization) string with outer
    // parens already removed by `buildNormalizationInfoBySource`.
    expect(aLeaf?.normalizationState).toBe("partial");
    expect(aLeaf?.normalizedPushdownExpr).toBe("a.x == 1");
    expect(aLeaf?.normalizedLeftoverExpr).toBe("unknown_fn(a.y)");
    expect(aLeaf?.originalPredicateExpr).toBe("(a.x == 1) and unknown_fn(a.y)");
    expect(aLeaf?.normalizedPredicateExpr).toBe(
      "(a.x == 1) and unknown_fn(a.y)",
    );

    expect(bLeaf?.normalizationState).toBe("complete");
    expect(bLeaf?.normalizedPushdownExpr).toBe("b.flag == true");
    expect(bLeaf?.normalizedLeftoverExpr).toBe("none");
    expect(bLeaf?.originalPredicateExpr).toBe("b.flag == true");
    expect(bLeaf?.normalizedPredicateExpr).toBe("b.flag == true");
  });

  it("renders original vs rewritten predicate lines for a scan leaf", () => {
    const source = makeSource("a");
    const normalizationInfo = buildNormalizationInfoBySource(
      parseExpressionString(
        "not (a.x in {1, 2}) and a.y == 3 and unknown_fn(a.z)",
      ),
      new Set(["a"]),
    );

    const plan = explainJoinTree(
      { kind: "leaf", source },
      {
        analyze: false,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: false,
      },
      undefined,
      normalizationInfo,
    );

    const rendered = formatExplainOutput(
      {
        plan,
        planningTimeMs: 0,
      },
      {
        analyze: false,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: false,
      },
    );

    // Pre-normalization form is preserved for user readability.
    expect(rendered).toContain("Original Predicate:");
    expect(rendered).toContain("not");
    expect(rendered).toContain(" in ");
    // Post-normalization rewrites `not in {...}` to AND of `~=` disjuncts.
    expect(rendered).toContain("Normalized Predicate:");
    expect(rendered).toMatch(/a\.x ~= 1[\s\S]*a\.x ~= 2/);
    // Separately shows the pushable part and the leftover part.
    expect(rendered).toContain("Normalized Pushdown:");
    expect(rendered).toContain("Normalized Leftover:");
    expect(rendered).toContain("unknown_fn(a.z)");
    expect(rendered).toContain("Normalization: partial");
  });
});

describe("formatExplainOutput node section ordering", () => {
  function requireOrder(rendered: string, labels: string[]): void {
    const indices = labels.map((label) => ({
      label,
      index: rendered.indexOf(label),
    }));
    for (const { label, index } of indices) {
      expect(index, `expected "${label}" to appear in output`).toBeGreaterThan(
        -1,
      );
    }
    for (let i = 1; i < indices.length; i++) {
      expect(
        indices[i].index,
        `expected "${indices[i].label}" after "${indices[i - 1].label}"`,
      ).toBeGreaterThan(indices[i - 1].index);
    }
  }

  function makeSourceWithPushdown(name: string): JoinSource {
    return {
      name,
      expression: parseExpressionString(name),
      stats: {
        rowCount: 100,
        ndv: new Map<string, number>([
          ["id", 100],
          ["x", 100],
          ["y", 100],
        ]),
        avgColumnCount: 4,
        statsSource: "computed-exact-small",
        executionCapabilities: {
          engines: [
            {
              id: "bitmap",
              name: "bitmap",
              kind: "bitmap",
              capabilities: [
                "scan-bitmap",
                "stage-where",
                "pred-eq",
                "pred-neq",
                "pred-in",
                "bool-and",
                "bool-not",
              ],
              baseCostWeight: 0.6,
              priority: 20,
            },
          ],
        },
      },
    };
  }

  it("join node pairs each condition/filter with its 'Rows Removed' stat in order", async () => {
    const aItems = [
      { id: 1, price: 5, keep: 1 },
      { id: 2, price: 20, keep: 1 },
      { id: 3, price: 30, keep: 1 },
    ];
    const bItems = [
      { id: 1, min_price: 10 },
      { id: 2, min_price: 10 },
      { id: 3, min_price: 50 },
    ];

    const sources: JoinSource[] = [
      {
        ...makeSource("a", aItems),
        hint: {
          type: "JoinHint",
          kind: "loop",
          ctx: {} as any,
        },
      },
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

    const explainOpts = {
      analyze: true,
      verbose: true,
      summary: false,
      costs: true,
      timing: false,
      hints: false,
    } as const;

    const plan = explainJoinTree(joinTree, explainOpts);
    const env = testEnvWithSources({ a: aItems, b: bItems });
    await executeAndInstrument(
      joinTree,
      plan,
      env,
      LuaStackFrame.lostFrame,
      explainOpts,
      undefined,
      undefined,
      0,
    );

    const rendered = formatExplainOutput(
      { plan, planningTimeMs: 0 },
      explainOpts,
    );

    // Join Filter (equi) → Residual Join Filter → Rows Removed by Join Filter:
    // predicate definitions come first, the runtime stat sits immediately
    // next to them.
    requireOrder(rendered, [
      "Join Filter: a.id == b.id",
      "Residual Join Filter: a.price > b.min_price",
      "Rows Removed by Join Filter:",
    ]);
  });

  it("scan leaf orders: filter → rows removed → hints → pushdown detail → engine → estimation", () => {
    const source = makeSourceWithPushdown("a");
    const plan = explainJoinTree(
      { kind: "leaf", source },
      {
        analyze: false,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: false,
      },
      new Map([["a", "(a.x == 1)"]]),
      new Map([
        [
          "a",
          {
            state: "partial",
            originalExpr: "(a.x == 1) and unknown_fn(a.y)",
            normalizedExpr: "((a.x == 1) and unknown_fn(a.y))",
            pushdownExpr: "(a.x == 1)",
            leftoverExpr: "unknown_fn(a.y)",
          },
        ],
      ]),
    );
    plan.rowsRemovedByFilter = 5;
    plan.actualRows = 10;

    const rendered = formatExplainOutput(
      { plan, planningTimeMs: 0 },
      {
        analyze: true,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: false,
      },
    );

    requireOrder(rendered, [
      "Pushdown Filter: a.x == 1",
      "Rows Removed by Pushdown Filter: 5",
      "Pushdown Capability:",
      "Normalization: partial",
      "Original Predicate:",
      "Normalized Predicate:",
      "Normalized Pushdown:",
      "Normalized Leftover:",
      "Execution Scan:",
      "Stats: computed-exact-small",
    ]);
  });

  it("GroupAggregate node orders its own lines: Group Key → Aggregate → Stats", () => {
    const basePlan: ExplainNode = {
      nodeType: "Scan",
      source: "t",
      startupCost: 0,
      estimatedCost: 100,
      estimatedRows: 100,
      estimatedWidth: 5,
      statsSource: "computed-exact-small",
      children: [],
    };

    const wrapped = wrapPlanWithQueryOps(
      basePlan,
      {
        groupBy: [{ expr: parseExpressionString("t.g") }],
        select: parseExpressionString("{ g = t.g, s = sum(t.v) }"),
      },
      undefined,
      undefined,
      undefined,
      new Config(),
    );

    // Locate the GroupAggregate sub-block within the full render so we can
    // check ordering inside that one node specifically.
    const rendered = formatExplainOutput(
      { plan: wrapped, planningTimeMs: 0 },
      {
        analyze: false,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: false,
      },
    );

    const lines = rendered.split("\n");
    const start = lines.findIndex((l) => l.includes("Group Aggregate"));
    expect(start).toBeGreaterThanOrEqual(0);
    // Sub-block ends at the next `->` sibling or eof; take the next ~10 lines.
    const block = lines.slice(start, start + 10).join("\n");

    requireOrder(block, [
      "Group Aggregate",
      "Group Key: t.g",
      "Aggregate: sum(t.v)",
      "Stats: computed-exact-small",
    ]);
  });

  it("Limit node exposes Count/Offset before operator stats", () => {
    const basePlan: ExplainNode = {
      nodeType: "Scan",
      source: "t",
      startupCost: 0,
      estimatedCost: 100,
      estimatedRows: 100,
      estimatedWidth: 5,
      statsSource: "computed-exact-small",
      children: [],
    };

    const wrapped = wrapPlanWithQueryOps(basePlan, {
      limit: 10,
      offset: 5,
    });

    const rendered = formatExplainOutput(
      { plan: wrapped, planningTimeMs: 0 },
      {
        analyze: false,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: false,
      },
    );

    requireOrder(rendered, ["Limit", "Count: 10", "Offset: 5"]);
  });

  it("verbose section comes strictly after all non-verbose operator lines", () => {
    const source = makeSourceWithPushdown("a");
    const plan = explainJoinTree(
      { kind: "leaf", source },
      {
        analyze: true,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: true,
      },
      new Map([["a", "(a.x == 1)"]]),
      new Map([
        [
          "a",
          {
            state: "complete",
            originalExpr: "(a.x == 1)",
            normalizedExpr: "(a.x == 1)",
            pushdownExpr: "(a.x == 1)",
            leftoverExpr: "none",
          },
        ],
      ]),
    );
    plan.rowsRemovedByFilter = 3;
    plan.actualRows = 7;

    const rendered = formatExplainOutput(
      { plan, planningTimeMs: 0 },
      {
        analyze: true,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: true,
      },
    );

    // `Rows Removed by Pushdown Filter` is operator-level (non-verbose);
    // everything verbose (Pushdown Capability, Normalization, Execution
    // Scan, Stats) must appear strictly after it.
    requireOrder(rendered, [
      "Rows Removed by Pushdown Filter: 3",
      "Pushdown Capability:",
      "Normalization: complete",
      "Execution Scan:",
      "Stats:",
    ]);
  });
});

describe("expression paren normalization", () => {
  it("stripOuterParens removes one enclosing pair only", () => {
    expect(stripOuterParens("(a.x == 1)")).toBe("a.x == 1");
    expect(stripOuterParens("((a) and (b))")).toBe("(a) and (b)");
    expect(stripOuterParens("(a) or (b)")).toBe("(a) or (b)");
    expect(stripOuterParens("unknown_fn(x)")).toBe("unknown_fn(x)");
    expect(stripOuterParens("")).toBe("");
    expect(stripOuterParens("()")).toBe("");
  });

  it("exprToDisplayString drops the outermost parens from Binary expressions", () => {
    const expr = parseExpressionString("a.x == 1");
    expect(exprToString(expr)).toBe("(a.x == 1)");
    expect(exprToDisplayString(expr)).toBe("a.x == 1");
  });

  it("exprToDisplayString keeps nested parens intact", () => {
    const expr = parseExpressionString("(a.x == 1) and (a.y == 2)");
    expect(exprToString(expr)).toBe("((a.x == 1) and (a.y == 2))");
    expect(exprToDisplayString(expr)).toBe("(a.x == 1) and (a.y == 2)");
  });

  it("every expression surface in the rendered output is paren-free at the outer level", async () => {
    const aItems = [
      { id: 1, x: 1, y: 3, keep: true, price: 10 },
      { id: 2, x: 2, y: 3, keep: true, price: 20 },
      { id: 3, x: 5, y: 3, keep: false, price: 30 },
    ];
    const bItems = [
      { id: 1, min_price: 5 },
      { id: 2, min_price: 15 },
      { id: 3, min_price: 50 },
    ];

    const sources: JoinSource[] = [
      makeSource("a", aItems),
      makeSource("b", bItems),
    ];

    const where = parseExpressionString(
      "a.id == b.id and a.price > b.min_price and a.keep == true",
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

    const normInfo = buildNormalizationInfoBySource(where, new Set(["a", "b"]));
    const pushedByName = new Map<string, string>();
    for (const [name, info] of normInfo) {
      pushedByName.set(name, info.pushdownExpr);
    }

    const explainOpts = {
      analyze: false,
      verbose: true,
      summary: false,
      costs: false,
      timing: false,
      hints: false,
    } as const;

    const plan = wrapPlanWithQueryOps(
      explainJoinTree(joinTree, explainOpts, pushedByName, normInfo),
      {
        where: residualWhere,
        orderBy: [
          {
            expr: parseExpressionString("a.id"),
            desc: false,
          },
        ],
        groupBy: [{ expr: parseExpressionString("a.keep") }],
        select: parseExpressionString(
          "{ k = a.keep, total = sum(a.price) filter(where a.price > 5) }",
        ),
      },
      undefined,
      undefined,
      undefined,
      new Config(),
    );

    const rendered = formatExplainOutput(
      { plan, planningTimeMs: 0 },
      explainOpts,
    );

    // Every line that carries an expression after a `<label>:` prefix must
    // NOT have its expression wrapped in redundant outer parens.
    const labelPrefixes = [
      "Filter",
      "Pushdown Filter",
      "Having Condition",
      "Aggregate Filter",
      "Hash Condition",
      "Merge Condition",
      "Join Filter",
      "Residual Join Filter",
      "Original Predicate",
      "Normalized Predicate",
      "Normalized Pushdown",
      "Normalized Leftover",
    ];

    for (const line of rendered.split("\n")) {
      const trimmed = line.trim();
      for (const prefix of labelPrefixes) {
        const marker = `${prefix}: `;
        const idx = trimmed.indexOf(marker);
        if (idx !== 0) continue;
        const value = trimmed.slice(marker.length);
        // An outer `(...)` pair that encloses the whole value is redundant.
        expect(
          !(
            value.length >= 2 &&
            value.startsWith("(") &&
            value.endsWith(")") &&
            stripOuterParens(value) !== value
          ),
          `line "${line}" still has redundant outer parens`,
        ).toBe(true);
      }
    }

    // Positive: expected paren-free strings are present for the sample
    // query.  This dataset picks Nested Loop over Hash Join, so the
    // equi-condition is labelled `Join Filter:`.
    expect(rendered).toContain("Sort Key: a.id");
    expect(rendered).toContain("Group Key: a.keep");
    expect(rendered).toContain(
      "Output: k = a.keep, total = sum(a.price) filter(a.price > 5)",
    );
    expect(rendered).toContain("Join Filter: a.id == b.id");
    expect(rendered).toContain("Residual Join Filter: a.price > b.min_price");
    expect(rendered).toContain("Filter: a.keep == true");
    expect(rendered).toContain("Pushdown Filter: a.keep == true");
    expect(rendered).toContain("Original Predicate: a.keep == true");
    expect(rendered).toContain("Normalized Predicate: a.keep == true");
    expect(rendered).toContain("Normalized Pushdown: a.keep == true");
    expect(rendered).toContain(
      "Aggregate Filter: sum(a.price) filter(a.price > 5)",
    );
    expect(rendered).toContain("Aggregate: sum(a.price) filter(a.price > 5)");
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
        hints: false,
      }),
      {
        where: residualWhere,
      },
      new Map(),
    );

    expect(explain.nodeType).toBe("Project");
    expect(explain.children[0].nodeType).toBe("Filter");
    expect(explain.children[0].filterExpr).toBe("a.keep == 1");
    expect(explain.children[0].children[0].joinResidualExprs).toEqual([
      "a.price > b.min_price",
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

describe("source with-hints in explain and planning", () => {
  it("leaf explain uses rows, width, and cost hints", () => {
    const source: JoinSource = {
      name: "p",
      expression: parseExpressionString("p"),
      stats: {
        rowCount: 100,
        ndv: new Map([["id", 100]]),
        avgColumnCount: 8,
        statsSource: "computed-exact-small",
      },
      withHints: {
        rows: 7,
        width: 3,
        cost: 11,
      } as any,
    };

    const plan = explainJoinTree(
      { kind: "leaf", source },
      {
        analyze: false,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: true,
      },
    );

    expect(plan.nodeType).toBe("Scan");
    expect(plan.estimatedRows).toBe(7);
    expect(plan.estimatedWidth).toBe(3);
    expect(plan.estimatedCost).toBe(11);
    expect(plan.statsSource).toBe("computed-exact-small");
    expect(plan.sourceHints).toEqual(["rows=7", "width=3", "cost=11"]);
  });

  it("leaf explain includes materialized together with source hints", () => {
    const source: JoinSource = {
      name: "p",
      expression: parseExpressionString("p"),
      materialized: true,
      stats: {
        rowCount: 100,
        ndv: new Map([["id", 100]]),
        avgColumnCount: 8,
        statsSource: "computed-exact-small",
      },
      withHints: {
        rows: 5,
        width: 2,
        cost: 13,
      } as any,
    };

    const rendered = formatExplainOutput(
      {
        plan: explainJoinTree(
          { kind: "leaf", source },
          {
            analyze: false,
            verbose: true,
            summary: false,
            costs: true,
            timing: false,
            hints: true,
          },
        ),
        planningTimeMs: 0,
      },
      {
        analyze: false,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: true,
      },
    );

    expect(
      rendered.includes("Hints: materialized, rows=5, width=2, cost=13"),
    ).toBe(true);
    expect(rendered.includes("Stats: computed-exact-small")).toBe(true);
  });

  it("does not render Hints line when hints option is disabled", () => {
    const source: JoinSource = {
      name: "p",
      expression: parseExpressionString("p"),
      stats: {
        rowCount: 100,
        ndv: new Map([["id", 100]]),
        avgColumnCount: 8,
        statsSource: "computed-exact-small",
      },
      withHints: {
        rows: 7,
        width: 3,
        cost: 11,
      } as any,
    };

    const rendered = formatExplainOutput(
      {
        plan: explainJoinTree(
          { kind: "leaf", source },
          {
            analyze: false,
            verbose: true,
            summary: false,
            costs: true,
            timing: false,
            hints: false,
          },
        ),
        planningTimeMs: 0,
      },
      {
        analyze: false,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: false,
      },
    );

    expect(rendered.includes("Hints:")).toBe(false);
    expect(rendered.includes("Stats: computed-exact-small")).toBe(true);
  });

  it("join tree estimation uses hinted rows on leaf sources", () => {
    const left: JoinSource = {
      ...makeSource("a"),
      withHints: {
        rows: 5,
        width: 2,
      } as any,
    };
    const right: JoinSource = makeSource("b");

    const tree = buildJoinTree([left, right]);

    expect(tree.kind).toBe("join");
    if (tree.kind !== "join") {
      throw new Error("expected join");
    }

    const leftPlan = explainJoinTree(tree, {
      analyze: false,
      verbose: true,
      summary: false,
      costs: true,
      timing: false,
      hints: true,
    }).children[0];

    expect(leftPlan.estimatedRows).toBe(5);
    expect(leftPlan.estimatedWidth).toBe(2);
    expect(leftPlan.statsSource).toBe("computed-exact-small");
  });

  it("hinted source cost affects join estimated cost", () => {
    const sourceA: JoinSource = {
      ...makeSource("a"),
      withHints: {
        rows: 10,
        width: 2,
        cost: 1,
      } as any,
    };
    const sourceB: JoinSource = {
      ...makeSource("b"),
      withHints: {
        rows: 10,
        width: 2,
        cost: 1000,
      } as any,
    };

    const planA = explainJoinTree(
      { kind: "leaf", source: sourceA },
      {
        analyze: false,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: true,
      },
    );
    const planB = explainJoinTree(
      { kind: "leaf", source: sourceB },
      {
        analyze: false,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: true,
      },
    );

    expect(planA.estimatedCost).toBe(1);
    expect(planB.estimatedCost).toBe(1000);
  });
});

describe("aggregate-local explain nodes", () => {
  const basePlan: ExplainNode = {
    nodeType: "Scan",
    source: "t",
    startupCost: 0,
    estimatedCost: 100,
    estimatedRows: 100,
    estimatedWidth: 5,
    children: [],
  };

  it("adds Filter (Aggregate) node for aggregate filter clause", () => {
    const wrapped = wrapPlanWithQueryOps(
      basePlan,
      {
        select: parseExpressionString(
          "{ total = sum(t.v) filter(where t.v > 10) }",
        ),
      },
      undefined,
      undefined,
      undefined,
      undefined,
    );

    expect(wrapped.nodeType).toBe("Project");
    expect(wrapped.children).toHaveLength(1);

    const aggNode = wrapped.children[0];
    expect(aggNode.nodeType).toBe("GroupAggregate");
    expect(aggNode.children).toHaveLength(1);

    const filterNode = aggNode.children[0];
    expect(filterNode.nodeType).toBe("Filter");
    expect(filterNode.filterType).toBe("aggregate");
    expect(filterNode.filterExpr).toContain("sum(t.v) filter(t.v > 10)");
  });

  it("renders Filter (Aggregate) in explain output", () => {
    const wrapped = wrapPlanWithQueryOps(
      basePlan,
      {
        select: parseExpressionString(
          "{ total = sum(t.v) filter(where t.v > 10) }",
        ),
      },
      undefined,
      undefined,
      undefined,
      undefined,
    );

    const rendered = formatExplainOutput(
      {
        plan: wrapped,
        planningTimeMs: 0,
      },
      {
        analyze: false,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: false,
      },
    );

    expect(rendered.includes("Filter (Aggregate)")).toBe(true);
    expect(
      rendered.includes("Aggregate Filter: sum(t.v) filter(t.v > 10)"),
    ).toBe(true);
  });

  it("adds Sort (Group) node for aggregate-local order by", () => {
    const wrapped = wrapPlanWithQueryOps(
      basePlan,
      {
        select: parseExpressionString(
          "{ xs = array_agg(t.v order by t.k desc) }",
        ),
      },
      undefined,
      undefined,
      undefined,
      undefined,
    );

    expect(wrapped.nodeType).toBe("Project");
    expect(wrapped.children).toHaveLength(1);

    const aggNode = wrapped.children[0];
    expect(aggNode.nodeType).toBe("GroupAggregate");
    expect(aggNode.children).toHaveLength(1);

    const sortNode = aggNode.children[0];
    expect(sortNode.nodeType).toBe("Sort");
    expect(sortNode.sortType).toBe("group");
    expect(sortNode.sortKeys).toEqual(["t.k desc"]);
  });

  it("renders Sort (Group) in explain output", () => {
    const wrapped = wrapPlanWithQueryOps(
      basePlan,
      {
        select: parseExpressionString(
          "{ xs = array_agg(t.v order by t.k desc) }",
        ),
      },
      undefined,
      undefined,
      undefined,
      undefined,
    );

    const rendered = formatExplainOutput(
      {
        plan: wrapped,
        planningTimeMs: 0,
      },
      {
        analyze: false,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: false,
      },
    );

    expect(rendered.includes("Sort (Group)")).toBe(true);
    expect(rendered.includes("Sort Key (Group): t.k desc")).toBe(true);
  });

  it("nests aggregate filter above group sort when both are present", () => {
    const wrapped = wrapPlanWithQueryOps(
      basePlan,
      {
        select: parseExpressionString(
          "{ xs = array_agg(t.v order by t.k asc) filter(where t.keep == true) }",
        ),
      },
      undefined,
      undefined,
      undefined,
      undefined,
    );

    expect(wrapped.nodeType).toBe("Project");
    const aggNode = wrapped.children[0];
    expect(aggNode.nodeType).toBe("GroupAggregate");

    const filterNode = aggNode.children[0];
    expect(filterNode.nodeType).toBe("Filter");
    expect(filterNode.filterType).toBe("aggregate");

    const sortNode = filterNode.children[0];
    expect(sortNode.nodeType).toBe("Sort");
    expect(sortNode.sortType).toBe("group");
    expect(sortNode.sortKeys).toEqual(["t.k"]);
  });

  it("renders aggregate-local sort and filter together", () => {
    const wrapped = wrapPlanWithQueryOps(
      basePlan,
      {
        select: parseExpressionString(
          "{ xs = array_agg(t.v order by t.k asc) filter(where t.keep == true) }",
        ),
      },
      undefined,
      undefined,
      undefined,
      undefined,
    );

    const rendered = formatExplainOutput(
      {
        plan: wrapped,
        planningTimeMs: 0,
      },
      {
        analyze: false,
        verbose: true,
        summary: false,
        costs: true,
        timing: false,
        hints: false,
      },
    );

    expect(rendered.includes("Filter (Aggregate)")).toBe(true);
    expect(rendered.includes("Aggregate Filter:")).toBe(true);
    expect(rendered.includes("Sort (Group)")).toBe(true);
    expect(rendered.includes("Sort Key (Group): t.k")).toBe(true);
  });
});

describe("aggregate filter analyze stats", () => {
  it("attachAnalyzeQueryOpStats records rows removed by aggregate filter for implicit aggregate", async () => {
    const plan: ExplainNode = wrapPlanWithQueryOps(
      {
        nodeType: "Scan",
        source: "t",
        startupCost: 0,
        estimatedCost: 10,
        estimatedRows: 4,
        estimatedWidth: 2,
        children: [],
      },
      {
        select: parseExpressionString(
          "{ total = sum(t.v) filter(where t.keep == true) }",
        ),
      },
      undefined,
      undefined,
      undefined,
      new Config(),
    );

    attachAnalyzeQueryOpStats(plan, {
      rowsRemovedByAggregateFilter: 3,
    });

    const aggNode = plan.children[0];
    expect(aggNode.nodeType).toBe("GroupAggregate");
    const filterNode = aggNode.children[0];
    expect(filterNode.nodeType).toBe("Filter");
    expect(filterNode.filterType).toBe("aggregate");
    expect(filterNode.rowsRemovedByAggregateFilter).toBe(3);
  });

  it("attachAnalyzeQueryOpStats records rows removed by aggregate filter for grouped aggregate", async () => {
    const plan: ExplainNode = wrapPlanWithQueryOps(
      {
        nodeType: "Scan",
        source: "t",
        startupCost: 0,
        estimatedCost: 10,
        estimatedRows: 5,
        estimatedWidth: 3,
        children: [],
      },
      {
        groupBy: [
          {
            expr: parseExpressionString("t.g"),
          },
        ],
        select: parseExpressionString(
          "{ g = t.g, total = sum(t.v) filter(where t.keep == true) }",
        ),
      },
      undefined,
      undefined,
      undefined,
      new Config(),
    );

    await attachAnalyzeQueryOpStats(plan, {
      rowsRemovedByAggregateFilter: 3,
    });

    const projectNode = plan;
    expect(projectNode.nodeType).toBe("Project");

    const groupNode = projectNode.children[0];
    expect(groupNode.nodeType).toBe("GroupAggregate");

    const filterNode = groupNode.children[0];
    expect(filterNode.nodeType).toBe("Filter");
    expect(filterNode.filterType).toBe("aggregate");
    expect(filterNode.rowsRemovedByAggregateFilter).toBe(3);
  });

  it("executeAndInstrument plus attachAnalyzeQueryOpStats renders aggregate filter removal count", async () => {
    const items = [
      { v: 10, keep: true },
      { v: 20, keep: false },
      { v: 30, keep: false },
      { v: 40, keep: false },
    ];

    const source: JoinSource = {
      ...makeSource("t", items),
      expression: parseExpressionString("t"),
    };

    const tree = buildJoinTree([source]);
    const explainLeaf = explainJoinTree(tree, analyzeOpts());
    const wrapped = wrapPlanWithQueryOps(
      explainLeaf,
      {
        select: parseExpressionString(
          "{ total = sum(t.v) filter(where t.keep == true) }",
        ),
      },
      new Map([
        [
          "t",
          {
            rowCount: items.length,
            ndv: new Map([
              ["v", 4],
              ["keep", 2],
            ]),
            avgColumnCount: 2,
            statsSource: "computed-exact-small",
          },
        ],
      ]),
      undefined,
      undefined,
      new Config(),
    );

    const env = testEnvWithSources({ t: items });

    await executeAndInstrument(
      tree,
      wrapped.children[0].children[0],
      env,
      LuaStackFrame.lostFrame,
      analyzeOpts(),
      undefined,
      undefined,
      0,
    );

    await attachAnalyzeQueryOpStats(wrapped, {
      rowsRemovedByAggregateFilter: 3,
    });

    const rendered = formatExplainOutput(
      {
        plan: wrapped,
        planningTimeMs: 0,
        executionTimeMs: 0,
      },
      analyzeOpts(),
    );

    expect(rendered.includes("Filter (Aggregate)")).toBe(true);
    expect(rendered.includes("Rows Removed by Aggregate Filter: 3")).toBe(true);
  });
});

describe("normalizePushdownExpression IN / NOT IN rewrites", () => {
  it("preserves `o.x in {1,2,3}` as a QueryIn over a literal table", () => {
    const expr = parseExpressionString("o.x in {1, 2, 3}");
    const normalized = normalizePushdownExpression(expr);
    expect(normalized.type).toBe("QueryIn");
    const rendered = exprToString(normalized);
    expect(rendered).toContain("o.x in");
    expect(rendered).toContain("1");
    expect(rendered).toContain("2");
    expect(rendered).toContain("3");
  });

  it("rewrites `not (o.x in {1, 2, 3})` into `o.x ~= 1 and o.x ~= 2 and o.x ~= 3`", () => {
    const expr = parseExpressionString("not (o.x in {1, 2, 3})");
    const normalized = normalizePushdownExpression(expr);
    expect(normalized.type).toBe("Binary");
    const rendered = exprToString(normalized);
    expect(rendered).toContain("o.x ~= 1");
    expect(rendered).toContain("o.x ~= 2");
    expect(rendered).toContain("o.x ~= 3");
    expect(rendered).not.toContain(" in ");
    expect(rendered).not.toContain("not ");
  });

  it("leaves `not (o.x in other)` unchanged when RHS is not a literal table", () => {
    const expr = parseExpressionString("not (o.x in other_table)");
    const normalized = normalizePushdownExpression(expr);
    const rendered = exprToString(normalized);
    expect(rendered).toContain("not ");
    expect(rendered).toContain(" in ");
  });
});

describe("extractSingleSourceFilters with IN / NOT IN", () => {
  it("pushes down `o.x in {literal, ...}` as a single-source filter", () => {
    const expr = parseExpressionString("o.x in {1, 2, 3}");
    const { pushed, residual } = extractSingleSourceFilters(
      expr,
      new Set(["o"]),
    );
    expect(residual).toBeUndefined();
    expect(pushed).toHaveLength(1);
    expect(pushed[0].sourceName).toBe("o");
  });

  it("pushes `not (o.x in {literals})` as a single-source filter (rewritten to ANDed ~=)", () => {
    const expr = parseExpressionString("not (o.x in {1, 2, 3})");
    const { pushed, residual } = extractSingleSourceFilters(
      expr,
      new Set(["o"]),
    );
    expect(residual).toBeUndefined();
    expect(pushed).toHaveLength(1);
    expect(pushed[0].sourceName).toBe("o");
    const rendered = exprToString(pushed[0].expression);
    expect(rendered).toContain("o.x ~= 1");
    expect(rendered).toContain("o.x ~= 3");
  });

  it("keeps a mixed-source `in` out of single-source pushdown", () => {
    const expr = parseExpressionString("o.x in {1, b.y, 3}");
    const { pushed, residual } = extractSingleSourceFilters(
      expr,
      new Set(["o", "b"]),
    );
    expect(pushed).toHaveLength(0);
    expect(residual).toBeDefined();
  });
});
