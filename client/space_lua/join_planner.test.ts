import { describe, expect, it } from "vitest";
import { wrapPlanWithQueryOps, type ExplainNode } from "./join_planner.ts";

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

    // Top node is now the implicit Project wrapping the GroupAggregate
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

    // Top node is now the implicit Project wrapping the GroupAggregate
    expect(wrapped.nodeType).toBe("Project");
    expect(wrapped.children.length).toBe(1);
    const groupNode = wrapped.children[0];
    expect(groupNode.nodeType).toBe("GroupAggregate");
    expect(groupNode.estimatedRows).toBe(42);
  });
});
