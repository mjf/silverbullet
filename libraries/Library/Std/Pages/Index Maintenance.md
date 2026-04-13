#meta

Use this page to inspect planner-visible index statistics and run common recovery actions:

- ${widgets.commandButton("Space: Reindex")} or
- ${widgets.commandButton("Client: Wipe")} (in rare cases).

# Weak stats

Shows tags whose planner-visible source stats are not fully trusted or do not currently advertise bitmap-capable pushdown. If this section is empty, no weak source stats were found.

${query [[
  from
    s = index.stats()
  where
    (s.column == nil or s.column == "page") and
    (
      s.statsSource ~= "persisted-complete" or
      s.predicatePushdown ~= "bitmap-basic"
    )
  select
    tag = s.tag,
    rowCount = s.rowCount,
    avgColumnCount = s.avgColumnCount,
    statsSource = s.statsSource,
    predicatePushdown = s.predicatePushdown,
    scanKind = s.scanKind
  order by
    s.rowCount desc,
    s.tag
]]}

# Large tags without pushdown

Shows larger tags that currently do not advertise bitmap-capable pushdown. These are good candidates for reindexing or further inspection. If this section is empty, no large tags without bitmap pushdown were found.

${query [[
  from
    s = index.stats()
  where
    (s.column == nil or s.column == "page") and
    s.rowCount >= 100 and
    s.predicatePushdown ~= "bitmap-basic"
  select
    tag = s.tag,
    rowCount = s.rowCount,
    avgColumnCount = s.avgColumnCount,
    statsSource = s.statsSource,
    predicatePushdown = s.predicatePushdown
  order by
    s.rowCount desc
]]}

# Highly selective indexed columns

Shows indexed columns whose NDV is close to row count. These columns may be expensive to index unless intentionally forced. If this section is empty, no highly selective indexed columns were found.

${query [[
  from
    s = index.stats()
  where
    s.column ~= nil and
    s.indexed == true and
    s.rowCount >= 20 and
    s.ndv ~= nil and
    (s.ndv / s.rowCount) > 0.8
  select
    tag = s.tag,
    column = s.column,
    rowCount = s.rowCount,
    ndv = s.ndv,
    ndvRatio = s.rowCount > 0 and (s.ndv / s.rowCount) or 0,
    trackedMcvValues = s.trackedMcvValues
  order by
    ndvRatio desc,
    s.rowCount desc
]]}

# Indexed columns with zero NDV

Shows indexed columns that currently report zero distinct values despite having rows. This is unusual and may point to a bookkeeping or data-shape issue. If this section is empty, no indexed columns with zero NDV were found.

${query [[
  from
    s = index.stats()
  where
    s.column ~= nil and
    s.indexed == true and
    s.rowCount > 0 and
    s.ndv == 0
  select
    tag = s.tag,
    column = s.column,
    rowCount = s.rowCount,
    ndv = s.ndv,
    indexed = s.indexed,
    trackedMcvValues = s.trackedMcvValues
  order by
    s.rowCount desc,
    s.tag,
    s.column
]]}

# Wide tags

Shows tags with many average columns and enough rows to matter operationally. Wide tags may be more expensive to scan and join. If this section is empty, no wide tags were found.

${query [[
  from
    s = index.stats()
  where
    (s.column == nil or s.column == "page") and
    s.rowCount >= 20 and
    s.avgColumnCount >= 15
  select
    tag = s.tag,
    rowCount = s.rowCount,
    avgColumnCount = s.avgColumnCount,
    statsSource = s.statsSource
  order by
    s.avgColumnCount desc,
    s.rowCount desc
]]}

# Non-indexed columns

Shows columns that planner stats know about but bitmap indexing currently does not use. This can explain missing pushdown opportunities. If this section is empty, no non-indexed columns were found.

${query [[
  from
    s = index.stats()
  where
    s.column ~= nil and
    s.indexed == false
  select
    tag = s.tag,
    column = s.column,
    rowCount = s.rowCount,
    ndv = s.ndv,
    ndvRatio = s.rowCount > 0 and (s.ndv / s.rowCount) or 0
  order by
    s.rowCount desc,
    s.tag,
    s.column
]]}

# Empty or incomplete tags

Shows tags that look empty or whose base source stats are incomplete. If this section is empty, no empty or incomplete tags were found.

${query [[
  from
    s = index.stats()
  where
    s.column == nil and
    (s.statsSource == "computed-empty" or s.rowCount == 0)
  select
    tag = s.tag,
    rowCount = s.rowCount,
    statsSource = s.statsSource,
    predicatePushdown = s.predicatePushdown
  order by
    s.tag
]]}

# Summary

Shows the main issues worth acting on first. If this section is empty, no summary issues were found.

${query [[
  from
    s = index.stats()
  where
    (s.column == nil or s.column == "page") and
    (
      s.statsSource ~= "persisted-complete" or
      (s.rowCount >= 100 and s.predicatePushdown ~= "bitmap-basic")
    )
  select
    tag = s.tag,
    rowCount = s.rowCount,
    avgColumnCount = s.avgColumnCount,
    statsSource = s.statsSource,
    predicatePushdown = s.predicatePushdown,
    warning =
      s.statsSource ~= "persisted-complete" and
        "reindex recommended" or
      (s.rowCount >= 100 and s.predicatePushdown ~= "bitmap-basic") and
        "large tag without bitmap pushdown" or
      nil
  order by
    s.rowCount desc,
    s.tag
]]}
