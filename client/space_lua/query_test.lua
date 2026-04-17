local function assertEquals(a, b)
  if a ~= b then
    error(
      "Assertion failed: "
        .. tostring(a) .. " (" .. type(a) .. ")"
        .. " is not equal to "
        .. tostring(b) .. " (" .. type(b) .. ")"
    )
  end
end

local function assertTrue(v, msg)
  if not v then
    error("Assertion failed: " .. (msg or "expected truthy value"))
  end
end

-- Dataset
local pages = {
  { name = "Alice", tags = {"work", "urgent"},     size = 10, age = 31 },
  { name = "Bob",   tags = {"work"},               size = 20, age = 25 },
  { name = "Carol", tags = {"personal", "urgent"}, size =  5, age = 41 },
  { name = "Dave",  tags = {"personal"},           size = 15, age = 52 },
  { name = "Ed",    tags = {},                     size =  3, age = 19 },
  { name = "Fran",  tags = {"random"},             size =  1, age = 55 },
  { name = "Greg",  tags = {"work", "fun"},        size =  2, age = 63 },
}

-- 1. Basic `from` — all rows returned

do
  local r = query [[
    from
      pages
  ]]
  assertEquals(#r, #pages)
end

do
  local r = query [[
    from
      p = pages
  ]]
  assertEquals(#r, #pages)
  assertEquals(r[1].name, "Alice")
end

-- 2. Select / projection

-- 2a. Unbound: bare field names
do
  local r = query [[
    from
      pages
    select {
      n = name,
    }
  ]]
  assertEquals(r[1].n, "Alice")
end

-- 2b. Bound: qualified access
do
  local r = query [[
    from
      p = pages
    select {
      n = p.name,
    }
  ]]
  assertEquals(r[1].n, "Alice")
  assertEquals(r[2].n, "Bob")
end

-- 2c. Unbound: mixed with nil-guard for undefined binding
do
  local r = query [[
    from
      pages
    select {
      a = name,
      b = p and p.tags[1],
    }
  ]]
  assertEquals(r[1].a, "Alice")
end

-- 2d. Select single field (not table constructor)
do
  local r = query [[
    from
      pages
    select
      name
  ]]
  assertEquals(r[1], "Alice")
  assertEquals(r[2], "Bob")
end

-- 2e. Select single field, bound
do
  local r = query [[
    from
      p = pages
    select
      p.name
  ]]
  assertEquals(r[1], "Alice")
  assertEquals(r[2], "Bob")
end

-- 2f. Select with expression
do
  local r = query [[
    from
      pages
    select
      name .. " (" .. size .. ")"
  ]]
  assertEquals(r[1], "Alice (10)")
end

-- 2g. Select with expression, bound
do
  local r = query [[
    from
      p = pages
    select
      p.name .. " (" .. p.size .. ")"
  ]]
  assertEquals(r[1], "Alice (10)")
end

-- 2h. Select whole object via object variable
do
  local r = query [[
    from
      p = pages
    select
      p
  ]]
  assertEquals(r[1].name, "Alice")
  assertEquals(r[1].size, 10)
end

-- 2i. Select table with aliased fields (key ~= value name)
do
  local r = query [[
    from
      pages
    select {
      pageName = name,
      sz = size,
    }
  ]]
  assertEquals(r[1].pageName, "Alice")
  assertEquals(r[1].sz, 10)
end

-- 3. Limit and offset

-- 3a. Limit only
do
  local r = query [[
    from
      pages
    limit
      2
  ]]
  assertEquals(#r, 2)
end

-- 3b. Limit with offset
do
  local r = query [[
    from
      p = pages
    select {
      name = p.name,
    }
    limit
      3, 2
  ]]
  assertEquals(#r, 3)
  assertEquals(r[1].name, "Carol")
end

-- 3c. Large offset reduces result count
do
  local r = query [[
    from
      pages
    limit
      100, 5
  ]]
  assertEquals(#r, 2)
end

-- 3d. Limit 0 returns empty
do
  local r = query [[
    from
      pages
    limit
      0
  ]]
  assertEquals(#r, 0)
end

-- 4. Order by

-- 4a. Order by field, unbound
do
  local r = query [[
    from
      pages
    select {
      name = name,
    }
    order by
      size desc
  ]]
  assertEquals(r[1].name, "Bob")
end

-- 4b. Order by field, bound
do
  local r = query [[
    from
      p = pages
    select {
      age = p.age,
    }
    order by
      p.age
  ]]
  assertEquals(r[1].age, 19)
end

-- 4c. Order by multiple fields
do
  local r = query [[
    from
      p = pages
    select {
      name = p.name,
      size = p.size,
    }
    order by
      p.size, p.name
  ]]
  -- size=1 Fran, size=2 Greg, size=3 Ed, size=5 Carol, ...
  assertEquals(r[1].name, "Fran")
  assertEquals(r[2].name, "Greg")
end

-- 4d. Order by desc + limit
do
  local r = query [[
    from
      p = pages
    order by
      p.age desc
    limit
      1
  ]]
  assertEquals(r[1].name, "Greg") -- age 63
end

-- 5. Where

-- 5a. Where, unbound
do
  local r = query [[
    from
      pages
    where
      size > 10
    select {
      name = name,
    }
  ]]
  assertEquals(#r, 2) -- Bob (20), Dave (15)
  assertEquals(r[1].name, "Bob")
end

-- 5b. Where, bound
do
  local r = query [[
    from
      p = pages
    where
      p.age < 30
    select {
      name = p.name,
    }
  ]]
  assertEquals(r[1].name, "Bob")
  assertEquals(r[2].name, "Ed")
end

-- 5c. Where with truthy check, unbound
do
  local r = query [[
    from
      pages
    where
      name
  ]]
  assertEquals(#r, #pages)
end

-- 5d. Where with nil check on nested field, unbound
do
  local r = query [[
    from
      pages
    where
      tags[1] ~= nil
  ]]
  -- Ed has empty tags -> tags[1] is nil -> excluded
  assertEquals(#r, 6)
end

-- 5e. Where with nil check, bound
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
  ]]
  assertEquals(#r, 6)
end

-- 5f. Where + order by + limit + select (full pipeline without grouping)
do
  local r = query [[
    from
      p = pages
    where
      p.size > 2
    select {
      name = p.name,
      size = p.size,
    }
    order by
      p.name
    limit
      3
  ]]
  assertEquals(#r, 3)
  assertEquals(r[1].name, "Alice")
  assertEquals(r[2].name, "Bob")
  assertEquals(r[3].name, "Carol")
end

-- 5g. Where + order by + limit, unbound
do
  local r = query [[
    from
      pages
    where
      size > 2
    select {
      name = name,
      size = size,
    }
    order by
      name
    limit
      3
  ]]
  assertEquals(#r, 3)
  assertEquals(r[1].name, "Alice")
end

-- 6. Group by — single key

-- 6a. Group by with where filter, unbound
do
  local r = query [[
    from
      pages
    where
      tags[1] ~= nil
    group by
      tags[1]
    select {
      key = key,
    }
  ]]
  assertTrue(type(r[1].key) == "string" or r[1].key == nil)
  -- work, personal, random -> 3 groups
  assertEquals(#r, 3)
end

-- 6b. Group by with where filter, bound
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      key = key,
    }
  ]]
  assertEquals(#r, 3)
end

-- 6c. Group by with group access
do
  local r = query [[
    from
      pages
    where
      tags[1] ~= nil
    group by
      tags[1]
    select {
      k = key,
      gc = #group,
      first = group[1].name,
    }
  ]]
  assertTrue(type(r[1].gc) == "number")
end

-- 6d. Group by bound with group access
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      k = key,
      gc = #group,
      n = group[1].name,
    }
  ]]
  assertTrue(type(r[1].gc) == "number")
end

-- 6e. Group by nil key (Ed has empty tags, tags[1] is nil)
do
  local r = query [[
    from
      pages
    group by
      tags[1]
    select {
      k = key,
      gc = #group,
    }
  ]]
  -- 4 groups: work, personal, random, nil (Ed)
  assertEquals(#r, 4)
end

-- 7. Group by — composite key

-- 7a. Multi-key group by, unbound
do
  local r = query [[
    from
      pages
    where
      tags[1] ~= nil and tags[2] ~= nil
    group by
      tags[1], tags[2]
    select {
      k1 = key[1],
      k2 = key[2],
    }
  ]]
  -- Alice: work,urgent; Carol: personal,urgent; Greg: work,fun -> 3 combos
  assertEquals(#r, 3)
end

-- 7b. Multi-key group by, bound
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil and p.tags[2] ~= nil
    group by
      p.tags[1], p.tags[2]
    select {
      k1 = key[1],
      k2 = key[2],
    }
  ]]
  assertEquals(#r, 3)
end

-- 8. Aggregation builtins

-- 8a. count
do
  local r1 = query [[
    from
      pages
    where
      tags[1] ~= nil
    group by
      tags[1]
    select {
      count = count(name),
    }
  ]]
  local r2 = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      count = count(p.name),
    }
  ]]
  assertTrue(type(r1[1].count) == "number")
  assertEquals(#r1, #r2)
end

-- 8b. min, max, avg, sum
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      mn = min(p.size),
      mx = max(p.size),
      av = avg(p.size),
      sm = sum(p.size),
    }
  ]]
  for _, row in ipairs(r) do
    assertTrue(type(row.mn) == "number" or row.mn == nil)
    assertTrue(type(row.av) == "number" or row.av == nil)
  end
end

-- 8c. array_agg
do
  local r1 = query [[
    from
      pages
    where
      tags[1] ~= nil
    group by
      tags[1]
    select {
      arr = array_agg(name),
    }
  ]]
  local r2 = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      arr = array_agg(p.name),
    }
  ]]
  assertTrue(type(r1[1].arr) == "table" or r1[1].arr == nil)
  assertEquals(#r1, #r2)
end

-- 8d. count() with no argument (counts all rows)
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      k = key,
      n = count(),
    }
  ]]
  assertTrue(type(r[1].n) == "number")
end

-- 8e. Multiple aggregates in one select
do
  local r = query [[
    from
      pages
    where
      tags[1] ~= nil
    group by
      tags[1]
    select {
      c = count(name),
      lo = min(size),
      hi = max(size),
      av = avg(size),
      sm = sum(size),
      arr = array_agg(name),
    }
  ]]
  assertTrue(type(r[1].c) == "number")
  assertTrue(type(r[1].lo) == "number" or r[1].lo == nil)
  assertTrue(type(r[1].arr) == "table" or r[1].arr == nil)
end

-- 8f. product
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      prod = product(p.size),
    }
  ]]
  for _, row in ipairs(r) do
    assertTrue(type(row.prod) == "number" or row.prod == nil)
  end
end

-- 8g. string_agg
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      names = string_agg(p.name),
    }
  ]]
  for _, row in ipairs(r) do
    assertTrue(type(row.names) == "string")
  end
end

-- 8h. string_agg with custom separator
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      names = string_agg(p.name, " | "),
    }
  ]]
  for _, row in ipairs(r) do
    assertTrue(type(row.names) == "string")
  end
end

-- 8i. yaml_agg
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      y = yaml_agg(p.name),
    }
  ]]
  for _, row in ipairs(r) do
    assertTrue(type(row.y) == "string")
  end
end

-- 8j. json_agg
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      j = json_agg(p.name),
    }
  ]]
  for _, row in ipairs(r) do
    assertTrue(type(row.j) == "string")
  end
end

-- 8k. bool_and, bool_or
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      all_big = bool_and(p.size > 5),
      any_big = bool_or(p.size > 5),
    }
  ]]
  for _, row in ipairs(r) do
    assertTrue(
      type(row.all_big) == "boolean" or row.all_big == nil,
      "bool_and must return boolean or nil"
    )
    assertTrue(
      type(row.any_big) == "boolean" or row.any_big == nil,
      "bool_or must return boolean or nil"
    )
  end
end

-- 8l. stddev_pop, var_pop
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      sd = stddev_pop(p.size),
      vr = var_pop(p.size),
    }
  ]]
  for _, row in ipairs(r) do
    assertTrue(type(row.sd) == "number" or row.sd == nil)
    assertTrue(type(row.vr) == "number" or row.vr == nil)
  end
end

-- 8m. stddev_samp, var_samp (nil for single-element groups)
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      k = key,
      sd = stddev_samp(p.size),
      vr = var_samp(p.size),
      n = count(p.name),
    }
  ]]
  for _, row in ipairs(r) do
    if row.n >= 2 then
      assertTrue(type(row.sd) == "number", "stddev_samp >= 2 items")
      assertTrue(type(row.vr) == "number", "var_samp >= 2 items")
    else
      -- single-element group -> nil
      assertEquals(row.sd, nil)
      assertEquals(row.vr, nil)
    end
  end
end

-- 8n. percentile_cont (needs order by ... asc inside aggregate)
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      k = key,
      med = percentile_cont(p.size, 0.5 order by p.size asc),
    }
  ]]
  for _, row in ipairs(r) do
    assertTrue(type(row.med) == "number" or row.med == nil)
  end
end

-- 8o. percentile_disc (needs order by ... asc inside aggregate)
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      k = key,
      pd = percentile_disc(p.size, 0.5 order by p.size asc),
    }
  ]]
  for _, row in ipairs(r) do
    assertTrue(type(row.pd) == "number" or row.pd == nil)
  end
end

-- 8p. quantile with explicit method (needs order by ... asc)
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      k = key,
      q = quantile(p.size, 0.25, "lower" order by p.size asc),
    }
  ]]
  for _, row in ipairs(r) do
    assertTrue(type(row.q) == "number" or row.q == nil)
  end
end

-- 8q. percentile_cont on known data for exact value check
do
  local scores = {
    { g = "a", v = 10 },
    { g = "a", v = 20 },
    { g = "a", v = 30 },
    { g = "a", v = 40 },
  }
  local r = query [[
    from
      s = scores
    group by
      s.g
    select {
      med = percentile_cont(s.v, 0.5 order by s.v asc),
    }
  ]]
  -- [10,20,30,40] q=0.5 -> idx=1.5 -> 20 + 0.5*(30-20) = 25
  assertEquals(r[1].med, 25)
end

-- 8r. percentile_disc on known data for exact value check
do
  local scores = {
    { g = "a", v = 10 },
    { g = "a", v = 20 },
    { g = "a", v = 30 },
    { g = "a", v = 40 },
    { g = "a", v = 50 },
  }
  local r = query [[
    from
      s = scores
    group by
      s.g
    select {
      p25 = percentile_disc(s.v, 0.25 order by s.v asc),
    }
  ]]
  -- [10,20,30,40,50] q=0.25 -> idx=1.0 -> lower -> values[1] = 20
  assertEquals(r[1].p25, 20)
end

-- 8s. mode: most frequent tag in dataset
do
  local data = {
    { g = "a", v = "x" },
    { g = "a", v = "y" },
    { g = "a", v = "x" },
    { g = "a", v = "x" },
    { g = "a", v = "y" },
    { g = "b", v = "q" },
  }
  local r = query [[
    from
      d = data
    group by
      d.g
    select {
      k = key,
      m = mode(d.v),
    }
    order by
      k
  ]]
  -- group "a": x=3, y=2 -> mode = "x"
  assertEquals(r[1].m, "x")
  -- group "b": q=1 -> mode = "q"
  assertEquals(r[2].m, "q")
end

-- 8t. first / last with intra-aggregate order by
do
  local data = {
    { g = "a", v = "c", k = 3 },
    { g = "a", v = "a", k = 1 },
    { g = "a", v = "b", k = 2 },
  }
  local r = query [[
    from
      d = data
    group by
      d.g
    select {
      f = first(d.v order by d.k asc),
      l = last(d.v order by d.k asc),
    }
  ]]
  assertEquals(r[1].f, "a")
  assertEquals(r[1].l, "c")
end

-- 8u. first / last without order by (iteration order)
do
  local data = {
    { g = "x", v = 10 },
    { g = "x", v = 20 },
    { g = "x", v = 30 },
  }
  local r = query [[
    from
      d = data
    group by
      d.g
    select {
      f = first(d.v),
      l = last(d.v),
    }
  ]]
  assertEquals(r[1].f, 10)
  assertEquals(r[1].l, 30)
end

-- 8v. median on known data (odd)
do
  local data = {
    { g = "a", v = 30 },
    { g = "a", v = 10 },
    { g = "a", v = 20 },
  }
  local r = query [[
    from
      d = data
    group by
      d.g
    select {
      med = median(d.v order by d.v asc),
    }
  ]]
  assertEquals(r[1].med, 20)
end

-- 8w. median on known data (even, interpolated)
do
  local data = {
    { g = "a", v = 10 },
    { g = "a", v = 20 },
    { g = "a", v = 30 },
    { g = "a", v = 40 },
  }
  local r = query [[
    from
      d = data
    group by
      d.g
    select {
      med = median(d.v order by d.v asc),
    }
  ]]
  -- [10,20,30,40] -> 25
  assertEquals(r[1].med, 25)
end

-- 8x. first / last with filter
do
  local data = {
    { g = "a", v = 1,  big = false },
    { g = "a", v = 10, big = true  },
    { g = "a", v = 2,  big = false },
    { g = "a", v = 20, big = true  },
  }
  local r = query [[
    from
      d = data
    group by
      d.g
    select {
      fb = first(d.v order by d.v asc) filter(where d.big),
      lb = last(d.v order by d.v asc) filter(where d.big),
    }
  ]]
  assertEquals(r[1].fb, 10)
  assertEquals(r[1].lb, 20)
end

-- 9. Having

-- 9a. Having with aggregate, unbound
do
  local r = query [[
    from
      pages
    where
      tags[1] ~= nil
    group by
      tags[1]
    having
      count(name) > 1
    select {
      key = key,
    }
  ]]
  -- work: Alice,Bob,Greg (3); personal: Carol,Dave (2); random: Fran (1)
  -- Only work and personal pass
  assertEquals(#r, 2)
end

-- 9b. Having with aggregate, bound
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    having
      count(p.name) > 1
    select {
      key = key,
    }
  ]]
  assertEquals(#r, 2)
end

-- 9c. Having with sum
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    having
      sum(p.size) > 15
    select {
      key = key,
    }
  ]]
  -- work: 10+20+2=32; personal: 5+15=20; random: 1
  -- work and personal pass
  assertEquals(#r, 2)
end

-- 9d. Having without group by (acts as secondary filter), unbound
do
  local r = query [[
    from
      pages
    having
      size > 10
    select {
      name = name,
    }
  ]]
  assertEquals(#r, 2) -- Bob (20), Dave (15)
end

-- 9e. Having without group by, bound
do
  local r = query [[
    from
      p = pages
    having
      p.size > 10
    select {
      name = p.name,
    }
  ]]
  assertEquals(#r, 2)
end

-- 9f. Where + having without group by (both filter)
do
  local r = query [[
    from
      pages
    where
      age > 20
    having
      size > 10
    select {
      name = name,
    }
  ]]
  -- age>20: Alice(31),Bob(25),Carol(41),Dave(52),Fran(55),Greg(63)
  -- size>10: Bob(20),Dave(15)
  assertEquals(#r, 2)
  assertEquals(r[1].name, "Bob")
  assertEquals(r[2].name, "Dave")
end

-- 10. Group by + having + order by + select + limit (full pipeline)

-- 10a. Bound
do
  local r = query [[
    from
      p = pages
    where
      p.age > 20
    group by
      p.tags[1]
    having
      min(p.age) > 25
    select {
      tag = key,
      top = max(p.name),
      total = count(p.name),
      sum_size = sum(p.size),
      avg_size = avg(p.size),
    }
    order by
      avg_size desc
    limit
      2
  ]]
  assertTrue(#r <= 2)
  assertTrue(type(r[1].avg_size) == "number" or r[1].avg_size == nil)
end

-- 10b. Unbound
do
  local r = query [[
    from
      pages
    where
      age > 20
    group by
      tags[1]
    having
      min(age) > 25
    select {
      tag = key,
      top = max(name),
      total = count(name),
      sum_size = sum(size),
      avg_size = avg(size),
    }
    order by
      avg_size desc
    limit
      2
  ]]
  assertTrue(#r <= 2)
end

-- 11. Distinct

-- 11a. Distinct with select, unbound
do
  local r = query [[
    from
      pages
    where
      tags[1] ~= nil
    select {
      tag = tags[1],
    }
  ]]
  -- default distinct=true for queries, so should deduplicate
  -- work appears 3x, personal 2x, random 1x -> 3 distinct
  assertEquals(#r, 3)
end

-- 11b. Distinct with select, bound
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    select {
      tag = p.tags[1],
    }
  ]]
  assertEquals(#r, 3)
end

-- 12. Edge cases: empty results

-- 12a. Where that matches nothing
do
  local r = query [[
    from
      pages
    where
      size > 1000
  ]]
  assertEquals(#r, 0)
end

-- 12b. Having that matches nothing
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    having
      count(p.name) > 100
    select {
      key = key,
    }
  ]]
  assertEquals(#r, 0)
end

-- 12c. Empty source
do
  local empty = {}
  local r = query [[
    from
      empty
  ]]
  assertEquals(#r, 0)
end

-- 13. Group by + order by on aggregate result

-- 13a. Order by count desc
do
  local r = query [[
    from
      pages
    where
      tags[1] ~= nil
    group by
      tags[1]
    select {
      tag = key,
      n = count(),
    }
    order by
      n desc
  ]]
  -- work=3, personal=2, random=1
  assertTrue(r[1].n >= r[2].n)
end

-- 13b. Order by key
do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      tag = key,
      n = count(),
    }
    order by
      tag
  ]]
  -- alphabetical: personal, random, work
  assertEquals(r[1].tag, "personal")
  assertEquals(r[2].tag, "random")
  assertEquals(r[3].tag, "work")
end

-- 14. Group by key name binding

-- 14a. Single key name available via key variable in select
do
  local found_tag = false
  local r = query [[
    from
      pages
    where
      tags[1] ~= nil
    group by
      tags[1]
    select {
      k = key,
    }
  ]]
  for _, row in ipairs(r) do
    if row.k == "work" then
      found_tag = true
    end
  end
  assertTrue(found_tag, "expected to find 'work' group key")
end

-- 15. Caller-injected env variable must not be shadowed by item fields
--     (the requeueTimeouts / mq pattern)

do
  local threshold = 100
  local items = {
    { id = "a", ts = 50 },
    { id = "b", ts = 200 },
  }
  -- m.ts < ts where ts=threshold from parent env
  -- m.ts=50 < 100 -> true; m.ts=200 < 100 -> false
  local ts = threshold
  local r = query [[
    from
      m = items
    where
      m.ts < ts
  ]]
  assertEquals(#r, 1)
  assertEquals(r[1].id, "a")
end

-- 16. Unbound: item fields shadow outer locals

do
  local size = 999 -- this should be shadowed by item.size
  local r = query [[
    from
      pages
    where
      size < 10
    select {
      name = name,
      sz = size,
    }
  ]]
  -- size < 10: Carol(5), Ed(3), Fran(1), Greg(2) -> 4 items
  assertEquals(#r, 4)
  assertTrue(r[1].sz < 10)
end

-- 17. Bound: outer locals are accessible

do
  local threshold = 10
  local r = query [[
    from
      p = pages
    where
      p.size > threshold
    select {
      name = p.name,
    }
  ]]
  -- size > 10: Bob(20), Dave(15) -> 2
  assertEquals(#r, 2)
end

-- 18. Select with table constructor + non-Variable expressions

-- 18a. Expression value in PropField
do
  local r = query [[
    from
      p = pages
    select {
      label = p.name .. "!",
      double = p.size * 2,
    }
    limit
      2
  ]]
  assertEquals(r[1].label, "Alice!")
  assertEquals(r[1].double, 20)
  assertEquals(r[2].label, "Bob!")
  assertEquals(r[2].double, 40)
end

-- 18b. Unbound expression
do
  local r = query [[
    from
      pages
    select {
      label = name .. "!",
      double = size * 2,
    }
    limit
      2
  ]]
  assertEquals(r[1].label, "Alice!")
  assertEquals(r[1].double, 20)
end

-- 19. Order by + group by + having + limit combined

do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    having
      count() > 0
    select {
      tag = key,
      total = sum(p.size),
    }
    order by
      total desc
    limit
      2
  ]]
  assertEquals(#r, 2)
  assertTrue(r[1].total >= r[2].total)
end

-- 20. Offset with group by

do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      tag = key,
      n = count(),
    }
    order by
      tag
    limit
      2, 1
  ]]
  -- 3 groups sorted by tag, skip 1, take 2
  assertEquals(#r, 2)
end

-- 21. Where + having + group by combined

do
  local r = query [[
    from
      pages
    where
      age > 20
    group by
      tags[1]
    having
      #group > 1
    select {
      tag = key,
      gc = #group,
    }
  ]]
  -- After where (age>20): Alice(31),Bob(25),Carol(41),Dave(52),Fran(55),Greg(63)
  -- Groups by tags[1]: work=[Alice,Bob,Greg](3), personal=[Carol,Dave](2), random=[Fran](1)
  -- Having #group>1: work(3), personal(2)
  assertEquals(#r, 2)
end

-- Same, bound
do
  local r = query [[
    from
      p = pages
    where
      p.age > 20
    group by
      p.tags[1]
    having
      #group > 1
    select {
      tag = key,
      gc = #group,
    }
  ]]
  assertEquals(#r, 2)
end

-- 22. Select _ (the whole item, unbound)

do
  local r = query [[
    from
      pages
    select
      _
    limit
      1
  ]]
  assertEquals(r[1].name, "Alice")
  assertEquals(r[1].size, 10)
end

-- 23. Group by with order by on key

do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      tag = key,
    }
    order by
      key
  ]]
  assertEquals(r[1].tag, "personal")
  assertEquals(r[2].tag, "random")
  assertEquals(r[3].tag, "work")
end

-- 24. Aggregate on entire dataset (single group)

do
  local r = query [[
    from
      p = pages
    group by
      "all"
    select {
      total = count(),
      min_size = min(p.size),
      max_size = max(p.size),
      sum_size = sum(p.size),
    }
  ]]
  assertEquals(#r, 1)
  assertEquals(r[1].total, 7)
  assertEquals(r[1].min_size, 1)
  assertEquals(r[1].max_size, 20)
  assertEquals(r[1].sum_size, 56) -- 10+20+5+15+3+1+2
end

-- 25. Nested field access in group by

do
  local r = query [[
    from
      pages
    where
      tags[1] ~= nil
    group by
      tags[1]
    select {
      k = key,
      first_name = group[1].name,
      first_tag2 = group[1].tags[2],
    }
  ]]
  assertTrue(type(r[1].k) == "string" or r[1].k == nil)
end

-- 26. Mixed aggregate and non-aggregate in select

do
  local r = query [[
    from
      pages
    where
      tags[1] ~= nil
    group by
      tags[1]
    select {
      c = count(name),
      v = min(size),
      x = p and count(p.name),
    }
  ]]
  assertTrue(type(r[1].c) == "number")
end

-- 27. Having with #group

do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    having
      #group >= 2
    select {
      tag = key,
      gc = #group,
    }
  ]]
  for _, row in ipairs(r) do
    assertTrue(row.gc >= 2, "expected group count >= 2, got " .. tostring(row.gc))
  end
end

-- 28. Order by desc on aggregate

do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      tag = key,
      sm = sum(p.size),
    }
    order by
      sm desc
  ]]
  for i = 1, #r - 1 do
    assertTrue(
      (r[i].sm or 0) >= (r[i + 1].sm or 0),
      "expected descending sum order"
    )
  end
end

-- 29. Singleton collection (single object, not array)

do
  local item = { name = "solo", size = 42 }
  local r = query [[
    from
      item
    select {
      name = name,
      size = size,
    }
  ]]
  assertEquals(#r, 1)
  assertEquals(r[1].name, "solo")
  assertEquals(r[1].size, 42)
end

do
  local item = { name = "solo", size = 42 }
  local r = query [[
    from
      p = item
    select {
      name = p.name,
      size = p.size,
    }
  ]]
  assertEquals(#r, 1)
  assertEquals(r[1].name, "solo")
end

-- 30. Where on singleton

do
  local item = { name = "solo", size = 42 }
  local r = query [[
    from
      item
    where
      size > 100
  ]]
  assertEquals(#r, 0)
end

do
  local item = { name = "solo", size = 42 }
  local r = query [[
    from
      item
    where
      size > 10
    select {
      name = name,
    }
  ]]
  assertEquals(#r, 1)
  assertEquals(r[1].name, "solo")
end

-- 31. Where with comparison operators

do
  local r = query [[
    from
      pages
    where
      size == 10
  ]]
  assertEquals(#r, 1)
  assertEquals(r[1].name, "Alice")
end

do
  local r = query [[
    from
      pages
    where
      size >= 15
  ]]
  assertEquals(#r, 2) -- Bob(20), Dave(15)
end

do
  local r = query [[
    from
      pages
    where
      size <= 3
  ]]
  assertEquals(#r, 3) -- Ed(3), Fran(1), Greg(2)
end

-- 32. Where with logical operators

do
  local r = query [[
    from
      pages
    where
      size > 10 and age < 40
  ]]
  -- Bob: size=20, age=25 -> true
  assertEquals(#r, 1)
  assertEquals(r[1].name, "Bob")
end

do
  local r = query [[
    from
      pages
    where
      size > 15 or age > 60
  ]]
  -- Bob: size=20 -> true; Greg: age=63 -> true
  assertEquals(#r, 2)
end

do
  local r = query [[
    from
      p = pages
    where
      p.size > 10 and p.age < 40
  ]]
  assertEquals(#r, 1)
  assertEquals(r[1].name, "Bob")
end

-- 33. Where with not

do
  local r = query [[
    from
      pages
    where
      not (size > 10)
  ]]
  -- size <= 10: Alice(10), Carol(5), Ed(3), Fran(1), Greg(2) -> 5
  assertEquals(#r, 5)
end

-- 34. Order by with nulls

do
  local data = {
    { name = "a", val = 3 },
    { name = "b" },
    { name = "c", val = 1 },
  }
  local r = query [[
    from
      p = data
    select {
      name = p.name,
    }
    order by
      p.val
  ]]
  -- nulls sort last in ascending
  assertEquals(r[1].name, "c")
  assertEquals(r[2].name, "a")
  assertEquals(r[3].name, "b")
end

-- 35. Select preserves order after where + order by

do
  local r = query [[
    from
      p = pages
    where
      p.size >= 5
    select {
      name = p.name,
      size = p.size,
    }
    order by
      p.size
  ]]
  -- size >= 5: Carol(5), Alice(10), Dave(15), Bob(20) in order
  assertEquals(r[1].name, "Carol")
  assertEquals(r[2].name, "Alice")
  assertEquals(r[3].name, "Dave")
  assertEquals(r[4].name, "Bob")
end

-- 36. Multiple group by keys produce composite key table

do
  local data = {
    { a = "x", b = 1, v = 10 },
    { a = "x", b = 1, v = 20 },
    { a = "x", b = 2, v = 30 },
    { a = "y", b = 1, v = 40 },
  }
  local r = query [[
    from
      p = data
    group by
      p.a, p.b
    select {
      ka = key[1],
      kb = key[2],
      total = sum(p.v),
    }
    order by
      ka, kb
  ]]
  assertEquals(#r, 3) -- (x,1), (x,2), (y,1)
  assertEquals(r[1].ka, "x")
  assertEquals(r[1].kb, 1)
  assertEquals(r[1].total, 30) -- 10+20
  assertEquals(r[2].ka, "x")
  assertEquals(r[2].kb, 2)
  assertEquals(r[2].total, 30)
  assertEquals(r[3].ka, "y")
  assertEquals(r[3].total, 40)
end

-- 37. Having on composite group by

do
  local data = {
    { a = "x", b = 1, v = 10 },
    { a = "x", b = 1, v = 20 },
    { a = "x", b = 2, v = 30 },
    { a = "y", b = 1, v = 40 },
  }
  local r = query [[
    from
      p = data
    group by
      p.a, p.b
    having
      count() > 1
    select {
      ka = key[1],
      kb = key[2],
      n = count(),
    }
  ]]
  -- Only (x,1) has 2 items
  assertEquals(#r, 1)
  assertEquals(r[1].ka, "x")
  assertEquals(r[1].kb, 1)
  assertEquals(r[1].n, 2)
end

-- 38. Bound: select p returns full item

do
  local r = query [[
    from
      p = pages
    select
      p
    limit
      1
  ]]
  assertEquals(r[1].name, "Alice")
  assertEquals(r[1].age, 31)
  assertEquals(r[1].size, 10)
end

-- 39. Numeric aggregates with bound and unbound

do
  local nums = {
    { val = 10 },
    { val = 20 },
    { val = 30 },
  }
  local r = query [[
    from
      nums
    group by
      "all"
    select {
      mn = min(val),
      mx = max(val),
      av = avg(val),
      sm = sum(val),
      ct = count(),
    }
  ]]
  assertEquals(r[1].mn, 10)
  assertEquals(r[1].mx, 30)
  assertEquals(r[1].av, 20)
  assertEquals(r[1].sm, 60)
  assertEquals(r[1].ct, 3)
end

do
  local nums = {
    { val = 10 },
    { val = 20 },
    { val = 30 },
  }
  local r = query [[
    from
      n = nums
    group by
      "all"
    select {
      mn = min(n.val),
      mx = max(n.val),
      av = avg(n.val),
      sm = sum(n.val),
      ct = count(),
    }
  ]]
  assertEquals(r[1].mn, 10)
  assertEquals(r[1].mx, 30)
  assertEquals(r[1].av, 20)
  assertEquals(r[1].sm, 60)
  assertEquals(r[1].ct, 3)
end

-- 40. Order by with nulls — default behavior

do
  local data = {
    { name = "alice", priority = 10 },
    { name = "bob" },
    { name = "carol", priority = 50 },
    { name = "dave" },
    { name = "eve", priority = 1 },
  }

  -- 40a. asc default: nulls last
  local r1 = query [[
    from
      p = data
    select { name = p.name }
    order by
      p.priority
  ]]
  assertEquals(r1[1].name, "eve")
  assertEquals(r1[2].name, "alice")
  assertEquals(r1[3].name, "carol")
  -- nulls at end (bob and dave, order between them is unspecified)
  assertTrue(r1[4].name == "bob" or r1[4].name == "dave", "expected null-priority item")
  assertTrue(r1[5].name == "bob" or r1[5].name == "dave", "expected null-priority item")

  -- 40b. desc default: nulls first
  local r2 = query [[
    from
      p = data
    select { name = p.name }
    order by
      p.priority desc
  ]]
  assertTrue(r2[1].name == "bob" or r2[1].name == "dave", "expected null-priority item")
  assertTrue(r2[2].name == "bob" or r2[2].name == "dave", "expected null-priority item")
  assertEquals(r2[3].name, "carol")
  assertEquals(r2[4].name, "alice")
  assertEquals(r2[5].name, "eve")
end

-- 41. Order by with explicit nulls last / nulls first

do
  local data = {
    { name = "alice", priority = 10 },
    { name = "bob" },
    { name = "carol", priority = 50 },
    { name = "dave" },
    { name = "eve", priority = 1 },
  }

  -- 41a. desc nulls last (override default)
  local r3 = query [[
    from
      p = data
    select { name = p.name }
    order by
      p.priority desc nulls last
  ]]
  assertEquals(r3[1].name, "carol")
  assertEquals(r3[2].name, "alice")
  assertEquals(r3[3].name, "eve")
  assertTrue(r3[4].name == "bob" or r3[4].name == "dave", "expected null-priority item")
  assertTrue(r3[5].name == "bob" or r3[5].name == "dave", "expected null-priority item")

  -- 41b. asc nulls first (override default)
  local r4 = query [[
    from
      p = data
    select { name = p.name }
    order by
      p.priority asc nulls first
  ]]
  assertTrue(r4[1].name == "bob" or r4[1].name == "dave", "expected null-priority item")
  assertTrue(r4[2].name == "bob" or r4[2].name == "dave", "expected null-priority item")
  assertEquals(r4[3].name, "eve")
  assertEquals(r4[4].name, "alice")
  assertEquals(r4[5].name, "carol")
end

-- 42. Order by nulls with unbound access

do
  local data = {
    { name = "a", val = 3 },
    { name = "b" },
    { name = "c", val = 1 },
  }

  -- 42a. desc nulls last, unbound
  local r = query [[
    from
      data
    select { name = name }
    order by
      val desc nulls last
  ]]
  assertEquals(r[1].name, "a")
  assertEquals(r[2].name, "c")
  assertEquals(r[3].name, "b")

  -- 42b. asc nulls first, unbound
  local r2 = query [[
    from
      data
    select { name = name }
    order by
      val nulls first
  ]]
  assertEquals(r2[1].name, "b")
  assertEquals(r2[2].name, "c")
  assertEquals(r2[3].name, "a")
end

-- 43. Order by nulls with multiple keys

do
  local data = {
    { name = "a", x = 1, y = 10 },
    { name = "b", x = 1 },
    { name = "c", x = 2, y = 5 },
    { name = "d", x = 2 },
  }

  local r = query [[
    from
      p = data
    select { name = p.name }
    order by
      p.x, p.y nulls first
  ]]
  assertEquals(r[1].name, "b")
  assertEquals(r[2].name, "a")
  assertEquals(r[3].name, "d")
  assertEquals(r[4].name, "c")
end

-- 44. Explicit asc keyword (same as default)

do
  local r = query [[
    from
      p = pages
    select { name = p.name }
    order by
      p.size asc
    limit
      2
  ]]
  assertEquals(r[1].name, "Fran")
  assertEquals(r[2].name, "Greg")
end

-- 45. Count with filter

do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      tag = key,
      total = count(),
      big = count(p.name) filter(where p.size > 10),
    }
    order by
      tag
  ]]
  -- personal: Carol(5),Dave(15) -> big=1 (Dave)
  -- random: Fran(1) -> big=0
  -- work: Alice(10),Bob(20),Greg(2) -> big=1 (Bob)
  assertEquals(#r, 3)
  for _, row in ipairs(r) do
    if row.tag == "personal" then
      assertEquals(row.big, 1)
      assertEquals(row.total, 2)
    elseif row.tag == "random" then
      assertEquals(row.big, 0)
      assertEquals(row.total, 1)
    elseif row.tag == "work" then
      assertEquals(row.big, 1)
      assertEquals(row.total, 3)
    end
  end
end

-- 46. Sum with filter

do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      tag = key,
      total_size = sum(p.size),
      big_size = sum(p.size) filter(where p.size > 5),
    }
    order by
      tag
  ]]
  for _, row in ipairs(r) do
    if row.tag == "work" then
      -- Alice(10)+Bob(20)+Greg(2)=32 total, big: 10+20=30
      assertEquals(row.total_size, 32)
      assertEquals(row.big_size, 30)
    elseif row.tag == "personal" then
      -- Carol(5)+Dave(15)=20 total, big: 15
      assertEquals(row.total_size, 20)
      assertEquals(row.big_size, 15)
    elseif row.tag == "random" then
      -- Fran(1) total=1, big: nil (none pass)
      assertEquals(row.total_size, 1)
      assertEquals(row.big_size, nil)
    end
  end
end

-- 47. Min/max with filter

do
  local r = query [[
    from
      p = pages
    group by
      "all"
    select {
      min_big = min(p.size) filter(where p.size > 5),
      max_small = max(p.size) filter(where p.size <= 5),
    }
  ]]
  assertEquals(#r, 1)
  assertEquals(r[1].min_big, 10)   -- smallest > 5: Alice(10)
  assertEquals(r[1].max_small, 5)  -- largest <= 5: Carol(5)
end

-- 48. Avg with filter

do
  local r = query [[
    from
      p = pages
    group by
      "all"
    select {
      avg_all = avg(p.size),
      avg_big = avg(p.size) filter(where p.size >= 10),
    }
  ]]
  assertEquals(#r, 1)
  -- all: (10+20+5+15+3+1+2)/7 = 56/7 = 8
  assertEquals(r[1].avg_all, 8)
  -- big (>=10): (10+20+15)/3 = 45/3 = 15
  assertEquals(r[1].avg_big, 15)
end

-- 49. Array_agg with filter

do
  local r = query [[
    from
      p = pages
    group by
      "all"
    select {
      all_names = array_agg(p.name),
      big_names = array_agg(p.name) filter(where p.size > 10),
    }
  ]]
  assertEquals(#r, 1)
  assertEquals(#r[1].all_names, 7)
  -- size > 10: Bob(20), Dave(15)
  assertEquals(#r[1].big_names, 2)
end

-- 50. Unbound access in filter

do
  local r = query [[
    from
      pages
    where
      tags[1] ~= nil
    group by
      tags[1]
    select {
      tag = key,
      big = count(name) filter(where size > 10),
    }
    order by
      tag
  ]]
  for _, row in ipairs(r) do
    if row.tag == "work" then
      assertEquals(row.big, 1) -- Bob(20)
    end
  end
end

-- 51. Count without argument with filter

do
  local r = query [[
    from
      p = pages
    group by
      "all"
    select {
      total = count(),
      big = count() filter(where p.size > 10),
    }
  ]]
  assertEquals(r[1].total, 7)
  assertEquals(r[1].big, 2) -- Bob(20), Dave(15)
end

-- 52. Filter that matches nothing

do
  local r = query [[
    from
      p = pages
    group by
      "all"
    select {
      n = count() filter(where p.size > 1000),
      s = sum(p.size) filter(where p.size > 1000),
    }
  ]]
  assertEquals(r[1].n, 0)
  assertEquals(r[1].s, nil)
end

-- 53. Multiple filters in one select

do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      tag = key,
      young = count(p.name) filter(where p.age < 30),
      old = count(p.name) filter(where p.age >= 50),
    }
    order by
      tag
  ]]
  for _, row in ipairs(r) do
    if row.tag == "work" then
      -- young: Bob(25) -> 1; old: Greg(63) -> 1
      assertEquals(row.young, 1)
      assertEquals(row.old, 1)
    elseif row.tag == "personal" then
      -- young: none; old: Dave(52) -> 1
      assertEquals(row.young, 0)
      assertEquals(row.old, 1)
    elseif row.tag == "random" then
      -- young: none; old: Fran(55) -> 1
      assertEquals(row.young, 0)
      assertEquals(row.old, 1)
    end
  end
end

-- 54. `using` with named function comparator

local function reverseAlpha(a, b)
  return a > b
end

do
  local r = query [[
    from
      p = pages
    order by
      p.name using reverseAlpha
    select p.name
  ]]
  assertEquals(r[1], "Greg")
  assertEquals(r[#r], "Alice")
end

-- 55. `using` with inline anonymous function

do
  local r = query [[
    from
      p = pages
    order by
      p.size using function(a, b) return a > b end
    select { name = p.name, size = p.size }
  ]]
  assertEquals(r[1].name, "Bob")   -- size 20
  assertEquals(r[2].name, "Dave")  -- size 15
end

-- 56. `using` with `nulls last`

do
  local r = query [[
    from
      p = pages
    order by
      p.tags[1] using function(a, b) return a < b end nulls last
    select { name = p.name, tag = p.tags[1] }
  ]]
  assertEquals(r[#r].name, "Ed")
end

-- 57. `using` with `nulls first`

do
  local r = query [[
    from
      p = pages
    order by
      p.tags[1] using function(a, b) return a < b end nulls first
    select { name = p.name, tag = p.tags[1] }
  ]]
  assertEquals(r[1].name, "Ed")
end

-- 58. `using` named function with `nulls last`

do
  local r = query [[
    from
      p = pages
    order by
      p.tags[1] using reverseAlpha nulls last
    select { name = p.name, tag = p.tags[1] }
  ]]
  assertEquals(r[#r].name, "Ed")
  assertEquals(r[1].tag, "work")
end

-- 59. `using` anonymous function with `nulls first`

do
  local r = query [[
    from
      p = pages
    order by
      p.tags[1] using function(a, b) return a < b end nulls first
    select { name = p.name, tag = p.tags[1] }
  ]]
  assertEquals(r[1].name, "Ed")
end

-- 60. `using` on one key, normal on another

do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    order by
      p.tags[1] using reverseAlpha,
      p.name
    select { tag = p.tags[1], name = p.name }
  ]]
  assertEquals(r[1].tag, "work")
  assertEquals(r[1].name, "Alice")
end

-- 61. Multiple keys with mixed using and desc

do
  local data = {
    { name = "a", x = 2, y = 10 },
    { name = "b", x = 1, y = 20 },
    { name = "c", x = 2, y = 5 },
    { name = "d", x = 1, y = 15 },
  }
  local r = query [[
    from
      p = data
    order by
      p.x using function(a, b) return a < b end,
      p.y desc
    select { name = p.name }
  ]]
  assertEquals(r[1].name, "b")
  assertEquals(r[2].name, "d")
  assertEquals(r[3].name, "a")
  assertEquals(r[4].name, "c")
end

-- 62. `using` anonymous function with `nulls first`

do
  local data = {
    { name = "a", val = 3 },
    { name = "b" },
    { name = "c", val = 1 },
  }
  local r = query [[
    from
      p = data
    order by
      p.val using function(a, b) return a > b end nulls first
    select { name = p.name }
  ]]
  assertEquals(r[1].name, "b")
  assertEquals(r[2].name, "a")
  assertEquals(r[3].name, "c")
end

-- 63. `using` with group by + aggregate order

do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      tag = key,
      n = count(),
    }
    order by
      n using function(a, b) return a > b end
  ]]
  assertEquals(r[1].tag, "work")
  assertEquals(r[1].n, 3)
  assertEquals(r[#r].n, 1)
end

-- 64. `using` named function in unbound mode

do
  local function byLen(a, b)
    return #a < #b
  end
  local r = query [[
    from
      pages
    order by
      name using byLen
    select name
  ]]
  assertEquals(r[1], "Ed")
end

-- 65. `using` anonymous function alone

do
  local r = query [[
    from
      p = pages
    order by
      p.age using function(a, b) return a > b end
    select { name = p.name, age = p.age }
  ]]
  assertEquals(r[1].name, "Greg")  -- age 63
  assertEquals(r[#r].name, "Ed")   -- age 19
end

-- 66. `using` with nulls on different keys

do
  local data = {
    { name = "a", x = 1, y = 10 },
    { name = "b", x = 1 },
    { name = "c", x = 2, y = 5 },
    { name = "d", x = 2 },
  }
  local r = query [[
    from
      p = data
    order by
      p.x using function(a, b) return a < b end,
      p.y using function(a, b) return a < b end nulls first
    select { name = p.name }
  ]]
  assertEquals(r[1].name, "b")
  assertEquals(r[2].name, "a")
  assertEquals(r[3].name, "d")
  assertEquals(r[4].name, "c")
end

-- 67. Intra-aggregate order by: array_agg asc

do
  local data = {
    { grp = "a", name = "cherry", val = 3 },
    { grp = "a", name = "apple",  val = 1 },
    { grp = "a", name = "banana", val = 2 },
  }
  local r = query [[
    from
      p = data
    group by
      p.grp
    select {
      names = array_agg(p.name order by p.val asc),
    }
  ]]
  assertEquals(#r, 1)
  assertEquals(r[1].names[1], "apple")
  assertEquals(r[1].names[2], "banana")
  assertEquals(r[1].names[3], "cherry")
end

-- 68. Intra-aggregate order by: array_agg desc

do
  local data = {
    { grp = "a", name = "cherry", val = 3 },
    { grp = "a", name = "apple",  val = 1 },
    { grp = "a", name = "banana", val = 2 },
  }
  local r = query [[
    from
      p = data
    group by
      p.grp
    select {
      names = array_agg(p.name order by p.val desc),
    }
  ]]
  assertEquals(r[1].names[1], "cherry")
  assertEquals(r[1].names[2], "banana")
  assertEquals(r[1].names[3], "apple")
end

-- 69. Intra-aggregate order by: same aggregate, asc vs desc in one select

do
  local data = {
    { grp = "x", name = "c", val = 3 },
    { grp = "x", name = "a", val = 1 },
    { grp = "x", name = "b", val = 2 },
  }
  local r = query [[
    from
      p = data
    group by
      p.grp
    select {
      asc_names = array_agg(p.name order by p.val asc),
      desc_names = array_agg(p.name order by p.val desc),
    }
  ]]
  assertEquals(r[1].asc_names[1], "a")
  assertEquals(r[1].asc_names[3], "c")
  assertEquals(r[1].desc_names[1], "c")
  assertEquals(r[1].desc_names[3], "a")
end

-- 70. Intra-aggregate order by: unbound access

do
  local data = {
    { grp = "x", name = "c", val = 3 },
    { grp = "x", name = "a", val = 1 },
    { grp = "x", name = "b", val = 2 },
  }
  local r = query [[
    from
      data
    group by
      grp
    select {
      names = array_agg(name order by val asc),
    }
  ]]
  assertEquals(r[1].names[1], "a")
  assertEquals(r[1].names[2], "b")
  assertEquals(r[1].names[3], "c")
end

-- 71. Intra-aggregate order by: order by the aggregated expression itself

do
  local data = {
    { grp = "x", name = "cherry" },
    { grp = "x", name = "apple"  },
    { grp = "x", name = "banana" },
  }
  local r = query [[
    from
      p = data
    group by
      p.grp
    select {
      names = array_agg(p.name order by p.name asc),
    }
  ]]
  assertEquals(r[1].names[1], "apple")
  assertEquals(r[1].names[2], "banana")
  assertEquals(r[1].names[3], "cherry")
end

-- 72. Intra-aggregate order by: multiple sort keys

do
  local data = {
    { grp = "x", name = "a2", cat = 1, pri = 2 },
    { grp = "x", name = "b1", cat = 2, pri = 1 },
    { grp = "x", name = "a1", cat = 1, pri = 1 },
    { grp = "x", name = "b2", cat = 2, pri = 2 },
  }
  local r = query [[
    from
      p = data
    group by
      p.grp
    select {
      names = array_agg(p.name order by p.cat asc, p.pri desc),
    }
  ]]
  assertEquals(r[1].names[1], "a2")
  assertEquals(r[1].names[2], "a1")
  assertEquals(r[1].names[3], "b2")
  assertEquals(r[1].names[4], "b1")
end

-- 73. Intra-aggregate order by: with nulls in sort key

do
  local data = {
    { grp = "x", name = "b", val = 2 },
    { grp = "x", name = "n" },
    { grp = "x", name = "a", val = 1 },
  }
  local r = query [[
    from
      p = data
    group by
      p.grp
    select {
      names = array_agg(p.name order by p.val asc),
    }
  ]]
  assertEquals(r[1].names[1], "a")
  assertEquals(r[1].names[2], "b")
  assertEquals(r[1].names[3], "n")
end

-- 74. Intra-aggregate order by: nulls first

do
  local data = {
    { grp = "x", name = "b", val = 2 },
    { grp = "x", name = "n" },
    { grp = "x", name = "a", val = 1 },
  }
  local r = query [[
    from
      p = data
    group by
      p.grp
    select {
      names = array_agg(p.name order by p.val asc nulls first),
    }
  ]]
  assertEquals(r[1].names[1], "n")
  assertEquals(r[1].names[2], "a")
  assertEquals(r[1].names[3], "b")
end

-- 75. Intra-aggregate order by: nulls last explicit on desc

do
  local data = {
    { grp = "x", name = "b", val = 2 },
    { grp = "x", name = "n" },
    { grp = "x", name = "a", val = 1 },
  }
  local r = query [[
    from
      p = data
    group by
      p.grp
    select {
      names = array_agg(p.name order by p.val desc nulls last),
    }
  ]]
  assertEquals(r[1].names[1], "b")
  assertEquals(r[1].names[2], "a")
  assertEquals(r[1].names[3], "n")
end

-- 76. Intra-aggregate order by: multiple groups

do
  local data = {
    { grp = "a", name = "z", val = 3 },
    { grp = "a", name = "x", val = 1 },
    { grp = "b", name = "m", val = 2 },
    { grp = "b", name = "k", val = 4 },
    { grp = "a", name = "y", val = 2 },
  }
  local r = query [[
    from
      p = data
    group by
      p.grp
    select {
      g = key,
      names = array_agg(p.name order by p.val asc),
    }
    order by
      g
  ]]
  assertEquals(#r, 2)

  assertEquals(r[1].names[1], "x")
  assertEquals(r[1].names[2], "y")
  assertEquals(r[1].names[3], "z")

  assertEquals(r[2].names[1], "m")
  assertEquals(r[2].names[2], "k")
end

-- 77. Intra-aggregate order by combined with filter

do
  local data = {
    { grp = "a", name = "d", val = 4, big = true },
    { grp = "a", name = "a", val = 1, big = false },
    { grp = "a", name = "c", val = 3, big = true },
    { grp = "a", name = "b", val = 2, big = false },
  }
  local r = query [[
    from
      p = data
    group by
      p.grp
    select {
      all_sorted = array_agg(p.name order by p.val asc),
      big_sorted = array_agg(p.name order by p.val desc)
                       filter(where p.big),
      small_sorted = array_agg(p.name order by p.name asc)
                       filter(where not p.big),
    }
  ]]
  assertEquals(r[1].all_sorted[1], "a")
  assertEquals(r[1].all_sorted[4], "d")

  assertEquals(r[1].big_sorted[1], "d")
  assertEquals(r[1].big_sorted[2], "c")

  assertEquals(r[1].small_sorted[1], "a")
  assertEquals(r[1].small_sorted[2], "b")
end

-- 78. Intra-aggregate order by: sum is unaffected (order doesn't change sum)

do
  local data = {
    { grp = "a", val = 10 },
    { grp = "a", val = 30 },
    { grp = "a", val = 20 },
  }
  local r = query [[
    from
      p = data
    group by
      p.grp
    select {
      s1 = sum(p.val order by p.val asc),
      s2 = sum(p.val order by p.val desc),
      s3 = sum(p.val),
    }
  ]]
  assertEquals(r[1].s1, 60)
  assertEquals(r[1].s2, 60)
  assertEquals(r[1].s3, 60)
end

-- 79. Intra-aggregate order by: count is unaffected

do
  local data = {
    { grp = "a", name = "c", val = 3 },
    { grp = "a", name = "a", val = 1 },
    { grp = "a", name = "b", val = 2 },
  }
  local r = query [[
    from
      p = data
    group by
      p.grp
    select {
      c1 = count(p.name order by p.val asc),
      c2 = count(p.name order by p.val desc),
      c3 = count(p.name),
    }
  ]]
  assertEquals(r[1].c1, 3)
  assertEquals(r[1].c2, 3)
  assertEquals(r[1].c3, 3)
end

-- 80. Intra-aggregate order by on pages dataset

do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      tag = key,
      names_asc = array_agg(p.name order by p.name asc),
      names_desc = array_agg(p.name order by p.name desc),
    }
    order by
      tag
  ]]
  assertEquals(r[1].tag, "personal")
  assertEquals(r[1].names_asc[1], "Carol")
  assertEquals(r[1].names_asc[2], "Dave")
  assertEquals(r[1].names_desc[1], "Dave")
  assertEquals(r[1].names_desc[2], "Carol")

  assertEquals(r[2].tag, "random")
  assertEquals(r[2].names_asc[1], "Fran")

  assertEquals(r[3].tag, "work")
  assertEquals(r[3].names_asc[1], "Alice")
  assertEquals(r[3].names_asc[2], "Bob")
  assertEquals(r[3].names_asc[3], "Greg")
  assertEquals(r[3].names_desc[1], "Greg")
  assertEquals(r[3].names_desc[2], "Bob")
  assertEquals(r[3].names_desc[3], "Alice")
end

-- 81. Intra-aggregate order by on pages dataset, order by size

do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      tag = key,
      by_size_asc = array_agg(p.name order by p.size asc),
      by_size_desc = array_agg(p.name order by p.size desc),
    }
    order by
      tag
  ]]
  -- work: Greg(2), Alice(10), Bob(20)
  assertEquals(r[3].tag, "work")
  assertEquals(r[3].by_size_asc[1], "Greg")
  assertEquals(r[3].by_size_asc[2], "Alice")
  assertEquals(r[3].by_size_asc[3], "Bob")
  assertEquals(r[3].by_size_desc[1], "Bob")
  assertEquals(r[3].by_size_desc[2], "Alice")
  assertEquals(r[3].by_size_desc[3], "Greg")
end

-- 82. Intra-aggregate order by + filter on pages dataset

do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      tag = key,
      big_by_age = array_agg(p.name order by p.age desc) filter(where p.size>2),
    }
    order by
      tag
  ]]
  assertEquals(r[1].tag, "personal")
  assertEquals(r[1].big_by_age[1], "Dave")
  assertEquals(r[1].big_by_age[2], "Carol")

  assertEquals(r[2].tag, "random")
  assertEquals(#r[2].big_by_age, 0)

  assertEquals(r[3].tag, "work")
  assertEquals(#r[3].big_by_age, 2)
  assertEquals(r[3].big_by_age[1], "Alice") -- age 31 > 25
  assertEquals(r[3].big_by_age[2], "Bob")
end

-- 83. Intra-aggregate order by with group by "all"

do
  local r = query [[
    from
      p = pages
    group by
      "all"
    select {
      youngest_first = array_agg(p.name order by p.age asc),
      oldest_first = array_agg(p.name order by p.age desc),
    }
  ]]
  assertEquals(#r, 1)

  assertEquals(r[1].youngest_first[1], "Ed")
  assertEquals(r[1].youngest_first[7], "Greg")
  assertEquals(r[1].oldest_first[1], "Greg")
  assertEquals(r[1].oldest_first[7], "Ed")
end

-- 84. Intra-aggregate order by: empty group produces empty array

do
  local data = {
    { grp = "a", name = "x", val = 1 },
  }
  local r = query [[
    from
      p = data
    group by
      p.grp
    select {
      filtered = array_agg(p.name order by p.val asc) filter(where p.val>100),
    }
  ]]
  assertEquals(#r[1].filtered, 0)
end

-- 85. Full pipeline

do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    having
      count() > 1
    select {
      tag = key,
      total = count(),
      sorted_names = array_agg(p.name order by p.age desc),
      young_names = array_agg(p.name order by p.name asc)
        filter(where p.age < 40),
      oldest = max(p.age),
    }
    order by
      total desc, tag
    limit
      2
  ]]
  assertEquals(#r, 2)

  assertEquals(r[1].tag, "work")
  assertEquals(r[1].total, 3)

  assertEquals(r[1].sorted_names[1], "Greg")
  assertEquals(r[1].sorted_names[2], "Alice")
  assertEquals(r[1].sorted_names[3], "Bob")

  assertEquals(r[1].young_names[1], "Alice")
  assertEquals(r[1].young_names[2], "Bob")
  assertEquals(r[1].oldest, 63)

  assertEquals(r[2].tag, "personal")
  assertEquals(r[2].total, 2)

  assertEquals(r[2].sorted_names[1], "Dave")
  assertEquals(r[2].sorted_names[2], "Carol")

  assertEquals(#r[2].young_names, 0)
  assertEquals(r[2].oldest, 52)
end

-- 86. Full pipeline with composite group by + intra-aggregate order by

do
  local data = {
    { dept = "eng",   level = "sr", name = "Alice", salary = 100 },
    { dept = "eng",   level = "sr", name = "Bob",   salary = 120 },
    { dept = "eng",   level = "jr", name = "Carol", salary = 60  },
    { dept = "sales", level = "sr", name = "Dave",  salary = 90  },
    { dept = "sales", level = "jr", name = "Eve",   salary = 50  },
    { dept = "sales", level = "jr", name = "Fran",  salary = 55  },
  }
  local r = query [[
    from
      p = data
    group by
      p.dept, p.level
    having
      count() > 1
    select {
      dept = key[1],
      level = key[2],
      n = count(),
      names_by_salary = array_agg(p.name order by p.salary desc),
      total_salary = sum(p.salary),
      top_earner = max(p.salary),
    }
    order by
      dept, level
  ]]
  assertEquals(#r, 2)

  assertEquals(r[1].dept, "eng")
  assertEquals(r[1].level, "sr")
  assertEquals(r[1].n, 2)
  assertEquals(r[1].names_by_salary[1], "Bob")   -- 120
  assertEquals(r[1].names_by_salary[2], "Alice") -- 100
  assertEquals(r[1].total_salary, 220)
  assertEquals(r[1].top_earner, 120)

  assertEquals(r[2].dept, "sales")
  assertEquals(r[2].level, "jr")
  assertEquals(r[2].n, 2)
  assertEquals(r[2].names_by_salary[1], "Fran") -- 55
  assertEquals(r[2].names_by_salary[2], "Eve")  -- 50
  assertEquals(r[2].total_salary, 105)
  assertEquals(r[2].top_earner, 55)
end

-- 87. Intra-aggregate order by with unbound access, full pipeline

do
  local r = query [[
    from
      pages
    where
      tags[1] ~= nil
    group by
      tags[1]
    having
      count() >= 2
    select {
      tag = key,
      by_age = array_agg(name order by age asc),
      by_size = array_agg(name order by size desc),
    }
    order by
      tag
    limit
      2
  ]]
  assertEquals(#r, 2)

  assertEquals(r[1].tag, "personal")
  assertEquals(r[1].by_age[1], "Carol")
  assertEquals(r[1].by_age[2], "Dave")

  assertEquals(r[1].by_size[1], "Dave")
  assertEquals(r[1].by_size[2], "Carol")

  assertEquals(r[2].tag, "work")
  assertEquals(r[2].by_age[1], "Bob")
  assertEquals(r[2].by_age[2], "Alice")
  assertEquals(r[2].by_age[3], "Greg")

  assertEquals(r[2].by_size[1], "Bob")
  assertEquals(r[2].by_size[2], "Alice")
  assertEquals(r[2].by_size[3], "Greg")
end

-- 88. Intra-aggregate order by does not affect min/max/avg results

do
  local data = {
    { grp = "a", val = 30 },
    { grp = "a", val = 10 },
    { grp = "a", val = 20 },
  }
  local r = query [[
    from
      p = data
    group by
      p.grp
    select {
      mn1 = min(p.val order by p.val asc),
      mn2 = min(p.val order by p.val desc),
      mx1 = max(p.val order by p.val asc),
      mx2 = max(p.val order by p.val desc),
      av1 = avg(p.val order by p.val asc),
      av2 = avg(p.val order by p.val desc),
    }
  ]]
  assertEquals(r[1].mn1, 10)
  assertEquals(r[1].mn2, 10)
  assertEquals(r[1].mx1, 30)
  assertEquals(r[1].mx2, 30)
  assertEquals(r[1].av1, 20)
  assertEquals(r[1].av2, 20)
end

-- 89. Intra-aggregate order by + filter + outer order by + offset

do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      tag = key,
      names = array_agg(p.name order by p.size asc) filter(where p.size > 1),
    }
    order by
      tag
    limit
      2, 1
  ]]
  assertEquals(#r, 2)
  assertEquals(r[1].tag, "random")
  assertEquals(r[2].tag, "work")
  assertEquals(r[2].names[1], "Greg")
  assertEquals(r[2].names[2], "Alice")
  assertEquals(r[2].names[3], "Bob")
end

-- 90. Complex full pipeline

do
  local data = {
    { dept = "eng",   name = "Alice", salary = 100, active = true  },
    { dept = "eng",   name = "Bob",   salary = 150, active = true  },
    { dept = "eng",   name = "Carol", salary = 80,  active = false },
    { dept = "sales", name = "Dave",  salary = 90,  active = true  },
    { dept = "sales", name = "Eve",   salary = 70,  active = true  },
    { dept = "sales", name = "Fran",  salary = 60,  active = false },
    { dept = "hr",    name = "Greg",  salary = 50,  active = true  },
  }

  local function salaryDesc(a, b)
    return a > b
  end

  local r = query [[
    from
      p = data
    where
      p.active
    group by
      p.dept
    having
      count() >= 2
    select {
      dept = key,
      headcount = count(),
      total_salary = sum(p.salary),
      avg_salary = avg(p.salary),
      top_salary = max(p.salary),
      names_by_sal = array_agg(p.name order by p.salary desc),
      cheap_names = array_agg(p.name order by p.name asc)
        filter(where p.salary < 100),
    }
    order by
      total_salary using salaryDesc
    limit
      2
  ]]

  assertEquals(#r, 2)

  assertEquals(r[1].dept, "eng")
  assertEquals(r[1].headcount, 2)
  assertEquals(r[1].total_salary, 250)
  assertEquals(r[1].avg_salary, 125)
  assertEquals(r[1].top_salary, 150)
  assertEquals(r[1].names_by_sal[1], "Bob")   -- 150
  assertEquals(r[1].names_by_sal[2], "Alice") -- 100
  assertEquals(#r[1].cheap_names, 0)

  assertEquals(r[2].dept, "sales")
  assertEquals(r[2].headcount, 2)
  assertEquals(r[2].total_salary, 160)
  assertEquals(r[2].avg_salary, 80)
  assertEquals(r[2].top_salary, 90)
  assertEquals(r[2].names_by_sal[1], "Dave") -- 90
  assertEquals(r[2].names_by_sal[2], "Eve")  -- 70
  assertEquals(r[2].cheap_names[1], "Dave")
  assertEquals(r[2].cheap_names[2], "Eve")
end

-- 91. Attempt to use `order by` in non-aggregate function call errors

do
  local ok, err = pcall(function()
    local r = query [[
      from
        p = pages
      select
        tostring(p.name order by p.name)
    ]]
  end)
  assertEquals(ok, false)
  assertTrue(
    string.find(tostring(err), "'order by' is not allowed") ~= nil,
    "expected `order by` error, got: " .. tostring(err)
  )
end

-- 92. Standalone offset clause

-- 92a. Offset only, bound
do
  local r = query [[
    from
      p = pages
    select { name = p.name }
    order by
      p.name
    offset
      2
  ]]
  assertEquals(#r, 5)
  assertEquals(r[1].name, "Carol")
end

-- 92b. Offset only, unbound
do
  local r = query [[
    from
      pages
    order by
      name
    offset
      5
  ]]
  assertEquals(#r, 2)
  assertEquals(r[1].name, "Fran")
  assertEquals(r[2].name, "Greg")
end

-- 92c. Offset 0 returns everything
do
  local r = query [[
    from
      pages
    offset
      0
  ]]
  assertEquals(#r, #pages)
end

-- 92d. Offset larger than dataset returns empty
do
  local r = query [[
    from
      pages
    offset
      100
  ]]
  assertEquals(#r, 0)
end

-- 92e. Offset equal to dataset size returns empty
do
  local r = query [[
    from
      pages
    offset
      7
  ]]
  assertEquals(#r, 0)
end

-- 93. Standalone offset + limit (separate clauses)

-- 93a. Offset before limit
do
  local r = query [[
    from
      p = pages
    select { name = p.name }
    order by
      p.name
    offset
      2
    limit
      3
  ]]
  -- skip 2, take 3 -> Carol, Dave, Ed
  assertEquals(#r, 3)
  assertEquals(r[1].name, "Carol")
  assertEquals(r[2].name, "Dave")
  assertEquals(r[3].name, "Ed")
end

-- 93b. Limit before offset (order of clauses doesn't matter)
do
  local r = query [[
    from
      p = pages
    select { name = p.name }
    order by
      p.name
    limit
      3
    offset
      2
  ]]
  assertEquals(#r, 3)
  assertEquals(r[1].name, "Carol")
end

-- 93c. Offset beyond available rows with limit returns empty
do
  local r = query [[
    from
      pages
    limit
      3
    offset
      100
  ]]
  assertEquals(#r, 0)
end

-- 93d. Limit larger than remaining after offset
do
  local r = query [[
    from
      p = pages
    order by
      p.name
    offset
      5
    limit
      100
  ]]
  assertEquals(#r, 2)
end

-- 94. Standalone offset with where + order by

do
  local r = query [[
    from
      p = pages
    where
      p.size >= 5
    order by
      p.size
    select { name = p.name }
    offset
      1
  ]]
  assertEquals(#r, 3)
  assertEquals(r[1].name, "Alice")
  assertEquals(r[2].name, "Dave")
  assertEquals(r[3].name, "Bob")
end

-- 95. Standalone offset with group by

do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    select {
      tag = key,
      n = count(),
    }
    order by
      tag
    offset
      1
  ]]
  assertEquals(#r, 2)
  assertEquals(r[1].tag, "random")
  assertEquals(r[2].tag, "work")
end

-- 96. Standalone offset + limit with group by + having

do
  local r = query [[
    from
      p = pages
    where
      p.tags[1] ~= nil
    group by
      p.tags[1]
    having
      count() > 0
    select {
      tag = key,
      n = count(),
    }
    order by
      tag
    offset
      1
    limit
      1
  ]]
  assertEquals(#r, 1)
  assertEquals(r[1].tag, "random")
end

-- 97. Standalone offset wins over inline offset (last one wins)
do
  local r = query [[
    from
      p = pages
    order by
      p.name
    limit
      3, 1
    offset
      2
  ]]
  assertEquals(#r, 3)
  assertEquals(r[1].name, "Carol")
end

-- 98. Multi-source from (cross join) — basic

-- 98a. Two-source cross join, basic
do
  local colors = {
    { color = "red" },
    { color = "blue" },
  }
  local sizes = {
    { size = "S" },
    { size = "M" },
    { size = "L" },
  }
  local r = query [[
    from
      c = colors,
      s = sizes
    select {
      color = c.color,
      size = s.size,
    }
  ]]
  assertEquals(#r, 6, "98a: row count")
end

-- 98b. Two-source cross join with where
do
  local colors = {
    { color = "red" },
    { color = "blue" },
  }
  local sizes = {
    { size = "S" },
    { size = "M" },
    { size = "L" },
  }
  local r = query [[
    from
      c = colors,
      s = sizes
    where
      s.size ~= "L"
    select {
      color = c.color,
      size = s.size,
    }
  ]]
  assertEquals(#r, 4, "98b: row count")
end

-- 98c. Two-source cross join with order by
do
  local xs = {
    { x = 2 },
    { x = 1 },
  }
  local ys = {
    { y = "b" },
    { y = "a" },
  }
  local r = query [[
    from
      a = xs,
      b = ys
    select {
      x = a.x,
      y = b.y,
    }
    order by
      a.x, b.y
  ]]
  assertEquals(#r, 4, "98c: row count")
  assertEquals(r[1].x, 1, "98c: r1.x")
  assertEquals(r[1].y, "a", "98c: r1.y")
  assertEquals(r[2].x, 1, "98c: r2.x")
  assertEquals(r[2].y, "b", "98c: r2.y")
  assertEquals(r[3].x, 2, "98c: r3.x")
  assertEquals(r[3].y, "a", "98c: r3.y")
  assertEquals(r[4].x, 2, "98c: r4.x")
  assertEquals(r[4].y, "b", "98c: r4.y")
end

-- 98d. Two-source cross join with limit
do
  local xs = { { v = 1 }, { v = 2 }, { v = 3 } }
  local ys = { { v = 10 }, { v = 20 } }
  local r = query [[
    from
      a = xs,
      b = ys
    select {
      s = a.v + b.v,
    }
    limit
      3
  ]]
  assertEquals(#r, 3, "98d: row count")
end

-- 98e. Two-source cross join, select single expression
do
  local as = { { n = "x" }, { n = "y" } }
  local bs = { { n = "1" }, { n = "2" } }
  local r = query [[
    from
      a = as,
      b = bs
    select
      a.n .. b.n
  ]]
  assertEquals(#r, 4, "98e: row count")
end

-- 99. Three-source cross join

-- 99a. Three-source basic
do
  local xs = { { x = 1 }, { x = 2 } }
  local ys = { { y = "a" }, { y = "b" } }
  local zs = { { z = "p" }, { z = "q" } }
  local r = query [[
    from
      a = xs,
      b = ys,
      c = zs
    select {
      x = a.x,
      y = b.y,
      z = c.z,
    }
  ]]
  assertEquals(#r, 8, "99a: row count")
end

-- 99b. Three-source with where filter
do
  local xs = { { x = 1 }, { x = 2 }, { x = 3 } }
  local ys = { { y = 10 }, { y = 20 } }
  local zs = { { z = 100 } }
  local r = query [[
    from
      a = xs,
      b = ys,
      c = zs
    where
      a.x + b.y > 15
    select {
      total = a.x + b.y + c.z,
    }
  ]]
  -- 1+20=21 yes, 2+20=22 yes, 3+20=23 yes -> 3 combos
  -- totals: 121, 122, 123 all distinct
  assertEquals(#r, 3, "99b: row count")
end

-- 99c. Three-source with order by + limit
do
  local xs = { { v = 1 }, { v = 2 } }
  local ys = { { v = 30 }, { v = 40 } }
  local zs = { { v = 500 }, { v = 600 } }
  local r = query [[
    from
      a = xs,
      b = ys,
      c = zs
    select {
      s = a.v + b.v + c.v,
    }
    order by
      a.v + b.v + c.v
    limit
      3
  ]]
  assertEquals(#r, 3, "99c: row count")
  assertEquals(r[1].s, 531, "99c: first")   -- 1+30+500
  assertEquals(r[2].s, 532, "99c: second")  -- 2+30+500
  assertEquals(r[3].s, 541, "99c: third")   -- 1+40+500
end

-- 100. Four-source cross join

-- 100a. Four-source basic
do
  local a = { { v = 1 } }
  local b = { { v = 2 }, { v = 3 } }
  local c = { { v = 4 } }
  local d = { { v = 50 }, { v = 60 } }
  local r = query [[
    from
      w = a,
      x = b,
      y = c,
      z = d
    select {
      s = w.v + x.v + y.v + z.v,
    }
    order by
      w.v + x.v + y.v + z.v
  ]]
  -- 1*2*1*2 = 4 rows, sums: 57,67,58,68 all distinct
  assertEquals(#r, 4, "100a: row count")
  assertEquals(r[1].s, 57, "100a: first")   -- 1+2+4+50
  assertEquals(r[2].s, 58, "100a: second")  -- 1+3+4+50
  assertEquals(r[3].s, 67, "100a: third")   -- 1+2+4+60
  assertEquals(r[4].s, 68, "100a: fourth")  -- 1+3+4+60
end

-- 101. Join hints: hash, loop, merge

-- 101a. Two-source with `hash` hint
do
  local xs = { { x = 1 }, { x = 2 } }
  local ys = { { y = 10 }, { y = 20 } }
  local r = query [[
    from
      a = xs,
      b = ys hash
    select {
      x = a.x,
      y = b.y,
    }
  ]]
  assertEquals(#r, 4, "101a: row count")
end

-- 101b. Two-source with `loop` hint
do
  local xs = { { x = 1 }, { x = 2 } }
  local ys = { { y = 10 }, { y = 20 } }
  local r = query [[
    from
      a = xs,
      b = ys loop
    select {
      x = a.x,
      y = b.y,
    }
  ]]
  assertEquals(#r, 4, "101b: row count")
end

-- 101c. Three-source with mixed hints
do
  local xs = { { x = 1 }, { x = 2 } }
  local ys = { { y = "a" } }
  local zs = { { z = "p" }, { z = "q" } }
  local r = query [[
    from
      a = xs,
      b = ys hash,
      c = zs loop
    select {
      x = a.x,
      y = b.y,
      z = c.z,
    }
  ]]
  assertEquals(#r, 4, "101d: row count")
end

-- 101d. Hint on first source in multi-source
do
  local xs = { { x = 1 } }
  local ys = { { y = 2 } }
  local r = query [[
    from
      a = xs hash,
      b = ys
    select {
      x = a.x,
      y = b.y,
    }
  ]]
  assertEquals(#r, 1, "101e: row count")
end

-- 101e. All three hint types produce same results
do
  local xs = { { x = 1 }, { x = 2 } }
  local ys = { { y = 10 }, { y = 20 } }
  local rh = query [[
    from a = xs, b = ys hash
    select { x = a.x, y = b.y }
    order by a.x, b.y
  ]]
  local rl = query [[
    from a = xs, b = ys loop
    select { x = a.x, y = b.y }
    order by a.x, b.y
  ]]
  local rm = query [[
    from a = xs, b = ys
    select { x = a.x, y = b.y }
    order by a.x, b.y
  ]]
  assertEquals(#rh, #rl, "101f: hash vs loop count")
  assertEquals(#rh, #rm, "101f: hash vs merge count")
  for i = 1, #rh do
    assertEquals(rh[i].x, rl[i].x, "101f: hash vs loop x at " .. i)
    assertEquals(rh[i].y, rl[i].y, "101f: hash vs loop y at " .. i)
    assertEquals(rh[i].x, rm[i].x, "101f: hash vs merge x at " .. i)
    assertEquals(rh[i].y, rm[i].y, "101f: hash vs merge y at " .. i)
  end
end

-- 102. leading

-- 102a. Two-source with leading
do
  local xs = { { x = 1 }, { x = 2 } }
  local ys = { { y = 10 }, { y = 20 } }
  local r = query [[
    from
      a = xs,
      b = ys
    leading b, a
    select {
      x = a.x,
      y = b.y,
    }
  ]]
  assertEquals(#r, 4, "102a: row count")
end

-- 102b. Three-source with leading
do
  local xs = { { v = 1 } }
  local ys = { { v = 20 }, { v = 30 } }
  local zs = { { v = 400 } }
  local r = query [[
    from
      a = xs,
      b = ys,
      c = zs
    leading c, a, b
    select {
      s = a.v + b.v + c.v,
    }
    order by
      a.v + b.v + c.v
  ]]
  assertEquals(#r, 2, "102b: row count")
  assertEquals(r[1].s, 421, "102b: first")  -- 1+20+400
  assertEquals(r[2].s, 431, "102b: second") -- 1+30+400
end

-- 102c. leading with join hint
do
  local xs = { { x = 1 }, { x = 2 } }
  local ys = { { y = 10 }, { y = 20 } }
  local r = query [[
    from
      a = xs hash,
      b = ys loop
    leading b, a
    select {
      x = a.x,
      y = b.y,
    }
  ]]
  assertEquals(#r, 4, "102c: row count")
end

-- 102d. leading with where filter
do
  local xs = { { x = 1 }, { x = 2 }, { x = 3 } }
  local ys = { { y = 10 }, { y = 20 } }
  local r = query [[
    from
      a = xs,
      b = ys
    leading a, b
    where
      a.x > 1
    select {
      x = a.x,
      y = b.y,
    }
    order by
      a.x, b.y
  ]]
  assertEquals(#r, 4, "102d: row count")
  assertEquals(r[1].x, 2, "102d: r1.x")
  assertEquals(r[1].y, 10, "102d: r1.y")
end

-- 102e. leading partial (only some sources named)
do
  local xs = { { v = 1 }, { v = 2 } }
  local ys = { { v = 100 } }
  local zs = { { v = 1000 } }
  local r = query [[
    from
      a = xs,
      b = ys,
      c = zs
    leading c
    select {
      s = a.v + b.v + c.v,
    }
    order by
      a.v + b.v + c.v
  ]]
  assertEquals(#r, 2, "102e: row count")
  assertEquals(r[1].s, 1101, "102e: first")  -- 1+100+1000
  assertEquals(r[2].s, 1102, "102e: second") -- 2+100+1000
end

-- 103. Multi-source with group by and aggregates
do
  local depts = {
    { dept = "eng" },
    { dept = "sales" },
  }
  local employees = {
    { name = "Alice", dept = "eng" },
    { name = "Bob",   dept = "eng" },
    { name = "Carol", dept = "sales" },
  }
  local r = query [[
    from
      d = depts,
      e = employees
    where
      d.dept == e.dept
    select all {
      dept = d.dept,
      name = e.name,
    }
    order by
      dept, name
  ]]

  assertEquals(#r, 3)
  assertEquals(r[1].dept, "eng")
  assertEquals(r[1].name, "Alice")
  assertEquals(r[2].dept, "eng")
  assertEquals(r[2].name, "Bob")
  assertEquals(r[3].dept, "sales")
  assertEquals(r[3].name, "Carol")
end

-- 103a. Cross join + group by + count
do
  local depts = {
    { dept = "eng" },
    { dept = "sales" },
  }
  local employees = {
    { name = "Alice", dept = "eng" },
    { name = "Bob",   dept = "eng" },
    { name = "Carol", dept = "sales" },
  }
  local r = query [[
    from
      d = depts,
      e = employees
    where
      d.dept == e.dept
    group by
      d.dept
    select {
      dept = key,
      n = count(),
    }
    order by
      dept
  ]]

  assertEquals(#r, 2, "103a: row count")
  assertEquals(r[1].dept, "eng", "103a: r1.dept")
  assertTrue(type(r[1].n) == "number", "103a: r1.n type=" .. tostring(r[1].n))
  assertEquals(r[2].dept, "sales", "103a: r2.dept")
  assertTrue(type(r[2].n) == "number", "103a: r2.n type=" .. tostring(r[2].n))
end

-- 103b. Cross join + group by + sum + array_agg
do
  local categories = {
    { cat = "fruit" },
    { cat = "veg" },
  }
  local items = {
    { name = "apple",  cat = "fruit", price = 3 },
    { name = "banana", cat = "fruit", price = 2 },
    { name = "carrot", cat = "veg",   price = 1 },
    { name = "daikon", cat = "veg",   price = 4 },
  }
  local r = query [[
    from
      c = categories,
      i = items
    where
      c.cat == i.cat
    group by
      c.cat
    select {
      cat = key,
      total = sum(i.price),
      names = array_agg(i.name order by i.name asc),
    }
    order by
      cat
  ]]
  assertEquals(#r, 2, "103b: row count")
  assertEquals(r[1].cat, "fruit", "103b: r1.cat")
  assertEquals(r[1].total, 5, "103b: r1.total")
  assertEquals(r[1].names[1], "apple", "103b: r1.names[1]")
  assertEquals(r[1].names[2], "banana", "103b: r1.names[2]")
  assertEquals(r[2].cat, "veg", "103b: r2.cat")
  assertEquals(r[2].total, 5, "103b: r2.total")
  assertEquals(r[2].names[1], "carrot", "103b: r2.names[1]")
  assertEquals(r[2].names[2], "daikon", "103b: r2.names[2]")
end

-- 103c. Cross join + group by + having
do
  local depts = {
    { dept = "eng" },
    { dept = "sales" },
    { dept = "hr" },
  }
  local employees = {
    { name = "Alice", dept = "eng" },
    { name = "Bob",   dept = "eng" },
    { name = "Carol", dept = "sales" },
  }
  local r = query [[
    from
      d = depts,
      e = employees
    where
      d.dept == e.dept
    group by
      d.dept
    having
      count() >= 2
    select {
      dept = key,
      n = count(),
    }
  ]]
  assertEquals(#r, 1, "103c: row count")
  assertEquals(r[1].dept, "eng", "103c: dept")
  assertEquals(r[1].n, 2, "103c: n")
end

-- 103d. Cross join + group by + min/max/avg
do
  local groups = { { g = "A" }, { g = "B" } }
  local vals = {
    { g = "A", v = 10 },
    { g = "A", v = 20 },
    { g = "A", v = 30 },
    { g = "B", v = 5 },
    { g = "B", v = 15 },
  }
  local r = query [[
    from
      gr = groups,
      item = vals
    where
      gr.g == item.g
    group by
      gr.g
    select {
      g = key,
      lo = min(item.v),
      hi = max(item.v),
      av = avg(item.v),
    }
    order by
      g
  ]]
  assertEquals(#r, 2, "103d: row count")
  assertEquals(r[1].lo, 10, "103d: A lo")
  assertEquals(r[1].hi, 30, "103d: A hi")
  assertEquals(r[1].av, 20, "103d: A avg")
  assertEquals(r[2].lo, 5, "103d: B lo")
  assertEquals(r[2].hi, 15, "103d: B hi")
  assertEquals(r[2].av, 10, "103d: B avg")
end

-- 104. Edge cases for multi-source from

-- 104a. Empty first source
do
  local xs = {}
  local ys = { { y = 1 }, { y = 2 } }
  local r = query [[
    from
      a = xs,
      b = ys
    select {
      y = b.y,
    }
  ]]
  assertEquals(#r, 0, "104a: row count")
end

-- 104b. Empty second source
do
  local xs = { { x = 1 }, { x = 2 } }
  local ys = {}
  local r = query [[
    from
      a = xs,
      b = ys
    select {
      x = a.x,
    }
  ]]
  assertEquals(#r, 0, "104b: row count")
end

-- 104c. Singleton * singleton
do
  local xs = { { x = 42 } }
  local ys = { { y = 99 } }
  local r = query [[
    from
      a = xs,
      b = ys
    select {
      x = a.x,
      y = b.y,
    }
  ]]
  assertEquals(#r, 1, "104c: row count")
  assertEquals(r[1].x, 42, "104c: x")
  assertEquals(r[1].y, 99, "104c: y")
end

-- 104d. Same source twice (self-join)
do
  local xs = { { v = 1 }, { v = 2 }, { v = 3 } }
  local r = query [[
    from
      a = xs,
      b = xs
    where
      a.v < b.v
    select {
      av = a.v,
      bv = b.v,
    }
    order by
      a.v, b.v
  ]]
  assertEquals(#r, 3, "104d: row count")
  assertEquals(r[1].av, 1, "104d: r1.av")
  assertEquals(r[1].bv, 2, "104d: r1.bv")
  assertEquals(r[2].av, 1, "104d: r2.av")
  assertEquals(r[2].bv, 3, "104d: r2.bv")
  assertEquals(r[3].av, 2, "104d: r3.av")
  assertEquals(r[3].bv, 3, "104d: r3.bv")
end

-- 104e. Self-join three-way (triangles)
do
  local nums = { { v = 1 }, { v = 2 }, { v = 3 }, { v = 4 } }
  local r = query [[
    from
      a = nums,
      b = nums,
      c = nums
    where
      a.v < b.v and b.v < c.v
    select {
      triple = a.v .. "-" .. b.v .. "-" .. c.v,
    }
    order by
      a.v, b.v, c.v
  ]]
  assertEquals(#r, 4, "104e: row count")
  assertEquals(r[1].triple, "1-2-3", "104e: first")
  assertEquals(r[2].triple, "1-2-4", "104e: second")
  assertEquals(r[3].triple, "1-3-4", "104e: third")
  assertEquals(r[4].triple, "2-3-4", "104e: fourth")
end

-- 104f. Cross join result used with offset
do
  local xs = { { x = 1 }, { x = 2 } }
  local ys = { { y = "a" }, { y = "b" }, { y = "c" } }
  local r = query [[
    from
      a = xs,
      b = ys
    select {
      x = a.x,
      y = b.y,
    }
    order by
      a.x, b.y
    limit
      3, 2
  ]]
  assertEquals(#r, 3, "104f: row count")
end

-- 104g. Without select, each row has source names as fields
do
  local xs = { { x = 1 } }
  local ys = { { y = 2 } }
  local r = query [[
    from
      a = xs,
      b = ys
  ]]
  assertEquals(#r, 1, "104g: row count")
  assertEquals(r[1].a.x, 1, "104g: a.x")
  assertEquals(r[1].b.y, 2, "104g: b.y")
end

-- 104h. Where that matches nothing
do
  local xs = { { v = 1 }, { v = 2 } }
  local ys = { { v = 3 }, { v = 4 } }
  local r = query [[
    from
      a = xs,
      b = ys
    where
      a.v > 100
    select {
      av = a.v,
    }
  ]]
  assertEquals(#r, 0, "104h: row count")
end

-- 104i. Outer variable accessible in where
do
  local threshold = 15
  local xs = { { v = 10 }, { v = 20 } }
  local ys = { { v = 1 }, { v = 2 } }
  local r = query [[
    from
      a = xs,
      b = ys
    where
      a.v + b.v > threshold
    select {
      s = a.v + b.v,
    }
    order by
      a.v + b.v
  ]]
  assertEquals(#r, 2, "104i: row count")
  assertEquals(r[1].s, 21, "104i: first")
  assertEquals(r[2].s, 22, "104i: second")
end

-- 104j. Nested field access
do
  local xs = { { info = { label = "x1" } }, { info = { label = "x2" } } }
  local ys = { { info = { label = "y1" } } }
  local r = query [[
    from
      a = xs,
      b = ys
    select {
      pair = a.info.label .. "-" .. b.info.label,
    }
    order by
      a.info.label
  ]]
  assertEquals(#r, 2, "104j: row count")
  assertEquals(r[1].pair, "x1-y1", "104j: first")
  assertEquals(r[2].pair, "x2-y1", "104j: second")
end

-- 105. Multi-source with existing pages dataset

-- 105a. Pages self-join (find pairs sharing same first tag)
do
  local r = query [[
    from
      p1 = pages,
      p2 = pages
    where
      p1.name < p2.name and p1.tags[1] == p2.tags[1]
    select {
      a = p1.name,
      b = p2.name,
      tag = p1.tags[1],
    }
    order by
      p1.name, p2.name
  ]]
  assertEquals(#r, 4, "105a: row count")
  assertEquals(r[1].a, "Alice", "105a: first pair a")
  assertEquals(r[1].b, "Bob", "105a: first pair b")
end

-- 105b. Pages cross with small dataset, group by
do
  local thresholds = {
    { label = "small", max_size = 5 },
    { label = "big", max_size = 100 },
  }
  local r = query [[
    from
      p = pages,
      t = thresholds
    where
      p.size <= t.max_size
    group by
      t.label
    select {
      label = key,
      n = count(),
    }
    order by
      label
  ]]
  assertEquals(#r, 2, "105b: row count")
  assertEquals(r[1].label, "big", "105b: r1.label")
  assertEquals(r[1].n, 7, "105b: r1.n")
  assertEquals(r[2].label, "small", "105b: r2.label")
  assertEquals(r[2].n, 4, "105b: r2.n")
end

-- 106. leading + full pipeline

-- 106a. leading + where + select + order by + limit
do
  local xs = { { x = 1 }, { x = 2 }, { x = 3 } }
  local ys = { { y = 100 }, { y = 200 }, { y = 300 } }
  local r = query [[
    from
      a = xs,
      b = ys
    leading b, a
    where
      a.x + b.y <= 202
    select {
      s = a.x + b.y,
    }
    order by
      a.x + b.y desc
    limit
      4
  ]]
  assertEquals(#r, 4, "106a: row count")
  assertEquals(r[1].s, 202, "106a: first")
  assertEquals(r[2].s, 201, "106a: second")
  assertEquals(r[3].s, 103, "106a: third")
  assertEquals(r[4].s, 102, "106a: fourth")
end

-- 107. Multi-source with order by using comparator

-- 107a. Custom comparator on cross join result
do
  local xs = { { v = 1 }, { v = 2 } }
  local ys = { { v = 100 }, { v = 200 } }
  local r = query [[
    from
      a = xs,
      b = ys
    select {
      s = a.v + b.v,
    }
    order by
      a.v + b.v using function(a, b) return a > b end
  ]]
  assertEquals(r[1].s, 202, "107a: first")
  assertEquals(r[#r].s, 101, "107a: last")
end

-- 107b. Order by field from each source
do
  local xs = { { v = 2 }, { v = 1 } }
  local ys = { { v = 20 }, { v = 10 } }
  local r = query [[
    from
      a = xs,
      b = ys
    select {
      av = a.v,
      bv = b.v,
    }
    order by
      a.v, b.v
  ]]
  assertEquals(#r, 4, "107b: row count")
  assertEquals(r[1].av, 1, "107b: r1.av")
  assertEquals(r[1].bv, 10, "107b: r1.bv")
  assertEquals(r[2].av, 1, "107b: r2.av")
  assertEquals(r[2].bv, 20, "107b: r2.bv")
  assertEquals(r[3].av, 2, "107b: r3.av")
  assertEquals(r[3].bv, 10, "107b: r3.bv")
  assertEquals(r[4].av, 2, "107b: r4.av")
  assertEquals(r[4].bv, 20, "107b: r4.bv")
end

-- 108. Four-source with full pipeline

do
  local colors   = { { c = "red" }, { c = "blue" } }
  local sizes    = { { s = "S" }, { s = "M" }, { s = "L" } }
  local prices   = { { p = 10 }, { p = 20 } }
  local discounts = { { d = 0 }, { d = 5 } }
  local r = query [[
    from
      co = colors,
      si = sizes,
      pr = prices,
      di = discounts
    where
      pr.p - di.d > 5
    group by
      co.c
    having
      count() >= 3
    select {
      color = key,
      combos = count(),
      max_net = max(pr.p - di.d),
    }
    order by
      color
  ]]
  assertEquals(#r, 2, "108: row count")
  assertEquals(r[1].color, "blue", "108: r1.color")
  assertEquals(r[2].color, "red", "108: r2.color")
  assertEquals(r[1].combos, 9, "108: r1.combos")
  assertEquals(r[1].max_net, 20, "108: r1.max_net")
end

-- 109. Negative / error tests

-- 109a. Nil source in cross join
do
  local ok, err = pcall(function()
    local xs = { { v = 1 } }
    local r = query [[
      from
        a = xs,
        b = nonexistent
      select {
        av = a.v,
      }
    ]]
  end)
  assertTrue(not ok, "109a: expected error for nil source in cross join")
end

-- 109b. Multi-source without named bindings should error
do
  local ok, err = pcall(function()
    local xs = { { x = 1 } }
    local ys = { { y = 2 } }
    local r = query [[
      from
        a = xs,
        ys
    ]]
  end)
  assertTrue(not ok, "109b: expected error for unnamed source in multi-source from")
end

-- 110. Duplicate rows in cross join (distinct deduplication by default)

-- 110a. Duplicate select results are deduped (default distinct=true)
do
  local xs = { { v = 1 }, { v = 1 } }
  local ys = { { v = 2 } }
  local r = query [[
    from
      a = xs,
      b = ys
    select {
      av = a.v,
      bv = b.v,
    }
  ]]
  assertEquals(#r, 1, "110a: deduped to 1")
  assertEquals(r[1].av, 1, "110a: av")
end

-- 110b. Non-duplicate rows are all kept
do
  local xs = { { v = 1 }, { v = 2 } }
  local ys = { { v = 10 }, { v = 20 } }
  local r = query [[
    from
      a = xs,
      b = ys
    select {
      av = a.v,
      bv = b.v,
    }
  ]]
  assertEquals(#r, 4, "110b: all distinct kept")
end

-- 110c. Without select, raw rows with same content are deduped
do
  local xs = { { v = 1 }, { v = 1 } }
  local ys = { { v = 2 } }
  local r = query [[
    from
      a = xs,
      b = ys
  ]]
  assertEquals(#r, 1, "110c: deduped raw rows")
end

-- 111. loop using join predicate

-- 111a. loop using with inline function
do
  local xs = {
    { id = 1, name = "Alice" },
    { id = 2, name = "Bob" },
    { id = 3, name = "Carol" },
  }
  local ys = {
    { fk = 2, val = "X" },
    { fk = 1, val = "Y" },
    { fk = 3, val = "Z" },
    { fk = 1, val = "W" },
  }

  local joined = query [[
    from
      a = xs,
      b = ys loop using function(a, b)
        return a.id == b.fk
      end
    select {
      a_name = a.name,
      b_val  = b.val,
    }
    order by
      a.name, b.val
  ]]

  assertEquals(#joined, 4, "111a: row count")
  assertEquals(joined[1].a_name, "Alice")
  assertEquals(joined[1].b_val, "W")
  assertEquals(joined[2].a_name, "Alice")
  assertEquals(joined[2].b_val, "Y")
  assertEquals(joined[3].a_name, "Bob")
  assertEquals(joined[3].b_val, "X")
  assertEquals(joined[4].a_name, "Carol")
  assertEquals(joined[4].b_val, "Z")
end

-- 111b. loop using with named function
do
  local xs = {
    { id = 1, name = "Alice" },
    { id = 2, name = "Bob" },
  }
  local ys = {
    { fk = 2, val = "X" },
    { fk = 1, val = "Y" },
    { fk = 2, val = "Z" },
  }

  local function matchById(a, b)
    return a.id == b.fk
  end

  local joined = query [[
    from
      a = xs,
      b = ys loop using matchById
    select {
      a_name = a.name,
      b_val  = b.val,
    }
    order by
      a.name, b.val
  ]]

  assertEquals(#joined, 3, "111b: row count")
  assertEquals(joined[1].a_name, "Alice")
  assertEquals(joined[1].b_val, "Y")
  assertEquals(joined[2].a_name, "Bob")
  assertEquals(joined[2].b_val, "X")
  assertEquals(joined[3].a_name, "Bob")
  assertEquals(joined[3].b_val, "Z")
end

-- 111c. loop using with no matches returns empty
do
  local xs = {
    { id = 1 },
    { id = 2 },
  }
  local ys = {
    { fk = 99 },
    { fk = 100 },
  }

  local joined = query [[
    from
      a = xs,
      b = ys loop using function(a, b)
        return a.id == b.fk
      end
    select {
      x = a.id,
      y = b.fk,
    }
  ]]

  assertEquals(#joined, 0, "111c: row count")
end

-- 111d. loop using with all matches (same as cross join)
do
  local xs = {
    { v = 1 },
    { v = 2 },
  }
  local ys = {
    { v = 10 },
    { v = 20 },
  }

  local joined = query [[
    from
      a = xs,
      b = ys loop using function(a, b)
        return true
      end
    select {
      s = a.v + b.v,
    }
    order by
      a.v + b.v
  ]]

  assertEquals(#joined, 4, "111d: row count")
  assertEquals(joined[1].s, 11, "111d: first")
  assertEquals(joined[2].s, 12, "111d: second")
  assertEquals(joined[3].s, 21, "111d: third")
  assertEquals(joined[4].s, 22, "111d: fourth")
end

-- 111e. loop using with where clause (post-join filter)
do
  local xs = {
    { id = 1, name = "Alice" },
    { id = 2, name = "Bob" },
    { id = 3, name = "Carol" },
  }
  local ys = {
    { fk = 1, val = "Y" },
    { fk = 2, val = "X" },
    { fk = 1, val = "W" },
    { fk = 3, val = "Z" },
  }

  local joined = query [[
    from
      a = xs,
      b = ys loop using function(a, b)
        return a.id == b.fk
      end
    where
      a.name ~= "Bob"
    select {
      a_name = a.name,
      b_val  = b.val,
    }
    order by
      a.name, b.val
  ]]

  assertEquals(#joined, 3, "111e: row count")
  assertEquals(joined[1].a_name, "Alice")
  assertEquals(joined[1].b_val, "W")
  assertEquals(joined[2].a_name, "Alice")
  assertEquals(joined[2].b_val, "Y")
  assertEquals(joined[3].a_name, "Carol")
  assertEquals(joined[3].b_val, "Z")
end

-- 111f. loop using with group by and aggregates
do
  local depts = {
    { dept = "eng" },
    { dept = "sales" },
    { dept = "hr" },
  }
  local employees = {
    { name = "Alice", dept = "eng",   sal = 100 },
    { name = "Bob",   dept = "eng",   sal = 120 },
    { name = "Carol", dept = "sales", sal = 90 },
    { name = "Dave",  dept = "sales", sal = 80 },
    { name = "Eve",   dept = "hr",    sal = 70 },
  }

  local joined = query [[
    from
      d = depts,
      e = employees loop using function(d, e)
        return d.dept == e.dept
      end
    group by
      d.dept
    select {
      dept  = key,
      n     = count(),
      total = sum(e.sal),
    }
    order by
      dept
  ]]

  assertEquals(#joined, 3, "111f: row count")
  assertEquals(joined[1].dept, "eng")
  assertEquals(joined[1].n, 2)
  assertEquals(joined[1].total, 220)
  assertEquals(joined[2].dept, "hr")
  assertEquals(joined[2].n, 1)
  assertEquals(joined[2].total, 70)
  assertEquals(joined[3].dept, "sales")
  assertEquals(joined[3].n, 2)
  assertEquals(joined[3].total, 170)
end

-- 111g. loop using with limit and offset
do
  local xs = {
    { id = 1, name = "A" },
    { id = 2, name = "B" },
    { id = 3, name = "C" },
  }
  local ys = {
    { fk = 1, val = "p" },
    { fk = 2, val = "q" },
    { fk = 3, val = "r" },
    { fk = 1, val = "s" },
  }

  local joined = query [[
    from
      a = xs,
      b = ys loop using function(a, b)
        return a.id == b.fk
      end
    select {
      a_name = a.name,
      b_val  = b.val,
    }
    order by
      a.name, b.val
    limit
      2, 1
  ]]

  -- Full sorted result: A/p, A/s, B/q, C/r -> skip 1, take 2
  assertEquals(#joined, 2, "111g: row count")
  assertEquals(joined[1].a_name, "A")
  assertEquals(joined[1].b_val, "s")
  assertEquals(joined[2].a_name, "B")
  assertEquals(joined[2].b_val, "q")
end

-- 111h. loop using with inequality predicate
do
  local xs = {
    { v = 1 },
    { v = 2 },
    { v = 3 },
  }
  local ys = {
    { v = 2 },
    { v = 3 },
    { v = 4 },
  }

  local joined = query [[
    from
      a = xs,
      b = ys loop using function(a, b)
        return a.v < b.v
      end
    select {
      av = a.v,
      bv = b.v,
    }
    order by
      a.v, b.v
  ]]

  -- 1<2,1<3,1<4, 2<3,2<4, 3<4 -> 6 pairs
  assertEquals(#joined, 6, "111h: row count")
  assertEquals(joined[1].av, 1)
  assertEquals(joined[1].bv, 2)
  assertEquals(joined[6].av, 3)
  assertEquals(joined[6].bv, 4)
end

-- 111i. loop using on empty source
do
  local xs = {}
  local ys = {
    { fk = 1, val = "Y" },
  }

  local joined = query [[
    from
      a = xs,
      b = ys loop using function(a, b)
        return true
      end
    select {
      bv = b.val,
    }
  ]]

  assertEquals(#joined, 0, "111i: row count")
end

-- 111j. loop using produces same results as plain loop + where
do
  local xs = {
    { id = 1, name = "A" },
    { id = 2, name = "B" },
  }
  local ys = {
    { fk = 1, val = "p" },
    { fk = 2, val = "q" },
    { fk = 1, val = "r" },
  }

  local r_using = query [[
    from
      a = xs,
      b = ys loop using function(a, b)
        return a.id == b.fk
      end
    select {
      n = a.name,
      v = b.val,
    }
    order by
      a.name, b.val
  ]]

  local r_where = query [[
    from
      a = xs,
      b = ys loop
    where
      a.id == b.fk
    select {
      n = a.name,
      v = b.val,
    }
    order by
      a.name, b.val
  ]]

  assertEquals(#r_using, #r_where, "111j: same count")
  for i = 1, #r_using do
    assertEquals(r_using[i].n, r_where[i].n, "111j: name at " .. i)
    assertEquals(r_using[i].v, r_where[i].v, "111j: val at " .. i)
  end
end

-- 112. select distinct / select all

-- 112a. select distinct removes duplicate rows (explicit)
do
  local data = {
    { cat = "a", val = 1 },
    { cat = "a", val = 1 },
    { cat = "b", val = 2 },
    { cat = "a", val = 1 },
  }

  local r = query [[
    from
      p = data
    select distinct {
      cat = p.cat,
      val = p.val,
    }
    order by
      p.cat, p.val
  ]]

  assertEquals(#r, 2, "112a: row count")
  assertEquals(r[1].cat, "a")
  assertEquals(r[1].val, 1)
  assertEquals(r[2].cat, "b")
  assertEquals(r[2].val, 2)
end

-- 112b. select all keeps duplicate rows
do
  local data = {
    { cat = "a", val = 1 },
    { cat = "a", val = 1 },
    { cat = "b", val = 2 },
    { cat = "a", val = 1 },
  }

  local r = query [[
    from
      p = data
    select all {
      cat = p.cat,
      val = p.val,
    }
    order by
      p.cat, p.val
  ]]

  assertEquals(#r, 4, "112b: row count")
  assertEquals(r[1].cat, "a")
  assertEquals(r[2].cat, "a")
  assertEquals(r[3].cat, "a")
  assertEquals(r[4].cat, "b")
end

-- 112c. default select (no qualifier) deduplicates
do
  local data = {
    { v = 10 },
    { v = 10 },
    { v = 20 },
  }

  local r = query [[
    from
      p = data
    select {
      v = p.v,
    }
  ]]

  assertEquals(#r, 2, "112c: default distinct row count")
  -- Note: if default changes to all, update this to assertEquals(#r, 3)
end

-- 112d. select distinct on single scalar expression
do
  local data = {
    { v = "x" },
    { v = "y" },
    { v = "x" },
    { v = "z" },
    { v = "y" },
  }

  local r = query [[
    from
      p = data
    select distinct
      p.v
    order by
      p.v
  ]]

  assertEquals(#r, 3, "112d: row count")
  assertEquals(r[1], "x")
  assertEquals(r[2], "y")
  assertEquals(r[3], "z")
end

-- 112e. select all on single scalar expression
do
  local data = {
    { v = "x" },
    { v = "y" },
    { v = "x" },
    { v = "z" },
    { v = "y" },
  }

  local r = query [[
    from
      p = data
    select all
      p.v
    order by
      p.v
  ]]

  assertEquals(#r, 5, "112e: row count")
  assertEquals(r[1], "x")
  assertEquals(r[2], "x")
  assertEquals(r[3], "y")
  assertEquals(r[4], "y")
  assertEquals(r[5], "z")
end

-- 112f. select distinct with cross join
do
  local xs = {
    { v = 1 },
    { v = 1 },
  }
  local ys = {
    { v = 10 },
  }

  local r = query [[
    from
      a = xs,
      b = ys
    select distinct {
      av = a.v,
      bv = b.v,
    }
  ]]

  assertEquals(#r, 1, "112f: deduped cross join")
  assertEquals(r[1].av, 1)
  assertEquals(r[1].bv, 10)
end

-- 112g. select all with cross join keeps duplicates
do
  local xs = {
    { v = 1 },
    { v = 1 },
  }
  local ys = {
    { v = 10 },
  }

  local r = query [[
    from
      a = xs,
      b = ys
    select all {
      av = a.v,
      bv = b.v,
    }
  ]]

  assertEquals(#r, 2, "112g: all cross join keeps dupes")
  assertEquals(r[1].av, 1)
  assertEquals(r[2].av, 1)
end

-- 112h. select distinct with group by (group results are already unique)
do
  local data = {
    { cat = "a", val = 1 },
    { cat = "a", val = 2 },
    { cat = "b", val = 3 },
  }

  local r = query [[
    from
      p = data
    group by
      p.cat
    select distinct {
      cat = key,
      n   = count(),
    }
    order by
      cat
  ]]

  assertEquals(#r, 2, "112h: row count")
  assertEquals(r[1].cat, "a")
  assertEquals(r[1].n, 2)
  assertEquals(r[2].cat, "b")
  assertEquals(r[2].n, 1)
end

-- 112i. select all with where + order by + limit
do
  local data = {
    { name = "A", tag = "x" },
    { name = "B", tag = "x" },
    { name = "C", tag = "y" },
    { name = "D", tag = "x" },
  }

  local r = query [[
    from
      p = data
    where
      p.tag == "x"
    select all {
      tag = p.tag,
    }
    order by
      p.name
    limit
      2
  ]]

  -- All 3 matching rows have tag="x", select all keeps all, limit 2
  assertEquals(#r, 2, "112i: row count")
  assertEquals(r[1].tag, "x")
  assertEquals(r[2].tag, "x")
end

-- 112j. select distinct vs select all side by side
do
  local data = {
    { v = 1 },
    { v = 2 },
    { v = 1 },
    { v = 3 },
    { v = 2 },
    { v = 1 },
  }

  local rd = query [[
    from
      p = data
    select distinct
      p.v
    order by
      p.v
  ]]

  local ra = query [[
    from
      p = data
    select all
      p.v
    order by
      p.v
  ]]

  assertEquals(#rd, 3, "112j: distinct count")
  assertEquals(rd[1], 1)
  assertEquals(rd[2], 2)
  assertEquals(rd[3], 3)

  assertEquals(#ra, 6, "112j: all count")
  assertEquals(ra[1], 1)
  assertEquals(ra[2], 1)
  assertEquals(ra[3], 1)
  assertEquals(ra[4], 2)
  assertEquals(ra[5], 2)
  assertEquals(ra[6], 3)
end

-- 112k. select distinct on unbound access
do
  local data = {
    { tag = "x", val = 1 },
    { tag = "x", val = 1 },
    { tag = "y", val = 2 },
  }

  local r = query [[
    from
      data
    select distinct {
      tag = tag,
      val = val,
    }
    order by
      tag
  ]]

  assertEquals(#r, 2, "112k: row count")
  assertEquals(r[1].tag, "x")
  assertEquals(r[2].tag, "y")
end

-- 112l. select all on unbound access
do
  local data = {
    { tag = "x", val = 1 },
    { tag = "x", val = 1 },
    { tag = "y", val = 2 },
  }

  local r = query [[
    from
      data
    select all {
      tag = tag,
      val = val,
    }
    order by
      tag
  ]]

  assertEquals(#r, 3, "112l: row count")
  assertEquals(r[1].tag, "x")
  assertEquals(r[2].tag, "x")
  assertEquals(r[3].tag, "y")
end

-- 112m. select all with loop using (duplicates from predicate join kept)
do
  local xs = {
    { id = 1, name = "A" },
    { id = 1, name = "A" },
  }
  local ys = {
    { fk = 1, val = "p" },
  }

  local r = query [[
    from
      a = xs,
      b = ys loop using function(a, b)
        return a.id == b.fk
      end
    select all {
      n = a.name,
      v = b.val,
    }
  ]]

  assertEquals(#r, 2, "112m: all keeps predicate join dupes")
  assertEquals(r[1].n, "A")
  assertEquals(r[2].n, "A")
end

-- 112n. select distinct with loop using (duplicates from predicate join removed)
do
  local xs = {
    { id = 1, name = "A" },
    { id = 1, name = "A" },
  }
  local ys = {
    { fk = 1, val = "p" },
  }

  local r = query [[
    from
      a = xs,
      b = ys loop using function(a, b)
        return a.id == b.fk
      end
    select distinct {
      n = a.name,
      v = b.val,
    }
  ]]

  assertEquals(#r, 1, "112n: distinct removes predicate join dupes")
  assertEquals(r[1].n, "A")
  assertEquals(r[1].v, "p")
end

-- 113. loop using on non-leaf left side sees the full left row

do
  local as = {
    { aid = 1 },
    { aid = 2 },
  }
  local bs = {
    { aid = 1, bid = 10 },
    { aid = 2, bid = 20 },
  }
  local cs = {
    { bid = 10, val = "x" },
    { bid = 20, val = "y" },
  }

  local r = query [[
    from
      a = as,
      b = bs loop using function(a, b)
        return a.aid == b.aid
      end,
      c = cs loop using function(left, c)
        return left.b.bid == c.bid
      end
    select {
      aid = a.aid,
      bid = b.bid,
      val = c.val,
    }
    order by
      aid
  ]]

  assertEquals(#r, 2)
  assertEquals(r[1].aid, 1)
  assertEquals(r[1].bid, 10)
  assertEquals(r[1].val, "x")
  assertEquals(r[2].aid, 2)
  assertEquals(r[2].bid, 20)
  assertEquals(r[2].val, "y")
end

-- 114. loop using on non-leaf left side can inspect multiple prior bindings

do
  local users = {
    { uid = 1, org = "eng" },
    { uid = 2, org = "sales" },
  }
  local memberships = {
    { uid = 1, team = "compiler" },
    { uid = 2, team = "field" },
  }
  local permissions = {
    { org = "eng", team = "compiler", perm = "write" },
    { org = "sales", team = "field", perm = "read" },
    { org = "eng", team = "field", perm = "deny" },
  }

  local r = query [[
    from
      u = users,
      m = memberships loop using function(u, m)
        return u.uid == m.uid
      end,
      p = permissions loop using function(left, p)
        return left.u.org == p.org and left.m.team == p.team
      end
    select {
      uid = u.uid,
      team = m.team,
      perm = p.perm,
    }
    order by
      uid
  ]]

  assertEquals(#r, 2)
  assertEquals(r[1].uid, 1)
  assertEquals(r[1].team, "compiler")
  assertEquals(r[1].perm, "write")
  assertEquals(r[2].uid, 2)
  assertEquals(r[2].team, "field")
  assertEquals(r[2].perm, "read")
end

-- 115. loop using preserves existing two-source semantics (left arg is source item)

do
  local xs = {
    { id = 1, name = "A" },
    { id = 2, name = "B" },
  }
  local ys = {
    { fk = 1, val = "p" },
    { fk = 2, val = "q" },
    { fk = 1, val = "r" },
  }

  local r = query [[
    from
      a = xs,
      b = ys loop using function(a, b)
        return a.id == b.fk
      end
    select {
      n = a.name,
      v = b.val,
    }
    order by
      a.name, b.val
  ]]

  assertEquals(#r, 3)
  assertEquals(r[1].n, "A")
  assertEquals(r[1].v, "p")
  assertEquals(r[2].n, "A")
  assertEquals(r[2].v, "r")
  assertEquals(r[3].n, "B")
  assertEquals(r[3].v, "q")
end

-- 116. explicit single-source pushdown remains correct

do
  local xs = {
    { size = 5, name = "a" },
    { size = 20, name = "b" },
    { size = 30, name = "c" },
  }
  local ys = {
    { tag = "x" },
    { tag = "y" },
  }

  local r = query [[
    from
      x = xs,
      y = ys
    where
      x.size > 10
    select {
      pair = x.name .. ":" .. y.tag,
    }
    order by
      x.name, y.tag
  ]]

  assertEquals(#r, 4)
  assertEquals(r[1].pair, "b:x")
  assertEquals(r[2].pair, "b:y")
  assertEquals(r[3].pair, "c:x")
  assertEquals(r[4].pair, "c:y")
end

-- 117. unqualified field references are not pushed down

do
  local size = 10
  local xs = {
    { size = 5, name = "a" },
    { size = 20, name = "b" },
  }
  local ys = {
    { tag = "x" },
  }

  local r = query [[
    from
      x = xs,
      y = ys
    where
      size > 10
    select {
      name = x.name,
    }
  ]]

  -- In multi-source queries, bare `size` is not an explicitly single-source
  -- reference, so it must not be pushed into x. It resolves from outer env.
  assertEquals(#r, 0)
end

-- 118. equi join with boolean keys works through hash join

do
  local xs = {
    { ok = true,  name = "a" },
    { ok = false, name = "b" },
    { ok = true,  name = "c" },
  }
  local ys = {
    { ok = true,  code = "T" },
    { ok = false, code = "F" },
  }

  local r = query [[
    from
      x = xs,
      y = ys hash
    where
      x.ok == y.ok
    select {
      pair = x.name .. ":" .. y.code,
    }
    order by
      x.name, y.code
  ]]

  assertEquals(#r, 3)
  assertEquals(r[1].pair, "a:T")
  assertEquals(r[2].pair, "b:F")
  assertEquals(r[3].pair, "c:T")
end

-- 119. hash join ignores nil join keys

do
  local xs = {
    { id = 1, name = "a" },
    { name = "b" },
    { id = 2, name = "c" },
  }
  local ys = {
    { fk = 1, val = "x" },
    { fk = 2, val = "y" },
    { val = "z" },
  }

  local r = query [[
    from
      x = xs,
      y = ys hash
    where
      x.id == y.fk
    select {
      pair = x.name .. ":" .. y.val,
    }
    order by
      x.name, y.val
  ]]

  assertEquals(#r, 2)
  assertEquals(r[1].pair, "a:x")
  assertEquals(r[2].pair, "c:y")
end

-- 120. loop using still supports inequality joins

do
  local xs = {
    { v = 1 },
    { v = 2 },
    { v = 3 },
  }
  local ys = {
    { v = 2 },
    { v = 3 },
    { v = 4 },
  }

  local joined = query [[
    from
      a = xs,
      b = ys loop using function(a, b)
        return a.v < b.v
      end
    select {
      pair = a.v .. ":" .. b.v,
    }
    order by
      a.v, b.v
  ]]

  assertEquals(#joined, 6)
  assertEquals(joined[1].pair, "1:2")
  assertEquals(joined[2].pair, "1:3")
  assertEquals(joined[3].pair, "1:4")
  assertEquals(joined[4].pair, "2:3")
  assertEquals(joined[5].pair, "2:4")
  assertEquals(joined[6].pair, "3:4")
end

-- 121. explicit single-source filter plus join predicate still works

do
  local users = {
    { uid = 1, active = true },
    { uid = 2, active = false },
    { uid = 3, active = true },
  }
  local events = {
    { uid = 1, kind = "a" },
    { uid = 1, kind = "b" },
    { uid = 2, kind = "c" },
    { uid = 3, kind = "d" },
  }

  local r = query [[
    from
      u = users,
      e = events loop using function(u, e)
        return u.uid == e.uid
      end
    where
      u.active
    select {
      pair = u.uid .. ":" .. e.kind,
    }
    order by
      u.uid, e.kind
  ]]

  assertEquals(#r, 3)
  assertEquals(r[1].pair, "1:a")
  assertEquals(r[2].pair, "1:b")
  assertEquals(r[3].pair, "3:d")
end

-- 122. conservative pushdown does not change outer-variable semantics

do
  local threshold = 15
  local xs = {
    { v = 10, name = "a" },
    { v = 20, name = "b" },
    { v = 30, name = "c" },
  }
  local ys = {
    { tag = "x" },
  }

  local r = query [[
    from
      x = xs,
      y = ys
    where
      x.v > threshold
    select {
      name = x.name,
    }
    order by
      x.name
  ]]

  assertEquals(#r, 2)
  assertEquals(r[1].name, "b")
  assertEquals(r[2].name, "c")
end

-- 123. full-row left argument on deeper join can still project original aliases

do
  local arows = {
    { id = 1, name = "A" },
  }
  local brows = {
    { aid = 1, bid = 100 },
  }
  local crows = {
    { bid = 100, label = "ok" },
  }

  local r = query [[
    from
      a = arows,
      b = brows loop using function(a, b)
        return a.id == b.aid
      end,
      c = crows loop using function(left, c)
        return left.a.id == 1 and left.b.bid == c.bid
      end
    select {
      pair = a.name .. ":" .. c.label,
    }
  ]]

  assertEquals(#r, 1)
  assertEquals(r[1].pair, "A:ok")
end

-- 124. missing named predicate reference errors cleanly

do
  local ok, err = pcall(function()
    local xs = {
      { id = 1 },
    }
    local ys = {
      { fk = 1 },
    }

    local _r = query [[
      from
        a = xs,
        b = ys loop using missingPredicate
      select {
        id = a.id,
      }
    ]]
  end)

  assertEquals(ok, false)
  assertTrue(
    string.find(tostring(err), "Join predicate 'missingPredicate' is not defined") ~= nil,
    "expected missing predicate error, got: " .. tostring(err)
  )
end

-- 125. semi join with equi predicate keeps matching left rows only

do
  local xs = {
    { id = 1, name = "a" },
    { id = 2, name = "b" },
    { id = 3, name = "c" },
  }
  local ys = {
    { fk = 2 },
    { fk = 3 },
  }

  local r = query [[
    from
      x = xs,
      y = ys semi hash
    where
      x.id == y.fk
    select {
      name = x.name,
    }
    order by
      x.name
  ]]

  assertEquals(#r, 2)
  assertEquals(r[1].name, "b")
  assertEquals(r[2].name, "c")
end

-- 126. anti join with equi predicate keeps non-matching left rows only

do
  local xs = {
    { id = 1, name = "a" },
    { id = 2, name = "b" },
    { id = 3, name = "c" },
  }
  local ys = {
    { fk = 2 },
    { fk = 3 },
  }

  local r = query [[
    from
      x = xs,
      y = ys anti hash
    where
      x.id == y.fk
    select {
      name = x.name,
    }
    order by
      x.name
  ]]

  assertEquals(#r, 1)
  assertEquals(r[1].name, "a")
end

-- 127. semi join with loop using predicate works

do
  local xs = {
    { id = 1, name = "a" },
    { id = 2, name = "b" },
    { id = 3, name = "c" },
  }
  local ys = {
    { fk = 2 },
    { fk = 3 },
  }

  local r = query [[
    from
      x = xs,
      y = ys semi loop using function(x, y)
        return x.id == y.fk
      end
    select {
      name = x.name,
    }
    order by
      x.name
  ]]

  assertEquals(#r, 2)
  assertEquals(r[1].name, "b")
  assertEquals(r[2].name, "c")
end

-- 127b. explain shows semi loop join type

do
  local xs = {
    { id = 1, name = "a" },
    { id = 2, name = "b" },
    { id = 3, name = "c" },
  }
  local ys = {
    { fk = 2 },
    { fk = 3 },
  }

  local plan = query [[
    explain
    from
      x = xs,
      y = ys semi loop using function(x, y)
        return x.id == y.fk
      end
    select {
      name = x.name,
    }
  ]]

  assertTrue(
    string.find(plan, "Semi") ~= nil,
    "expected explain output to mention semi join, got: " .. tostring(plan)
  )
end

-- 127c. explain shows anti loop join type

do
  local xs = {
    { id = 1, name = "a" },
    { id = 2, name = "b" },
    { id = 3, name = "c" },
  }
  local ys = {
    { fk = 2 },
    { fk = 3 },
  }

  local plan = query [[
    explain
    from
      x = xs,
      y = ys anti loop using function(x, y)
        return x.id == y.fk
      end
    select {
      name = x.name,
    }
  ]]

  assertTrue(
    string.find(plan, "Anti") ~= nil,
    "expected explain output to mention anti join, got: " .. tostring(plan)
  )
end

-- 128. anti join with loop using predicate works

do
  local xs = {
    { id = 1, name = "a" },
    { id = 2, name = "b" },
    { id = 3, name = "c" },
  }
  local ys = {
    { fk = 2 },
    { fk = 3 },
  }

  local r = query [[
    from
      x = xs,
      y = ys anti loop using function(x, y)
        return x.id == y.fk
      end
    select {
      name = x.name,
    }
    order by
      x.name
  ]]

  assertEquals(#r, 1)
  assertEquals(r[1].name, "a")
end

-- 129. semi join preserves left multiplicity only

do
  local xs = {
    { id = 1, name = "a1" },
    { id = 1, name = "a2" },
    { id = 2, name = "b" },
  }
  local ys = {
    { fk = 1 },
    { fk = 1 },
  }

  local r = query [[
    from
      x = xs,
      y = ys semi hash
    where
      x.id == y.fk
    select all {
      name = x.name,
    }
    order by
      x.name
  ]]

  assertEquals(#r, 2)
  assertEquals(r[1].name, "a1")
  assertEquals(r[2].name, "a2")
end

-- 130. anti join preserves unmatched left multiplicity only

do
  local xs = {
    { id = 1, name = "a1" },
    { id = 1, name = "a2" },
    { id = 2, name = "b1" },
    { id = 2, name = "b2" },
  }
  local ys = {
    { fk = 1 },
  }

  local r = query [[
    from
      x = xs,
      y = ys anti hash
    where
      x.id == y.fk
    select all {
      name = x.name,
    }
    order by
      x.name
  ]]

  assertEquals(#r, 2)
  assertEquals(r[1].name, "b1")
  assertEquals(r[2].name, "b2")
end

-- 131. semi join with non-leaf left side can inspect full left row

do
  local users = {
    { uid = 1, org = "eng" },
    { uid = 2, org = "sales" },
  }
  local memberships = {
    { uid = 1, team = "compiler" },
    { uid = 2, team = "field" },
  }
  local permissions = {
    { org = "eng", team = "compiler" },
    { org = "eng", team = "field" },
  }

  local r = query [[
    from
      u = users,
      m = memberships loop using function(u, m)
        return u.uid == m.uid
      end,
      p = permissions semi loop using function(left, p)
        return left.u.org == p.org and left.m.team == p.team
      end
    select {
      pair = u.uid .. ":" .. m.team,
    }
    order by
      u.uid, m.team
  ]]

  assertEquals(#r, 1)
  assertEquals(r[1].pair, "1:compiler")
end

-- 132. anti join with non-leaf left side can inspect full left row

do
  local users = {
    { uid = 1, org = "eng" },
    { uid = 2, org = "sales" },
  }
  local memberships = {
    { uid = 1, team = "compiler" },
    { uid = 2, team = "field" },
  }
  local permissions = {
    { org = "eng", team = "compiler" },
  }

  local r = query [[
    from
      u = users,
      m = memberships loop using function(u, m)
        return u.uid == m.uid
      end,
      p = permissions anti loop using function(left, p)
        return left.u.org == p.org and left.m.team == p.team
      end
    select {
      pair = u.uid .. ":" .. m.team,
    }
    order by
      u.uid, m.team
  ]]

  assertEquals(#r, 1)
  assertEquals(r[1].pair, "2:field")
end

-- 133. semi join without predicate errors

do
  local ok, err = pcall(function()
    local xs = {
      { id = 1 },
      { id = 2 },
    }
    local ys = {
      { fk = 2 },
    }

    local _r = query [[
      from
        x = xs,
        y = ys semi loop
      select {
        id = x.id,
      }
    ]]
  end)

  assertEquals(ok, false)
  assertTrue(
    string.find(tostring(err), "semi loop join requires using predicate") ~= nil,
    "expected semi loop predicate error, got: " .. tostring(err)
  )
end

-- 134. anti join without predicate errors

do
  local ok, err = pcall(function()
    local xs = {
      { id = 1 },
      { id = 2 },
    }
    local ys = {
      { fk = 2 },
    }

    local _r = query [[
      from
        x = xs,
        y = ys anti hash
      select {
        id = x.id,
      }
    ]]
  end)

  assertEquals(ok, false)
  assertTrue(
    string.find(tostring(err), "anti loop join requires using predicate") ~= nil,
    "expected anti join predicate error, got: " .. tostring(err)
  )
end

-- 135. hinted hash join keeps correct equi-predicate orientation

do
  local xs = {
    { ok = true,  name = "a" },
    { ok = false, name = "b" },
    { ok = true,  name = "c" },
  }
  local ys = {
    { ok = true,  code = "T" },
    { ok = false, code = "F" },
  }

  local r = query [[
    from
      x = xs,
      y = ys hash
    where
      x.ok == y.ok
    select {
      pair = x.name .. ":" .. y.code,
    }
    order by
      x.name, y.code
  ]]

  assertEquals(#r, 3)
  assertEquals(r[1].pair, "a:T")
  assertEquals(r[2].pair, "b:F")
  assertEquals(r[3].pair, "c:T")
end

-- 136. width-sensitive planning prefers narrower source before wider source

do
  local base = {}
  local small = {}
  local wide = {}

  for i = 1, 50 do
    base[i] = { id = i }

    small[i] = {
      id = i,
      k = i,
    }

    wide[i] = {
      id = i,
      k = i,
      a1 = "x", a2 = "x", a3 = "x", a4 = "x", a5 = "x",
      a6 = "x", a7 = "x", a8 = "x", a9 = "x", a10 = "x",
      a11 = "x", a12 = "x", a13 = "x", a14 = "x", a15 = "x",
    }
  end

  local plan = query [[
    explain
    from
      b = base,
      w = wide,
      s = small
    where
      b.id == w.id and b.id == s.id
    select {
      id = b.id
    }
  ]]

  local sPos = string.find(plan, "Scan on s")
  local wPos = string.find(plan, "Scan on w")

  assert(sPos ~= nil)
  assert(wPos ~= nil)
  assert(sPos < wPos)
end

-- 137. GroupAggregate estimate uses NDV for single grouping key

do
  local rows = {}

  for i = 1, 10 do
    for j = 1, 3 do
      rows[#rows + 1] = {
        name = "page-" .. i,
        v = j,
      }
    end
  end

  local plan = query [[
    explain (costs)
    from
      r = rows
    group by
      r.name
    select {
      key = r.name,
      c = count(r.v),
    }
  ]]

  assertTrue(
    string.find(plan, "Group Aggregate", 1, true) ~= nil,
    "136: expected Group Aggregate in plan, got: " .. tostring(plan)
  )
  assertTrue(
    string.find(plan, "rows=10", 1, true) ~= nil,
    "136: expected rows=10 in plan, got: " .. tostring(plan)
  )
end

-- 138. GroupAggregate falls back when grouping expression is not a simple source column

do
  local rows = {}

  for i = 1, 10 do
    for j = 1, 3 do
      rows[#rows + 1] = {
        name = "page-" .. i,
        v = j,
      }
    end
  end

  local plan = query [[
    explain (costs)
    from
      r = rows
    group by
      r.name .. ""
    select {
      key = r.name .. "",
      c = count(r.v),
    }
  ]]

  assertTrue(
    string.find(plan, "Group Aggregate", 1, true) ~= nil,
    "137: expected GroupAggregate in plan, got: " .. tostring(plan)
  )
  -- 30 input rows -> fallback heuristic 15
  assertTrue(
    string.find(plan, "rows=15", 1, true) ~= nil,
    "137: expected fallback rows=15 in plan, got: " .. tostring(plan)
  )
end

-- 139. GroupAggregate NDV estimate for multiple keys is capped by input rows

do
  local rows = {}

  for i = 1, 10 do
    for j = 1, 3 do
      rows[#rows + 1] = {
        a = "a-" .. i,
        b = "b-" .. j,
        v = i * j,
      }
    end
  end

  local plan = query [[
    explain (costs)
    from
      r = rows
    group by
      r.a, r.b
    select {
      a = r.a,
      b = r.b,
      c = count(r.v),
    }
  ]]

  assertTrue(
    string.find(plan, "Group Aggregate", 1, true) ~= nil,
    "138: expected GroupAggregate in plan, got: " .. tostring(plan)
  )
  -- NDV(a)=10, NDV(b)=3, product=30, capped by input rows=30
  assertTrue(
    string.find(plan, "rows=30", 1, true) ~= nil,
    "138: expected capped rows=30 in plan, got: " .. tostring(plan)
  )
end

-- 140. Multi-source inner equi join applies join predicate

do
  local depts = {
    { dept = "eng" },
    { dept = "sales" },
  }
  local employees = {
    { name = "Alice", dept = "eng" },
    { name = "Bob",   dept = "eng" },
    { name = "Carol", dept = "sales" },
  }
  local r = query [[
    from
      d = depts,
      e = employees
    where
      d.dept == e.dept
    select all {
      dept = d.dept,
      name = e.name,
    }
    order by
      dept, name
  ]]

  assertEquals(#r, 3)
  assertEquals(r[1].dept, "eng")
  assertEquals(r[1].name, "Alice")
  assertEquals(r[2].dept, "eng")
  assertEquals(r[2].name, "Bob")
  assertEquals(r[3].dept, "sales")
  assertEquals(r[3].name, "Carol")
end

-- 141. Multi-source group by + count uses filtered join rows

do
  local depts = {
    { dept = "eng" },
    { dept = "sales" },
  }
  local employees = {
    { name = "Alice", dept = "eng" },
    { name = "Bob",   dept = "eng" },
    { name = "Carol", dept = "sales" },
  }
  local r = query [[
    from
      d = depts,
      e = employees
    where
      d.dept == e.dept
    group by
      d.dept
    select {
      dept = key,
      n = count(),
    }
    order by
      dept
  ]]

  assertEquals(#r, 2)
  assertEquals(r[1].dept, "eng")
  assertEquals(r[1].n, 2)
  assertEquals(r[2].dept, "sales")
  assertEquals(r[2].n, 1)
end

-- 142. Multi-source group by + sum uses filtered join rows

do
  local categories = {
    { cat = "fruit" },
    { cat = "veg" },
  }
  local items = {
    { name = "apple",  cat = "fruit", price = 3 },
    { name = "pear",   cat = "fruit", price = 2 },
    { name = "carrot", cat = "veg",   price = 1 },
    { name = "beet",   cat = "veg",   price = 4 },
  }
  local r = query [[
    from
      c = categories,
      i = items
    where
      c.cat == i.cat
    group by
      c.cat
    select {
      cat = key,
      total = sum(i.price),
    }
    order by
      cat
  ]]

  assertEquals(#r, 2)
  assertEquals(r[1].cat, "fruit")
  assertEquals(r[1].total, 5)
  assertEquals(r[2].cat, "veg")
  assertEquals(r[2].total, 5)
end

-- 143. Semi join with equi predicate keeps matching left rows only

do
  local xs = {
    { id = 1, name = "a" },
    { id = 2, name = "b" },
    { id = 3, name = "c" },
  }
  local ys = {
    { fk = 2 },
    { fk = 3 },
  }

  local r = query [[
    from
      x = xs,
      y = ys semi hash
    where
      x.id == y.fk
    select {
      name = x.name,
    }
    order by
      x.name
  ]]

  assertEquals(#r, 2)
  assertEquals(r[1].name, "b")
  assertEquals(r[2].name, "c")
end

-- 144. Anti join with equi predicate keeps non-matching left rows only

do
  local xs = {
    { id = 1, name = "a" },
    { id = 2, name = "b" },
    { id = 3, name = "c" },
  }
  local ys = {
    { fk = 2 },
    { fk = 3 },
  }

  local r = query [[
    from
      x = xs,
      y = ys anti hash
    where
      x.id == y.fk
    select {
      name = x.name,
    }
  ]]

  assertEquals(#r, 1)
  assertEquals(r[1].name, "a")
end

-- 103. Single-source explain works

do
  local plan = query [[
    explain
    from
      p = pages
    select {
      name = p.name,
    }
  ]]

  plan = tostring(plan)

  assertTrue(
    string.find(plan, "Scan on p", 1, true) ~= nil,
    "103: expected single-source explain scan, got: " .. tostring(plan)
  )
end

-- 145. Explain works for from {}

do
  local plan = query [[
    explain (costs)
    from
      {}
    select
      _
  ]]

  plan = tostring(plan)

  assertTrue(
    string.find(plan, "Scan on _", 1, true) ~= nil,
    "104: expected scan on _, got: " .. tostring(plan)
  )
  assertTrue(
    string.find(plan, "rows=", 1, true) ~= nil,
    "104: expected row estimate, got: " .. tostring(plan)
  )
end

-- 146. Distinct explain analyze works

do
  local plan = query [[
    explain analyze (costs, timing)
    from
      p = pages
    where
      p.tags[1] ~= nil
    select {
      tag = p.tags[1],
    }
  ]]

  plan = tostring(plan)

  assertTrue(
    string.find(plan, "Unique", 1, true) ~= nil,
    "105: expected Unique node, got: " .. tostring(plan)
  )
  assertTrue(
    string.find(plan, "actual", 1, true) ~= nil,
    "105: expected analyze actuals, got: " .. tostring(plan)
  )
  assertTrue(
    string.find(plan, "Execution Time:", 1, true) ~= nil,
    "105: expected execution time, got: " .. tostring(plan)
  )
end

-- 147. materialized sources

do
  local r = query [[
    from
      materialized 1
    select all
      _
  ]]
  assertEquals(#r, 1)
  assertEquals(r[1], 1)
end

do
  local r = query [[
    from
      materialized {{a = 1}, {a = 2}, {a = 3}}
    select all
      _.a
    order by
      _.a
  ]]
  assertEquals(#r, 3)
  assertEquals(r[1], 1)
  assertEquals(r[2], 2)
  assertEquals(r[3], 3)
end

do
  local rows = {
    { id = 2, name = "b" },
    { id = 1, name = "a" },
    { id = 3, name = "c" },
  }

  local r = query [[
    from
      materialized rows
    select all
      _.name
    order by
      _.id
  ]]
  assertEquals(#r, 3)
  assertEquals(r[1], "a")
  assertEquals(r[2], "b")
  assertEquals(r[3], "c")
end

do
  local xs = {
    { id = 1, name = "a" },
    { id = 2, name = "b" },
    { id = 3, name = "c" },
  }
  local ys = {
    { fk = 2, val = "x" },
    { fk = 3, val = "y" },
  }

  local r = query [[
    from
      materialized x = xs,
      y = ys hash
    where
      x.id == y.fk
    select all {
      name = x.name,
      val = y.val,
    }
    order by
      x.name
  ]]
  assertEquals(#r, 2)
  assertEquals(r[1].name, "b")
  assertEquals(r[1].val, "x")
  assertEquals(r[2].name, "c")
  assertEquals(r[2].val, "y")
end

-- 148. with rows/width/cost hints appear in explain

do
  local plan = query [[
    explain verbose hints
    from
      p = pages with rows 7 width 3 cost 11
    select {
      name = p.name,
    }
  ]]

  plan = tostring(plan)

  assertTrue(
    string.find(plan, "Scan on p", 1, true) ~= nil,
    "148: expected scan on p, got: " .. tostring(plan)
  )
  assertTrue(
    string.find(plan, "Hints: rows=7, width=3, cost=11", 1, true) ~= nil,
    "148: expected hinted source metadata, got: " .. tostring(plan)
  )
  assertTrue(
    string.find(plan, "Stats: computed-exact-small", 1, true) ~= nil,
    "148: expected hinted stats source, got: " .. tostring(plan)
  )
end

-- 149. materialized + with hints both appear in explain

do
  local plan = query [[
    explain verbose hints
    from
      materialized p = pages with rows 5 width 2 cost 13
    select {
      name = p.name,
    }
  ]]

  plan = tostring(plan)

  assertTrue(
    string.find(plan, "Hints: materialized, rows=5, width=2, cost=13", 1, true) ~= nil,
    "149: expected materialized+hints metadata, got: " .. tostring(plan)
  )
  assertTrue(
    string.find(plan, "Stats: computed-exact-small", 1, true) ~= nil,
    "149: expected hinted stats source, got: " .. tostring(plan)
  )
end

-- 150. later with-hint entries override earlier ones

do
  local plan = query [[
    explain verbose hints
    from
      p = pages with rows 7 rows 9 width 3 width 4 cost 11 cost 12
    select {
      name = p.name,
    }
  ]]

  plan = tostring(plan)

  assertTrue(
    string.find(plan, "Hints: rows=9, width=4, cost=12", 1, true) ~= nil,
    "150: expected last with-hints to win, got: " .. tostring(plan)
  )
end
