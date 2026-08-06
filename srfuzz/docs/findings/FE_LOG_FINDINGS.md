# FE-log findings — 2026-08-01

Defects mined from the FE's own `fe.warn.log` / `fe.log` on the fuzz cluster, not from anything the
client printed.

**This is a whole oracle the harness was not reading.** `clusterfuzz.sh` has only ever looked at what
`mysql` returned. The FE logs exceptions with full stack traces that never reach the client — a rule
that threw and was caught, an NPE swallowed by a fallback, a deparse that produced nonsense — and
none of it was being collected. Everything below was already on disk while the harness reported those
rounds as clean.

Nothing here is minimized. Counts are from the current logs plus rotations still on the box, so they
measure recurrence within this run, not severity.

---

## F1 — deparser emits an empty table qualifier: `` ``.fn(...) ``

**Counts:** 392 in `fe.log.20260730-1`, 18 current, plus rotations. Recurring across days.

```sql
Exception, sql: SELECT DISTINCT
  bitmap_to_string(``.bitmap_agg(`srfuzz_mut_40`.`t1`.`c2`)) AS `bitmap_to_string(bitmap_union(to_bitmap(c2)))`,
  bitmap_to_string(``.bitmap_agg(`srfuzz_mut_40`.`t1`.`c3`)) AS `bitmap_to_string(bitmap_union(to_bitmap(c3)))`,
  bitmap_to_string(bitmap_agg(`srfuzz_mut_40`.`t1`.`c4`))    AS `bitmap_to_string(bitmap_agg(c4))`
FROM `srfuzz_mut_40`.`t1` GROUP BY ... ORDER BY ...
```

Read the three select items together — they are the evidence:

- items 1 and 2 were written `bitmap_union(to_bitmap(cN))` (the alias preserves the original text) and
  were **rewritten** by the optimizer into `bitmap_agg(cN)`. Both come out with an empty qualifier.
- item 3 was written `bitmap_agg(c4)` directly, was **not** rewritten, and has no qualifier.

So the empty qualifier tracks the rewrite, not the function. Something in the rewrite produces a
`TableName` that is present but blank, and the deparser renders present-but-blank as ``` ``. ``` rather
than omitting it. `` ``.bitmap_agg(...) `` does not parse, so any consumer of this SQL — a view
definition, a query dump, an audit log replayed later — gets something it cannot read back.

Same family as the star-expansion defect already fixed in `bugfix/star-expand-ambiguous-ref`: the
deparser producing text that will not survive a round trip.

## F2 — plan type check fails on `array_agg`: declared type disagrees with actual

**Counts:** ~330 across several distinct arg ids.

```
StarRocksPlannerException: Invalid plan: Type check failed. the type of arg 19: array_agg in expr
  '19: array_agg' is defined as ARRAY<INT>, but the actual type is ARRAY<VARCHAR(255)>

... the type of arg 16: array_agg ... is defined as ARRAY<TIME>, but the actual type is ARRAY<DATETIME>
```

The planner's own validator rejects the plan: the column ref for an `array_agg` result carries one
type while the expression producing it has another. Two distinct shapes are visible — `ARRAY<INT>` vs
`ARRAY<VARCHAR(255)>`, and `ARRAY<TIME>` vs `ARRAY<DATETIME>` — so this is not one accidental
mismatch. Type propagation through `array_agg` is losing or overwriting the element type.

Note this surfaces through the same `Invalid plan:` message whose *formatting* was fixed in
`8f0fad3ab23`. The formatting fix is why the reason is legible here at all; the underlying mismatch
is untouched and is its own defect.

## F3 — NPE in the statistics calculator during join reorder

**Counts:** 82 at the innermost frame; 72 via `visitLogicalProject`, 24 via `visitLogicalTopN`,
12 each via `visitLogicalLimit` and `visitLogicalAnalytic`.

```
java.lang.NullPointerException: Cannot invoke
  "com.starrocks.sql.optimizer.statistics.Statistics.getColumnStatistics()"
  because "inputStatistics" is null
    at StatisticsCalculator.computeTopNNode(StatisticsCalculator.java:2033)
    at StatisticsCalculator.visitLogicalTopN(StatisticsCalculator.java:2001)
    at LogicalTopNOperator.accept(LogicalTopNOperator.java:219)
    at StatisticsCalculator.estimatorStats(StatisticsCalculator.java:251)
    at Utils.calculateStatistics(Utils.java:937)
    at QueryOptimizer.logicalJoinReorder(QueryOptimizer.java:916)
    at QueryOptimizer.pushDownAggregation(QueryOptimizer.java:866)
    at QueryOptimizer.logicalRuleRewrite(QueryOptimizer.java:706)
```

`inputStatistics` is null and four different node visitors dereference it without checking. The
spread across node types points at one producer returning null rather than four independent bugs —
`Statistics.getColumnStatistic(Statistics.java:145)` is the most frequent innermost frame and is the
place to start.

Reached through `pushDownAggregation` → `logicalJoinReorder`, so it needs a join and an aggregate,
which is why ordinary queries do not hit it.

## F4 — `only found column statistics: {...}, but missing statistic of col: N`

**Counts:** ~730 across variants — the single largest class in the logs.

```
StarRocksPlannerException: only found column statistics: {12: count}, but missing statistic of col: 3: c2.
StarRocksPlannerException: only found column statistics: {2: c1, 3: c2, 20: GROUPING_ID},
                           but missing statistic of col: 14: expr.
```

The optimizer computed statistics for some columns of a node and then demanded one it had not
computed. The second variant is the more suggestive: `14: expr` and `20: GROUPING_ID` are
optimizer-synthesised columns, so the gap is likely between what a rewrite introduces and what the
statistics pass knows to compute for it.

Whether each instance is a defect or an expected bail-out needs checking — this is the one class here
I would not assert on. But at ~730 occurrences it is either a real hole or a very loud non-event, and
either answer is worth having.

---

## What this says about the harness

The cluster harness classifies rounds by what `mysql` printed. All four classes above were invisible
to it: F1 and F2 were caught-and-logged, F3 is an NPE the optimizer recovers from, F4 aborts one
plan among many. Rounds containing them were recorded as `errors=0`.

**Reading the FE log should be a first-class oracle**, on the same footing as the BE fatal counter:
snapshot `fe.warn.log` size before each round, diff after, extract exception classes and their throw
sites, dedupe by signature exactly as the BE crashes are. That is a small change to `clusterfuzz.sh`
and it roughly doubles what a round can observe.

Credit where due: I did not find this. It was spotted by reading `fe.warn.log` directly and noticing
`` ``.bitmap_agg( `` in a logged statement.
