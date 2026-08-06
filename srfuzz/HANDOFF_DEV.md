# Handoff: fuzzer development

For the session that changes the fuzzer itself. The session that *runs* it and triages findings reads
`HANDOFF_OPS.md` instead.

## Where the code is

- Source of truth: `/home/public/sr-fuzzer`, branch `fuzz/ast-mutation`. Nothing here is pushed.
- The fuzzer is a JUnit test: `fe/fe-core/src/test/java/com/starrocks/fuzz/AstMutationFuzzerTest.java`,
  plus one class per operator in the same package.
- Runner for concurrent soaking: `srfuzz/soak/parallel_soak.sh`.

## Build and test go in a dev container, never locally and never on a bare host

This is the single biggest time sink discovered so far. Current `main` needs **thrift 0.23**; the local
box and the bare dev1 host both have **0.22**, so BE test binaries cannot be linked and FE builds fail
in `fe-grammar`. Three subagents each lost most of their run rediscovering this, one built a private
fmt overlay to get around it, and another's `pkill` killed a third's build.

Working container pattern (dev1 = `ssh fha@39.99.136.234`):

```
git -C /home/disk5/fha/DorisDB worktree add /home/disk1/fha/sr-ws/<name> -b <branch> starrocks-2/main
docker run -d --name sr-dev-<name> --network host --privileged \
  -v /home/disk5/fha/DorisDB/.git:/home/disk5/fha/DorisDB/.git \
  -v /home/disk1/fha/sr-ws/<name>:/home/disk1/fha/sr-ws/<name> \
  -v <any output dir you write to>:<same path> \
  -v /home/disk5/fha/.m2:/root/.m2 -v /home/disk5/fha/mold:/opt/mold \
  172.26.92.142:5000/starrocks/dev-env-ubuntu:latest sleep infinity
```

Inside it you will need `git config --global --add safe.directory <path>`, and for BE test binaries
`export LD_LIBRARY_PATH=/var/local/thirdparty/installed/open_jdk/lib/server:$LD_LIBRARY_PATH`.

**Mount every directory you write to.** A run whose output directory was not mounted wrote into the
container's own filesystem, so the host saw an empty log and it looked like the job never started.

## What the mutator does

Per mutant: parse a corpus seed, apply 1..N edits, analyze, deparse, reparse, analyze again. Each step
that fails is an outcome; the round-trip is the oracle. Operators:

| id | what it does |
| --- | --- |
| M1-M4 | expression-level: subtree swap, function swap, literal boundary, identifier rebind |
| M5-clause | ORDER BY / LIMIT / DISTINCT |
| M6-nesting | wrap in subquery, add join, table function |
| M7-typestress | complex-type accessors, conditionals |
| M9 | session-flag perturbation on 15% of mutants |
| M10-splice | **combines two seeds** — CROSS JOIN a derived table, or EXISTS in WHERE |
| M11-predicate | **the only operator that produces a condition** — conjoins/disjoins/negates WHERE, adds HAVING, adds an ON conjunct |

Knobs: `-Dsrfuzz.mutations` (per seed, default 10, soak uses 40), `-Dsrfuzz.chain` (max stacked edits,
default 4, geometric with 35% continuation), `-Dsrfuzz.shards` / `-Dsrfuzz.shard`, `-Dsrfuzz.corpus`,
`-Dsrfuzz.seed`, `-Dsrfuzz.report`, `-Dsrfuzz.emit`, `-Dsrfuzz.maxFiles`.

## Measured facts, so you do not re-derive them

**The corpus is the ceiling for anything the mutator cannot manufacture.** Over 21947 SQL-Tester
queries: median 91 characters, 89% no join, 86.5% no subquery, 96.5% no CTE.

**Chaining edits alone changed nothing** (median 306 -> 308 chars). The reason is the operator mix:
across 6154 rejections it is 48% M1-M4-expr, 27% M7, 14% M5 and only 11% M6-nesting, so three quarters
of edits replace a leaf. Biasing chained steps to 90% structural moved subqueries per query from 0.353
to 0.425 and cost 16% of usable output.

**M10 splice was the real step change**: joins per query 0.071 -> 0.170, queries with a join 4.1% ->
11.9%, subqueries 0.353 -> 0.553, and usable output went *up*, because a type-agnostic splice survives
the analyzer where a type-matched one does not.

**The benchmark catalog is a much better corpus source than the SQL-Tester tree.**
`CREATE EXTERNAL CATALOG bench PROPERTIES('type'='benchmark')` gives tpch/tpcds/ssb schemas and data
with no setup. Materialise them into native tables (`CREATE TABLE ... AS SELECT ...`) rather than
querying the external catalog directly: the external path goes through `BenchmarkScanner` and **never
touches the OLAP storage engine**, which is where most of the crashes found so far live. 32 tables /
28M rows import in ~70s. Benchmark queries are 1019 chars median, 99.2% have a WHERE, 24.8% a CTE.

**The benchmark corpus is currently dead weight in the soak.** Its 125 files were installed under
`test/sql/benchmark_suite/T/` and each begins with `USE bench_tpcds;`, but the soak's in-process FE has
only the tables `StarRocksAssert` created — `bench_tpch` and `bench_tpcds` exist solely on the dev2
cluster. Measured: `corpus: 125 files` -> `seeds: 0, mutants: 0`. Every one is dropped as stale, so the
benchmark queries raise complexity **only on the cluster replay side**, where they are not mutated at
all. To make them count in the soak, put the 32 tables' DDL into each file's setup section rather than
a bare `USE`; the soak only analyzes, so no data is needed and `StarRocksAssert.withTable` suffices.

## Concurrency: what is actually true

Shards are **processes, not threads**. Schema setup goes through `StarRocksAssert` into the process's
one in-process catalog and `GlobalStateMgr` is a singleton, so two threads setting up different corpus
files would race on table names.

Measured scaling on a 104-core box:

| shards | round | aggregate |
| --- | --- | --- |
| 1 | 80s | 316/s |
| 4 | 83s | 1112/s |
| 12 | 141s | 1982/s |
| 24 | 456s | ~1400/s (worse) |

**The bottleneck is load imbalance, not contention.** Per-shard rate is the same alone (316/s) as at
12-way (308/s). But shard time ranges 51s to 149s because stride partitioning assumes equal work per
file while seed counts per file vary 3x. The round waits for the tail; the machine sits at load 6.
Three wrong hypotheses were tested and disproved first: framework startup (only 7s),
`-XX:ActiveProcessorCount` (no effect at all), disk I/O (`wa=0`, no D-state processes).

**Open fix**: greedy bin-pack shards by seed count instead of `fi += shards`. Should take 12-way from
141s to about 80s with no extra processes.

Two traps that cost a round each: twelve mavens sharing one `~/.m2` serialise on lock files (one round
hung 29 minutes on a `.part.lock`) — shards now use `-Dmaven.repo.local` pointing at a private copy;
and twelve mavens running the full `test` lifecycle drive the thrift codegen plugin into the same
`target/`, which lost 10 of 12 shards. Build once, then shards run `surefire:test` only.

## What the soak can and cannot see

~~**The soak is FE-only and stops at the analyzer.**~~ **No longer true.** It plans every
round-tripped mutant (`srfuzz.plan`, default 100%) and reports `PLAN_REJECTED` /
`PLAN_INTERNAL_ERROR` classified by `ErrorType`. Optimizer defects now come out of the soak
directly -- `InputDependenciesChecker`, `PlanValidator`, `MultiDistinctByMultiFuncRewriter` and the
varbinary statistics defect were all found this way.

The old chain (parse → mutate → analyze → deparse → reparse → re-analyze) still runs; planning is
an extra stage on top of it. Optimizer coverage comes entirely from the cluster replay, which has three oracles:
crash/error, the knob differential, and the FE warn-log signatures — the last of which is what caught
the push-down-distinct stale-projection defect, because that one planned fine and returned the right
rows while silently losing statistics for a whole subtree.

## Grey-box: what is measured, what works, what does not

⚠️ **Two things this section used to say are wrong, and one number it quoted should not be reused.**
Both were corrected by measurement on 2026-08-06; the corrected design is what ships today.

| signal | covers | responds to statistics | cost | use |
|---|---|---|---|---|
| `RuleCoverage` (`appliedRuleMasks`) | memo phase ONLY | **no** | needs a 2nd optimize | not in the loop |
| ~~`RuleFiringTap`~~ | **nothing usable -- see below** | — | high | **do not use** |
| **`RuleTrace`** (product's own `Tracers`) | the RBO phase, rules genuinely applied | untested | free | **feedback key** |
| **`PlanShape.elements`** | the plan the optimizer CHOSE | **yes** | free | **feedback key** |
| **`SqlFeatures`** | what the statement CONTAINS, pre-plan | n/a | free | **feedback key** |

**`RuleFiringTap` reports rules CONSIDERED, not applied.** It intercepts
`RewriteTreeTask#applyRules` and sets a bit for every rule in the `rules` argument, but the product
only reaches `transform` after four guards (`RewriteTreeTask.java:100-108`: rule disabled, rule
exhausted, pattern match, `Rule#check`). Measured: it reads **121 rules for a bare scan and 124 for
a filtered join** -- three bits apart -- where the real firing sets are twelve apart. It is very
nearly a constant, so **the "real coverage is 178/265 ≈ 67%" figure is retired**: that was the
phase's static rule list.

**The correct hook was already in the product and costs nothing.** `RewriteTreeTask.java:117` wraps
`rule.transform` in `Tracers.watchScope(Module.OPTIMIZER, rule.toString())`, INSIDE those guards, and
`Rule#toString()` returns `type().name()`. So arming the tracer per query and probing
`Tracers.getSpecifiedTimer(<RuleType name>)` gives the real per-query RBO vector -- no JMockit, no
agent, no second optimize. `StatementPlanner` never calls `Tracers.init`/`register` itself, so the
mask armed before planning survives the whole plan. Measured on six shapes: scan 17, filter 22,
agg 25, join 29, sortlimit 21, union 18, **union 40**; of a filtered query's rules, **22 are
invisible to the masks and 1 is visible**. Regression test: `RuleTraceProbeTest`.

**The masks are blind to cost** (this part still holds). With `FuzzStatisticStorage` consulted 90,240
times over 816 production queries, the fired-rule set did not move by one bit. Exploration applies
every rule that MATCHES; cost only picks among the alternatives.

**The feedback key is no longer the fingerprint string.** An exact-match set has no partial credit --
a plan one operator away from a known one scores the same as a repeat -- and it saturates once the
common shapes are enumerated, which is exactly when a long run needs the signal. `CoverageMap` counts
namespaced ELEMENTS instead: `R:` rules, `OP:`/`EDGE:` plan operators and parent→child pairs, `F:`
static SQL features. Rarity is relative and recomputed every 1000 observations, so "rare" means the
same share of elements at mutant one million as at mutant one thousand.

**Features are read off the parse tree, so every mutant is credited.** This matters more than it
sounds: measured, **947 of 2286 mutants are rejected by the ANALYZER** and never reach the planner.
Collecting features inside `planMutant` credited 1548 of them; collecting at the top of `evaluate`
credits all 2404. Do not move it back.

Energy rides on the splice pool: material is drawn uniformly, so inserting a mutant once per unit of
energy is a power schedule in the mechanism that already exists. Capped at 4, pool bounded at 5000 --
a loop free to fill the pool with its own output drifts off the production shapes the corpus came
from.

**Measured on 3 production corpus files, 20 mutations/seed, 118 seeds, 2286 mutants:**

```
coverage: 363 elements over 2404 mutants (rules 104/265, plan-ops 36, plan-edges 156, sql-features 67)
plan shapes: 268 distinct; 225 of 2404 credited mutants gained coverage
rarest: EDGE:PHYSICAL_HASH_AGG->PHYSICAL_NESTLOOP_JOIN[RIGHT OUTER JOIN](1),
        EDGE:PHYSICAL_TABLE_FUNCTION->PHYSICAL_TABLE_FUNCTION(1), R:TF_PUSH_DOWN_PREDICATE_CTE_CONSUME(1)
```

**104/265 rules is the first honest optimizer-coverage number this project has had.** It is not
comparable with the masks' 10.2% (different phase) or the tap's 67% (different question).

**A/B in flight on dev1** -- `astsoak` (feedback=0) vs `astfb` (feedback=100), identical but for
`SRFUZZ_EXTRA`. Judge it on **de-duplicated defect signatures after 50+ rounds**, not on shape or
rule counts: coverage and defect-finding are not the same claim. Design and stopping conditions in
`.fuzzwork/plan_ab.md`.

⚠️ To confirm the two arms actually differ, look for `(fed back into the splice pool)` in the
`plan shapes:` line. Do NOT grep the shard log for `srfuzz.feedback=` -- that log holds test output,
not the maven command line, so it never appears and the check silently passes for both arms.

## Open work, roughly by value

1. ~~**M11 predicate mutation.**~~ **Done.** `PredicateMutation.java`, registered in `OPERATORS`.
   Conjoins, disjoins and negates WHERE; adds and extends HAVING; adds a conjunct to a join's ON
   clause. Constants are drawn from the pool's harvested scalars, so they sit in the corpus's own value
   ranges; column references come from the block's own `SlotRef`s, so they resolve where they land.
   The generator is **biased towards weak predicates** (`IS NOT NULL`, tautologies, wide `BETWEEN`) for
   the reason this item always carried: an over-selective predicate empties the baseline, and the
   cluster differential cannot compare two plans over zero rows. `diff_empty` now measures exactly
   that, so the bias is checkable rather than assumed — watch it in `rounds.tsv` after M11 ships.
   The one thing deliberately NOT done: a predicate is never attached to a join that has no ON clause.
   That turns a cross join into an inner join, which is a shape change and belongs to M6.

   **Measured A/B**, same 40 corpus files, same rng seed 4242, 20 mutations/seed, 1207 seeds — the only
   difference between the two runs is whether `PredicateMutation` is in `OPERATORS`:

   | | control | with M11 | |
   | --- | ---: | ---: | --- |
   | mutants | 22700 | 22736 | +0.2% |
   | emitted statements | 13885 | 14163 | +2.0% |
   | statements with a WHERE | 3071 | 3644 | **+18.7%** |
   | statements with a HAVING | 433 | 1263 | **+192%** |
   | `AND` occurrences | 1256 | 1315 | +4.7% |
   | analyzer rejections | 9990 | 9750 | −2.4% |

   HAVING nearly tripled, which is the number this operator was written for. **It costs nothing in
   usable output** — that was the real risk, since M10 showed a type-matched injection loses output
   where a type-agnostic one does not, and rejections went slightly *down* rather than up. M11 takes
   9.0% of rejections, in line with M10 (9.3%) and M5 (8.5%), so it is not a rejection factory.

   The `AND` delta is deliberately reported as the modest number it is: M11 is one of five operators,
   draws roughly 9% of edits, and adds at most one conjunct per application. Moving the corpus-wide
   0.36 `AND`/query materially needs either operator weights (item 2) or a longer chain, not a better
   predicate generator.
2. **Operator weights**, e.g. `-Dsrfuzz.weights=M10-splice:4,M6-nesting:3`. Today operators are shuffled
   uniformly and the first applicable one wins, so the effective mix is "uniform x applicability" and
   cannot be steered. Useful as a targeted mode on a couple of shards, not as a permanent global change.
3. **Corpus seed filtering**, e.g. `-Dsrfuzz.seedFilter=window`, to stress one feature without paying
   for the whole corpus.
4. **Load-balanced sharding** (above).
5. **Mutating DDL/DML.** 52% of corpus statements (ALTER, INSERT, CREATE MV, schema change) are replayed
   verbatim and never mutated, and schema change is historically bug-dense.

## Rules that have earned their place

**Verify the artifact contains your change before you measure with it.** Class timestamp plus a
`strings`/`javap` check for something only the new code has. An A/B ran twice over the same stale
classes and its identical results nearly retired a hypothesis that was fine. (Anonymous inner
classes carry their generics in `Outer$1.class`, not `Outer.class`.)

**A measurement that cannot fail loudly is one whose zero means nothing.** Increment the counter
BEFORE the work and report failures separately. `catch (Throwable ignored)` in probe code
manufactures data: a collector that threw on all 132 samples reported "0 of 0 sampled".

**Truncation belongs in the display layer, never in the record.** Three caps in series
(`sample` 1000 → `oneLineSql` 400 → `abbrev` 300), each justified by the next being smaller, threw
away the only statement that could reproduce a finding. Lifting the outermost changed nothing.

**Nothing reaches a long-running tree without checkstyle and a full `-am` build first.** The soak
runs checkstyle bound to `validate` and stops after three consecutive build failures. Both arms of
the A/B died that way -- twice in one day, the second time after the lesson was already written down.

**One maven per tree.** Concurrent builds clobber `target/` and produce compile errors unrelated to
anything you changed; concurrent JVMs starve the in-process cluster until `beforeAll` times out.

**Never `pkill -f` a pattern that appears in your own command line** -- including inside a commit
message or heredoc. Five self-kills in one session. Put the logic in a script file, or filter by PID.

- An operator returning null is normal and cheap. Prefer it to forcing a mutation that does not fit.
- Never share an `Expr` between two trees; keep injected fragments as text and reparse at injection.
- The tree is unanalyzed when an operator runs: decide from the node class, never from `getType()`.
- Worry about producing a tree that deparses to something the parser accepts but that *means something
  different*. That is a false finding, and it is the failure mode to design against.
