# Cluster-replay fuzz findings — 2026-07-31

Mutated queries from the AST fuzzer replayed against a live ASan cluster as **mixed load**
(3 readers + 2 writers concurrent, 45 s per corpus group). This is the first run of that harness.

**Round 1 status (2026-08-01).** Six of these were minimized and fixed by parallel agents; the
fixes are cherry-picked onto the cluster and rebuilding. Each entry below carries its own status
line. What has NOT happened yet: no fix is verified on the cluster, nothing is pushed, no PR is
open. The original caveat still applies to anything still marked candidate — those are fuzzer
signals, and the status line is the claim being made, not more.

## Environment

| | |
|---|---|
| Cluster | dev2 `172.26.92.205`, container `sr-dev-fuzz`, via dev1 `fha@39.99.136.234` |
| Build | `33f70799576` + `8bcb98e1949` (approx_top_k FE fix), ASan BE |
| Harness | `/home/disk1/fha/sr-ws/fuzz/clusterfuzz/clusterfuzz.sh` |
| Corpus | 760 groups emitted by the FE fuzzer at `3a97497da92` |
| Coverage | ~100 rounds, one corpus group per round |

Raw evidence: `clusterfuzz/findings.md` (stacks), `clusterfuzz/error-signatures.tsv`,
`clusterfuzz/rounds.tsv`. See memory `srfuzz-cluster-replay` for how to restart it.

---

## C0 — use-after-free introduced by the pass-through cancel fix (#76603), shipped

**FIXED, cluster verification pending.** `83bd3e15559` — share ownership of `PassThroughChannel`
with its sink and receiver, so the two early guard-release sites (`_fail_cleanup` AND the success
path `count_down_execution_group`) can no longer strand a raw pointer. Reproduced under ASan in a
standalone unit test — same write/free pair as the cluster report — and the test was seen failing
first. Not yet linked into the full `compute_env_test` and not yet run on the cluster. The 4.1
backport #76889 carries the same defect and needs the same patch.


**Status: confirmed by construction. The write site is the line the fix added. Affects main and 4.1.**

Round 44, group `mut_055`. ASan, so this is a real memory error rather than an assertion.

```
==372390==ERROR: AddressSanitizer: heap-use-after-free on address 0x50b00068cb98
WRITE of size 1 at 0x50b00068cb98 thread T507
    #2 starrocks::PassThroughChannel::set_cancelled()   local_pass_through_buffer.cpp:115
    #3 starrocks::PassThroughContext::set_cancelled()   local_pass_through_buffer.cpp:180
    #4 starrocks::DataStreamRecvr::close()              data_stream_recvr.cpp:289
    #5 starrocks::pipeline::ExchangeSourceOperatorFactory::~ExchangeSourceOperatorFactory()
   ... starrocks::pipeline::Pipeline::~Pipeline()       pipeline.h:33

freed by thread T507 here:
    #1 starrocks::PassThroughChunkBuffer::~PassThroughChunkBuffer()
    #2 starrocks::PassThroughChunkBufferManager::close_fragment_instance()
    #3 starrocks::DataStreamMgr::destroy_pass_through_chunk_buffer()
    #4 starrocks::pipeline::PassThroughChunkBufferGuard::~PassThroughChunkBufferGuard()
                                                        fragment_context.cpp:77
    #8 starrocks::pipeline::FragmentContext::destroy_pass_through_chunk_buffer()
                                                        fragment_context.cpp:376
    #9 starrocks::orchestration::FragmentExecutor::_fail_cleanup(bool)
                                                        fragment_executor.cpp:1060
```

Same thread, in order: the fragment fails, `_fail_cleanup` destroys the `PassThroughChunkBuffer` that
owns the channels, and then the pipeline is torn down — and `~ExchangeSourceOperatorFactory` calls
`DataStreamRecvr::close()`, which writes `_cancelled` on a channel inside the buffer that was just
freed.

**Why this is a regression rather than an old bug.** `set_cancelled()`, the `_cancelled` flag, and the
call to it from `DataStreamRecvr::close()` were all *added* by `bd22b6363d0`, "[BugFix] Stop
pass-through exchange from appending after the receiver is cancelled (#76603)":

```
--- b/be/src/compute_env/data_stream/data_stream_recvr.cpp
@@ -283,6 +285,8 @@ void DataStreamRecvr::close() {
+    // Stop the peer sink from appending into the shared pass-through buffer.
+    _pass_through_context.set_cancelled();
```

Before that commit, `close()` did not touch the shared buffer, so the teardown order did not matter.
The fix gave the receiver a write into an object whose lifetime belongs to a different teardown path,
with nothing ordering the two. `bd22b6363d0` is an ancestor of this cluster's build, which is why the
crash reproduces here at all.

**Shipped.** `bd22b6363d0` is merged to main as #76603 and backported to 4.1 as `72103970f15` /
#76889. Both carry the same call.

**Next:** minimize, then decide the shape of the correction — either `close()` must not reach a buffer
the fragment-failure path may already have destroyed, or the channel's lifetime has to outlive both
sides rather than being owned by `FragmentContext`. Also worth checking whether the sibling call added
to `remove_sender()` in the same commit has the same exposure.

## C1 — `AggStateUnion::merge` type confusion in the storage scan path

**Status: strongest candidate. Repeats. Matches no record I could find.**

Rounds 54, 60, 81 and continuing; groups `mut_067`, `mut_076`, `mut_1012`.

```
casts.h:78: To down_cast(From*)
  [with To = const starrocks::BinaryColumnBase<unsigned int>*; From = const starrocks::Column]:
  Assertion `f == NULL || dynamic_cast<To>(f) != NULL' failed.

  down_cast<const BinaryColumnBase<unsigned int>*>
  <- starrocks::GetContainer<(starrocks::LogicalType)17>::get_data(Column const*)
  <- starrocks::NullableAggregateFunctionBase<...>
  <- starrocks::AggStateUnion::merge(FunctionContext*, Column const*, unsigned char*, unsigned long)
  <- AggregateFunctionBatchHelper<AggStateUnionState, AggStateUnion>::merge_batch_single_state
  <- starrocks::AggFuncBasedValueAggregator::aggregate_batch_impl
  <- starrocks::AggFuncBasedValueAggregator::aggregate_values
  <- starrocks::ChunkAggregator::aggregate()
  <- starrocks::AggregateIterator::do_get_next(Chunk*)
  <- starrocks::TabletReader::do_get_next(Chunk*)
  <- starrocks::pipeline::OlapChunkSource::_read_chunk_from_storage
```

A column being merged as an agg-state value is not the `BinaryColumn` the function expects. It
happens while **reading a tablet**, not while executing an aggregation node, so the state is coming
off disk rather than out of a plan.

**Why this matters more than an assert failure normally would:** `down_cast` asserts, and asserts
compile out of a release build, where the same line becomes an unchecked `static_cast`. On a release
binary this path does not abort — it reads one column type as another.

Both triggering groups declare `array_agg` in their setup. Whether the trigger is the aggregation
itself or the harness's amplification step (`INSERT INTO t SELECT * FROM t`, which for an aggregate
table re-inserts already-aggregated values) is **not yet established** — and that distinction decides
whether this is reachable by a user.

**Next:** minimize. Take `mut_067`, bisect the setup and the query file to the smallest pair that
still aborts, then check whether it reproduces without the amplification step.

## C1b — `JsonColumn::append` DCHECK on the same tablet-read path as C1

**FIX APPLIED, verification pending.** `007e57a5028` cherry-picked to the cluster. This also tests
whether it accounts for C1, since they share the path.


**Status: candidate. A fix exists on an unpushed local branch; not deployed here.** Round ~120s of
the first run, 14:29. Missed in the first pass of this document -- I classified the crashes from a
snapshot and did not re-read the log before restarting the fuzzer.

```
json_column.cpp:347] Check failed: 0 == this->size() (0 vs. 4)
  starrocks::JsonColumn::append(Column const&, size_t, size_t)
  <- starrocks::NullableColumn::append
  <- starrocks::Chunk::append(Chunk const&, size_t, size_t)
  <- starrocks::HeapMergeIterator::do_get_next(Chunk*)
  <- starrocks::AggregateIterator::do_get_next(Chunk*)
  <- starrocks::TabletReader::do_get_next(Chunk*)
```

The assertion and the assumption behind it are both in the source:

```cpp
void JsonColumn::append(const Column& src, size_t offset, size_t count) {
    const auto* other_json = down_cast<const JsonColumn*>(&src);
    if (other_json->is_flat_json() && !is_flat_json()) {
        // only hit in AggregateIterator (Aggregate mode in storage)
        DCHECK_EQ(0, this->size());
```

The comment asserts this is only reached from `AggregateIterator` with an empty destination. The
stack *is* from `AggregateIterator`, and the destination had 4 rows, so the second half of the
assumption is wrong.

**Same path as C1.** C1 is `AggStateUnion::merge` via `ChunkAggregator`; this is `JsonColumn::append`
via `HeapMergeIterator`. Both are `AggregateIterator` <- `TabletReader` — reading an aggregate table
and merging a complex-typed value column during the scan. Two column implementations, two invariants,
one path. Worth treating as possibly one root cause until shown otherwise.

**A fix already exists, unpushed.** `007e57a5028` "[BugFix] Reconcile mismatched flat-JSON schemas in
JsonColumn append", on branch `feature/auto-dev-iteration`, does exactly this:

```diff
-    if (other_json->is_flat_json() && !is_flat_json()) {
-        DCHECK_EQ(0, this->size());
+    if (other_json->is_flat_json() && !is_flat_json() && this->size() == 0) {
```

and adds real schema reconciliation (`is_equallity_schema` + `json_merger`) for the mismatched case,
covering both `append` and `append_selective`, with 128 lines of unit test. It is not in this
cluster's build, and not merged anywhere.

**Next:** deploy `007e57a5028` to the cluster and confirm the DCHECK stops firing; then check whether
it also accounts for C1, since they share the path. The commit is unpushed and carries tests, so it
should go to a PR on its own merit regardless.

## C2 — planner emits a `TExprNode` with a null `node_type`

**FIXED.** `c2f5d58d4fd`. Minimal repro:
`SELECT dense_rank() OVER (ORDER BY abs((SELECT max(v1) FROM t0)));`
Root cause was not a class forgetting to set the field — it was a `Subquery` reaching thrift at all.
`QueryTransformer.window()` translated the window spec with the builder-less `translate()`, producing
a bare `SubqueryOperator` typed INVALID that no rule converts. The fix plans those subqueries into an
Apply, and `ExprToThrift.visitSubqueryExpr` now throws instead of emitting an incomplete node.


**Status: clean candidate, unminimized.** Round 57, group `mut_071`.

```
ERROR 1064 (HY000) at line N: Required field 'S' was not present!
  Struct: TExprNode(node_type:null, type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarT...
```

The FE serialized an expression node without setting `node_type`, and thrift rejected it on the way
to the BE. This is an FE-side defect by construction: whatever expression produced it, the planner
should not be able to emit an incomplete node. Cleanest of the planner candidates because it needs no
argument about whose contract was broken.

## C3 — `VARBINARY(-1)` reaches the BE in a cast

**Status: candidate, unminimized.** Groups `mut_009`, `mut_025`, `mut_084`, `mut_1007`.

```
ERROR 1064 (HY000) at line 39: Not support cast LARGEINT to VARBINARY(-1). backend [id=10001]
ERROR 1064 (HY000) at line N:  Not support cast BOOLEAN  to VARBINARY(-N)
ERROR 1064 (HY000) at line N:  Not support cast VARBINARY(-N) to DOUBLE
ERROR 1064 (HY000) at line N:  Not support cast VARBINARY(-N) to DECIMAL128(N, N)
```

**FIXED, but my framing above was wrong and the correction matters.**

I wrote that `VARBINARY(-1)` "is not a type anyone can write" and called it the defect. In fact -1 is
a deliberate sentinel for "unspecified length", set by `TypeParser.getBaseType` for bare `VARBINARY`
exactly as it is for bare `CHAR`/`VARCHAR`, and legal inside the FE (`isWildcardVarchar` tests it,
`toSql()` renders the bare name for it). **The BE uses the same sentinel** — `TypeDescriptor::len`
defaults to -1 — and `CAST(x AS VARCHAR)` ships `VARCHAR(-1)` today and works. The `(-1)` in the
error text is `debug_string()` printing the length unconditionally. So this is a consistency and
hygiene defect, not a crash or a corruption, and I overstated it.

What IS wrong, and is fixed by `cb3570617c7`: nothing resolves the sentinel on the expression path,
so it reaches persisted metadata. Verified: `CREATE VIEW v AS SELECT CAST(v1 AS VARBINARY)` stores a
column with `len == -1`; with the fix it stores `varbinary(1048576)`, matching what an explicit
column definition gets. Minimal repro is just `SELECT CAST(v1 AS VARBINARY) FROM t0`. The fix
resolves unsized VARBINARY in `ExpressionAnalyzer.visitCastExpr` on the explicit-cast branch only,
recursing through ARRAY/MAP/STRUCT, without mutating the shared `VarbinaryType.VARBINARY` singleton.

**The real contract gap is the second item, and it is NOT fixed.** `PrimitiveType.java:192` makes the
FE accept VARBINARY to and from every basic type, while `be/src/exprs/cast_expr.cpp:2154` only
implements string to/from varbinary. The FE plans casts the BE cannot build. **Same family as C4** —
the FE's registration surface is wider than the BE's implementation. Narrowing the FE matrix is a
user-visible behaviour change and belongs in its own PR.

Also found, not fixed: `VarBinaryLiteral` assigns the shared mutable `VarbinaryType.VARBINARY`
singleton to its own `type` field, which is a hazard independent of the length; and binary literals
and builtins returning VARBINARY still serialize `len:-1`.

## C4 — `Invalid agg function plan: max_by_v2` over VARBINARY

**FIXED.** `62dbfd9fc64`. `FunctionSet` registers max_by/min_by over the full type cross product
while the BE resolver only instantiates `lt_is_aggregate || lt_is_json || lt_is_collection`, so
VARBINARY, HLL, BITMAP, PERCENTILE, VARIANT and TIME resolve in the FE and die in the BE. The FE now
rejects them. NOTE: the guard keys on the user-facing names, so the agg-state combinator forms
(`max_by_state`, `max_by_union`) still reach the same BE gap.


**Status: candidate, unminimized.** Round 63, group `mut_081`.

```
ERROR 1064 (HY000) at line N: Invalid agg function plan: max_by_v2 with
  (arg type VARBINARY, serde type VARBINARY, result type VARBINARY, nullable true) backend [id=...]
```

The FE produced an aggregate plan the BE refuses to build. Same family as C3: the rejection is
correct, the fact that it is the BE doing it is the finding.

## C5 — `slot type shouldn't be invalid` / `Invalid plan:`

**BOTH FIXED, and "weak candidates" was wrong — they were three defects, not two.**

`slot type shouldn't be invalid` has TWO independent causes, found by two different agents. That is
worth stating because the signature must not be deduplicated as one bug:

- `a66ac16df34` — `count(a)` where `a ARRAY<DECIMAL64(10,2)>`. `argumentTypeContainDecimalV3` matches
  on the ARRAY's item type, routing `count` into decimal rewriting, but `commonType` is only computed
  when the argument itself is decimal, so INVALID is stamped as the return type and nothing rejects
  it until `SlotDescriptor.setType`.
- The C2 subquery-in-window defect reaches the same message when the subquery is the ORDER BY
  expression directly.

`Invalid plan:` was also two defects:

- `18ea932b748` — `AggregationAnalyzer.visitCollectionElementExpr` visited only `getChild(0)`, so a
  bare column in the subscript escaped the GROUP BY check entirely and the planner then rejected its
  own output. Minimal: `SELECT map_agg(v1, v2)[v3] FROM t0`.
- `8f0fad3ab23` — the empty body. The message is `"Invalid plan:" + newline + <whole plan dump> +
  reason`, so the reason sits past the point every client and log truncates. Reordered. Same commit
  guards an unprotected `e.getMessage()` that would NPE inside its own catch.

Sibling gaps flagged but not fixed: `visitLikePredicate` and `visitMatchExpr` have the same
`getChild(0)`-only shape, so a bare column on the right of LIKE/MATCH likely escapes the same check.

Original triage note, kept because it was wrong in an instructive way: I called these "weak
candidates ... worth a look mainly because they are cheap to check". They were the densest source of
real defects in the run.



```
Getting analyzing error. Detail message: slot type shouldn't be invalid.
ERROR 1064 (HY000) at line N: Invalid plan:
```

Both are the planner refusing its own output. `Invalid plan:` arrived with an empty body, which is
its own small defect — an error that does not say what was invalid is not actionable. Worth a look
mainly because they are cheap to check once C1 stops eating the cluster.

---

## Known, not findings

**K1 — `histogram()` called directly aborts the BE.** Round 39, group `mut_049`.

```
histogram.h:121] Check failed: false
  <- starrocks::HistogramAggregationFunction<(LogicalType)11, double>::update
  <- AggregateFunction::update_batch_exception_safe
  <- Aggregator::compute_batch_agg_states
```

Already recorded in memory `histogram-internal-only-fe-guard`: `histogram()` and
`histogram_hll_ndv()` implement only the single-state path and abort when a user calls them
directly. The FE guard that rejects the direct call exists locally as `0f9c0c36d3e` and is **not
deployed on this cluster** — so it keeps firing and keeps taking the BE down with it.

**K2 — approx_top_k merge constant-argument shift.** Not seen in this run, because the FE fix
`edff3489e50` was applied before it started. It had killed the BE for the seven hours before that,
with the fatal counter at 5. Worth noting that the stack differed from the one in
`HANDOFF_CRASH_A.md`: that one goes through `compute_single_agg_state`, this cluster's last one went
through `compute_batch_agg_states`. One root cause, two reachable paths.

## Harness artifacts, not findings

**H1 — retracted.** Round 44 was first written up here as a harness artifact, on the strength of a
`RuntimeEnv::init_execution_thread_pools` frame that looked like BE startup. That frame is from the
*thread-creation* section of an ASan report, not from the fault. The fault is C0, a genuine
use-after-free. Reading one stack out of a multi-stack ASan report and classifying from it is the
mistake; the report's own `WRITE of size 1 ... #0` header is the part that says what happened.

**H2 — crash-induced error cascade.** `Backend node not found`, `Query cancelled by crash of
backends`, `Tablet lost replicas` are all consequences of C1/K1 killing the BE mid-round. The
`benign_error` filter does not cover them, so they are polluting the signature list.

**H3 — errors that belong in FE validation, not here.** `Percentile rate must be between 0 and 1`,
`compression parameter must be positive in percentile_approx_weighted`, `percentile parameter must be
between 0 and 1`. These are constant arguments the BE checks at runtime; per memory
`prefer-fe-validation-for-const-args` they should be rejected during analysis. Real, but a different
kind of work than crash hunting.

---

## What to do next, in order

1. **C0.** It is the only entry that is confirmed rather than candidate, it is a use-after-free
   rather than an assert, and it is already merged to main and backported to 4.1. Minimize it and
   correct the fix that introduced it.
2. **Deploy the histogram FE guard (K1) to the cluster.** One known fix, removes one crash that is
   masking everything after it. Same move already made for approx_top_k.
3. **Fix H2 in the harness** so the findings file stops filling with the consequences of crashes
   rather than crashes.
4. **Deploy `007e57a5028` for C1b** and check whether it also accounts for C1 — they share a path,
   and one deployment answers both.
5. **Minimize C1** if it survives that.
6. **Then C2**, the cleanest of the planner candidates.

---

## Round 9-11 (2026-08-01/02): statistics fix verified, three harness defects removed

### F3/F4 RESOLVED — `PushDownDistinctAggregateRewriter` left dropped columns in a projection

Both reported "missing statistic of col" shapes were **one producer**, not two.
`cbo_push_down_distinct_below_window` rewrites `select distinct a, sum(b) over (...)` into an
aggregation on the scan that pre-aggregates `b`, so `b` is no longer emitted. The rewriter remapped
the projections between scan and window but merged the rewritten map in with `putAll` instead of
replacing it, so the stale `b -> b` entry survived. `pushDownAggregation` then runs
`logicalJoinReorder -> Utils.calculateStatistics` on that tree immediately, before the column pruning
it schedules afterwards, and statistics derivation trips on the stale entry.

The exception is **swallowed**, so the query still plans — but the whole subtree is left without
derived statistics, and join reordering and aggregate push-down then choose a plan from statistics
that were never computed. That is the real damage; the log line is only the symptom.

- Minimal: `select distinct v1, sum(v3) over () from t0`
- Fix: `/home/public/sr-projdrop` `95740e7993b`, test `bfab685d678`
- Keep an original entry only when its column is also a grouping key of the pushed-down aggregation.
- **Verified end-to-end on the cluster: 21 occurrences before, 0 after.** Not pushed, no PR.
- Why the earlier agents' unit tests missed it: `PlanTestBase` sets `cbo_push_down_aggregate_mode=-1`,
  which skips the very call that surfaces it. Tests must set it back to `0`.

### New — dangling MV foreign-key constraint breaks metadata replay (low severity)

```
IllegalArgumentException: BaseInfo's db 220930 should not be null in the foreign key constraint
  ForeignKeyConstraint.getTableBaseInfo:250 <- parse:212 <- TableProperty.buildConstraint:979
  <- buildMvProperties:549 <- gsonPostProcess:1519
```
Hit on **FE startup replay** and in `global-state-checkpoint-worker`. Dropping a database referenced
by an MV's foreign-key constraint leaves a persisted constraint that no longer parses. Caught and
ignored ("Failed to parse foreign key constraints"), so it is metadata hygiene, not a crash — but
the constraint is silently lost and the message tells the operator to drop constraints by hand.

### Open — the VARBINARY unsized sentinel is not fully resolved by my own fix

`bugfix/varbinary-negative-length` (`cb3570617c7`) is **already deployed on this cluster**, and the
fuzzer still produces `Not support cast VARBINARY(-N) to BOOLEAN`. That fix resolves the sentinel for
`CAST(x AS VARBINARY)`, i.e. VARBINARY as the cast *target*. Here `VARBINARY(-1)` is the cast
*source*, so something still produces or retains the unsized type on another path. The fix has a hole.
Not yet minimized — the direct SQL attempts ran against a dead BE and must be redone.

### Harness defects fixed this round (all three are "a failure that was not recorded as one")

1. **Setup failures were invisible.** The harness counted only query errors, so a group whose tables
   failed to create looked identical to a clean round. `rounds.tsv` now carries `tables` and
   `setup_fail`, and distinct setup failures go to `setup-failures.tsv`. This had been true for
   seven rounds.
2. **Blanket DROP-stripping corrupted schemas.** Stripping *every* DROP (the workaround for trailing
   teardown deleting tables the queries need) collapsed mid-file `DROP t; CREATE t` pairs into two
   bare CREATEs, so the second died with `Table already exists` and the group replayed against the
   **old schema**. Now object-aware: keep a DROP only when the same object is created again later.
   Result: `Table already exists` and `Cannot cast ARRAY<struct<...>>` signature classes gone,
   mut_000 setup failures 2 -> 0, mut_009 20 -> 11, and recovered coverage shows up directly in
   query counts (mut_001 7394 -> 7488, mut_003 4882 -> 4966, mut_010 837 -> 851).
3. **`be_alive` asked the OS, not the FE.** It used `pgrep -x starrocks_be`, but a backend accepts a
   pid instantly and takes ~30s to register. One round ran 2198 queries against a not-yet-serving
   backend and logged **79128 bogus errors and 4 bogus FE signatures**
   (`reportDataNodeNotFoundException` at `NormalBackendSelector.computeScanRangeAssignment`). Now it
   asks the FE for `Alive: true`, and a still-registering process is waited for rather than restarted.
   This is the identical mistake already fixed once for `fe_alive` and left in place here.

### Environment

- `default_replication_num` was 3 on a 1-BE cluster; 171 of 2203 corpus `CREATE TABLE`s omit
  `replication_num`, giving 1418 create-table failures. Set to 1 **in `fe.conf`** — `admin set
  frontend config` does not survive the FE restarts the harness performs.
- The BE was SIGTERMed externally at 17:43:38 (clean shutdown, no ASan report, no OOM). Shared box.

### Still no new crash

Rounds 9-11 produced no ASan report and no new crash signature. The only new error signatures are the
VARBINARY cast matrix (`LARGEINT/BOOLEAN/TINYINT -> VARBINARY`), which is the fifth instance of the
recurring theme: **the FE registration surface is wider than the BE implementation.**

---

## The harness was losing crashes (audited 2026-08-02)

**Yes, crashes were lost.** Mining every `be.out` still on disk turned up **10 distinct crash
signatures**, against 22 `## BE CRASH` entries that were mostly unusable. One archived log alone holds
**68 crash markers against 10 recorded entries**.

Three defects combined, all of them the same disease as everything else in this file — a failure that
was not recorded as a failure:

1. **One record per round, no signature dedup.** `record_crash` fired once per round based on a
   before/after marker count. A round that crashed five different ways recorded one entry, and
   nothing said which crash it was.
2. **The recorded evidence was the wrong text.** It captured `tail -60 "$BELOG"` — but the BE
   restarts after it dies, so by the time that ran, the end of `be.out` was the *new process's
   startup banner*. This is why I read the one recorded crash, saw a startup banner plus an empty
   `be.FATAL` plus zero ASan reports, and **wrongly called it a false positive**. `be.FATAL` is empty
   because a release build reports through glog's `FailureSignalHandler` into `be.out`, and there are
   no ASan reports because this cluster runs a **RELEASE** build. All three checks were the wrong
   checks.
3. **Recording was gated on the marker count rising.** `be.out` rotates and the BE restarts (both by
   this harness and by a neighbour on the box), either of which resets the count and hides a crash
   that did happen.

Fixed: signatures are now extracted per crash banner and deduped in `crash-signatures.txt`; recording
runs every round independent of the counter; the stack is located by seeking to the banner **matching
that signature**; and the crash banner's `query_id` is resolved against `fe.audit.log` to print the
exact statement. There is nothing left to bisect — the harness used to halve query files while the
answer sat one grep away.

### C6 — unbounded cursor read in the Teradata and Joda datetime parsers (FIXED)

```sql
select to_tera_timestamp('', ';yyyy')   -- SIGSEGV @0x0
select str_to_jodatime('', ';yyyy')     -- SIGSEGV @0x0
```

`TeradataFormat::parse` and `JodaFormat::parse` pass every token parser a `val_end` and the
literal-character parsers never look at it — they dereference the cursor directly. On empty input the
cursor is the empty string's null data pointer. Reaching it only needs the format to **begin with a
literal character**: the numeric parsers go through `str_to_int64` and fail safely, which is why a
format starting with `yyyy` returns NULL and one starting with `;` kills the backend. No table and no
privileges required.

Fix: `/home/public/sr-teraparse` `9f659844f86` — bound each cursor read by `val_end`, covering the
Teradata punctuation/am/pm parsers and the Joda literal parser. Not pushed, no PR.

### The remaining signatures, retriaged

| signature | verdict |
| --- | --- |
| `ArrayAggAggregateState<` (6) | known agg-state dictionary-encoding family; fix exists, undeployed here |
| `RunTimeTypeTraits<` (4) | **not new** — expands to `ApproxTopK::get_k_and_counter_num` → `get_const_value<5>` → `cast_to_raw<5>`; the known constant-argument shift from `SplitAggregateRule.appendConstantColumns` |
| `TeradataRuntimeState` (2) | C6 above, fixed |
| `JsonColumn::append` (2) | known, fixed |
| `BinaryColumnBase<unsigned` (2) | known low-cardinality VARCHAR family |
| `VectorizedInConstPredicateGeneric::evaluate_checked` (1) | **genuinely new** — a fatal CHECK in a projection-context `IN (...)`; delegated |
| `MemPool::allocate_with_reserve` (1) | known, #76910 |
| `joda::JodaRuntimeState` (1) | C6 above, fixed |
| `HistogramAggregationFunction<` (1) | known, FE guard exists |
| `ArrayUnionAggAggregateFunction<` (1) | SIGSEGV **@0x1** on the storage-read `AggStateUnion` path — the same dictionary-code-as-pointer family as `ArrayAgg` (@0x9); delegated to check whether the existing FE `DecodeCollector` fix covers this variant |

**This also revises an earlier claim in this file.** "346 rounds, no new crash" was wrong on two
counts: the harness was hiding crashes, and separately the amplification step suspected of triggering
the agg-state family had been silently broken for hundreds of rounds, so its trigger was absent.

### C7 — `VectorizedInConstPredicateGeneric` sizes its result from a cached one-row column

`in_const_predicate.hpp` caches every constant child in `open()` by evaluating it against a **null
chunk**, so each cached column holds exactly one row. `evaluate_checked` then took its output size
from `columns_ref[0]->size()`, which is 1 whenever the left operand is constant. For any chunk with
more than one row that breaks the invariant asserted at `in_const_predicate.hpp:494`:

```
Check failed: ptr == nullptr || ptr->num_rows() == size
```

The non-generic `VectorizedInConstPredicate<Type>` re-evaluates the left operand against the real
chunk, which is why only the Generic class (JSON, ARRAY/MAP/STRUCT) is affected.

**Severity is lower than the stack suggests, and the build matters.** That assertion is a `DCHECK`.
Checking the build banners settles where each archived crash came from:

| archive | build |
| --- | --- |
| `round1` (this crash, and the approx_top_k one) | **ASAN** |
| `round5` (`ArrayUnionAgg`) | RELEASE |
| current (`to_tera` / `str_to_jodatime`) | RELEASE |

So this fired in a checked build, where the DCHECK is live. On the RELEASE cluster I could not make
it manifest at all: `SELECT [1,2] IN ([1,2],[3,4]) FROM t`, the JSON/MAP/STRUCT equivalents, and the
variable-IN-list variants all returned correct results over a 4-row table, because
`ExprContext::evaluate` resizes a constant result downstream. Recording that plainly — **no
release-visible wrong answer was demonstrated**.

The fix is still worth having: it stops depending on that downstream fixup, and it closes a genuine
unchecked heap over-read for a non-constant operand that cannot supply `size` rows.

Fix `/home/public/sr-inconst` `4ea392b9897`, tests `155c15a6f74` — seen failing first in both NDEBUG
(wrong size) and DCHECK-live (abort) builds; 11/11 pass after. Not pushed.

### Correction to how these were prioritised

Earlier entries in this file described the fuzz cluster as a single "Release" environment. It is not:
the campaign switched from an ASAN build to RELEASE partway through, and crash severity cannot be
read without checking the banner in the same `be.out`. Release-visible so far: **C6** (`to_tera` /
`str_to_jodatime`, no table or privileges needed) and the `ArrayUnionAgg` agg-state dictionary family.

> **Owned elsewhere — do not touch from this campaign.** The gap where the agg-state fix covers only
> the V2 low-cardinality rule (`DecodeCollector`) and not V1 (`AddDecodeNodeForDictStringRule`, which
> has no agg-state guard at all and would dictionary-encode a scalar-VARCHAR agg-state column such as
> `v max(varchar(20))`) is being fixed in a separate session. Do not edit
> `AddDecodeNodeForDictStringRule.java`, `DecodeCollector.java`, or the
> `bugfix/array-agg-state-union-crash` branch from here.

### `RunTimeTypeTraits` resolved — the known `approx_top_k` constant shift, and it is worse on Release

Verdict: not a distinct defect. The template parameters decode the case on their own —
**LogicalType 1 = `TYPE_TINYINT`** (the aggregated *value* argument) and **LogicalType 5 = `TYPE_INT`**
(`k`'s declared type, since `approx_top_k(type, INT, INT)`). A column of argument 0's type sitting in
slot 1 *is* the "shifted by one" signature; an unrelated defect would show some other type.

`SplitAggregateRule.appendConstantColumns` appends **every** constant child including child 0, which
the merge phase has already replaced with the intermediate column. `Aggregator::_evaluate_const_columns`
fills `_constant_columns` positionally from the actual children (4 entries) while `get_num_args()`
comes from the signature (3), so the extra column is never noticed. Confirmed on unmodified
`starrocks-2/main`: the merge phase really is `approx_top_k(18: approx_top_k, 1, 3, 10)`, including in
the no-`GROUP BY` shape whose merge runs through `Aggregator::compute_single_agg_state` — the exact
frame in the stack.

**The important part for this campaign:** `cast_to_raw`'s check is a `DCHECK` and `down_cast` uses a
plain `assert`, so this stack cannot come from a true `NDEBUG` Release build — consistent with the
`round1` banner reading ASAN. On a real Release build the same plan **silently reinterprets the int8
as an int32**, producing a garbage `k`/`counter_num` and a garbage counter reservation. So it is a
correctness bug there, not a crash. *Absence of this abort on the Release nodes does not mean absence
of the bug* — which is precisely the trap that made me miscount "no new crashes" earlier.

The FE fix additionally repairs `group_concat` (was reading arg 0 as the separator), `histogram`,
`percentile_approx`, `minmax_n`, and `percentile_cont` — all shifted the same way whenever the
aggregated value was constant. The `window_funnel` carve-out was proven **load-bearing**, not
defensive: removing it drops `1800` from the merge plan, so the BE would read the INT mode as a
BIGINT `window_size`.

Right branch: `bugfix/approx-topk-merge-const-shift` (FE root-cause fix). The alternative BE-side fix
in `wip/topkfix-3commits` counts constants from the end — a workaround layered on positional reading,
not needed. Verification worktree `/home/public/sr-topkverify` `440874e8dab` + tests `4ce78921f45`;
3445 plan tests, 0 failures. The SQL regression case under `test/sql/test_agg_function/` is **unrun**
(needs a live cluster).
