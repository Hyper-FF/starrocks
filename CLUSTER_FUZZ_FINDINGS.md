# Cluster-replay fuzz findings — 2026-07-31

Mutated queries from the AST fuzzer replayed against a live ASan cluster as **mixed load**
(3 readers + 2 writers concurrent, 45 s per corpus group). This is the first run of that harness.

**Nothing here is minimized and nothing is confirmed as a product defect yet.** These are fuzzer
signals, classified by how much they look like one. Treat the "status" column as the claim being
made, not more.

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

## C2 — planner emits a `TExprNode` with a null `node_type`

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

Two separate things here, and they should not be conflated:

1. **The negative length.** `VARBINARY(-1)` is not a type anyone can write. The FE built it and
   passed it down. That looks like the actual defect.
2. The unsupported cast itself is arguably fine to reject — but rejecting it *at the BE* means the FE
   planned a cast it cannot execute, which is the same FE/BE contract gap as C4.

## C4 — `Invalid agg function plan: max_by_v2` over VARBINARY

**Status: candidate, unminimized.** Round 63, group `mut_081`.

```
ERROR 1064 (HY000) at line N: Invalid agg function plan: max_by_v2 with
  (arg type VARBINARY, serde type VARBINARY, result type VARBINARY, nullable true) backend [id=...]
```

The FE produced an aggregate plan the BE refuses to build. Same family as C3: the rejection is
correct, the fact that it is the BE doing it is the finding.

## C5 — `slot type shouldn't be invalid` / `Invalid plan:`

**Status: weak candidates, need triage.** Groups `mut_081`, `mut_054`.

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

**H1 — `RuntimeEnv::init_execution_thread_pools`.** Round 44. Recorded as a crash because the BE was
not alive when the round ended, but the frame is BE *startup*, i.e. the restart logic racing itself.
The liveness check must not run while a restart is in flight.

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

1. **Deploy the histogram FE guard (K1) to the cluster.** One known fix, removes one crash that is
   masking everything after it. Same move that was already made for approx_top_k.
2. **Fix H1 and H2 in the harness.** Cheap, and without them the findings file keeps filling with
   consequences of crashes rather than crashes.
3. **Minimize C1.** It is the only thing here that looks new, it repeats, and until it is minimized
   it cannot be filed.
4. **Then C2**, which is the cleanest of the planner candidates.
