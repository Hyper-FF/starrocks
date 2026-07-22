# Pipeline Work-Stealing — Simplified Design (refactor blueprint)

Status: refactor in progress on branch `feature/pipeline-work-stealing-simplified`
(off clean HEAD `2774e34825c`). This supersedes the 2200-line version; target ~500 lines.

## Why this refactor (decisions, evidence-backed)

The original feature accreted two steal paths, 6 gating predicates, and an event-driven
keep-alive (StealWaiterSet + a new scheduler event) with a UAF risk and a lost-wake bug —
~2200 lines, deep hash-join intrusion. A full review + a fable architecture review + live
experiments established:

1. **Value is real but narrow — position as skew-join's RUNTIME FALLBACK.** StarRocks'
   built-in `enable_optimize_skew_join_v2` is faster (128 vs 156ms) and uses 60% less CPU
   (209 vs 332ms) because it BROADCASTs hot-key build rows (hot rows never shuffle). But it
   is compile-time and needs stats/hint; when stats are missing/stale or no hint is given it
   does not fire. Work-stealing's only defensible niche: **runtime-detected skew with zero
   prior knowledge** (no plan change). Single-node focus (multi-node has an extra intra-BE
   limitation that shrinks steal further — out of scope for now).
2. **partition-free (local-exchange) steal covers ~nothing** — passthrough self-balances.
   DELETE it entirely.
3. **The event-driven keep-alive is not worth it.** A poller-only experiment (coarse ~10ms
   periodic retry, no StealWaiterSet) retained 94% of the ROI: 2.78x vs 2.97x, byte-identical.
   The elaborate producer-side wake (StealWaiterSet + STEAL event + edge-trigger) that caused
   the UAF / F2 lost-wake / F4 off-path-atomic buys ~nothing. REPLACE with a coarse timed
   re-trigger.
4. **Scan is the real ceiling** (~3.9x at 4 buckets); no scheduler cleverness beats it. So the
   design should be simple, not squeeze the last %.

## Target architecture (single node, event-scheduler-native)

One steal path: an idle sibling driver steals one buffered chunk from a hot shuffle
`ExchangeSource` lane, probes it against a clone of the peer partition's read-only build
table, emits on its own lane through a redistribution/gather sink. Default-off.

### Wakeup: timed re-trigger (replaces the whole StealWaiterSet machinery)

Model on the existing RF-timeout timer (`PipelineDriver::_update_global_rf_timer` /
`_unschedule_global_rf_timer`, `pipeline_driver.cpp:1099-1122`):
- When a steal-eligible driver parks (empty own source, not finished), schedule a
  `StealRetryTimeout` PipelineTimerTask (~10ms) via `_pipeline_timer_context->schedule(...)`,
  attached to the driver's own observer.
- On fire → `observer()->steal_trigger()` → `_do_update` directly `try_schedule`s the driver
  (KEEP the STEAL_CHANGE_EVENT direct-schedule branch — it is driver-side and safe; only the
  PRODUCER-side wake is deleted). Driver wakes, tries to steal (burst-drains), re-schedules
  the timer if still eligible.
- `_unschedule_global_rf_timer`-style `unschedule_and_join` on finalize — **the `join`
  structurally eliminates the UAF** (the timer thread is joined before the driver is
  destroyed; no raw observer pointer is ever held by a producer thread).

Why this is correct-by-construction where StealWaiterSet was not: the timer is owned by the
driver, cleaned up with join on the universal finalize path; there is no cross-brpc-thread raw
`PipelineObserver*` and no edge-trigger latch to get lost. Coarse ~10ms period is proven
sufficient (poller experiment).

### Gating: collapse 6 predicates → 1 `can_steal_partition(partition_id)`

On `HashJoinProbeOperator`, one predicate that internally checks, in order:
- `distribution_mode() == PARTITIONED` && `!has_post_probe(join_type)` (INNER/LEFT_*),
- peer `builder_for_partition(p)` exists && `is_build_done()` && `supports_partition_aware_steal()`
  (single, non-sub-partitioned build — Adaptive `_partition_num==1`),
- sink `breaks_partition_identity()` (§5b output safety: hash-shuffle / unpartitioned / random
  exchange, or partial non-finalizing aggregate),
- `all_builds_ready()` (every partition registered + done).
Callers (driver) see ONE predicate; complexity is encapsulated and unit-testable.

## What to DELETE

- `steal_waiter_set.h` (whole file).
- `data_stream_recvr`/`sender_queue`: `_steal_waiters`, register/deregister, `notify_lane_backlog`,
  `notify_all` steal wakes, `_steal_backlog_threshold`.
- `exchange_source_operator`: `register_steal_waiter`/`deregister_steal_waiter`.
- `source_operator.h`: `register_steal_waiter`/`deregister_steal_waiter` virtuals.
- `pipeline_driver`: register-then-recheck-park block; keep the steal hook.
- Partition-free path: `LocalExchange` steal (`source_partition_free`/`is_hash_partitioned`
  steal use), `local_exchange_source_operator` steal members (`_stolen_chunk`, passthrough/
  partition steal), `local_exchange_memory_test` passthrough-steal UTs, `chunk_accumulate`/
  `project`/`local_exchange_sink` `is_stealable` marks, `Pipeline::fully_stealable`/
  `compute_steal_barrier`.
- Dead config: `pipeline_steal_cooldown_ns`, `pipeline_steal_max_per_round` (thrift + Java + TQueryOptions).

## What to KEEP (partition-aware core)

- `ExchangeSourceOperator` steal: `support_steal`/`stealable_backlog`/`try_steal_unit` +
  `DataStreamRecvr::steal_chunk_for_pipeline`/`buffered_chunks_for_pipeline` +
  `PipelineSenderQueue::steal_chunk`/`buffered_chunks` (concurrency-safe pop, no `unpluging`).
- `HashJoinProbeOperator` peer-prober: `accepts_stolen_input`/`push_stolen_chunk`/`_peer_prober`,
  `clone_readable_table(fresh_probe_state)`, `reset_probe`, empty-build `is_done()` guard.
- `HashJoinerFactory::all_builds_ready`/`builder_for_partition`; `supports_partition_aware_steal`.
- `pipeline_driver::try_steal_from_siblings` (partition-aware routing).
- Sink `breaks_partition_identity` (exchange sinks + partial agg).
- Session vars `enable_pipeline_work_stealing` + `pipeline_steal_backlog_threshold`.

## MUST-FIX during refactor

- **F1**: `try_steal_from_siblings` must `return unit_or.status()` on a hard (non-OK) error
  from `try_steal_unit` (chunk already popped), NOT `continue` (silent row loss). Distinguish
  "invalid/lost race" (continue) from "hard error" (return).

## Batches (each ends compile-clean + module-boundary-clean)

- **Batch 1**: delete StealWaiterSet machinery + add StealRetryTimeout timed re-trigger.
- **Batch 2**: delete partition-free path.
- **Batch 3**: collapse predicates → `can_steal_partition`; fix F1; delete dead config.
- **Batch 4**: single-node re-validate (skew join off==on byte-identical + ~2.8x; uniform
  correct; parked CPU ~0). Update UT.

## Validation target (single node)
32M 90%-skew PARTITIONED `[SHUFFLE]` join, dop=16, event scheduler ON: off==on byte-identical,
~2.8x; uniform (sub-partitioned build) off==on (steal skipped). No UAF, no lost rows.
