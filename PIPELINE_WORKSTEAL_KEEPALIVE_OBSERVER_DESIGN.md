# Work-Stealing Keep-Alive — Event-Driven Notification Redesign

Status: design (superseding the busy-poll keep-alive). Focus of this revision: **how a
drained thief is notified to steal** — the crux, since the naive "reuse the existing
wake" is shown below to not work.
Scope: BE pipeline engine only. Default-off (`enable_pipeline_work_stealing`).
Branch: `feature/pipeline-work-stealing` (worktree `/home/public/sr-worksteal`), on top
of commit `3f459225f2a`.

## 1. Problem

Keep-alive lets an idle sibling driver — one that has drained its own shuffle
partition — keep stealing from a *hot* partition for the whole query, so the hot
partition's receiver queue is drained by many consumers in parallel. This relieves the
shuffle back-pressure that otherwise serialises a skewed `[SHUFFLE]` join on a single
probe driver (measured ~2.7× on a 32M, 90%-skew PARTITIONED shuffle join).

The current implementation makes keep-alive work by **busy-polling**: a drained thief
stays in `DriverState::READY` and re-runs `process()` in a tight loop, re-scanning all
siblings (`_has_stealable_backlog()`, O(dop)) every iteration until it can steal or the
query finishes. This is wrong for the event-scheduler model: a `READY` thief burns a
core even when nothing is immediately stealable, competing with productive drivers and
potentially regressing unrelated queries that share the executor.

Goal: drive keep-alive **event-driven** — a drained thief parks (no CPU) and is woken
only when a lane it can steal from actually has backlog — while preserving the ~2.7×.

Non-goals: no change to the steal *path* correctness fixes (§6); no FE/plan/thrift
change; no change to the default-off contract.

## 2. Why the naive "reuse the existing wake" does NOT work

The tempting design — park the thief and rely on the receiver's existing `add_chunks`
→ observer notification — does NOT work, for a concrete reason:

- `DataStreamRecvr::add_chunks()` does broadcast today: `auto notify = defer_notify();`
  → `Observable::notify_source_observers()` fires `source_trigger()` on **every**
  attached observer (all probe drivers share one recvr). BUT there is an explicit
  `// TODO: We just need to notify the affected channels.` at that site
  (`data_stream_recvr.cpp:251`) — the broadcast is an acknowledged over-notification
  slated to be narrowed to the owning lane. Depending on it is fragile.
- More decisively, even with the broadcast the reschedule **predicate** gates on the
  driver's OWN lane. `on_source_update()`
  (`exec/runtime/schedule/pipeline_driver_observer.cpp:40`):
  ```cpp
  static void on_source_update(EventScheduler* es, PipelineDriver* driver) {
      auto source = driver->source_operator();
      if (source->is_finished() || source->has_output()) {   // OWN lane only
          es->try_schedule(driver);
      }
  }
  ```
  A thief that drained its own (light) lane has `has_output()==false`, and while the hot
  lane still has data `is_finished()==false` → **neither holds → it is NOT rescheduled**,
  even though the broadcast reached it. It stays parked. (This is why the pre-keep-alive
  version stole only ~995 rows — parked thieves were never re-woken to steal — and why
  the busy-poll "worked": it never parked, sidestepping this predicate.)

So keep-alive needs a **dedicated notification** whose wake predicate is *"a lane I can
steal from has backlog"*, not *"my own lane is ready"*.

## 3. Notification design — a general targeted steal-wake contract

This is NOT specific to the network receiver. There are **two** steal sources and both
need the same wake, because the busy-poll (which they both relied on) is being removed:

1. Network shuffle — `ExchangeSourceOperator` over a shared `DataStreamRecvr`; producer
   is `add_chunks` on a brpc thread.
2. Local exchange — `LocalExchangeSourceOperator` (passthrough / partition); producer is
   the sink calling `add_chunk` on the target lane's source.

So the wake is modelled as a **source-agnostic contract**, with a per-source
implementation. (Today the local path already has a *broadcast* steal-notify from Phase 2
— `LocalExchangeSourceOperator::add_chunk` fires `notify_source_observers()` edge-triggered
via `_steal_notified`, local_exchange_source_operator.cpp:40 — but it (a) only covers
*passthrough* `add_chunk`, not the *partition* one, and (b) uses the broadcast whose
`on_source_update` own-lane predicate gates parked thieves out, so it too relied on the
busy-poll. This design replaces it with the targeted STEAL wake below.)

Model: a drained steal-eligible thief **parks** (no CPU) and **registers as a steal
waiter on its source**. When the producer puts a batch into a lane whose backlog crosses
the steal threshold, it fires a **dedicated steal wake** on the registered waiters,
scheduling them to run and steal. No busy-poll, no all-driver broadcast, no own-lane
predicate gate.

### 3.0 Source-agnostic contract

On the `SourceOperator` steal interface (source_operator.h), add:

```cpp
// Register/deregister this driver's observer as a steal waiter on the shared buffer this
// source reads from, so the producer can wake it when a peer lane has stealable backlog.
// Default no-op: a source that does not support stealing never registers.
virtual void register_steal_waiter(pipeline::PipelineObserver* observer) {}
virtual void deregister_steal_waiter() {}
```

The driver-side is generic: when a steal-eligible driver parks (§3.2) it calls
`source_operator()->register_steal_waiter(observer())`, and deregisters on wake and in
`finalize`. Each concrete source routes the registry + the producer-side trigger to its
own shared buffer:

- `ExchangeSourceOperator` → the shared `DataStreamRecvr` (registry indexed by
  `driver_sequence`; trigger in `PipelineSenderQueue::add_chunks`).
- `LocalExchangeSourceOperator` → the `LocalExchangeSourceFactory` (which already owns all
  per-lane source operators and an `observes()` observable; registry indexed by lane;
  trigger in both `add_chunk` overloads — passthrough and partition).

Both fit the same shape: a collection of per-lane queues, a producer that enqueues to
lane L, N consumer drivers (one per lane). §3.1–§3.6 below describe the mechanism once;
§3.7 lists the per-source wiring.

Prerequisite already true (network): `PipelineSenderQueue::add_chunks` routes each chunk
to `_chunk_queues[chunk.driver_sequence]` (sender_queue.cpp:82). Local: the sink calls
`add_chunk` directly on the target lane's `LocalExchangeSourceOperator`, so the lane is
known by construction.

### 3.1 New event: `STEAL_CHANGE_EVENT`

- Add `STEAL_CHANGE_EVENT = 1 << 3` and `steal_trigger()` on `PipelineObserver`
  (default `{}` so non-driver observers ignore it).
- `PipelineDriverObserver::steal_trigger()` → `_active_event(STEAL_CHANGE_EVENT);
  _update();`.
- In `_do_update`, for a driver blocked as `INPUT_EMPTY`, `_is_steal_changed(event)` →
  `try_schedule(driver)` **directly, with no own-lane gate**. Sound because the receiver
  fires `steal_trigger` only when a stealable lane actually has backlog — the wake
  itself means "there is work to steal", so the woken driver will find it.

### 3.2 Steal-waiter registry on the receiver (per-recvr)

- `std::vector<std::atomic<pipeline::PipelineObserver*>> _steal_waiters`, sized to the
  degree of parallelism, indexed by `driver_sequence`. Entry = that lane driver's
  observer while it is parked-waiting-to-steal, else `nullptr`. Lock-free: register /
  deregister is one atomic store; `add_chunks` scans this small (size == dop) array. The
  per-lane observer pointer is captured at `bind_profile`/`attach_observer` time (already
  per-`driver_sequence`).
- Register: in `process()`'s empty-source branch, just before a steal-eligible driver
  parks (`INPUT_EMPTY`), store its observer into `_steal_waiters[my_seq]`.
- Deregister: at the top of the next steal attempt (on wake) and in `finalize`/finish,
  clear `_steal_waiters[my_seq]`.

### 3.3 Trigger point (producer side)

In `PipelineSenderQueue::add_chunks`, right after enqueuing a chunk to `_chunk_queues[L]`,
if `_chunk_queues[L].size_approx() >= steal_backlog_threshold` **and** any
`_steal_waiters[*]` is set, fire `steal_trigger()` on each set waiter. Piggybacks on the
existing per-batch `add_chunks`; no new thread/timer.

This is a **separate** path from the owner-notify (`defer_notify`). If the "notify
affected channels only" TODO lands and narrows the owner-notify, the steal wake is
unaffected — robust by construction.

### 3.4 Lost-wakeup safety (register-then-recheck)

Race: `add_chunks` could fire between the thief's backlog check and its registration.
Closed by ordering — the thief **registers first**, then re-scans for stealable backlog;
if any lane already has backlog ≥ threshold it does NOT park (steals now). Because
registration happens-before the recheck, and `add_chunks`'s enqueue happens-before its
trigger scan, any batch that made a lane stealable is seen either by the recheck (→ steal
now) or by the trigger (→ wake) — never lost.

### 3.5 Termination

`ExchangeSourceOperator::is_finished()` is global (`_num_remaining_senders == 0 &&
_total_chunks == 0`). On sender finish / cancel (`remove_sender` / `cancel_stream`, which
already run `defer_notify`), also fire `steal_trigger` on all registered waiters so a
parked thief re-runs, observes `is_finished()`/cancel, deregisters, and finishes. No
thief hangs on a drained-but-not-yet-globally-finished receiver.

### 3.6 Which lane to steal / burst draining

The wake carries no lane id: the woken thief runs the existing
`try_steal_from_siblings()`, which scans siblings and steals from whichever lane has
backlog (the O(dop) scan happens only when *running to steal* — productive — never on
the wake-decision path). It keeps stealing while productive (existing post-success
`continue`), draining a burst of the hot lane per activation, then re-parks +
re-registers when it can no longer steal.

### 3.7 Changes summary

Generic (shared by both sources):
- `pipeline_observer.h`: `STEAL_CHANGE_EVENT = 1<<3`, `steal_trigger()` (default no-op on
  the base observer).
- `pipeline_driver_observer.{h,cpp}`: `steal_trigger()` impl; `_do_update` handles
  `STEAL_CHANGE_EVENT` → `try_schedule` for `INPUT_EMPTY`.
- `source_operator.h`: `register_steal_waiter(observer)` / `deregister_steal_waiter()`
  virtuals (default no-op).
- `pipeline_driver.{h,cpp}`: replace the stay-`READY` busy-poll branch with
  register-then-recheck-then-park (via `source_operator()->register_steal_waiter`);
  deregister on wake and in `finalize`. Keep the steal hook + post-success `continue`.
  Remove `_has_stealable_backlog()` (its scan moves into the register-recheck and into
  `try_steal_from_siblings`).
- A small reusable `StealWaiterSet` (per-lane `vector<atomic<PipelineObserver*>>` +
  `register/deregister/notify_if_backlog/notify_all`), embedded by each source's shared
  buffer, so the registry + trigger logic is written once.

Network source:
- `data_stream_recvr.{h,cpp}` / `sender_queue.{h,cpp}`: embed a `StealWaiterSet`; wire
  `ExchangeSourceOperator::register_steal_waiter` to it; fire the steal wake in
  `add_chunks` on threshold-cross and in `remove_sender`/`cancel_stream`.

Local source:
- `local_exchange_source_operator.{h,cpp}` / the `LocalExchangeSourceFactory`: embed a
  `StealWaiterSet` on the factory; wire `LocalExchangeSourceOperator::register_steal_waiter`
  to it; fire the steal wake in BOTH `add_chunk` overloads (passthrough + partition) on
  threshold-cross, and on finish/cancel. **Replaces** the existing broadcast
  `_steal_notified` notify (which was passthrough-only and predicate-gated).

Unchanged:
- Poller fallback (`pipeline_driver_poller.cpp`) — it already scans blocked drivers and
  calls `try_steal_from_siblings()` on its own cadence, for both source types.

## 4. Why this keeps the ~2.7×

Back-pressure scenario: 16 scan-sink writers fill the hot lane's receiver queue faster
than one owner drains it. Writers push *continuously*, so `add_chunks` crosses the
backlog threshold continuously → the receiver fires steal wakes to the registered thieves
continuously → they steal continuously and drain the hot lane in parallel — the same
effect as the busy-poll, but driven by real arrivals to *only the parked thieves*, with
no empty spins. The CPU the busy-poll wasted is returned to the executor.

## 5. Correctness — orthogonal to the wake, unchanged

The four steal-*path* concurrency fixes remain and are independent of the notification
mechanism:

1. `HashJoinerFactory::all_builds_ready()` also requires `_builder_map.size() ==
   _builder_dop` (every partition's builder registered, not just the registered ones
   done).
2. `try_steal_from_siblings()` never silently drops a unit already popped from a victim
   queue: `StatusOr<bool>`, post-pop failures surfaced (query fails cleanly). Poller path
   handles the error too.
3. `JoinHashTable::clone_readable_table(fresh_probe_state=true)` on the steal path: a
   thief snapshotting a peer partition's live table shares only the immutable built
   `_table_items` and gets a fresh probe state, never a copy of the peer's live,
   concurrently-mutated `HashTableProbeState`.
4. The fresh probe state is prepared via `peer_prober->reset_probe(state)` before first
   use (else SIGSEGV on uninitialised probe scratch).

## 5b. Partition-aware output safety — the put-back gate

Partition-aware steal moves *processing* of a partition-H chunk to a thief on lane T, but
the join output is emitted on lane T. Correctness therefore depends on **how that output
reaches partition-sensitive consumers**:

- **Case 1 — a redistribution exists downstream (safe, put-back is automatic).** If the
  probe pipeline's sink re-partitions by a key (hash exchange) or gathers / is
  partition-agnostic, then lane identity is broken and re-derived from the row data: the
  thief's partition-H rows are re-hashed to the correct destination regardless of which
  lane produced them. The downstream exchange *is* the put-back — no explicit routing, no
  tagging. (Our validated `count/sum` plan is this case: probe → gather local exchange →
  single global aggregate.)
- **Case 2 — a co-located per-lane stateful build with no redistribution (unsafe).** If
  the probe output feeds a one-phase aggregate / sort / analytic build directly (same
  driver, `lane L == partition L`), a key's rows split between the owner lane and the
  thief lane are aggregated into two per-lane hash tables and never merged → wrong result.
  There is no exchange to put them back, and injecting into the owner's build concurrently
  would be a data race. **Decision: disable partition-aware steal for this case.**

### Gate

Add a sink predicate `breaks_partition_identity()` (default `false`):
- `LocalExchangeSinkOperator` → `exchanger->source_partition_free() ||
  exchanger->is_hash_partitioned()` (gather / passthrough-agnostic / hash-repartition all
  break or re-derive lane identity — Case 1).
- `ExchangeSinkOperator` (network) → partition type `HASH_PARTITIONED` / `UNPARTITIONED` /
  `RANDOM` (all redistribute or gather — Case 1).
- Default (aggregate / sort / analytic build, and any identity-preserving sink) → `false`
  → Case 2.

Partition-aware steal (`steal_enabled()`'s partition branch, alongside the existing
`_stolen_input_operator() != nullptr`) additionally requires
`sink_operator()->breaks_partition_identity()`. The sink is fixed per pipeline, so compute
once (like the steal barrier). Conservative by construction: any sink we cannot prove
redistributes/gathers → treated as Case 2 → steal disabled (safe, at worst lost ROI).

This makes partition-aware steal correct for arbitrary downstream plans: Case 1 relies on
the existing downstream redistribution to put the stolen output back on the right
partition; Case 2 is disabled.

## 6. Lost-wakeup / correctness argument for the notification

- The thief transitions blocked→ready only via `try_schedule` from `steal_trigger`
  (steal path) or the normal source/sink triggers (own-lane path). Both go through the
  same `_active_event`/`_update` latch, so a trigger racing the park is latched and
  re-evaluated, not lost.
- Register-then-recheck (§3.4) covers the enqueue/register race.
- Termination (§3.5) covers "backlog drained, senders finishing" — waiters are woken to
  observe global finish, so no thief parks forever.
- The steal wake never fires spuriously with no work (it is gated on the just-written
  lane's `size_approx() >= threshold`), so a woken thief that finds nothing (rare, lost a
  race to another thief) simply re-parks + re-registers — bounded, not a spin.

## 7. Validation plan

1. Build BE (remote profile `worksteal`).
2. Correctness: 32M, 90%-skew PARTITIONED `[SHUFFLE]` join, dop=16, threshold=1.
   `enable_pipeline_work_stealing` off vs on **byte-identical** across ≥8 repeats
   (count/sum/sum), no loss, no crash. Repeat the ON run (a single ON pass can pass by
   luck — high volume + repetition is what caught the earlier races).
3. Performance: interleaved off/on ×8; expect ON ≈ 0.4× of OFF wall (~2.7×), i.e. the
   busy-poll ROI retained.
4. Regression guard: a **non-skew** shuffle join (even partitions) with the flag ON must
   not regress vs OFF — the specific thing the busy-poll could hurt and this design is
   meant to fix. Watch CPU / scheduler-active time, not just wall.
5. Confirm steal still fires (StealSuccess > 0) and that parked thieves consume ~0 CPU
   (no busy-spin) via driver ScheduleCount / active-vs-idle timers.

## 8. Risks / open questions

- Registry vs Observable: `Observable::add_observer` is prepare-phase-only
  (non-thread-safe), so the dynamic steal-waiter registry is a separate lock-free array,
  not the existing `_observable`. Confirmed necessary.
- Trigger frequency: `steal_trigger` fires per `add_chunks` batch that crosses threshold;
  a woken thief burst-drains, so churn is per-burst, not per-chunk. Confirm empirically
  (step 3/5).
- Threshold semantics: reuse `pipeline_steal_backlog_threshold`; a lane at exactly the
  boundary may flap — acceptable (it just means an extra wake/recheck).
- `remove_sender`/`cancel_stream` must fire the steal wake too (not only add_chunks), or
  a thief parked when the last batch already arrived (no further add_chunks) would miss
  termination — see §3.5.
