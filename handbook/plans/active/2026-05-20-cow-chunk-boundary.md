# Chunk COW Boundary Tightening

- Status: active
- Owner: Storage / Execution
- Last Updated: 2026-05-20

## Summary

StarRocks already ships a ClickHouse-inspired clone-on-write framework
(`be/src/common/cow.h`) and integrates it into `Column`
(`be/src/column/column.h`: `Column::mutate()` / `Cow::try_mutate()`).
`ColumnPtr` (`ImmutPtr<Column>`) and `MutableColumnPtr` (`MutPtr<Column>`)
provide the immutable/mutable type split, and the `enable_cow_optimization`
config gates the runtime behavior. The infrastructure exists; what is
missing is enforcement at the boundary where most mutations actually
happen — `Chunk`.

Today every operator that wants to write into a column reaches through
one of the four `Chunk::get_column_raw_ptr_by_*` accessors. All four
implementations `const_cast` the underlying `ColumnPtr` and return a raw
`Column*`, each annotated with `// TODO(COW): return a mutable column
raw pointer to be compatible with the old codes`. As a result the COW
ref-count is never consulted at the mutation site, and a shared
`ColumnPtr` can be silently mutated through this back door. There are
~328 call sites of these four accessors today (5 / 75 / 19 / 229 for
name / index / id / slot_id respectively) and the heavy users
(`exec/schema_scanner`, exec operators, exprs/lambdas, formats, storage
metadata reader, connectors) all rely on this implicit mutability.

The goal of this plan is to make the mutation boundary at `Chunk`
explicit and COW-aware without changing operator semantics. The split
gives callers two clearly-named accessors (`mutable_column_*` vs.
`column_*`), routes the mutable path through `try_mutate()` so that
shared columns are cloned on first write, and retires the four
`TODO(COW)` accessors. Operators that already own their chunk should be
migrated to `MutableChunk`, which carries `MutableColumnPtr` end-to-end
and bypasses the per-access ref-count check entirely.

Scope is intentionally narrow: this plan does **not** rewrite the
high-frequency in-place mutation hotspots (primary-key partial column
updates, `Chunk::filter_range`, sort / aggregate / window
`remove_first_n_values` / `reset_column`). Those land in a follow-up
plan once the boundary is in place and the diagnostics from phase A
have produced a concrete inventory.

## Acceptance Criteria

### Phase A — Diagnostic baseline (no behavior change)

- A short report listing every call site at which a `Chunk` raw-ptr
  accessor is invoked on a column whose `use_count() > 1`. The report
  is produced by running `column_test`, `chunk_test`,
  `runtime_core_test`, `exec_core_test`, `exec_sorting_core_test`,
  `exec_join_core_test`, and the existing schema scanner / storage UT
  binaries with `cow_optimization_diagnose_level=1` and aggregating
  the warning lines that come out of `as_mutable_raw_ptr`.
- A temporary log hook on each of the four `Chunk::get_column_raw_ptr_by_*`
  accessors (gated behind the same diagnose level) that records the
  caller's `use_count`. The hook is removed at the end of phase D and
  must not be enabled in release builds.
- The report is committed under `handbook/plans/local/active/cow-chunk-boundary/`
  (gitignored) so the inventory can be iterated on without churning
  tracked history.

### Phase B — Boundary split on `Chunk`

- `Chunk` gains two accessor families with non-overlapping semantics,
  next to the existing const accessors:
  - `mutable_column_by_name / _by_index / _by_id / _by_slot_id` —
    returns `Column*` after invoking `try_mutate()` on the underlying
    `ColumnPtr` slot. The slot is rewritten with the mutate result so
    subsequent reads observe the deep clone when one was required.
  - `column_by_name / _by_index / _by_id / _by_slot_id` — returns
    `const Column*` only. These already exist with the
    `const`-qualified overloads; the plan keeps their signature and
    simply removes the ambiguous non-const overloads that return a
    mutable raw pointer.
- The four `// TODO(COW)` comments in `be/src/column/chunk.h` are
  deleted. The non-const `get_column_raw_ptr_by_*` overloads are
  removed; `column_test` and `chunk_test` cover the new accessor
  contract.
- A static_assert / type-level check rejects calling
  `mutable_column_by_*` on a `const Chunk&`.

### Phase C — Call-site migration

- Every former call site of `Chunk::get_column_raw_ptr_by_*` is
  reclassified as one of:
  1. Read-only — migrates to `column_by_*` and stops paying any COW
     cost.
  2. Genuinely mutating — migrates to `mutable_column_by_*` so the
     `try_mutate()` is applied exactly once per access, at the boundary.
  3. Builder / scan output that owns the chunk — migrates to
     `MutableChunk` end-to-end. `exec/schema_scanner/*` is the largest
     batch (50 of the 229 `slot_id` sites) and is a natural fit.
- Connector reader and `formats/orc`, `formats/avro` outputs are
  audited to confirm the columns they fill are freshly allocated
  (use_count == 1) — these stay on the mutable accessor without
  needing `MutableChunk`.
- After migration, a grep for `const_cast<Column` in
  `be/src/column/chunk.{h,cpp}` returns zero hits.

### Phase D — Lock in the boundary

- `as_mutable_raw_ptr` / `as_mutable_ptr` / `as_mutable_ref` on `Cow`
  are marked with a deprecation attribute that can be silenced per
  call site for the known exceptions (primary-key partial update,
  sort/aggregate hotspots), each tagged with a `// COW-EXEMPT:
  <reason>` comment so the next plan can find them mechanically.
- The default `cow_optimization_diagnose_level` for `Debug` and `ASAN`
  builds is set to 1 via `be/src/common/config.h`; release default
  remains 0. A handful of UT failures expected from this change are
  treated as bugs and fixed under this plan.
- A new `column_cow_test` target exercises:
  - shadow clone when `use_count == 1`
  - deep clone when `use_count > 1`
  - recursion through `mutate_each_subcolumn` for
    `NullableColumn`, `ArrayColumn`, `MapColumn`, `StructColumn`,
    `JsonColumn`, `BinaryColumn` (the `BinaryColumnBase::update_rows`
    equal-length fast path is exercised explicitly)

## Non-Goals

- Rewriting `Chunk::update_rows`, `Chunk::filter_range`, or
  `BinaryColumnBase::update_rows` to be COW-aware on the inside —
  tracked separately once the phase A inventory exists.
- Touching the `ColumnHelper` / `ColumnViewer` / `ColumnBuilder`
  utility layer.
- Changing `ColumnPtr` from intrusive ref-count to anything else.
- Performance work on the atomic `use_count` load. Phase B accepts one
  extra relaxed atomic load per `mutable_column_by_*` call. If this
  shows up in profiles, an explicit "I already cloned, hand me the raw"
  fast path can be added later.

## Risks and Mitigations

- **Hidden writes through `column_by_*` const overload + cast**:
  reviewers must reject any new `const_cast<Column*>(chunk.column_by_*())`
  pattern. A lint rule can be added in a follow-up.
- **MutableChunk transitions are noisy in diffs**: phase C is sliced
  per top-level directory (`exec/schema_scanner` first as the largest
  homogeneous block) to keep each PR reviewable.
- **Debug-only diagnose flips UT colors**: the diagnostic is a
  `LOG(WARNING)`, not a failure, so it won't break CI; failures only
  surface if `column_cow_test` asserts on a specific scenario.

## Decision Log

- 2026-05-20: Pick "lock down the `Chunk` boundary" over "retrofit COW
  into every mutation hotspot first". The boundary fix is the smallest
  diff that turns the existing `Cow<Column>` infrastructure from
  opt-in into the default path; hotspot retrofits become straightforward
  once they go through the boundary.
- 2026-05-20: Keep `as_mutable_raw_ptr` available behind a deprecation
  attribute rather than deleting it outright. The primary-key partial
  update and sort/aggregate hotspots legitimately need the no-refcount
  fast path; the plan codifies them as `COW-EXEMPT` instead of forcing
  a same-PR rewrite.
- 2026-05-20: Prioritize correctness over performance for phase B/C.
  One relaxed atomic load per `mutable_column_*` access is accepted
  cost; profile-driven optimizations are out of scope until the
  boundary is in place.
- 2026-05-20: Diagnostic output is checkout-local
  (`handbook/plans/local/active/`) so successive runs do not churn
  tracked plan history.
