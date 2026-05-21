# Column COW Full Refactor

- Status: active
- Owner: Storage / Execution
- Last Updated: 2026-05-20

## Summary

The `Cow<Column>` framework in `be/src/common/cow.h` is sound and
roughly 1:1 with ClickHouse's `COW<IColumn>`. The reason it does not
actually save copies today is structural: the `Column` and `Chunk`
APIs are built around **borrow-style mutable access**
(`get_column_raw_ptr_by_*`, `as_mutable_raw_ptr`, in-place `filter` /
`update_rows`), which leaves the Chunk's `ColumnPtr` ref alive while
the underlying object is being written. With the slot ref still held,
`use_count > 1` is the rule rather than the exception, so any
`try_mutate()` that *is* invoked degrades to a deep clone. The result
is that 277 `as_mutable_*` call sites and 328 `Chunk` raw-pointer
accessors collectively bypass COW, while only 87 `mutate()` and 6
`try_mutate()` sites exercise it.

The fix is to switch StarRocks from borrow-style to
**ownership-transfer-style** mutable access, the way ClickHouse's
`Chunk::mutateColumns()` / `setColumns()` and
`IColumn::mutate(std::move(slot.column))` work. Concretely: getting a
mutable column always moves the `ColumnPtr` out of its slot, so the
slot's ref is gone before any write; writes happen on a
`MutableColumnPtr` whose `use_count` is 1 by construction; the result
is put back via an explicit `set_columns` / RAII handle. Under this
invariant `try_mutate()` becomes a free shadow clone in the common
case, and a deep clone only when the column is genuinely shared with
another chunk or operator — which is the behavior the framework was
designed for from day one.

This plan is the umbrella refactor that retires
`as_mutable_raw_ptr` / `as_mutable_ptr` / `as_mutable_ref`, retires
the four `Chunk::get_column_raw_ptr_by_*` accessors, folds
`MutableChunk` back into `Chunk`, and reshapes the `Column` mutation
surface to match. The previously committed plan
[`2026-05-20-cow-chunk-boundary.md`](2026-05-20-cow-chunk-boundary.md)
remains valid as the cautious first slice of this work
(diagnostic + boundary split) and is superseded scope-wise by this
document.

## Terminal API Shape

When the refactor is done, the column / chunk mutation surface is
exactly four entry points plus a tightened `Column` contract.

### Chunk entry points

```cpp
// Read-only.
const ColumnPtr& column_at(SlotId) const;
const ColumnPtr& column_at(size_t idx) const;
const Columns&   columns() const;

// Take ownership of one slot and write. RAII; on destruction the slot
// receives the (possibly cloned) MutableColumnPtr back as ColumnPtr.
// Move-only handle; nodiscard.
[[nodiscard]] ColumnHandle take_mut(SlotId);
[[nodiscard]] ColumnHandle take_mut(size_t idx);

// Take ownership of all slots at once. Chunk becomes a hollow shell
// (slots present, columns null) until set_columns is called.
[[nodiscard]] MutableColumns take_mutable_columns();

// Put columns back. Mandatory pair for take_mutable_columns.
void set_columns(MutableColumns columns);
```

The non-const `get_column_raw_ptr_by_name / _index / _id / _slot_id`
overloads are deleted. The const versions returning `const Column*`
stay.

### Column contract

- Mutating methods are **only** the ones a builder / scan / accumulator
  needs: `append*`, `insert_from`, `insert_range_from`, `reserve`,
  `resize`, `resize_uninitialized`, `reset_column`, `swap_column`.
  These are non-const and may only be called through
  `MutableColumnPtr`.
- Transform methods are **const and return a new `MutableColumnPtr`**:
  `filter`, `filter_range`, `permute`, `replicate`, `cut`,
  `convertToFullColumnIfConst`. The current in-place `filter` /
  `filter_range` overloads are deleted.
- `update_rows`, `remove_first_n_values`, `update_has_null`,
  `assign`, `fill_default` are reclassified case by case in Phase 4;
  most should become const + return new, the few hot ones become
  member functions of `MutableColumnPtr`'s underlying type with a
  documented `COW-EXEMPT` rationale.
- `as_mutable_raw_ptr` / `as_mutable_ptr` / `as_mutable_ref` on `Cow`
  are deleted. The `Column::mutate() const&&` and
  `Column::mutate(Ptr)` static remain as the single legitimate path
  for the rare "I have a `ColumnPtr` outside a Chunk and need to
  write" case.

### Single invariant

> At any point in time, code holding a writable handle to a column
> guarantees the chunk's slot for that column does **not** also hold a
> ref to it. Equivalently: `use_count == 1` whenever a write occurs.

Every API in the terminal shape preserves this invariant by
construction. `take_mut` and `take_mutable_columns` move the ref out
of the slot before returning the handle; the matching `set_columns`
or RAII destructor returns ownership to the slot only after writes
finish. There is no operation that simultaneously yields a writable
column and leaves the slot referencing it.

## Migration Phases

### Phase 0 — Diagnostic baseline (no behavior change, ≈1 week)

Already partially specified in
[`2026-05-20-cow-chunk-boundary.md`](2026-05-20-cow-chunk-boundary.md).
Goal: produce a per-call-site inventory of where today's borrow-style
access happens with `use_count > 1`. Inputs:
`cow_optimization_diagnose_level=1` plus temporary `LOG(INFO)` on the
four `Chunk::get_column_raw_ptr_by_*` accessors. Outputs: report under
`handbook/plans/local/active/cow-full-refactor/`.

This phase blocks nothing; Phase 1 can start in parallel.

### Phase 1 — Introduce the new Chunk surface (≈2 weeks)

- Implement `Chunk::take_mut(slot)`, `take_mut(idx)`,
  `take_mutable_columns()`, `set_columns()`, `column_at()` in
  `be/src/column/chunk.{h,cpp}`.
- `take_mut` returns a `ColumnHandle` defined alongside `Chunk`:
  move-only, `[[nodiscard]]`, holds the slot's index and the
  `MutableColumnPtr`. On destruction it writes back, on commit it can
  hand off a different `MutableColumnPtr` (for transform-returns-new
  scenarios).
- Old `get_column_raw_ptr_by_*` non-const overloads remain, marked
  `[[deprecated("use take_mut(...)")]]`. The four `TODO(COW)` comments
  stay until Phase 5.
- Unit tests in `column_test` and `chunk_test` cover:
  shadow clone when slot is the only ref; deep clone when a copy of
  the ColumnPtr is held elsewhere; exception safety of the handle
  (slot is restored on exception); recursive `mutate_each_subcolumn`
  through Nullable / Array / Map / Struct.

### Phase 2 — Reshape Column transform methods (≈3 weeks)

- Add const overloads of `filter`, `filter_range`, `permute`,
  `replicate`, `cut` that return `MutableColumnPtr`. Implementation
  reuses the in-place version on a freshly-cloned mutable column,
  guaranteeing `use_count == 1` inside.
- Existing in-place overloads stay temporarily, marked `[[deprecated]]`.
- Migrate the obvious callers (predicate pushdown, runtime filter
  application, sort key extraction) to the const-returning form.
- `column_test` gains parameterized tests proving const transform is
  semantically equivalent to in-place transform across all column
  subtypes (BinaryColumn, FixedLengthColumn variants, NullableColumn,
  ConstColumn, ArrayColumn, MapColumn, StructColumn, JsonColumn,
  ObjectColumn).

### Phase 3 — Migrate call sites in priority order (≈6 weeks)

Migration is batched by directory so each PR is reviewable.
Approximate sizes from the current grep:

| Batch | Source | Approx sites | Migration mode |
|---|---|---|---|
| 3a | `exec/schema_scanner/` | 50 slot_id + builders | Direct to `take_mutable_columns()` — these own the chunk |
| 3b | `exec/pipeline/`, `exec/sorting/`, `exec/aggregator*` | ~80 | Mix of `take_mut` per slot and full take/set |
| 3c | `exec/file_scanner/`, `formats/orc`, `formats/avro`, `formats/parquet` | ~20 | `take_mutable_columns()` (scan output) |
| 3d | `exprs/` (lambdas, array_sort, map_apply) | ~10 | `take_mut` per call |
| 3e | `connector/`, `storage/meta_reader.cpp`, misc | ~10 | Case by case |
| 3f | Remaining `as_mutable_raw_ptr` outside Chunk path | 277 sites | Audit; most are inside Column subclass impls (`*_column.cpp`) and stay internal |

For each batch:
1. Replace `get_column_raw_ptr_by_*` with `take_mut` /
   `take_mutable_columns` / `column_at`.
2. Replace in-place `filter` with const transform + assignment.
3. Delete any local `const_cast<Column*>` that survives.

### Phase 4 — Retire in-place hotspots (≈4 weeks)

The known dangerous in-place paths get explicit treatment, not blanket
conversion:

| Path | Decision |
|---|---|
| Primary-key partial update (`rowset_column_update_state.cpp` + `Chunk::update_rows` + `BinaryColumnBase::update_rows`) | Wrap entry with `take_mutable_columns()` once per chunk; internal `update_rows` stays in-place; document as `COW-EXEMPT` because the chunk is exclusively owned by the update pipeline |
| `Chunk::filter_range` | Becomes a Chunk-level helper that performs `take_mutable_columns()` → per-column const filter → `set_columns()` |
| Sort / aggregate / window `remove_first_n_values`, `reset_column` | Caller restructures to take the column out for the duration of the operator window; documented invariant: operator-private columns |
| Dictionary decoding (`binary_dict_page.cpp`) | Already safe (decoder owns destination column); add `DCHECK(col->use_count() == 1)` and `COW-EXEMPT` |
| `update_has_null`, `update_rows` static-shape variants | Move to `MutableColumn*` member set; require `MutableColumnPtr` to invoke |

### Phase 5 — Lock in (≈1 week)

- Delete deprecated APIs: `as_mutable_raw_ptr`, `as_mutable_ptr`,
  `as_mutable_ref`, non-const `get_column_raw_ptr_by_*`, the four
  `TODO(COW)` comments, in-place `filter` / `filter_range` /
  `permute` / `replicate` / `cut` overloads.
- Fold `MutableChunk` into `Chunk`. Its construction-phase usage
  pattern (`MutableColumns` held for the lifetime of a builder) is now
  expressible as `Chunk` + `take_mutable_columns()` held until commit;
  there is no remaining justification for two types. Migration helper:
  `MutableChunk` becomes a thin `using MutableChunk = Chunk;` typedef
  for one release, then deleted.
- Default `cow_optimization_diagnose_level=1` for `Debug` and `ASAN`
  builds in `be/src/common/config.h`. Treat any remaining warning at
  diagnose level 1 as a UT failure in `column_cow_test`.
- Remove `enable_cow_optimization` config — at this point COW is the
  only path, not an optimization.

## Acceptance Criteria

- `grep -rn "as_mutable_raw_ptr\|as_mutable_ptr\|as_mutable_ref" be/src/`
  returns 0 hits.
- `grep -rn "get_column_raw_ptr_by_" be/src/` returns 0 hits.
- `grep -rn "const_cast<Column" be/src/column/` returns 0 hits.
- `grep -rn "TODO(COW)" be/src/` returns 0 hits.
- `MutableChunk` is deleted (or is a `using` alias scheduled for
  deletion in the next release).
- `column_cow_test` exists and covers shadow/deep clone, recursive
  `mutate_each_subcolumn`, exception safety, and one regression case
  per Column subclass.
- `cow_optimization_diagnose_level=1` produces zero warnings during
  the BE UT suite.
- Benchmarks `column_test --gtest_filter=*Bench*`,
  `runtime_core_test`, and the standard TPCH SF1 micro suite are
  within ±2% of the pre-refactor baseline.

## Non-Goals

- Touching `cow.h`'s reference-counting implementation. Intrusive
  atomic `_use_count` stays.
- Introducing Velox-style dictionary / constant wrap layers to defer
  materialization. That is a separate, larger plan that this refactor
  enables but does not include.
- Reworking `ColumnHelper`, `ColumnViewer`, `ColumnBuilder`. They
  ride along because they consume `MutableColumnPtr` after the
  refactor, but their public shape is unchanged.
- Changing the on-disk format, RPC wire types, or the
  Frontend-Backend protocol.
- Changing JIT / vectorized kernel signatures. Kernels keep operating
  on raw `Column*` arguments; the caller is responsible for ensuring
  those pointers come from a `MutableColumnPtr` it owns.

## Risks and Mitigations

- **Behavior change in `filter` / `filter_range`** (in-place → returns
  new). Callers that used the in-place form and ignored the return
  silently lose their filter. Mitigation: the in-place overloads are
  marked `[[deprecated]]` in Phase 2, deleted in Phase 5, with
  matching `column_test` proving equivalence; Phase 3 migration walks
  every caller.
- **Atomic `use_count` load on every `take_mut`.** Relaxed atomic
  load on uncontended cache lines is a few cycles, but the operator
  hot loops call it once per row in pathological cases. Mitigation:
  `take_mutable_columns()` once per chunk amortizes; `take_mut` is
  intended for once-per-operator-output-batch, not per-row. The
  Phase 0 diagnostic identifies any per-row regression before it
  ships.
- **600+ call site migration is noisy.** Mitigation: batches in
  Phase 3 are scoped by top-level directory so each PR is reviewable
  and individually revertable.
- **Hidden writes through `column_at()` const + `const_cast`.**
  Reviewers must reject any new `const_cast<Column*>(chunk.column_at(...))`
  pattern. A `clang-tidy` rule
  (`misc-const-correctness`) is added in Phase 5 to enforce
  mechanically.
- **Cross-thread sharing during pipeline parallelism.** A column
  shared across drivers must trigger deep clone, not shadow clone.
  Mitigation: this is exactly what `Cow::try_mutate()` already does;
  the refactor does not change concurrency semantics.

## Decision Log

- 2026-05-20: Adopt ClickHouse-style ownership-transfer access as the
  terminal API. `Chunk::take_mut` + `take_mutable_columns` +
  `set_columns` is the smallest API surface that makes the
  "`use_count == 1` at write" invariant hold by construction.
- 2026-05-20: Move `Column::filter` / `filter_range` / `permute` /
  `replicate` to const + return new rather than keeping them in-place
  behind a take-mut handle. The cost (one extra allocation per
  transform) is bounded by `try_mutate()`'s shadow-clone fast path,
  and the API symmetry with ClickHouse outweighs the per-call cost.
- 2026-05-20: Fold `MutableChunk` into `Chunk` rather than keeping
  them as parallel types. With `take_mutable_columns()` /
  `set_columns()` available on `Chunk`, the "everything mutable, hand
  back at the end" pattern that `MutableChunk` exists to express has
  a first-class shape.
- 2026-05-20: Retire `enable_cow_optimization` config at the end of
  Phase 5. A correctness-critical invariant is not an optimization;
  keeping a kill switch around invites regression.
- 2026-05-20: Keep `Column::mutate() const&&` and
  `Column::mutate(Ptr)` static. They remain the path for code that
  holds a `ColumnPtr` outside a Chunk (cache, intermediate result,
  spill).
- 2026-05-20: Treat the existing
  [`2026-05-20-cow-chunk-boundary.md`](2026-05-20-cow-chunk-boundary.md)
  plan as the cautious first slice (≈ Phase 0 + a degenerate Phase 1
  that keeps both APIs side by side). This umbrella plan supersedes
  its terminal vision; if both are followed they converge.
