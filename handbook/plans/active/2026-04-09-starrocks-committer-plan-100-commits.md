# StarRocks Committer Plan — 100+ Commits Roadmap

- Status: active
- Owner: contributor
- Last Updated: 2026-04-09

## Goal

Become a StarRocks Committer within 3–4 months by contributing **100+ high-quality commits** across documentation, testing, bug fixes, enhancements, and features. The plan is structured in 5 phases, progressing from low-risk familiarity-building contributions to high-impact architectural work.

## Strategy Overview

| Phase | Timeline | Commits | Focus | Risk |
|-------|----------|---------|-------|------|
| 1 — Warmup | Week 1–2 | 15–20 | Docs, typos, config sync | Very Low |
| 2 — Testing | Week 3–5 | 20–25 | Unit tests, SQL tests, test infra | Low |
| 3 — Bug Fixes | Week 6–9 | 20–25 | TODO items, edge cases, null handling | Medium |
| 4 — Enhancements | Week 10–13 | 20–25 | Optimizer rules, functions, connectors | Medium–High |
| 5 — Features | Week 14–16 | 15–20 | New features, architectural improvements | High |

---

## Phase 1: Warmup — Documentation & Housekeeping (15–20 commits)

**Goal**: Build familiarity with the codebase, CI pipeline, and PR workflow. Establish yourself as a reliable contributor.

### 1.1 Documentation Fixes (8–10 commits)

| # | Task | Type | Files |
|---|------|------|-------|
| 1 | Add missing "Example" sections to 8+ SQL function docs | [Doc] | `docs/en/sql-reference/sql-functions/string-functions/character_length.md`, `seconds_add.md`, `years_add.md`, `adddate.md`, etc. |
| 2 | Sync Chinese docs (`docs/zh/`) with English for recently added functions | [Doc] | `docs/zh/sql-reference/sql-functions/` |
| 3 | Add NULL handling behavior documentation to aggregate functions | [Doc] | `docs/en/sql-reference/sql-functions/aggregate-functions/` |
| 4 | Document edge cases and type coercion rules for date/time functions | [Doc] | `docs/en/sql-reference/sql-functions/date-time-functions/` |
| 5 | Complete TBC sections in CONTRIBUTING.md (code structure, directories, coding style) | [Doc] | `CONTRIBUTING.md` |
| 6 | Add performance tips and best-practice examples to window function docs | [Doc] | `docs/en/sql-reference/sql-functions/Window_function.md` |
| 7 | Improve FE configuration docs — add missing parameters from `Config.java` | [Doc] | `docs/en/administration/management/FE_configuration.md`, `Config.java` |
| 8 | Improve BE configuration docs — add missing parameters from `config.h` | [Doc] | `docs/en/administration/management/BE_configuration.md`, `config.h` |

### 1.2 Code Housekeeping (5–8 commits)

| # | Task | Type | Files |
|---|------|------|-------|
| 9 | Fix typos and grammar in code comments (BE) | [Doc] | `be/src/` various files |
| 10 | Fix typos and grammar in code comments (FE) | [Doc] | `fe/fe-core/src/` various files |
| 11 | Remove dead imports and unused variables in FE | [Refactor] | `fe/fe-core/src/main/java/` |
| 12 | Standardize log message formats in FE connectors | [Refactor] | `fe/connector/` |
| 13 | Update outdated comments referencing old class/method names | [Refactor] | Various |

### 1.3 Metrics & Monitoring (2–3 commits)

| # | Task | Type | Files |
|---|------|------|-------|
| 14 | Sync FE metrics docs with actual registered metrics | [Doc] | `docs/en/administration/management/monitoring/metrics.md` |
| 15 | Add missing metric descriptions and labels to monitoring docs | [Doc] | Same as above |

---

## Phase 2: Testing — Build Domain Knowledge (20–25 commits)

**Goal**: Deeply understand query execution, optimization, and storage through test writing. Build trust with reviewers.

### 2.1 FE Unit Tests (8–10 commits)

| # | Task | Type | Target |
|---|------|------|--------|
| 16 | Add plan tests for ASOF JOIN edge cases (NULL keys, boundary conditions) | [UT] | `fe/fe-core/src/test/java/com/starrocks/sql/plan/` |
| 17 | Add analyzer tests for complex subquery correlation | [UT] | `com.starrocks.sql.analyzer` test package |
| 18 | Add tests for MV rewrite with multi-table joins | [UT] | `com.starrocks.sql.optimizer.rule.transformation.materialization` |
| 19 | Add tests for SchemaChangeHandler — ALTER TABLE edge cases | [UT] | `com.starrocks.alter.SchemaChangeHandlerTest` |
| 20 | Add tests for privilege checking in DDL operations | [UT] | `com.starrocks.privilege` test package |
| 21 | Add tests for connector metadata refresh edge cases | [UT] | `com.starrocks.connector` test package |
| 22 | Add plan tests for window function with different frame types | [UT] | Plan test classes |
| 23 | Add tests for JSON functions with deeply nested structures | [UT] | SQL function test classes |
| 24 | Add tests for decimal overflow/underflow in arithmetic | [UT] | Expression evaluation tests |

### 2.2 SQL Integration Tests (8–10 commits)

| # | Task | Type | Target |
|---|------|------|--------|
| 25 | Add integration tests for PIVOT/UNPIVOT operations | [UT] | `test/sql/test_pivot/` |
| 26 | Add integration tests for window functions with RANGE frames | [UT] | `test/sql/test_window_function/` |
| 27 | Add integration tests for complex CTE (recursive, nested) | [UT] | `test/sql/test_cte/` |
| 28 | Add integration tests for partial update with generated columns | [UT] | `test/sql/test_partial_update/` |
| 29 | Add integration tests for cross-database queries | [UT] | `test/sql/test_cross_db/` |
| 30 | Add integration tests for INSERT OVERWRITE with partition expressions | [UT] | `test/sql/test_insert_overwrite/` |
| 31 | Add integration tests for bitmap/HLL functions edge cases | [UT] | `test/sql/test_bitmap/` |
| 32 | Add integration tests for large IN-list pruning | [UT] | `test/sql/test_predicate/` |

### 2.3 BE Unit Tests (4–5 commits)

| # | Task | Type | Target |
|---|------|------|--------|
| 33 | Add unit tests for hash join probe with NULL keys | [UT] | `be/test/exec/` |
| 34 | Add unit tests for chunks_sorter with spill edge cases | [UT] | `be/test/exec/` |
| 35 | Add unit tests for column encoding/decoding roundtrip | [UT] | `be/test/column/` |
| 36 | Add unit tests for runtime filter with different types | [UT] | `be/test/exec/` |
| 37 | Add unit tests for VARBINARY column operations | [UT] | `be/test/column/` |

---

## Phase 3: Bug Fixes — Resolve Technical Debt (20–25 commits)

**Goal**: Demonstrate ability to diagnose and fix real issues. Work through TODO/FIXME backlog.

### 3.1 FE Bug Fixes (10–12 commits)

| # | Task | Type | Target |
|---|------|------|--------|
| 38 | Fix type mismatch for map literal construction | [BugFix] | FE type system (`TODO` in codebase) |
| 39 | Fix missing privilege check in certain DDL paths | [BugFix] | `com.starrocks.privilege` |
| 40 | Fix incorrect cost estimation for skewed joins | [BugFix] | `com.starrocks.sql.optimizer.cost` |
| 41 | Fix MV refresh failure with certain partition types | [BugFix] | `com.starrocks.scheduler` |
| 42 | Fix incorrect NULL handling in CASE WHEN expressions | [BugFix] | Analyzer/Optimizer |
| 43 | Fix SessionVariable validation for out-of-range values | [BugFix] | `SessionVariable.java` |
| 44 | Fix connector metadata cache invalidation race condition | [BugFix] | `com.starrocks.connector` |
| 45 | Fix incorrect column statistics for complex expressions | [BugFix] | `com.starrocks.sql.optimizer.statistics` |
| 46 | Fix error message improvement for unsupported operations | [BugFix] | Various analyzers |
| 47 | Add proper exception handling replacing broad `catch Exception` | [BugFix] | 40+ files identified |
| 48 | Fix OlapTable null pointer on partition access | [BugFix] | `OlapTable.java` |
| 49 | Fix DELETE statement with complex subquery predicates | [BugFix] | `com.starrocks.sql.analyzer` |

### 3.2 BE Bug Fixes (8–10 commits)

| # | Task | Type | Target |
|---|------|------|--------|
| 50 | Fix DATE column casting edge case in olap_scan_prepare | [BugFix] | `be/src/exec/olap_scan_prepare.cpp:102` |
| 51 | Fix NULL normalization in predicate extraction | [BugFix] | `be/src/exec/olap_scan_prepare.cpp:526` |
| 52 | Fix limit handling in olap_scan_node | [BugFix] | `be/src/exec/olap_scan_node.cpp:170` |
| 53 | Fix memory leak in JSON scanner error path | [BugFix] | `be/src/exec/file_scanner/json_scanner.cpp` |
| 54 | Replace raw pointers with smart pointers in tablet_sink | [BugFix] | `be/src/exec/tablet_sink.cpp` |
| 55 | Fix race condition in fragment-level MemPool | [BugFix] | `be/src/runtime/` |
| 56 | Fix incorrect chunk size estimation in spill path | [BugFix] | `be/src/exec/chunks_sorter.cpp` |
| 57 | Fix missing error status propagation in pipeline operators | [BugFix] | `be/src/exec/pipeline/` |
| 58 | Fix constexpr optimization for has_null_column | [BugFix] | `be/src/exec/` (TODO item) |

### 3.3 GitHub Issues Triage (3–5 commits)

| # | Task | Type | Target |
|---|------|------|--------|
| 59 | Pick and fix 3–5 issues labeled `good-first-issue` from GitHub | [BugFix] | Various |
| 60 | Reproduce and fix community-reported SQL correctness issues | [BugFix] | Various |

---

## Phase 4: Enhancements — Meaningful Improvements (20–25 commits)

**Goal**: Show architectural understanding. Contribute features reviewers care about.

### 4.1 Optimizer Enhancements (6–8 commits)

| # | Task | Type | Target |
|---|------|------|--------|
| 61 | Add new rewrite rule: constant folding for CASE WHEN | [Enhancement] | `com.starrocks.sql.optimizer.rule.transformation` |
| 62 | Improve predicate pushdown through UNION ALL | [Enhancement] | Optimizer rules |
| 63 | Add statistics derivation for string functions | [Enhancement] | `com.starrocks.sql.optimizer.statistics` |
| 64 | Optimize join order for star schema queries | [Enhancement] | `com.starrocks.sql.optimizer.rule.join` |
| 65 | Improve cost model for multi-stage aggregation | [Enhancement] | Cost model classes |
| 66 | Add partition pruning for IN-list with expressions | [Enhancement] | Partition pruning logic |
| 67 | Improve MV candidate selection with cost-based ranking | [Enhancement] | MV rewrite rules |

### 4.2 New Scalar/Aggregate Functions (5–6 commits)

| # | Task | Type | Target |
|---|------|------|--------|
| 68 | Add `array_distinct_agg` aggregate function | [Feature] | FE + BE + docs + tests |
| 69 | Add `map_keys_sorted` / `map_values_sorted` functions | [Feature] | FE + BE + docs + tests |
| 70 | Add `string_agg` with ORDER BY support | [Enhancement] | FE + BE + docs + tests |
| 71 | Add `date_diff` with configurable units | [Enhancement] | FE + BE + docs + tests |
| 72 | Add `json_array_length` / `json_object_keys` functions | [Feature] | FE + BE + docs + tests |
| 73 | Add `approx_top_k` aggregate function | [Feature] | FE + BE + docs + tests |

### 4.3 Connector & Loading Enhancements (4–5 commits)

| # | Task | Type | Target |
|---|------|------|--------|
| 74 | Add support for Iceberg V2 position deletes in scan | [Enhancement] | `com.starrocks.connector.iceberg` |
| 75 | Improve Hive connector error messages with actionable suggestions | [Enhancement] | `com.starrocks.connector.hive` |
| 76 | Add retry logic for transient connector failures | [Enhancement] | `com.starrocks.connector` |
| 77 | Optimize Parquet column reader for nested types | [Enhancement] | BE Parquet reader |
| 78 | Add file format auto-detection for external table scan | [Enhancement] | FE connector framework |

### 4.4 Observability & DevEx (4–5 commits)

| # | Task | Type | Target |
|---|------|------|--------|
| 79 | Add query profile metrics for spill operations | [Enhancement] | BE runtime profile |
| 80 | Improve EXPLAIN output with partition/tablet info | [Enhancement] | FE plan explanation |
| 81 | Add slow query analysis helper views | [Enhancement] | FE system tables |
| 82 | Improve build error messages in build.sh | [Tool] | `build.sh` |
| 83 | Add developer-facing linting pre-commit hooks | [Tool] | `.pre-commit-config.yaml` |

---

## Phase 5: Features — Architectural Contributions (15–20 commits)

**Goal**: Demonstrate committer-level technical depth. Drive a feature end-to-end.

### 5.1 Pick ONE Major Feature (8–12 commits across sub-PRs)

Choose one based on community roadmap alignment:

**Option A: Null-Aware Anti Join (BE-heavy)**
| # | Task | Type |
|---|------|------|
| 84 | Design doc: null-aware left anti join with shuffle | [Doc] |
| 85 | Implement null-aware hash join probe | [Feature] |
| 86 | Add pipeline operator support | [Feature] |
| 87 | Add FE plan generation | [Feature] |
| 88 | Add unit tests | [UT] |
| 89 | Add SQL integration tests | [UT] |
| 90 | Add performance benchmark | [UT] |
| 91 | Update documentation | [Doc] |

**Option B: Advanced Materialized View (FE-heavy)**
| # | Task | Type |
|---|------|------|
| 84 | Design doc: incremental MV refresh with complex joins | [Doc] |
| 85 | Implement delta detection for multi-table MV | [Feature] |
| 86 | Add partition-aware incremental refresh | [Feature] |
| 87 | Add cost-based MV selection improvements | [Enhancement] |
| 88 | Add unit tests | [UT] |
| 89 | Add SQL integration tests | [UT] |
| 90 | Update MV documentation | [Doc] |
| 91 | Add monitoring metrics for MV refresh | [Enhancement] |

**Option C: Query Execution Optimization (Full-Stack)**
| # | Task | Type |
|---|------|------|
| 84 | Design doc: two-level hashmap for aggregation | [Doc] |
| 85 | Implement two-level hashmap in BE | [Feature] |
| 86 | Integrate with pipeline aggregation operator | [Feature] |
| 87 | Add adaptive switching based on cardinality | [Enhancement] |
| 88 | Add FE hints for aggregation strategy | [Enhancement] |
| 89 | Add unit tests | [UT] |
| 90 | Add SQL integration tests with large datasets | [UT] |
| 91 | Performance benchmark and documentation | [Doc] |

### 5.2 Refactoring Contributions (5–8 commits)

| # | Task | Type | Target |
|---|------|------|--------|
| 92 | Split AstBuilder.java (9976 lines) into category-specific builders | [Refactor] | `com.starrocks.sql.parser` |
| 93 | Extract predicate handling from olap_scan_prepare.cpp (1533 lines) | [Refactor] | `be/src/exec/` |
| 94 | Decompose StmtExecutor.java (4005 lines) into phase handlers | [Refactor] | `com.starrocks.qe` |
| 95 | Extract PlanFragmentBuilder into context-specific builders | [Refactor] | `com.starrocks.sql.plan` |
| 96 | Reduce module boundary baseline violations | [Refactor] | `be/` modules |

### 5.3 Build & Infra (2–3 commits)

| # | Task | Type | Target |
|---|------|------|--------|
| 97 | Modularize build.sh into phase-specific scripts | [Tool] | `build.sh` |
| 98 | Improve `.clang-tidy` coverage and add new checks | [Tool] | `.clang-tidy` |
| 99 | Add automated doc-sync check for config changes | [Tool] | CI scripts |
| 100 | Add benchmark CI for performance regression detection | [Tool] | CI configuration |

---

## Acceptance Criteria

- [ ] 100+ merged PRs across all 5 phases
- [ ] At least 15 PRs in each of: docs, tests, bug fixes, enhancements
- [ ] At least 1 major feature (Phase 5) fully delivered
- [ ] Positive review history with core committers
- [ ] Active participation in code reviews (review 50+ PRs from others)
- [ ] Responsive to review feedback (< 24h turnaround)
- [ ] Zero reverted commits

## Key Success Metrics

| Metric | Target |
|--------|--------|
| Total merged PRs | ≥ 100 |
| PR acceptance rate | ≥ 90% |
| Avg review turnaround | < 24 hours |
| PRs reviewed (others) | ≥ 50 |
| Critical bug fixes | ≥ 5 |
| Features delivered | ≥ 1 major |
| Community interactions | Active on GitHub issues |

## Tactical Advice

### Getting PRs Merged Fast
1. **Start small** — Phase 1 PRs should be merge-ready in 1 review cycle
2. **One concern per PR** — Never mix doc fixes with code changes
3. **Always include tests** — PRs without tests get delayed or rejected
4. **Follow the template** — Fill PR template completely (checkboxes, behavior changes)
5. **Tag the right reviewer** — Study `git log --format='%an'` per directory

### Building Reviewer Trust
1. **Review others' PRs** — Start reviewing docs and test PRs in week 1
2. **Respond to feedback quickly** — Same-day turnaround on review comments
3. **Be humble on early PRs** — Accept suggestions graciously
4. **Write design docs** — For anything > 200 lines changed, write a brief RFC
5. **Join community channels** — Participate in Slack/Discord/mailing lists

### Avoiding Common Pitfalls
1. **Don't refactor without tests** — Always add tests FIRST, then refactor
2. **Don't break module boundaries** — Run `check_be_module_boundaries.py` before every BE PR
3. **Don't forget doc sync** — Config/metrics changes MUST update docs
4. **Don't submit 1000-line PRs** — Break into ≤300 line incremental PRs
5. **Don't ignore CI** — Green CI is a hard requirement, fix failures immediately

### Weekly Cadence

| Day | Activity |
|-----|----------|
| Mon | Triage 5 GitHub issues, plan week's PRs |
| Tue–Thu | Code 2–3 PRs, review 3–5 community PRs |
| Fri | Address review feedback, update docs, community engagement |
| Weekend | (Optional) Work on Phase 5 major feature design |

## Decision Log

- 2026-04-09: Created initial plan. Strategy: breadth-first (docs→tests→fixes→enhancements→features) to build trust progressively.
- 2026-04-09: Chose 5-phase approach over topic-based approach to manage risk and build reviewer relationships.
