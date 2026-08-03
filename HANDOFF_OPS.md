# Handoff: running the fuzzer and triaging results

For the session that keeps the campaign running and turns findings into fixes. The session that
*changes* the fuzzer reads `HANDOFF_DEV.md`.

## The one thing to internalise

**A broken round and a quiet round look identical unless something makes them differ.** This has bitten
the campaign nine separate times, and every instance cost hours:

1. FE died; the harness spun 602 rounds over 7.5 hours. 650 rounds, 50 of them valid.
2. Emitted setup was the corpus file's final schema, so tables the queries needed had been dropped.
3. Setup failures were not counted, only query errors. 171 of 2203 corpus `CREATE TABLE`s were failing
   because `default_replication_num` was 3 on a one-BE cluster. Took 7 rounds to notice.
4. `be_alive()` asked the OS for a pid instead of asking the FE for `Alive: true`. One round fired 2198
   queries at a backend that was not serving and recorded **79128 fake errors** plus 4 fake signatures.
5. Crash evidence was captured with `tail -60 be.out`, but the BE restarts after it dies — so the
   captured text was the *new process's startup banner*. A real SIGSEGV was dismissed as a false
   positive because of it.
6. `gen_data.py` built its INSERT as a command-line argument and died on ARG_MAX for hundreds of rounds,
   reported only as "data generation failed or timed out".
7. Renaming corpus files to `deep2_mut_*` broke the database-name derivation, so 882 groups (54% of the
   corpus) ran every query against a database that did not exist.
8. A differential knob named `enable_low_cardinality_optimize` does not exist. The server errored,
   `diff_run` returned empty, and empty is indistinguishable from "this query has no rows" — a third of
   the differential silently never ran.
9. `UnsupportedException` extends `RuntimeException`, not `SemanticException`, so deliberate analyzer
   refusals were filed as internal errors, i.e. reported as bugs.

Guards now exist for most of these (`preflight`, `validate_knobs`, per-round `gen_rows`, signature
dedup, the empty-round backoff in `parallel_soak.sh`). **When output drops, suspect the harness before
concluding there are no bugs left.**

## What runs where

| | host | what |
| --- | --- | --- |
| cluster fuzz | dev2 `172.26.92.205`, container `sr-dev-fuzz` | replays the mutant corpus against a real FE+BE |
| soak | dev1 `39.99.136.234`, container `sr-dev-fuzzdev` | FE-only, 12 shards, no BE |

dev2 is reached through dev1: `ssh fha@39.99.136.234` then `ssh 172.26.92.205`.

Cluster harness: `/home/disk1/fha/sr-ws/fuzz/clusterfuzz/clusterfuzz.sh`. Start it **detached from the
ssh session** — `docker exec -d ... exec ./clusterfuzz.sh > run.stdout 2> run.stderr` — because
`setsid nohup` inside a `docker exec` dies when the exec's session ends, and a timed-out ssh takes the
whole process group with it. Keep stderr: discarding it is how one death became undiagnosable.

Soak: `docker exec -d sr-dev-fuzzdev bash -lc 'ROOT=... OUT=... SHARDS=12 XMX=3g exec .../parallel_soak.sh'`.
12 is the measured optimum; 24 is *slower* (see `HANDOFF_DEV.md`).

**Never edit `clusterfuzz.sh` in place while it is running.** Bash reads a script incrementally, from a
byte offset, so an edit under a live instance makes it execute the tail of the new file from the old
position — arbitrary half-statements, with no error that names the cause. Stage as
`clusterfuzz.next.sh`, `bash -n` it, then stop the loop and swap. The pre-M11 original is kept as
`clusterfuzz.sh.bak-pre-m11`.

## Reading a round

`rounds.tsv` columns: `round group tables setup_fail gen_rows queries errors diff_checked diff_bad
diff_empty diff_void tlp_checked tlp_bad tlp_skipped fatal_delta be_restarts new_fe_sigs secs`.

The five middle columns are new. A file written under the old header is **rotated** to `rounds.tsv.N`
on first start rather than appended to, because mixing widths makes every column-indexed awk read the
wrong field.

- `tables=0` on a group that should have a schema means setup failed.
- `gen_rows=0` with `tables>0` means the data generator ran and loaded nothing — a harness defect, and
  the log says so explicitly. `gen_rows=-1` means a benchmark group, which is not amplified.
- `diff_checked` is how many statements the differential compared; `diff_bad` is how many returned
  different rows under a knob. A knob mismatch is a **correctness** finding, not a crash.
- `diff_empty` is how many statements were **never compared** because their baseline returned no rows.
  If it dominates `diff_checked`, the differential is running on air — the corpus is producing queries
  that select nothing, and `diff_checked=0 diff_bad=0` would otherwise read as a clean round.
- `diff_void` is how many knob runs returned nothing where the baseline had rows. That is not
  agreement, it is the run failing: a timeout, an error, or a knob the server rejected. This is
  incident 8 made visible at run time; a run of them is logged as a WARNING line.
- `tlp_checked` / `tlp_bad` / `tlp_skipped`: the TLP metamorphic oracle. For a predicate `p`, every row
  satisfies exactly one of `p`, `NOT p`, `p IS NULL`, so `SELECT * FROM t` must equal the three-way
  UNION ALL of those branches. Unlike the knob differential this needs no second plan, so it catches a
  rule that is wrong in **every** plan. `tlp_skipped` is mostly tables above `TLP_MAX_ROWS` (default
  20000) — the bench tables are all far above it, so TLP only really runs on corpus groups.
- The knob pool is now ~50 session variables covering predicate pushdown, join reorder, CTE reuse,
  table/partition pruning, the agg rewrites and runtime filters, and each statement is checked against
  `DIFF_KNOB_SAMPLE` (default 4) of them drawn at random. Per-round cost is unchanged; coverage
  accumulates across rounds. Every name was validated against a live FE, and `validate_knobs` still
  refuses to start if one is rejected.
- `be_restarts` above zero without a crash signature is usually the neighbour on that box SIGTERMing
  the BE, not a defect. Check `be.out` for a clean "BE is shutting down".

## Triaging a crash

The crash recorder dedups by stack signature and resolves the crashing statement itself: the BE crash
banner carries `query_id:...`, and that is looked up in `fe.audit.log`. So `findings.md` gives you the
exact SQL with no bisection. Before this existed, hundreds of rounds reported zero crashes while five
distinct ones were happening.

**Always check the build banner in the same `be.out` before judging severity.** The campaign switched
from ASAN to RELEASE partway through, and it changes what a stack means:

- `down_cast` and `cast_to_raw` are `DCHECK`s. On ASAN they abort loudly; under `NDEBUG` they are a
  bare `static_cast` that reads one column type as another. Several findings are **release-only wild
  reads** that a debug build would have caught cleanly.
- Conversely, a `DCHECK` failure on ASAN may be harmless on release — verify before filing.

## Known open work

- **`get_const_value<T>()` has dozens of BE call sites**, each assuming the FE guaranteed a non-null
  constant of exactly the right type. Two were found by luck (`tokenize`, `encode_fingerprint_sha256`).
  This deserves a systematic sweep rather than waiting for the fuzzer to stumble on them.
- **`ngram_search` has no executed reproduction.** The branch is pushed; the crashing statement is known
  from the crash log and the fix rejects it, but nobody has watched it crash on an unfixed release
  build. Needs a build without the FE guard.
- **Decimal literal overflow**: an integer literal converted into a declared decimal element type gets
  no range check, while a written-decimal literal does. Located, not fixed, not root-caused.
- **F1**: deparser emits an empty table qualifier (`` `​`.fn(...) ``). Never dispatched.
- **The V1 low-cardinality rule** (`AddDecodeNodeForDictStringRule`) has no agg-state guard at all;
  the accepted fix only covers V2 (`DecodeCollector`). Owned by a different session — do not edit
  `AddDecodeNodeForDictStringRule.java` or `DecodeCollector.java` from here.

## Branch inventory

Upstream `main` merged 12 of this campaign's fixes on 2026-08-03 (#77052, #77066, #77069, #77087,
#77088, #77090, #77096, #77098, #77099, #77103, #77104, and the PrepareCollectMetaTask NPE #77051).
Their worktrees and fork branches have been removed.

Seven fixes remain, all rebased onto `starrocks-2/main` (`bdf38028ad4`) and all in worktrees under
`/home/public/`:

| worktree | branch | state |
| --- | --- | --- |
| `sr-fnargs` | `bugfix/tokenize-ngram-const-args` | pushed to fork, **no PR** |
| `sr-jsonbytes` | `bugfix/json-column-byte-size` | not pushed |
| `sr-arrayunion` | `bugfix/array-union-agg-dict` | not pushed |
| `sr-inconst` | `bugfix/in-const-predicate-fatal` | not pushed |
| `sr-projdrop` | `bugfix/projection-over-dropped-column` | not pushed |
| `sr-asofside` | `bugfix/asof-single-sided-condition` | not pushed |
| `sr-arraydeparse` | `bugfix/deparse-untyped-array-npe` | not pushed |

`sr-jsonbytes` is still needed despite #77090 landing: that PR fixed flat-JSON schema reconciliation in
`json_column.cpp`, while the INTERSECT crash comes from `ObjectColumn::deserialize_and_append_batch`
being a `DCHECK(false)` stub, which is unchanged in `main`. The two are complementary; verify the same
way before assuming any remaining branch was superseded.

Two things to check before pushing any branch:

- **Author email.** Three subagents committed as `drfeng08@gmail.com` instead of the repo default
  `Hyper-FF@users.noreply.github.com`. Check `git log --format=%ae` before pushing; fixing it after
  needs a force-push.
- **Whether the test was seen failing first.** Some branches have it, some do not, and the PR
  description should not claim what was not done.

## Environment traps

- **`build.sh --fe` regenerates `output/fe/conf/fe.conf` from the source template**, wiping any edit.
  `default_replication_num = 1` has to be re-asserted after every FE rebuild; `preflight` now does it,
  but anything else you put there is also gone.
- `admin set frontend config` does not survive an FE restart.
- Corpus files renamed with a prefix must keep the numeric index intact: the emitted SQL is fully
  qualified with `srfuzz_mut_<i>`, and the harness derives the database name from the group name.
- Benchmark groups (`bench_*`) carry `-- benchmark-db: <db>` in their setup file. That database is
  **shared and must never be dropped** — it holds 28M rows that took minutes to materialise. Writers
  against it are capped by `BENCH_ROW_CAP`.
