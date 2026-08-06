# srfuzz

Everything the SQL fuzzing effort owns, in one directory.

It used to be seventeen loose files at the repository root plus two top-level directories, mixed in
with upstream StarRocks' own. That costs twice: nobody could tell at a glance which files were ours,
and this fork rebases onto upstream `main`, where seventeen root-level files are seventeen chances
to conflict. One directory is one.

## Layout

| path | what |
| --- | --- |
| `soak/` | the FE arm: `parallel_soak.sh` runs N sharded JVMs over disjoint corpus slices |
| `cluster/` | the cluster arm: `clusterfuzz.next.sh` replays against a real BE, `gen_data.py` fills its tables |
| `docs/SQL_AST_FUZZER_PLAN.md` | the design: operators, oracles, phases |
| `docs/findings/` | one report per campaign or defect family |
| `HANDOFF_DEV.md` | for the session that CHANGES the fuzzer |
| `HANDOFF_OPS.md` | for the session that RUNS it and triages what it finds |

**The mutator itself is not here.** It is a JUnit test and has to live in the module it tests:
`fe/fe-core/src/test/java/com/starrocks/fuzz/`. `fe/fe-fuzz/` is a separate Maven module for the
round-trip fidelity checker.

## What is deliberately not in git

`seeds/` and `emit/` at the repository root, plus `.fuzzwork/`. Seeds are anonymised production
data; emitted corpora are round output -- 1520 files and 136 MB of them reached git once before this
was enforced. Both are inputs to a run and neither is source, and keeping them out is what makes
`git status` mean anything after a soak.

Corpus files carry their own provenance instead (`-- srfuzz-origin:`, `-- srfuzz-generation:`,
`-- srfuzz-root:`), so a corpus is self-describing wherever it ends up -- which matters more than
version control here, since a corpus is routinely copied between hosts and containers.
`soak/stamp_origin.sh` stamps harvested corpora as generation 0.

## Running it

Both arms take their settings from the environment; neither is started from this directory.
`HANDOFF_OPS.md` has the current deployments, the containers they run in, and what to check when a
round produces nothing.

⚠️ `soak/parallel_soak.sh` moved here from the repository root. A deployment whose launcher still
says `./soak/parallel_soak.sh` will fail to start after picking this up -- the path is now
`./srfuzz/soak/parallel_soak.sh`. That is a loud failure rather than a silent one, but it is a
failure, so update the launcher in the same change that deploys this.
