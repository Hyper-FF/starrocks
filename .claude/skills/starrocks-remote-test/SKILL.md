---
name: starrocks-remote-test
description: >-
  Run StarRocks tests on the remote SSH host and stream results back — Backend
  C++ unit tests (run-be-ut.sh), Frontend Java unit tests (run-fe-ut.sh), and SQL
  integration tests (test/run.py). Use when the user wants to "run be ut on
  sr-dev", "远程跑 fe 单测", "run the sql tests remotely", or test a specific
  module/filter/case. Push changes first with starrocks-remote-sync.
---

# StarRocks Remote Test

Runs the repo's test runners on the remote (alias `sr-dev`) and returns output.
Sync local edits first: `bash .claude/skills/lib/sr-remote.sh sync --push`.

## Backend unit tests (C++)

```bash
# All BE UTs
bash .claude/skills/lib/sr-remote.sh test be

# A specific gtest filter / module / single target
bash .claude/skills/lib/sr-remote.sh test be --gtest_filter='MyTest.*'
bash .claude/skills/lib/sr-remote.sh test be --module exec
bash .claude/skills/lib/sr-remote.sh test be --build-target some_test
```

## Frontend unit tests (Java)

```bash
bash .claude/skills/lib/sr-remote.sh test fe
bash .claude/skills/lib/sr-remote.sh test fe <maven/test args>
```

## SQL integration tests

```bash
# Runs `python3 run.py` inside test/ on the remote
bash .claude/skills/lib/sr-remote.sh test sql -v
bash .claude/skills/lib/sr-remote.sh test sql -d <case-dir-or-file>
```

SQL tests need a running cluster — bring one up with **starrocks-remote-deploy**
first. All flags pass through to the underlying runner; consult each script's
`--help` (e.g. `sr-remote.sh ssh "bash run-be-ut.sh --help"`) for the full set.
