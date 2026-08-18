#!/usr/bin/env python3
"""Turn StarRocks' own SQLTest suite into corpus files the cluster harness can replay.

`test/sql/*/T/*` is the largest body of SQL in the repository that is known to bind, known to run,
and known to be maintained: every new feature lands with cases here. The cluster corpus had no
SQLTest family at all -- it was AST mutants, a Doris regression dump, TPC-H/DS and a few harvested
production shapes -- so every shape that only StarRocks' own suite exercises (agg-state columns,
subfield pruning, IVM, query cache lanes, partial updates) was invisible to the fuzzer.

A T file is not directly replayable, and the ways it breaks are all silent:

  * **`-- name: <case>` starts a new case.** Cases in one file are independent and routinely reuse
    table names; concatenating them makes the second CREATE TABLE fail and every query over it get
    dropped by the bind gate. One case becomes one corpus file.
  * **The harness owns the database.** `CREATE DATABASE` / `USE` / especially `DROP DATABASE` in a
    corpus would delete the database the round is running in, taking the other instance's tables
    with it (the group name, not the file, decides the database).
  * **`function:` and `shell:` lines are SQLTest directives, not SQL.** Left in, each one becomes a
    parse error filed against the product, forever, in every round.
  * **`${var}` is SQLTest interpolation.** Replayed through a plain client it is a literal, so the
    statement fails as a syntax error that looks like a product bug.
  * Cases needing infrastructure the fuzz cluster does not have (external catalogs, broker/routine
    load, backups) can never bind; they would be permanent bind-gate noise.

What survives is written as one flat file per case, DDL and queries in original order, which is
exactly what pack_seed_corpus.py consumes.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from pathlib import Path

NAME_RE = re.compile(r'^\s*--\s*name:\s*(\S+)', re.I)

# Directive lines: SQLTest's own vocabulary, never SQL.
DIRECTIVE_RE = re.compile(r'^\s*(function|shell):', re.I)

# Statements the harness must own, or that reach outside this BE. Matched on the flattened
# statement, so a wrapped `CREATE\n  DATABASE` is caught too.
FORBIDDEN_RE = re.compile(
    r'^\s*('
    r'(create|drop|alter)\s+database\b'
    r'|use\b'
    r'|admin\b'
    r'|alter\s+system\b'
    r'|set\s+global\b'
    r'|set\s+catalog\b'
    r'|kill\b'
    r'|analyze\b|drop\s+all\s+analyze\b|kill\s+analyze\b'
    r'|(create|drop|alter|show)\s+(external\s+)?(catalog|resource|repository|storage\s+volume)\b'
    r'|(create|drop|alter|submit|stop|pause|resume)\s+(routine\s+load|task|pipe)\b'
    r'|(backup|restore|cancel\s+backup|cancel\s+restore)\b'
    r'|(create|drop|alter|show)\s+(user|role|security\s+integration)\b'
    r'|grant\b|revoke\b'
    r'|(create|drop)\s+(file|dictionary)\b'
    r'|install\s+plugin\b|uninstall\s+plugin\b'
    r')',
    re.I)

# A cross-database reference escapes the database the round owns. `UPDATE
# default_catalog.information_schema.be_configs SET value = ...` is the worst of them: it rewrites a
# BE config for every instance on the machine and outlives the round that ran it.
QUALIFIED_RE = re.compile(r'\b(default_catalog|information_schema|_statistics_|sys)\s*\.', re.I)

# The harness's generator fills every table it finds each round, so a corpus INSERT only has to
# establish shape. A quarter-million-row generate_series in setup costs that much every round, on
# every instance, forever.
BIG_SERIES_RE = re.compile(r'(generate_series\s*\(\s*1\s*,\s*)(\d+)', re.I)
BIG_SERIES_CAP = 2000

# `${uuidN}` is SQLTest asking the runner for a unique name, and it is 95% of all interpolation in
# the suite (12706 of 13400 occurrences). Substituting a name derived from the case makes those
# cases replayable instead of discarding them; every other variable names an external resource the
# fuzz cluster does not have, so those cases stay dropped.
UUID_RE = re.compile(r'\$\{uuid(\d*)\}', re.I)


def resolve_uuids(body: str, case_key: str) -> str:
    def sub(m):
        h = hashlib.sha1(f'{case_key}/{m.group(1) or "0"}'.encode()).hexdigest()[:12]
        return f'u{h}'
    return UUID_RE.sub(sub, body)


# A case that needs any of these can never bind on the fuzz cluster; drop the whole case rather
# than leave it half-working.
# SQLTest block syntax -- `CLEANUP { ... }`, `SET_VAR ... END` -- is not SQL and does not survive
# a split on `;`: the closing brace glues onto the next statement, which is how
# `} END SET_VAR drop database test_db_x` appeared in a generated corpus file. Drop those cases
# whole rather than trying to parse a second grammar.
BLOCK_SYNTAX_RE = re.compile(r'\bCLEANUP\s*\{|\bSET_VAR\b|\bUNCHECK\b|^\s*\}\s*$', re.I | re.M)

CASE_KILL_RE = re.compile(
    r'(\$\{|\bhive\b\s*\.|\biceberg\b\s*\.|\bhudi\b|\bpaimon\b|\bdelta_lake\b|\bjdbc\b'
    r'|\bbroker\s+load\b|\bs3\s*://|\bhdfs\s*://|\boss\s*://|\bfiles\s*\('
    r'|\bproperties\s*\(\s*"type"\s*=|create\s+external\s+catalog|set\s+catalog)',
    re.I)


# Applied to the finished file, not to a single parsed statement, so a statement the split glued
# together wrongly is still caught.
DESTRUCTIVE_RE = re.compile(
    r'\b(drop|create|alter)\s+database\b|\badmin\s|\balter\s+system\b'
    r'|\bset\s+global\s+\w|\bkill\s+(query|connection)\b|^\s*use\s', re.I | re.M)


def cases(text: str):
    """Split a T file on `-- name:` markers. Text before the first marker is its own case."""
    cur_name, cur = None, []
    for line in text.splitlines():
        m = NAME_RE.match(line)
        if m:
            if cur:
                yield cur_name, '\n'.join(cur)
            cur_name, cur = m.group(1), []
            continue
        cur.append(line)
    if cur:
        yield cur_name, '\n'.join(cur)


def flatten(text: str):
    """Yield single-line statements, comments dropped first (a comment can hold a `;`)."""
    text = '\n'.join(l for l in text.splitlines()
                     if not l.strip().startswith('--') and not DIRECTIVE_RE.match(l))
    for raw in text.split(';'):
        body = ' '.join(raw.split())
        if body:
            yield body


def convert(path: Path, out_dir: Path, stats: dict):
    text = path.read_text(errors='replace')
    suite = path.parent.parent.name
    written = 0
    for idx, (name, body) in enumerate(cases(text)):
        stats['cases'] += 1
        resolved = resolve_uuids(body, f'{path}#{name or idx}')
        if resolved != body:
            stats['case_uuid_resolved'] += 1
            body = resolved
        if BLOCK_SYNTAX_RE.search(body):
            stats['case_block_syntax'] += 1
            continue
        if CASE_KILL_RE.search(body):
            stats['case_needs_infra'] += 1
            continue
        stmts = [s for s in flatten(body)]
        if not stmts:
            stats['case_empty'] += 1
            continue
        kept = [s for s in stmts if not FORBIDDEN_RE.match(s)]
        stats['stmt_forbidden'] += len(stmts) - len(kept)
        after_qual = [s for s in kept if not QUALIFIED_RE.search(s)]
        stats['stmt_qualified'] += len(kept) - len(after_qual)
        kept = after_qual
        capped = []
        for s in kept:
            new, n = BIG_SERIES_RE.subn(
                lambda m: m.group(1) + (str(BIG_SERIES_CAP) if int(m.group(2)) > BIG_SERIES_CAP
                                        else m.group(2)), s)
            stats['stmt_series_capped'] += 1 if new != s else 0
            capped.append(new)
        kept = capped
        # A case is only useful if something actually queries.
        queries = [s for s in kept if not re.match(
            r'^\s*(create|alter|drop|insert|set|truncate|analyze|refresh)\b', s, re.I)]
        if not queries:
            stats['case_no_query'] += 1
            continue
        # Belt and braces: whatever the parsing did, a corpus file that still contains a statement
        # able to destroy the round's database or reconfigure the process is not worth its shapes.
        payload = ';\n'.join(kept) + ';\n'
        if DESTRUCTIVE_RE.search(payload):
            stats['case_destructive_sweep'] += 1
            continue
        tag = name or f'{path.stem}_{idx}'
        tag = re.sub(r'[^0-9A-Za-z_]', '_', tag)
        (out_dir / f'{suite}__{tag}.sql').write_text(payload)
        written += 1
        stats['kept'] += 1
        stats['kept_stmts'] += len(kept)
    return written


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument('--out', required=True, type=Path)
    ap.add_argument('--tests-root', required=True, type=Path, help='the repo test/sql directory')
    a = ap.parse_args()

    a.out.mkdir(parents=True, exist_ok=True)
    stats = dict(files=0, cases=0, kept=0, kept_stmts=0, case_needs_infra=0, case_empty=0,
                 case_no_query=0, stmt_forbidden=0, stmt_qualified=0, stmt_series_capped=0,
                 case_uuid_resolved=0, case_block_syntax=0, case_destructive_sweep=0)
    for path in sorted(a.tests_root.glob('*/T/*')):
        if not path.is_file():
            continue
        stats['files'] += 1
        convert(path, a.out, stats)
    for k, v in stats.items():
        print(f'{k}: {v}')


if __name__ == '__main__':
    sys.exit(main())
