#!/usr/bin/env python3
"""Pack harvested seed corpora into groups the cluster harness replays directly.

The cluster arm reads a corpus as `<group>.setup.sql` + `<group>.query.sql` pairs. Until now the
only producer of those was `pack_cluster_corpus.sh`, which renames an AST-fuzzer emit directory.
That path needs a mutation run first, so a harvested corpus -- production shapes, or an external
engine's regression suite -- could not reach a real BE at all. This packs one directly.

Four constraints come from the harness, and every one of them is silent when broken:

  * **One statement per line.** `minimize_group` reads `query.sql` with `while read line` and runs
    each line on its own. A statement wrapped over three lines is minimised as three broken
    fragments, and the finding is filed with a reproducer that cannot parse.
  * **At most MINIMIZE_MAX_STMTS (120) statements per group**, or minimisation refuses the group
    and a crash gets recorded with no reproducing statement.
  * **The group name decides the database**: `srfuzz_mut_<n>` after stripping any generation prefix
    and leading zeros from `<prefix>_mut_<n>`. Two groups sharing an index share a database, and
    the sharding hashes the database name -- so a collision with the existing corpus puts two
    instances into one database, creating and dropping it underneath each other. Hence --start.
  * **Statements run with the database already selected** (`mysql "$db"`), so table references must
    be unqualified. A harvested corpus is unqualified already; this only has to not add anything.

DDL is pruned to the tables a group's queries actually reference, transitively through views. The
setup runs once per round and the data generator then amplifies every table it finds, so carrying a
warehouse's full 312-table schema into a group that touches four of them pays for 308 table
creations and 308 rounds of row generation, every single round.
"""

from __future__ import annotations

import argparse
import hashlib
import random
import re
import sys
from pathlib import Path

# Statements that build the schema rather than query it. `insert` is setup: the harness's data
# generator adds rows on top, it does not replace corpus-provided ones.
DDL_HEADS = ('create ', 'alter ', 'insert ', 'insert\t', 'drop ', 'set ', 'use ', 'truncate ',
             'analyze ', 'admin ')

CREATE_NAME = re.compile(
    r"""^create\s+(?:or\s+replace\s+)?(?:external\s+|temporary\s+)?
        (?P<kind>table|view|materialized\s+view)\s+(?:if\s+not\s+exists\s+)?
        (?:`(?P<q>(?:[^`]|``)+)`|(?P<b>[A-Za-z_][\w$]*))""",
    re.I | re.X)


def statements(text: str):
    """Split a corpus file into single-line statements, comments dropped.

    Comment lines are removed BEFORE the split, not after. A comment can contain a semicolon -- the
    production fixtures open with "-- The schema is NOT the customer's; it exists only so the shapes
    bind." -- and splitting first cuts that comment in half, leaving the tail as ordinary text glued
    onto the front of the next statement. The observed cost: the first CREATE TABLE of four of the
    five cluster_b fixtures became `it exists only so the shapes bind. CREATE TABLE ...`, failed at
    setup, and every query over that table was then dropped by the bind gate as "Unknown table".
    Nothing anywhere said "a comment ate your first table".

    The split itself is still naive, same as split_fixture.py -- a `;` inside a string literal cuts
    a statement in half. That is why the packer counts what it dropped: a corpus whose parse is
    falling apart shows up as a dropped count, not as a quietly smaller group.
    """
    text = '\n'.join(l for l in text.splitlines() if not l.strip().startswith('--'))
    for raw in text.split(';'):
        body = raw.strip()
        if not body:
            continue
        # The harness reads query.sql line by line; a wrapped statement has to become one line.
        # The terminator goes back on: splitting consumed it, and without it `mysql -f < query.sql`
        # reads the whole file as ONE unterminated statement, while minimisation's `grep -c ';'`
        # counts zero statements and declines the group. Both failures are silent.
        yield ' '.join(body.split()) + ';'


def classify(stmts):
    ddl, queries = [], []
    for s in stmts:
        (ddl if s.lower().startswith(DDL_HEADS) else queries).append(s)
    return ddl, queries


def created_name(stmt: str):
    m = CREATE_NAME.match(stmt.strip())
    if not m:
        return None
    return (m.group('q') or m.group('b')).replace('``', '`')


def referenced(names, text_lower):
    """Which of `names` the text mentions, by identifier match.

    Backticked and bare forms both count, so the boundary is word characters only -- a backtick must
    NOT count as one. Excluding it dropped every quoted reference, which in an anonymised corpus is
    all of them, and the groups then looked like queries over tables nobody had declared.

    Substring matching would be wrong in the other direction: `tbl_1` is a substring of `tbl_10`, so
    a group referencing only `tbl_10` would drag in `tbl_1` -- silent bloat, which is the failure
    this whole function exists to prevent.
    """
    hit = set()
    for n in names:
        if re.search(r'(?<!\w)' + re.escape(n.lower()) + r'(?!\w)', text_lower):
            hit.add(n)
    return hit


def prune_ddl(ddl, queries):
    """Keep the DDL a group needs: statements creating a referenced object, plus their closure."""
    owner = {}          # object name -> list of statements that build it
    unnamed = []        # anything we cannot attribute (set/use/analyze/...); always kept
    for s in ddl:
        name = created_name(s)
        if name:
            owner.setdefault(name, []).append(s)
            continue
        m = re.match(r'^(?:insert\s+(?:into|overwrite)|truncate\s+table|alter\s+table)\s+'
                     r'(?:`((?:[^`]|``)+)`|([A-Za-z_][\w$]*))', s, re.I)
        if m:
            owner.setdefault((m.group(1) or m.group(2)).replace('``', '`'), []).append(s)
        else:
            unnamed.append(s)

    keep = referenced(owner, ' '.join(queries).lower())
    # A view's body names its base tables, so pruning has to reach through it. Fixpoint rather than
    # one pass: views over views are rare but they exist, and one pass would drop the second level.
    while True:
        body = ' '.join(s for n in keep for s in owner[n]).lower()
        grown = keep | referenced(owner, body)
        if grown == keep:
            break
        keep = grown

    out = list(unnamed)
    for s in ddl:                       # original order: a view must follow its tables
        name = created_name(s) or None
        if name is None:
            m = re.match(r'^(?:insert\s+(?:into|overwrite)|truncate\s+table|alter\s+table)\s+'
                         r'(?:`((?:[^`]|``)+)`|([A-Za-z_][\w$]*))', s, re.I)
            name = (m.group(1) or m.group(2)).replace('``', '`') if m else None
        if name is not None and name in keep:
            out.append(s)
    return out, len(keep)


def write_group(out_dir, name, root, source, ddl, queries):
    header = [f'-- srfuzz-origin: harvested',
              f'-- srfuzz-generation: 0',
              f'-- srfuzz-root: {root}',
              f'-- srfuzz-source: {source}']
    (out_dir / f'{name}.setup.sql').write_text('\n'.join(header + ddl) + '\n')
    (out_dir / f'{name}.query.sql').write_text('\n'.join(header + queries) + '\n')


def pack_file(path, out_dir, prefix, root, index, per_group, max_stmts, manifest, no_prune=False):
    ddl_all, queries = classify(statements(Path(path).read_text(errors='replace')))
    packed = 0
    for i in range(0, len(queries), per_group):
        chunk = queries[i:i + per_group]
        if len(chunk) > max_stmts:
            chunk = chunk[:max_stmts]
        ddl = ddl_all if no_prune else prune_ddl(ddl_all, chunk)[0]
        if not ddl:
            # A group with no schema runs its queries against an empty database: every statement
            # fails with "Unknown table", which is indistinguishable from a corpus of bad queries.
            manifest.append((f'{prefix}_mut_{index}', str(path), root, 0, len(chunk), 'SKIPPED-no-ddl'))
            continue
        name = f'{prefix}_mut_{index}'
        write_group(out_dir, name, root, Path(path).name, ddl, chunk)
        manifest.append((name, str(path), root, len(ddl), len(chunk), 'ok'))
        index += 1
        packed += 1
    return index, packed, len(queries)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument('--out', required=True, type=Path)
    ap.add_argument('--prefix', required=True,
                    help='group-name prefix; the database becomes srfuzz_mut_<index>')
    ap.add_argument('--root', required=True, help='provenance root (prod_cluster_b, doris_regression, ...)')
    ap.add_argument('--start', type=int, required=True,
                    help='first index; must not collide with any index already in the target corpus')
    ap.add_argument('--per-group', type=int, default=60)
    ap.add_argument('--max-stmts', type=int, default=120, help="the harness's MINIMIZE_MAX_STMTS")
    ap.add_argument('--sample', type=int, default=0, help='pack at most N source files (deterministic)')
    ap.add_argument('--sample-seed', type=int, default=1)
    ap.add_argument('--min-queries', type=int, default=1, help='skip source files with fewer queries')
    ap.add_argument('--max-ddl', type=int, default=0,
                    help='skip source files declaring more than N objects (0 = no limit)')
    ap.add_argument('--no-prune', action='store_true')
    ap.add_argument('sources', nargs='+')
    a = ap.parse_args()

    files = [Path(s) for s in a.sources]
    files = [f for d in files for f in (sorted(d.glob('*.sql')) if d.is_dir() else [d])]

    kept = []
    for f in files:
        ddl, q = classify(statements(f.read_text(errors='replace')))
        if len(q) < a.min_queries or (a.max_ddl and len(ddl) > a.max_ddl):
            continue
        kept.append(f)
    dropped_filter = len(files) - len(kept)

    if a.sample and len(kept) > a.sample:
        # Hash-ordered rather than random.sample so adding files to the corpus does not reshuffle
        # the selection: a rerun keeps the groups already on the cluster and only extends them.
        kept.sort(key=lambda p: hashlib.sha256(f'{a.sample_seed}:{p.name}'.encode()).hexdigest())
        kept = sorted(kept[:a.sample])

    a.out.mkdir(parents=True, exist_ok=True)
    manifest, index, groups, queries = [], a.start, 0, 0
    for f in kept:
        index, packed, nq = pack_file(f, a.out, a.prefix, a.root, index, a.per_group,
                                      a.max_stmts, manifest, a.no_prune)
        groups += packed
        queries += nq

    man = a.out / f'MANIFEST-{a.prefix}.tsv'
    with man.open('w') as fh:
        fh.write('group\tsource\troot\tddl\tqueries\tstatus\n')
        for row in manifest:
            fh.write('\t'.join(str(x) for x in row) + '\n')

    ddl_counts = [r[3] for r in manifest if r[5] == 'ok']
    print(f'{a.prefix}: sources={len(kept)} (filtered out {dropped_filter}) '
          f'groups={groups} queries={queries} '
          f'index={a.start}..{index - 1} '
          f'ddl/group avg={sum(ddl_counts) / max(1, len(ddl_counts)):.1f} max={max(ddl_counts or [0])}')
    skipped = sum(1 for r in manifest if r[5] != 'ok')
    if skipped:
        print(f'  skipped {skipped} chunks with no reachable DDL (see {man.name})')
    return 0


if __name__ == '__main__':
    sys.exit(main())
