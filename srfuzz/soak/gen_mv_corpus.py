#!/usr/bin/env python3
"""Add materialized views to a corpus, because the MV rewrite family is dark without them.

Measured over a full run: MV 1/10 traced, IVM 0/13, REWRITE 3/14 -- about a third of everything
the fuzzer never reaches, and none of it reachable by mutating harder. Those rules fire when the
optimizer finds a materialized view it can answer a query from, and the harvested production
corpora carry no CREATE MATERIALIZED VIEW at all: the harvest keeps query shapes, and an MV is
schema.

The harness already handles them -- CorpusReader.isSchemaSetup accepts CreateMaterializedViewStatement
and applySchemaSetup routes it to StarRocksAssert.withMaterializedView. Only the input was missing.

    ./gen_mv_corpus.py <corpus-dir> [--per-file N] [--cluster]

--cluster produces MVs for a real cluster: MANUAL refresh with an explicit synchronous refresh, and
a staleness window, so the rewrite is not lost to the harness loading data after setup has run.

Rewrites each .sql in place, inserting MV definitions after the last CREATE TABLE. Idempotent: a
file that already carries generated MVs is left alone.
"""
import os
import re
import sys

MARKER = "-- srfuzz-generated materialized views"

# Types an aggregate can be built over, and types worth grouping BY. Grouping by a wide string is
# legal and useless -- the MV ends up with one row per row, so nothing can ever be answered from it.
NUMERIC = ("tinyint", "smallint", "int", "bigint", "largeint", "float", "double", "decimal")
GROUPABLE = ("int", "tinyint", "smallint", "bigint", "date", "datetime", "varchar", "char", "boolean")

CREATE_TABLE = re.compile(r"CREATE TABLE(?: IF NOT EXISTS)?\s+`([^`]+)`\s*\((.*?)\n\)", re.S | re.I)
COLUMN = re.compile(r"^\s*`([^`]+)`\s+([A-Za-z0-9_]+(?:\([^)]*\))?)", re.M)


def columns_of(body):
    out = []
    for m in COLUMN.finditer(body):
        name, typ = m.group(1), m.group(2).lower()
        # The column list ends where the key definition starts; those lines have no backticked
        # leading name so the regex skips them, but a defensive check costs nothing.
        if name and typ:
            out.append((name, typ))
    return out


def pick(columns, kinds, avoid=None):
    for name, typ in columns:
        if avoid and name in avoid:
            continue
        if typ.startswith(kinds):
            # A wide varchar makes a poor grouping key: it is close to unique, so the MV is as big
            # as the table and the optimizer has no reason to prefer it.
            m = re.search(r"\((\d+)\)", typ)
            if typ.startswith(("varchar", "char")) and m and int(m.group(1)) > 100:
                continue
            return name
    return None


def mvs_for(table, columns, idx, cluster=False):
    """Two MVs per table: an aggregate one and a filtered projection.

    The aggregate is what the rewrite rules are mostly about -- answering SUM/COUNT from a
    pre-aggregated view. The projection covers the simpler case and costs nothing to add.
    """
    group = pick(columns, GROUPABLE)
    measure = pick(columns, NUMERIC, avoid={group})
    out = []

    # The FE arm has no BE, so nothing ever refreshes and ASYNC costs nothing. The cluster arm does
    # refresh, and there the mode decides whether the MV is usable at all:
    #
    #   MvRewritePreprocessor.isMVValidToRewriteQuery rejects an MV when !mv.isActive(), and again
    #   when getRefreshMode().isIncrementalOrAuto() -- "query rewrite is not supported for
    #   refresh_mode=". So INCREMENTAL and AUTO are out if the goal is rewrite coverage.
    #
    # MANUAL plus an explicit synchronous refresh makes the state deterministic. ASYNC on a cluster
    # is a race against the round: setup runs, gen_data loads rows, queries follow, and an async
    # refresh that has not finished leaves the MV inactive -- rewrite silently never fires and the
    # output is indistinguishable from "MVs did not help".
    refresh = "REFRESH MANUAL" if cluster else "REFRESH ASYNC"
    # Staleness tolerance, because the ordering is against us even with a sync refresh: the cluster
    # harness loads data with gen_data AFTER setup has run, so the MV is stale by the time the
    # queries arrive. Without this the optimizer skips it and the whole exercise reports nothing.
    props = '"replication_num" = "1"'
    if cluster:
        props += ', "mv_rewrite_staleness_second" = "2592000"'

    if group and measure:
        out.append(
            "CREATE MATERIALIZED VIEW `srfuzz_mv_agg_%d`\n"
            "DISTRIBUTED BY HASH(`%s`) BUCKETS 2\n"
            "%s\n"
            "PROPERTIES (%s)\n"
            "AS SELECT `%s`, sum(`%s`) AS `s`, count(*) AS `c`\n"
            "   FROM `%s` GROUP BY `%s`;" % (idx, group, refresh, props, group, measure, table, group))
    if group:
        out.append(
            "CREATE MATERIALIZED VIEW `srfuzz_mv_proj_%d`\n"
            "DISTRIBUTED BY HASH(`%s`) BUCKETS 2\n"
            "%s\n"
            "PROPERTIES (%s)\n"
            "AS SELECT `%s`%s FROM `%s` WHERE `%s` IS NOT NULL;"
            % (idx, group, refresh, props, group, (", `%s`" % measure) if measure else "", table, group))
    return out


def process(path, per_file, cluster=False):
    with open(path) as f:
        text = f.read()
    if MARKER in text:
        return 0

    tables = CREATE_TABLE.findall(text)
    if not tables:
        return 0

    blocks = []
    idx = 0
    for table, body in tables:
        cols = columns_of(body)
        if not cols:
            continue
        for stmt in mvs_for(table, cols, idx, cluster):
            blocks.append(stmt)
            idx += 1
        if idx >= per_file:
            break
    if not blocks:
        return 0

    # After the LAST CREATE TABLE, so every base table exists when the MV is created. Appending at
    # the end of the file would put them after the queries, and the corpus reader applies setup in
    # file order -- every query would then plan against a schema with no views in it.
    last = None
    for m in CREATE_TABLE.finditer(text):
        last = m
    at = last.end()
    # Skip to the end of that statement, which the regex stops just short of.
    semi = text.find(";", at)
    at = semi + 1 if semi >= 0 else at

    if cluster:
        # One synchronous refresh per view, after every create. An MV that has never refreshed is
        # not active, and an inactive MV is rejected before any rewrite rule is considered.
        for name in re.findall(r"CREATE MATERIALIZED VIEW `([^`]+)`", "\n".join(blocks)):
            blocks.append("REFRESH MATERIALIZED VIEW `%s` WITH SYNC MODE;" % name)
    injected = "\n\n" + MARKER + "\n" + "\n".join(blocks) + "\n"
    with open(path, "w") as f:
        f.write(text[:at] + injected + text[at:])
    return len(blocks)


def main():
    if len(sys.argv) < 2:
        print("usage: %s <corpus-dir> [--per-file N]" % sys.argv[0], file=sys.stderr)
        return 2
    root = sys.argv[1]
    per_file = 4
    if "--per-file" in sys.argv:
        per_file = int(sys.argv[sys.argv.index("--per-file") + 1])
    cluster = "--cluster" in sys.argv

    files = added = 0
    for name in sorted(os.listdir(root)):
        if not name.endswith(".sql"):
            continue
        n = process(os.path.join(root, name), per_file, cluster)
        if n:
            files += 1
            added += n
    print("materialized views added: %d across %d files%s"
          % (added, files, " (cluster mode: MANUAL refresh + sync + staleness)" if cluster else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
