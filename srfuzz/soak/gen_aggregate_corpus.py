#!/usr/bin/env python3
"""Generate a seed corpus that calls every builtin aggregate and window function.

Companion to gen_function_corpus.py, which covers scalars. It cannot cover aggregates: scalars are
declared in gensrc/script/functions.py, but aggregates are registered in Java by
FunctionSet.initAggregateBuiltins(), and functions.py contains exactly zero of them. That is why 278
aggregate names were invisible to the mutator.

Reading FunctionSet.java statically is the wrong way in. It registers through loops over type arrays,
so the file gives you the loops rather than the signatures. A running FE has already evaluated those
loops, and `SHOW FULL BUILTIN FUNCTIONS` prints the result -- same registry, resolved:

    mysql -h127.0.0.1 -P9030 -uroot -N -e 'SHOW FULL BUILTIN FUNCTIONS' > registry.tsv
    ./gen_aggregate_corpus.py --registry registry.tsv --out <corpus>/T

Columns are `signature<TAB>return<TAB>type<TAB>intermediate<TAB>properties`, and `type` is one of
Scalar / Aggregate / Table.

Unlike the scalar corpus, these calls need a table: an aggregate over constants folds away and never
reaches the aggregation pipeline at all. Each file therefore carries its own CREATE TABLE, which the
soak turns into real in-process schema via CorpusReader.isSchemaSetup, and which the cluster replay
executes directly.

Every emitted call still has to be checked against a live FE (--verify). Two things here are guesses
that only a real analyzer can settle: whether a given aggregate accepts a window, and whether an
argument position demands a constant rather than a column.
"""

import argparse
import collections
import os
import re
import subprocess
import sys

TABLE = 'agg_src'

# The DDL is emitted into every corpus file. Verified accepted as written; keep it that way, since a
# rejected CREATE TABLE turns every query in the file into a stale seed and the file silently
# contributes nothing.
SETUP = """CREATE TABLE %s (
  k int, c_boolean boolean, c_tinyint tinyint, c_smallint smallint, c_int int, c_bigint bigint,
  c_largeint largeint, c_float float, c_double double, c_decimalv2 decimal(27,9),
  c_decimal32 decimal32(9,2), c_decimal64 decimal64(18,2), c_decimal128 decimal128(38,2),
  c_varchar varchar(64), c_char char(8), c_date date, c_datetime datetime,
  c_json json, c_varbinary varbinary, c_arr_int array<int>, c_arr_varchar array<varchar(16)>,
  c_map map<varchar(16),int>, c_struct struct<a int, b varchar(16)>
) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1');
""" % TABLE

# Argument type -> expression of that type over the table. Types that cannot be a column (TIME is not
# a storage type; BITMAP/HLL/PERCENTILE/VARIANT need a producer) are built from one that can.
ARG = {
    'BOOLEAN': 'c_boolean',
    'TINYINT': 'c_tinyint',
    'SMALLINT': 'c_smallint',
    'INT': 'c_int',
    'BIGINT': 'c_bigint',
    'LARGEINT': 'c_largeint',
    'FLOAT': 'c_float',
    'DOUBLE': 'c_double',
    'DECIMALV2': 'c_decimalv2',
    'DECIMAL32': 'c_decimal32',
    'DECIMAL64': 'c_decimal64',
    'DECIMAL128': 'c_decimal128',
    'DECIMAL256': 'cast(c_double as decimal256(50,2))',
    'VARCHAR': 'c_varchar',
    'CHAR': 'c_char',
    'DATE': 'c_date',
    'DATETIME': 'c_datetime',
    'TIME': "cast('03:04:05' as time)",
    'JSON': 'c_json',
    'VARBINARY': 'c_varbinary',
    'BITMAP': 'to_bitmap(c_int)',
    'HLL': 'hll_hash(c_varchar)',
    'PERCENTILE': 'percentile_hash(c_double)',
    'VARIANT': 'cast(c_json as variant)',
    # SHOW renders the ANY_* placeholders as these; ANY_ALTS below decides what they become.
    'INVALID_TYPE': 'c_int',
    'UNKNOWN_TYPE': 'c_int',
    'ARRAY<INT>': 'c_arr_int',
    'ARRAY<VARCHAR>': 'c_arr_varchar',
}

# An ANY position accepts anything, and collapsing it to one expression is what hid array_unique_agg
# (wants an array), sum_map (a map) and retention (an array of booleans). Try each in turn and let
# the analyzer say which fits. All ANY positions in one signature move together, so this costs
# len(ANY_ALTS) variants rather than a product over positions.
ANY_ALTS = ['c_int', 'c_varchar', 'c_arr_int', '[true, false]', 'c_map', 'c_json']

# Fallback constants, tried for non-leading argument positions when the column form is rejected.
# Several aggregates require a literal there -- percentile_approx's fraction, ntile's bucket count,
# lead/lag's offset -- and a column is a semantic error, not a defect.
CONST = {
    'BOOLEAN': 'true',
    'TINYINT': 'cast(1 as tinyint)',
    'SMALLINT': 'cast(1 as smallint)',
    'INT': 'cast(1 as int)',
    'BIGINT': 'cast(1 as bigint)',
    'LARGEINT': 'cast(1 as largeint)',
    'FLOAT': 'cast(0.5 as float)',
    'DOUBLE': 'cast(0.5 as double)',
    'DECIMALV2': 'cast(0.5 as decimal(27,9))',
    'DECIMAL32': 'cast(0.5 as decimal32(9,2))',
    'DECIMAL64': 'cast(0.5 as decimal64(18,2))',
    'DECIMAL128': 'cast(0.5 as decimal128(38,2))',
    'VARCHAR': "','",
    'CHAR': "','",
}

# Families used to spread the per-name cap across genuinely different code paths rather than across
# four integer widths that share one template.
FAMILY = {
    'BOOLEAN': 'bool', 'TINYINT': 'int', 'SMALLINT': 'int', 'INT': 'int', 'BIGINT': 'int',
    'LARGEINT': 'int', 'FLOAT': 'float', 'DOUBLE': 'float', 'DECIMALV2': 'decimal',
    'DECIMAL32': 'decimal', 'DECIMAL64': 'decimal', 'DECIMAL128': 'decimal',
    'DECIMAL256': 'decimal', 'VARCHAR': 'string', 'CHAR': 'string', 'DATE': 'date',
    'DATETIME': 'date', 'TIME': 'date', 'JSON': 'json', 'VARBINARY': 'binary',
    'BITMAP': 'opaque', 'HLL': 'opaque', 'PERCENTILE': 'opaque', 'VARIANT': 'variant',
}

SIG = re.compile(r'^([a-z_0-9]+)\((.*)\)$')


def parse_registry(path, want):
    """(name, [arg types]) for every signature of the requested function type."""
    out = []
    with open(path, encoding='utf-8', errors='ignore') as f:
        for line in f:
            parts = line.rstrip('\n').split('\t')
            if len(parts) < 3 or parts[2] != want:
                continue
            m = SIG.match(parts[0].strip())
            if not m:
                continue
            raw = m.group(2).strip()
            args = [a.strip() for a in raw.split(',')] if raw else []
            out.append((m.group(1), args))
    return out


ANY = ('INVALID_TYPE', 'UNKNOWN_TYPE')


def render(args, const_from=None, variadic_repeat=1, any_expr=None):
    """Argument list, or None if some type has no expression.

    const_from: first position to substitute a literal for a column.
    variadic_repeat: how many extra copies `...` contributes. It means "one or more", so the minimal
    arity is a legal call too, and for some functions it is the *only* legal one -- array_agg's
    second argument is `ORDER BY`, not a value, so array_agg(x, y) is rejected while array_agg(x)
    analyzes. Emitting only the padded form dropped those names entirely.
    """
    vals = []
    for i, a in enumerate(args):
        if a == '...':
            if vals:
                vals.extend([vals[-1]] * variadic_repeat)
            continue
        if a in ANY and any_expr is not None:
            vals.append(any_expr)
            continue
        src = ARG
        if const_from is not None and i >= const_from:
            if a not in CONST:
                return None
            src = CONST
        if a not in src:
            return None
        vals.append(src[a])
    return ', '.join(vals)


def select_signatures(sigs, cap):
    """Cap per function name while keeping type diversity: one signature per (arity, family) before
    any second signature of a family already covered."""
    by_name = collections.OrderedDict()
    for name, args in sigs:
        by_name.setdefault(name, []).append(args)

    chosen = collections.OrderedDict()
    for name, variants in by_name.items():
        seen_key, picked, spare = set(), [], []
        for args in variants:
            sig = tuple(args)
            key = (len(args), tuple(FAMILY.get(a, a) for a in args))
            if key in seen_key:
                spare.append(sig)
                continue
            seen_key.add(key)
            picked.append(sig)
        # Fill any remaining budget with the widths that were folded away, so a name with a single
        # family still contributes more than one call.
        picked = picked[:cap] + spare[:max(0, cap - len(picked))]
        chosen[name] = picked
    return chosen


def build_calls(chosen):
    """(name, sql) pairs. Both a grouped and a windowed form: which one is legal depends on the
    function and is settled by --verify, not guessed here."""
    calls, unrepresentable = [], []
    for name, variants in chosen.items():
        ok, emitted = False, set()

        def emit(arglist):
            if arglist is None or arglist in emitted:
                return False
            emitted.add(arglist)
            call = '%s(%s)' % (name, arglist)
            calls.append((name, 'SELECT k, %s FROM %s GROUP BY k;' % (call, TABLE)))
            calls.append((name, 'SELECT %s OVER (PARTITION BY k ORDER BY c_int) FROM %s;'
                          % (call, TABLE)))
            return True

        for args in variants:
            variadic = '...' in args
            anys = [x for x in ANY_ALTS] if any(a in ANY for a in args) else [None]
            # Minimal arity first, then padded: for a variadic signature both are legal shapes and
            # which one analyzes is the function's business, not something to guess here.
            for repeat in ((0, 1) if variadic else (1,)):
                for ax in anys:
                    if emit(render(list(args), variadic_repeat=repeat, any_expr=ax)):
                        ok = True
                    # Retry shapes for positions that demand a literal rather than a column.
                    # const_from=0 covers ntile, whose only argument is a bucket count.
                    for cf in (1, 0):
                        if cf <= len(args) - 1 or cf == 0:
                            emit(render(list(args), const_from=cf, variadic_repeat=repeat,
                                        any_expr=ax))
        if not ok:
            unrepresentable.append(name)
    return calls, unrepresentable


def verify(calls, db, mysql_cmd):
    """Run every call against a live FE and keep the ones that analyze.

    Attribution is by marker rather than by the line number in mysql's `ERROR ... at line N`. The
    line arithmetic happens to agree with the markers today, but it depends on every statement
    occupying exactly one line and on the setup preamble keeping its length -- a marker before each
    call is unambiguous under both.

    Without this step the corpus would carry the guesses about windowability and constant argument
    positions as if they were facts.
    """
    script = ['CREATE DATABASE IF NOT EXISTS %s;' % db, 'USE %s;' % db,
              'DROP TABLE IF EXISTS %s;' % TABLE, SETUP.replace('\n', ' ').strip()]
    setup_lines = len(script)
    for i, (_, sql) in enumerate(calls):
        script.append("SELECT '@@%d@@';" % i)
        script.append(sql)

    # One interleaved stream, not stdout+stderr concatenated: markers go to stdout and errors to
    # stderr, so concatenating puts every marker before every error and the attribution collapses
    # onto whichever marker came last. That reads as "everything passed".
    proc = subprocess.run(mysql_cmd, input='\n'.join(script) + '\n', shell=True, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    bad, reasons, cur, seen_marker = set(), collections.Counter(), None, False
    for line in proc.stdout.splitlines():
        m = re.search(r'@@(\d+)@@', line)
        if m:
            cur, seen_marker = int(m.group(1)), True
            continue
        e = re.match(r'ERROR\s+\d+\s*\([^)]*\)(?:\s*at line (\d+))?:\s*(.*)', line)
        if not e:
            continue
        if cur is None:
            # Nothing has been marked yet, so this is the setup failing. Emitting a corpus whose
            # every query references a table that was never created is worse than emitting nothing.
            sys.exit('setup failed, refusing to emit a corpus against a table that does not '
                     'exist: %s' % e.group(2))
        bad.add(cur)
        reasons[e.group(2)[:70]] += 1
    if not seen_marker:
        sys.exit('verification produced no markers -- the mysql command printed nothing usable, so '
                 '"no errors" would mean "nothing ran". Command was: %s' % mysql_cmd)
    if setup_lines and len(bad) == len(calls):
        sys.exit('every call was rejected, which means the harness is broken rather than the calls')
    return [c for i, c in enumerate(calls) if i not in bad], bad, reasons


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--registry', required=True,
                    help="TSV from `SHOW FULL BUILTIN FUNCTIONS` on a live FE")
    ap.add_argument('--out', required=True)
    ap.add_argument('--per-name', type=int, default=6,
                    help='signature cap per function name; 278 names average 15.8 overloads, most '
                         'differing only in integer width')
    ap.add_argument('--per-file', type=int, default=40)
    ap.add_argument('--verify', metavar='MYSQL_CMD',
                    help="shell command reading SQL on stdin, e.g. 'mysql -h127.0.0.1 -P9030 -uroot "
                         "--force'. Strongly recommended.")
    ap.add_argument('--verify-db', default='srfuzz_aggprobe')
    a = ap.parse_args()

    sigs = parse_registry(a.registry, 'Aggregate')
    if not sigs:
        sys.exit('no Aggregate rows in %s -- wrong file, or SHOW output format changed' % a.registry)

    chosen = select_signatures(sigs, a.per_name)
    calls, unrepresentable = build_calls(chosen)

    print('aggregate signatures : %d over %d names' % (len(sigs), len(chosen)))
    print('after per-name cap   : %d' % sum(len(v) for v in chosen.values()))
    print('candidate calls      : %d' % len(calls))

    if a.verify:
        calls, bad, reasons = verify(calls, a.verify_db, a.verify)
        print('verified on live FE  : %d kept, %d rejected' % (len(calls), len(bad)))
        for r, n in reasons.most_common(6):
            print('    %5d  %s' % (n, r))
    else:
        print('NOT VERIFIED -- windowability and constant-argument positions are guesses')

    covered = sorted({n for n, _ in calls})
    missing = sorted(set(chosen) - set(covered))

    os.makedirs(a.out, exist_ok=True)
    for i in range(0, len(calls), a.per_file):
        chunk = calls[i:i + a.per_file]
        with open(os.path.join(a.out, 'aggregate_functions_%03d.sql' % (i // a.per_file)),
                  'w', encoding='utf-8') as f:
            f.write('-- Generated by srfuzz/soak/gen_aggregate_corpus.py from a live FE registry.\n'
                    '-- One minimal legal call per aggregate signature, over a real table: an\n'
                    '-- aggregate over constants folds away without reaching the aggregation\n'
                    '-- pipeline. Breaking the arguments is the mutator\'s job.\n')
            f.write(SETUP)
            for _, sql in chunk:
                f.write(sql + '\n')

    print('corpus files         : %d in %s' % ((len(calls) + a.per_file - 1) // a.per_file, a.out))
    print('names covered        : %d of %d' % (len(covered), len(chosen)))
    if unrepresentable:
        print('no expressible call  : %d\n    %s' % (len(unrepresentable),
                                                     ' '.join(unrepresentable)))
    if missing:
        # Printed in full. A truncated list reads as a short list, and the difference between "20
        # names uncovered" and "111 names uncovered" is the difference between shipping and not.
        print('every call rejected  : %d\n    %s' % (len(missing), ' '.join(missing)))


if __name__ == '__main__':
    main()
