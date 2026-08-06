#!/usr/bin/env python3
"""Generate a seed corpus that calls every builtin function once.

The corpus is the ceiling for anything the mutator cannot manufacture, and function calls are where
most of this campaign's defects have been: 12 of 15 located so far. But none of them were found
because a function had never been *called* -- they were found because a mutator broke an argument.
`tokenize` got a TIME where a tokenizer name belongs, `ngram_search` a JSON where an int belongs,
`to_tera_timestamp` an empty string with a format starting in punctuation, `generate_series` a
distance of exactly min() with step -1.

So the value of this file is not the calls themselves. It is that every function reaches the mutator
at all: 81 of 288 builtins never appear anywhere in the SQL-Tester corpus, so no amount of argument
mutation was ever going to reach them. Emit one minimal legal call per signature and let M3/M7 do the
breaking.

Reads gensrc/script/functions.py. Each entry is
    [id, name, is_vectorized, is_variadic, return_type, [arg_types], be_symbol, prepare?, close?]
"""

import argparse
import os
import re
import sys

# One literal per declared argument type. Deliberately boring: the point is to produce a call that
# *analyzes*, so the mutator has a valid tree to work from. Interesting values are the mutator's job.
LITERAL = {
    'BOOLEAN': 'true',
    'TINYINT': 'cast(1 as tinyint)',
    'SMALLINT': 'cast(1 as smallint)',
    'INT': 'cast(1 as int)',
    'BIGINT': 'cast(1 as bigint)',
    'LARGEINT': 'cast(1 as largeint)',
    'FLOAT': 'cast(1.5 as float)',
    'DOUBLE': 'cast(1.5 as double)',
    'DECIMALV2': 'cast(1.5 as decimalv2(10,2))',
    'DECIMAL32': 'cast(1.5 as decimal32(9,2))',
    'DECIMAL64': 'cast(1.5 as decimal64(18,2))',
    'DECIMAL128': 'cast(1.5 as decimal128(38,2))',
    'DECIMAL256': 'cast(1.5 as decimal128(38,2))',
    'VARCHAR': "'abc'",
    'DATE': "cast('2020-01-02' as date)",
    'DATETIME': "cast('2020-01-02 03:04:05' as datetime)",
    'TIME': "cast('03:04:05' as time)",
    'JSON': "parse_json('{\"k\":1}')",
    'VARBINARY': "to_binary('abc')",
    'BITMAP': 'to_bitmap(1)',
    'HLL': 'hll_hash(1)',
    'PERCENTILE': 'percentile_hash(1.0)',
    'ANY_ELEMENT': 'cast(1 as int)',
    'ANY_ARRAY': '[1, 2]',
    'ANY_MAP': "map{1:'a'}",
    'ANY_STRUCT': "row(1, 'a')",
    'MAP_VARCHAR_VARCHAR': "map{'a':'b'}",
}

# ARRAY_<T> is spelled out rather than derived, because an empty or NULL-typed array does not analyze
# the same way a populated one does, and a seed that does not analyze is dropped.
ARRAY_LITERAL = {
    'BOOLEAN': '[true, false]',
    'TINYINT': '[cast(1 as tinyint), cast(2 as tinyint)]',
    'SMALLINT': '[cast(1 as smallint), cast(2 as smallint)]',
    'INT': '[cast(1 as int), cast(2 as int)]',
    'BIGINT': '[cast(1 as bigint), cast(2 as bigint)]',
    'LARGEINT': '[cast(1 as largeint), cast(2 as largeint)]',
    'FLOAT': '[cast(1.5 as float)]',
    'DOUBLE': '[cast(1.5 as double)]',
    'DECIMALV2': '[cast(1.5 as decimalv2(10,2))]',
    'DECIMAL32': '[cast(1.5 as decimal32(9,2))]',
    'DECIMAL64': '[cast(1.5 as decimal64(18,2))]',
    'DECIMAL128': '[cast(1.5 as decimal128(38,2))]',
    'VARCHAR': "['a', 'b']",
    'DATE': "[cast('2020-01-02' as date)]",
    'DATETIME': "[cast('2020-01-02 03:04:05' as datetime)]",
    'JSON': "[parse_json('1')]",
}

# Functions that cannot produce a useful seed here, with the reason. Keeping the reason inline so the
# next person does not have to re-derive why something is absent.
SKIP = {
    'ai_query': 'needs an external model service',
    'esquery': 'Elasticsearch external table only',
    'host_name': 'environment-dependent, differs per BE',
    'current_version': 'environment-dependent',
    'dict_encode': 'internal, requires a global dictionary',
    'dict_decode': 'internal, requires a global dictionary',
}
SKIP_PREFIX = ('__',)          # __iceberg_transform_* and friends are internal
# A lambda argument cannot be written as a literal; these need a real lambda body and are left to the
# hand-written corpus.
SKIP_ARG_TYPES = {'FUNCTION'}


def literal_for(t):
    if t in LITERAL:
        return LITERAL[t]
    if t.startswith('ARRAY_'):
        return ARRAY_LITERAL.get(t[len('ARRAY_'):])
    return None


def parse_signatures(path):
    text = open(path, encoding='utf-8', errors='ignore').read()
    sigs = []
    pattern = re.compile(
        r"\[\s*\d+\s*,\s*'([a-z_0-9]+)'\s*,[^,]*,[^,]*,\s*'([A-Z_0-9]+)'\s*,\s*\[([^\]]*)\]")
    for m in pattern.finditer(text):
        name, ret, raw = m.group(1), m.group(2), m.group(3)
        args = re.findall(r"'([A-Z_0-9.]+)'", raw)
        sigs.append((name, ret, args))
    return sigs


def build_call(name, args):
    """A call for one signature, or None when the signature cannot be expressed as literals."""
    if name in SKIP or name.startswith(SKIP_PREFIX):
        return None
    values = []
    for a in args:
        if a == '...':
            # Variadic: the fixed part is already emitted, one more of the previous type is enough to
            # exercise the variadic path.
            if values:
                values.append(values[-1])
            continue
        if a in SKIP_ARG_TYPES:
            return None
        lit = literal_for(a)
        if lit is None:
            return None
        values.append(lit)
    return '%s(%s)' % (name, ', '.join(values))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--functions', default='gensrc/script/functions.py')
    ap.add_argument('--out', required=True, help='corpus directory to write into')
    ap.add_argument('--per-file', type=int, default=40,
                    help='calls per corpus file; one giant file would make a single shard the tail')
    a = ap.parse_args()

    sigs = parse_signatures(a.functions)
    if not sigs:
        sys.exit('no signatures parsed from %s -- the file format changed' % a.functions)

    calls, skipped = [], {}
    seen = set()
    for name, ret, args in sigs:
        call = build_call(name, args)
        if call is None:
            skipped.setdefault(name, SKIP.get(name, 'unrepresentable argument type'))
            continue
        if call in seen:
            continue
        seen.add(call)
        calls.append((name, call))

    os.makedirs(a.out, exist_ok=True)
    written = 0
    for i in range(0, len(calls), a.per_file):
        chunk = calls[i:i + a.per_file]
        path = os.path.join(a.out, 'builtin_functions_%03d.sql' % (i // a.per_file))
        with open(path, 'w', encoding='utf-8') as f:
            f.write('-- Generated by srfuzz/soak/gen_function_corpus.py from gensrc/script/functions.py.\n'
                    '-- One minimal legal call per signature, so every builtin reaches the mutator.\n'
                    '-- The calls are deliberately boring: breaking the arguments is M3/M7\'s job, and\n'
                    '-- that is how every function defect in this campaign was actually found.\n')
            for name, call in chunk:
                f.write('SELECT %s;\n' % call)
        written += 1

    names = sorted({n for n, _ in calls})
    print('signatures parsed : %d' % len(sigs))
    print('distinct calls    : %d over %d function names' % (len(calls), len(names)))
    print('corpus files      : %d in %s' % (written, a.out))
    print('skipped functions : %d' % len(skipped))
    for n in sorted(skipped)[:12]:
        print('    %-32s %s' % (n, skipped[n]))


if __name__ == '__main__':
    main()
