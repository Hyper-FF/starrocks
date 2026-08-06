#!/usr/bin/env python3
"""Fill every table in the replay databases with boundary-biased random rows.

The point of an ASAN run is the BE, and the BE only does interesting work when it has data to
read: column readers, decoders, dictionaries, aggregation and join over real rows. Empty tables
plan and return in the FE, which is why an earlier replay showed scan bytes = 0 for everything.

Values are drawn to hit edges rather than to look realistic: NULL, type minima and maxima, empty
and oversized strings, characters that matter to a parser or a dictionary encoder, and complex
values nested deeper than a hand-written test usually goes.
"""
import os
import random
import time
import re
import subprocess
import sys

MYSQL = ["mysql", "-h127.0.0.1", "-P9030", "-uroot", "-N", "-B"]
ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 200
# The seed is fixed so a round can be replayed, but it must be CHANGEABLE: with one hard-coded value
# every round of every day draws the same NULL positions, the same boundary values and the same
# strings, so the corpus keeps re-walking one slice of the value space. Change it to move the slice;
# keep it recorded so a finding stays reproducible.
GEN_SEED = int(os.environ.get("SRFUZZ_GEN_SEED", "20260731"))
random.seed(GEN_SEED)


def q(sql, db=None):
    # Feed the statement on stdin, not as an -e argument. A multi-thousand-row INSERT ... VALUES
    # blows past ARG_MAX and execve fails with "Argument list too long" before mysql even starts,
    # which the harness could only report as "data generation failed or timed out".
    cmd = MYSQL + (["-D", db] if db else [])
    r = subprocess.run(cmd, input=sql, capture_output=True, text=True, timeout=120)
    return r.returncode, r.stdout, r.stderr


def rnd_int(lo, hi):
    # weight the edges: overflow and sign-boundary handling is where the bugs are
    return random.choice([lo, hi, 0, -1, 1, random.randint(lo, hi)])


def _fit(s, maxlen):
    """Cut a string to fit varchar(maxlen), which is a limit in BYTES."""
    b = s.encode("utf-8")
    return s if len(b) <= maxlen else b[:maxlen].decode("utf-8", "ignore")


def rnd_str(maxlen):
    if random.random() < 0.15:
        return ""
    pool = "abcXYZ0123_-.,'\"\\%{}[]:/ \t\u00e9\u4e2d\u6587"
    n = random.choice([1, 2, maxlen, max(1, maxlen // 2), random.randint(1, max(1, maxlen))])
    s = "".join(random.choice(pool) for _ in range(min(n, maxlen)))
    # varchar(N) is N BYTES, and this pool is deliberately multi-byte -- ten CJK characters are 30
    # bytes in a varchar(20). An over-long value does not merely get itself rejected: it fails the
    # whole INSERT ... VALUES, so one of them costs all 4000 rows and the table stays empty. The
    # generator then exits 0 having loaded nothing, which is indistinguishable from a table it had
    # no reason to fill. Oversized strings are still worth generating, but as a value the engine
    # accepts and stores, not as one that kills the batch before it arrives.
    b = s.encode("utf-8")
    if len(b) > maxlen:
        s = b[:maxlen].decode("utf-8", "ignore")
    return s


def esc(s):
    return s.replace("\\", "\\\\").replace("'", "\\'")


def value(t, allow_null=True, pool=None):
    # NULL only where the schema allows one. A single NULL anywhere in a 4000-row INSERT ... VALUES
    # rejects the WHOLE statement ("Insert has filtered data"), so drawing NULL at 12% per value made
    # every table with a NOT NULL column -- which is most of them, since key columns usually are --
    # fail on every attempt. The generator then exited 0 having loaded nothing, and those groups ran
    # their whole round against the handful of rows the corpus setup inserted. That is precisely the
    # "scan bytes = 0, everything plans and returns in the FE" state this generator exists to prevent,
    # and it was invisible: rounds.tsv showed gen_rows=0, which reads as "nothing to load".
    # Nested values keep drawing NULLs: an ARRAY<INT> that is NOT NULL may still contain null elements.
    t = t.lower().strip()
    if allow_null and random.random() < 0.12:
        return "NULL"
    if t.startswith("tinyint"):
        return str(rnd_int(-128, 127))
    if t.startswith("smallint"):
        return str(rnd_int(-32768, 32767))
    if t.startswith("bigint"):
        return str(rnd_int(-9223372036854775808, 9223372036854775807))
    if t.startswith("largeint"):
        return str(rnd_int(-(2 ** 127), 2 ** 127 - 1))
    if t.startswith("int"):
        return str(rnd_int(-2147483648, 2147483647))
    if t.startswith("boolean"):
        return random.choice(["true", "false"])
    if t.startswith(("float", "double")):
        return random.choice(["0", "-0.0", "1e308", "-1e308", "3.4e38", str(random.uniform(-1e6, 1e6))])
    if t.startswith("decimal"):
        m = re.search(r"\((\d+)\s*,\s*(\d+)\)", t)
        p, sc = (int(m.group(1)), int(m.group(2))) if m else (10, 2)
        intpart = "9" * max(1, p - sc)
        return random.choice(["0", "-0", intpart + ("." + "9" * sc if sc else ""), str(round(random.uniform(-100, 100), min(sc, 6)))])
    if t.startswith("datetime"):
        return random.choice(["'1970-01-01 00:00:00'", "'9999-12-31 23:59:59'", "'2020-02-29 12:00:00'",
                              "'%04d-%02d-%02d %02d:%02d:%02d'" % (random.randint(1970, 2030), random.randint(1, 12),
                                                                   random.randint(1, 28), random.randint(0, 23),
                                                                   random.randint(0, 59), random.randint(0, 59))])
    if t.startswith("date"):
        return random.choice(["'1970-01-01'", "'9999-12-31'", "'2020-02-29'",
                              "'%04d-%02d-%02d'" % (random.randint(1970, 2030), random.randint(1, 12), random.randint(1, 28))])
    if t.startswith(("varchar", "char", "string", "text")):
        m = re.search(r"\((\d+)\)", t)
        maxlen = min(int(m.group(1)), 200) if m else 40
        if pool is not None:
            # Draw from a small fixed pool so the column stays a global-dictionary candidate. See
            # low_card_pool: every string this generator wrote used to be freshly random, which put
            # every string column over the threshold and made the whole low-cardinality subsystem
            # unreachable on the cluster arm.
            #
            # Truncated by BYTES, not characters -- varchar(N) is N bytes, the pool holds multi-byte
            # values, and one over-long value fails the whole INSERT ... VALUES and leaves the table
            # empty. Same trap rnd_str carries, and a character slice walks straight into it.
            return "'" + esc(_fit(pool[random.randrange(len(pool))], maxlen)) + "'"
        return "'" + esc(rnd_str(maxlen)) + "'"
    if t.startswith("json"):
        return random.choice(["'{}'", "'[]'", "'null'", "'{\"a\":1}'", "'[1,[2,[3,[4]]]]'",
                              "'{\"a\":{\"b\":{\"c\":[1,2,3]}}}'", "'{\"A\":1,\"a\":2}'"])
    if t.startswith("array"):
        inner = t[t.index("<") + 1:t.rindex(">")]
        n = random.choice([0, 1, 3])
        return "[" + ",".join(value(inner) for _ in range(n)) + "]"
    if t.startswith("map"):
        inner = t[t.index("<") + 1:t.rindex(">")]
        depth, cut = 0, -1
        for i, c in enumerate(inner):
            if c == "<":
                depth += 1
            elif c == ">":
                depth -= 1
            elif c == "," and depth == 0:
                cut = i
                break
        kt, vt = (inner[:cut], inner[cut + 1:]) if cut > 0 else ("int", "int")
        n = random.choice([0, 1, 2])
        return "map{" + ",".join("%s:%s" % (value(kt), value(vt)) for _ in range(n)) + "}"
    if t.startswith("struct"):
        inner = t[t.index("<") + 1:t.rindex(">")]
        parts, depth, cur = [], 0, ""
        for c in inner:
            if c == "<":
                depth += 1
            elif c == ">":
                depth -= 1
            if c == "," and depth == 0:
                parts.append(cur)
                cur = ""
            else:
                cur += c
        parts.append(cur)
        return "row(" + ",".join(value(p.strip().split(None, 1)[1] if len(p.strip().split(None, 1)) > 1 else "int") for p in parts) + ")"
    if t.startswith(("bitmap", "hll", "percentile")):
        return None  # not expressible as a literal
    return "NULL"


# ---------------------------------------------------------------------------------------------
# Low-cardinality string columns.
#
# The global dictionary is the entry point to a whole subsystem -- dictionary-encoded scans, the
# rewrites behind cbo_enable_low_cardinality_optimize and its family, and the dictionary MERGE that
# runs when a UNION ALL puts two dictionary columns in the same slot. None of it was reachable on
# the cluster arm: every string this generator wrote was drawn fresh from a 25-character pool at a
# random length, so every string column blew past the threshold and the optimizer never saw a
# dictionary at all.
#
# Two rules, and the second matters more than it looks.
#
# 1. Make some columns low-cardinality on purpose: draw from a pool of at most
#    LOW_CARD_POOL_MAX distinct values, well under the server's low_cardinality_threshold of 255.
#
# 2. NEVER WIDEN a column the corpus already loaded as low-cardinality. FE-side rejection is
#    PERMANENT: CacheDictManager.NO_DICT_STRING_COLUMNS is a static set with an add() and a
#    contains() and no remove() anywhere in the file, so one over-threshold column is disqualified
#    for the life of the FE process. Writing random strings into a column the corpus had already
#    made a dictionary candidate destroys it, and the corpus is where the realistic candidates come
#    from -- production seeds carry status, region and type columns with a handful of values each.
#    So: sample what is already there, and if it is narrow, keep drawing from exactly those values.
LOW_CARD_POOL_MAX = int(os.environ.get("SRFUZZ_LOWCARD_POOL", "50"))
# Share of string columns with no existing narrow values that are given a synthetic pool anyway.
# Not all of them: a run where every string column is low-cardinality is as unrepresentative as one
# where none is, and the wide path still needs to be exercised.
LOW_CARD_SHARE = float(os.environ.get("SRFUZZ_LOWCARD_SHARE", "0.5"))

_SYNTH_POOL = ["", "a", "A", "ab", "NULL", "null", "0", "1", "-1", "true", "false",
               "active", "inactive", "pending", "CN", "US", "EU", "中文", "é",
               "x" * 32, "  ", "\t", "'", "\\", "%"]


def low_card_pool(db, tbl, col, coltype, has_rows):
    """The value pool for one string column, or None to keep drawing wide random strings.

    Existing values win over a synthetic pool. A corpus column holding four statuses is a better
    dictionary candidate than anything invented here, and reusing its values preserves it instead
    of widening it.
    """
    t = (coltype or "").lower().strip()
    if not t.startswith(("varchar", "char", "string", "text")):
        return None
    if has_rows:
        # LIMIT one past the cap so "narrow" and "already too wide" are distinguishable. NULL is
        # excluded because it is not a dictionary entry and would take a slot in the sample.
        rc, out, _ = q("SELECT DISTINCT `%s` FROM `%s` WHERE `%s` IS NOT NULL LIMIT %d"
                       % (col, tbl, col, LOW_CARD_POOL_MAX + 1), db)
        if rc == 0:
            vals = [v for v in out.split("\n") if v != ""]
            if 0 < len(vals) <= LOW_CARD_POOL_MAX:
                return vals
            if len(vals) > LOW_CARD_POOL_MAX:
                # Already wide. Nothing to protect and nothing to gain -- a synthetic pool here
                # would not make it a candidate again, because the rows already written decide that.
                return None
    if random.random() >= LOW_CARD_SHARE:
        return None
    n = random.randint(2, min(LOW_CARD_POOL_MAX, len(_SYNTH_POOL)))
    return random.sample(_SYNTH_POOL, n)


def _errmsg(err):
    # mysql echoes the offending statement before the diagnostic, and a 4000-row INSERT means the
    # message itself is thousands of characters in. Keep the ERROR line, not the echo.
    e = " ".join(err.split())
    i = e.find("ERROR")
    return (e[i:i + 300] if i >= 0 else e[-300:])


GEN_LOG = "/home/disk1/fha/sr-ws/fuzz/clusterfuzz/gen_data.log"


def _log(msg):
    # Append-only, one line per event. Several instances write here concurrently; a single short
    # line written with one write() call does not interleave.
    try:
        with open(GEN_LOG, "a") as f:
            f.write("%s %s\n" % (time.strftime("%F %T"), msg))
    except OSError:
        pass


# ---------------------------------------------------------------------------------------------
# Partition-aware values.
#
# SHOW PARTITIONS reports, per partition:
#     PartitionKey: dt
#            Range: [types: [DATE]; keys: [2024-03-10]; ..types: [DATE]; keys: [2024-03-11]; )
# so the declared windows are recoverable. The lower bound of a range is ALWAYS inside it, which is
# the property this leans on: whatever the type, `lo` is a value the table accepts. Dates and
# integers additionally get interpolated inside the window so a partitioned table still sees more
# than one distinct key.
_PART_CACHE = {}


def _q_with_header(sql):
    # SHOW PARTITIONS is not a table, so the columns cannot be selected by name -- and their ORDER
    # differs between versions. Ask for the header and locate columns by name instead of by index.
    cmd = [c for c in MYSQL if c != "-N"]
    r = subprocess.run(cmd, input=sql, capture_output=True, text=True, timeout=60)
    return r.returncode, r.stdout


def _partition_spec(db, tbl):
    """(partition columns, [(lo_values, hi_values), ...]) or None when the table is not partitioned."""
    key = (db, tbl)
    if key in _PART_CACHE:
        return _PART_CACHE[key]
    spec = None
    try:
        rc, out = _q_with_header("SHOW PARTITIONS FROM `%s`.`%s`" % (db, tbl))
        lines = [l for l in out.splitlines() if l.strip()]
        if rc == 0 and len(lines) >= 2:
            hdr = lines[0].split("\t")
            ik = hdr.index("PartitionKey") if "PartitionKey" in hdr else -1
            ir = hdr.index("Range") if "Range" in hdr else -1
            cols, ranges = None, []
            for ln in lines[1:]:
                f = ln.split("\t")
                if ik < 0 or ik >= len(f) or not f[ik].strip():
                    continue
                cols = [c.strip() for c in f[ik].split(",") if c.strip()]
                if ir < 0 or ir >= len(f):
                    continue
                keys = re.findall(r"keys: \[(.*?)\]", f[ir])
                if len(keys) >= 2:
                    ranges.append(([x.strip() for x in keys[0].split(",")],
                                   [x.strip() for x in keys[1].split(",")]))
                elif len(keys) == 1:
                    # LIST partitioning: the enumerated key is itself a valid value.
                    ranges.append(([x.strip() for x in keys[0].split(",")], None))
            if cols and ranges:
                spec = (cols, ranges)
    except Exception:
        spec = None
    _PART_CACHE[key] = spec
    return spec


def _interp(lo, hi, t):
    """A value inside [lo, hi). Falls back to lo, which is always inside the range."""
    t = (t or "").lower()
    try:
        if t.startswith("date"):
            import datetime
            fmt = "%Y-%m-%d %H:%M:%S" if len(lo) > 10 else "%Y-%m-%d"
            a = datetime.datetime.strptime(lo[:19], fmt)
            if hi and hi.upper() != "MAXVALUE":
                b = datetime.datetime.strptime(hi[:19], "%Y-%m-%d %H:%M:%S" if len(hi) > 10 else "%Y-%m-%d")
                span = int((b - a).total_seconds())
                if span > 1:
                    a = a + datetime.timedelta(seconds=random.randint(0, span - 1))
            return a.strftime(fmt)
        n = int(lo)
        if hi and hi.upper() != "MAXVALUE":
            m = int(hi)
            if m - n > 1:
                return str(random.randint(n, m - 1))
        return str(n)
    except Exception:
        return lo


def _partition_value(db, tbl, col, coltype):
    """A literal for a partition column, or None when the column is not one."""
    spec = _partition_spec(db, tbl)
    if not spec:
        return None
    cols, ranges = spec
    if col not in cols:
        return None
    idx = cols.index(col)
    lo, hi = random.choice(ranges)
    if idx >= len(lo):
        return None
    v = _interp(lo[idx], hi[idx] if hi and idx < len(hi) else None, coltype)
    if v.upper() in ("MINVALUE", "MAXVALUE"):
        return None
    t = (coltype or "").lower()
    quoted = t.startswith(("date", "varchar", "char", "string", "text"))
    return ("'%s'" % esc(v)) if quoted else v


rc, dbs, _ = q("SHOW DATABASES")
targets = [d for d in dbs.split() if d.startswith("srfuzz_mut_")]

# Fill only the databases this instance owns.
#
# Without this every instance fills EVERY srfuzz_mut_* database, including the one another instance
# is in the middle of a round on. That instance's differential and TLP oracles compare a baseline
# against a variant while these rows land underneath them, so they report mismatches that this
# generator caused -- a fabricated correctness finding, which is worse than no finding at all. It
# also races the round boundary's `drop database`, and makes every generator run N times the work.
#
# The rule must be the SAME rule clusterfuzz.next.sh shards by, or an instance's own database falls
# outside its own shard and it silently loads nothing into it: cksum over the database name plus the
# trailing newline a shell here-string adds. Shelling out to cksum rather than reimplementing it,
# because a reimplementation that disagreed would be invisible from either side.
#
# An explicit database as argv[2] overrides all of it. NINSTANCES unset or 1 keeps the old behaviour
# byte for byte, so the single-instance harness is unaffected.
def _cksum(s):
    r = subprocess.run(["cksum"], input=s + "\n", capture_output=True, text=True)
    return int(r.stdout.split()[0])


DB = sys.argv[2] if len(sys.argv) > 2 else None
NINST = int(os.environ.get("NINSTANCES") or 1)
INST = int(os.environ.get("INSTANCE") or 0)
# preflight builds srfuzz_mut_preflight<INSTANCE> and then asserts this generator loaded rows into
# it. Hashing that name would hand it to some other instance, every instance would load nothing into
# its own probe, and all of them would abort with "loaded 0 rows into a fresh table" -- a harness
# change that reads as a broken cluster. A probe database belongs to the instance that named itself
# in it.
_PROBE_DB = re.compile(r"^srfuzz_mut_(?:preflight|repprobe)(\d+)$")


def _owned(d):
    m = _PROBE_DB.match(d)
    if m:
        return int(m.group(1)) == INST
    return _cksum(d) % NINST == INST


if DB:
    targets = [d for d in targets if d == DB]
elif NINST > 1:
    targets = [d for d in targets if _owned(d)]
print("databases: %d (instance %s of %s) seed=%s" % (len(targets), INST, NINST, GEN_SEED), flush=True)
_log("RUN inst=%s/%s seed=%s rows=%s targets=%s" % (INST, NINST, GEN_SEED, ROWS, ",".join(targets)[:200]))
filled = skipped = 0
# Counted and printed: a low-cardinality mode nobody can see the effect of is indistinguishable from
# one that silently never fired, which is how the whole subsystem stayed unreachable in the first place.
lowcard_cols = analyze_failed = 0
for db in targets:
    # SHOW TABLES lists views and materialised views alongside real tables, and inserting into either
    # is rejected -- "the data of a materialized view must be consistent with the base table". Every
    # such attempt was a harness-manufactured planner error in the FE log, indistinguishable in the
    # report from an engine defect. Ask for base tables only.
    rc, tbls, _ = q("SELECT TABLE_NAME FROM information_schema.tables "
                    "WHERE TABLE_SCHEMA = '%s' AND TABLE_TYPE = 'BASE TABLE'" % db, db)
    for tbl in tbls.split():
        rc, desc, err = q("DESC `%s`" % tbl, db)
        if rc != 0:
            continue
        cols, bad = [], False
        for line in desc.strip().splitlines():
            f = line.split("\t")
            if len(f) < 2:
                continue
            v = value(f[1])
            if v is None:
                bad = True
                break
            nullable = len(f) < 3 or f[2].strip().upper() != "NO"
            cols.append((f[0], f[1], nullable))
        if bad or not cols:
            skipped += 1
            continue
        # Whether the corpus already put rows here decides whether a string column's existing values
        # are worth preserving, so ask once per table rather than once per column.
        rc, cnt, _ = q("SELECT COUNT(*) FROM `%s`" % tbl, db)
        has_rows = rc == 0 and cnt.strip().isdigit() and int(cnt.strip()) > 0
        pools = {c[0]: low_card_pool(db, tbl, c[0], c[1], has_rows) for c in cols}
        lowcard = sum(1 for p in pools.values() if p is not None)
        rows = []
        for _ in range(ROWS):
            vals = []
            for c in cols:
                pv = _partition_value(db, tbl, c[0], c[1])
                vals.append(pv if pv is not None else value(c[1], c[2], pools.get(c[0])))
            rows.append("(" + ",".join(vals) + ")")
        sql = "INSERT INTO `%s` VALUES %s" % (tbl, ",".join(rows))
        rc, _, err = q(sql, db)
        if rc == 0:
            filled += 1
            lowcard_cols += lowcard
            # Statistics, so the cost model has something to work with. Without them the optimizer
            # has no reason to choose among the plans a dictionary makes available, and half the
            # low-cardinality rewrites are cost-gated. Failure is logged, not fatal: a table with no
            # statistics still holds the rows this generator came to write.
            rc2, _, err2 = q("ANALYZE TABLE `%s`" % tbl, db)
            if rc2 != 0:
                analyze_failed += 1
                _log("ANALYZE-FAIL %s.%s :: %s" % (db, tbl, _errmsg(err2)))
        else:
            skipped += 1
            _log("REJECT %s.%s rc=%s cols=%s :: %s" % (
                db, tbl, rc, ",".join("%s:%s" % (c[0], c[1]) for c in cols)[:200],
                _errmsg(err)))
print("filled=%d skipped=%d lowcard_cols=%d analyze_failed=%d"
      % (filled, skipped, lowcard_cols, analyze_failed), flush=True)
_log("DONE filled=%d skipped=%d lowcard_cols=%d analyze_failed=%d"
     % (filled, skipped, lowcard_cols, analyze_failed))
