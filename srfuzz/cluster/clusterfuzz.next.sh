#!/usr/bin/env bash
# Replay mutated queries against a live StarRocks cluster and watch for the failures the FE-only
# fuzzer structurally cannot see: BE crashes, planner internal errors, and hangs.
#
# Runs the corpus as MIXED LOAD, not as a script. Every crash recorded in HANDOFF_CRASH_A.md happened
# while data generation and queries overlapped, and every attempt to reproduce one by running a single
# statement on its own failed. So writers and readers run concurrently by construction.
#
# Heartbeat first: $RUN/status always answers "what is it doing right now". A soak whose only output
# arrives at the end of a round is indistinguishable from a dead one -- learned the hard way.

set -u

W=/home/disk1/fha/sr-ws/fuzz
OUT=$W/clusterfuzz
CORPUS=$OUT/emit
BELOG=$W/output/be/log/be.out
MYSQL="mysql -h127.0.0.1 -P9030 -uroot"

# ---------------------------------------------------------------------------------------------
# Multi-instance sharding.
#
# One instance covers 1767 groups at roughly 90s a round, which is a 44-hour pass over the corpus,
# on a 104-core box whose load average sits around 2. The cluster is not the limit; the serial loop
# is. N instances walk disjoint slices of the corpus against the one FE/BE.
#
# NINSTANCES=1 keeps the single-instance layout byte-for-byte, so nothing moves for an existing run.
NINSTANCES=${NINSTANCES:-1}
INSTANCE=${INSTANCE:-0}
case "$NINSTANCES" in ''|*[!0-9]*) echo "NINSTANCES must be a number" >&2; exit 1 ;; esac
case "$INSTANCE" in ''|*[!0-9]*) echo "INSTANCE must be a number" >&2; exit 1 ;; esac
[ "$NINSTANCES" -ge 1 ] || { echo "NINSTANCES must be >= 1" >&2; exit 1; }
[ "$INSTANCE" -lt "$NINSTANCES" ] || { echo "INSTANCE must be < NINSTANCES" >&2; exit 1; }

# Per-instance state, shared findings.
#
# Everything a round writes under a FIXED name -- rounds.tsv, status, the log, and every scratch file
# -- has to be per instance, or two instances silently overwrite each other's accounting and the
# harness reports numbers that were never true. That failure mode has cost this campaign nine
# incidents; it is the reason this split exists rather than a lock.
#
# The signature and findings files stay SHARED on purpose: dedup is only useful if it is global, and
# a defect found by instance 3 must not be reported again by instance 1. Concurrent claims on them
# go through claim_signature.
RUN=$OUT
[ "$NINSTANCES" -gt 1 ] && RUN=$OUT/inst$INSTANCE

FELOG=$W/output/fe/log/fe.warn.log
FEDIR=$W/output/fe/log
FESIG=$OUT/fe-signatures.tsv
SETUPSIG=$OUT/setup-failures.tsv
CRASHSIG=$OUT/crash-signatures.txt
DIFFSIG=$OUT/diff-signatures.txt
FINDINGS=$OUT/findings.md
ERRSIG=$OUT/error-signatures.tsv
BELOCK=$OUT/be-restart.lock
FELOCK=$OUT/fe-restart.lock
STATUS=$RUN/status
LOG=$RUN/clusterfuzz.log
STATE=$RUN/rounds.tsv

READERS=3
WRITERS=2
PHASE_SECONDS=45
# Row ceiling for a shared benchmark table before writers stop growing it.
BENCH_ROW_CAP=${BENCH_ROW_CAP:-12000000}
QUERY_TIMEOUT=60
# A result set is captured into a shell variable; bash cannot survive an unbounded one.
DIFF_MAX_BYTES=${DIFF_MAX_BYTES:-4000000}
# Beyond this a per-statement sweep costs more than the round it came from.
MINIMIZE_MAX_STMTS=120
# Rows per table for the boundary-biased generator. Enough to span several pipeline chunks and to
# let a low-cardinality column actually get a global dictionary collected for it -- which is what the
# AGG_STATE dict-encoding crash needed and what duplicating existing rows could never produce.
GEN_ROWS=4000
# Not /tmp: it gets wiped, and when it does the harness degrades silently to "run against whatever
# the corpus loaded" -- amplification stopped for hundreds of rounds without anything looking broken.
GEN_DATA=${GEN_DATA:-$(dirname "$0")/gen_data.py}

mkdir -p "$OUT" "$RUN"
touch "$FINDINGS" "$ERRSIG" "$FESIG" "$SETUPSIG" "$CRASHSIG" "$DIFFSIG"

# Claim a signature, or report that someone else already has it.
#
# The plain "grep -Fxq then append" this replaces is a read-modify-write on a file several instances
# append to. Two of them can both miss and both append, which reports one defect twice and puts two
# near-identical blocks in findings.md. The lock is per signature file and held for a grep over a
# small file, so it costs nothing measurable.
#
#   $1 file, $2 cut spec for the key fields, $3 key to look for, $4 line to append when it is new
# Returns 0 when THIS caller claimed it (so the caller should report), 1 when it was already known.
claim_signature() {
    local file=$1 spec=$2 key=$3 line=$4 rc=1
    {
        flock 9
        if ! cut -f"$spec" "$file" 2>/dev/null | grep -Fxq "$key"; then
            printf '%s\n' "$line" >> "$file"
            rc=0
        fi
    } 9>>"$file.lock"
    return $rc
}
STATE_HEADER=$'round\tgroup\ttables\tsetup_fail\tgen_rows\tqueries\terrors\tdiff_checked\tdiff_bad\tdiff_empty\tdiff_void\tdiff_skipped\ttlp_checked\ttlp_bad\ttlp_skipped\tfatal_delta\tbe_restarts\tnew_fe_sigs\tsecs'
# Appending wider rows to a file written under the old header produces a ragged TSV: every awk that
# reads it by column index silently reports the wrong field, and a harness that lies about its own
# numbers is what nine of this campaign's incidents were made of. Rotate instead, loudly.
if [ -f "$STATE" ]; then
    if [ "$(head -1 "$STATE")" != "$STATE_HEADER" ]; then
        for n in $(seq 1 99); do
            [ -e "$STATE.$n" ] && continue
            mv "$STATE" "$STATE.$n"
            printf '%s\n' "$STATE_HEADER" > "$STATE"
            echo "rounds.tsv had the previous column set; rotated to $(basename "$STATE").$n" >&2
            break
        done
    fi
else
    printf '%s\n' "$STATE_HEADER" > "$STATE"
fi

TAG=''
[ "$NINSTANCES" -gt 1 ] && TAG="[i$INSTANCE] "
say() { printf '%s %s%s\n' "$(date '+%F %T')" "$TAG" "$*" | tee -a "$LOG"; }

# grep -c exits 1 on a zero count, so a bare `|| echo 0` prints TWO zeros and corrupts every
# arithmetic use of the result. Force one line, always.
fatal_count() { grep -cE 'Check failed|SIGSEGV|SIGABRT|SIGBUS|AddressSanitizer' "$BELOG" 2>/dev/null | head -1 | tr -dc '0-9' | sed 's/^$/0/'; }
# A live process is not a live backend. starrocks_be accepts a pid ~instantly but takes ~30s to
# register with the FE, and queries in that window fail with "Backend node not found" -- one round
# restarted the BE, saw pgrep succeed, and logged 79128 bogus errors plus 4 bogus FE signatures
# against a backend that was not serving yet. Ask the FE whether the backend is Alive, as fe_alive
# asks the FE whether it can answer a query.
be_proc_alive() { pgrep -x starrocks_be >/dev/null 2>&1; }
be_alive() {
    be_proc_alive || return 1
    timeout 20 $MYSQL -N -e 'show backends' 2>/dev/null | awk -F'\t' '{print $9}' | grep -qi true
}
# Wait for the backend to actually start serving. Returns non-zero if it never does.
be_wait_registered() {
    local i
    for i in $(seq 1 "${1:-30}"); do
        be_alive && return 0
        sleep 5
    done
    return 1
}

# The FE is the half this harness forgot. When it died, every round still "completed" -- 602 of them,
# seven hours, every query an ECONNREFUSED counted as an error -- and nothing said so. Liveness here
# is "answers a query", not "a process matches the name": the process that matched was the start
# script's shell wrapper, 3.8 MB and one thread, while the JVM was long gone.
fe_alive() { timeout 15 $MYSQL -N -e 'select 1' >/dev/null 2>&1; }

# Locked for the same reason restart_be is, and it matters more here: the frontend is what every
# instance connects THROUGH, so when it dies every instance notices at once. Unlocked, each one runs
# stop_fe.sh -- and one instance's stop kills the frontend another has just brought up, so a single
# death can keep the cluster down for as long as they keep racing. The liveness check is repeated
# inside the lock because by the time an instance gets it, the frontend is usually already back.
restart_fe() {
    local rc
    {
        flock 9
        if fe_alive; then
            say "FE already back (restarted by another instance)"
            rc=0
        else
            restart_fe_locked
            rc=$?
        fi
    } 9>>"$FELOCK"
    return $rc
}

restart_fe_locked() {
    say "restarting FE"
    export JAVA_HOME=${JAVA_HOME:-/lib/jvm/java-21-openjdk}
    "$W/output/fe/bin/stop_fe.sh" >/dev/null 2>&1
    sleep 5
    # 9>&- : the lock this block holds lives on fd 9, and a daemon started here INHERITS it. The
    # backend then holds the restart lock for as long as it runs, so the next restart -- from any
    # instance or from the watchdog -- blocks in flock forever. That does not look like a deadlock
    # from outside: the instance process is alive, it just never finishes another round, which reads
    # as "slow" rather than "stuck". Close the fd for the child.
    nohup "$W/output/fe/bin/start_fe.sh" --daemon >/dev/null 2>&1 9>&-
    for _ in $(seq 1 24); do
        sleep 10
        if fe_alive; then say "FE back up"; return 0; fi
    done
    say "FE DID NOT COME BACK -- pausing 300s"
    sleep 300
    return 1
}

# Under a lock, and re-checking after taking it.
#
# The backend is the one thing every instance shares. Without the lock, N instances that all notice
# the same crash all call stop_be/start_be at once, and they fight: one instance's stop_be kills the
# backend another has just brought up, so a single crash can leave the cluster down for as long as
# they keep racing. With it, the first instance restarts and the rest find the backend already Alive
# and return -- which is why the check is repeated INSIDE the lock rather than trusted from before it.
restart_be() {
    local rc
    {
        flock 9
        if be_alive; then
            say "BE already back (restarted by another instance)"
            rc=0
        else
            restart_be_locked
            rc=$?
        fi
    } 9>>"$BELOCK"
    return $rc
}

restart_be_locked() {
    say "restarting BE"
    "$W/output/be/bin/stop_be.sh"  >/dev/null 2>&1
    sleep 3
    # setsid, not just nohup. Without its own session the backend stays in this harness's process
    # group, so stopping the harness the obvious way -- kill the process group -- takes the BE down
    # with it. That looks nothing like a mistake afterwards: the next start fails preflight with
    # "BE is not registered and Alive", which reads as a cluster problem rather than as the stop
    # having been too wide.
    # 9>&- : the lock this block holds lives on fd 9, and a daemon started here INHERITS it. The
    # backend then holds the restart lock for as long as it runs, so the next restart -- from any
    # instance or from the watchdog -- blocks in flock forever. That does not look like a deadlock
    # from outside: the instance process is alive, it just never finishes another round, which reads
    # as "slow" rather than "stuck". Close the fd for the child.
    setsid nohup "$W/output/be/bin/start_be.sh" --daemon >/dev/null 2>&1 9>&-
    for _ in $(seq 1 30); do
        sleep 5
        if $MYSQL -e 'show backends\G' 2>/dev/null | grep -qE '^ *Alive: true'; then
            say "BE back up"
            return 0
        fi
    done
    say "BE DID NOT COME BACK -- pausing 300s"
    sleep 300
    return 1
}

# A crash is the finding. Capture enough to act on: the stack, and which group was running.
# Every crash banner in be.out, keyed by the first starrocks frame under it. Counting fatal markers
# and writing ONE record per round loses every distinct crash after the first: one archived log holds
# 68 crash markers against 10 recorded entries, and mining these signatures by hand turned up three
# crash families that were never triaged at all. Dedup by signature so each distinct crash is seen once.
# ---------------------------------------------------------------------------------------------
# Differential oracle: the same query under two session settings must return the same rows.
#
# Until now the oracles were "did it crash" and "did the deparse round-trip". Nothing checked that
# results were RIGHT, and this campaign found at least two defects that are silently wrong answers,
# caught only because they also happened to crash: the JSON set-operation column that appended
# nothing (a release no-op stub, so a chunk escaping memory accounting would simply return fewer
# rows) and approx_top_k's shifted constant arguments (a CHECK on a debug build, a garbage k on
# release). Anything in that class that does not also crash is invisible to us.
#
# A knob is the cheapest oracle available: it is meant to change the plan, not the answer. If
# flipping one changes the rows, one of the two plans is wrong -- and no expected output is needed.
#
# The first four knobs were chosen because this campaign produced evidence for them:
#   - low-cardinality rewriting has two independent implementations and V1 has no agg-state guard
#   - cbo_push_down_aggregate_mode is what PlanTestBase disables, which is exactly why the
#     push-down-distinct-below-window defect survived several rounds of unit tests
#   - query cache has three known correctness holes found by hand, all of this shape
#
# That list was retrospective: every knob in it was added after something else had already found the
# defect, so the differential was regression-proofing three optimizer features rather than exploring.
# Predicate pushdown, join reorder, CTE reuse, partition/table pruning, runtime filters and the agg
# rewrites had no knob at all, which is a structural reason no defect in them could ever be reported.
#
# The pool below covers those areas. Every name was read out of `show variables` on the live FE
# before being added -- see validate_knobs and incident 8 for why a name nobody checked is worse than
# no knob at all. Two classes are deliberately EXCLUDED:
#   - knobs that make a query fail rather than differ (enable_cross_join, enable_nested_loop_join):
#     the error path returns nothing, and nothing is indistinguishable from an empty result.
#   - knobs whose rewrite is legitimately approximate (enable_count_distinct_rewrite_by_hll_bitmap,
#     count_distinct_implementation): a difference there is the feature working, not a defect.
#
# A knob is only a valid oracle if it is meant to change the PLAN and not the ANSWER. Anything added
# here has to satisfy that, or it manufactures false findings faster than it finds real ones.
DIFF_KNOBS=${DIFF_KNOBS:-"\
set cbo_enable_low_cardinality_optimize = false|\
set low_cardinality_optimize_v2 = false|\
set cbo_enable_low_cardinality_optimize_for_join = false|\
set array_agg_low_cardinality_optimize = false|\
set enable_low_cardinality_optimize_for_union_all = false|\
set array_low_cardinality_optimize = false|\
set struct_low_cardinality_optimize = false|\
set cbo_push_down_aggregate_mode = -1|\
set enable_query_cache = true|\
set enable_predicate_move_around = false|\
set enable_predicate_reorder = true|\
set enable_fine_grained_range_predicate = true|\
set cbo_derive_join_is_null_predicate = false|\
set cbo_derive_range_join_predicate = true|\
set enable_predicate_col_late_materialize = false|\
set disable_join_reorder = true|\
set enable_outer_join_reorder = false|\
set enable_inner_join_to_semi = false|\
set enable_ukfk_join_reorder = true|\
set enable_join_reorder_before_deduplicate = true|\
set cbo_max_reorder_node_use_exhaustive = 1|\
set enable_partition_hash_join = false|\
set enable_hash_join_range_direct_mapping_opt = false|\
set cbo_cte_reuse = false|\
set enable_rbo_table_prune = true|\
set enable_cbo_table_prune = true|\
set enable_dynamic_prune_scan_range = false|\
set enable_rewrite_partition_column_minmax = false|\
set enable_filter_unused_columns_in_scan_stage = false|\
set cbo_prune_json_subfield = false|\
set enable_prune_complex_types = false|\
set enable_eliminate_agg = false|\
set enable_cost_based_multi_stage_agg = false|\
set cbo_push_down_distinct_below_window = false|\
set enable_distinct_agg_over_window = false|\
set new_planner_agg_stage = 2|\
set streaming_preaggregation_mode = force_preaggregation|\
set enable_sort_aggregate = true|\
set enable_split_topn_agg = false|\
set enable_rewrite_sum_by_associative_rule = false|\
set enable_rewrite_simple_agg_to_meta_scan = false|\
set enable_rewrite_or_to_union_all_join = true|\
set enable_rewrite_groupingsets_to_union_all = true|\
set enable_global_runtime_filter = false|\
set enable_topn_runtime_filter = false|\
set enable_multicolumn_global_runtime_filter = false|\
set runtime_filter_on_exchange_node = true|\
set enable_group_execution = false|\
set enable_local_shuffle_agg = false|\
set enable_group_by_compressed_key = false|\
set push_down_heavy_exprs = false|\
set enable_lambda_pushdown = false|\
set enable_materialized_view_rewrite = false"}
DIFF_MAX_STMTS=${DIFF_MAX_STMTS:-40}

# How many knobs each statement is checked against, drawn at random from the pool above.
#
# Running every statement against every knob would multiply the differential's cost by the size of
# the pool -- 50 knobs x 40 statements is 2000 comparisons per group, and a round that takes ten
# times as long finds less, not more. Sampling keeps the per-round cost where it was while letting
# coverage accumulate across rounds: a defect that needs one specific knob is found a few rounds
# later rather than never, and the campaign runs continuously.
#
# The knob that fired is recorded in the finding, so a sampled hit is exactly as reproducible.
DIFF_KNOB_SAMPLE=${DIFF_KNOB_SAMPLE:-4}

# Rows, normalised. A query without ORDER BY may return them in any order, and comparing raw output
# would report every such query as a mismatch.
diff_run() {
    local db=$1 setup=$2 sql=$3
    printf '%s;\n%s\n' "$setup" "$sql" \
        | timeout 60 $MYSQL "$db" -N -B 2>/dev/null \
        | LC_ALL=C sort \
        | head -c "$DIFF_MAX_BYTES"
}

# True when a statement's answer is allowed to change between two runs, so a difference proves
# nothing about the plan.
# True when some LIMIT sits in a query block that has no ORDER BY of its own. Parenthesis
# depth stands in for query block: good enough for the corpus, and it errs toward skipping,
# which costs coverage rather than manufacturing findings.
limit_unordered_in_its_block() {
    python3 - "$1" <<'PYEOF'
import re, sys
sql = sys.argv[1].lower()
sql = re.sub(r"'(?:[^'\\]|\\.)*'", "''", sql)
depth = 0
ordered = [False]
for m in re.finditer(r"\(|\)|\border\s+by\b|\blimit\b", sql):
    tok = m.group(0)
    if tok == "(":
        depth += 1
        if len(ordered) <= depth:
            ordered.append(False)
        else:
            ordered[depth] = False
    elif tok == ")":
        depth = max(0, depth - 1)
    elif tok.startswith("order"):
        ordered[depth] = True
    else:
        if not ordered[depth]:
            sys.exit(0)
sys.exit(1)
PYEOF
}

diff_skippable() {
    if grep -qiE '\b(rand|random|now|current_timestamp|current_date|curdate|curtime|uuid|last_query_id|connection_id|current_user)\b' <<< "$1"; then
        return 0
    fi
    # LIMIT without ORDER BY. WHICH rows come back is undefined, so two plans returning different
    # ones is correct behaviour, not a defect -- and diff_run's sort cannot help, because it
    # normalises the ORDER of a row set and this is the row set itself changing.
    #
    # Found the honest way: the first mismatch the widened knob pool reported was
    #   ... CROSS JOIN (... UNION ALL ...) LIMIT 7, 1
    # under enable_partition_hash_join=false, with one row on each side and different contents. The
    # old four-knob pool could hit this too; it just changed the plan rarely enough that it never did.
    #
    # ORDER BY alone is not a guarantee either -- a non-unique sort key leaves ties free to come back
    # in either order, so a LIMIT over one can still differ legitimately. That residual is accepted
    # rather than skipped: excluding every LIMIT would drop a large part of the corpus, and ties are a
    # far narrower hole than no ordering at all.
    # Asked per query block, not per statement: an outer ORDER BY cannot order rows a LIMIT
    # below it already threw away. 7 of the first 8 reported mismatches were that shape.
    if limit_unordered_in_its_block "$1"; then
        return 0
    fi
    return 1
}

# Reject a knob the server does not know, at startup, loudly.
#
# The first version of this list carried "enable_low_cardinality_optimize", which is not a session
# variable at all -- the real ones are cbo_enable_low_cardinality_optimize and
# low_cardinality_optimize_v2. Setting it made mysql error, diff_run returned nothing, and the empty
# result was indistinguishable from "this query returns no rows", so that third of the differential
# coverage never ran and never complained.
# ---------------------------------------------------------------------------------------------
# Preflight: assert the things this harness silently assumed and got wrong.
#
# Every one of these checks exists because the assumption behind it broke without a single line in
# the report changing. The pattern has repeated nine times in this campaign, so the invariants are
# now checked rather than assumed:
#
#   - default_replication_num was 3 on a one-BE cluster, so 171 of 2203 corpus CREATE TABLEs failed.
#     Fixed once in fe.conf -- and then `build.sh --fe` regenerated fe.conf from the source template
#     and silently wiped it, which is why it is re-asserted here instead of trusted.
#   - gen_data.py built its INSERT as a command-line argument and died on ARG_MAX for hundreds of
#     rounds, reported only as "data generation failed or timed out".
#   - A differential knob that the server rejects returns nothing, which is indistinguishable from a
#     query with no rows, so a third of the coverage never ran (see validate_knobs).
# Total rows across a database's base tables. Used to tell "the generator ran" from "the generator
# loaded something", which are not the same thing and were conflated for hundreds of rounds.
db_row_total() {
    timeout 60 $MYSQL -N -e "select ifnull(sum(table_rows),0) from information_schema.tables
        where table_schema='$1' and table_type='BASE TABLE'" 2>/dev/null | tr -dc '0-9' | sed 's/^$/0/'
}

preflight() {
    local bad=0

    # 1. Replication. Runtime-settable, so fix it rather than refuse to start -- but say so, because
    #    silently correct is how it came back the second time.
    # Test the behaviour, not the reported value: what matters is whether a corpus CREATE TABLE that
    # omits replication_num succeeds, and parsing `admin show frontend config` output was itself a
    # wrong assumption the first time this check was written.
    rep_probe() {
        timeout 30 $MYSQL -e "drop database if exists srfuzz_mut_repprobe${INSTANCE}; create database srfuzz_mut_repprobe${INSTANCE}" >/dev/null 2>&1
        timeout 30 $MYSQL srfuzz_mut_repprobe${INSTANCE} -e "create table r(k int) duplicate key(k) distributed by hash(k) buckets 1" >/dev/null 2>&1
        local ok=$?
        timeout 30 $MYSQL -e "drop database if exists srfuzz_mut_repprobe${INSTANCE}" >/dev/null 2>&1
        return $ok
    }
    if ! rep_probe; then
        say "PREFLIGHT: a CREATE TABLE without replication_num fails -- on a single-BE cluster that is"
        say "           default_replication_num != 1, and 8% of corpus groups cannot build their schema."
        timeout 20 $MYSQL -e "admin set frontend config ('default_replication_num'='1')" >/dev/null 2>&1
        grep -q '^default_replication_num' "$W/output/fe/conf/fe.conf" 2>/dev/null \
            || echo 'default_replication_num = 1' >> "$W/output/fe/conf/fe.conf"
        if rep_probe; then
            say "preflight: default_replication_num repaired (build.sh --fe regenerates fe.conf and wipes it)"
        else
            say "PREFLIGHT FAILED: still cannot create a table without replication_num"; bad=1
        fi
    fi

    # 2. The data generator must exist AND actually work. "Exists" was not enough: it existed and
    #    failed on every invocation.
    if [ ! -f "$GEN_DATA" ]; then
        say "PREFLIGHT FAILED: $GEN_DATA missing -- every group would run against whatever the corpus loaded"
        bad=1
    else
        # gen_data only targets databases named srfuzz_mut_*, so the probe has to be one.
        local probe=srfuzz_mut_preflight${INSTANCE}
        timeout 30 $MYSQL -e "drop database if exists $probe; create database $probe" >/dev/null 2>&1
        # Integer columns only. The generator deliberately emits oversized strings, so a varchar(20)
        # probe would reject its own INSERT and this check would blame the generator for working.
        timeout 30 $MYSQL "$probe" -e "create table p(k int, v bigint) duplicate key(k) distributed by hash(k) buckets 1" >/dev/null 2>&1
        if ! timeout 120 python3 "$GEN_DATA" 50 >/dev/null 2>&1; then
            say "PREFLIGHT FAILED: $GEN_DATA ran but exited non-zero -- rows would never be loaded"
            bad=1
        else
            local n
            n=$(timeout 30 $MYSQL "$probe" -N -e "select count(*) from p" 2>/dev/null | tr -dc '0-9')
            if [ "${n:-0}" -eq 0 ]; then
                say "PREFLIGHT FAILED: $GEN_DATA loaded 0 rows into a fresh table"
                bad=1
            else
                say "preflight: data generator loaded $n rows into a probe table"
            fi
        fi
        timeout 30 $MYSQL -e "drop database if exists $probe" >/dev/null 2>&1
    fi

    # 3. The BE has to be serving, not merely running -- a pid is not a backend.
    be_alive || { say "PREFLIGHT FAILED: BE is not registered and Alive"; bad=1; }

    [ "$bad" -eq 0 ] || { say "Preflight failed. Fix the above, then restart -- a run that starts anyway looks healthy and measures nothing."; exit 1; }
    say "preflight OK"
}

validate_knobs() {
    local knob bad=0
    while IFS= read -r knob; do
        [ -z "$knob" ] && continue
        if ! timeout 20 $MYSQL -N -e "$knob" >/dev/null 2>&1; then
            say "FATAL: differential knob rejected by the server: $knob"
            bad=1
        fi
    done <<< "$(tr '|' '\n' <<< "$DIFF_KNOBS")"
    [ "$bad" -eq 0 ] || { say "Fix DIFF_KNOBS before running: a knob that errors is silently skipped."; exit 1; }
    say "differential knobs validated: $(tr '|' '\n' <<< "$DIFF_KNOBS" | grep -c .)"
}

# Results come back in globals, NOT on stdout.
#
# This used to `printf "$checked $mismatched"` and the caller read it through a command substitution
# -- which also captures everything say() prints, because say() writes to stdout. So the moment the
# differential actually found something, the say() line describing the finding was parsed as the
# counters, and the round recorded diff_checked="2026-08-03". The oracle corrupted its own accounting
# exactly when it worked, and silently agreed with itself every other time. Globals cannot be
# captured by accident, and the function now has to run in this shell rather than a subshell.
differential_phase() {
    local g=$1 gname=$2 db=$3 round=$4
    local checked=0 mismatched=0 emptybase=0 voidknob=0 skipped=0
    local knob stmt base var sig knobs

    # Split on the statement terminator, not on newlines. The mutant corpus writes one statement per
    # line, but a benchmark query is a multi-line TPC-DS statement with no trailing newline, so a
    # line-oriented read saw zero statements in it and the differential silently checked nothing.
    while IFS= read -r -d ';' stmt; do
        [ "$checked" -ge "$DIFF_MAX_STMTS" ] && break
        stmt=${stmt%;}
        [ -z "$stmt" ] && continue
        grep -qiE '^[[:space:]]*(select|with)\b' <<< "$stmt" || continue
        # Counted, like every other thing this oracle declines to look at. A statement skipped as
        # non-deterministic is coverage that did not happen, and an uncounted skip is how a harness
        # comes to look busier than it is.
        if diff_skippable "$stmt"; then
            skipped=$((skipped + 1))
            continue
        fi

        base=$(diff_run "$db" "set enable_profile = false" "$stmt")
        # An error, an empty result or a timeout leaves nothing to compare; the error oracle owns those.
        #
        # Counted, not just skipped. This branch is the differential's blind spot: a statement whose
        # baseline is empty is never compared against anything, and until it was counted nobody could
        # say whether that was 2% of the corpus or 60% of it. diff_checked alone cannot tell a round
        # that compared little from a round that had little to compare -- the same ambiguity that let
        # incident 8 hide a third of the coverage. If diff_empty dominates diff_checked, the corpus is
        # generating queries that select nothing and the oracle is running on air.
        if [ -z "$base" ]; then
            emptybase=$((emptybase + 1))
            continue
        fi
        # At the cap the capture is truncated, and two truncated streams differ for reasons
        # that have nothing to do with the knob. Skip it -- and count the skip, so a corpus
        # full of huge result sets shows up as lost coverage instead of as agreement.
        if [ "${#base}" -ge "$DIFF_MAX_BYTES" ]; then
            skipped=$((skipped + 1))
            continue
        fi
        checked=$((checked + 1))

        # A random sample of the pool rather than all of it -- see DIFF_KNOB_SAMPLE.
        knobs=$(tr '|' '\n' <<< "$DIFF_KNOBS" | grep . | shuf -n "$DIFF_KNOB_SAMPLE")
        while IFS= read -r knob; do
            [ -z "$knob" ] && continue
            var=$(diff_run "$db" "$knob" "$stmt")
            # The baseline had rows and this run did not. That is not "no difference": it is the
            # statement erroring, timing out, or the knob being rejected -- exactly the shape of
            # incident 8, where a knob the server did not know returned nothing and the emptiness was
            # read as agreement. validate_knobs catches an unknown name at startup; this catches the
            # same silence arising at run time, when only a counter can show it.
            if [ -z "$var" ]; then
                voidknob=$((voidknob + 1))
                continue
            fi
            [ "$base" = "$var" ] && continue
            mismatched=$((mismatched + 1))
            sig="diff:${knob}"
            # Full detail the first time a knob differs, a one-liner after that. With a four-knob pool
            # every mismatch was worth writing out; with fifty, a knob that disagrees on most of a
            # group's statements would bury every other finding in the file under near-identical
            # blocks. The repeat line still records the statement, so nothing is lost -- but the
            # signature stays the unit of triage, which is what makes findings.md readable at all.
            if ! claim_signature "$DIFFSIG" 1 "$sig" "$sig"; then
                printf -- '- repeat %s  round %s  group %s  `%s`\n' \
                    "$sig" "$round" "$gname" "$(cut -c1-160 <<< "$stmt")" >> "$FINDINGS"
            else
                {
                    printf '\n## RESULT DIFFERS UNDER A SESSION KNOB  round %s  group %s  %s\n\n' \
                        "$round" "$gname" "$(date '+%F %T')"
                    printf 'knob: `%s`\n\n```sql\n%s\n```\n\n' "$knob" "$stmt"
                    printf 'baseline rows: %s, with knob: %s\n\n' \
                        "$(printf '%s' "$base" | grep -c .)" "$(printf '%s' "$var" | grep -c .)"
                    printf 'first differing lines:\n```\n%s\n```\n' \
                        "$(diff <(printf '%s\n' "$base") <(printf '%s\n' "$var") | head -6)"
                } >> "$FINDINGS"
                say "  RESULT DIFF [$knob] :: $(cut -c1-110 <<< "$stmt")"
            fi
            break
        done <<< "$knobs"
    done < <(cat "$g.query.sql"; printf ';')

    DIFF_CHECKED=$checked
    DIFF_BAD=$mismatched
    DIFF_EMPTY=$emptybase
    DIFF_VOID=$voidknob
    DIFF_SKIPPED=$skipped
}

# ---------------------------------------------------------------------------------------------
# TLP: an oracle that does not need a second plan to disagree with the first.
#
# The knob differential can only see a defect that exactly ONE of two plans gets wrong. A rule that
# is wrong the same way however the query is planned -- the common case, because a bad rule fires in
# every plan that reaches it -- returns identical rows under every knob and is structurally invisible
# to it. Ternary Logic Partitioning needs no second plan at all.
#
# For any predicate p, three-valued logic is total: every row satisfies exactly one of `p`, `NOT p`,
# `p IS NULL`. So for every table t and every p,
#
#     SELECT * FROM t
#   ==  (SELECT * FROM t WHERE p)
#       UNION ALL (SELECT * FROM t WHERE NOT (p))
#       UNION ALL (SELECT * FROM t WHERE (p) IS NULL)
#
# as multisets. The right-hand side is the point: it is three predicated scans, so it goes through
# predicate pushdown, range extraction, zone-map and partition pruning and every rule that only fires
# when a scan carries a predicate. Those are precisely the rules this campaign has never reached,
# because the corpus supplies almost no predicates of its own. A mismatch is a wrong answer, proved
# without a reference implementation and without any expected output.
#
# Each limit below is a false-positive source if it is removed:
#   - `SELECT *` only. An aggregate does not distribute over the partition -- the SUM of the parts is
#     not a part of the SUM -- so the law does not hold and a mismatch would prove nothing.
#   - No LIMIT, no ORDER BY: a LIMIT applies per branch. Both sides are sorted before comparison, so
#     row order is not part of the claim.
#   - The predicate's constant is sampled out of the column itself, so the split is non-trivial. A
#     predicate that matches every row or no row still satisfies the law but exercises no pruning.
#   - Tables above TLP_MAX_ROWS are skipped, and the skip is COUNTED and reported. A silent cap is
#     how a harness comes to report coverage it never had.
TLP_ENABLE=${TLP_ENABLE:-1}
TLP_MAX_TABLES=${TLP_MAX_TABLES:-4}
TLP_MAX_ROWS=${TLP_MAX_ROWS:-20000}

# Types a comparison predicate is meaningful and total for. JSON, ARRAY, MAP, STRUCT, BITMAP, HLL,
# PERCENTILE and the binary types are excluded: a comparison against one either errors (which returns
# nothing, which reads as agreement) or is not a total order, and neither makes a usable oracle.
tlp_type_ok() {
    case "$1" in
        tinyint|smallint|int|integer|bigint|largeint|float|double|boolean) return 0 ;;
        decimal|decimalv2|decimal32|decimal64|decimal128|date|datetime|timestamp) return 0 ;;
        char|varchar|string) return 0 ;;
        *) return 1 ;;
    esac
}

tlp_numeric() {
    case "$1" in
        tinyint|smallint|int|integer|bigint|largeint|float|double|boolean) return 0 ;;
        decimal|decimalv2|decimal32|decimal64|decimal128) return 0 ;;
        *) return 1 ;;
    esac
}

# Like differential_phase, this reports through globals rather than stdout, so that a say() from
# inside it can never be read back as a counter.
tlp_phase() {
    local gname=$1 db=$2 round=$3
    local checked=0 bad=0 skipped=0
    local t rows col ctype v off p base part sig ops op nrows

    TLP_CHECKED=0
    TLP_BAD=0
    TLP_SKIPPED=0
    [ "$TLP_ENABLE" = "1" ] || return

    while IFS= read -r t; do
        [ -z "$t" ] && continue
        nrows=$(timeout 30 $MYSQL "$db" -N -B -e "select count(*) from \`$t\`" 2>/dev/null | tr -dc '0-9')
        [ -z "$nrows" ] && { skipped=$((skipped + 1)); continue; }
        # An empty table satisfies the law trivially and tests nothing.
        [ "$nrows" -eq 0 ] && { skipped=$((skipped + 1)); continue; }
        if [ "$nrows" -gt "$TLP_MAX_ROWS" ]; then
            skipped=$((skipped + 1))
            say "  TLP skip $t: $nrows rows > TLP_MAX_ROWS=$TLP_MAX_ROWS"
            continue
        fi

        # One column, one predicate per table per round. Another round draws differently.
        read -r col ctype <<< "$(timeout 30 $MYSQL -N -B -e \
            "select COLUMN_NAME, DATA_TYPE from information_schema.columns
             where TABLE_SCHEMA='$db' and TABLE_NAME='$t'" 2>/dev/null \
            | while read -r c ty; do tlp_type_ok "$(tr 'A-Z' 'a-z' <<< "$ty")" \
                && printf '%s %s\n' "$c" "$(tr 'A-Z' 'a-z' <<< "$ty")"; done | shuf -n1)"
        [ -z "${col:-}" ] && { skipped=$((skipped + 1)); continue; }

        off=$((RANDOM % nrows))
        v=$(timeout 30 $MYSQL "$db" -N -B -e \
            "select \`$col\` from \`$t\` where \`$col\` is not null limit 1 offset $off" 2>/dev/null | head -1)
        # The offset can land past the non-null rows; fall back to the first one before giving up.
        [ -z "$v" ] && v=$(timeout 30 $MYSQL "$db" -N -B -e \
            "select \`$col\` from \`$t\` where \`$col\` is not null limit 1" 2>/dev/null | head -1)
        [ -z "$v" ] && { skipped=$((skipped + 1)); continue; }

        if tlp_numeric "$ctype"; then
            # Anything that is not plainly a number would have to be quoted, and guessing which is
            # how a harness starts manufacturing its own syntax errors.
            grep -qE '^-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?$' <<< "$v" || { skipped=$((skipped + 1)); continue; }
        else
            # A quote or a backslash would need escaping rules this does not implement. Skipping is
            # cheap; getting the escaping subtly wrong produces a "finding" that is the harness's own
            # bad SQL, and this campaign has already spent days on findings of that kind.
            case "$v" in *\'*|*\\*) skipped=$((skipped + 1)); continue ;; esac
            v="'$v'"
        fi

        ops=("<" ">" "=" "<>")
        op=${ops[$((RANDOM % 4))]}
        p="\`$col\` $op $v"

        base="select * from \`$t\`"
        part="(select * from \`$t\` where $p)
              union all (select * from \`$t\` where not ($p))
              union all (select * from \`$t\` where ($p) is null)"

        local lhs rhs
        lhs=$(diff_run "$db" "set enable_profile = false" "$base")
        [ -z "$lhs" ] && { skipped=$((skipped + 1)); continue; }
        rhs=$(diff_run "$db" "set enable_profile = false" "$part")
        # Empty here is the partitioned form failing, not agreeing. Same trap as the differential's
        # void counter: silence is not a pass.
        if [ -z "$rhs" ]; then
            skipped=$((skipped + 1))
            say "  TLP void on $t.$col ($ctype $op): partitioned form returned nothing"
            continue
        fi
        checked=$((checked + 1))
        [ "$lhs" = "$rhs" ] && continue

        bad=$((bad + 1))
        # Dedup on the shape, not the table: a defect in DATE range extraction and one in VARCHAR
        # comparison are different bugs, while the same defect seen on forty tables is one.
        sig="tlp:${ctype}:${op}"
        if ! claim_signature "$DIFFSIG" 1 "$sig" "$sig"; then
            printf -- '- repeat %s  round %s  group %s  table %s\n' "$sig" "$round" "$gname" "$t" >> "$FINDINGS"
        else
            {
                printf '\n## TLP PARTITION MISMATCH  round %s  group %s  %s\n\n' \
                    "$round" "$gname" "$(date '+%F %T')"
                printf 'A row of `%s` satisfies exactly one of `p`, `NOT p`, `p IS NULL`, so these must\n' "$t"
                printf 'return the same multiset. They do not.\n\n'
                printf 'predicate: `%s`   column type: `%s`   table rows: %s\n\n' "$p" "$ctype" "$nrows"
                printf '```sql\n-- baseline\n%s;\n\n-- partitioned\n%s;\n```\n\n' "$base" "$part"
                printf 'baseline rows: %s, partitioned rows: %s\n\n' \
                    "$(printf '%s' "$lhs" | grep -c .)" "$(printf '%s' "$rhs" | grep -c .)"
                printf 'first differing lines:\n```\n%s\n```\n' \
                    "$(diff <(printf '%s\n' "$lhs") <(printf '%s\n' "$rhs") | head -8)"
            } >> "$FINDINGS"
            say "TLP MISMATCH round=$round group=$gname table=$t predicate=$p"
        fi
    done <<< "$(timeout 30 $MYSQL -N -B -e "select TABLE_NAME from information_schema.tables
                    where TABLE_SCHEMA='$db' and TABLE_TYPE='BASE TABLE'" 2>/dev/null | shuf -n "$TLP_MAX_TABLES")"

    TLP_CHECKED=$checked
    TLP_BAD=$bad
    TLP_SKIPPED=$skipped
}

crash_signatures() {
    awk '
      # A deliberate stop is not a crash: SIGTERM prints the same banner and would
      # otherwise be filed as starrocks::sigterm_handler.
      /stack trace:/ { if ($0 ~ /SIGTERM/) { inc=0; next } inc=1; n=0; next }
      inc {
        if ($0 ~ /starrocks::/ && $0 !~ /FailureSignalHandler|failure_function|ThreadPool::dispatch|Thread::supervise/) {
          s=$0
          match(s, /starrocks::[A-Za-z_:~<>]+/)
          if (RSTART>0) { print substr(s, RSTART, RLENGTH); inc=0 }
        }
        if (++n > 14) inc=0
      }
    ' "$BELOG" 2>/dev/null | sort -u
}

# Record any crash signature this run has not reported yet, each with the statement that produced it.
record_new_crash_signatures() {
    local round=$1 group=$2 sig
    crash_signatures | while IFS= read -r sig; do
        [ -z "$sig" ] && continue
        claim_signature "$CRASHSIG" 1 "$sig" "$sig" || continue
        record_crash_detail "$round" "$group" "$sig"
    done
}

record_crash_detail() {
    local round=$1 group=$2 sig=$3
    # NOT tail: the BE restarts after it dies, so by the time this runs the end of be.out is the new
    # process's startup banner, not the stack. Reporting that banner is what made a real SIGSEGV in
    # to_tera_timestamp look like a false positive for a whole day. Seek to the LAST crash marker and
    # print forward from a few lines above it, which is where glog puts query_id and fragment id.
    # Seek to the banner of the crash that MATCHES this signature, not merely the last crash in the
    # file: one round can produce several distinct crashes, and pairing a signature with someone
    # else's stack is worse than printing none.
    local line start
    line=$(awk -v sig="$sig" '
        /\*\*\* (SIGSEGV|SIGABRT|SIGBUS|Aborted)|Check failed|ERROR: AddressSanitizer/ { cand=NR; want=1; n=0; next }
        want && index($0, sig) { print cand; want=0 }
        want && ++n > 20 { want=0 }
    ' "$BELOG" 2>/dev/null | tail -1)
    [ -z "$line" ] && line=$(grep -nE '\*\*\* (SIGSEGV|SIGABRT|SIGBUS|Aborted)|Check failed|ERROR: AddressSanitizer' "$BELOG" 2>/dev/null | tail -1 | cut -d: -f1)
    start=$(( ${line:-1} - 6 )); [ "$start" -lt 1 ] && start=1

    # The crash banner carries the query_id of the statement that killed the BE. Resolving it against
    # the FE audit log names the exact SQL, so there is nothing left to bisect -- the harness used to
    # minimize by halving the query file while the answer sat one grep away.
    local qid stmt=""
    # Both spellings: the banner writes "query_id:<uuid>", glog's FATAL line "query_id=<uuid>".
    # The banner's is all zeros when the aborting thread had no query runtime state, so reject the
    # zero uuid and take the first real one in the window -- the FATAL line sits inside it already.
    qid=$(sed -n "${start},\$p" "$BELOG" 2>/dev/null \
          | grep -oE 'query_id[:=][0-9a-f-]+' | sed 's/^query_id[:=]//' \
          | grep -vE '^[0-]+$' | head -1)
    if [ -n "$qid" ]; then
        stmt=$(grep -h "$qid" "$FEDIR"/fe.audit.log* 2>/dev/null | head -1 \
               | grep -oE 'Stmt=.*' | sed 's/|Digest=.*//' | cut -c1-2000)
    fi
    {
        printf '\n## BE CRASH  round %s  group %s  %s\n\n' "$round" "$group" "$(date '+%F %T')"
        printf 'signature: `%s`\n\n' "$sig"
        if [ -n "$stmt" ]; then
            printf 'CRASHING STATEMENT (query_id %s):\n\n```sql\n%s\n```\n\n' "$qid" "${stmt#Stmt=}"
        elif [ -n "$qid" ]; then
            printf 'query_id %s -- not found in the FE audit log\n\n' "$qid"
        else
            printf 'no usable query_id near the crash (banner and FATAL line both absent or zero)\n\n'
        fi
        printf 'stack:\n\n```\n'
        sed -n "${start},$(( start + 45 ))p" "$BELOG" 2>/dev/null
        printf '```\n'
    } >> "$FINDINGS"
    if [ -n "$stmt" ]; then
        say "BE CRASH round=$round group=$group sig=$sig :: ${stmt#Stmt=}"
    else
        say "BE CRASH round=$round group=$group sig=$sig (no stmt resolved)"
    fi
}

# Errors the mixed load produces on its own. They say nothing about the engine, and reporting them
# would bury the ones that do.
benign_error() {
    grep -qiE 'Unknown database|is not found|does not exist|Table .* was dropped|no partition|Query timeout|timeout|closed connection|Lost connection|has been closed|Failed to send|version.*not found|tablet.*not found' <<< "$1"
}

fe_log_size() { wc -c < "$FELOG" 2>/dev/null | tr -dc '0-9' | sed 's/^$/0/'; }

# Everything the FE logged during this round that carries a stack, keyed by exception class plus the
# first starrocks frame under it. This is the oracle the harness spent its whole first life not
# reading: F1 through F4 were all logged here during rounds this script recorded as errors=0.
fe_log_signatures() {
    awk '
      match($0, /[A-Za-z_]+(Exception|Error)/) {
          pending = substr($0, RSTART, RLENGTH); look = 10; next
      }
      pending && look > 0 {
          look--
          if (match($0, /at com\.starrocks\.[A-Za-z0-9_.$]+\([A-Za-z0-9_]+\.java:[0-9]+\)/)) {
              print pending "\t" substr($0, RSTART + 3, RLENGTH - 3)
              pending = ""
          }
      }
    ' "$1" 2>/dev/null | sort -u
}

# FE-log signatures produced by the harness itself rather than by the engine. Filtered by THROW SITE,
# not by exception class: a SemanticException from MetaUtils is a table this replay dropped, while a
# SemanticException from the analyzer may well be a finding, and collapsing them by class would throw
# the second away with the first.
benign_fe_signature() {
    grep -qE 'MysqlChannel\.|MySQLReadListener\.|ConnectProcessor\.|ConnectScheduler\.|LocalMetastore\.createOlapTablets|DDLStmtExecutor|SimpleExecutor\.executeDDL|TableKeeper|MetaUtils\.getSessionAwareTable|StatisticsMetaManager|StatisticAutoCollector|CheckpointController|JournalWriter' <<< "$1"
}

# Errors that exist only because the BE died mid-round. They are consequences of a crash already
# recorded above, and reporting them as findings buries the crash that caused them under its own
# fallout -- which is what the first run of this harness did.
crash_fallout() {
    grep -qiE 'Backend node not found|Backend node.*not alive|cancelled by crash of backends|inBlacklist: true|Tablet lost replicas|backend is down|Check if any backend' <<< "$1"
}

# Run one statement (or a few) from a group and say whether it still triggers what we are chasing.
# Single statements first because most crashes are one statement; if none of them does it alone, the
# honest answer is "needs the concurrent phase", not a wrong minimal case.
minimize_group() {
    local g=$1 gname=$2 db=$3 mode=$4 want=$5 before_fatal=$6
    local total; total=$(grep -c ';' "$g.query.sql" 2>/dev/null || echo 0)
    [ "${total:-0}" -eq 0 ] && return 1
    [ "$total" -gt "$MINIMIZE_MAX_STMTS" ] && { say "  minimize: $gname has $total statements, over the cap"; return 1; }

    local i=0 line
    while IFS= read -r line; do
        i=$((i + 1))
        [ -z "${line// }" ] && continue
        printf '%s\n' "$line" > "$RUN/min.sql"
        local fb; fb=$(fatal_count)
        local fe_b; fe_b=$(fe_log_size)
        timeout "$QUERY_TIMEOUT" $MYSQL "$db" -f < "$RUN/min.sql" >/dev/null 2>&1
        if [ "$mode" = crash ]; then
            local fa; fa=$(fatal_count)
            if [ "$fa" -gt "$fb" ] || ! be_alive; then
                say "  MINIMIZED $gname to statement $i/$total"
                { printf '\n### minimized to one statement (line %s of %s)\n\n```sql\n%s\n```\n' \
                    "$i" "$total" "$(cut -c1-1200 <<< "$line")"; } >> "$FINDINGS"
                restart_be
                return 0
            fi
        else
            local fe_a; fe_a=$(fe_log_size)
            [ "$fe_a" -lt "$fe_b" ] && fe_b=0
            tail -c +$((fe_b + 1)) "$FELOG" 2>/dev/null > "$RUN/min.felog"
            if fe_log_signatures "$RUN/min.felog" | grep -Fq "$want"; then
                say "  MINIMIZED $gname to statement $i/$total for: $want"
                { printf '\n### minimized to one statement (line %s of %s)\n\n```sql\n%s\n```\n' \
                    "$i" "$total" "$(cut -c1-1200 <<< "$line")"; } >> "$FINDINGS"
                return 0
            fi
        fi
    done < "$g.query.sql"

    say "  minimize: no single statement of $gname reproduces it -- needs the concurrent phase"
    printf '\n### not reducible to one statement: every statement of %s was run alone and none reproduced it\n' \
        "$gname" >> "$FINDINGS"
    return 1
}

round=$(( $(wc -l < "$STATE") - 1 ))
[ "$round" -lt 0 ] && round=0
groups=()
while IFS= read -r f; do groups+=("${f%.setup.sql}"); done < <(ls "$CORPUS"/*.setup.sql 2>/dev/null)
[ "${#groups[@]}" -eq 0 ] && { say "no corpus in $CORPUS"; exit 1; }

# The database a group runs in, derived exactly as the round loop derives it. Kept next to that code
# in intent even though it lives here: if the two ever disagree, sharding stops being disjoint and
# two instances quietly share a database.
group_db() {
    local g=$1 gname bench
    gname=$(basename "$g")
    bench=$(sed -n 's/^-- benchmark-db: *//p' "$g.setup.sql" 2>/dev/null | head -1)
    if [ -n "$bench" ]; then
        printf '%s' "$bench"
        return
    fi
    local db="srfuzz_mut_$(sed -E 's/^([a-z0-9]+_)?mut_0*//' <<< "$gname")"
    [ "$db" = "srfuzz_mut_" ] && db="srfuzz_mut_0"
    printf '%s' "$db"
}

# Shard by DATABASE, not by group index.
#
# The obvious `index % NINSTANCES` is wrong here, and silently so. The database name strips the
# generation prefix and the leading zeros -- deep2_mut_007 and mut_007 BOTH resolve to srfuzz_mut_7 --
# so a stride over group indices routinely puts two groups that share one database on two different
# instances, which then create, populate and drop that database underneath each other. Hashing the
# database name instead guarantees that everything touching a database lands on one instance.
#
# It also settles the benchmark databases for free: every bench_* group carries the same
# `-- benchmark-db:` marker, so all of them hash to a single instance. That is exactly the constraint
# those groups need -- bench_tpch and bench_tpcds are shared and never dropped, so two instances
# running them at once would have one instance's writers amplifying a table while the other compares
# it, and every differential and TLP result on it would be a false mismatch.
if [ "$NINSTANCES" -gt 1 ]; then
    mine=()
    for g in "${groups[@]}"; do
        h=$(cksum <<< "$(group_db "$g")" | cut -d' ' -f1)
        [ $(( h % NINSTANCES )) -eq "$INSTANCE" ] && mine+=("$g")
    done
    say "sharding: instance $INSTANCE of $NINSTANCES owns ${#mine[@]} of ${#groups[@]} groups"
    [ "${#mine[@]}" -eq 0 ] && { say "this instance owns no groups -- lower NINSTANCES"; exit 1; }
    groups=("${mine[@]}")
fi

validate_knobs
preflight
say "corpus: ${#groups[@]} groups, readers=$READERS writers=$WRITERS phase=${PHASE_SECONDS}s"

while true; do
    round=$((round + 1))
    g="${groups[$(( (round - 1) % ${#groups[@]} ))]}"
    gname=$(basename "$g")
    # The corpus is deparsed with full qualification, including the throwaway database the FE-only
    # run used -- `srfuzz_mut_7`.`t`. Replaying it under any other database name makes every table
    # reference unresolvable, which showed up as 14248 "Unknown table" errors and nothing else. The
    # driver names that database "srfuzz_mut_<i>" for the same <i> the emit file is numbered with,
    # so recreating it under that name is what makes the corpus mean anything here.
    # Strip any generation prefix before the index. Corpora regenerated by a newer mutator are added
    # alongside the old ones under names like deep2_mut_007 so they do not overwrite them, and the
    # index is what has to survive: the SQL says `srfuzz_mut_7` regardless of which generation the
    # file came from. Deriving the database from the whole basename instead produced
    # srfuzz_mut_deep2_mut_007, and every query in all 882 renamed groups failed with
    # "Unknown database 'srfuzz_mut_7'" -- 54% of the corpus generating nothing but noise.
    # A benchmark group runs against a shared database that already holds real TPC data in native OLAP
    # tables -- 28M rows across 32 tables, materialised once from the built-in benchmark catalog. Those
    # must not be dropped and rebuilt each round, and need no setup replay. The point of them is that
    # the data is real: predicates get meaningful selectivity instead of matching everything or nothing,
    # and the scan goes through OlapChunkSource rather than the handful of rows a corpus file creates.
    benchdb=$(sed -n 's/^-- benchmark-db: *//p' "$g.setup.sql" 2>/dev/null | head -1)
    if [ -n "$benchdb" ]; then
        db="$benchdb"
    else
        db="srfuzz_mut_$(sed -E 's/^([a-z0-9]+_)?mut_0*//' <<< "$gname")"
        [ "$db" = "srfuzz_mut_" ] && db="srfuzz_mut_0"
    fi
    started=$(date +%s)
    restarts=0

    printf 'round %s RUNNING since %s\n  group %s  db %s\n  live: tail -f %s\n' \
        "$round" "$(date '+%F %T')" "$gname" "$db" "$LOG" > "$STATUS"

    if ! fe_alive; then
        printf 'round %s BLOCKED: FE down\n' "$round" > "$STATUS"
        restart_fe || continue
        restarts=$((restarts + 1))
    fi
    if ! be_alive; then
        # Restarting, not crashing. The previous round already recorded whatever killed it, and the
        # liveness probe must not fire again while the restart is settling.
        # A process that is merely starting needs waiting for, not restarting: give it a window to
        # register before tearing it down, so a round never runs against a backend that is still
        # coming up. Every query in that window fails for a reason that has nothing to do with SQL.
        if be_proc_alive && be_wait_registered 12; then
            say "BE was still registering; proceeded without restart"
        else
            restart_be || { printf 'round %s BLOCKED: BE down\n' "$round" > "$STATUS"; continue; }
        fi
        restarts=$((restarts + 1))
    fi
    before=$(fatal_count)
    fe_before=$(fe_log_size)

    if [ -z "$benchdb" ]; then
        $MYSQL -e "drop database if exists $db; create database $db" >/dev/null 2>&1
    fi
    # The emitted setup is the corpus file's whole DDL history flattened, so it ends in the file's
    # FINAL schema -- and a third of the corpus drops a table after its first query. mut_001 creates
    # target_table on line 15 and drops it on line 32 while every one of its queries reads it: replay
    # the drop and setup "succeeds" while 15302 queries fail on a table that was not meant to be gone.
    #
    # Stripping EVERY drop was the first workaround, and it became the largest source of setup failure:
    # a `DROP TABLE t; CREATE TABLE t(...)` pair mid-file collapses into two bare CREATEs, so the
    # second one dies with "Table already exists" and the group replays against the OLD schema.
    #
    # So decide per object: keep the drop when the same object is created again later in the file
    # (net effect is the object exists in its later form), strip it when nothing re-creates it.
    # The real fix belongs in the emitter, which should record the setup prefix each seed was
    # collected under; this keeps the cluster useful until that lands.
    awk '
    function objname(line,   l, n) {
        l = tolower(line)
        sub(/^[ \t]+/, "", l)
        if (l !~ /^(drop|create)[ \t]/) return ""
        sub(/^(drop|create)[ \t]+/, "", l)
        sub(/^(external[ \t]+)?(temporary[ \t]+)?(materialized[ \t]+view|view|table)[ \t]+/, "", l)
        sub(/^if[ \t]+(not[ \t]+)?exists[ \t]+/, "", l)
        n = l
        sub(/[ \t(;].*$/, "", n)
        gsub(/[`"]/, "", n)
        sub(/^.*\./, "", n)          # unqualify: db.t and t are the same object here
        return n
    }
    NR == FNR {
        if (tolower($0) ~ /^[ \t]*create[ \t]/) { o = objname($0); if (o != "") created[o] = FNR }
        next
    }
    {
        if (tolower($0) ~ /^[ \t]*drop[ \t]+(external[ \t]+)?(temporary[ \t]+)?(materialized[ \t]+view|view|table)[ \t]/) {
            o = objname($0)
            # keep only if something creates this object AFTER this drop
            if (o == "" || !(o in created) || created[o] < FNR) next
        }
        print
    }' "$g.setup.sql" "$g.setup.sql" > "$RUN/setup.sql"
    timeout 300 $MYSQL "$db" -f < "$RUN/setup.sql" > /dev/null 2>"$RUN/setup.err"

    # Setup failures are not query errors and must not be counted as them. A table that fails to
    # create takes every query against it with it, and the round then looks like a round with errors
    # rather than a round that never ran. This went unnoticed for seven rounds: 171 of the corpus's
    # 2203 CREATE TABLEs omit replication_num, the cluster defaulted to 3, and it has one BE.
    nsetup=$(grep -c '^ERROR' "$RUN/setup.err" 2>/dev/null | head -1 | tr -dc '0-9' | sed 's/^$/0/')
    ntables=$(timeout 60 $MYSQL "$db" -N -e 'show tables' 2>/dev/null | grep -c . | head -1 | tr -dc '0-9')
    ntables=${ntables:-0}
    if [ "${nsetup:-0}" -gt 0 ]; then
        say "  setup: $nsetup statement(s) failed for $gname, $ntables table(s) exist"
        # Distinct setup failures are worth seeing once each; they are harness or environment
        # problems far more often than engine defects, so they go to their own file, not findings.
        grep '^ERROR' "$RUN/setup.err" 2>/dev/null \
            | sed -E "s/[0-9]+/N/g; s/'[^']*'/'S'/g" | cut -c1-160 | sort -u \
            | while IFS= read -r ss; do
                  if claim_signature "$SETUPSIG" 1 "$ss" "$(printf '%s\t%s\t%s' "$ss" "$round" "$gname")"; then
                      say "  NEW SETUP FAILURE :: $(cut -c1-120 <<< "$ss")"
                  fi
              done
    fi

    # Fill with boundary-biased rows rather than doubling what the corpus loaded. Duplicating rows
    # changes only the row count: same distinct values, same null density, same string lengths. That
    # leaves the low-cardinality/global-dictionary, spill and skew paths untouched -- and the
    # AGG_STATE dict-encoding crash needed precisely a real low-cardinality column whose global
    # dictionary got collected. The generator draws NULLs, type minima and maxima, empty and
    # oversized strings, on a fixed seed so a round stays reproducible.
    # Count rows before and after, because "the generator exited 0" is not the same as "rows were
    # loaded". A benchmark group already holds real data and is not amplified, so it is skipped.
    if [ -n "$benchdb" ]; then
        genrows=-1
    elif [ -f "$GEN_DATA" ]; then
        rows_before=$(db_row_total "$db")
        if timeout 600 python3 "$GEN_DATA" "$GEN_ROWS" >/dev/null 2>&1; then
            genrows=$(( $(db_row_total "$db") - rows_before ))
            # Exit code 0 with nothing loaded is the failure mode that hid for hundreds of rounds.
            if [ "$genrows" -le 0 ] && [ "${ntables:-0}" -gt 0 ]; then
                say "  HARNESS DEFECT: data generator exited 0 but loaded no rows into $gname"
            fi
        else
            genrows=0
            say "  data generation failed or timed out for $gname"
        fi
    else
        genrows=0
        say "  HARNESS DEFECT: $GEN_DATA missing; $gname runs against whatever the corpus loaded"
    fi

    # Differential check first, while the data is still what setup produced. Once the writers start
    # the same query legitimately returns different rows between two runs, and every comparison
    # would be a false mismatch.
    # Called directly, not through a command substitution: the counters come back in globals so that
    # a say() from inside cannot be captured as one. See differential_phase.
    differential_phase "$g" "$gname" "$db" "$round"
    ndiff=$DIFF_CHECKED; nmiss=$DIFF_BAD; ndempty=$DIFF_EMPTY; ndvoid=$DIFF_VOID; ndskip=$DIFF_SKIPPED

    # Same window, and for the same reason: the partition law holds over a table that is not being
    # written to. Once the writers start, the three branches and the baseline see different data and
    # every comparison is a false mismatch.
    tlp_phase "$gname" "$db" "$round"
    ntlp=$TLP_CHECKED; ntlpbad=$TLP_BAD; ntlpskip=$TLP_SKIPPED

    # One error file per worker. Several processes appending to one file interleave mid-line and
    # manufacture signatures like "ERROR N (N)ERROR at line N" that match no real error.
    rm -f "$RUN"/w.*.err
    nq=0
    pids=()
    for r in $(seq 1 $READERS); do
        (
            end=$(( $(date +%s) + PHASE_SECONDS ))
            n=0
            fails=0
            while [ "$(date +%s)" -lt "$end" ]; do
                started_at=$(date +%s)
                timeout "$QUERY_TIMEOUT" $MYSQL "$db" -f < "$g.query.sql" >/dev/null 2>>"$RUN/w.r$r.err"
                rc=$?
                [ $rc -eq 124 ] && echo "ERROR TIMEOUT after ${QUERY_TIMEOUT}s in $gname" >> "$RUN/w.r$r.err"
                n=$((n + 1))
                # A run that fails instantly means the server is not there. Without this the loop
                # reconnects ~450 times a second, which is a denial of service against our own FE and
                # is the most likely reason it died: 20259 attempts in a 45-second phase.
                if [ $rc -ne 0 ] && [ $(( $(date +%s) - started_at )) -lt 2 ]; then
                    fails=$((fails + 1))
                    [ $fails -ge 3 ] && sleep 5
                    [ $fails -ge 10 ] && break
                else
                    fails=0
                fi
            done
            echo "$n" > "$RUN/w.r$r.count"
        ) & pids+=($!)
    done
    for w in $(seq 1 $WRITERS); do
        (
            end=$(( $(date +%s) + PHASE_SECONDS ))
            while [ "$(date +%s)" -lt "$end" ]; do
                # `show tables` lists views too, and INSERT INTO a view is not supported -- 154 of
                # this round's 238 client-visible planner errors were the harness doing that to itself.
                t=$(timeout 30 $MYSQL "$db" -N -e "select TABLE_NAME from information_schema.tables
                        where TABLE_SCHEMA='$db' and TABLE_TYPE='BASE TABLE'" 2>/dev/null | shuf -n1)
                [ -z "$t" ] && continue
                # A throwaway database is dropped at the end of the round, so its tables can grow
                # freely. A benchmark database is shared across every round that uses it, and doubling
                # a 6M-row table on each of 1642 rounds would fill the disk. Cap it: writing is still
                # what produces multiple rowsets and the read/write concurrency the storage layer needs
                # to be exercised under, but it must not run away.
                if [ -n "$benchdb" ]; then
                    rows=$(timeout 30 $MYSQL "$db" -N -e "select count(*) from \`$t\`" 2>/dev/null | tr -dc '0-9')
                    [ "${rows:-0}" -gt "$BENCH_ROW_CAP" ] && continue
                fi
                timeout 120 $MYSQL "$db" \
                    -e "insert into \`$t\` select * from \`$t\` limit 20000" >/dev/null 2>>"$RUN/w.w$w.err"
            done
        ) & pids+=($!)
    done
    wait "${pids[@]}" 2>/dev/null
    errfile=$RUN/round.err
    cat "$RUN"/w.*.err > "$errfile" 2>/dev/null || : > "$errfile"
    # awk, not bc: bc is not installed in this image and its absence silently made every count zero.
    nq=$(cat "$RUN"/w.r*.count 2>/dev/null | awk '{s+=$1} END {print s+0}')
    rm -f "$RUN"/w.r*.count

    after=$(fatal_count)
    nerr=$(grep -c '^ERROR' "$errfile" 2>/dev/null | head -1 | tr -dc '0-9' | sed 's/^$/0/')

    # Whatever the FE logged this round. A shrinking file means it rotated, so take it from the top.
    fe_after=$(fe_log_size)
    [ "$fe_after" -lt "$fe_before" ] && fe_before=0
    tail -c +$((fe_before + 1)) "$FELOG" 2>/dev/null > "$RUN/round.felog"
    nfe=0
    while IFS= read -r sig; do
        [ -z "$sig" ] && continue
        benign_fe_signature "$sig" && continue
        # A round in which the BE died logs a great deal that is downstream of the death.
        [ "$after" -gt "$before" ] && continue
        if claim_signature "$FESIG" 1,2 "$sig" "$(printf '%s\t%s\t%s' "$sig" "$round" "$gname")"; then
            nfe=$((nfe + 1))
            {
                printf '\n## NEW FE-LOG SIGNATURE  round %s  group %s  %s\n\n' "$round" "$gname" "$(date '+%F %T')"
                printf '```\n%s\n```\n\nfirst logged instance:\n```\n%s\n```\n' "$sig" \
                    "$(grep -m1 -F "$(cut -f1 <<< "$sig")" "$RUN/round.felog" | cut -c1-500)"
            } >> "$FINDINGS"
            say "NEW FE-LOG round=$round group=$gname :: $(tr '\t' ' ' <<< "$sig" | cut -c1-110)"
            minimize_group "$g" "$gname" "$db" felog "$sig" "$before"
        fi
    done <<< "$(fe_log_signatures "$RUN/round.felog")"


    # Unconditionally, not only when the marker count rose: be.out is rotated and the BE is restarted
    # both by this harness and by a neighbour on this box, either of which resets the count and hides
    # a crash that did happen. Scanning for unseen signatures does not depend on the counter at all.
    record_new_crash_signatures "$round" "$gname"

    if [ "$after" -gt "$before" ] || ! be_alive; then
        restart_be && restarts=$((restarts + 1))
        # Bisect while the database still exists. A crash nobody reduced is a crash nobody can file,
        # and every one of these has cost hours of hand bisection so far.
        minimize_group "$g" "$gname" "$db" crash "" "$before"
    fi

    # Distinct error shapes, counted. Anything not obviously produced by the mixed load itself is
    # written to findings the first time it is seen.
    if [ "$nerr" -gt 0 ]; then
        grep '^ERROR' "$errfile" | sed -E "s/[0-9]+/N/g; s/'[^']*'/'S'/g" | cut -c1-160 | sort -u |
        while IFS= read -r sig; do
            if benign_error "$sig" || crash_fallout "$sig"; then continue; fi
            # Everything in this round is suspect once the BE went down in it.
            if [ "$after" -gt "$before" ]; then continue; fi
            if claim_signature "$ERRSIG" 1 "$sig" "$(printf '%s\t%s\t%s' "$sig" "$round" "$gname")"; then
                {
                    printf '\n## NEW ERROR  round %s  group %s  %s\n\n' "$round" "$gname" "$(date '+%F %T')"
                    printf '```\n%s\n```\n\nfirst raw instance:\n```\n%s\n```\n' "$sig" \
                        "$(grep -m1 '^ERROR' "$errfile" | cut -c1-500)"
                } >> "$FINDINGS"
                say "NEW ERROR round=$round group=$gname :: $(cut -c1-110 <<< "$sig")"
            fi
        done
    fi

    elapsed=$(( $(date +%s) - started ))
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$round" "$gname" "${ntables:-0}" "${nsetup:-0}" "${genrows:-0}" "$nq" "$nerr" "${ndiff:-0}" "${nmiss:-0}" \
        "${ndempty:-0}" "${ndvoid:-0}" "${ndskip:-0}" "${ntlp:-0}" "${ntlpbad:-0}" "${ntlpskip:-0}" \
        "$((after - before))" "$restarts" "${nfe:-0}" "$elapsed" >> "$STATE"
    say "round $round done in ${elapsed}s: group=$gname tables=${ntables:-0} setupfail=${nsetup:-0} queryruns=$nq errors=$nerr diff=${ndiff:-0}/${nmiss:-0} empty=${ndempty:-0} void=${ndvoid:-0} skip=${ndskip:-0} tlp=${ntlp:-0}/${ntlpbad:-0} tlpskip=${ntlpskip:-0} fatal_delta=$((after - before)) restarts=$restarts fe_sigs=${nfe:-0}"
    # A knob that produced nothing where the baseline had rows did not agree -- it did not run. One
    # or two is a timeout; a run of them is incident 8 happening again, so it gets said out loud
    # rather than left in a column nobody reads.
    if [ "${ndvoid:-0}" -gt "${ndiff:-0}" ] && [ "${ndvoid:-0}" -gt 4 ]; then
        say "  WARNING: $ndvoid knob runs returned nothing against $ndiff compared statements -- check the knob pool"
    fi
    printf 'round %s DONE at %s in %ss: group=%s errors=%s fatal_delta=%s\n  next round starting\n' \
        "$round" "$(date '+%F %T')" "$elapsed" "$gname" "$nerr" "$((after - before))" > "$STATUS"

    # Never drop a benchmark database: it is shared across rounds and took minutes to materialise.
    [ -z "$benchdb" ] && $MYSQL -e "drop database if exists $db" >/dev/null 2>&1
done
