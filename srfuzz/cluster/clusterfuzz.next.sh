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
        # -w, and carry on when it expires. An unbounded flock here turns any stuck holder into a
        # stuck campaign, and the worst case of proceeding without the lock is that one finding gets
        # written twice -- far better than losing it, and far better than the round loop stopping.
        flock -w 30 9 || say "claim_signature: lock on $(basename "$file") timed out; proceeding unlocked"
        if ! cut -f"$spec" "$file" 2>/dev/null | grep -Fxq "$key"; then
            printf '%s\n' "$line" >> "$file"
            rc=0
        fi
    } 9>>"$file.lock"
    return $rc
}
STATE_HEADER=$'round\tgroup\ttables\tsetup_fail\tgen_rows\tqueries\terrors\tdiff_checked\tdiff_bad\tdiff_empty\tdiff_void\tdiff_skipped\tdiff_unstable\ttlp_checked\ttlp_bad\ttlp_skipped\tfatal_delta\tbe_restarts\tnew_fe_sigs\tsecs\tqc_checked\tqc_bad\tqc_void\tqc_skipped\tqc_unstable'
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
        # -w rather than an unbounded wait. A normal restart under this lock takes minutes (stop 120s
        # + start + up to 150s waiting for the service), so 15 minutes only expires when the holder
        # is itself stuck -- and then blocking forever behind it is the worse failure: it is how one
        # wedged instance took its sibling down with it for 38 hours. On expiry, fall through and
        # re-check liveness; if the service came back anyway there is nothing to do.
        flock -w 900 9 || say "restart_fe: waited 900s for $(basename "$FELOCK"); the holder is stuck, proceeding"
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
    # Same as the backend stop below: close the lock fd for the child and bound the wait, or a stop
    # that never returns holds the restart lock for the life of the run.
    timeout 120 "$W/output/fe/bin/stop_fe.sh" >/dev/null 2>&1 9>&-
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
        # -w rather than an unbounded wait. A normal restart under this lock takes minutes (stop 120s
        # + start + up to 150s waiting for the service), so 15 minutes only expires when the holder
        # is itself stuck -- and then blocking forever behind it is the worse failure: it is how one
        # wedged instance took its sibling down with it for 38 hours. On expiry, fall through and
        # re-check liveness; if the service came back anyway there is nothing to do.
        flock -w 900 9 || say "restart_be: waited 900s for $(basename "$BELOCK"); the holder is stuck, proceeding"
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
    # 9>&- and a timeout for the STOP too, not just the start below. stop_be.sh inherits the fd the
    # lock lives on, so for as long as it runs it holds the restart lock -- and it runs forever when
    # the backend it is waiting for will not die. The cluster then looks alive from outside (the FE
    # answers, the instances are up) while every later restart blocks in flock and the BE never comes
    # back. Seen on two boxes at once, both stuck on a stop_be.sh holding fd 9.
    timeout 120 "$W/output/be/bin/stop_be.sh" >/dev/null 2>&1 9>&-
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
    # A crashed BE leaves a zombie -- this container's PID 1 is `sleep infinity`, which never reaps --
    # and a pid file still naming it. start_be.sh sees a pid that "exists" and refuses, so the BE never
    # comes back and the harness spins against a dead backend. That cost 5.5 hours of empty rounds the
    # first time it happened, while the watchdog correctly logged "BE is not registered Alive" and
    # nothing acted on it. Drop the pid file when it names nothing runnable.
    _bepid=$(cat "$W/output/be/bin/be.pid" 2>/dev/null)
    if [ -n "$_bepid" ]; then
        case "$(ps -o stat= -p "$_bepid" 2>/dev/null | tr -d " ")" in
            ""|Z*) rm -f "$W/output/be/bin/be.pid" ;;
        esac
    fi
    setsid nohup "$W/output/be/bin/start_be.sh" --daemon >/dev/null 2>&1 9>&-
    # be_alive, not a bare client. Every other probe in this file is wrapped in `timeout` and this
    # one was not, which is how one box lost 38 hours: the frontend's acceptor thread had been killed
    # by a heap OOM, so the JVM stayed up, kept its BDBJE leadership and kept writing fe.log while
    # accepting no connection ever again. The client here inherited that -- connected, never
    # answered, never returned -- and since this loop runs while holding the BE restart lock on fd 9,
    # the sibling instance blocked in flock behind it. Neither process died, so both looked healthy;
    # rounds.tsv simply stopped growing. Bound every wait against the cluster.
    for _ in $(seq 1 30); do
        sleep 5
        if be_alive; then
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
#
# The last seven are a different KIND of knob, added 2026-09-21 after auditing five weeks of upstream
# [BugFix] commits for what this campaign could never have found. Every knob above changes the plan;
# none of them changes how the plan is EXECUTED -- the batch size, the parallelism, whether an
# operator spills. A defect that is wrong only at a chunk boundary, only at dop > 1, or only on the
# spill path produces identical rows under all fifty-odd of them, and was structurally invisible:
#   set chunk_size = 255        batch boundaries. #78722 (substr returning the neighbouring row's
#                               bytes), #79007 (dict page decoder not advanced when a filter rejects
#                               the whole page), #78685, and our own late-materialisation null-flag
#                               desync -- all of them are one chunk's state leaking into the next.
#   set pipeline_dop = 1        parallelism. EXCEPT/INTERSECT driver-partition misalignment, the
#                               skew-join hint replicating a preserved side, array_sort sharing one
#                               error Status across drivers: all wrong only when dop > 1, so the
#                               single-driver run is the reference the oracle never had.
#   enable_spill + spill_mode   the spill path is a second implementation of join/agg/sort that the
#                               campaign has never once entered. #79085 lives there.
#   tablet_internal_parallel    two ways to split one tablet's scan; force_split takes the other.
#   global_late_materialization on by default, so the campaign only ever ran the GLM plan.
#   pipeline_level_shuffle      on by default, likewise.
# These are SAFER than the plan knobs, not riskier: they do not rewrite the query at all, so any
# difference is by construction a defect. They are also SLOWER (chunk_size = 255 multiplies the
# per-chunk overhead), which is why diff_run_incomplete and the knob-side size cap matter more now
# than they did -- a knob that pushes a query past QUERY_TIMEOUT must be read as "did not finish",
# never as "returned fewer rows".
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
set enable_materialized_view_rewrite = false|\
set chunk_size = 255|\
set pipeline_dop = 1|\
set enable_spill = true; set spill_mode = 'force'|\
set enable_tablet_internal_parallel = false|\
set tablet_internal_parallel_mode = 'force_split'|\
set enable_global_late_materialization = false|\
set enable_pipeline_level_shuffle = false"}
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
# Session settings applied to the MAIN query path -- the readers -- rather than to the differential.
#
# Every oracle above compares two runs of the same statement. This one does not compare anything: it
# changes the plan the ordinary reader phase executes, so that shapes the corpus cannot express are
# still reached, and it is the ERROR and CRASH oracles that judge the result. That is the right home
# for a setting whose failure mode is "the query errors": the differential SKIPS those on both sides
# (an errored baseline is counted as diff_empty, an errored knob run as diff_void), so a plan that
# only ever fails is structurally invisible to it and visible here.
#
# The CTE reuse rate is the first entry, and it was chosen from a measured gap rather than a guess.
# Every CTE this corpus contains has exactly ONE consumer -- measured on emit/, 518 of 760 groups
# declare a WITH and not one of them references a CTE twice, because the mutator's CTE shape is
# `WITH c AS (...) SELECT * FROM c` by construction. PhysicalCTEProduce / PhysicalCTEConsume, the
# multicast sink and the dictification of consume columns were therefore unreachable from this
# campaign, and an FE defect that lives there (upstream #78006: a dict expr dropped from the fragment
# when a projection hides a column the predicate still uses, so the BE fails with "couldn't found
# dict cid") could not have been found here no matter how long it ran.
#
# Both halves of the repair are necessary and neither is sufficient -- measured on the plan, not
# assumed:
#   - a one-consumer CTE is force-inlined no matter what the rate says, so this pool alone changes
#     nothing about the existing corpus;
#   - a two-consumer CTE (the new M6 CTE_REUSE shape) is NOT materialised at the default rate of
#     1.15 either, so the shape alone changes nothing;
#   - shape + `cbo_cte_reuse_rate = 0` together produce the multicast plan, and on an FE without the
#     #78006 fix that plan is missing exactly the two dict exprs the fix adds.
# -1 is the opposite direction (force inline) for the same two-consumer shapes.
#
# cbo_cte_reuse is set alongside it because the rate is inert while reuse is off, and it is off in
# more places than one would expect. The visible alias `cbo_cte_reuse_rate` is used rather than
# cbo_cte_reuse_rate_v2, which is INVISIBLE.
#
# One value is drawn per ROUND, not per statement, and half the rounds run with none at all. Per
# round because a finding has to be reproducible: the value is printed with every error this round
# reports and prepended to the minimiser's replay, so the SQL in findings.md is the SQL that failed.
# Half with none because the default is not a value to be traded away -- it is what the campaign has
# always run, and a regression that only shows up without a var must still be reachable.
EXEC_VARS=${EXEC_VARS:-"\
set cbo_cte_reuse = true; set cbo_cte_reuse_rate = 0|\
set cbo_cte_reuse = true; set cbo_cte_reuse_rate = -1|\
"}
# The value chosen for the current round, or empty. Read by the reader phase, the minimiser and the
# error reporter, so it is a global rather than a parameter threaded through all three.
ROUND_EXEC_VAR=""

DIFF_KNOB_SAMPLE=${DIFF_KNOB_SAMPLE:-4}

# Which knobs a statement is checked against, when the statement itself says which ones matter.
#
# The sample above is uniform: 4 knobs drawn from 60 gives any one knob a 6.7% chance of being asked
# about any one statement. For a knob whose defect family only EXISTS in a statement that uses the
# feature -- push_down_heavy_exprs needs a heavy expression, enable_lambda_pushdown needs a lambda,
# cbo_prune_json_subfield needs JSON -- that 6.5% is spent mostly on statements where the knob
# cannot possibly matter, and the few statements where it can are the ones that get skipped.
# [[fuzzer-blind-to-data-layout]] measured the cost on a real defect: the knob that would have caught
# the heavy-expr crash was in the pool the whole time, at an 8% chance per statement.
#
# So: entries are <ERE>::<knob>, one per line, matched case-insensitively against the statement. Up
# to DIFF_KNOB_FORCED of the matching knobs are drawn first and the rest of the sample is filled
# uniformly, which keeps the per-statement cost exactly where it was. The cap is what stops a
# statement that matches five patterns from spending its whole budget on them and never testing
# anything else.
#
# A knob named here MUST also appear in DIFF_KNOBS -- validate_knobs enforces it, because a biased
# knob that never went through startup validation is incident 8 with extra steps.
DIFF_KNOB_BIAS=${DIFF_KNOB_BIAS:-"
(substr|substring|concat|lpad|rpad|repeat|reverse|instr|locate|split|regexp_extract)[[:space:]]*\(::set chunk_size = 255
regexp::set push_down_heavy_exprs = false
(array_map|array_filter|array_sort|array_sortby)[[:space:]]*\(::set enable_lambda_pushdown = false
(except|intersect)[[:space:]]::set pipeline_dop = 1
(get_json|json_query|json_exists|->)::set cbo_prune_json_subfield = false
count[[:space:]]*\([[:space:]]*distinct::set new_planner_agg_stage = 2
over[[:space:]]*\(::set chunk_size = 255
group[[:space:]]+by::set enable_spill = true; set spill_mode = 'force'
(left|right|full)[[:space:]]+(outer[[:space:]]+)?join::set pipeline_dop = 1
order[[:space:]]+by::set enable_global_late_materialization = false
"}
DIFF_KNOB_FORCED=${DIFF_KNOB_FORCED:-2}

# The knobs one statement is checked against: the biased ones it matched, then uniform fill.
select_knobs() {
    local stmt=$1 forced rest nforced nfill
    forced=$(while IFS= read -r entry; do
                 [ -z "$entry" ] && continue
                 case "$entry" in *::*) ;; *) continue ;; esac
                 if grep -qiE "${entry%%::*}" <<< "$stmt"; then printf '%s\n' "${entry#*::}"; fi
             done <<< "$DIFF_KNOB_BIAS" | sort -u \
             | grep -xF -f <(tr '|' '\n' <<< "$DIFF_KNOBS" | grep .) | shuf -n "$DIFF_KNOB_FORCED")
    nforced=$(grep -c . <<< "$forced")
    nfill=$((DIFF_KNOB_SAMPLE - nforced))
    [ "$nfill" -lt 0 ] && nfill=0
    rest=$(tr '|' '\n' <<< "$DIFF_KNOBS" | grep . | grep -vxF "$forced" | shuf -n "$nfill")
    printf '%s\n%s\n' "$forced" "$rest" | grep .
}

# Rows, normalised. A query without ORDER BY may return them in any order, and comparing raw output
# would report every such query as a mismatch.
# The client's exit status, for the caller. diff_run is used inside a command substitution, so it
# cannot set a variable the caller can see; a file is the only channel out of the subshell.
DIFF_RC=$RUN/diff.rc

diff_run() {
    local db=$1 setup=$2 sql=$3
    printf '%s;\n%s\n' "$setup" "$sql" \
        | { timeout "$QUERY_TIMEOUT" $MYSQL "$db" -N -B 2>/dev/null; printf '%s' "$?" > "$DIFF_RC"; } \
        | LC_ALL=C sort \
        | head -c "$DIFF_MAX_BYTES"
}

# A killed client leaves the rows it had already written, and those rows look exactly like a complete
# result -- non-empty, under the size cap, perfectly sortable. Two runs of the same slow query then
# stop at different points and the comparison reports a difference that belongs entirely to the
# clock. Both low-cardinality "row loss" findings of 2026-08-19 were this: one query delivers
# 3012861 rows in 65s (0 rows inside the 60s budget), the other 995802 rows in 29s, and what the
# oracle compared was 90972 against 113028, and 2503 against 57033. Neither had anything to do with
# the knob. The existing guards do not cover it: the result is not empty, and it is under
# DIFF_MAX_BYTES precisely because it was cut short.
diff_run_incomplete() {
    local rc; rc=$(cat "$DIFF_RC" 2>/dev/null)
    [ -n "$rc" ] && [ "$rc" != 0 ]
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

# True when some OVER() in the statement has no ORDER BY of its own, so which row the window
# function reads is undefined and two plans may legitimately read different ones.
window_unordered() {
    python3 - "$1" <<'PYEOF'
import re, sys
sql = re.sub(r"'(?:[^'\\]|\\.)*'", "''", sys.argv[1].lower())
for m in re.finditer(r"\bover\s*\(", sql):
    i, depth = m.end(), 1
    while i < len(sql) and depth:
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
        i += 1
    if not re.search(r"\border\s+by\b", sql[m.end():i]):
        sys.exit(0)
sys.exit(1)
PYEOF
}

diff_skippable() {
    if grep -qiE '\b(rand|random|now|current_timestamp|current_date|curdate|curtime|uuid|last_query_id|connection_id|current_user)\b' <<< "$1"; then
        return 0
    fi
    # Aggregates whose VALUE, not merely whose row order, is free to change with the plan. diff_run's
    # sort normalises the order of rows; it cannot normalise the order of elements inside one cell,
    # and it cannot make a tie-break repeatable. Nor does the re-run confirmation gate below help:
    # these are deterministic for a GIVEN plan and differ BETWEEN plans, which is precisely the shape
    # the oracle is built to report. Measured on ns0911: about half of 106 knob differences across
    # two instances were this, and noise at that rate is what makes a findings file unreadable.
    #   approx_top_k   approximate, ties at the tail come back in whatever order the sketch holds
    #   any_value      documented as "any"
    #   min_by/max_by  ties resolved by whichever row arrived first
    #   array_agg / group_concat   element order inside the cell follows the input order
    if grep -qiE '\b(approx_top_k|any_value|min_by|max_by|min_by_if|max_by_if|array_agg|array_agg_distinct|group_concat)[[:space:]]*\(' <<< "$1"; then
        return 0
    fi
    # A window with no ORDER BY inside its OVER(): same argument one level down.
    if window_unordered "$1"; then
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
#
# COUNTED, not read off information_schema.tables.table_rows. That column is a STATISTIC refreshed
# asynchronously by TabletStatMgr, and every round drops and recreates its database -- so its tables
# are always younger than the statistic, both the before-read and the after-read return 0, gen_rows
# is ALWAYS 0, and the harness cries "data generator exited 0 but loaded no rows" every round.
# Measured on the live campaign before this fix: 1081 of 1104 rounds on dev1 instance 0, ~900 alarms
# per instance -- while a count(*) on the round's own table at that moment returned 823200 rows. The
# data was fine; the column that exists to tell a broken round from a quiet one was the broken thing.
# That is incident 6 wearing the opposite mask: there the generator failed silently, here it works
# and the measurement claims it failed.
#
# Costs two count(*) per table per round, which is what a number that means what it says is worth.
db_row_total() {
    local db=$1 t c total=0
    while IFS= read -r t; do
        [ -n "$t" ] || continue
        c=$(timeout 30 $MYSQL "$db" -N -B -e "select count(*) from \`$t\`" 2>/dev/null | tr -dc '0-9')
        total=$((total + ${c:-0}))
    done < <(timeout 30 $MYSQL -N -B -e "select TABLE_NAME from information_schema.tables
        where table_schema='$db' and table_type='BASE TABLE'" 2>/dev/null)
    printf '%s' "$total"
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
        # Name the probe database. Without argv[2] the generator walks every database this instance
        # owns, so "can the generator load a row" turns into a full pass over the shard: minutes once
        # the corpus is real, and the 120s budget below expires before it reaches the probe. Every
        # start then fails preflight for a generator that works. It also stopped each start from
        # re-loading databases another instance was mid-differential on.
        if ! timeout 120 python3 "$GEN_DATA" 50 "$probe" >/dev/null 2>&1; then
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
    # Every biased knob has to be one of the knobs just validated. A name that is only in the bias
    # map never went through the loop above, so the server's opinion of it is unknown -- and a knob
    # the server rejects returns nothing, which this oracle reads as agreement. That is incident 8.
    local orphans=0
    while IFS= read -r entry; do
        [ -z "$entry" ] && continue
        case "$entry" in *::*) ;; *) continue ;; esac
        grep -qxF "${entry#*::}" <<< "$(tr '|' '\n' <<< "$DIFF_KNOBS")" || {
            orphans=$((orphans + 1))
            say "NOTE: DIFF_KNOB_BIAS names a knob this instance does not hold: ${entry#*::}"
        }
    done <<< "$DIFF_KNOB_BIAS"
    # Said, not fatal, and not silent either. srfuzz-launch.sh splits the pool across instances, so
    # an instance legitimately holds only part of it and select_knobs drops the rest -- but a bias
    # map where EVERY entry is absent is a typo'd knob name, not a split, and that is worth stopping
    # for: the biased knobs would silently never be drawn and the sampling would be uniform again.
    if [ "$orphans" -gt 0 ] && [ "$orphans" -eq "$(grep -c '::' <<< "$DIFF_KNOB_BIAS")" ]; then
        say "FATAL: not one knob named in DIFF_KNOB_BIAS is in this instance's DIFF_KNOBS"
        bad=1
    fi
    # Same check for the exec-var pool, and for a sharper reason: an unknown DIFF knob returns an
    # empty result that the oracle reads as agreement, but an unknown EXEC var is a `set` statement
    # prepended to every reader iteration, so it would manufacture one ERROR per run -- a signature
    # of our own making, reported as a finding on the first round.
    while IFS= read -r knob; do
        [ -z "$knob" ] && continue
        if ! timeout 20 $MYSQL -N -e "$knob" >/dev/null 2>&1; then
            say "FATAL: exec var rejected by the server: $knob"
            bad=1
        fi
    done <<< "$(tr '|' '\n' <<< "$EXEC_VARS")"
    [ "$bad" -eq 0 ] || { say "Fix DIFF_KNOBS/EXEC_VARS before running: a setting that errors is worse than none."; exit 1; }
    say "differential knobs validated: $(tr '|' '\n' <<< "$DIFF_KNOBS" | grep -c .), exec vars: $(tr '|' '\n' <<< "$EXEC_VARS" | grep -c .)"
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
        # A timed-out baseline is a partial result, not a baseline. Skip before anything is compared.
        if diff_run_incomplete; then
            skipped=$((skipped + 1))
            continue
        fi
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

        # A sample of the pool rather than all of it, biased toward the knobs this statement's own
        # text says are relevant -- see DIFF_KNOB_SAMPLE and DIFF_KNOB_BIAS.
        knobs=$(select_knobs "$stmt")
        while IFS= read -r knob; do
            [ -z "$knob" ] && continue
            var=$(diff_run "$db" "$knob" "$stmt")
            # Same for the knob run: a partial result differs from a complete baseline for reasons
            # the knob had no part in.
            if diff_run_incomplete; then
                voidknob=$((voidknob + 1))
                continue
            fi
            # The baseline had rows and this run did not. That is not "no difference": it is the
            # statement erroring, timing out, or the knob being rejected -- exactly the shape of
            # incident 8, where a knob the server did not know returned nothing and the emptiness was
            # read as agreement. validate_knobs catches an unknown name at startup; this catches the
            # same silence arising at run time, when only a counter can show it.
            if [ -z "$var" ]; then
                voidknob=$((voidknob + 1))
                continue
            fi
            # The same size cap the baseline gets. It was on one side only until 2026-09-21, and the
            # asymmetry has its own false finding: base < DIFF_MAX_BYTES <= var (a concurrent writer
            # grew the table between the two runs) passes every other guard and reports as "the knob
            # returned FEWER rows" -- ch_mut_50948, disproved after the fact by showing the knob did
            # not even change the plan. A truncated comparison proves nothing in either direction.
            if [ "${#var}" -ge "$DIFF_MAX_BYTES" ]; then
                skipped=$((skipped + 1))
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

# ---------------------------------------------------------------------------------------------
# Query cache oracle: a statement's answer must not depend on what ran before it.
#
# The differential above compares ONE statement under two session settings. That cannot see the
# query cache's characteristic failure, because a single statement with the cache on is consistent
# with itself -- the damage is cross-statement. The cache stores a per-tablet partial aggregate
# under a digest of the plan, and when the digest omits something the plan depends on, two DIFFERENT
# statements share an entry and whichever runs first decides the other's answer.
#
# Six such omissions were found by hand before this oracle existed (join predicate CSEs, the ASOF
# temporal condition, the ANN search spec, the columns a late-materialization FetchNode defers,
# AggregationNode.localLimit, and the scan's schema after a fast schema evolution). Each needed a
# specific plan shape, and each was found by guessing that shape. A mutation fuzzer already produces
# the pairs this oracle needs for free: a seed and its mutant are two statements that are almost the
# same plan, which is exactly the population where digests collide.
#
# Oracle, per adjacent pair (A, B) drawn from the corpus:
#     truth_A = A with the cache off,  truth_B = B with the cache off
#     both must repeat -- an unstable statement has no truth to compare against
#     if truth_A == truth_B: skip, the pair proves nothing
#     run A with the cache on, then B with the cache on
#     B != truth_B  =>  B inherited A's entries
#
# No cold-cache reset between pairs. Entries surviving from earlier pairs can only make B wrong,
# and a B that is wrong for that reason is the same defect. Skipping the reset also keeps the phase
# affordable: invalidating the cache means bumping every partition version, which costs more than
# the whole phase.
#
# A TIMEOUT is reported rather than skipped. The first cache defect found in this area was a hang,
# not a wrong answer: MultilaneOperator deadlocked when a lane's join had an empty build side, and
# the query ran to its timeout with every operator idle. Treating a timeout as "nothing to compare"
# would have hidden it.
QC_MAX_PAIRS=${QC_MAX_PAIRS:-12}
QC_SESSION=${QC_SESSION:-'set enable_query_cache = true, query_cache_hot_partition_num = 100'}

# Returns the sorted rows, the empty string for a query that legitimately matched nothing, or
# QC_ERR for an error or a timeout. The differential above collapses the last two into "" and skips
# both, which is right for it -- an error leaves nothing to compare. Here it would throw away the
# most discriminating pairs there are: a statement whose correct answer is no rows is exactly the
# one that exposes the cache handing it somebody else's.
qc_run() {
    local db=$1 cache=$2 sql=$3 out rc
    local setup="set enable_profile = false"
    [ "$cache" = on ] && setup="$setup; $QC_SESSION"
    out=$(printf '%s;\n%s\n' "$setup" "$sql" | timeout 60 $MYSQL "$db" -N -B 2>&1)
    rc=$?
    if [ "$rc" -ne 0 ] || grep -qE '^ERROR [0-9]+' <<< "$out"; then
        printf 'QC_ERR'
        return
    fi
    [ -z "$out" ] && return
    LC_ALL=C sort <<< "$out" | head -c "$DIFF_MAX_BYTES"
}

query_cache_phase() {
    local g=$1 gname=$2 db=$3 round=$4
    local checked=0 bad=0 skipped=0 unstable=0 hangs=0
    local prev="" prev_truth="" stmt truth truth2 got sig
    QC_CHECKED=0; QC_BAD=0; QC_SKIPPED=0; QC_UNSTABLE=0; QC_HANGS=0

    while IFS= read -r -d ';' stmt; do
        [ "$checked" -ge "$QC_MAX_PAIRS" ] && break
        stmt=${stmt%;}
        [ -z "$stmt" ] && continue
        grep -qiE '^[[:space:]]*(select|with)\b' <<< "$stmt" || continue
        # Same exclusions as the differential: a statement whose answer is allowed to move cannot
        # accuse the cache of moving it.
        if diff_skippable "$stmt"; then
            skipped=$((skipped + 1))
            continue
        fi

        truth=$(qc_run "$db" off "$stmt")
        if [ "$truth" = QC_ERR ]; then
            skipped=$((skipped + 1))
            continue
        fi
        truth2=$(qc_run "$db" off "$stmt")
        if [ "$truth2" != "$truth" ]; then
            unstable=$((unstable + 1))
            prev=""; prev_truth=""
            continue
        fi

        if [ -n "$prev" ] && [ "$prev_truth" != "$truth" ]; then
            checked=$((checked + 1))
            qc_run "$db" on "$prev" > /dev/null
            got=$(qc_run "$db" on "$stmt")
            if [ "$got" = QC_ERR ]; then
                # The statement answered cleanly with the cache off and errored or timed out with it
                # on. A hang lands here, which is why it is a finding and not a skip.
                hangs=$((hangs + 1))
                sig="qc-void"
                if claim_signature "$DIFFSIG" 1 "$sig" "$sig"; then
                    {
                        printf '\n## QUERY CACHE: NO ROWS WITH THE CACHE ON  round %s  group %s  %s\n\n' \
                            "$round" "$gname" "$(date '+%F %T')"
                        printf 'ran after:\n```sql\n%s\n```\n\n' "$(cut -c1-400 <<< "$prev")"
                        printf 'this errored or timed out (cache off gives %s rows):\n```sql\n%s\n```\n' \
                            "$(printf '%s' "$truth" | grep -c .)" "$(cut -c1-400 <<< "$stmt")"
                    } >> "$FINDINGS"
                fi
            elif [ "$got" != "$truth" ]; then
                bad=$((bad + 1))
                sig="qc-poison"
                if ! claim_signature "$DIFFSIG" 1 "$sig" "$sig"; then
                    printf -- '- repeat %s  round %s  group %s  `%s`\n' \
                        "$sig" "$round" "$gname" "$(cut -c1-160 <<< "$stmt")" >> "$FINDINGS"
                else
                    {
                        printf '\n## QUERY CACHE SERVES ANOTHER STATEMENT ANSWER  round %s  group %s  %s\n\n' \
                            "$round" "$gname" "$(date '+%F %T')"
                        printf 'populated by:\n```sql\n%s\n```\n\n' "$prev"
                        printf 'then this returned the wrong rows:\n```sql\n%s\n```\n\n' "$stmt"
                        printf 'cache off: %s rows, cache on after the above: %s rows\n\n' \
                            "$(printf '%s' "$truth" | grep -c .)" "$(printf '%s' "$got" | grep -c .)"
                        printf 'first differing lines:\n```\n%s\n```\n' \
                            "$(diff <(printf '%s\n' "$truth") <(printf '%s\n' "$got") | head -6)"
                    } >> "$FINDINGS"
                fi
            fi
        fi
        prev=$stmt; prev_truth=$truth
    done < <(cat "$g.query.sql"; printf ';')

    QC_CHECKED=$checked; QC_BAD=$bad; QC_SKIPPED=$skipped
    QC_UNSTABLE=$unstable; QC_HANGS=$hangs
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
        if [ -z "$lhs" ] || diff_run_incomplete; then skipped=$((skipped + 1)); continue; fi
        rhs=$(diff_run "$db" "set enable_profile = false" "$part")
        # The partitioned form runs three scans where the baseline runs one, so it is the side that
        # times out first -- and a partial rhs against a complete lhs is a manufactured row loss,
        # which is the single most expensive kind of false finding this oracle can produce.
        if diff_run_incomplete; then
            skipped=$((skipped + 1))
            say "  TLP skipped on $t.$col ($ctype $op): partitioned form did not finish in ${QUERY_TIMEOUT}s"
            continue
        fi
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
        # Same row count is a different finding from a row loss, and conflating them cost three hours
        # of triage on one report. The law this oracle checks is about the MULTISET: rows appearing or
        # disappearing is the violation. When the counts agree and only the content differs, the usual
        # cause is a column whose value has no stable order -- array_agg_distinct and the agg_state
        # columns finalize through a hash set, so two plans can read the same stored row and emit its
        # elements in a different order while holding exactly the same elements. Report both, but
        # never under the same heading, and dedup them separately so one does not bury the other.
        local nlhs nrhs kind
        nlhs=$(printf '%s' "$lhs" | grep -c .)
        nrhs=$(printf '%s' "$rhs" | grep -c .)
        if [ "$nlhs" -eq "$nrhs" ]; then kind=content; else kind=rows; fi
        # Dedup on the shape, not the table: a defect in DATE range extraction and one in VARCHAR
        # comparison are different bugs, while the same defect seen on forty tables is one.
        sig="tlp:${kind}:${ctype}:${op}"
        if ! claim_signature "$DIFFSIG" 1 "$sig" "$sig"; then
            printf -- '- repeat %s  round %s  group %s  table %s\n' "$sig" "$round" "$gname" "$t" >> "$FINDINGS"
        else
            {
                if [ "$kind" = rows ]; then
                    printf '\n## TLP PARTITION MISMATCH  round %s  group %s  %s\n\n' \
                        "$round" "$gname" "$(date '+%F %T')"
                    printf 'A row of `%s` satisfies exactly one of `p`, `NOT p`, `p IS NULL`, so these must\n' "$t"
                    printf 'return the same multiset. They do not: the two sides return a different NUMBER\n'
                    printf 'of rows, so rows were lost or duplicated by the partitioning.\n\n'
                else
                    printf '\n## TLP CONTENT DIFFERS  round %s  group %s  %s\n\n' \
                        "$round" "$gname" "$(date '+%F %T')"
                    printf 'Both sides of the `p` / `NOT p` / `p IS NULL` split return the SAME number of rows\n'
                    printf 'from `%s`, and some row differs in content. This is not a row loss.\n\n' "$t"
                    printf 'Check the unordered-container explanation before treating it as a defect: a column\n'
                    printf 'built by an unordered aggregate (array_agg_distinct, the agg_state columns) finalizes\n'
                    printf 'through a hash set, so two plans can read the same stored row and emit its elements in\n'
                    printf 'a different order while holding exactly the same elements. Compare element MULTISETS,\n'
                    printf 'not positions, and run `show create table` -- the aggregate shows up there and not in\n'
                    printf '`desc`. Only if the multisets differ is this a defect.\n\n'
                fi
                printf 'predicate: `%s`   column type: `%s`   table rows: %s\n\n' "$p" "$ctype" "$nrows"
                printf '```sql\n-- baseline\n%s;\n\n-- partitioned\n%s;\n```\n\n' "$base" "$part"
                printf 'baseline rows: %s, partitioned rows: %s\n\n' \
                    "$(printf '%s' "$lhs" | grep -c .)" "$(printf '%s' "$rhs" | grep -c .)"
                printf 'first differing lines:\n```\n%s\n```\n' \
                    "$(diff <(printf '%s\n' "$lhs") <(printf '%s\n' "$rhs") | head -8)"
            } >> "$FINDINGS"
            if [ "$kind" = rows ]; then
                say "TLP MISMATCH round=$round group=$gname table=$t predicate=$p (rows $nlhs vs $nrhs)"
            else
                say "TLP content differs (same row count) round=$round group=$gname table=$t predicate=$p"
            fi
        fi
    done <<< "$(timeout 30 $MYSQL -N -B -e "select TABLE_NAME from information_schema.tables
                    where TABLE_SCHEMA='$db' and TABLE_TYPE='BASE TABLE'" 2>/dev/null | shuf -n "$TLP_MAX_TABLES")"

    TLP_CHECKED=$checked
    TLP_BAD=$bad
    TLP_SKIPPED=$skipped
}

# Where this run started reading be.out. The log is append-only and outlives the campaign that
# wrote it: restarting on a new baseline re-reads every crash the previous months produced and files
# all of them as findings of the first round. Measured on the 2026-08-19 restart: 7 signatures on
# dev1 and 11 on dev2 within twenty minutes, every one of them a report from a PID that had not
# existed for days -- including the EXCEPT/INTERSECT overallocation that the new baseline fixes.
# A campaign whose first act is to re-file its own history cannot be read at all.
BELOG_BASE=0
belog_base_init() {
    BELOG_BASE=$(wc -c < "$BELOG" 2>/dev/null | tr -dc '0-9')
    BELOG_BASE=${BELOG_BASE:-0}
    say "crash oracle reads be.out from byte $BELOG_BASE (everything before it belongs to an earlier run)"
}

crash_signatures() {
    local size
    size=$(wc -c < "$BELOG" 2>/dev/null | tr -dc '0-9')
    size=${size:-0}
    # Shrunk means rotated or truncated, and then the old offset would start mid-record; the only
    # safe reading of a log that went backwards is to read all of what is there now.
    [ "$size" -lt "$BELOG_BASE" ] && BELOG_BASE=0
    tail -c "+$((BELOG_BASE + 1))" "$BELOG" 2>/dev/null | awk '
      # A deliberate stop is not a crash: SIGTERM prints the same banner and would
      # otherwise be filed as starrocks::sigterm_handler.
      /stack trace:/ { if ($0 ~ /SIGTERM/) { inc=0; next } inc=1; n=0; next }
      # A sanitizer report is a crash and has to be recorded like one. ASan writes its own banner and
      # its own frame format ("#7 0x... in starrocks::X") and never the glog "stack trace:" line, so
      # on an ASAN build -- which is what this campaign runs -- every one of them was invisible to
      # this function. Eight triage passes in a row had to find them by hand with
      # `grep -a "ERROR: AddressSanitizer" be.out`, and the EXCEPT/INTERSECT 512GB overallocation was
      # only ever seen that way.
      /ERROR: AddressSanitizer/ { inc=1; n=0; next }
      inc {
        # The allocator frames are not the identity of the bug. An OOM report starts inside
        # SystemAllocator/MemPool no matter what asked for the memory, so keying on the first
        # starrocks frame would file every distinct overallocation under one signature -- the same
        # "one signature swallows many bugs" failure this function was written to avoid. Skip down to
        # the first frame that belongs to the query.
        if ($0 ~ /starrocks::/ && $0 !~ /FailureSignalHandler|failure_function|ThreadPool::dispatch|Thread::supervise|SystemAllocator|MemChunkAllocator|MemPool::|MemTracker|allocate_via_malloc/) {
          s=$0
          match(s, /starrocks::[A-Za-z_:~<>]+/)
          if (RSTART>0) { print substr(s, RSTART, RLENGTH); inc=0 }
        }
        if (++n > 24) inc=0
      }
    ' 2>/dev/null | sort -u
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
        # Under the same setting the round ran, or the minimiser is replaying a different query
        # than the one that failed and will report "not reducible to one statement" for every
        # finding a session setting produced.
        { [ -n "$ROUND_EXEC_VAR" ] && printf '%s;\n' "$ROUND_EXEC_VAR"; printf '%s\n' "$line"; } > "$RUN/min.sql"
        local fb; fb=$(fatal_count)
        local fe_b; fe_b=$(fe_log_size)
        timeout "$QUERY_TIMEOUT" $MYSQL "$db" -f < "$RUN/min.sql" >/dev/null 2>&1
        if [ "$mode" = crash ]; then
            local fa; fa=$(fatal_count)
            if [ "$fa" -gt "$fb" ] || ! be_alive; then
                say "  MINIMIZED $gname to statement $i/$total"
                { printf '\n### minimized to one statement (line %s of %s)%s\n\n```sql\n%s\n```\n' \
                    "$i" "$total" "${ROUND_EXEC_VAR:+, under \`$ROUND_EXEC_VAR\`}" \
                    "$(cut -c1-1200 <<< "$line")"; } >> "$FINDINGS"
                restart_be
                return 0
            fi
        else
            local fe_a; fe_a=$(fe_log_size)
            [ "$fe_a" -lt "$fe_b" ] && fe_b=0
            tail -c +$((fe_b + 1)) "$FELOG" 2>/dev/null > "$RUN/min.felog"
            if fe_log_signatures "$RUN/min.felog" | grep -Fq "$want"; then
                say "  MINIMIZED $gname to statement $i/$total for: $want"
                { printf '\n### minimized to one statement (line %s of %s)%s\n\n```sql\n%s\n```\n' \
                    "$i" "$total" "${ROUND_EXEC_VAR:+, under \`$ROUND_EXEC_VAR\`}" \
                    "$(cut -c1-1200 <<< "$line")"; } >> "$FINDINGS"
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
belog_base_init
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
    # Half the rounds run with the server defaults, half with one setting from EXEC_VARS. Drawn here,
    # once, so every error this round reports can name the setting it ran under.
    if [ $((RANDOM % 2)) -eq 0 ]; then
        ROUND_EXEC_VAR=$(tr '|' '\n' <<< "$EXEC_VARS" | grep . | shuf -n 1)
    else
        ROUND_EXEC_VAR=""
    fi

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
        timeout 120 $MYSQL -e "drop database if exists $db; create database $db" >/dev/null 2>&1
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
        # Name the database, exactly as preflight has to. Without argv[2] the generator walks every
        # database this instance owns and fills all of them -- 2782 databases at the current corpus
        # size -- while the round only ever measures, and only ever queries, this one. The round then
        # pays for the whole shard every time, exceeds the 600s budget, and reports the amplification
        # it did not get to do as a failure. It also re-loads databases a sibling instance is
        # mid-differential on, which is a row-count change underneath a comparison that assumes none.
        if timeout 600 python3 "$GEN_DATA" "$GEN_ROWS" "$db" >/dev/null 2>&1; then
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

    # Same quiet window as the two above, and for the same reason: once the writers start, a
    # statement legitimately answers differently between the cache-off truth and the cache-on run,
    # and every pair would be a false accusation.
    query_cache_phase "$g" "$gname" "$db" "$round"
    nqc=$QC_CHECKED; nqcbad=$QC_BAD; nqcskip=$QC_SKIPPED; nqcunst=$QC_UNSTABLE; nqcvoid=$QC_HANGS

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
                # Piped rather than redirected so the round's session setting can be prepended.
                # It is a `set` on the same connection as the statements, which is the only way to
                # reach a plan the corpus cannot express -- there is no per-statement hint to inject
                # into a corpus file that was deparsed from someone else's SQL.
                { [ -n "$ROUND_EXEC_VAR" ] && printf '%s;\n' "$ROUND_EXEC_VAR"; cat "$g.query.sql"; } |
                    timeout "$QUERY_TIMEOUT" $MYSQL "$db" -f >/dev/null 2>>"$RUN/w.r$r.err"
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
                    # The session setting the readers ran under, when there was one. Without it a
                    # finding produced by a non-default plan cannot be reproduced from findings.md,
                    # and the first person to try would conclude the harness invented it.
                    [ -n "$ROUND_EXEC_VAR" ] && printf 'session: `%s`\n\n' "$ROUND_EXEC_VAR"
                    printf '```\n%s\n```\n\nfirst raw instance:\n```\n%s\n```\n' "$sig" \
                        "$(grep -m1 '^ERROR' "$errfile" | cut -c1-500)"
                } >> "$FINDINGS"
                say "NEW ERROR round=$round group=$gname :: $(cut -c1-110 <<< "$sig")"
            fi
        done
    fi

    elapsed=$(( $(date +%s) - started ))
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$round" "$gname" "${ntables:-0}" "${nsetup:-0}" "${genrows:-0}" "$nq" "$nerr" "${ndiff:-0}" "${nmiss:-0}" \
        "${ndempty:-0}" "${ndvoid:-0}" "${ndskip:-0}" "${ndunst:-0}" "${ntlp:-0}" "${ntlpbad:-0}" "${ntlpskip:-0}" \
        "$((after - before))" "$restarts" "${nfe:-0}" "$elapsed" \
        "${nqc:-0}" "${nqcbad:-0}" "${nqcvoid:-0}" "${nqcskip:-0}" "${nqcunst:-0}" >> "$STATE"
    say "round $round done in ${elapsed}s: group=$gname${ROUND_EXEC_VAR:+ [$ROUND_EXEC_VAR]} tables=${ntables:-0} setupfail=${nsetup:-0} queryruns=$nq errors=$nerr diff=${ndiff:-0}/${nmiss:-0} empty=${ndempty:-0} void=${ndvoid:-0} skip=${ndskip:-0} unstable=${ndunst:-0} tlp=${ntlp:-0}/${ntlpbad:-0} tlpskip=${ntlpskip:-0} qc=${nqc:-0}/${nqcbad:-0} qcvoid=${nqcvoid:-0} qcskip=${nqcskip:-0} qcunstable=${nqcunst:-0} fatal_delta=$((after - before)) restarts=$restarts fe_sigs=${nfe:-0}"
    # A knob that produced nothing where the baseline had rows did not agree -- it did not run. One
    # or two is a timeout; a run of them is incident 8 happening again, so it gets said out loud
    # rather than left in a column nobody reads.
    if [ "${ndvoid:-0}" -gt "${ndiff:-0}" ] && [ "${ndvoid:-0}" -gt 4 ]; then
        say "  WARNING: $ndvoid knob runs returned nothing against $ndiff compared statements -- check the knob pool"
    fi
    printf 'round %s DONE at %s in %ss: group=%s errors=%s fatal_delta=%s\n  next round starting\n' \
        "$round" "$(date '+%F %T')" "$elapsed" "$gname" "$nerr" "$((after - before))" > "$STATUS"

    # Never drop a benchmark database: it is shared across rounds and took minutes to materialise.
    [ -z "$benchdb" ] && timeout 120 $MYSQL -e "drop database if exists $db" >/dev/null 2>&1
done
