#!/bin/bash
# Restart one run directory's cluster-fuzz instances with the environment that started them.
#
# srfuzz-launch.sh copies this file into the run directory as restart.sh. Run it INSIDE the
# container, which is where 127.0.0.1:9030 means the cluster under test:
#
#   docker exec -d <container> bash -lc /path/to/clusterfuzz-<run>/restart.sh
#
# It takes NO arguments and substitutes nothing. Everything it needs is either its own location or
# already written down in run.conf, which is the point: the previous version of this file had the
# run directory hardcoded to a run that no longer existed, so the watchdog's alert named a remedy
# that restarted nothing -- and the version before THAT tried to template four values through
# sh_run's double quotes, a heredoc and python at once.
#
# Starting an instance takes four environment variables and that instance's own knob file. Every one
# of them is load-bearing, and dropping one produces a run that looks healthy:
#   INSTANCE / NINSTANCES  which slice of the corpus this instance walks
#   SRFUZZ_GEN_SEED        the data generator's seed; a different one is a different corpus order
#   DIFF_MAX_STMTS         how many statements per group reach the differential
#   DIFF_KNOBS             THIS instance's knobs_$k.txt. Without it the harness falls back to the
#                          built-in pool, so an instance meant to run a third of the knobs runs all
#                          of them, every round costs three times as much, and nothing says so.
set -u

R=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CONF=$R/run.conf
[ -f "$CONF" ] || { echo "no run.conf in $R -- is this a run directory?" >&2; exit 1; }

# Read from run.conf rather than from arguments: the values that started the run are the only ones
# that can restart it, and they are already recorded there for exactly this reason.
conf() { sed -n "s/^$1=\([^ ]*\).*/\1/p" "$CONF" | head -1; }
N=${NINSTANCES:-$(conf RUN_INSTANCES)}
SEED=${SRFUZZ_GEN_SEED:-$(conf RUN_SEED)}
DMS=${DIFF_MAX_STMTS:-$(conf DIFF_MAX_STMTS)}
[ -n "$N" ] && [ -n "$SEED" ] && [ -n "$DMS" ] || {
    echo "run.conf is missing RUN_INSTANCES/RUN_SEED/DIFF_MAX_STMTS -- refusing to guess" >&2; exit 1; }

k=0
while [ "$k" -lt "$N" ]; do
    [ -s "$R/knobs_$k.txt" ] || { echo "knobs_$k.txt is missing or empty in $R" >&2; exit 1; }
    k=$((k + 1))
done

echo "restarting $N instance(s) in $R (seed=$SEED diff_max_stmts=$DMS)"
pkill -f "$R/clusterfuzz.run.sh"
sleep 3
pkill -9 -f "$R/clusterfuzz.run.sh" 2>/dev/null
sleep 1

cd "$R" || exit 1
k=0
while [ "$k" -lt "$N" ]; do
    mkdir -p "$R/inst$k"
    NINSTANCES=$N INSTANCE=$k SRFUZZ_GEN_SEED=$SEED DIFF_MAX_STMTS=$DMS \
        DIFF_KNOBS="$(cat "$R/knobs_$k.txt")" \
        setsid ./clusterfuzz.run.sh >> "$R/inst$k/run.stdout" 2>> "$R/inst$k/run.stderr" &
    k=$((k + 1))
    sleep 2
done

sleep 8
# Count what is actually running, and count only THIS run's instances -- several runs share a host.
printf 'instances now running in %s: %s (wanted %s)\n' \
    "$R" "$(pgrep -fc "$R/clusterfuzz.run.sh" || echo 0)" "$N"
