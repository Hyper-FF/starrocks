#!/usr/bin/env bash
# Watch the concurrent cluster-fuzz instances, and revive the ones that die.
#
# The campaign's recurring failure is that a broken run looks exactly like a healthy one. With N
# instances there is a new way for that to happen: an instance exits, the remaining ones keep writing
# perfectly normal rounds, and the only symptom is that the corpus takes longer to get through --
# which nobody is measuring. So this does not just observe. It counts the instances, says so on every
# tick, and restarts the missing ones loudly.
#
# Run detached with `docker exec -d`, not `setsid nohup` inside a docker exec: the latter dies with
# the exec's session, which is how one earlier run vanished without an explanation.
set -u

CF=/home/disk1/fha/sr-ws/fuzz/clusterfuzz
W=/home/disk1/fha/sr-ws/fuzz
N=${NINSTANCES:-3}
INTERVAL=${INTERVAL:-300}
MYSQL="mysql -h127.0.0.1 -P9030 -uroot"
LOG=$CF/watch.log
SNAP=$CF/watch.tsv
# earlyoom on this box fires at 10% free. Warn above that, while there is still room to react.
MEM_WARN_PCT=${MEM_WARN_PCT:-18}
# The bound the backend is supposed to be running under. Setting it in be.conf is not enough: a BE
# rebuild or redeploy regenerates that file and silently drops the line -- it happened within hours
# of it being added, exactly the way build.sh --fe wipes default_replication_num. An unbounded BE on
# this box grows past 100 GB and earlyoom then kills it, and other people's processes with it. So
# check what the RUNNING process reports, not what the config file says.
WANT_MEM_LIMIT=${WANT_MEM_LIMIT:-60G}
# A round is 45s of load plus setup, generation and the oracles; a minimize pass over a group can add
# minutes. Well past that, an instance is stuck rather than slow.
STALE_SECONDS=${STALE_SECONDS:-1500}

say() { printf '%s %s\n' "$(date '+%F %T')" "$*" >> "$LOG"; }

# Memory, sampled fast enough to catch the thing that causes the kill.
#
# earlyoom fired at 10:37:38 with the box below 10% available, four minutes after a 300s snapshot
# showed 103 GB free -- so the whole excursion happened between two samples and named nobody. It also
# SIGKILLed other users' processes, which makes "was it us" a question worth answering rather than
# assuming either way. Every 30s, with the three largest RSS consumers, so the next spike is
# attributable instead of merely recorded.
MEMSNAP=$CF/mem.tsv
[ -f "$MEMSNAP" ] || printf 'ts\tavail_pct\tbe_rss_gb\ttop3\n' > "$MEMSNAP"
mem_sampler() {
    while true; do
        local tot avail pct berss top
        read -r tot avail <<< "$(free -m | awk '/^Mem:/ {print $2, $7}')"
        pct=$(( avail * 100 / tot ))
        berss=$(ps -o rss= -C starrocks_be 2>/dev/null | head -1 | tr -dc '0-9')
        berss=$(awk -v r="${berss:-0}" 'BEGIN{printf "%.1f", r/1048576}')
        top=$(ps -eo rss,comm --sort=-rss 2>/dev/null | sed -n '2,4p' \
              | awk '{printf "%s:%.1fG ", $2, $1/1048576}')
        printf '%s\t%s\t%s\t%s\n' "$(date '+%F %T')" "$pct" "$berss" "$top" >> "$MEMSNAP"
        [ "$pct" -lt 20 ] && say "MEM $pct% available, be=${berss}G, top: $top"
        sleep 30
    done
}

[ -f "$SNAP" ] || printf 'ts\tinstances_up\tfe\tbe\tmem_free_pct\tbe_rss_gb\tfe_starts\trounds_total\tgen_zero_total\terrors_total\n' > "$SNAP"

# The instance number a running harness belongs to, read from its own environment. A top-level
# instance is one whose parent is not itself part of a harness -- the round loop forks subshells for
# every reader and writer, and counting those would report 20 instances where there are 3.
instance_of() { tr '\0' '\n' < "/proc/$1/environ" 2>/dev/null | sed -n 's/^INSTANCE=//p' | head -1; }

running_instances() {
    local p ppid k
    {
        for p in $(pgrep -f '[c]lusterfuzz.next.sh' 2>/dev/null); do
            ppid=$(awk '{print $4}' "/proc/$p/stat" 2>/dev/null)
            grep -q 'clusterfuzz.next.sh' "/proc/$ppid/cmdline" 2>/dev/null && continue
            k=$(instance_of "$p")
            [ -n "$k" ] && printf '%s\n' "$k"
        done
    } 2>/dev/null | sort -u
}

# Relaunching an instance that was in fact running is worse than missing one for a tick: two
# instances on one shard create, populate and drop the same databases underneath each other, and the
# rounds they produce look perfectly ordinary. A /proc read that loses a race is enough to cause it,
# so a missing instance is confirmed by a second look before anything is started.
confirm_missing() {
    local k=$1
    sleep 20
    ! grep -qx "$k" <<< "$(running_instances)"
}

# Relaunching from in here does not work and is worse than not trying: an instance started with
# setsid from inside a docker exec belongs to that exec's tree, so it is SIGTERMed when the exec
# churns -- 72 relaunches, each killed mid-round, rounds.tsv frozen while the log showed the
# watchdog dutifully "relaunching". Instances must be started from OUTSIDE with `docker exec -d`,
# one exec per instance. Default here is to report and let a human (or the host-side launcher) act.
launch_instance() {
    local k=$1
    if [ "${AUTO_RELAUNCH:-0}" != "1" ]; then
        say "ALERT: instance $k is NOT running -- start it from the host: /tmp/launch_cf.sh $k $N"
        return 0
    fi
    mkdir -p "$CF/inst$k"
    setsid bash -lc "cd $CF && NINSTANCES=$N INSTANCE=$k SRFUZZ_GEN_SEED=${SRFUZZ_GEN_SEED:-20260731} exec ./clusterfuzz.next.sh >> inst$k/run.stdout 2>> inst$k/run.stderr" \
        >/dev/null 2>&1 &
    say "ALERT: instance $k was not running -- relaunched"
}

# Liveness by asking the service, never by asking the OS for a process name. The one time this rule
# was broken the harness fired 2198 queries at a backend that was not serving and recorded 79128
# errors that meant nothing.
fe_alive() { timeout 15 $MYSQL -N -e 'select 1' >/dev/null 2>&1; }
be_alive() { timeout 20 $MYSQL -N -e 'show backends' 2>/dev/null | awk -F'\t' '{print $9}' | grep -qi true; }

say "watchdog started: N=$N interval=${INTERVAL}s gen_seed=${SRFUZZ_GEN_SEED:-20260731}"
mem_sampler &
say "memory sampler started (30s, $MEMSNAP)"

while true; do
    up=$(running_instances)
    nup=$(printf '%s' "$up" | grep -c . )

    for k in $(seq 0 $((N - 1))); do
        grep -qx "$k" <<< "$up" && continue
        say "instance $k not seen -- confirming"
        confirm_missing "$k" && launch_instance "$k" || say "instance $k reappeared on the second look; not relaunched"
    done

    # Running but not advancing is its own failure, and it does not show up in a process count.
    for k in $(seq 0 $((N - 1))); do
        f=$CF/inst$k/rounds.tsv
        [ -f "$f" ] || continue
        age=$(( $(date +%s) - $(stat -c %Y "$f") ))
        [ "$age" -gt "$STALE_SECONDS" ] && say "ALERT: instance $k has not finished a round in ${age}s"
    done

    fe=down; be=down
    fe_alive && fe=up
    [ "$fe" = up ] && { be_alive && be=up; }
    # An instance only reaches its own restart_fe at the top of a round, and with the frontend gone
    # every step of the round it is currently in has to time out first -- so the cluster can sit dead
    # for many minutes with three healthy-looking instances attached to it. Restart it here, under
    # the SAME lock the instances use, and re-check inside the lock so this never fights them.
    if [ "$fe" = down ]; then
        say "ALERT: FE is not answering -- restarting it"
        {
            flock 9
            if fe_alive; then
                say "FE already back (an instance got there first)"
            else
                export JAVA_HOME=${JAVA_HOME:-/lib/jvm/java-21-openjdk}
                "$W/output/fe/bin/stop_fe.sh" >/dev/null 2>&1
                sleep 5
                # 9>&- : see clusterfuzz.next.sh -- a daemon started inside the lock block
                # inherits fd 9 and holds the lock until it dies.
                setsid nohup "$W/output/fe/bin/start_fe.sh" --daemon >/dev/null 2>&1 9>&-
                for _ in $(seq 1 18); do
                    sleep 10
                    fe_alive && { say "FE back up (watchdog)"; fe=up; break; }
                done
                [ "$fe" = down ] && say "ALERT: FE did not come back"
            fi
        } 9>>"$CF/fe-restart.lock"
    fi
    [ "$fe" = up ] && [ "$be" = down ] && say "ALERT: BE is not registered Alive"

    read -r memtotal memfree <<< "$(free -m | awk '/^Mem:/ {print $2, $7}')"
    mempct=$(( memfree * 100 / memtotal ))
    [ "$mempct" -lt "$MEM_WARN_PCT" ] && say "ALERT: ${mempct}% memory available -- earlyoom (-m 10) territory; top: $(ps -eo rss,comm --sort=-rss | sed -n '2,4p' | tr '\n' ' ')"

    berss=$(ps -o rss= -C starrocks_be 2>/dev/null | head -1 | tr -dc '0-9')
    berss=$(awk -v r="${berss:-0}" 'BEGIN{printf "%.1f", r/1048576}')
    eff_limit=$(curl -s --max-time 8 http://127.0.0.1:8040/varz 2>/dev/null | sed -n 's/^mem_limit=//p' | head -1)
    if [ -n "$eff_limit" ] && [ "$eff_limit" != "$WANT_MEM_LIMIT" ]; then
        say "ALERT: BE mem_limit is '$eff_limit', expected '$WANT_MEM_LIMIT' -- be.conf was regenerated and the backend is unbounded again"
    fi

    festarts=$(grep -c 'transfer: LEADER' "$W/output/fe/log/fe.out" 2>/dev/null | head -1 | tr -dc '0-9')

    rounds=0; genzero=0; errors=0
    for k in $(seq 0 $((N - 1))); do
        read -r r z e <<< "$(awk -F'\t' 'NR>1{n++; if($5==0 && $3>0) z++; e+=$7} END{print n+0, z+0, e+0}' "$CF/inst$k/rounds.tsv" 2>/dev/null)"
        rounds=$((rounds + ${r:-0})); genzero=$((genzero + ${z:-0})); errors=$((errors + ${e:-0}))
    done
    # Every round generating nothing is the signature of a dead FE, not of a broken generator:
    # gen_data.py cannot reach the server, SHOW DATABASES comes back empty and it still exits 0.
    [ "$rounds" -gt 6 ] && [ "$genzero" -eq "$rounds" ] && say "ALERT: every round loaded zero rows -- check the FE, not the generator"

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$(date '+%F %T')" "$nup" "$fe" "$be" "$mempct" "$berss" "${festarts:-0}" "$rounds" "$genzero" "$errors" >> "$SNAP"

    sleep "$INTERVAL"
done
