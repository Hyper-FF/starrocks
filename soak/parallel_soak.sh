#!/usr/bin/env bash
# Concurrent AST-mutation fuzzing: N JVMs over disjoint corpus shards, one merged report per round.
#
# The single-process soak used under three of this box's 104 cores, so throughput was never limited
# by the mutator -- it was limited by running one JVM. Sharding is done at the process level because
# schema setup goes through StarRocksAssert into the process's one in-process catalog and
# GlobalStateMgr is a singleton: two threads setting up different corpus files would race on table
# names and see each other's schemas. Separate processes share nothing.
#
# Each shard gets its own RNG seed derived from the round seed, so shards explore different mutations
# of their own files rather than replaying the same RNG stream.

set -u

ROOT=${ROOT:-/home/disk1/fha/sr-ws/fuzzdev}
OUT=${OUT:-/home/disk1/fha/soak-parallel}
SHARDS=${SHARDS:-12}
MUTATIONS=${MUTATIONS:-40}
CHAIN=${CHAIN:-4}
XMX=${XMX:-3g}
# Ceiling for one shard. Above this it is hung, not slow.
SHARD_TIMEOUT=${SHARD_TIMEOUT:-1500}

SEEN=$OUT/seen-signatures.txt
FINDINGS=$OUT/new-findings.md
STATE=$OUT/rounds.tsv
STATUS=$OUT/status

say() { printf '%s %s\n' "$(date '+%F %T')" "$*"; }

export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}
export PATH=$JAVA_HOME/bin:$PATH
export MAVEN_OPTS=-Xmx${XMX}

mkdir -p "$OUT/reports" "$OUT/logs"
touch "$SEEN"
[ -f "$STATE" ] || printf 'round\tseed\tshards\tmutants\tok\trejected\tbugsigs\tnewsigs\tsecs\n' > "$STATE"

round=$(( $(wc -l < "$STATE") - 1 ))
[ "$round" -lt 0 ] && round=0

while true; do
    round=$((round + 1))
    seed=$(( (RANDOM << 15 | RANDOM) ))
    started=$(date +%s)
    stamp=$(printf '%05d' "$round")
    rm -f "$OUT"/reports/r${stamp}-s*.md

    say "round $round starting, seed=$seed, shards=$SHARDS"
    printf 'round %s RUNNING since %s (seed %s, %s shards)\n' \
        "$round" "$(date '+%F %T')" "$seed" "$SHARDS" > "$STATUS"

    # A leftover *.part.lock in the shared repo blocks EVERY later resolution of that artifact,
    # forever, because its owner is long gone. All 12 shards share $OUT/m2ro, so one stale lock wedges
    # the whole fan-out -- that is how one round sat for 16.7 hours. The locks are only meaningful
    # while a maven is running, and none is at this point in the round, so sweeping here is safe.
    find "$OUT/m2ro" -name '*.part' -delete 2>/dev/null
    find "$OUT/m2ro" -name '*.lock' -delete 2>/dev/null

    # Compile ONCE, before the fan-out. Twelve mavens running the full `test` lifecycle against one
    # tree all drive the thrift codegen plugin into the same target/ directory and clobber each other:
    # the first attempt lost 10 of 12 shards to "maven-thrift-plugin ... thrift failed". Shards then
    # run surefire:test alone, which executes tests without regenerating or recompiling anything.
    if ! (cd "$ROOT/fe" && mvn -q -pl fe-core -am test-compile -DskipTests -Djacoco.skip=true \
            > "$OUT/logs/r${stamp}-build.log" 2>&1); then
        say "  BUILD FAILED, skipping round -- see $OUT/logs/r${stamp}-build.log"
        say "  $(grep -E 'ERROR.*(java|Failed to execute)' "$OUT/logs/r${stamp}-build.log" 2>/dev/null | head -1 | cut -c1-180)"
        deadrounds=$((${deadrounds:-0} + 1))
        [ "$deadrounds" -ge 3 ] && { say "  STOPPING: three failed builds in a row."; exit 1; }
        sleep $((deadrounds * 10))
        continue
    fi

    # One shared copy of the resolved repository for the shards to read. Made once per round, after
    # the build has resolved everything, so no shard ever writes to it and none can lock another out.
    if [ ! -d "$OUT/m2ro" ]; then
        say "  priming a read-only maven repository for the shards"
        cp -a "${HOME:-/root}/.m2/repository" "$OUT/m2ro" 2>/dev/null || mkdir -p "$OUT/m2ro"
    fi

    pids=()
    for i in $(seq 0 $((SHARDS - 1))); do
        (
            cd "$ROOT/fe" || exit 1
            # A distinct seed per shard: sharing one would make every shard walk the same RNG stream
            # over different files, which correlates their mutation choices for no reason.
            # Offline, and with a private read-only view of the repository. Twelve mavens sharing one
            # ~/.m2 serialise on its lock files: a round hung for 29 minutes on a single
            # starcommon-0.2.7.jar.part.lock while eleven shards waited and the machine sat at 0.2 load.
            # The build phase above has already resolved everything, so shards need no writes at all.
            # A hung shard must not hold the round. One did, for 29 minutes, and the merge simply
            # waited: the round looked like it was working because nothing said otherwise.
            # -k: SIGTERM alone does not kill a wedged JVM. One shard sat in futex_wait for
            # 16.7 HOURS after its timeout fired, and the round waited on it the whole time --
            # zero output, process tree perfectly healthy-looking. Escalate to SIGKILL.
            # -s QUIT before the kill: a JVM dumps every thread stack on SIGQUIT, so a wedged
            # shard writes WHY it is wedged into its own log before it dies. The one that hung
            # for 16.7 hours took the answer with it -- there was nothing to look at afterwards.
            # QUIT does not terminate the JVM, so -k still does the actual killing.
            timeout -k 60 -s QUIT "$SHARD_TIMEOUT" \
            mvn -q -o -Dmaven.repo.local="$OUT/m2ro" -pl fe-core surefire:test -Dtest=AstMutationFuzzerTest \
                -Dsrfuzz.corpus="$ROOT/test/sql" \
                -Dsrfuzz.mutations="$MUTATIONS" \
                -Dsrfuzz.chain="$CHAIN" \
                -Dsrfuzz.seed=$(( seed + i * 7919 )) \
                -Dsrfuzz.shards="$SHARDS" -Dsrfuzz.shard="$i" \
                -Dsrfuzz.report="$OUT/reports/r${stamp}-s${i}.md" \
                -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false -Djacoco.skip=true \
                > "$OUT/logs/r${stamp}-s${i}.log" 2>&1
        ) & pids+=($!)
    done
    # A shard that dies must not be silently dropped: its report simply would not exist, and the
    # merged totals would look like a smaller but healthy round.
    failed=0
    for p in "${pids[@]}"; do wait "$p" || failed=$((failed + 1)); done
    elapsed=$(( $(date +%s) - started ))

    present=$(ls "$OUT"/reports/r${stamp}-s*.md 2>/dev/null | wc -l)
    if [ "$present" -lt "$SHARDS" ]; then
        say "  WARNING: $((SHARDS - present)) of $SHARDS shards produced no report (exit failures: $failed)"
        say "  first shard log tail: $(tail -3 "$OUT/logs/r${stamp}-s0.log" 2>/dev/null | tr '\n' ' ' | cut -c1-200)"
    fi

    # A round that produced nothing is a broken round, not a quiet one, and looping straight into the
    # next one at full speed is how a harness burns hours looking busy. Every long-running loop in this
    # campaign has made that mistake at least once, so this one refuses to.
    if [ "$present" -eq 0 ]; then
        deadrounds=$((${deadrounds:-0} + 1))
        say "  ROUND PRODUCED NOTHING ($deadrounds consecutive). Backing off ${deadrounds}0s."
        printf 'round %s DEAD at %s -- %s consecutive empty rounds, see %s\n' \
            "$round" "$(date '+%F %T')" "$deadrounds" "$OUT/logs/r${stamp}-s0.log" > "$STATUS"
        if [ "$deadrounds" -ge 3 ]; then
            say "  STOPPING: three empty rounds in a row. Fix the environment, then restart."
            exit 1
        fi
        sleep $((deadrounds * 10))
        continue
    fi
    deadrounds=0

    # Merge. Totals add up; signatures are deduped across shards, because the same defect reached
    # from two different files is one finding, not two.
    mutants=0; ok=0; rej=0
    for f in "$OUT"/reports/r${stamp}-s*.md; do
        [ -f "$f" ] || continue
        m=$(sed -n 's/^seeds: [0-9]*, mutants: \([0-9]*\).*/\1/p' "$f" | head -1)
        o=$(sed -n 's/^| OK | \([0-9]*\) |.*/\1/p' "$f" | head -1)
        r=$(sed -n 's/^| ANALYZE_REJECTED | \([0-9]*\) |.*/\1/p' "$f" | head -1)
        mutants=$((mutants + ${m:-0})); ok=$((ok + ${o:-0})); rej=$((rej + ${r:-0}))
    done

    sigs=$(for f in "$OUT"/reports/r${stamp}-s*.md; do
               [ -f "$f" ] || continue
               awk '/^## Bug candidates/{on=1; next} /^## /{on=0} on && /^### /{
                        sub(/ \(x[0-9]+\)$/, ""); sub(/^### /, ""); print}' "$f"
           done | sort -u)
    nsig=$(printf '%s' "$sigs" | grep -c . || true)

    newsigs=0
    while IFS= read -r sig; do
        [ -z "$sig" ] && continue
        grep -Fxq "$sig" "$SEEN" && continue
        printf '%s\n' "$sig" >> "$SEEN"
        newsigs=$((newsigs + 1))
        {
            printf '\n## NEW %s (round %s, seed %s)\n\n' "$sig" "$round" "$seed"
            for f in "$OUT"/reports/r${stamp}-s*.md; do
                [ -f "$f" ] || continue
                awk -v want="$sig" '
                    index($0, want) && /^### /{on=1}
                    on{print}
                    on && /^$/{blank++; if (blank>2) exit}
                ' "$f" | head -14
            done
        } >> "$FINDINGS"
        say "  NEW SIGNATURE :: $(cut -c1-120 <<< "$sig")"
    done <<< "$sigs"

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$round" "$seed" "$present" "$mutants" "$ok" "$rej" "$nsig" "$newsigs" "$elapsed" >> "$STATE"
    say "round $round done in ${elapsed}s: shards=$present mutants=$mutants ok=$ok rejected=$rej sigs=$nsig new=$newsigs"
    printf 'round %s DONE at %s in %ss: mutants=%s sigs=%s new=%s\n' \
        "$round" "$(date '+%F %T')" "$elapsed" "$mutants" "$nsig" "$newsigs" > "$STATUS"

    ls -t "$OUT"/reports/*.md 2>/dev/null | tail -n +200 | xargs -r rm -f
    ls -t "$OUT"/logs/*.log   2>/dev/null | tail -n +200 | xargs -r rm -f
done
