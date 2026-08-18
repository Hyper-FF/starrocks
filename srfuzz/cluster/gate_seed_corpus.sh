#!/usr/bin/env bash
# Run a packed seed corpus against a real cluster once, and keep only the statements that are worth
# replaying forever.
#
# A harvested corpus binds partially -- production shapes arrive with synthesised schemas, an
# external engine's suite arrives in a dialect that is only mostly ours. Measured on the FE, the
# production fixtures bind at 59-77%. Merged unfiltered, the missing 23-41% do not just waste the
# round: every one of them raises an error the harness classifies and files, so a corpus defect
# arrives looking exactly like a product defect, forever, on every pass over the corpus.
#
# So each statement runs once, alone, against a scratch database, and the error decides:
#
#   * an ANALYSIS error (unknown table/column, no matching function, parse error) is a defect in the
#     corpus. Dropped, with the reason recorded -- a drop nobody can audit is how a corpus quietly
#     shrinks to the statements that happened to be easy.
#   * an INTERNAL error (Invalid plan, internal error, an FE exception) is exactly what the campaign
#     hunts. KEPT, and listed separately: it is a finding candidate before the fuzzer has mutated
#     anything.
#   * success is kept.
#
# The gate must NOT run against the campaign's own cluster. Setup for 640 groups creates tens of
# thousands of tablets, and the differential and TLP oracles compare row counts on the same backend.
#
#   ./gate_seed_corpus.sh <corpus-dir> <out-dir> [workers]
#
# Env: MYSQL_HOST/MYSQL_PORT (the GATE cluster), QUERY_TIMEOUT (default 20s).
set -u

CORPUS=${1:?usage: gate_seed_corpus.sh <corpus-dir> <out-dir> [workers]}
OUT=${2:?usage: gate_seed_corpus.sh <corpus-dir> <out-dir> [workers]}
WORKERS=${3:-4}
HOST=${MYSQL_HOST:-127.0.0.1}
PORT=${MYSQL_PORT:-9030}
QUERY_TIMEOUT=${QUERY_TIMEOUT:-20}
MYSQL="mysql -h$HOST -P$PORT -uroot"

mkdir -p "$OUT"

# Errors that mean "this statement was never going to run here". Matched on the message, not on an
# exception class: the FE reports the same analysis failure under several classes depending on which
# analyzer refused it, and classing by exception is what filed deliberate refusals as internal errors
# in an earlier incident.
#
# `No viable statement` and `Unknown command` were missing from the first version, and both are
# parser-level rejections of the corpus itself. Four statements survived the gate because of it and
# went on to be filed as NEW ERROR findings by the campaign -- a corpus defect wearing a product
# defect's clothes, which is the exact failure this gate exists to prevent.
corpus_defect() {
    grep -qiE 'Unknown table|Unknown database|Unknown column|Column .* cannot be resolved|cannot be resolved|Unknown partition|Getting analyzing error|No matching function|Unsupported|not supported|You have an error in your SQL syntax|Unexpected input|Ambiguous column|is ambiguous|Invalid column|Table .* is not found|does not exist|Unknown function|Illegal type|Invalid type|cannot cast|Not support|no viable alternative|No viable statement|extraneous input|mismatched input|Unknown command|Access denied' <<< "$1"
}

# The opposite end: an error that names an engine fault rather than a corpus fault.
internal_error() {
    grep -qiE 'Invalid plan|Internal error|NullPointerException|IndexOutOfBounds|IllegalState|Unexpected exception|check failed|Backend node|assert|Segmentation' <<< "$1"
}

run_worker() {
    local wid=$1
    local db="srfuzz_gate_$wid"
    local kept_total=0 dropped_total=0 internal_total=0 groups_ok=0 groups_dead=0 i=0

    for setup in "$CORPUS"/*.setup.sql; do
        i=$((i + 1))
        [ $(( (i - 1) % WORKERS )) -eq "$wid" ] || continue
        local g=${setup%.setup.sql}
        local name; name=$(basename "$g")
        local query="$g.query.sql"
        [ -s "$query" ] || continue

        timeout 60 $MYSQL -e "drop database if exists $db; create database $db" >/dev/null 2>&1
        timeout 300 $MYSQL "$db" -f < "$setup" > /dev/null 2>"$OUT/$name.setup.err"
        local ntables
        ntables=$(timeout 60 $MYSQL "$db" -N -e 'show tables' 2>/dev/null | grep -c .)
        if [ "${ntables:-0}" -eq 0 ]; then
            # No schema: every query would fail with Unknown table and the whole group would be
            # dropped one confusing line at a time. Record it as one group-level fact instead.
            printf '%s\tsetup-produced-no-tables\t%s\n' "$name" \
                "$(tr '\n' ' ' < "$OUT/$name.setup.err" | cut -c1-300)" >> "$OUT/dead-groups.w$wid.tsv"
            groups_dead=$((groups_dead + 1))
            continue
        fi
        rm -f "$OUT/$name.setup.err"

        local kept=0 dropped=0 line err
        : > "$OUT/$name.query.kept"
        while IFS= read -r line; do
            case "$line" in ''|--*) continue ;; esac
            err=$(printf '%s\n' "$line" | timeout "$QUERY_TIMEOUT" $MYSQL "$db" 2>&1 >/dev/null)
            # The client echoes the offending statement around the message, so the raw capture is
            # mostly a copy of the query. Classification reads the whole blob; the recorded reason
            # is the ERROR line alone, or a drop log is unreadable and nobody audits it.
            local reason; reason=$(grep -o 'ERROR [0-9].*' <<< "$err" | head -1)
            [ -z "$reason" ] && reason=$(tr '\n' ' ' <<< "$err")
            if [ -z "$err" ]; then
                printf '%s\n' "$line" >> "$OUT/$name.query.kept"
                kept=$((kept + 1))
            elif internal_error "$err"; then
                # Kept on purpose, and reported: this is a finding candidate found by replay alone.
                printf '%s\n' "$line" >> "$OUT/$name.query.kept"
                printf '%s\t%s\t%s\n' "$name" "$(cut -c1-200 <<< "$reason")" \
                    "$(cut -c1-400 <<< "$line")" >> "$OUT/internal-errors.w$wid.tsv"
                kept=$((kept + 1))
                internal_total=$((internal_total + 1))
            elif corpus_defect "$err"; then
                printf '%s\t%s\t%s\n' "$name" "$(cut -c1-160 <<< "$reason")" \
                    "$(cut -c1-300 <<< "$line")" >> "$OUT/dropped.w$wid.tsv"
                dropped=$((dropped + 1))
            else
                # Unclassified: kept, because dropping what we do not understand is how a gate
                # silently becomes a filter for "queries that are easy to run".
                printf '%s\n' "$line" >> "$OUT/$name.query.kept"
                printf '%s\t%s\t%s\n' "$name" "$(cut -c1-160 <<< "$reason")" \
                    "$(cut -c1-300 <<< "$line")" >> "$OUT/unclassified.w$wid.tsv"
                kept=$((kept + 1))
            fi
        done < "$query"

        if [ "$kept" -eq 0 ]; then
            rm -f "$OUT/$name.query.kept"
            printf '%s\tall-%s-queries-dropped\t-\n' "$name" "$dropped" >> "$OUT/dead-groups.w$wid.tsv"
            groups_dead=$((groups_dead + 1))
        else
            cp "$setup" "$OUT/$name.setup.sql"
            mv "$OUT/$name.query.kept" "$OUT/$name.query.sql"
            groups_ok=$((groups_ok + 1))
        fi
        kept_total=$((kept_total + kept)); dropped_total=$((dropped_total + dropped))
        printf 'w%s %s: kept=%s dropped=%s (groups ok=%s dead=%s)\n' \
            "$wid" "$name" "$kept" "$dropped" "$groups_ok" "$groups_dead"
    done

    timeout 60 $MYSQL -e "drop database if exists $db" >/dev/null 2>&1
    printf 'w%s DONE groups_ok=%s groups_dead=%s kept=%s dropped=%s internal=%s\n' \
        "$wid" "$groups_ok" "$groups_dead" "$kept_total" "$dropped_total" "$internal_total" \
        | tee -a "$OUT/SUMMARY.txt"
}

echo "gate: corpus=$CORPUS out=$OUT workers=$WORKERS cluster=$HOST:$PORT"
for w in $(seq 0 $((WORKERS - 1))); do
    run_worker "$w" &
done
wait

cat "$OUT"/dropped.w*.tsv 2>/dev/null > "$OUT/dropped.tsv"
cat "$OUT"/dead-groups.w*.tsv 2>/dev/null > "$OUT/dead-groups.tsv"
cat "$OUT"/internal-errors.w*.tsv 2>/dev/null > "$OUT/internal-errors.tsv"
cat "$OUT"/unclassified.w*.tsv 2>/dev/null > "$OUT/unclassified.tsv"
rm -f "$OUT"/dropped.w*.tsv "$OUT"/dead-groups.w*.tsv "$OUT"/internal-errors.w*.tsv "$OUT"/unclassified.w*.tsv

printf 'gated corpus: %s groups, %s statements\n' \
    "$(ls "$OUT"/*.setup.sql 2>/dev/null | wc -l)" \
    "$(cat "$OUT"/*.query.sql 2>/dev/null | grep -vc '^--')"
printf 'dropped %s statements, %s dead groups, %s internal-error statements kept\n' \
    "$(wc -l < "$OUT/dropped.tsv" 2>/dev/null || echo 0)" \
    "$(wc -l < "$OUT/dead-groups.tsv" 2>/dev/null || echo 0)" \
    "$(wc -l < "$OUT/internal-errors.tsv" 2>/dev/null || echo 0)"
