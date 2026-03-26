#!/usr/bin/env bash
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# ============================================================
# page_cache_stall_detect.sh
#
# Diagnoses OS page cache reclaim stalls affecting StarRocks BE.
#
# Root cause scenarios detected:
#   1. Direct reclaim      - business thread blocked in kernel doing page reclaim
#   2. Writeback stall     - dirty pages waiting for disk IO before reclaim
#   3. kswapd pressure     - background reclaimer unable to keep up
#   4. madvise DONTNEED    - jemalloc returning memory blocked by THP compaction
#   5. drop_caches cronjob - burst reclaim from manual cache drops
#   6. cgroup limit        - memory cgroup throttling StarRocks BE
#
# Usage:
#   sudo ./page_cache_stall_detect.sh [OPTIONS]
#
# Options:
#   -d SECONDS   Collection duration (default: 60)
#   -t MS        Stall reporting threshold in milliseconds (default: 10)
#   -p PID       StarRocks BE pid (auto-detected if omitted)
#   -o DIR       Output directory (default: /tmp/sr_page_stall_<timestamp>)
#   -h           Show this help
#
# Requirements:
#   - Linux kernel >= 4.14
#   - bpftrace >= 0.12
#   - root or CAP_BPF + CAP_PERFMON
# ============================================================

set -euo pipefail

# ── defaults ─────────────────────────────────────────────────
DURATION=60
THRESHOLD_MS=10
BE_PID=""
OUTPUT_DIR=""
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# ── colours ──────────────────────────────────────────────────
RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
title() { echo -e "\n${BOLD}$*${NC}"; echo "$*" | sed 's/./─/g'; }

usage() {
    sed -n '/^# Usage/,/^# Requirements/p' "$0" | grep -v '^# ====' | sed 's/^# //'
    exit 0
}

while getopts "d:t:p:o:h" opt; do
    case $opt in
        d) DURATION=$OPTARG    ;;
        t) THRESHOLD_MS=$OPTARG ;;
        p) BE_PID=$OPTARG      ;;
        o) OUTPUT_DIR=$OPTARG  ;;
        h) usage               ;;
        *) usage               ;;
    esac
done

[[ -z "$OUTPUT_DIR" ]] && OUTPUT_DIR="/tmp/sr_page_stall_${TIMESTAMP}"
mkdir -p "$OUTPUT_DIR"

LOG="$OUTPUT_DIR/detect.log"
REPORT="$OUTPUT_DIR/report.txt"
exec > >(tee -a "$LOG") 2>&1

# ── findings registry ─────────────────────────────────────────
ISSUE_COUNT=0
declare -A FINDINGS  # key -> human description
declare -A SEVERITY  # key -> INFO | WARN | CRIT

add_finding() {
    local key=$1 sev=$2; shift 2
    FINDINGS[$key]="$*"
    SEVERITY[$key]=$sev
    (( ISSUE_COUNT++ )) || true
}

# ════════════════════════════════════════════════════════════
# 1. PREREQUISITES
# ════════════════════════════════════════════════════════════
title "1. Checking prerequisites"

[[ $EUID -ne 0 ]] && { error "Must be run as root."; exit 1; }

KVER=$(uname -r)
KMAJ=$(echo "$KVER" | cut -d. -f1)
KMIN=$(echo "$KVER" | cut -d. -f2)
(( KMAJ < 4 || (KMAJ == 4 && KMIN < 14) )) && { error "Kernel $KVER too old (need >= 4.14)."; exit 1; }
ok "Kernel $KVER"

command -v bpftrace &>/dev/null || { error "bpftrace not found. Install: apt install bpftrace"; exit 1; }
BT_VER=$(bpftrace --version 2>&1 | grep -oE '[0-9]+\.[0-9]+' | head -1)
ok "bpftrace $BT_VER"

if [[ -z "$BE_PID" ]]; then
    BE_PID=$(pgrep -f "starrocks_be" | head -1 || true)
fi
if [[ -z "$BE_PID" ]]; then
    warn "starrocks_be not found — per-process probe skipped"
else
    ok "starrocks_be pid=$BE_PID"
fi

info "Output    : $OUTPUT_DIR"
info "Duration  : ${DURATION}s  |  Threshold: ${THRESHOLD_MS}ms"

# ════════════════════════════════════════════════════════════
# 2. STATIC CHECKS
# ════════════════════════════════════════════════════════════
title "2. Static system checks"

# THP
THP_MODE=$(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null \
           | grep -oP '\[\K[^\]]+' || echo "unknown")
if [[ "$THP_MODE" == "always" ]]; then
    warn "THP=always: compaction can block madvise for hundreds of ms"
    add_finding "thp" "WARN" "THP=always — madvise(MADV_DONTNEED) may stall during huge page split"
else
    ok "THP=$THP_MODE"
fi

# dirty ratios
DIRTY_RATIO=$(cat /proc/sys/vm/dirty_ratio)
DIRTY_BG=$(cat /proc/sys/vm/dirty_background_ratio)
if (( DIRTY_RATIO > 15 )); then
    warn "vm.dirty_ratio=$DIRTY_RATIO — dirty pages accumulate before writeback triggers"
    add_finding "dirty_ratio" "WARN" "vm.dirty_ratio=$DIRTY_RATIO > 15 — bulk writeback can block reclaim"
else
    ok "vm.dirty_ratio=$DIRTY_RATIO  vm.dirty_background_ratio=$DIRTY_BG"
fi

# swappiness
SWAPPINESS=$(cat /proc/sys/vm/swappiness)
if (( SWAPPINESS > 10 )); then
    warn "vm.swappiness=$SWAPPINESS — kernel may swap StarRocks anon memory before reclaiming cache"
    add_finding "swappiness" "WARN" "vm.swappiness=$SWAPPINESS — recommend 1 for database workloads"
else
    ok "vm.swappiness=$SWAPPINESS"
fi

# drop_caches cronjobs
DROP_CACHES_JOBS=$(grep -r "drop_caches" /etc/cron* /var/spool/cron/ 2>/dev/null || true)
if [[ -n "$DROP_CACHES_JOBS" ]]; then
    warn "drop_caches cron job detected — causes burst direct reclaim on every run"
    echo "$DROP_CACHES_JOBS"
    add_finding "drop_caches" "CRIT" "Cron job calling drop_caches: $DROP_CACHES_JOBS"
else
    ok "No drop_caches cron jobs"
fi

# memory availability
MEMINFO=$(cat /proc/meminfo)
MEM_TOTAL=$(awk '/MemTotal/    {print $2}' <<< "$MEMINFO")
MEM_AVAIL=$(awk '/MemAvailable/{print $2}' <<< "$MEMINFO")
CACHE_KB=$(awk  '/^Cached:/    {print $2}' <<< "$MEMINFO")
BUFFERS_KB=$(awk '/^Buffers:/  {print $2}' <<< "$MEMINFO")
AVAIL_PCT=$(( MEM_AVAIL * 100 / MEM_TOTAL ))
info "Memory: total=$(( MEM_TOTAL/1024 ))MB  available=$(( MEM_AVAIL/1024 ))MB (${AVAIL_PCT}%)  page_cache=$(( (CACHE_KB+BUFFERS_KB)/1024 ))MB"
if (( AVAIL_PCT < 10 )); then
    warn "Available memory < 10% — high direct reclaim risk"
    add_finding "low_mem" "CRIT" "Available memory only ${AVAIL_PCT}% — expect frequent direct reclaim"
fi

# cgroup memory limit
if [[ -n "$BE_PID" ]]; then
    CGROUP_PATH=$(awk -F: '/memory/{print $3; exit}' /proc/"$BE_PID"/cgroup 2>/dev/null || true)
    if [[ -n "$CGROUP_PATH" ]]; then
        CGROUP_LIMIT=$(cat "/sys/fs/cgroup${CGROUP_PATH}/memory.max" 2>/dev/null \
                    || cat "/sys/fs/cgroup/memory${CGROUP_PATH}/memory.limit_in_bytes" 2>/dev/null \
                    || echo "")
        if [[ "$CGROUP_LIMIT" =~ ^[0-9]+$ ]] && (( CGROUP_LIMIT < 9223372036854775807 )); then
            LIMIT_MB=$(( CGROUP_LIMIT / 1024 / 1024 ))
            CGROUP_USAGE=$(cat "/sys/fs/cgroup${CGROUP_PATH}/memory.current" 2>/dev/null \
                        || cat "/sys/fs/cgroup/memory${CGROUP_PATH}/memory.usage_in_bytes" 2>/dev/null \
                        || echo "0")
            USAGE_MB=$(( CGROUP_USAGE / 1024 / 1024 ))
            USAGE_PCT=$(( USAGE_MB * 100 / LIMIT_MB ))
            info "cgroup: ${USAGE_MB}MB / ${LIMIT_MB}MB (${USAGE_PCT}%)"
            if (( USAGE_PCT > 85 )); then
                warn "cgroup memory at ${USAGE_PCT}% — in-cgroup reclaim active"
                add_finding "cgroup_mem" "CRIT" "cgroup memory usage ${USAGE_PCT}% of limit"
            fi
        else
            ok "No cgroup memory limit"
        fi
    fi
fi

# ════════════════════════════════════════════════════════════
# 3. vmstat BASELINE
# ════════════════════════════════════════════════════════════
title "3. Capturing vmstat baseline"

vmstat_keys="pgscand|pgsteal_anon|pgsteal_file|pgscan_kswapd|pgpgout|nr_dirty|nr_writeback"
VMSTAT_BEFORE="$OUTPUT_DIR/vmstat_before.txt"
VMSTAT_AFTER="$OUTPUT_DIR/vmstat_after.txt"
grep -E "^($vmstat_keys) " /proc/vmstat > "$VMSTAT_BEFORE"
ok "Baseline captured"

# ════════════════════════════════════════════════════════════
# 4. eBPF PROBES  (all run concurrently)
# ════════════════════════════════════════════════════════════
title "4. Starting eBPF probes"

EBPF_PIDS=()

cleanup() {
    for p in "${EBPF_PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
}
trap cleanup EXIT

# ── 4-a. System-wide direct reclaim ──────────────────────────
# Which process triggered reclaim, how long it was blocked, how many pages freed.
bpftrace --timeout "$DURATION" -e "
tracepoint:vmscan:mm_vmscan_direct_reclaim_begin {
    @dr_start[tid] = nsecs;
    @dr_comm[tid]  = comm;
    @dr_pid[tid]   = pid;
}
tracepoint:vmscan:mm_vmscan_direct_reclaim_end {
    if (@dr_start[tid]) {
        \$ms = (nsecs - @dr_start[tid]) / 1000000;
        if (\$ms >= $THRESHOLD_MS) {
            printf(\"DIRECT_RECLAIM comm=%-16s pid=%-6d tid=%-6d pages=%-6d blocked_ms=%lld\n\",
                   @dr_comm[tid], @dr_pid[tid], tid, args->nr_reclaimed, \$ms);
        }
        delete(@dr_start[tid]); delete(@dr_comm[tid]); delete(@dr_pid[tid]);
    }
}
" > "$OUTPUT_DIR/ebpf_direct_reclaim.txt" 2>&1 &
EBPF_PIDS+=($!); info "  [1/5] direct_reclaim probe  pid=$!"

# ── 4-b. Writeback stall ──────────────────────────────────────
# How long writeback takes per device — dirty pages hold up reclaim until IO completes.
bpftrace --timeout "$DURATION" -e "
tracepoint:writeback:writeback_start {
    @wb_start[args->sb_dev]  = nsecs;
    @wb_reason[args->sb_dev] = args->reason;
}
tracepoint:writeback:writeback_written {
    if (@wb_start[args->sb_dev]) {
        \$ms = (nsecs - @wb_start[args->sb_dev]) / 1000000;
        if (\$ms >= $THRESHOLD_MS) {
            printf(\"WRITEBACK dev=%-8d reason=%-2d pages=%-6d blocked_ms=%lld\n\",
                   args->sb_dev, @wb_reason[args->sb_dev], args->nr_pages, \$ms);
        }
        delete(@wb_start[args->sb_dev]);
    }
}
" > "$OUTPUT_DIR/ebpf_writeback.txt" 2>&1 &
EBPF_PIDS+=($!); info "  [2/5] writeback probe        pid=$!"

# ── 4-c. madvise(MADV_DONTNEED) stall ───────────────────────
# jemalloc calls madvise to return memory to OS.
# With THP=always the kernel must split huge pages first — this blocks the caller.
# MADV_DONTNEED = 4
bpftrace --timeout "$DURATION" -e "
tracepoint:syscalls:sys_enter_madvise / args->behavior == 4 / {
    @ma_start[tid] = nsecs;
    @ma_len[tid]   = args->len_in;
    @ma_comm[tid]  = comm;
    @ma_pid[tid]   = pid;
}
tracepoint:syscalls:sys_exit_madvise {
    if (@ma_start[tid]) {
        \$ms = (nsecs - @ma_start[tid]) / 1000000;
        if (\$ms >= $THRESHOLD_MS) {
            printf(\"MADVISE_DONTNEED comm=%-16s pid=%-6d len_mb=%-5lld blocked_ms=%lld\n\",
                   @ma_comm[tid], @ma_pid[tid], @ma_len[tid] / 1048576, \$ms);
        }
        delete(@ma_start[tid]); delete(@ma_len[tid]);
        delete(@ma_comm[tid]); delete(@ma_pid[tid]);
    }
}
" > "$OUTPUT_DIR/ebpf_madvise.txt" 2>&1 &
EBPF_PIDS+=($!); info "  [3/5] madvise probe          pid=$!"

# ── 4-d. kswapd activity ─────────────────────────────────────
# kswapd waking up frequently means memory pressure is sustained.
# If kswapd can't keep up, the kernel falls back to direct reclaim.
bpftrace --timeout "$DURATION" -e "
tracepoint:vmscan:mm_vmscan_kswapd_wake  { @wakeups = count(); }
tracepoint:vmscan:mm_vmscan_kswapd_sleep { @sleeps  = count(); }
interval:s:10 {
    printf(\"KSWAPD wakeups=%-6lld sleeps=%lld\n\", @wakeups, @sleeps);
    clear(@wakeups); clear(@sleeps);
}
" > "$OUTPUT_DIR/ebpf_kswapd.txt" 2>&1 &
EBPF_PIDS+=($!); info "  [4/5] kswapd probe           pid=$!"

# ── 4-e. StarRocks BE — direct reclaim with user stack ───────
# Captures the exact call path inside starrocks_be that triggered reclaim.
# The user-space stack points to which operator / memory allocation caused it.
if [[ -n "$BE_PID" ]]; then
    bpftrace --timeout "$DURATION" -e "
tracepoint:vmscan:mm_vmscan_direct_reclaim_begin / pid == $BE_PID / {
    @sr_start[tid] = nsecs;
    printf(\"SR_RECLAIM_BEGIN tid=%d\\n\", tid);
    print(ustack);
}
tracepoint:vmscan:mm_vmscan_direct_reclaim_end / @sr_start[tid] / {
    \$ms = (nsecs - @sr_start[tid]) / 1000000;
    printf(\"SR_RECLAIM_END   tid=%d pages=%d blocked_ms=%lld\\n\",
           tid, args->nr_reclaimed, \$ms);
    delete(@sr_start[tid]);
}
" > "$OUTPUT_DIR/ebpf_starrocks_be.txt" 2>&1 &
    EBPF_PIDS+=($!); info "  [5/5] starrocks_be probe     pid=$!"
else
    info "  [5/5] starrocks_be probe     skipped (no BE process)"
fi

# ════════════════════════════════════════════════════════════
# 5. vmstat TIME-SERIES  (1s resolution, runs alongside eBPF)
# ════════════════════════════════════════════════════════════
title "5. Recording vmstat time-series"

POLL_FILE="$OUTPUT_DIR/vmstat_timeseries.csv"
printf "ts,pgscand,pgsteal_file,pgsteal_anon,pgscan_kswapd,nr_dirty,nr_writeback,pgpgout\n" > "$POLL_FILE"

poll_vmstat() {
    local deadline=$(( $(date +%s) + DURATION ))
    while (( $(date +%s) < deadline )); do
        # read all keys in one pass to minimise skew
        local row
        row=$(awk '
            /^pgscand /        { pgscand=$2 }
            /^pgsteal_file /   { pgsteal_file=$2 }
            /^pgsteal_anon /   { pgsteal_anon=$2 }
            /^pgscan_kswapd /  { pgscan_kswapd=$2 }
            /^nr_dirty /       { nr_dirty=$2 }
            /^nr_writeback /   { nr_writeback=$2 }
            /^pgpgout /        { pgpgout=$2 }
            END { printf "%d,%d,%d,%d,%d,%d,%d,%d\n",
                  systime(), pgscand, pgsteal_file, pgsteal_anon,
                  pgscan_kswapd, nr_dirty, nr_writeback, pgpgout }
        ' /proc/vmstat)
        echo "$row" >> "$POLL_FILE"
        sleep 1
    done
}

poll_vmstat &
POLL_PID=$!
info "  vmstat 1s poller pid=$POLL_PID"

# ════════════════════════════════════════════════════════════
# 6. WAIT
# ════════════════════════════════════════════════════════════
echo ""
info "Collecting for ${DURATION}s — Ctrl-C stops early but still produces a report..."
sleep "$DURATION" || true
kill "$POLL_PID" 2>/dev/null || true
wait "${EBPF_PIDS[@]}" 2>/dev/null || true
grep -E "^($vmstat_keys) " /proc/vmstat > "$VMSTAT_AFTER"

# ════════════════════════════════════════════════════════════
# 7. ANALYSIS
# ════════════════════════════════════════════════════════════
title "7. Analysing results"

vmstat_delta() {
    local key=$1
    local b a
    b=$(awk -v k="$key" '$0 ~ "^"k" " {print $2}' "$VMSTAT_BEFORE")
    a=$(awk -v k="$key" '$0 ~ "^"k" " {print $2}' "$VMSTAT_AFTER")
    echo $(( ${a:-0} - ${b:-0} ))
}

PGSCAND=$(vmstat_delta pgscand)
PGSTEAL_FILE=$(vmstat_delta pgsteal_file)
PGSTEAL_ANON=$(vmstat_delta pgsteal_anon)
PGSCAN_KSWAPD=$(vmstat_delta pgscan_kswapd)
PGPGOUT=$(vmstat_delta pgpgout)
PGSCAND_RATE=$(( PGSCAND / DURATION ))

info "vmstat deltas over ${DURATION}s:"
printf "  %-30s %d  (%d/s)\n" "pgscand (direct reclaim):" "$PGSCAND" "$PGSCAND_RATE"
printf "  %-30s %d\n" "pgsteal_file (cache stolen):" "$PGSTEAL_FILE"
printf "  %-30s %d\n" "pgsteal_anon (anon stolen):"  "$PGSTEAL_ANON"
printf "  %-30s %d\n" "pgscan_kswapd:"                "$PGSCAN_KSWAPD"
printf "  %-30s %d\n" "pgpgout (pages written out):"  "$PGPGOUT"

# direct reclaim severity
if (( PGSCAND > 0 )); then
    if (( PGSCAND_RATE > 1000 )); then
        add_finding "direct_reclaim" "CRIT" \
            "Heavy direct reclaim at ${PGSCAND_RATE} pages/s — business threads blocked in kernel"
    else
        add_finding "direct_reclaim" "WARN" \
            "Direct reclaim at ${PGSCAND_RATE} pages/s — intermittent stalls"
    fi
fi

# dirty writeback driving reclaim
if (( PGSTEAL_FILE > 0 && PGPGOUT > 100000 )); then
    add_finding "writeback_pressure" "WARN" \
        "${PGPGOUT} pages written out — dirty writeback adding latency to reclaim"
fi

# eBPF event analysis
DR_EVENTS=$(grep -c "^DIRECT_RECLAIM"    "$OUTPUT_DIR/ebpf_direct_reclaim.txt"  2>/dev/null || echo 0)
WB_EVENTS=$(grep -c "^WRITEBACK"         "$OUTPUT_DIR/ebpf_writeback.txt"        2>/dev/null || echo 0)
MA_EVENTS=$(grep -c "^MADVISE_DONTNEED"  "$OUTPUT_DIR/ebpf_madvise.txt"          2>/dev/null || echo 0)
SR_EVENTS=$(grep -c "^SR_RECLAIM_BEGIN"  "$OUTPUT_DIR/ebpf_starrocks_be.txt"     2>/dev/null || echo 0)

info ""
info "eBPF events >= ${THRESHOLD_MS}ms:"
printf "  %-35s %d\n" "system direct_reclaim events:"   "$DR_EVENTS"
printf "  %-35s %d\n" "writeback stall events:"          "$WB_EVENTS"
printf "  %-35s %d\n" "madvise DONTNEED stall events:"   "$MA_EVENTS"
printf "  %-35s %d\n" "starrocks_be reclaim events:"     "$SR_EVENTS"

if (( SR_EVENTS > 0 )); then
    SR_MAX_MS=$(grep "^SR_RECLAIM_END" "$OUTPUT_DIR/ebpf_starrocks_be.txt" 2>/dev/null \
              | grep -oP 'blocked_ms=\K[0-9]+' | sort -n | tail -1 || echo 0)
    add_finding "sr_direct_reclaim" "CRIT" \
        "starrocks_be triggered direct reclaim $SR_EVENTS time(s), max stall ${SR_MAX_MS}ms — see ebpf_starrocks_be.txt for call stacks"
fi

if (( MA_EVENTS > 0 )); then
    MA_MAX_MS=$(grep "^MADVISE_DONTNEED" "$OUTPUT_DIR/ebpf_madvise.txt" 2>/dev/null \
              | grep -oP 'blocked_ms=\K[0-9]+' | sort -n | tail -1 || echo 0)
    add_finding "madvise_stall" "WARN" \
        "madvise(MADV_DONTNEED) stalled $MA_EVENTS time(s), max ${MA_MAX_MS}ms — likely THP split overhead"
fi

# spike detection from 1s time-series
SPIKE_COUNT=0
PREV_PGSCAND=0
while IFS=, read -r ts pgscand _rest; do
    [[ "$ts" == "ts" ]] && { PREV_PGSCAND=$pgscand; continue; }
    DELTA=$(( pgscand - PREV_PGSCAND ))
    if (( DELTA > 5000 )); then
        (( SPIKE_COUNT++ )) || true
        info "  reclaim spike at $(date -d "@$ts" '+%H:%M:%S' 2>/dev/null || echo "$ts"): +${DELTA} pages in 1s"
    fi
    PREV_PGSCAND=$pgscand
done < "$POLL_FILE"
if (( SPIKE_COUNT > 0 )); then
    add_finding "reclaim_spikes" "CRIT" \
        "$SPIKE_COUNT direct reclaim spike(s) >5000 pages/s — sudden memory pressure bursts"
fi

# ════════════════════════════════════════════════════════════
# 8. REPORT
# ════════════════════════════════════════════════════════════
title "8. Report"

{
printf "StarRocks Page Cache Stall Diagnostic Report\n"
printf "Generated : %s\n"   "$(date)"
printf "Host      : %s\n"   "$(hostname)"
printf "Kernel    : %s\n"   "$KVER"
printf "Duration  : %ds\n"  "$DURATION"
printf "Threshold : %dms\n" "$THRESHOLD_MS"
printf "BE PID    : %s\n"   "${BE_PID:-N/A}"

printf "\n══════════════════════════════════════════════\n"
printf " FINDINGS (%d)\n" "$ISSUE_COUNT"
printf "══════════════════════════════════════════════\n"

if (( ISSUE_COUNT == 0 )); then
    printf "  No significant page cache stall indicators in this window.\n"
else
    for key in "${!FINDINGS[@]}"; do
        case "${SEVERITY[$key]}" in
            CRIT) icon="[CRITICAL]" ;;
            WARN) icon="[WARNING] " ;;
            *)    icon="[INFO]    " ;;
        esac
        printf "\n%s %s\n" "$icon" "${FINDINGS[$key]}"
    done
fi

printf "\n══════════════════════════════════════════════\n"
printf " RECOMMENDATIONS\n"
printf "══════════════════════════════════════════════\n"

[[ -v FINDINGS[thp] || -v FINDINGS[madvise_stall] ]] && printf "
[THP] Disable compaction to prevent madvise stalls:
  echo madvise      > /sys/kernel/mm/transparent_hugepage/enabled
  echo defer+madvise > /sys/kernel/mm/transparent_hugepage/defrag
  # Persist: add to /etc/rc.local or a tuned profile
"

[[ -v FINDINGS[dirty_ratio] ]] && printf "
[Dirty writeback] Flush dirty pages earlier to avoid bulk stalls:
  sysctl -w vm.dirty_ratio=5
  sysctl -w vm.dirty_background_ratio=2
  sysctl -w vm.dirty_expire_centisecs=1000
  # Persist: /etc/sysctl.d/99-starrocks.conf
"

[[ -v FINDINGS[swappiness] ]] && printf "
[Swappiness] Prefer reclaiming page cache over swapping anon memory:
  sysctl -w vm.swappiness=1
"

[[ -v FINDINGS[drop_caches] ]] && printf "
[drop_caches] Remove the cron job — it triggers burst reclaim on every run.
"

[[ -v FINDINGS[sr_direct_reclaim] || -v FINDINGS[low_mem] ]] && printf "
[StarRocks memory] Leave headroom for OS page cache:
  # be.conf — keep BE below 70-75%% of physical RAM
  mem_limit = 70%%

  # Smooth jemalloc memory return to OS (avoid burst madvise)
  JEMALLOC_CONF=\"dirty_decay_ms:5000,muzzy_decay_ms:10000,background_thread:true\"

  # Review which BE operation triggered reclaim:
  grep SR_RECLAIM_BEGIN %s/ebpf_starrocks_be.txt
" "$OUTPUT_DIR"

[[ -v FINDINGS[cgroup_mem] ]] && printf "
[cgroup] StarRocks is near its memory limit — raise limit or lower mem_limit in be.conf.
"

printf "\n══════════════════════════════════════════════\n"
printf " OUTPUT FILES\n"
printf "══════════════════════════════════════════════\n"
printf "  %s/\n" "$OUTPUT_DIR"
for f in "$OUTPUT_DIR"/*.txt "$OUTPUT_DIR"/*.csv; do
    [[ -f "$f" ]] && printf "    %-42s %d lines\n" "$(basename "$f")" "$(wc -l < "$f")"
done

} | tee "$REPORT"

echo ""
ok "Report : $REPORT"
ok "Data   : $OUTPUT_DIR/"
