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
#   1. Direct reclaim  - business thread blocked doing page reclaim
#   2. Writeback stall - dirty pages waiting for disk IO
#   3. kswapd pressure - background reclaim unable to keep up
#   4. drop_caches     - manual cache drop causing burst reclaim
#   5. cgroup limit    - memory cgroup throttling StarRocks
#
# Usage:
#   sudo ./page_cache_stall_detect.sh [OPTIONS]
#
# Options:
#   -d SECONDS   Collection duration (default: 60)
#   -t MS        Stall threshold in milliseconds (default: 10)
#   -p PID       StarRocks BE pid (auto-detected if omitted)
#   -o DIR       Output directory (default: /tmp/sr_page_stall_<timestamp>)
#   -s           Skip eBPF probes, use /proc polling only
#   -h           Show this help
#
# Requirements:
#   - Linux kernel >= 4.14
#   - bpftrace >= 0.12 (optional, falls back to /proc polling)
#   - root or CAP_BPF + CAP_PERFMON
# ============================================================

set -euo pipefail

# ── defaults ────────────────────────────────────────────────
DURATION=60
THRESHOLD_MS=10
BE_PID=""
OUTPUT_DIR=""
SKIP_EBPF=false
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# ── colours ─────────────────────────────────────────────────
RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
title() { echo -e "\n${BOLD}$*${NC}"; echo "$(echo "$*" | sed 's/./─/g')"; }

# ── usage ────────────────────────────────────────────────────
usage() {
    sed -n '/^# Usage/,/^# Requirements/p' "$0" | grep -v '^# ====' | sed 's/^# //'
    exit 0
}

# ── argument parsing ─────────────────────────────────────────
while getopts "d:t:p:o:sh" opt; do
    case $opt in
        d) DURATION=$OPTARG ;;
        t) THRESHOLD_MS=$OPTARG ;;
        p) BE_PID=$OPTARG ;;
        o) OUTPUT_DIR=$OPTARG ;;
        s) SKIP_EBPF=true ;;
        h) usage ;;
        *) usage ;;
    esac
done

[[ -z "$OUTPUT_DIR" ]] && OUTPUT_DIR="/tmp/sr_page_stall_${TIMESTAMP}"
mkdir -p "$OUTPUT_DIR"

LOG="$OUTPUT_DIR/detect.log"
REPORT="$OUTPUT_DIR/report.txt"
exec > >(tee -a "$LOG") 2>&1

# ── counters for final report ────────────────────────────────
ISSUE_COUNT=0
declare -A FINDINGS   # key -> description
declare -A SEVERITY   # key -> INFO / WARN / CRIT

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

# root check
if [[ $EUID -ne 0 ]]; then
    error "This script must be run as root (or with sudo)."
    exit 1
fi

# kernel version
KVER=$(uname -r)
KMAJ=$(echo "$KVER" | cut -d. -f1)
KMIN=$(echo "$KVER" | cut -d. -f2)
if (( KMAJ < 4 || (KMAJ == 4 && KMIN < 14) )); then
    error "Kernel $KVER is too old. Requires >= 4.14."
    exit 1
fi
ok "Kernel $KVER"

# bpftrace availability
HAVE_BPFTRACE=false
if ! $SKIP_EBPF && command -v bpftrace &>/dev/null; then
    BT_VER=$(bpftrace --version 2>&1 | grep -oE '[0-9]+\.[0-9]+' | head -1)
    ok "bpftrace $BT_VER found — eBPF probes enabled"
    HAVE_BPFTRACE=true
else
    if $SKIP_EBPF; then
        warn "eBPF skipped by -s flag — using /proc polling only"
    else
        warn "bpftrace not found — falling back to /proc polling"
        warn "  Install: apt install bpftrace  OR  yum install bpftrace"
    fi
fi

# find starrocks_be pid
if [[ -z "$BE_PID" ]]; then
    BE_PID=$(pgrep -f "starrocks_be" | head -1 || true)
fi
if [[ -z "$BE_PID" ]]; then
    warn "starrocks_be process not found — some per-process checks skipped"
else
    ok "starrocks_be pid=$BE_PID"
fi

info "Output dir : $OUTPUT_DIR"
info "Duration   : ${DURATION}s"
info "Threshold  : ${THRESHOLD_MS}ms"
echo ""

# ════════════════════════════════════════════════════════════
# 2. STATIC CHECKS (instant, no waiting)
# ════════════════════════════════════════════════════════════
title "2. Static system checks"

# 2-a. Transparent Huge Pages
THP=$(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || echo "unknown")
THP_MODE=$(echo "$THP" | grep -oP '\[\K[^\]]+' || echo "$THP")
if [[ "$THP_MODE" == "always" ]]; then
    warn "THP=always: compaction can block madvise for hundreds of ms"
    add_finding "thp" "WARN" "THP=always — set to madvise or never to reduce madvise latency"
else
    ok "THP=$THP_MODE"
fi

# 2-b. dirty ratio
DIRTY_RATIO=$(cat /proc/sys/vm/dirty_ratio)
DIRTY_BG=$(cat /proc/sys/vm/dirty_background_ratio)
if (( DIRTY_RATIO > 15 )); then
    warn "vm.dirty_ratio=$DIRTY_RATIO (high — dirty pages accumulate before writeback)"
    add_finding "dirty_ratio" "WARN" "vm.dirty_ratio=$DIRTY_RATIO > 15 — bulk writeback can block reclaim"
else
    ok "vm.dirty_ratio=$DIRTY_RATIO  vm.dirty_background_ratio=$DIRTY_BG"
fi

# 2-c. swappiness
SWAPPINESS=$(cat /proc/sys/vm/swappiness)
if (( SWAPPINESS > 10 )); then
    warn "vm.swappiness=$SWAPPINESS — OS may swap StarRocks memory instead of dropping cache"
    add_finding "swappiness" "WARN" "vm.swappiness=$SWAPPINESS — recommend 1 for database workloads"
else
    ok "vm.swappiness=$SWAPPINESS"
fi

# 2-d. drop_caches cronjob
DROP_CACHES_JOBS=$(grep -r "drop_caches" /etc/cron* /var/spool/cron/ 2>/dev/null || true)
if [[ -n "$DROP_CACHES_JOBS" ]]; then
    warn "drop_caches cron job found!"
    echo "$DROP_CACHES_JOBS"
    add_finding "drop_caches" "CRIT" "Cron job calling drop_caches — causes burst direct reclaim: $DROP_CACHES_JOBS"
else
    ok "No drop_caches cron jobs found"
fi

# 2-e. memory overview
MEMINFO=$(cat /proc/meminfo)
MEM_TOTAL=$(echo "$MEMINFO" | awk '/MemTotal/    {print $2}')
MEM_AVAIL=$(echo "$MEMINFO" | awk '/MemAvailable/{print $2}')
CACHE_KB=$(echo "$MEMINFO"  | awk '/^Cached:/    {print $2}')
BUFFERS_KB=$(echo "$MEMINFO"| awk '/^Buffers:/   {print $2}')
AVAIL_PCT=$(( MEM_AVAIL * 100 / MEM_TOTAL ))
info "Memory: total=$(( MEM_TOTAL/1024 ))MB  available=$(( MEM_AVAIL/1024 ))MB (${AVAIL_PCT}%)  page_cache=$(( (CACHE_KB+BUFFERS_KB)/1024 ))MB"
if (( AVAIL_PCT < 10 )); then
    add_finding "low_mem" "CRIT" "Available memory only ${AVAIL_PCT}% — high direct reclaim risk"
    warn "Available memory < 10% — expect heavy direct reclaim"
fi

# 2-f. cgroup memory limit for starrocks_be
if [[ -n "$BE_PID" ]]; then
    CGROUP_PATH=$(cat /proc/"$BE_PID"/cgroup 2>/dev/null | grep memory | head -1 | cut -d: -f3)
    if [[ -n "$CGROUP_PATH" ]]; then
        # try cgroup v2 first, then v1
        CGROUP_LIMIT=$(cat "/sys/fs/cgroup${CGROUP_PATH}/memory.max" 2>/dev/null \
                    || cat "/sys/fs/cgroup/memory${CGROUP_PATH}/memory.limit_in_bytes" 2>/dev/null \
                    || echo "")
        if [[ "$CGROUP_LIMIT" =~ ^[0-9]+$ ]] && (( CGROUP_LIMIT < 9223372036854775807 )); then
            LIMIT_MB=$(( CGROUP_LIMIT / 1024 / 1024 ))
            info "cgroup memory limit: ${LIMIT_MB}MB"
            CGROUP_USAGE=$(cat "/sys/fs/cgroup${CGROUP_PATH}/memory.current" 2>/dev/null \
                        || cat "/sys/fs/cgroup/memory${CGROUP_PATH}/memory.usage_in_bytes" 2>/dev/null \
                        || echo "0")
            USAGE_MB=$(( CGROUP_USAGE / 1024 / 1024 ))
            USAGE_PCT=$(( USAGE_MB * 100 / LIMIT_MB ))
            info "cgroup memory usage: ${USAGE_MB}MB / ${LIMIT_MB}MB (${USAGE_PCT}%)"
            if (( USAGE_PCT > 85 )); then
                add_finding "cgroup_mem" "CRIT" "cgroup memory usage ${USAGE_PCT}% of limit — kernel will reclaim within cgroup"
                warn "cgroup memory at ${USAGE_PCT}% — StarRocks may trigger in-cgroup reclaim"
            fi
        else
            ok "No cgroup memory limit"
        fi
    fi
fi

# ════════════════════════════════════════════════════════════
# 3. /proc BASELINE
# ════════════════════════════════════════════════════════════
title "3. Capturing /proc baseline"

snapshot_vmstat() {
    awk '/^pgscand|^pgsteal|^pgscan_kswapd|^pgpgout|^nr_dirty|^nr_writeback/{print}' /proc/vmstat
}

VMSTAT_BEFORE="$OUTPUT_DIR/vmstat_before.txt"
VMSTAT_AFTER="$OUTPUT_DIR/vmstat_after.txt"
snapshot_vmstat > "$VMSTAT_BEFORE"
ok "Baseline captured"

# ════════════════════════════════════════════════════════════
# 4. eBPF PROBES
# ════════════════════════════════════════════════════════════
title "4. Running eBPF probes (${DURATION}s)"

EBPF_PIDS=()

# ── 4-a. Direct reclaim stall probe ──────────────────────────
run_direct_reclaim_probe() {
    local out="$OUTPUT_DIR/ebpf_direct_reclaim.txt"
    bpftrace -e "
tracepoint:vmscan:mm_vmscan_direct_reclaim_begin {
    @dr_start[tid]  = nsecs;
    @dr_comm[tid]   = comm;
    @dr_pid[tid]    = pid;
}
tracepoint:vmscan:mm_vmscan_direct_reclaim_end {
    if (@dr_start[tid]) {
        \$ms = (nsecs - @dr_start[tid]) / 1000000;
        if (\$ms >= $THRESHOLD_MS) {
            printf(\"DIRECT_RECLAIM comm=%-16s pid=%-6d tid=%-6d reclaimed=%-6d pages  blocked=%lld ms\n\",
                   @dr_comm[tid], @dr_pid[tid], tid, args->nr_reclaimed, \$ms);
        }
        delete(@dr_start[tid]);
        delete(@dr_comm[tid]);
        delete(@dr_pid[tid]);
    }
}
" --timeout "$DURATION" > "$out" 2>&1 &
    EBPF_PIDS+=($!)
    info "  direct_reclaim probe started (pid $!)"
}

# ── 4-b. Writeback stall probe ───────────────────────────────
run_writeback_probe() {
    local out="$OUTPUT_DIR/ebpf_writeback.txt"
    bpftrace -e "
tracepoint:writeback:writeback_start {
    @wb_start[args->sb_dev]  = nsecs;
    @wb_reason[args->sb_dev] = args->reason;
}
tracepoint:writeback:writeback_written {
    if (@wb_start[args->sb_dev]) {
        \$ms = (nsecs - @wb_start[args->sb_dev]) / 1000000;
        if (\$ms >= $THRESHOLD_MS) {
            printf(\"WRITEBACK dev=%d reason=%-2d pages=%-6d blocked=%lld ms\n\",
                   args->sb_dev, @wb_reason[args->sb_dev],
                   args->nr_pages, \$ms);
        }
        delete(@wb_start[args->sb_dev]);
    }
}
" --timeout "$DURATION" > "$out" 2>&1 &
    EBPF_PIDS+=($!)
    info "  writeback probe started (pid $!)"
}

# ── 4-c. madvise MADV_DONTNEED stall probe ───────────────────
run_madvise_probe() {
    local out="$OUTPUT_DIR/ebpf_madvise.txt"
    # MADV_DONTNEED = 4
    bpftrace -e "
tracepoint:syscalls:sys_enter_madvise
/ args->behavior == 4 /
{
    @ma_start[tid] = nsecs;
    @ma_len[tid]   = args->len_in;
    @ma_comm[tid]  = comm;
    @ma_pid[tid]   = pid;
}
tracepoint:syscalls:sys_exit_madvise {
    if (@ma_start[tid]) {
        \$ms = (nsecs - @ma_start[tid]) / 1000000;
        if (\$ms >= $THRESHOLD_MS) {
            printf(\"MADVISE_DONTNEED comm=%-16s pid=%-6d len_mb=%-5lld blocked=%lld ms\n\",
                   @ma_comm[tid], @ma_pid[tid],
                   @ma_len[tid] / 1024 / 1024, \$ms);
        }
        delete(@ma_start[tid]);
        delete(@ma_len[tid]);
        delete(@ma_comm[tid]);
        delete(@ma_pid[tid]);
    }
}
" --timeout "$DURATION" > "$out" 2>&1 &
    EBPF_PIDS+=($!)
    info "  madvise probe started (pid $!)"
}

# ── 4-d. kswapd wakeup frequency probe ──────────────────────
run_kswapd_probe() {
    local out="$OUTPUT_DIR/ebpf_kswapd.txt"
    bpftrace -e "
tracepoint:vmscan:mm_vmscan_kswapd_wake {
    @kswapd_wakeups = count();
}
tracepoint:vmscan:mm_vmscan_kswapd_sleep {
    @kswapd_sleeps = count();
}
interval:s:10 {
    printf(\"KSWAPD_STATS wakeups=%lld sleeps=%lld\n\",
           @kswapd_wakeups, @kswapd_sleeps);
    clear(@kswapd_wakeups); clear(@kswapd_sleeps);
}
" --timeout "$DURATION" > "$out" 2>&1 &
    EBPF_PIDS+=($!)
    info "  kswapd probe started (pid $!)"
}

# ── 4-e. Per-process reclaim attribution (if BE running) ─────
run_per_process_probe() {
    [[ -z "$BE_PID" ]] && return
    local out="$OUTPUT_DIR/ebpf_per_process.txt"
    bpftrace -e "
tracepoint:vmscan:mm_vmscan_direct_reclaim_begin
/ pid == $BE_PID /
{
    @sr_reclaim_start[tid] = nsecs;
    printf(\"SR_RECLAIM_BEGIN tid=%d\n\", tid);
    print(ustack);
}
tracepoint:vmscan:mm_vmscan_direct_reclaim_end
/ @sr_reclaim_start[tid] /
{
    \$ms = (nsecs - @sr_reclaim_start[tid]) / 1000000;
    printf(\"SR_RECLAIM_END   tid=%d reclaimed=%d pages  blocked=%lld ms\n\",
           tid, args->nr_reclaimed, \$ms);
    delete(@sr_reclaim_start[tid]);
}
" --timeout "$DURATION" > "$out" 2>&1 &
    EBPF_PIDS+=($!)
    info "  per-process (BE pid=$BE_PID) probe started (pid $!)"
}

cleanup_ebpf() {
    for p in "${EBPF_PIDS[@]:-}"; do
        kill "$p" 2>/dev/null || true
    done
}
trap cleanup_ebpf EXIT

if $HAVE_BPFTRACE; then
    run_direct_reclaim_probe
    run_writeback_probe
    run_madvise_probe
    run_kswapd_probe
    run_per_process_probe
else
    info "  (eBPF probes skipped)"
fi

# ════════════════════════════════════════════════════════════
# 5. /proc POLLING (runs in parallel with eBPF)
# ════════════════════════════════════════════════════════════
title "5. Polling /proc/vmstat every 5s"

POLL_FILE="$OUTPUT_DIR/vmstat_poll.csv"
echo "timestamp,pgscand,pgsteal_anon,pgsteal_file,pgscan_kswapd,nr_dirty,nr_writeback,pgpgout" > "$POLL_FILE"

poll_vmstat() {
    local deadline=$(( $(date +%s) + DURATION ))
    while (( $(date +%s) < deadline )); do
        local ts pgscand pgsteal_anon pgsteal_file pgscan_kswapd nr_dirty nr_writeback pgpgout
        ts=$(date +%s)
        read -r pgscand     <<< "$(awk '/^pgscand /        {print $2}' /proc/vmstat)"
        read -r pgsteal_anon <<< "$(awk '/^pgsteal_anon/   {print $2}' /proc/vmstat)"
        read -r pgsteal_file <<< "$(awk '/^pgsteal_file/   {print $2}' /proc/vmstat)"
        read -r pgscan_kswapd <<< "$(awk '/^pgscan_kswapd/ {print $2}' /proc/vmstat)"
        read -r nr_dirty    <<< "$(awk '/^nr_dirty /       {print $2}' /proc/vmstat)"
        read -r nr_writeback <<< "$(awk '/^nr_writeback /  {print $2}' /proc/vmstat)"
        read -r pgpgout     <<< "$(awk '/^pgpgout /        {print $2}' /proc/vmstat)"
        echo "$ts,${pgscand:-0},${pgsteal_anon:-0},${pgsteal_file:-0},${pgscan_kswapd:-0},${nr_dirty:-0},${nr_writeback:-0},${pgpgout:-0}" >> "$POLL_FILE"
        sleep 5
    done
}

poll_vmstat &
POLL_PID=$!
info "  vmstat polling started (pid $POLL_PID)"

# ════════════════════════════════════════════════════════════
# 6. WAIT
# ════════════════════════════════════════════════════════════
echo ""
info "Collecting for ${DURATION}s — press Ctrl-C to stop early and still get a report..."
sleep "$DURATION" || true
kill "$POLL_PID" 2>/dev/null || true
wait "${EBPF_PIDS[@]:-}" 2>/dev/null || true
snapshot_vmstat > "$VMSTAT_AFTER"

# ════════════════════════════════════════════════════════════
# 7. ANALYSIS
# ════════════════════════════════════════════════════════════
title "7. Analysing collected data"

# ── helper: delta between before/after vmstat ────────────────
vmstat_delta() {
    local key=$1
    local before after
    before=$(awk -v k="$key" '$0 ~ "^"k" " {print $2}' "$VMSTAT_BEFORE")
    after=$(awk  -v k="$key" '$0 ~ "^"k" " {print $2}' "$VMSTAT_AFTER")
    echo $(( ${after:-0} - ${before:-0} ))
}

PGSCAND=$(vmstat_delta pgscand)              # pages scanned by direct reclaim
PGSTEAL_ANON=$(vmstat_delta pgsteal_anon)
PGSTEAL_FILE=$(vmstat_delta pgsteal_file)   # page cache pages stolen
PGSCAN_KSWAPD=$(vmstat_delta pgscan_kswapd)
NR_DIRTY_DELTA=$(vmstat_delta nr_dirty)
PGPGOUT=$(vmstat_delta pgpgout)

PGSTEAL_TOTAL=$(( PGSTEAL_ANON + PGSTEAL_FILE ))
PGSCAND_RATE=$(( PGSCAND / DURATION ))      # pages/sec scanned in direct reclaim

info "vmstat deltas over ${DURATION}s:"
info "  pgscand (direct reclaim scan): $PGSCAND  (${PGSCAND_RATE}/s)"
info "  pgsteal_file (cache evicted) : $PGSTEAL_FILE"
info "  pgsteal_anon (anon evicted)  : $PGSTEAL_ANON"
info "  pgscan_kswapd                : $PGSCAN_KSWAPD"
info "  pgpgout (pages written out)  : $PGPGOUT"

# direct reclaim active?
if (( PGSCAND > 0 )); then
    if (( PGSCAND_RATE > 1000 )); then
        add_finding "direct_reclaim_rate" "CRIT" \
            "Heavy direct reclaim: ${PGSCAND_RATE} pages/s scanned — business threads blocked in kernel"
    else
        add_finding "direct_reclaim_low" "WARN" \
            "Direct reclaim active at ${PGSCAND_RATE} pages/s — intermittent stalls likely"
    fi
fi

# writeback driven reclaim?
if (( PGSTEAL_FILE > 0 && PGPGOUT > 100000 )); then
    add_finding "writeback_pressure" "WARN" \
        "High writeback: ${PGPGOUT} pages written — dirty page reclaim adding latency"
fi

# ── eBPF event counts ────────────────────────────────────────
if $HAVE_BPFTRACE; then
    DR_EVENTS=$(grep -c "^DIRECT_RECLAIM" "$OUTPUT_DIR/ebpf_direct_reclaim.txt" 2>/dev/null || echo 0)
    WB_EVENTS=$(grep -c "^WRITEBACK"      "$OUTPUT_DIR/ebpf_writeback.txt"      2>/dev/null || echo 0)
    MA_EVENTS=$(grep -c "^MADVISE"        "$OUTPUT_DIR/ebpf_madvise.txt"        2>/dev/null || echo 0)
    SR_EVENTS=$(grep -c "^SR_RECLAIM_BEGIN" "$OUTPUT_DIR/ebpf_per_process.txt"  2>/dev/null || echo 0)

    info ""
    info "eBPF event counts (threshold >= ${THRESHOLD_MS}ms):"
    info "  system direct_reclaim events : $DR_EVENTS"
    info "  writeback stall events       : $WB_EVENTS"
    info "  madvise DONTNEED stall events: $MA_EVENTS"
    info "  starrocks_be reclaim events  : $SR_EVENTS"

    if (( SR_EVENTS > 0 )); then
        DR_MAX_MS=$(grep "^SR_RECLAIM_END" "$OUTPUT_DIR/ebpf_per_process.txt" 2>/dev/null \
                  | grep -oP 'blocked=\K[0-9]+' | sort -n | tail -1 || echo 0)
        add_finding "sr_direct_reclaim" "CRIT" \
            "starrocks_be triggered direct reclaim $SR_EVENTS times (max stall: ${DR_MAX_MS}ms) — queries/load blocked in kernel"
    fi

    if (( MA_EVENTS > 0 )); then
        MA_MAX_MS=$(grep "^MADVISE" "$OUTPUT_DIR/ebpf_madvise.txt" 2>/dev/null \
                  | grep -oP 'blocked=\K[0-9]+' | sort -n | tail -1 || echo 0)
        add_finding "madvise_stall" "WARN" \
            "madvise(MADV_DONTNEED) stalled $MA_EVENTS times (max: ${MA_MAX_MS}ms) — jemalloc memory return blocked"
    fi
fi

# ── check poll CSV for spikes ────────────────────────────────
# A spike = pgscand jumps > 10000 in one 5s window
if [[ -f "$POLL_FILE" ]]; then
    SPIKE_COUNT=0
    PREV_PGSCAND=0
    while IFS=, read -r ts pgscand _rest; do
        [[ "$ts" == "timestamp" ]] && { PREV_PGSCAND=$pgscand; continue; }
        DELTA=$(( pgscand - PREV_PGSCAND ))
        if (( DELTA > 10000 )); then
            (( SPIKE_COUNT++ )) || true
            info "  reclaim spike at $(date -d @"$ts" '+%H:%M:%S'): +${DELTA} pages in 5s"
        fi
        PREV_PGSCAND=$pgscand
    done < "$POLL_FILE"
    if (( SPIKE_COUNT > 0 )); then
        add_finding "reclaim_spikes" "CRIT" \
            "$SPIKE_COUNT direct reclaim spike(s) detected — sudden cache pressure events"
    fi
fi

# ════════════════════════════════════════════════════════════
# 8. REPORT
# ════════════════════════════════════════════════════════════
title "8. Generating report"

{
printf "StarRocks Page Cache Stall Diagnostic Report\n"
printf "Generated : %s\n" "$(date)"
printf "Host      : %s\n" "$(hostname)"
printf "Kernel    : %s\n" "$KVER"
printf "Duration  : %ds\n" "$DURATION"
printf "BE PID    : %s\n\n" "${BE_PID:-N/A}"

printf "══════════════════════════════════════════════\n"
printf " FINDINGS (%d)\n" "$ISSUE_COUNT"
printf "══════════════════════════════════════════════\n"

if (( ISSUE_COUNT == 0 )); then
    printf "  No significant page cache stall indicators found\n"
    printf "  during this collection window.\n"
else
    for key in "${!FINDINGS[@]}"; do
        sev="${SEVERITY[$key]}"
        case $sev in
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

# recommendations keyed on findings
if [[ -v FINDINGS[thp] ]]; then
printf "
[THP compaction]
  echo madvise > /sys/kernel/mm/transparent_hugepage/enabled
  echo defer+madvise > /sys/kernel/mm/transparent_hugepage/defrag
  # Persist in /etc/rc.local or tuned profile
"
fi

if [[ -v FINDINGS[dirty_ratio] ]]; then
printf "
[Dirty page writeback]
  sysctl -w vm.dirty_ratio=5
  sysctl -w vm.dirty_background_ratio=2
  sysctl -w vm.dirty_expire_centisecs=1000
  # Persist in /etc/sysctl.d/99-starrocks.conf
"
fi

if [[ -v FINDINGS[swappiness] ]]; then
printf "
[Swappiness]
  sysctl -w vm.swappiness=1
"
fi

if [[ -v FINDINGS[drop_caches] ]]; then
printf "
[drop_caches cron job]
  Remove or disable the cron job that calls drop_caches.
  It causes burst direct reclaim every time it runs.
"
fi

if [[ -v FINDINGS[sr_direct_reclaim] || -v FINDINGS[low_mem] ]]; then
printf "
[StarRocks memory sizing]
  Ensure StarRocks BE mem_limit leaves room for OS page cache.
  Recommended: mem_limit = 70-75%% of physical RAM.
  In be.conf:
    mem_limit = 70%%
  Also enable jemalloc background purge for smoother release:
    JEMALLOC_CONF=\"dirty_decay_ms:5000,muzzy_decay_ms:10000,background_thread:true\"
"
fi

if [[ -v FINDINGS[cgroup_mem] ]]; then
printf "
[cgroup memory limit]
  StarRocks is near its cgroup memory limit.
  Either raise the limit or reduce mem_limit in be.conf.
"
fi

if [[ -v FINDINGS[madvise_stall] ]]; then
printf "
[madvise / jemalloc purge stall]
  THP=always forces the kernel to split huge pages during madvise.
  Disabling THP or setting it to madvise resolves most cases.
  Also tune jemalloc decay:
    JEMALLOC_CONF=\"dirty_decay_ms:5000,muzzy_decay_ms:10000\"
"
fi

printf "\n══════════════════════════════════════════════\n"
printf " RAW DATA FILES\n"
printf "══════════════════════════════════════════════\n"
printf "  %s/\n" "$OUTPUT_DIR"
for f in "$OUTPUT_DIR"/*.txt "$OUTPUT_DIR"/*.csv; do
    [[ -f "$f" ]] && printf "    %-40s %s\n" "$(basename "$f")" "$(wc -l < "$f") lines"
done

} | tee "$REPORT"

echo ""
ok "Report saved to $REPORT"
ok "All raw data in  $OUTPUT_DIR/"
echo ""
