#!/bin/bash
#
# Reproducer: Tablet::overwrite_rowset() destroys rowsets that extend past the
# overwrite version.
#
# ALTER TABLE ... DISTRIBUTED BY runs OnlineOptimizeJobV2, which rewrites the
# partition with an INSERT OVERWRITE whose published version V is pinned to the
# version its SELECT scanned (StmtExecutor -> InsertTxnCommitAttachment ->
# DatabaseTransactionMgr, which also skips the usual version continuity check).
# Loads running during the job are double-written into the temporary partition
# carrying the source partition's version numbers, so the temporary partition
# ends up holding versions both below and above V, and compaction can merge
# across V.
#
# Tablet::overwrite_rowset() then picks the rowsets to replace with
# _pick_candicate_rowset_before_specify_version(), i.e. by start_version <= V,
# but only puts back a rowset covering [0, V]. A rowset with
# start <= V < end is deleted whole and everything after V is lost.
#
# The version graph keeps the deleted rowset's edges until the stale sweep runs
# and VersionGraph::_max_continuous_version never moves backward, so the tracker
# keeps reporting the old max while the tablet meta no longer has it:
#
#   Check failed: v == _max_continuous_version_from_beginning_unlocked().second
#       starrocks::Tablet::max_continuous_version() const
#       starrocks::run_publish_version_task(...)
#
# On a RELEASE backend nothing aborts: the rows loaded during the optimize
# window are silently gone and the backend keeps reporting the stale max to the
# frontend.
#
# REQUIREMENTS
#   * a single-node cluster whose BE was built with -DENABLE_FAULT_INJECTION=ON
#     (the publish_overwrite_txn_stall failpoint lives in txn_manager.cpp)
#   * this script must run somewhere that can read the BE's log directory
#   * mysql client, curl, python3
#
# THIS KILLS THE BACKEND on a build with DCHECKs enabled. That is the point.
#
# Usage: repro_overwrite_drops_versions.sh [fe_host] [fe_port]

set -u
FE=${1:-127.0.0.1}
FEP=${2:-9030}
DB=ovw_repro_$$

Q() { mysql -h"$FE" -P"$FEP" -uroot -N -B -e "$1" 2>&1; }
step() { echo; echo "=== $* ==="; }

# ---- discover the backend from the cluster itself -------------------------
BE_ROW=$(Q "show backends" | head -1)
BE_IP=$(echo "$BE_ROW" | cut -f2)
BE_HTTP_PORT=$(echo "$BE_ROW" | cut -f5)
BEHTTP="http://$BE_IP:$BE_HTTP_PORT"
BELOG="$(curl -s "$BEHTTP/varz" | sed -n 's/^sys_log_dir=//p')/be.INFO"
[ -r "$BELOG" ] || { echo "cannot read BE log at $BELOG (run this on the BE host)"; exit 1; }
echo "backend=$BEHTTP  log=$BELOG"

show() { for t in $1; do echo -n "  t$t "; curl -s "$BEHTTP/api/compaction/show?tablet_id=$t" \
  | python3 -c 'import sys,json;d=json.load(sys.stdin);print("cp=%s live=%s"%(d.get("cumulative_point"),[r["version"] for r in d["rowset_details"]]))' \
  2>/dev/null || echo "(unreachable -- backend down?)"; done; }

# prints every rowset that spans V; returns 0 if at least one exists
straddle() {
  local found=1 out
  for t in $1; do
    out=$(curl -s "$BEHTTP/api/compaction/show?tablet_id=$t" | python3 -c "
import sys, json
for r in json.load(sys.stdin)['rowset_details']:
    a, b = (int(x) for x in r['version'].split('-'))
    if a <= $2 < b:
        print('  STRADDLE tablet=$t rowset=[%d-%d] V=$2' % (a, b))
" 2>/dev/null)
    [ -n "$out" ] && { echo "$out"; found=0; }
  done
  return $found
}

cleanup() { touch /tmp/$DB.stop 2>/dev/null; Q "admin disable failpoint 'publish_overwrite_txn_stall'" >/dev/null 2>&1; }
trap cleanup EXIT

step "0. a duplicate-key table with one version"
Q "drop database if exists $DB; create database $DB;"
Q "use $DB; create table t (k int, v int) duplicate key(k)
   distributed by hash(k) buckets 1 properties('replication_num'='1');
   insert into t values (1,1);"
TABLE_ID=$(Q "select TABLE_ID from information_schema.tables_config
              where TABLE_SCHEMA='$DB' and TABLE_NAME='t'")
Q "admin enable failpoint 'publish_overwrite_txn_stall'"
LOGMARK=$(wc -l < "$BELOG")
echo "table_id=$TABLE_ID"

step "1. online optimize + throttled continuous loads that span the plan"
# The loads must straddle the moment the rewrite SQL is planned: V has to be a
# version that was already double-written into the temporary partition,
# otherwise V is a hole there and compaction cannot merge across it.
rm -f /tmp/$DB.stop
Q "alter table $DB.t distributed by hash(k) buckets 2" &
( i=0
  while [ ! -f /tmp/$DB.stop ]; do
      i=$((i+1))
      mysql -h"$FE" -P"$FEP" -uroot -N -B -e "insert into $DB.t values (1000+$i,$i)" >/dev/null 2>&1
      sleep 0.2   # unthrottled loads saturate the FE and stall ADMIN statements
  done ) &

step "2. wait for the overwrite publish to park in the failpoint"
V=""
for i in $(seq 1 200); do
  V=$(tail -n +"$LOGMARK" "$BELOG" | grep 'publish_overwrite_txn_stall: parking publish' \
      | grep -oE 'version: [0-9]+' | tail -1 | grep -oE '[0-9]+')
  [ -n "$V" ] && { echo "parked after ${i}s, pinned overwrite version V=$V"; break; }
  sleep 1
done
[ -z "$V" ] && { echo "the overwrite publish never parked -- is the failpoint compiled in?"; exit 1; }

step "3. keep loading well above V, then stop"
sleep 20
touch /tmp/$DB.stop
sleep 2
TEMP_TABLETS=$(Q "select TABLET_ID from information_schema.be_tablets where TABLE_ID=$TABLE_ID
                  and PARTITION_ID <> (select min(PARTITION_ID) from information_schema.be_tablets
                                       where TABLE_ID=$TABLE_ID)")
ALL_TABLETS=$(Q "select TABLET_ID from information_schema.be_tablets where TABLE_ID=$TABLE_ID")
echo "temp partition tablets: $(echo $TEMP_TABLETS)"
show "$ALL_TABLETS"

step "4. compact so a temp tablet holds one rowset spanning V"
# cumulative first to push the cumulative point past V, then base to merge the
# base rowset with everything above it. cumulative alone cannot touch the base
# rowset, which leaves a rowset ending exactly at V and reproduces nothing.
for round in 1 2 3; do
  for t in $ALL_TABLETS; do curl -s -XPOST "$BEHTTP/api/compact?tablet_id=$t&compaction_type=cumulative" >/dev/null; done
  for t in $ALL_TABLETS; do curl -s -XPOST "$BEHTTP/api/compact?tablet_id=$t&compaction_type=base" >/dev/null; done
  echo "-- round $round --"; show "$ALL_TABLETS"
  straddle "$TEMP_TABLETS" "$V" && break
  echo "   no rowset spans V yet, adding versions above V"
  for j in $(seq 1 8); do Q "insert into $DB.t values (2000+$round*10+$j,$j)" >/dev/null; done
done

step "5. precondition check"
# If the failpoint's 5-minute safety cap expired before we got here, the overwrite has already
# been applied and everything below would measure the healthy post-overwrite state: compaction
# legitimately merges across V once the overwrite fills the history, so a "straddle" hit here
# would be a false alarm. Detect that and bail out as inconclusive instead.
if tail -n +"$LOGMARK" "$BELOG" | grep -q "resuming publish"; then
  echo ">>> STALE: the failpoint expired (5-minute cap) before the release step; the overwrite"
  echo "    is already applied and the rowset layout below is the healthy post-overwrite state."
  echo "    Re-run on a warmed-up cluster so the sequence completes within the cap."
  exit 3
fi
if straddle "$TEMP_TABLETS" "$V"; then
  echo ">>> ARMED: a temp partition tablet holds a rowset spanning V=$V"
else
  echo ">>> NOT ARMED: no rowset spans V=$V, releasing anyway (no crash expected)."
  echo "    Re-run: the window depends on when compaction happened to fire."
fi
ROWS_BEFORE=$(Q "select count(*) from $DB.t")
echo "rows before release: $ROWS_BEFORE"

step "6. release the parked publish -> overwrite_rowset() runs"
Q "admin disable failpoint 'publish_overwrite_txn_stall'" &
sleep 25

step "7. verdict"
show "$ALL_TABLETS"
echo "rows after release: $(Q "select count(*) from $DB.t")   (was $ROWS_BEFORE)"
CRASH=$(tail -n +"$LOGMARK" "$BELOG" | grep "Check failed: v == _max_continuous_version" | tail -2)
if [ -n "$CRASH" ]; then
  echo
  echo ">>> REPRODUCED -- the backend aborted:"
  echo "$CRASH"
  exit 2
fi
echo ">>> no DCHECK fired (RELEASE build, or the precondition was not armed)."
echo "    On a RELEASE build compare the row counts above: a drop is the silent data loss."
