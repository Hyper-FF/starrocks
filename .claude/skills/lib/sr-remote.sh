#!/usr/bin/env bash
##############################################################################
# sr-remote.sh — shared driver for StarRocks remote development over SSH.
#
# All four starrocks-remote-* skills (sync / build / deploy / test) call into
# this single script so connection handling lives in one place.
#
# Configuration (override via environment; sensible defaults shown):
#   SR_REMOTE        SSH config alias of the build/deploy host   (default: sr-dev)
#   SR_REMOTE_HOME   StarRocks checkout path ON the remote host  (default: ~/starrocks)
#   SR_LOCAL_HOME    Local mirror of the checkout                (default: repo root)
#   SR_FE_QUERY_PORT MySQL protocol port of the FE               (default: 9030)
#   SR_FE_USER       SQL user for quick queries                  (default: root)
#
# The remote alias is expected to be defined in ~/.ssh/config, e.g.:
#   Host sr-dev
#       HostName 10.0.0.12
#       User     starrocks
#       IdentityFile ~/.ssh/sr_dev_ed25519
#
# Usage:
#   sr-remote.sh ssh    [cmd...]               run a command on the remote (in repo dir)
#   sr-remote.sh sync   [--pull|--push] [--dry-run]
#   sr-remote.sh build  [build.sh args...]     e.g. --be / --fe / --be --clean
#   sr-remote.sh deploy <start|stop|restart|status|init> [fe|be|all]
#   sr-remote.sh test   <be|fe|sql> [args...]
#   sr-remote.sh sql    "SELECT ..."           run a quick query against the FE
#   sr-remote.sh logs   <fe|be>                tail the most relevant log
#   sr-remote.sh check                         verify SSH connectivity + repo path
##############################################################################
set -uo pipefail

SR_REMOTE="${SR_REMOTE:-sr-dev}"
SR_REMOTE_HOME="${SR_REMOTE_HOME:-~/starrocks}"
SR_FE_QUERY_PORT="${SR_FE_QUERY_PORT:-9030}"
SR_FE_USER="${SR_FE_USER:-root}"

# Resolve the local repo root (two levels up from .claude/skills/lib).
_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SR_LOCAL_HOME="${SR_LOCAL_HOME:-$(cd "${_SCRIPT_DIR}/../../.." && pwd)}"

# Run on the SSH config alias only. We never inline host/user/key here so that
# no credentials end up in the repo or in process listings.
_ssh() { ssh -o BatchMode=yes "$SR_REMOTE" "$@"; }

# Run a command on the remote inside the StarRocks checkout.
remote_in_repo() {
  _ssh "cd ${SR_REMOTE_HOME} && $*"
}

die() { echo "[sr-remote] error: $*" >&2; exit 1; }
info() { echo "[sr-remote] $*" >&2; }

# ---------------------------------------------------------------------------
cmd_check() {
  info "remote alias : ${SR_REMOTE}"
  info "remote home  : ${SR_REMOTE_HOME}"
  info "local mirror : ${SR_LOCAL_HOME}"
  _ssh "echo connected as \$(whoami)@\$(hostname)" || die "cannot ssh to '${SR_REMOTE}' (check ~/.ssh/config and BatchMode key auth)"
  remote_in_repo "test -f build.sh && echo 'repo OK: '\$(pwd)" \
    || die "no StarRocks checkout at ${SR_REMOTE_HOME} on ${SR_REMOTE}"
}

cmd_ssh() {
  if [ "$#" -eq 0 ]; then
    _ssh
  else
    remote_in_repo "$*"
  fi
}

# rsync excludes: build artifacts, VCS, vendored thirdparty, caches.
_RSYNC_EXCLUDES=(
  --exclude '.git/'
  --exclude 'output/'
  --exclude 'thirdparty/'
  --exclude 'be/build*/'
  --exclude 'be/ut_build*/'
  --exclude '**/target/'
  --exclude '**/__pycache__/'
  --exclude '*.pyc'
  --exclude '*.o'
  --exclude '*.so'
  --exclude '.idea/'
  --exclude '.DS_Store'
)

cmd_sync() {
  local dir="push" dry=()
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --pull) dir="pull" ;;
      --push) dir="push" ;;
      --dry-run) dry=(--dry-run) ;;
      *) die "sync: unknown arg '$1'" ;;
    esac
    shift
  done
  # rsync over the same SSH alias so config/key handling is identical.
  local rsh="ssh"
  local local_tree="${SR_LOCAL_HOME}/"
  local remote_tree="${SR_REMOTE}:${SR_REMOTE_HOME}/"
  if [ "$dir" = "pull" ]; then
    info "pull: ${remote_tree} -> ${local_tree}"
    rsync -az --delete "${dry[@]}" "${_RSYNC_EXCLUDES[@]}" -e "$rsh" "$remote_tree" "$local_tree"
  else
    info "push: ${local_tree} -> ${remote_tree}"
    rsync -az --delete "${dry[@]}" "${_RSYNC_EXCLUDES[@]}" -e "$rsh" "$local_tree" "$remote_tree"
  fi
}

cmd_build() {
  [ "$#" -gt 0 ] || set -- --be --fe
  info "remote build: build.sh $*"
  # Stream output back live; build.sh sources env.sh on the remote side.
  remote_in_repo "bash build.sh $*"
}

_fe_out="${SR_REMOTE_HOME}/output/fe"
_be_out="${SR_REMOTE_HOME}/output/be"

cmd_deploy() {
  local action="${1:-status}"; shift || true
  local target="${1:-all}"
  case "$action" in
    start)
      [ "$target" != be ] && remote_in_repo "output/fe/bin/start_fe.sh --daemon" && info "FE started"
      [ "$target" != fe ] && remote_in_repo "output/be/bin/start_be.sh --daemon" && info "BE started"
      ;;
    stop)
      [ "$target" != be ] && remote_in_repo "output/fe/bin/stop_fe.sh" || true
      [ "$target" != fe ] && remote_in_repo "output/be/bin/stop_be.sh" || true
      ;;
    restart)
      cmd_deploy stop "$target"; sleep 2; cmd_deploy start "$target"
      ;;
    status)
      remote_in_repo "ps -ef | grep -E 'StarRocksFE|starrocks_be' | grep -v grep || echo 'no StarRocks processes running'"
      ;;
    init)
      # First-time bootstrap: register the BE with the FE so the cluster is usable.
      info "registering BE with FE (requires FE running)"
      local be_host be_heartbeat="9050"
      be_host="$(_ssh "hostname -i | awk '{print \$1}'")"
      cmd_sql "ALTER SYSTEM ADD BACKEND \"${be_host}:${be_heartbeat}\";" \
        && info "BE ${be_host}:${be_heartbeat} added; verify with: sr-remote.sh sql 'SHOW BACKENDS\\G'"
      ;;
    *) die "deploy: unknown action '$action' (start|stop|restart|status|init)" ;;
  esac
}

cmd_test() {
  local kind="${1:-}"; shift || true
  case "$kind" in
    be) remote_in_repo "bash run-be-ut.sh $*" ;;
    fe) remote_in_repo "bash run-fe-ut.sh $*" ;;
    sql) remote_in_repo "cd test && python3 run.py $*" ;;
    *) die "test: specify be | fe | sql" ;;
  esac
}

cmd_sql() {
  [ "$#" -gt 0 ] || die "sql: provide a query"
  local q="$*"
  # Use the mysql client on the remote against the local FE.
  remote_in_repo "mysql -h127.0.0.1 -P${SR_FE_QUERY_PORT} -u${SR_FE_USER} -e \"${q}\""
}

cmd_logs() {
  case "${1:-}" in
    fe) remote_in_repo "tail -n 100 -f output/fe/log/fe.log" ;;
    be) remote_in_repo "tail -n 100 -f output/be/log/be.INFO" ;;
    *) die "logs: specify fe | be" ;;
  esac
}

main() {
  local sub="${1:-}"; shift || true
  case "$sub" in
    check)  cmd_check "$@" ;;
    ssh)    cmd_ssh "$@" ;;
    sync)   cmd_sync "$@" ;;
    build)  cmd_build "$@" ;;
    deploy) cmd_deploy "$@" ;;
    test)   cmd_test "$@" ;;
    sql)    cmd_sql "$@" ;;
    logs)   cmd_logs "$@" ;;
    ""|-h|--help) sed -n '2,46p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' ;;
    *) die "unknown subcommand '$sub' (try --help)" ;;
  esac
}

main "$@"
