---
name: starrocks-remote-deploy
description: >-
  Deploy and manage a StarRocks cluster (FE + BE) on the remote SSH host from the
  build output — start, stop, restart, check process status, tail logs, run quick
  SQL against the FE, and do first-time BE registration. Use when the user wants
  to "start starrocks on sr-dev", "重启 be", "deploy the cluster", "restart fe and
  check it's up", "add backend", or "show backends". Runs after starrocks-remote-build.
---

# StarRocks Remote Deploy

Manages the cluster on the remote (alias `sr-dev`) using the start/stop scripts
under `output/fe/bin` and `output/be/bin` produced by the build.

## Process lifecycle

```bash
# Start / stop / restart (target: all | fe | be)
bash .claude/skills/lib/sr-remote.sh deploy start all
bash .claude/skills/lib/sr-remote.sh deploy restart be
bash .claude/skills/lib/sr-remote.sh deploy stop fe

# See what's running
bash .claude/skills/lib/sr-remote.sh deploy status
```

Processes start with `--daemon`. FE uses `output/fe/conf/fe.conf`, BE uses
`output/be/conf/be.conf` on the remote — edit those there (or in the synced
tree under `conf/` before build) to set `meta_dir`, `storage_root_path`,
`priority_networks`, ports, etc.

## First-time bootstrap

A freshly started FE does not know about any BE. With the FE up, register the BE:
```bash
bash .claude/skills/lib/sr-remote.sh deploy init
```
This runs `ALTER SYSTEM ADD BACKEND "<remote-ip>:9050"`. Verify:
```bash
bash .claude/skills/lib/sr-remote.sh sql "SHOW BACKENDS\G"
```
`Alive: true` means the cluster is ready.

## Inspect

```bash
# Quick SQL against the FE (mysql client on the remote, port 9030)
bash .claude/skills/lib/sr-remote.sh sql "SHOW FRONTENDS\G"

# Tail logs (Ctrl-C to stop)
bash .claude/skills/lib/sr-remote.sh logs fe
bash .claude/skills/lib/sr-remote.sh logs be
```

Override `SR_FE_QUERY_PORT` (default 9030) / `SR_FE_USER` (default root) if the
remote FE uses non-default settings.
