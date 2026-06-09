# StarRocks remote development skills

Develop, build, deploy, and test StarRocks on a remote machine over SSH while
driving it from Claude Code. StarRocks' Backend (C++) and Frontend (Java) are
heavy to build, so the work is offloaded to a powerful remote host; this
container acts as the editor + orchestrator.

## Skills

| Skill | Purpose |
|-------|---------|
| `starrocks-remote-sync`   | rsync the source tree local ↔ remote; verify SSH; sshfs option |
| `starrocks-remote-build`  | run `build.sh --be/--fe` on the remote, stream output |
| `starrocks-remote-deploy` | start/stop/restart FE+BE, status, logs, SQL, BE bootstrap |
| `starrocks-remote-test`   | run `run-be-ut.sh` / `run-fe-ut.sh` / `test/run.py` remotely |

All four call one shared driver, `lib/sr-remote.sh`, so SSH/connection handling
lives in a single place and no credentials are stored in the repo.

## One-time setup

Define the remote as an SSH config alias with key-based auth in `~/.ssh/config`:

```
Host sr-dev
    HostName <ip-or-dns>
    User     <user>
    IdentityFile ~/.ssh/<key>
```

Then verify:

```bash
bash .claude/skills/lib/sr-remote.sh check
```

## Configuration (env vars)

| Var | Default | Meaning |
|-----|---------|---------|
| `SR_REMOTE`        | `sr-dev`      | SSH config alias of the remote host |
| `SR_REMOTE_HOME`   | `~/starrocks` | StarRocks checkout path on the remote |
| `SR_LOCAL_HOME`    | repo root     | local mirror path |
| `SR_FE_QUERY_PORT` | `9030`        | FE MySQL-protocol port |
| `SR_FE_USER`       | `root`        | SQL user for quick queries |

## Typical loop

```
sync --pull → edit locally → sync --push → build --be/--fe
            → deploy restart → sql "SHOW BACKENDS\G" → test be/fe/sql
```
