---
name: starrocks-remote-sync
description: >-
  Sync the StarRocks source tree between the local checkout and a remote
  build/deploy host over SSH. Use when the user wants to push local edits to the
  remote, pull the authoritative remote tree down for Claude to read/edit/grep,
  set up an sshfs mount for live remote editing, or verify the SSH connection to
  the remote dev box. Trigger phrases: "同步代码到远程", "sync to sr-dev", "pull
  remote starrocks", "push my changes to the build machine".
---

# StarRocks Remote Sync

The remote host (default SSH alias `sr-dev`) holds the authoritative StarRocks
checkout. Because Claude's `Read`/`Edit`/`Grep` tools operate on the local
filesystem, keep a local mirror in sync: **pull before editing, push before
building.**

## Setup (once)

1. Define the alias in `~/.ssh/config` with key-based auth (no password):
   ```
   Host sr-dev
       HostName <ip-or-dns>
       User     <user>
       IdentityFile ~/.ssh/<key>
   ```
2. Verify connectivity and that the remote checkout exists:
   ```bash
   bash .claude/skills/lib/sr-remote.sh check
   ```

Override defaults with env vars when needed: `SR_REMOTE` (alias),
`SR_REMOTE_HOME` (remote repo path, default `~/starrocks`), `SR_LOCAL_HOME`.

## Commands

```bash
# Pull the remote tree into the local mirror (refresh before editing)
bash .claude/skills/lib/sr-remote.sh sync --pull

# Push local edits to the remote (do this before build/deploy)
bash .claude/skills/lib/sr-remote.sh sync --push

# Preview what would change, no transfer
bash .claude/skills/lib/sr-remote.sh sync --push --dry-run
```

`rsync --delete` is used, so the destination is made to match the source.
Build artifacts, `.git/`, `thirdparty/`, and caches are excluded — see the
`_RSYNC_EXCLUDES` list in `sr-remote.sh`.

## Live editing alternative (sshfs)

For true edit-in-place on the remote without push/pull cycles, mount it:
```bash
sshfs sr-dev:starrocks /home/user/starrocks-remote -o reconnect,follow_symlinks
```
Then point Claude at the mounted path. Slower for large greps; prefer
pull → edit locally → push for heavy work.

## Workflow

1. `sync --pull` → 2. edit locally → 3. `sync --push` → 4. hand off to
   **starrocks-remote-build** → **-deploy** → **-test**.
