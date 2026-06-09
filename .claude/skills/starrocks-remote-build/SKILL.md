---
name: starrocks-remote-build
description: >-
  Build StarRocks Backend (C++) and/or Frontend (Java) on the remote SSH host by
  running build.sh there and streaming the output back. Use when the user wants
  to compile StarRocks remotely — "build be on sr-dev", "远程编译 fe", "clean build
  starrocks remotely", "rebuild backend in debug/asan mode". Building locally in
  this container is impractical; this offloads it to the powerful remote box.
  Push local changes first with starrocks-remote-sync.
---

# StarRocks Remote Build

Runs `build.sh` on the remote (alias `sr-dev`) inside `$SR_REMOTE_HOME` and
streams the compile output back live. The remote `build.sh` sources `env.sh`
and expects the thirdparty libraries to already be compiled there.

## Before building

Make sure the remote tree reflects your latest edits:
```bash
bash .claude/skills/lib/sr-remote.sh sync --push
```

## Commands

```bash
# Build both BE and FE (default when no args)
bash .claude/skills/lib/sr-remote.sh build

# Backend only / Frontend only
bash .claude/skills/lib/sr-remote.sh build --be
bash .claude/skills/lib/sr-remote.sh build --fe

# Clean rebuild
bash .claude/skills/lib/sr-remote.sh build --be --clean
```

Any `build.sh` flags pass straight through. To pick a build type, set the env
var on the remote invocation, e.g. a Debug/Asan backend:
```bash
bash .claude/skills/lib/sr-remote.sh ssh "BUILD_TYPE=Asan bash build.sh --be"
```

## On failure

The remote command's non-zero exit and compiler errors are returned. Read the
last error, fix the source **locally**, `sync --push`, and rebuild. Artifacts
land in `output/fe` and `output/be` on the remote, consumed by
**starrocks-remote-deploy**.
