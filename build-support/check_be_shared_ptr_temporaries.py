#!/usr/bin/env python3

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

"""Lint for the use-after-free anti-pattern fixed in PR #71843.

Anti-pattern (flagged):

    auto* raw = dynamic_cast<T*>(some_lookup(key).get());
    raw->do_something();            // UAF: temporary shared_ptr is gone

When ``some_lookup`` returns ``std::shared_ptr<T>`` by value from a
concurrently-mutated container, the temporary returned by the call is
destroyed at the semicolon. If the only other strong reference was
dropped concurrently (e.g. another thread erased the map entry), the
underlying object is freed while the raw pointer is still being used.

Safe alternative:

    auto shared = some_lookup(key);
    auto* raw   = dynamic_cast<T*>(shared.get());
    raw->do_something();            // shared keeps the object alive

The check is heuristic. It flags only the shape
``cast<T*>(IDENT(args).get())`` where ``IDENT`` is directly followed by
``(`` — i.e. a free function or a ``this`` method call, whose return
value is almost always a shared_ptr copied by value. Member-chain
calls such as ``col->data_column().get()`` are not flagged because
the receiver's lifetime transitively extends the temporary.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


DEFAULT_BASELINE = "build-support/be_shared_ptr_temporary_baseline.json"
DEFAULT_SCAN_ROOTS = ("be/src",)
CODE_EXTENSIONS = {".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".hxx", ".inl"}
REPO_ROOT_MARKERS = ("be", "fe", "gensrc")


# Matches:  cast<...T*...>(  IDENT ( args ) . get ( )  )
#   - IDENT is directly followed by '(' (member-chain calls are excluded
#     because a member call starts with the receiver, not the member name).
#   - args may contain one level of nested parentheses.
#   - The cast expression must NOT be immediately followed by '->' or '.',
#     because in that case the raw pointer is consumed within the same
#     full-expression and the temporary is still alive (C++ full-expression
#     lifetime rule protects against UAF).
_PATTERN = re.compile(
    r"""
    \b(?P<cast>dynamic_cast|static_cast|reinterpret_cast|down_cast)
    \s*<[^<>;]*\*\s*>\s*
    \(\s*
    (?P<call>
        [A-Za-z_][\w:]*\s*
        \(
        (?: [^()] | \( [^()]* \) )*
        \)
    )
    \s*\.\s*get\s*\(\s*\)\s*\)
    (?!\s*(?:->|\.))
    """,
    re.VERBOSE,
)


@dataclass(frozen=True)
class Violation:
    path: str
    line: int
    snippet: str

    def as_dict(self) -> dict[str, object]:
        return {"path": self.path, "line": self.line, "snippet": self.snippet}


def find_repo_root(start: Path) -> Path:
    for candidate in [start, *start.parents]:
        if all((candidate / marker).exists() for marker in REPO_ROOT_MARKERS):
            return candidate
    raise RuntimeError(f"repo root not found above {start}")


def iter_source_files(roots: Iterable[Path]) -> Iterable[Path]:
    for root in roots:
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*")):
            if path.is_file() and path.suffix.lower() in CODE_EXTENSIONS:
                yield path


def git_changed_files(base: str, repo_root: Path) -> list[Path]:
    cmd = ["git", "-C", str(repo_root), "diff", "--name-only", base, "--"]
    out = subprocess.check_output(cmd, text=True)
    return [repo_root / line.strip() for line in out.splitlines() if line.strip()]


_CONTINUATION_RE = re.compile(r"^\s*(?:->|\.)")


def _next_nonblank(lines: list[str], idx: int) -> str:
    for j in range(idx + 1, len(lines)):
        if lines[j].strip():
            return lines[j]
    return ""


def scan_file(repo_root: Path, file_path: Path) -> list[Violation]:
    rel = file_path.relative_to(repo_root).as_posix()
    violations: list[Violation] = []
    try:
        text = file_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return violations
    lines = text.splitlines()
    for lineno, raw in enumerate(lines, start=1):
        stripped = raw.lstrip()
        if stripped.startswith("//") or stripped.startswith("*"):
            continue
        m = _PATTERN.search(raw)
        if not m:
            continue
        # If the match is at the end of the line, a method chain might
        # continue on the next non-blank line; in that case the raw
        # pointer is consumed within the same full-expression and the
        # temporary is still alive — safe.
        if m.end() >= len(raw.rstrip()):
            nxt = _next_nonblank(lines, lineno - 1)
            if _CONTINUATION_RE.match(nxt):
                continue
        violations.append(Violation(path=rel, line=lineno, snippet=raw.strip()))
    return violations


def load_baseline(path: Path) -> set[tuple[str, int]]:
    if not path.exists():
        return set()
    data = json.loads(path.read_text())
    return {
        (entry["path"], int(entry["line"]))
        for entry in data.get("violations", [])
    }


def write_baseline(path: Path, violations: Sequence[Violation]) -> None:
    payload = {
        "_comment": (
            "Pre-existing occurrences of the shared_ptr temporary UAF "
            "anti-pattern detected by check_be_shared_ptr_temporaries.py. "
            "See https://github.com/StarRocks/starrocks/pull/71843. "
            "This list must only shrink."
        ),
        "violations": [
            v.as_dict()
            for v in sorted(violations, key=lambda x: (x.path, x.line))
        ],
    }
    path.write_text(json.dumps(payload, indent=2) + "\n")


def resolve_files(
    mode: str,
    base: str,
    repo_root: Path,
    roots: Sequence[Path],
) -> list[Path]:
    if mode == "full":
        return list(iter_source_files(roots))
    candidates = git_changed_files(base, repo_root)
    out: list[Path] = []
    for f in candidates:
        if not f.exists():
            continue
        if f.suffix.lower() not in CODE_EXTENSIONS:
            continue
        if not any(str(f).startswith(str(r) + "/") or str(f) == str(r) for r in roots):
            continue
        out.append(f)
    return out


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--mode", choices=("full", "changed"), default="full")
    parser.add_argument(
        "--base",
        default="origin/main",
        help="git ref to diff against when --mode=changed",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=None,
        help="path to JSON baseline of pre-existing violations",
    )
    parser.add_argument(
        "--update-baseline",
        action="store_true",
        help="rewrite the baseline with every current match (for maintenance)",
    )
    parser.add_argument(
        "--roots",
        nargs="*",
        default=None,
        help=f"dirs to scan, relative to repo root (default: {' '.join(DEFAULT_SCAN_ROOTS)})",
    )
    args = parser.parse_args(argv)

    repo_root = find_repo_root(Path(__file__).resolve())
    roots = [repo_root / r for r in (args.roots or DEFAULT_SCAN_ROOTS)]
    baseline_path = args.baseline if args.baseline else repo_root / DEFAULT_BASELINE

    files = resolve_files(args.mode, args.base, repo_root, roots)

    violations: list[Violation] = []
    for f in files:
        violations.extend(scan_file(repo_root, f))

    if args.update_baseline:
        write_baseline(baseline_path, violations)
        print(f"wrote {len(violations)} entry/entries to {baseline_path}")
        return 0

    baseline = load_baseline(baseline_path)
    new_violations = [v for v in violations if (v.path, v.line) not in baseline]

    if new_violations:
        print(
            "shared_ptr temporary UAF anti-pattern detected.\n"
            "See https://github.com/StarRocks/starrocks/pull/71843.\n"
            "Fix: bind the shared_ptr to a named local first, then call .get():\n"
            "    auto shared = lookup(key);\n"
            "    auto* raw   = dynamic_cast<T*>(shared.get());\n",
            file=sys.stderr,
        )
        for v in new_violations:
            print(f"  {v.path}:{v.line}: {v.snippet}", file=sys.stderr)
        print(f"\n{len(new_violations)} new violation(s).", file=sys.stderr)
        return 1

    if args.mode == "full":
        stale = baseline - {(v.path, v.line) for v in violations}
        if stale:
            print(
                "stale baseline entries — please delete them:", file=sys.stderr
            )
            for path, line in sorted(stale):
                print(f"  {path}:{line}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
