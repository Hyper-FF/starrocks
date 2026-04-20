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

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parent / "check_be_shared_ptr_temporaries.py"
SPEC = importlib.util.spec_from_file_location("check_be_shared_ptr_temporaries", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"failed to load {MODULE_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def _make_repo(tmp: Path) -> Path:
    (tmp / "be" / "src" / "runtime").mkdir(parents=True)
    (tmp / "fe").mkdir()
    (tmp / "gensrc").mkdir()
    return tmp


def _scan(content: str, *, filename: str = "t.cpp") -> list:
    with tempfile.TemporaryDirectory() as tmpdir:
        repo = _make_repo(Path(tmpdir))
        f = repo / "be" / "src" / "runtime" / filename
        f.write_text(content)
        return MODULE.scan_file(repo, f)


class PatternTest(unittest.TestCase):
    def test_flags_dynamic_cast_of_temporary(self) -> None:
        violations = _scan(
            "auto* x = dynamic_cast<T*>(get_tablets_channel(key).get());\n"
        )
        self.assertEqual(len(violations), 1)
        self.assertEqual(violations[0].line, 1)
        self.assertIn("dynamic_cast", violations[0].snippet)

    def test_flags_down_cast_of_temporary(self) -> None:
        violations = _scan("auto* x = down_cast<Foo*>(make_foo().get());\n")
        self.assertEqual(len(violations), 1)

    def test_flags_static_and_reinterpret_casts(self) -> None:
        src = (
            "auto* a = static_cast<T*>(lookup(k).get());\n"
            "auto* b = reinterpret_cast<T*>(lookup(k).get());\n"
        )
        violations = _scan(src)
        self.assertEqual(len(violations), 2)

    def test_flags_namespaced_call(self) -> None:
        violations = _scan(
            "auto* x = dynamic_cast<T*>(foo::bar::lookup(k).get());\n"
        )
        self.assertEqual(len(violations), 1)

    def test_flags_real_pr71843_line(self) -> None:
        # The exact line that triggered the original bug.
        violations = _scan(
            "    auto local_tablets_channel = "
            "dynamic_cast<LocalTabletsChannel*>("
            "get_tablets_channel(key).get());\n"
        )
        self.assertEqual(len(violations), 1)

    def test_ignores_named_local_shared_ptr(self) -> None:
        src = (
            "auto channel = get_tablets_channel(key);\n"
            "auto* x = dynamic_cast<T*>(channel.get());\n"
        )
        self.assertEqual(_scan(src), [])

    def test_ignores_member_accessor_chain(self) -> None:
        # receiver->member() keeps the inner shared_ptr alive.
        src = (
            "auto* x = down_cast<const Foo*>(col->data_column().get());\n"
            "auto* y = down_cast<const Foo*>(col.data_column().get());\n"
        )
        self.assertEqual(_scan(src), [])

    def test_ignores_comments(self) -> None:
        src = (
            "// auto* x = dynamic_cast<T*>(lookup(k).get());\n"
            " * auto* x = dynamic_cast<T*>(lookup(k).get());\n"
        )
        self.assertEqual(_scan(src), [])

    def test_ignores_get_on_named_member(self) -> None:
        # _channel.get() binds through a member; not a temporary.
        src = "auto* x = dynamic_cast<T*>(_channel.get());\n"
        self.assertEqual(_scan(src), [])

    def test_allows_nested_paren_args(self) -> None:
        violations = _scan(
            "auto* x = dynamic_cast<T*>(lookup(make_key(a, b)).get());\n"
        )
        self.assertEqual(len(violations), 1)

    def test_ignores_arrow_chain_on_same_line(self) -> None:
        # The raw pointer is consumed within the same full-expression, so
        # the temporary shared_ptr is still alive when it's dereferenced.
        src = (
            "total = down_cast<T*>(lookup(k).get())->field_count();\n"
        )
        self.assertEqual(_scan(src), [])

    def test_ignores_arrow_chain_on_next_line(self) -> None:
        # Same as above, but the '->' call is wrapped to the next line.
        src = (
            "total = down_cast<T*>(lookup(k).get())\n"
            "            ->field_count();\n"
        )
        self.assertEqual(_scan(src), [])

    def test_ignores_dot_chain(self) -> None:
        src = "x = dynamic_cast<T*>(lookup(k).get()).field;\n"
        self.assertEqual(_scan(src), [])


class BaselineTest(unittest.TestCase):
    def test_new_violation_fails_without_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = _make_repo(Path(tmpdir))
            src = repo / "be" / "src" / "runtime" / "t.cpp"
            src.write_text(
                "auto* x = dynamic_cast<T*>(lookup(k).get());\n"
            )
            empty_baseline = repo / "empty.json"
            empty_baseline.write_text('{"violations": []}')

            violations = MODULE.scan_file(repo, src)
            self.assertEqual(len(violations), 1)

            baseline = MODULE.load_baseline(empty_baseline)
            new = [v for v in violations if (v.path, v.line) not in baseline]
            self.assertEqual(len(new), 1)

    def test_baseline_entry_suppresses_match(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = _make_repo(Path(tmpdir))
            src = repo / "be" / "src" / "runtime" / "t.cpp"
            src.write_text(
                "auto* x = dynamic_cast<T*>(lookup(k).get());\n"
            )
            baseline_path = repo / "baseline.json"
            baseline_path.write_text(json.dumps({
                "violations": [
                    {"path": "be/src/runtime/t.cpp", "line": 1, "snippet": ""}
                ]
            }))

            violations = MODULE.scan_file(repo, src)
            baseline = MODULE.load_baseline(baseline_path)
            new = [v for v in violations if (v.path, v.line) not in baseline]
            self.assertEqual(new, [])

    def test_write_baseline_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            baseline_path = Path(tmpdir) / "b.json"
            violations = [
                MODULE.Violation(path="be/src/a.cpp", line=10, snippet="x"),
                MODULE.Violation(path="be/src/a.cpp", line=5, snippet="y"),
            ]
            MODULE.write_baseline(baseline_path, violations)
            loaded = MODULE.load_baseline(baseline_path)
            self.assertEqual(
                loaded,
                {("be/src/a.cpp", 5), ("be/src/a.cpp", 10)},
            )


class ShippedBaselineTest(unittest.TestCase):
    """The repo's real baseline must stay consistent with the repo source."""

    def test_shipped_baseline_entries_match_source(self) -> None:
        script_dir = Path(__file__).resolve().parent
        repo_root = MODULE.find_repo_root(script_dir)
        baseline_path = repo_root / MODULE.DEFAULT_BASELINE
        if not baseline_path.exists():
            self.skipTest("no shipped baseline")
        baseline = MODULE.load_baseline(baseline_path)
        for path, line in baseline:
            src = repo_root / path
            if not src.exists():
                self.fail(f"baseline points at missing file: {path}")
            lines = src.read_text(encoding="utf-8", errors="replace").splitlines()
            self.assertLessEqual(line, len(lines), f"{path}:{line} beyond EOF")
            self.assertRegex(
                lines[line - 1],
                r"(dynamic_cast|static_cast|reinterpret_cast|down_cast)\s*<",
                f"baseline {path}:{line} no longer matches the anti-pattern",
            )


if __name__ == "__main__":
    unittest.main()
