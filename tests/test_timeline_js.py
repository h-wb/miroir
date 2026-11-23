"""The timeline pages' JavaScript logic (src/web/static/timeline-core.js).

Run with Node's built-in test runner when a `node` is on PATH (GitHub's runners
and the development pod have one); no package is installed for it. Skipped
without Node, or with a Node older than 18.15, the first to have
`--test-reporter`.
"""

import re
import shutil
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OLDEST_NODE = (18, 15)


def _version(node: str) -> tuple[int, ...]:
    found = subprocess.run(
        [node, "--version"], capture_output=True, text=True, check=False, timeout=30
    ).stdout
    match = re.match(r"v(\d+)\.(\d+)", found.strip())
    return tuple(int(part) for part in match.groups()) if match else (0, 0)


class TimelineScriptTests(unittest.TestCase):
    def test_the_pure_functions_pass_their_tests(self):
        node = shutil.which("node")
        if node is None:
            self.skipTest("node is not installed (Node 18.15 or later is needed)")
        if _version(node) < OLDEST_NODE:
            self.skipTest("Node 18.15 or later is needed for --test-reporter")
        files = sorted((ROOT / "tests" / "js").glob("*.test.mjs"))
        self.assertTrue(files)
        # TAP, named: the default reporter depends on the Node version and on
        # whether the output is a terminal.
        result = subprocess.run(
            [node, "--test", "--test-reporter=tap", *map(str, files)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        report = result.stdout + result.stderr
        # Non-zero when any test fails, whatever the report looks like.
        self.assertEqual(result.returncode, 0, report)
        ran = re.search(r"^# tests (\d+)$", result.stdout, re.MULTILINE)
        passed = re.search(r"^# pass (\d+)$", result.stdout, re.MULTILINE)
        assert ran is not None and passed is not None, report
        # An empty run cannot pass.
        self.assertGreater(int(ran.group(1)), 0, report)
        self.assertEqual(passed.group(1), ran.group(1), report)
