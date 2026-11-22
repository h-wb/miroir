"""The timeline pages' JavaScript logic (src/web/static/timeline-core.js).

Run with Node's built-in test runner when a `node` is on PATH (GitHub's runners
and the development pod have one); no package is installed for it. Skipped
without Node.
"""

import shutil
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class TimelineScriptTests(unittest.TestCase):
    def test_the_pure_functions_pass_their_tests(self):
        node = shutil.which("node")
        if node is None:
            self.skipTest("node is not installed")
        result = subprocess.run(
            [
                node,
                "--test",
                *map(str, sorted((ROOT / "tests" / "js").glob("*.test.mjs"))),
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=120,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("# fail 0", result.stdout)
