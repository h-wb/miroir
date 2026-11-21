import subprocess
import sys
import unittest
from pathlib import Path


@unittest.skipIf(sys.version_info < (3, 12), "agent requires Python 3.12")
class AgentPackageTests(unittest.TestCase):
    def test_agent_suite_with_same_interpreter(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "unittest",
                "discover",
                "-s",
                "tests",
                "-t",
                ".",
            ],
            cwd=Path(__file__).resolve().parents[1] / "agent",
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.assertEqual(
            result.returncode, 0, "\n".join(result.stdout.splitlines()[-40:])
        )
