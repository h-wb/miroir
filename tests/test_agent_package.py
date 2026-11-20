import subprocess
import sys
import unittest
from pathlib import Path


class AgentPackageTests(unittest.TestCase):
    def test_agent_suite_without_site_packages(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-S",
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
