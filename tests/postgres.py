"""A disposable Postgres cluster for one test class.

Set MIROIR_TEST_POSTGRES_BIN to an installed Postgres bin directory, or put
initdb/pg_ctl on PATH. It trusts local connections and listens on 127.0.0.1.
"""

import os
import shutil
import socket
import subprocess
import tempfile
import unittest
from pathlib import Path


def start_postgres(case: type[unittest.TestCase], prefix: str) -> tuple[Path, int]:
    """Start a cluster for `case`, stopped at its class cleanup: (root, port)."""
    binary = os.environ.get("MIROIR_TEST_POSTGRES_BIN")
    initdb = str(Path(binary) / "initdb") if binary else shutil.which("initdb")
    pg_ctl = str(Path(binary) / "pg_ctl") if binary else shutil.which("pg_ctl")
    if not initdb or not pg_ctl:
        raise unittest.SkipTest(
            "Postgres binaries unavailable; set MIROIR_TEST_POSTGRES_BIN"
        )
    tmp = tempfile.TemporaryDirectory(prefix=prefix)
    case.addClassCleanup(tmp.cleanup)
    root = Path(tmp.name)
    sockets = root / "socket"
    sockets.mkdir()
    datadir = root / "postgres"
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    subprocess.run(
        [initdb, "-D", str(datadir), "-A", "trust", "--no-locale", "--encoding=UTF8"],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            pg_ctl,
            "-D",
            str(datadir),
            "-l",
            str(root / "postgres.log"),
            "-o",
            f"-F -k {sockets} -h 127.0.0.1 -p {port}",
            "-w",
            "start",
        ],
        check=True,
        capture_output=True,
    )
    case.addClassCleanup(
        subprocess.run,
        [pg_ctl, "-D", str(datadir), "-m", "fast", "-w", "stop"],
        check=True,
        capture_output=True,
    )
    return root, port
