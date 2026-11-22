"""dbt builds of Miroir analyses, into each analysis's own schemas."""

import json
import tempfile
from pathlib import Path

from dbt.cli.main import dbtRunner
from prefect.concurrency.sync import concurrency

from src.core.analysis import AnalysisPlan

DBT_DIR = Path(__file__).parents[2] / "dbt"


def build_connection_models(plan: AnalysisPlan) -> None:
    """Build and test the models of one analysis plan.

    A plan's `full_refresh` rebuilds incremental models from scratch. Its `ready`
    runs once the dbt slot is held, before dbt starts; an exception from it stops
    the build.
    """
    args = ["build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    args += ["--select", plan.selector, "--vars", json.dumps(plan.variables)]
    if plan.full_refresh:
        args.append("--full-refresh")
    # one build at a time: loads finishing together would rebuild the same crossover tables
    with tempfile.TemporaryDirectory(prefix="miroir-dbt-") as tmp:
        # Fresh parsing and per-run artifacts avoid stale bindings and concurrent
        # processes sharing target/log files. Raw columns may evolve every load.
        args += [
            "--no-partial-parse",
            "--target-path",
            f"{tmp}/target",
            "--log-path",
            f"{tmp}/logs",
        ]
        with concurrency("dbt", occupy=1):
            if plan.ready is not None:
                plan.ready()
            result = dbtRunner().invoke(args)
    if not result.success:
        raise RuntimeError(
            f"dbt build failed: {result.exception or 'see the model/test errors above'}"
        )
