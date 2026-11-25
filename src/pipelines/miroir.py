"""Prefect entry point for miroir connections.

Deployed per connection (decision 013), and once for the miroir's derive runs
(the media deployment, decision 033 amendment). A flow run needs the miroir
configuration at `config_path`, the metadata database (MIROIR_METADATA_URL or
MIROIR_METADATA__* — see `metadata_url`), and, for built-in secrets,
MIROIR_SECRET_KEY (decision 011). A derive run (thumbnails, decision 040) also
needs MIROIR_MEDIA_ACCESS_KEY_ID and MIROIR_MEDIA_SECRET_ACCESS_KEY; without them
it is blocked and every other operation runs as before.
"""

import os
import uuid
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from prefect import flow
from prefect.runtime import deployment, flow_run

from src.adapters.s3 import from_environment
from src.core.config import load_miroir, metadata_url
from src.core.operations import MetadataStore
from src.core.runner import run_connection
from src.core.secrets import SecretBox, SecretStore, make_resolver
from src.pipelines.transform import build_connection_models


def schedule_build(
    store: MetadataStore, deployment_id: str, connection_id: str, config_path: str
) -> Callable[[str, datetime], None]:
    """Start a recorded operation on this deployment at a given time: a deferred
    build, or the next batch of a derive run (decision 040).

    The operation ID is the idempotency key, as for any submission.
    """
    from prefect.client.schemas.objects import FlowRun
    from prefect.deployments import run_deployment

    def schedule(operation_id: str, start_at: datetime) -> None:
        scheduled = run_deployment(
            uuid.UUID(deployment_id),
            parameters={
                "connection_id": connection_id,
                "operation_id": operation_id,
                "config_path": config_path,
            },
            scheduled_time=start_at,
            timeout=0,
            idempotency_key=operation_id,
            # Its own run: it must outlive the load that scheduled it.
            as_subflow=False,
        )
        assert isinstance(scheduled, FlowRun)  # synchronous outside an event loop
        store.bind_backend_run(operation_id, str(scheduled.id))

    return schedule


@flow
def sync_connection(
    connection_id: str,
    operation_id: str | None = None,
    config_path: str = "miroir.json",
) -> dict:
    """Sync one miroir connection into Postgres and rebuild its models.

    Args:
        connection_id: Connection ID from the miroir configuration.
        operation_id: Operation requested through the miroir CLI, if any.
            Runs without one (schedules, n8n) register their own operation.
        config_path: Miroir configuration file on the worker.
    """
    url = metadata_url()
    if url is None:
        raise RuntimeError(
            "Set MIROIR_METADATA_URL or MIROIR_METADATA__HOST/USERNAME/PASSWORD"
        )
    store = MetadataStore(url)
    store.migrate()
    # Outside a deployment there is nothing to schedule on; builds run with the load.
    deployment_id = deployment.id
    miroir = load_miroir(Path(config_path))
    outcome = run_connection(
        miroir,
        connection_id,
        store=store,
        destination="postgres",
        # Built-in secrets need MIROIR_SECRET_KEY on the worker (decision 011).
        resolve_credentials=make_resolver(
            SecretStore(store, SecretBox.from_environment())
            if os.environ.get("MIROIR_SECRET_KEY")
            else None
        ),
        origin="request" if operation_id else "prefect",
        operation_id=operation_id,
        backend_run_id=str(flow_run.id),
        transform=build_connection_models,
        # A build that waited for a lock checks the configuration again first.
        reload=lambda: load_miroir(Path(config_path)),
        defer=schedule_build(store, deployment_id, connection_id, config_path)
        if deployment_id
        else None,
        # Overlapping runs of one connection queue instead of failing.
        lock_wait=3600,
        object_store=from_environment(miroir.media) if miroir.media else None,
        # Watched folders register files where the server keeps artifacts:
        # MIROIR_ARTIFACT_DIR, or `artifacts` beside the configuration.
        artifact_root=Path(
            os.environ.get("MIROIR_ARTIFACT_DIR")
            or Path(config_path).parent / "artifacts"
        ),
    )
    return {
        "operation_id": outcome.operation_id,
        "attempt": outcome.attempt,
        "ingestion": outcome.ingestion,
        "transformation": outcome.transformation,
        "recorded": outcome.recorded,
    }
