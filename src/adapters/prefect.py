"""Prefect as the miroir's orchestrator (decision 013).

The miroir owns desired state (connections, schedules); Prefect owns execution.
This adapter only translates: deployments from configuration (plan/apply),
requests into flow runs (submit), and flow run states back into attempts (observe).

It blocks, so callers run it off the event loop (the web routes that reach it are
synchronous). On a loop Prefect's `to_deployment` and `run_deployment` would hand
back coroutines instead of results.
"""

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from typing import Any, Literal, ParamSpec, TypedDict, TypeVar

from src.core.config import ResolvedMiroir
from src.core.operations import MetadataStore, OperationError, OrchestratorUnavailable

P = ParamSpec("P")
R = TypeVar("R")

FLOW_NAME = "sync-connection"
# Prefect queues overlapping runs of one connection.
CONCURRENCY_LIMIT = 1
# The deployment attributes the miroir owns; a difference in any of them is drift.
OWNED = (
    "parameters",
    "schedules",
    "work_pool_name",
    "job_variables",
    "concurrency_limit",
)
UNAVAILABLE = (
    "Execution is unavailable; check the Prefect service and credentials,"
    " then try again."
)


def _readable_errors(function: Callable[P, R]) -> Callable[P, R]:
    """Prefect failures as product errors; an unreachable or refusing Prefect is
    OrchestratorUnavailable, so callers can ask to try again later."""

    @wraps(function)
    def call(*args: P.args, **kwargs: P.kwargs) -> R:
        import httpx
        from prefect.deployments.runner import DeploymentApplyError
        from prefect.exceptions import ObjectNotFound

        try:
            return function(*args, **kwargs)
        except ObjectNotFound as error:
            raise OperationError(
                "Execution work pool is unavailable; check the runtime configuration and try again."
            ) from error
        except DeploymentApplyError as error:
            if isinstance(error.__cause__, httpx.TransportError):
                raise OrchestratorUnavailable(UNAVAILABLE) from error
            # Creating a deployment wraps Prefect's answer (e.g. a missing work pool).
            raise OperationError(f"Prefect rejected the deployment: {error}") from error
        except httpx.HTTPStatusError as error:
            raise OrchestratorUnavailable(
                f"Prefect refused the request ({error.response.status_code}): check"
                " the Prefect service and the server's credentials, then try again."
            ) from error
        except httpx.HTTPError as error:
            raise OrchestratorUnavailable(UNAVAILABLE) from error

    return call


class DeploymentSpec(TypedDict):
    """One connection's deployment, in Prefect's own names: exactly the keyword
    arguments `Flow.to_deployment` takes (decision 029)."""

    name: str
    parameters: dict[str, Any]
    schedules: list[Any]  # Prefect schedule models
    work_pool_name: str
    # Includes `image`, which is where Prefect keeps it.
    job_variables: dict[str, Any]
    concurrency_limit: int
    tags: list[str]


def deployment_name(miroir_id: str, connection_id: str) -> str:
    return f"{miroir_id}-{connection_id}"


def _owned(attributes: Any) -> dict[str, Any]:
    """The owned attributes as JSON, in Prefect's serialization."""
    return {
        **{key: attributes[key] for key in OWNED},
        "schedules": [
            schedule.model_dump(mode="json") for schedule in attributes["schedules"]
        ],
    }


@dataclass
class PrefectOrchestrator:
    store: MetadataStore
    # Path of the miroir configuration where flow runs execute (worker or Job).
    config_path: str

    # ---- Deployments ------------------------------------------------------------

    def desired(self, miroir: ResolvedMiroir) -> dict[str, DeploymentSpec]:
        from prefect.client.schemas.schedules import CronSchedule

        declared = miroir.deployment
        if declared is None:
            raise OperationError(
                "No work pool configured: set deployment.work_pool in the configuration"
            )
        job_variables = dict(declared.job_variables or {})
        if declared.image:
            job_variables["image"] = declared.image
        specs: dict[str, DeploymentSpec] = {}
        for connection in miroir.connections:
            schedule = connection.config.schedule
            name = deployment_name(miroir.id, connection.config.id)
            specs[name] = {
                "name": name,
                "parameters": {
                    "connection_id": connection.config.id,
                    "config_path": self.config_path,
                },
                "schedules": [
                    CronSchedule(cron=schedule.cron, timezone=schedule.timezone)
                ]
                if schedule
                else [],
                "work_pool_name": declared.work_pool,
                "job_variables": dict(job_variables),
                "concurrency_limit": CONCURRENCY_LIMIT,
                "tags": ["miroir", self._tag(miroir)],
            }
        return specs

    def _tag(self, miroir: ResolvedMiroir) -> str:
        return f"miroir:{miroir.id}"

    def actual(self, miroir: ResolvedMiroir) -> dict[str, dict[str, Any]]:
        """What Prefect has for this miroir: per deployment, its ID and the
        owned attributes."""
        from prefect import get_client
        from prefect.client.schemas.filters import (
            DeploymentFilter,
            DeploymentFilterTags,
            FlowFilter,
            FlowFilterName,
        )

        with get_client(sync_client=True) as client:
            deployments = client.read_deployments(
                flow_filter=FlowFilter(name=FlowFilterName(any_=[FLOW_NAME])),
                deployment_filter=DeploymentFilter(
                    tags=DeploymentFilterTags(all_=[self._tag(miroir)])
                ),
            )
        result = {}
        for deployment in deployments:
            limit = deployment.global_concurrency_limit
            result[deployment.name] = {
                "id": str(deployment.id),
                "attributes": {
                    "parameters": dict(deployment.parameters or {}),
                    "schedules": [s.schedule for s in deployment.schedules],
                    "work_pool_name": deployment.work_pool_name,
                    "job_variables": dict(deployment.job_variables or {}),
                    "concurrency_limit": limit.limit if limit else None,
                },
            }
        return result

    @_readable_errors
    def plan(self, miroir: ResolvedMiroir) -> list[dict[str, Any]]:
        """Changes needed for Prefect to match the miroir. Read-only."""
        desired, actual = self.desired(miroir), self.actual(miroir)
        changes: list[dict[str, Any]] = []
        for name, spec in sorted(desired.items()):
            want = _owned(spec)
            connection_id = spec["parameters"]["connection_id"]
            if name not in actual:
                changes.append(
                    {
                        "action": "create",
                        "deployment": name,
                        "connection_id": connection_id,
                        "set": want,
                    }
                )
                continue
            have = _owned(actual[name]["attributes"])
            diff = {
                key: {"have": have[key], "want": want[key]}
                for key in want
                if have[key] != want[key]
            }
            if diff:
                changes.append(
                    {
                        "action": "update",
                        "deployment": name,
                        "connection_id": connection_id,
                        "changes": diff,
                    }
                )
        for name in sorted(set(actual) - set(desired)):
            changes.append(
                {"action": "delete", "deployment": name, "id": actual[name]["id"]}
            )
        return changes

    @_readable_errors
    def apply(self, miroir: ResolvedMiroir, changes: list[dict[str, Any]]) -> None:
        """Perform a plan. Create and update are the same upsert in Prefect."""
        from prefect import get_client
        from prefect.deployments.runner import RunnerDeployment

        from src.pipelines.miroir import sync_connection

        desired = self.desired(miroir)
        for change in changes:
            if change["action"] == "delete":
                with get_client(sync_client=True) as client:
                    client.delete_deployment(uuid.UUID(change["id"]))
                continue
            spec = desired[change["deployment"]]
            if change["action"] == "update":
                self._drop_slugged_schedules(spec["name"])
            deployment = sync_connection.to_deployment(**spec)
            assert isinstance(deployment, RunnerDeployment)  # see the module docstring
            deployment.apply()

    def _drop_slugged_schedules(self, name: str) -> None:
        """Prefect updates slugged schedules in place instead of replacing them
        (and refuses an unslugged replacement), so hand-added ones would stay."""
        from prefect import get_client

        with get_client(sync_client=True) as client:
            deployment = client.read_deployment_by_name(f"{FLOW_NAME}/{name}")
            for schedule in deployment.schedules:
                if schedule.slug is not None:
                    client.delete_deployment_schedule(deployment.id, schedule.id)

    # ---- Execution --------------------------------------------------------------

    @_readable_errors
    def submit(self, operation_id: str) -> None:
        """Start the connection's deployment for a requested operation.

        The operation ID is the idempotency key: resubmitting after a timeout or
        crash returns the existing flow run instead of starting another. An
        operation that states its earliest start (`not_before`, a debounced
        derive run) is scheduled for then.
        """
        from prefect.client.schemas.objects import FlowRun
        from prefect.deployments import run_deployment
        from prefect.exceptions import ObjectNotFound

        operation = self.store.get_operation(operation_id)
        name = "/".join(
            (FLOW_NAME, deployment_name(operation.miroir_id, operation.connection_id))
        )
        not_before = json.loads(operation.options or "{}").get("not_before")
        try:
            flow_run = run_deployment(
                name,
                parameters={
                    "connection_id": operation.connection_id,
                    "operation_id": operation.id,
                    "config_path": self.config_path,
                },
                scheduled_time=datetime.fromisoformat(not_before)
                if not_before
                else None,
                timeout=0,
                idempotency_key=operation.id,
            )
        except ObjectNotFound as error:
            raise OperationError(
                f"Deployment {name} does not exist. Apply the deployments first"
                " (Apply deployments in the interface, or `miroir:deploy`)."
            ) from error
        assert isinstance(flow_run, FlowRun)  # see the module docstring
        self.store.bind_backend_run(operation.id, str(flow_run.id))

    @_readable_errors
    def observe(self, miroir: ResolvedMiroir) -> list[dict[str, Any]]:
        """Close attempts whose flow run ended without recording an outcome."""
        from prefect import get_client
        from prefect.client.schemas.objects import StateType
        from prefect.exceptions import ObjectNotFound

        closed = []
        with get_client(sync_client=True) as client:
            for item in self.store.unverified(miroir.id):
                try:
                    run = client.read_flow_run(uuid.UUID(item["backend_run_id"]))
                    state = run.state.type if run.state else None
                    label = run.state.name if run.state else "unknown"
                except ObjectNotFound:
                    state, label = StateType.CRASHED, "deleted"
                outcome: Literal["cancelled", "lost"]
                if state in (StateType.CANCELLED, StateType.CANCELLING):
                    outcome = "cancelled"
                elif state in (
                    StateType.CRASHED,
                    StateType.FAILED,
                    StateType.COMPLETED,
                ):
                    # Failed or completed while our attempt still says running: the
                    # run ended before it could record anything.
                    outcome = "lost"
                else:
                    continue  # scheduled, pending, running, paused: not ours to decide
                recorded = self.store.close_unverified(
                    item["operation_id"],
                    item["attempt_id"],
                    outcome,
                    f"Prefect flow run ended as {label} without recording an outcome",
                )
                if recorded:
                    closed.append({**item, "outcome": outcome, "state": label})
        return closed
