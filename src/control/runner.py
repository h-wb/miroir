"""Run one connection: register the operation, load with dlt, then build models.

Every execution path calls `run_connection`, so local runs, CLI requests, Prefect
schedules, and external (n8n) triggers all leave the same operation record.

A sync fetches from the connector's API and/or collects its watched folder: new
files become artifacts and are imported as child operations, then the analysis
is built once (decision 021).
"""

import json
import logging
import os
import subprocess
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy.exc import SQLAlchemyError

from src.control.analysis import (
    AnalysisPlan,
    analyses_linking,
    analysis_plan,
    find_analysis,
)
from src.control.config import ResolvedAnalysis, ResolvedConnection, ResolvedMiroir
from src.control.operations import (
    ArtifactOrigin,
    ConnectionBusy,
    MetadataStore,
    Operation,
    Origin,
)
from src.kernel.errors import OperationError

if TYPE_CHECKING:
    from src.media.store import ObjectStore

log = logging.getLogger(__name__)

REPO = Path(__file__).resolve().parents[2]
MAX_ERROR_LENGTH = 500
# How long a load waits for another load deciding the same analysis's build.
BUILD_DECISION_WAIT = 60
# How long a request waits for another deciding the same connection's derive run.
DERIVE_DECISION_WAIT = 10


@dataclass(frozen=True)
class RunOutcome:
    operation_id: str
    attempt: int
    ingestion: str
    transformation: str
    load_ids: tuple[str, ...]
    # False when the load finished but its outcome could not be written.
    recorded: bool = True


@dataclass(frozen=True)
class Execution:
    """Where and how operations run: the same for a sync and its child imports."""

    store: MetadataStore
    destination: Any
    resolve_credentials: Callable[[ResolvedConnection], dict[str, str]]
    transform: Callable[[AnalysisPlan], None] | None = None
    # dlt's default working directory (~/.dlt/pipelines) unless one is given.
    pipelines_dir: str | None = None
    lock_wait: float = 0
    artifact_root: Path | None = None
    # Starts a recorded build at a later time (decision 040). Without it every
    # analysis builds with the load, whatever its package's interval.
    defer: Callable[[str, datetime], None] | None = None
    # Where a derive run reads originals and writes thumbnails (decision 040).
    object_store: "ObjectStore | None" = None
    # Reads the configuration again, so a build that waited for the connection
    # lock or the dbt slot can check it is still the build it was requested as.
    reload: Callable[[], ResolvedMiroir] | None = None


class Superseded(OperationError):
    """The configuration changed while the build waited; it must not run."""


class IngestionFailed(RuntimeError):
    """The load failed; its redacted cause is on the attempt record."""


class AlreadyImported(OperationError):
    """The artifact was loaded before with the same parser and options."""

    def __init__(self, operation_id: str):
        super().__init__(f"Already imported by operation {operation_id}")
        self.operation_id = operation_id


def code_revision() -> str:
    """The deployed commit, from the environment or the checkout."""
    if revision := os.environ.get("MIROIR_CODE_REVISION"):
        return revision[:64]
    try:
        head = subprocess.run(
            ["git", "-C", str(REPO), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "-C", str(REPO), "status", "--porcelain", "--untracked-files=no"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return f"{head}+dirty" if dirty else head


def redact(error: BaseException, secrets: list[str]) -> tuple[str, str]:
    message = str(error)
    # Longest first, so a secret containing another is fully masked.
    for secret in sorted(secrets, key=len, reverse=True):
        message = message.replace(secret, "***")
    kind = type(error)
    return f"{kind.__module__}.{kind.__qualname__}"[:200], message[:MAX_ERROR_LENGTH]


def failure_kind(error: BaseException) -> str | None:
    """Classify a failure from its exception chain (dlt wraps source errors)."""
    from src.control.secrets import SecretKeyError, SecretResolutionError
    from src.integrations.errors import IntegrationError, is_rate_limited

    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, (SecretResolutionError, SecretKeyError)):
            return "credentials_missing"
        if isinstance(current, IntegrationError):
            return current.kind
        response = getattr(current, "response", None)
        if response is not None and is_rate_limited(response):
            return "rate_limited"
        if getattr(response, "status_code", None) in (401, 403):
            return "credentials_rejected"
        current = (
            current.__cause__
            or current.__context__
            or getattr(current, "exception", None)
        )
    return None


def request_models(
    miroir: ResolvedMiroir,
    analysis_id: str,
    *,
    store: MetadataStore,
    origin: Origin,
    full_refresh: bool = False,
) -> Operation:
    """Record a rebuild of one analysis from already loaded data.

    It runs through the first linked input's connection (its deployment and
    lock); nothing is fetched or loaded. `full_refresh` rebuilds incremental
    tables from scratch.
    """
    analysis = find_analysis(miroir, analysis_id)
    linked = list(analysis.inputs.values())
    status = store.analysis_status(
        miroir.id, analysis.id, [c.config.id for c in linked]
    )
    if status["state"] == "no_data":
        raise OperationError("Import or sync data before rebuilding analysis")
    return _new_operation(
        store,
        miroir,
        linked[0],
        origin,
        kind="models",
        analysis_id=analysis.id,
        options=json.dumps(
            {
                "analysis_receipt": analysis_plan(analysis).receipt,
                **({"full_refresh": True} if full_refresh else {}),
            }
        ),
    )


def request_derive(
    miroir: ResolvedMiroir,
    connection_id: str,
    *,
    store: MetadataStore,
    origin: Origin,
    submit: Callable[[Operation], None],
    backfill: dict[str, Any] | None = None,
    join: bool = True,
    still_waiting: Callable[[str], bool] | None = None,
) -> str | None:
    """Record a derive run of a connection's media and `submit` it, or join one.

    Confirmations share one run (decision 040): a request joins a derive run
    that has not started and is bound to its own backend run, since it starts
    after this request's files were queued. One due more than a delay ago (a long
    load may hold the deployment, or the run died before recording anything) is
    joined only if `still_waiting` says its backend run is still to run; without
    an answer it is not. Otherwise the request records one due
    `derive_delay_seconds` from now; a backfill requests a run regardless. Without `join` (a backfill, or the next batch of a run that
    stopped at its limit) the run is due now. Returns the new operation's ID, or
    None when joined. ConnectionBusy when another request is deciding, which then
    covers this one.
    """
    if miroir.media is None:
        raise OperationError("Media is not configured")
    connection = _connection(miroir, connection_id)
    delay = timedelta(seconds=miroir.media.derive_delay_seconds)
    with store.connection_lock(
        f"derive-request-{miroir.id}-{connection_id}", DERIVE_DECISION_WAIT
    ):
        now = datetime.now(timezone.utc)
        if join and any(
            _still_waiting(waiting, now - delay, still_waiting)
            for waiting in store.unstarted_derives(miroir.id, connection_id)
        ):
            return None
        options: dict[str, Any] = {
            "not_before": (now + delay if join else now).isoformat()
        }
        if backfill is not None:
            options["backfill"] = backfill
        operation = _new_operation(
            store,
            miroir,
            connection,
            origin,
            kind="derive",
            options=json.dumps(options, sort_keys=True),
        )
        # Inside the lock: a request that joins finds it submitted or closed.
        submit(operation)
        return operation.id


def _still_waiting(
    waiting: Operation,
    overdue: datetime,
    ask: Callable[[str], bool] | None,
) -> bool:
    """A bound derive run that will still start: one due recently is taken on
    trust, an overdue one only on the orchestrator's word."""
    if waiting.backend_run_id is None:
        return False
    due = json.loads(waiting.options or "{}").get("not_before")
    if due is not None and datetime.fromisoformat(due) > overdue:
        return True
    if ask is None:
        return False
    try:
        return ask(waiting.backend_run_id)
    except Exception:  # noqa: BLE001 - no answer: do not join
        log.warning("Could not ask whether a derive run is still waiting")
        return False


def _new_operation(
    store: MetadataStore,
    miroir: ResolvedMiroir,
    connection: ResolvedConnection,
    origin: Origin,
    kind: str = "sync",
    **values: Any,
) -> Operation:
    values.setdefault("backend_run_id", None)
    return store.create_operation(
        miroir_id=miroir.id,
        connection_id=connection.config.id,
        integration=connection.integration.name,
        kind=kind,
        origin=origin,
        pipeline_name=connection.binding.pipeline_name,
        dataset_name=connection.binding.dataset_name,
        config_revision=connection.revision,
        code_revision=code_revision(),
        **values,
    )


def request_sync(
    miroir: ResolvedMiroir,
    connection_id: str,
    *,
    store: MetadataStore,
    origin: Origin,
) -> Operation:
    """Record a sync request. It shows as requested until an executor runs it."""
    connection = _connection(miroir, connection_id)
    if not connection.can_sync:
        raise OperationError(
            "Nothing to sync: send or upload files, or configure a watched folder"
        )
    return _new_operation(store, miroir, connection, origin)


def request_import(
    miroir: ResolvedMiroir,
    connection_id: str,
    artifact_id: str,
    *,
    store: MetadataStore,
    origin: Origin,
    replay: bool = False,
    parent_operation_id: str | None = None,
) -> Operation:
    """Record an import of one artifact, or raise AlreadyImported.

    An artifact already imported with the same parser version and options is not
    imported again unless `replay` is set; replays are merge-safe because records
    keep their source keys. A failed attempt never marks the artifact imported.
    """
    connection = _connection(miroir, connection_id)
    artifact = store.get_artifact(miroir.id, artifact_id)
    if artifact["connection_id"] != connection.config.id:
        raise OperationError("Artifact belongs to a different connection")
    parser_version = _format(connection, artifact).version
    options = import_options(connection, artifact)
    if not replay and (previous := _imported_as_is(store, connection, artifact)):
        raise AlreadyImported(previous)
    return _new_operation(
        store,
        miroir,
        connection,
        origin,
        kind="replay" if replay else "import",
        artifact_id=artifact_id,
        parser_version=parser_version,
        options=json.dumps(options, sort_keys=True),
        parent_operation_id=parent_operation_id,
    )


def _imported_as_is(
    store: MetadataStore, connection: ResolvedConnection, artifact: dict[str, Any]
) -> str | None:
    """The succeeded import with today's parser version and options, if any."""
    parser_version = _format(connection, artifact).version
    options = import_options(connection, artifact)
    for previous in store.artifact_imports(artifact["id"]):
        if (
            previous["ingestion"] == "succeeded"
            and previous["parser_version"] == parser_version
            and previous["options"] == options
        ):
            return str(previous["operation_id"])
    return None


def execute_operation(
    miroir: ResolvedMiroir,
    operation_id: str,
    *,
    store: MetadataStore,
    destination: Any,
    resolve_credentials: Callable[[ResolvedConnection], dict[str, str]],
    backend_run_id: str | None = None,
    transform: Callable[[AnalysisPlan], None] | None = None,
    pipelines_dir: str | None = None,
    lock_wait: float = 0,
    artifact_root: Path | None = None,
    defer: Callable[[str, datetime], None] | None = None,
    object_store: "ObjectStore | None" = None,
    reload: Callable[[], ResolvedMiroir] | None = None,
) -> RunOutcome:
    """Run one recorded operation as a new attempt and record each stage.

    `lock_wait` lets queued requests wait for a running execution of the same
    connection instead of being blocked (several exports arriving together).

    Registration must already have succeeded. After a load commits, a metadata
    failure is logged and reported as `recorded=False`, never as a failed load. A
    dbt failure leaves ingestion succeeded and the analysis stale.

    The operation runs only against the connection configuration it was requested
    with: an edit saved in between blocks it instead of silently changing it.
    """
    run = Execution(
        store,
        destination,
        resolve_credentials,
        transform,
        pipelines_dir,
        lock_wait,
        artifact_root,
        defer,
        object_store,
        reload,
    )
    return _execute_operation(run, miroir, operation_id, backend_run_id)


def _execute_operation(
    run: Execution,
    miroir: ResolvedMiroir,
    operation_id: str,
    backend_run_id: str | None = None,
) -> RunOutcome:
    from src.control.artifacts import stored_copy_is_intact
    from src.control.sources import open_fetch, open_import

    store = run.store
    operation = store.get_operation(operation_id)
    if operation.miroir_id != miroir.id:
        raise OperationError("Operation belongs to a different miroir")
    connection = _connection(miroir, operation.connection_id)
    if backend_run_id is not None:
        store.bind_backend_run(operation.id, backend_run_id)

    if operation.kind == "derive":
        # Its work is the queue and the media settings, not the connection's
        # settings, so an edit since the request does not block it.
        return _derive(run, miroir, operation, connection)
    reason = None
    make_source: Callable[[dict[str, str]], Any] | None = None
    if operation.config_revision != connection.revision:
        reason = "Connection configuration changed since this operation was requested"
    elif operation.kind == "sync":
        if connection.config.folder is not None and run.artifact_root is None:
            reason = "No artifact storage configured for collecting the folder"
        elif connection.integration.fetch is not None:
            make_source = lambda credentials: open_fetch(connection, credentials)
    elif operation.kind == "models":
        analysis = next(
            (a for a in miroir.analyses if a.id == operation.analysis_id), None
        )
        if analysis is None:
            reason = "This analysis no longer exists"
        elif (
            json.loads(operation.options or "{}").get("analysis_receipt")
            != analysis_plan(analysis).receipt
        ):
            reason = (
                "Analysis settings or models changed since this rebuild was requested"
            )
    else:
        artifact = store.get_artifact(miroir.id, str(operation.artifact_id))
        options = json.loads(operation.options or "{}")
        if operation.parser_version != _format(connection, artifact).version:
            reason = "Parser version changed since this import was requested"
        elif artifact["removed_at"] is not None:
            reason = (
                "Retention removed the stored copy; send the file again to import it"
            )
        elif not stored_copy_is_intact(artifact):
            reason = "Stored artifact is missing or does not match its checksum"
        path, input_format = artifact["storage_path"], artifact["input_format"]
        make_source = lambda credentials: open_import(
            connection, input_format, path, options
        )
    if reason is not None:
        _block(store, operation, "Blocked", reason)
        raise OperationError(reason)
    if operation.kind == "sync" and connection.config.folder is not None:
        return _sync_with_folder(run, miroir, operation, connection, make_source)
    return _run_locked(run, miroir, operation, connection, make_source)


def run_connection(
    miroir: ResolvedMiroir,
    connection_id: str,
    *,
    store: MetadataStore,
    origin: Origin,
    operation_id: str | None = None,
    backend_run_id: str | None = None,
    **execution: Any,
) -> RunOutcome:
    """Sync one connection now, adopting a requested operation or a backend retry.

    Without an operation ID, a backend run that already has an operation (a
    Prefect retry) adds an attempt to it; anything else registers a new one.
    """
    connection = _connection(miroir, connection_id)
    if operation_id is None and backend_run_id is not None:
        existing = store.find_by_backend_run(backend_run_id)
        if existing is not None:
            operation_id = existing.id
    if operation_id is None:
        operation_id = _new_operation(
            store, miroir, connection, origin, backend_run_id=backend_run_id
        ).id
    elif store.get_operation(operation_id).connection_id != connection.config.id:
        raise OperationError("Operation belongs to a different connection")
    return execute_operation(
        miroir,
        operation_id,
        store=store,
        backend_run_id=backend_run_id,
        **execution,
    )


def import_artifact(
    miroir: ResolvedMiroir,
    connection_id: str,
    artifact_id: str,
    *,
    store: MetadataStore,
    origin: Origin,
    replay: bool = False,
    **execution: Any,
) -> RunOutcome:
    """Request and immediately execute an artifact import."""
    operation = request_import(
        miroir, connection_id, artifact_id, store=store, origin=origin, replay=replay
    )
    return execute_operation(miroir, operation.id, store=store, **execution)


def import_options(
    connection: ResolvedConnection, artifact: dict[str, Any]
) -> dict[str, Any]:
    """The file's own import options plus connection settings that change how
    every file of its format is read (Health's cutoff). Recorded per import."""
    file_format = _format(connection, artifact)
    return {**artifact["options"], **file_format.interpretation(connection.settings)}


def _format(connection: ResolvedConnection, artifact: dict[str, Any]) -> Any:
    try:
        return connection.integration.format(artifact["input_format"])
    except KeyError:
        raise OperationError(
            "This integration no longer reads that file format"
        ) from None


def collect_folder(
    miroir: ResolvedMiroir,
    connection: ResolvedConnection,
    *,
    store: MetadataStore,
    artifact_root: Path,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Register the watched folder's matching files as artifacts.

    Returns (artifacts, rejected file names). Unchanged files map to their
    existing artifact; invalid files are skipped and reported, never stored.
    A file still in the folder is not a new arrival: its artifact is neither
    copied again nor re-dated, and a copy retention removed stays removed.
    """
    from src.control.artifacts import file_sha256, submit_artifact
    from src.integrations.validation import InvalidArtifact

    folder = connection.config.folder
    assert folder is not None and folder.pattern is not None
    root = Path(folder.path)
    if not root.is_dir():
        raise OperationError("The watched folder does not exist or is not a folder")
    collected, rejected = [], []
    for path in sorted(p for p in root.glob(folder.pattern) if p.is_file()):
        origin: ArtifactOrigin = "folder"
        try:
            known = store.find_artifact(
                miroir.id, connection.config.id, file_sha256(path)
            )
            # A removed copy that must be read again (new cutoff, new parser) is
            # restored from the folder below; otherwise known content is reused.
            reusable = known is not None and (
                Path(known["storage_path"]).is_file()
                if known["removed_at"] is None
                else _imported_as_is(store, connection, known) is not None
            )
        except (OSError, OperationError):
            rejected.append(path.name)
            continue
        if reusable and known is not None:
            collected.append(known)
            continue
        try:
            artifact, _ = submit_artifact(
                miroir,
                connection,
                path,
                folder.options,
                store=store,
                root=artifact_root,
                origin=origin,
                input_format=folder.format,
            )
        except (InvalidArtifact, OperationError):
            rejected.append(path.name)
            continue
        collected.append(artifact)
    return collected, rejected


def prune_folder_artifacts(
    miroir: ResolvedMiroir, connection: ResolvedConnection, *, store: MetadataStore
) -> list[str]:
    """Remove stored copies of older folder files beyond `keep_last`.

    Only files that were imported successfully are removed; their receipts and
    history stay. Returns the removed artifact IDs.
    """
    folder = connection.config.folder
    if folder is None or folder.keep_last is None:
        return []
    candidates = sorted(
        (
            a
            for a in store.list_artifacts(miroir.id, connection.config.id)
            if a["origin"] == "folder" and a["removed_at"] is None
        ),
        key=lambda a: a["last_received_at"],
        reverse=True,
    )[folder.keep_last :]
    removed = []
    for artifact in candidates:
        if not any(
            i["ingestion"] == "succeeded"
            for i in store.artifact_imports(artifact["id"])
        ):
            continue
        Path(artifact["storage_path"]).unlink(missing_ok=True)
        store.mark_artifact_removed(miroir.id, artifact["id"])
        removed.append(artifact["id"])
    return removed


def _sync_with_folder(
    run: Execution,
    miroir: ResolvedMiroir,
    operation: Operation,
    connection: ResolvedConnection,
    make_source: Callable[[dict[str, str]], Any] | None,
) -> RunOutcome:
    """Fetch (if the connector can), import the folder's new files, build once.

    Each new file is its own import operation, linked to this sync. Imports
    take the connection lock themselves, so the fetch releases it first. A scan
    that loads nothing records ingestion `not_run` and builds nothing.
    """
    store = run.store
    assert run.artifact_root is not None  # a sync without one is blocked earlier
    # The fetch and the imports only load; the analysis is built once, below.
    loading = replace(run, transform=None, artifact_root=None)
    load_ids: list[str] = []
    with _locked(run, operation, connection):
        attempt, number = store.start_attempt(operation.id, "running")
        if make_source is not None:
            # A failed fetch is recorded on this attempt and ends the sync.
            fetched = _execute(
                loading, operation, attempt, number, miroir, connection, make_source
            )
            load_ids += fetched.load_ids
    failures: list[str] = []
    try:
        collected, rejected = collect_folder(
            miroir, connection, store=store, artifact_root=run.artifact_root
        )
    except (OSError, OperationError) as error:
        store.update_attempt(
            attempt,
            ingestion="failed",
            transformation="not_run",
            error_type=type(error).__name__,
            error_message=str(error)[:MAX_ERROR_LENGTH],
        )
        raise IngestionFailed(str(error)) from None
    failures += [f"{name}: not a valid file" for name in rejected]
    for artifact in collected:
        try:
            child = request_import(
                miroir,
                connection.config.id,
                artifact["id"],
                store=store,
                origin=operation.origin,
                parent_operation_id=operation.id,
            )
        except AlreadyImported:
            continue
        try:
            outcome = _execute_operation(loading, miroir, child.id)
        except (IngestionFailed, OperationError) as error:
            failures.append(f"{artifact['original_name']}: {error}")
            continue
        load_ids += outcome.load_ids
    prune_folder_artifacts(miroir, connection, store=store)
    message = "; ".join(failures)[:MAX_ERROR_LENGTH]
    if failures:
        # Files that did load are still modelled below.
        store.update_attempt(
            attempt,
            ingestion="failed",
            error_type="FolderImportFailed",
            error_message=message,
            load_ids=load_ids,
            **({} if load_ids else {"transformation": "not_run"}),
        )
    elif not load_ids:
        store.update_attempt(
            attempt,
            ingestion="not_run",
            transformation="not_run",
            transformation_detail="No new data",
        )
        return RunOutcome(operation.id, number, "not_run", "not_run", (), True)
    else:
        store.update_attempt(attempt, ingestion="succeeded", load_ids=load_ids)
    transformation, recorded = (
        _transform(run, operation, attempt, miroir, connection, [])
        if load_ids
        else ("not_run", True)
    )
    if failures:
        raise IngestionFailed(f"FolderImportFailed: {message}")
    return RunOutcome(
        operation.id,
        number,
        "succeeded",
        transformation,
        tuple(load_ids),
        recorded,
    )


def _connection(miroir: ResolvedMiroir, connection_id: str) -> ResolvedConnection:
    for connection in miroir.connections:
        if connection.config.id == connection_id:
            return connection
    raise OperationError("Unknown connection")


def _block(
    store: MetadataStore, operation: Operation, error_type: str, message: str
) -> None:
    """Record an attempt that never ran."""
    attempt, _ = store.start_attempt(operation.id, "blocked")
    store.update_attempt(attempt, error_type=error_type, error_message=message)


@contextmanager
def _locked(
    run: Execution, operation: Operation, connection: ResolvedConnection
) -> Iterator[None]:
    """Hold the connection's lock; a busy connection is a blocked attempt."""
    try:
        with run.store.connection_lock(connection.binding.pipeline_name, run.lock_wait):
            yield
    except ConnectionBusy as error:
        _block(run.store, operation, "ConnectionBusy", str(error))
        raise


def _record(store: MetadataStore, attempt: int, what: str, **values: Any) -> bool:
    """Write an outcome of work that is already done: a metadata failure is
    logged and reported as unrecorded, never raised."""
    try:
        store.update_attempt(attempt, **values)
    except SQLAlchemyError:
        log.exception("%s could not be recorded", what)
        return False
    return True


def _run_locked(
    run: Execution,
    miroir: ResolvedMiroir,
    operation: Operation,
    connection: ResolvedConnection,
    make_source: Callable[[dict[str, str]], Any] | None,
) -> RunOutcome:
    with _locked(run, operation, connection):
        attempt, number = run.store.start_attempt(
            operation.id, "not_run" if make_source is None else "running"
        )
        if make_source is None:
            transformation, recorded = _transform(
                run, operation, attempt, miroir, connection, []
            )
            return RunOutcome(
                operation.id, number, "not_run", transformation, (), recorded
            )
        return _execute(
            run, operation, attempt, number, miroir, connection, make_source
        )


def _execute(
    run: Execution,
    operation: Operation,
    attempt: int,
    number: int,
    miroir: ResolvedMiroir,
    connection: ResolvedConnection,
    make_source: Callable[[dict[str, str]], Any],
) -> RunOutcome:
    import dlt

    store = run.store
    secrets: list[str] = []
    try:
        credentials = run.resolve_credentials(connection)
        secrets = list(credentials.values())
        options: dict[str, Any] = (
            {"pipelines_dir": run.pipelines_dir} if run.pipelines_dir else {}
        )
        pipeline = dlt.pipeline(
            pipeline_name=connection.binding.pipeline_name,
            dataset_name=connection.binding.dataset_name,
            destination=run.destination,
            **options,
        )
        with make_source(credentials) as source:
            info = pipeline.run(source)
    except Exception as error:  # noqa: BLE001 - any failure is recorded, then raised
        error_type, message = redact(error, secrets)
        store.update_attempt(
            attempt,
            ingestion="failed",
            transformation="not_run",
            error_type=error_type,
            error_message=message,
            failure_kind=failure_kind(error),
        )
        raise IngestionFailed(f"{error_type}: {message}") from None

    load_ids = tuple(info.loads_ids)
    recorded = _record(
        store,
        attempt,
        f"Successful load {load_ids}",
        ingestion="succeeded",
        load_ids=list(load_ids),
    )
    transformation, models_recorded = _transform(
        run, operation, attempt, miroir, connection, secrets
    )
    return RunOutcome(
        operation.id,
        number,
        "succeeded",
        transformation,
        load_ids,
        recorded and models_recorded,
    )


def _transform(
    run: Execution,
    operation: Operation,
    attempt: int,
    miroir: ResolvedMiroir,
    connection: ResolvedConnection,
    secrets: list[str],
) -> tuple[str, bool]:
    """Build after a load: every analysis linking the connection, once each.

    A requested rebuild builds exactly its analysis on its own attempt. After a
    load, each linked analysis is a child `models` operation with its own
    attempt; the load attempt records a summary. A failed build never fails the
    load (decision 019). An analysis whose package states a minimum interval may
    be deferred instead (decision 040); a load that deferred every build records
    its transformation as skipped.
    """
    store = run.store
    if operation.kind == "models":
        analysis = find_analysis(miroir, str(operation.analysis_id))
        return _build(run, operation, attempt, analysis, secrets)
    analyses = analyses_linking(miroir, connection.config.id)
    skipped = None
    if not analyses:
        skipped = "No analysis links this connection"
    elif run.transform is None:
        skipped = "no transform configured"
    if skipped is not None:
        return "skipped", _record(
            store,
            attempt,
            "Transformation outcome",
            transformation="skipped",
            transformation_detail=skipped,
        )
    recorded = _record(store, attempt, "Transformation start", transformation="running")
    outcomes = []
    for analysis in analyses:
        decision = _deferred(run, operation, miroir, connection, analysis)
        if decision is True:
            outcomes.append((analysis.id, "deferred"))
            continue
        try:
            child, child_attempt = (
                decision
                if isinstance(decision, tuple)
                else _register_build(store, miroir, connection, operation, analysis)
            )
        except SQLAlchemyError:
            log.exception("Build of %s could not be registered", analysis.id)
            outcomes.append((analysis.id, "failed"))
            recorded = False
            continue
        outcome, ok = _build(run, child, child_attempt, analysis, secrets)
        outcomes.append((analysis.id, outcome))
        recorded = recorded and ok
    # Deferred and blocked builds did not run; neither is a failed build.
    built = [o for _, o in outcomes if o not in ("deferred", "blocked")]
    if not built:
        transformation = "skipped"
    elif all(o == "succeeded" for o in built):
        transformation = "succeeded"
    else:
        transformation = "failed"
    summary = ", ".join(
        f"{analysis_id} {outcome}"
        + (" (its settings changed while it waited)" if outcome == "blocked" else "")
        for analysis_id, outcome in outcomes
    )
    ok = _record(
        store,
        attempt,
        "Transformation outcome",
        transformation=transformation,
        transformation_detail=f"Analyses: {summary}"[:200],
    )
    return transformation, recorded and ok


def _register_build(
    store: MetadataStore,
    miroir: ResolvedMiroir,
    connection: ResolvedConnection,
    operation: Operation,
    analysis: ResolvedAnalysis,
) -> tuple[Operation, int]:
    """Record a load's build of one analysis as a started child operation."""
    child = _new_operation(
        store,
        miroir,
        connection,
        operation.origin,
        kind="models",
        analysis_id=analysis.id,
        parent_operation_id=operation.id,
        options=json.dumps({"analysis_receipt": analysis_plan(analysis).receipt}),
    )
    child_attempt, _ = store.start_attempt(child.id, "not_run")
    return child, child_attempt


def _deferred(
    run: Execution,
    operation: Operation,
    miroir: ResolvedMiroir,
    connection: ResolvedConnection,
    analysis: ResolvedAnalysis,
) -> bool | tuple[Operation, int]:
    """Leave this load's build to one at the end of the package's interval.

    The load joins a scheduled build that has not started, or, inside the
    interval after the latest build started, schedules one at its end. False
    (build now, as without an interval) whenever that cannot be done safely: a
    redundant build costs time, a missing one leaves the analysis stale. A build
    due now is registered under the decision's lock, so a load deciding next sees
    it started; it is returned with its attempt.
    """
    interval = analysis.package.min_build_interval
    if interval is None or run.defer is None:
        return False
    store = run.store
    receipt = analysis_plan(analysis).receipt
    try:
        with store.connection_lock(
            f"analysis-build-{analysis.id}", BUILD_DECISION_WAIT
        ):
            now = datetime.now(timezone.utc)
            revisions = {c.config.id: c.revision for c in miroir.connections}
            for scheduled in store.unstarted_builds(miroir.id, analysis.id):
                options = json.loads(scheduled.options or "{}")
                # Not started, so it starts after this load committed. One that
                # would be blocked, or was due longer than an interval ago and
                # may never run, is not joined.
                # One with no flow run of its own was never scheduled.
                if (
                    scheduled.backend_run_id is not None
                    and options.get("analysis_receipt") == receipt
                    and revisions.get(scheduled.connection_id)
                    == scheduled.config_revision
                    and "not_before" in options
                    and datetime.fromisoformat(options["not_before"]) > now - interval
                ):
                    return True
            started = store.last_build_started(miroir.id, analysis.id)
            if started is None or now >= started + interval:
                return _register_build(store, miroir, connection, operation, analysis)
            due = started + interval
            build = _new_operation(
                store,
                miroir,
                connection,
                operation.origin,
                kind="models",
                analysis_id=analysis.id,
                parent_operation_id=operation.id,
                options=json.dumps(
                    {"analysis_receipt": receipt, "not_before": due.isoformat()}
                ),
            )
            try:
                run.defer(build.id, due)
            except Exception as error:  # noqa: BLE001 - build now instead
                log.warning(
                    "Build of %s could not be scheduled: %s",
                    analysis.id,
                    type(error).__name__,
                )
                _block(
                    store,
                    build,
                    "NotScheduled",
                    "Could not be scheduled; the load built the analysis instead",
                )
                return False
            return True
    except (ConnectionBusy, SQLAlchemyError):
        log.warning("Build of %s could not be deferred; building now", analysis.id)
        return False


def _still_requested(run: Execution, operation: Operation) -> None:
    """Raise Superseded unless the analysis read now still has this build's
    receipt. The receipt is everything the build applies (selector, variables
    with the raw bindings and parameters, relations, version), so a connection
    edit that leaves it unchanged does not stop a build: a load's own build runs
    after an edit saved during the load. Whether a request still matches its
    connection's revision is checked once, when it starts (_execute_operation)."""
    if run.reload is None:
        return
    try:
        miroir = run.reload()
    except Exception as error:  # noqa: BLE001 - unverifiable means not run
        raise Superseded(
            f"The configuration could not be read again: {type(error).__name__}"
        ) from None
    analysis = next((a for a in miroir.analyses if a.id == operation.analysis_id), None)
    receipt = json.loads(operation.options or "{}").get("analysis_receipt")
    if analysis is None or analysis_plan(analysis).receipt != receipt:
        raise Superseded("Analysis settings changed while this build waited to run")


def _build(
    run: Execution,
    operation: Operation,
    attempt: int,
    analysis: ResolvedAnalysis,
    secrets: list[str],
) -> tuple[str, bool]:
    """Run one analysis build and record it on `attempt`.

    Checked again once the connection lock and then the dbt slot are held: a
    build whose analysis settings changed while it waited ends as blocked.
    """
    full_refresh = bool(json.loads(operation.options or "{}").get("full_refresh"))
    plan = replace(
        analysis_plan(analysis),
        full_refresh=full_refresh,
        ready=lambda: _still_requested(run, operation),
    )
    detail = plan.receipt
    recorded = True
    if run.transform is None:
        transformation, detail = "skipped", "no transform configured"
    else:
        recorded = _record(
            run.store, attempt, "Transformation start", transformation="running"
        )
        try:
            _still_requested(run, operation)
            run.transform(plan)
            transformation = "succeeded"
        except Superseded as error:
            ok = _record(
                run.store,
                attempt,
                "Blocked build",
                ingestion="blocked",
                transformation="not_run",
                error_type="Blocked",
                error_message=str(error),
            )
            return "blocked", recorded and ok
        except Exception as error:  # noqa: BLE001 - dbt must not fail the load
            transformation = "failed"
            detail = ": ".join(redact(error, secrets))[:200]
            log.warning("Analysis %s failed: %s", analysis.id, detail)
    ok = _record(
        run.store,
        attempt,
        "Transformation outcome",
        transformation=transformation,
        transformation_detail=detail,
    )
    return transformation, recorded and ok


def _derive(
    run: Execution,
    miroir: ResolvedMiroir,
    operation: Operation,
    connection: ResolvedConnection,
) -> RunOutcome:
    """Thumbnails and `media_objects` rows for queued files (decision 040).

    One run at a time per connection. A run handles at most `derive_batch` files
    per kind; when more wait, it schedules the next run where it can (Prefect),
    and otherwise carries on itself. The outcome is a transformation: nothing
    of the connection's own data is loaded, so its status is unchanged.
    """
    from src.media.derive import MediaWriter, derive_media

    store = run.store
    if miroir.media is None or run.object_store is None:
        reason = "Media storage is not configured where this runs"
        _block(store, operation, "Blocked", reason)
        raise OperationError(reason)
    object_store = run.object_store
    target = connection.media_target(miroir.media)
    pipeline_name = connection.binding.pipeline_name

    def batch(backfill: dict[str, Any] | None) -> Any:
        writer = MediaWriter(
            target,
            run.destination,
            run.pipelines_dir,
            # The connection's own lock, around each write to its dataset.
            lambda: store.connection_lock(pipeline_name, run.lock_wait),
        )
        return derive_media(
            target,
            queue=store.media_queue,
            object_store=object_store,
            writer=writer,
            backfill=backfill,
        )

    try:
        with store.connection_lock(
            f"{connection.binding.pipeline_name}-media", run.lock_wait
        ):
            attempt, number = store.start_attempt(operation.id, "not_run")
            store.update_attempt(attempt, transformation="running")
            try:
                summary = batch(json.loads(operation.options or "{}").get("backfill"))
                # Without a scheduler, nothing else would take the next batch.
                while summary.remaining and run.defer is None and summary.progressed:
                    summary = summary.then(batch(None))
            except Exception as error:  # noqa: BLE001 - recorded, then raised
                error_type, message = redact(error, [])
                store.update_attempt(
                    attempt,
                    transformation="failed",
                    transformation_detail=f"{error_type}: {message}"[:200],
                    error_type=error_type,
                    error_message=message,
                )
                raise IngestionFailed(f"{error_type}: {message}") from None
    except ConnectionBusy as error:
        _block(store, operation, "ConnectionBusy", str(error))
        raise
    recorded = _record(
        store,
        attempt,
        "Derive outcome",
        transformation="succeeded",
        transformation_detail=summary.detail()[:200],
    )
    # Only after progress: a run that changed nothing would schedule itself forever.
    if summary.remaining and summary.progressed and run.defer is not None:
        _continue_derive(run, miroir, operation)
    return RunOutcome(operation.id, number, "not_run", "succeeded", (), recorded)


def _continue_derive(
    run: Execution, miroir: ResolvedMiroir, operation: Operation
) -> None:
    """Schedule the next batch now, behind whatever else waits for the deployment."""
    defer = run.defer
    assert defer is not None

    def schedule(next_run: Operation) -> None:
        try:
            defer(next_run.id, datetime.now(timezone.utc))
        except Exception:
            # Closed, so that no confirmation joins a run that will never start.
            _block(run.store, next_run, "NotScheduled", "Could not be scheduled")
            raise

    try:
        request_derive(
            miroir,
            operation.connection_id,
            store=run.store,
            origin=operation.origin,
            submit=schedule,
            join=False,
        )
    except Exception as error:  # noqa: BLE001 - the queue keeps the files
        log.warning(
            "The next derive run could not be scheduled: %s", type(error).__name__
        )
