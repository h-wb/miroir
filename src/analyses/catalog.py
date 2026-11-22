"""Analysis packages (decision 022); SQL dependencies remain owned by dbt.

A package declares its selector, outputs, parameters, and the inputs it reads:
named roles, each accepting certain integrations through one dbt source. An
analysis in the configuration links each role to a connection.
"""

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from pydantic import Field, field_validator

from src.core.models import StrictModel


class NoParameters(StrictModel):
    pass


class GitHubParameters(StrictModel):
    author_emails: list[str] = Field(
        default_factory=list,
        title="Your commit author emails",
        description="One email per line. Helps identify commits whose email is not "
        "linked to your GitHub account.",
    )

    @field_validator("author_emails")
    @classmethod
    def emails(cls, values: list[str]) -> list[str]:
        if any(
            "@" not in v
            or not v.strip()
            or len(v) > 254
            or any(c in v for c in "\n\r\x00")
            for v in values
        ):
            raise ValueError("expected email addresses")
        return sorted({v.strip().lower() for v in values})


# Lower-case unquoted identifiers only: dbt writes grantees into GRANT unquoted.
_ROLE = re.compile(r"[a-z_][a-z0-9_]{0,62}")
_RESERVED_ROLES = {"public", "current_role", "current_user", "session_user", "all"}


class TimelineParameters(StrictModel):
    reader_role: str | None = Field(
        default=None,
        title="Server's read-only role",
        description="The warehouse role the Miroir server reads the timeline with "
        "(for example miroir_timeline). Builds grant it read access to the "
        "timeline's tables; empty grants nothing.",
    )

    @field_validator("reader_role", mode="before")
    @classmethod
    def empty_is_none(cls, value: Any) -> Any:
        return None if isinstance(value, str) and not value.strip() else value

    @field_validator("reader_role")
    @classmethod
    def role_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if (
            not _ROLE.fullmatch(value)
            or value in _RESERVED_ROLES
            or value.startswith("pg_")
        ):
            raise ValueError(
                "expected a lower-case role name: letters, digits and underscores"
            )
        return value


@dataclass(frozen=True)
class InputSpec:
    """One role a package reads: which integrations fill it, via which dbt source."""

    title: str
    integrations: tuple[str, ...]
    dbt_source: str
    required: bool = True


def _no_variables(inputs: Mapping[str, Any], parameters: Any) -> dict[str, Any]:
    return {}


def _timeline_variables(
    inputs: Mapping[str, Any], parameters: TimelineParameters
) -> dict[str, Any]:
    return {"timeline_reader_role": parameters.reader_role}


def _github_variables(
    inputs: Mapping[str, Any], parameters: GitHubParameters
) -> dict[str, Any]:
    # Account identity is an input to the GitHub analysis, never a global env override.
    return {
        "github_login": inputs["github"].username,
        "github_author_emails": parameters.author_emails,
    }


@dataclass(frozen=True)
class Package:
    id: str
    title: str
    description: str
    selector: str
    group: str
    relations: tuple[str, ...]
    inputs: Mapping[str, InputSpec]
    parameters: type[StrictModel] = NoParameters
    version: int = 1
    # dbt variables from the linked inputs' settings (by role) and validated
    # parameters; unlinked optional roles are absent.
    bind: Callable[[Mapping[str, Any], Any], dict[str, Any]] = _no_variables
    # Loads inside this interval after a build coalesce into one build at its end
    # (decision 040). None builds after every load.
    min_build_interval: timedelta | None = None
    # Pass linked connection IDs by role (`miroir_connections`) to the models.
    # Off by default so other packages' plan receipts do not change.
    connection_ids: bool = False

    def variables(
        self, settings_by_role: Mapping[str, Any], parameters: dict[str, Any]
    ) -> dict[str, Any]:
        return self.bind(settings_by_role, self.parameters.model_validate(parameters))

    def describe(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "relations": list(self.relations),
            "inputs": {
                role: {
                    "title": spec.title,
                    "integrations": list(spec.integrations),
                    "required": spec.required,
                }
                for role, spec in self.inputs.items()
            },
            "parameters_schema": self.parameters.model_json_schema(),
        }


def _single(role: str, title: str, integration: str) -> Mapping[str, InputSpec]:
    return {role: InputSpec(title, (integration,), integration)}


PACKAGES: Mapping[str, Package] = {
    p.id: p
    for p in (
        Package(
            "music",
            "Music analysis",
            "Listening history: daily totals, sessions, artists, streaks.",
            "+path:models/music",
            "music",
            (
                "daily",
                "listens",
                "artists",
                "sessions",
                "monthly_top",
                "streaks",
                "on_repeat",
            ),
            _single("music", "Listening history", "listenbrainz"),
        ),
        Package(
            "health",
            "Health analysis",
            "Daily activity, heart, and sleep; workouts.",
            "+path:models/health",
            "health",
            ("daily_summary", "workout_summary"),
            _single("health", "Health exports", "apple_health"),
        ),
        Package(
            "github",
            "GitHub analysis",
            "Commits, pull requests, issues, releases, and contributions.",
            "+path:models/github",
            "github",
            (
                "commits",
                "pull_requests",
                "issues",
                "issue_comments",
                "releases",
                "contributions",
            ),
            _single("github", "GitHub account", "github"),
            GitHubParameters,
            bind=_github_variables,
        ),
        Package(
            "biking",
            "Bike share analysis",
            "Rides, stations, and routes.",
            "+path:models/biking",
            "biking",
            ("rides", "stations", "routes"),
            _single("biking", "Bike share account", "bikeshare"),
        ),
        Package(
            "browser",
            "Browser analysis",
            "Visits and time spent per site.",
            "+path:models/browser",
            "browser",
            ("visits", "domains_daily"),
            _single("browser", "Browser history", "browser_history"),
        ),
        Package(
            "messages",
            "Messages analysis",
            "Conversations, messages, reactions, participants, attachments, and "
            "daily totals; reply times in direct conversations. No message text.",
            "+path:models/messages",
            "messages",
            (
                "conversations",
                "messages",
                "reactions",
                "participants",
                "attachments",
                "daily",
            ),
            _single("messages", "Apple Messages", "apple_messages"),
        ),
        Package(
            "crossovers",
            "Across sources",
            "Days, nights, listens, and rides combined: what was playing during "
            "workouts and rides, late listening against sleep, correlations, and a "
            "year in review. Music and biking are optional; without them their "
            "measures are unknown, not zero.",
            "+path:models/crossovers",
            "crossovers",
            ("daily", "listens", "rides", "nights", "correlations", "years"),
            {
                "health": InputSpec(
                    "Health exports", ("apple_health",), "apple_health"
                ),
                "music": InputSpec(
                    "Listening history", ("listenbrainz",), "listenbrainz", False
                ),
                "biking": InputSpec(
                    "Bike share account", ("bikeshare",), "bikeshare", False
                ),
            },
        ),
        Package(
            "timeline",
            "Timeline",
            "Screen frames, app sessions and visits on one time axis, for the "
            "Miroir server's timeline. Link at least one input.",
            "+path:models/timeline",
            "timeline",
            ("screen_frames", "events", "days", "apps"),
            {
                "screen": InputSpec(
                    "Screen text and app usage",
                    ("screenmemory",),
                    "screenmemory",
                    False,
                ),
                "browser": InputSpec(
                    "Browser history", ("browser_history",), "browser_history", False
                ),
            },
            TimelineParameters,
            bind=_timeline_variables,
            min_build_interval=timedelta(minutes=5),
            connection_ids=True,
        ),
    )
}
