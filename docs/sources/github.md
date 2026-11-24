---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-24
summary: What the GitHub connector fetches from the API, how its models join the API rows to an older account export, and what "mine" means in them.
topics:
  - "[[Code activity]]"
  - "[[Warehouse]]"
created: 2026-09-24T22:01
---

# GitHub

My GitHub footprint: everything in the repos I own (any author, bots included)
and my activity in other people's repos.

- **Connection**: integration `github`, fetched on the connection's schedule;
  the installation's is `github`, bound to `github_data`
- **Code**: [`src/integrations/github/`](../../src/integrations/github/), one
  module per resource in `resources/` (decision 028)
- **Packages**: `github` (`commits`, `pull_requests`, `issues`, `issue_comments`,
  `releases`, `contributions`)
- **Dashboard**: GitHub (not checked here)

## Extraction

Records are loaded as the API returns them. Account level:

| Resource | Endpoint | Key | Notes |
| --- | --- | --- | --- |
| `repos` | `/user/repos` (owner, collaborator, organisation member) | `id` | |
| `stars` | `/user/starred` (`star+json` for `starred_at`) | `repo__id` | Merged: an unstar keeps its row |
| `events` | `/users/{u}/events` | `id` | GitHub keeps about 90 days or 300 events, so the daily run is the archive; `payload` is one JSON column |
| `contributions_daily` | GraphQL `contributionsCollection`, one year per query | `date` | The profile's heatmap |

Repo level, for every repo I own, forks included (`owned_repositories` is a
supporting resource that loads nothing):

| Resource | Endpoint | Key | Incremental |
| --- | --- | --- | --- |
| `commits` | `/repos/{r}/branches`, then `/repos/{r}/commits?sha=<branch>&since=`, then `/repos/{r}/commits/{sha}` for line stats (file patches dropped) | `html_url` | Run windows |
| `pull_requests` | `/repos/{r}/pulls?state=all&sort=updated` | `html_url` | `updated_at` |
| `issues` | `/repos/{r}/issues?since=` (also lists pull requests; the models drop them) | `html_url` | `updated_at` |
| `issue_comments` | `/repos/{r}/issues/comments?since=` | `html_url` | `updated_at` |
| `releases` | `/repos/{r}/releases` | `html_url` | Full every run |

My activity in other people's repos, through search (paced; a date range is
split in half past the 1,000-result cap):

| Resource | Query |
| --- | --- |
| `external_commits` | `author:<me> -user:<me>` on `/search/commits` (default branches only), then `/commits/{sha}` for stats |
| `external_issues` | `is:issue` and `is:pull-request` `author:<me> -user:<me>` on `/search/issues` |

Every incremental read re-reads three days, so late merges and edits are merged.
Commits and searches keep run windows rather than a cursor on record dates, since
an old-dated commit can be pushed or indexed later; the other cursors are dlt's
own, seeded once from the old watermarks (decision 028). A branch pushed or a
pull request merged more than three days after its dates can still be missed.

## The account export

History before the API came from a GitHub account-data export, loaded once into
raw `export_*` tables (`export_commits` from `git log --all --numstat` of every
bundled repo, `export_pull_requests`, `export_issues`, `export_issue_comments`,
`export_releases`, `export_bots`) by the legacy flow, retired by decision 025.
The tables stay and the models still read them, as optional inputs: per record
type they union export and API on the web URL, the API row winning and the export
filling fields the API lacks. No code loads an export today; a GitHub archive
file format would (decision 021).

## What "mine" means

- `is_mine` on commits: the author email is in the analysis parameter
  `author_emails` (set in the interface, kept out of this public repo), or the
  author login is mine, or a commit with no email names my login as its author.
  Git emails are often not linked to the account, so many commits match only
  this way.
- `is_bot`: GitHub user type `Bot`, a `[bot]` suffix, or the export's bot list.
- `is_fork_repo`: forks carry upstream history; filter on
  `is_mine or not is_fork_repo`.

## Settings and credentials

Setting `username`, the account that owns the token; a check confirms the token
belongs to it. Credential `access_token`: a fine-grained personal access token
on all own repos, read-only (Contents, Metadata, Issues, Pull requests;
account: Starring), in Miroir's store or as the fnox reference
`GITHUB__ACCESS_TOKEN`. Fine-grained tokens see neither private events nor
repos owned by others.

## Quirks

- Commit search only indexes default branches; own repos use the branch walk.
- Issue search requires `is:issue` or `is:pull-request` in the query (422
  otherwise).
- `contributions_daily.date` arrives as text; `contributions` casts it.
- The heatmap card is a native-SQL table with fixed week columns: Metabase
  cannot colour pivot cells.
