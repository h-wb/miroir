---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-24
summary: The analysis packages, what each reads and builds, and the tables of the messages and timeline packages in full.
topics:
  - "[[Warehouse]]"
  - "[[Dashboards]]"
created: 2026-11-24T23:30
---

# Analyses

An analysis is a configured instance of a package: it links a connection to each
of the package's input roles, has a time zone and typed parameters, and builds
the package's dbt models into schemas of its own (decision 022). Several
analyses may read one connection. Dashboards read these schemas (decision 042);
nothing builds the models outside an analysis.

## Packages

From `src/analyses/catalog.py`. Each selects `+path:models/<group>` in `dbt/`.

| Package | Inputs | Builds |
| --- | --- | --- |
| `music` | `music`: ListenBrainz | `daily`, `listens`, `artists`, `sessions`, `monthly_top`, `streaks`, `on_repeat` |
| `health` | `health`: Apple Health | `daily_summary`, `workout_summary` |
| `github` | `github`: GitHub; parameter `author_emails` | `commits`, `pull_requests`, `issues`, `issue_comments`, `releases`, `contributions` |
| `biking` | `biking`: Bike Share | `rides`, `stations`, `routes` |
| `browser` | `browser`: browser history | `visits`, `domains_daily` |
| `messages` | `messages`: Apple Messages; at most every 10 minutes | Below |
| `crossovers` | `health`: Apple Health; `music` and `biking` optional | `daily`, `listens`, `rides`, `nights`, `correlations`, `years` |
| `timeline` | `screen`: ScreenMemory, `browser`: browser history, both optional, one required; parameter `reader_role`; at most every 5 minutes | Below |

A new connection gets its integration's recommended analysis (all but
ScreenMemory have one). An analysis reads its inputs' actual bindings and writes
`miroir_<analysis>_<identity>_<group>` schemas; the analysis page lists the exact
relations. An input that is not linked is empty and its measures are null,
including counts; it never falls back to another dataset or account. A linked
input with no successful load shows on the analysis page. Health dates keep the
exported local date; the analysis's time zone sets local music and biking days
and hours. Absent optional raw fields are typed nulls; a table that lacks a
required key or time fails the build clearly.

On the overview, **Add an analysis** picks a package and links each input. An
analysis page changes inputs, time zone and parameters, rebuilds, shows outputs
and build history, or removes the configuration; removing it keeps its tables
and history. A connection cannot be removed while another analysis links it; its
own default analysis goes with it.

```bash
uv run --locked pipelines-miroir --config miroir.json analyses
uv run --locked pipelines-miroir --config miroir.json models together [--full-refresh]
```

How builds are triggered, serialised and judged current: [Connections and
runs](runs.md#analysis-builds).

### Dashboards

`dashboards` on an analysis and `links.metabase` give the interface its links.
Miroir does not create or reconcile Metabase assets.
[`examples/analysis-dashboards.json`](../../examples/analysis-dashboards.json)
holds SQL and chart seeds with logical `{health}` and `{music}` schema
placeholders: resolve them against the analysis's output schema, create the
assets in Metabase, and store their IDs on the analysis.

## The messages package

Reads one Apple Messages connection (decision 041) and builds six tables in its
`…_messages` schema, each a full build. The tables hold no message text, only
lengths and flags. Names come from the connection's `contacts` when the collector
sends them; without them a person is their number or address as written.

A person is a number or an address, compared the way the collector matches
contacts: an address in lower case, a number by its digits with a leading 1
dropped from eleven. So `Quinn@Example.com` and `quinn@example.com`, or
`5555550113` and `+15555550113`, are one person; `handle` and `sender_handle`
hold that form. A time before 2001-01-02 (Apple's epoch, a missing date) is no
time: such a message has no `sent_at`, counts in its conversation's totals, and
is absent from `daily`, from first and last times and from reply times.

- **conversations**: one row per conversation. A direct conversation is its
  person, so one person over iMessage, SMS and RCS, or the same chat seen from
  two Macs, is one row; a group is its group id. `conversation_key`,
  `display_name` (a group's title, else its participants' names, else `Group`; a
  direct conversation's contact name, else its identifier as written),
  `is_group`, `participants`, `first_message_at`, `last_message_at`,
  `messages_sent`, `messages_received`, `attachments`, `services` (for example
  `SMS, iMessage`).
- **messages**: one row per message, not reactions, group events, calls or poll
  votes. `guid`, `conversation_key` (null for the few messages in no chat),
  `is_group`, `sent_at`, local `day`, `hour` and `weekday` (1 is Monday),
  `is_from_me`, `sender_handle` and `sender_name` (null for my own, and when
  Apple stores no handle), `service`, `has_text` and `text_length` (null when
  the text could not be read), `attachments`, `is_thread_reply`, `is_edited`,
  `is_unsent`, `is_audio`, and `reply_minutes`: in a direct conversation, on the
  first message after the other side's run of messages, the minutes since the
  first message of that run, null beyond 24 hours and on every other message.
- **reactions**: one row per reaction that stands. A removal cancels the tapback
  it removes; a person's changed tapback counts once, as its latest kind; each
  sticker is its own row, and a sticker removal cancels none. `guid`,
  `conversation_key`, `sent_at`, `day`, `hour`, `kind` (`love`, `like`,
  `dislike`, `laugh`, `emphasis`, `question`, `emoji`, `sticker`), `is_from_me`,
  `sender_handle`, `sender_name`, `target_guid` (the message it reacts to) and
  `target_part` (which part of it, 0 for the first).
- **participants**: one row per conversation and person other than me: everyone
  listed in its chats and everyone who sent in it. `conversation_key`, `handle`,
  `display_name`, `has_contact_name`, `messages_sent`, `first_message_at`,
  `last_message_at`.
- **attachments**: one row per attachment of a message. `attachment_guid`,
  `message_guid`, `conversation_key`, `sent_at`, `day`, `is_from_me`, `kind`
  (`image`, `video`, `audio`, `document`, `other`, from the mime type or the
  extension), `mime_type`, `extension`, `bytes`, `has_media_key` (whether the
  file has a name to be stored under). Link previews are left out.
- **daily**: one row per local day from the first dated message or reaction to
  the last, quiet days included. `day`, `weekday`, `messages_sent`,
  `messages_received`, `conversations_active`, `attachments`, `reactions`.

## The timeline package

Builds the tables the [timeline](timeline.md) reads (decision 040). Inputs
`screen` (a ScreenMemory connection) and `browser` (a browser history
connection), both optional; link at least one.

```json
{"id": "timeline", "package": "timeline",
 "inputs": {"screen": "screen", "browser": "browser"},
 "timezone": "Europe/Paris",
 "parameters": {"reader_role": "miroir_timeline"}}
```

It writes these relations to `miroir_<analysis>_<identity>_timeline`:

| Relation | What |
| --- | --- |
| `screen_frames` | One row per frame: `captured_at`, `bundle_id` (the app usage row of the same second, or null), `text` (null when withheld), `withheld`, `masked`, `image_path`, and what the worker recorded of its screenshot: `media_status` (the thumbnail status), `thumbnail_w360`, `thumbnail_w1280`, all null while there is no record, which means unknown, not absent; and `media_withheld`, whether the frame is withheld, which wins over any record. Screen text is indexed only here: search with `to_tsvector('simple', text)`, the indexed expression |
| `events` | One row per event, the same columns for every source: `source`, `connection`, `event_id`, `started_at`, `ended_at`, `kind`, `title`, `body`, `media_kind`, `media_key`, `media_status`, `thumbnail_w360`, `thumbnail_w1280`, `group_key`, `payload`, `search`. The screen gives one row per app session (`kind` `app_session`, `group_key` the bundle, the media columns its first frame's), the browser one per visit (`group_key` the site). `search` weights the title over the body, `simple` configuration |
| `days` | Per local day, in the analysis's time zone, and source: `events` that started that day and, for the screen, `frames` |
| `apps` | Bundle identifier to `name` and `colour`, for a list of common apps and every app seen; others are named from their identifier, with a palette colour |

An app session is a run of frames of one app with no pause over five minutes.
Frames without an app neither start nor end one. Redirects, reloads, embeds and
framed links are not visits. Columns starting with `_` are the builds' own
bookkeeping. `timeline_screen` and `timeline_browser` are the two sources' own
models and are not meant to be read.

### How it builds

Each build is incremental. It reads only rows of loads that dlt lists as
complete in `_dlt_loads`, the first build too, and remembers the latest
completion time it has read; each build reads the loads completed after that
time less ten minutes, so a load that completes after a later one is still read.
A load that never completes is never read.

- A resent frame replaces its row. An app usage row that arrives after its frame
  gives the frame its app and moves the sessions around it.
- A session still open at the last build grows. A page's new title reaches its
  visits.
- A screenshot the worker records in `media_objects` after its frame was built
  reaches the frame and its session at the next build; only the frames of the
  records written since are read again. A record written before its frame is
  found when the frame arrives.
- A frame or visit that moves to another time leaves its old session and day
  (for visits, when Places reuses the IDs of cleared visits). The old times stay
  on the row until every model that recounts them has read it, so a build that
  fails part way loses none.
- The result is what a first build over the same rows gives.
- A row that still has no `media_withheld` (tables from before the media columns,
  or an upgrade that did not finish) makes the next build read every frame and
  session once. A row with nothing behind it (its raw frame or load gone) is
  settled at the end of that build with an unknown media state, so it does not
  make every later build read everything.
- Each model builds in one transaction: a failed build leaves its tables as they
  were. New rows are staged in an unlogged table of the timeline's schema, not a
  temporary table, so large reads do not depend on `temp_buffers`.
- Tested on PostgreSQL 17 and 18. A rebuild from scratch builds new tables and
  swaps them in.

When the analysis stops linking an input, or links another connection or
dataset in its place, the next build removes that input's rows from every table
and reads the current input from its start. Rebuild from scratch (the analysis
page, `models <analysis> --full-refresh`, or
`POST /api/analyses/{id}/models?full_refresh=true`) after a change to these
models or to the known apps: old rows otherwise keep what they were built with.

Builds create the indexes they look rows up by on the raw tables when missing
(load, second and image path for frames; page for visits; load ID and completion
time in dlt's list of loads). Each is built once, outside the model's
transaction, and committed at once; writes to that raw table wait meanwhile, so
the first build over a large table holds loads for that long. An index left
invalid is rebuilt. Creating one needs the build's role to own the raw table;
otherwise builds read it correctly but more slowly. Measured on synthetic data, a
million frames: an ordinary build under half a second per model, a first build
about 15 seconds. An ordinary build reads no table in full except `days` and
dlt's own small tables.

### Read access

`reader_role` names the role the server reads the timeline with. Each build
leaves the four relations readable by the owner and that role only, with
`SELECT`, and grants `USAGE` on their schema; the two source models stay readable
by the owner alone. The role must exist before the build that names it, or the
build fails. With `reader_role` empty nothing is granted, and the next build
takes back an earlier grant (the schema's `USAGE` stays). Make it a dedicated
role with no privileges beyond logging in, no other roles and no default
privileges: builds clear such grants from the timeline's tables, not from the
raw tables.

### Limits

- Load completion times come from the clocks of the workers that loaded them. A
  load recorded as completing more than ten minutes before one already read is
  read only by a rebuild from scratch.
- A build that reads a raw table while a load is still rewriting a row can miss
  that row's old version; nothing is locked against ingestion, and the next build
  corrects it.
- With more than 5,000 loads waiting, a build reads every completed load once.
- A change spread over more than 50 separate stretches of the screen history is
  recomputed as one span, from the first to the last.
- Where clocks go back at midnight, a day starts at the later of the two
  midnights; the hour before counts on the previous day.
