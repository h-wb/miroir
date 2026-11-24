---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-24
summary: How data moves into and out of the warehouse, one sequence diagram per flow, with what each flow guarantees and what it costs.
topics:
  - "[[Warehouse]]"
  - "[[Device collector]]"
  - "[[Job runner]]"
created: 2026-11-23T08:25
---

# Flows

How data moves into and out of the warehouse. The rules behind each flow are
[decisions](decisions.md), named here by number; the details of each step are on
[Connections and runs](runs.md), [Media](media.md) and [Timeline](timeline.md).
Which part of the code does what: [architecture](../architecture.md). Where each
part runs in the installation:
[home-ops](https://github.com/h-wb/home-ops/blob/main/kubernetes/apps/main/automation/miroir/README.md).

Every diagram uses the same columns in the same order: the device on the left,
Miroir in the middle (the server, Prefect, flow runs, the metadata store, the
warehouse), storage on the right (the artifact folder and the media bucket). A
flow shows only the columns it uses. An arrow shows where information moves.

There are two ways in (decision 021): a fetch, where Miroir pulls from a
service's API, and a delivered file, which reaches a connection by upload, its
receiving URL or a watched folder and is kept as an artifact before it is
imported. A sender, such as the collector or the phone's export app, is a client
of the receiving URL, not a transport of its own.

## A. A device delivers rows

The collector reads what is new in a local source, removes what must not leave,
and hands one file to Miroir. Screen text, browser history and messages arrive
this way (decisions 030, 032, 034, 038).

```mermaid
sequenceDiagram
    box Device
        participant S as Source on the device
        participant C as Collector
    end
    box Miroir
        participant M as Server
        participant R as Flow run
        participant W as Warehouse
    end
    box Storage
        participant F as Artifacts
    end
    Note over C: started by the device's scheduler
    S->>C: rows newer than the saved position
    Note over C: masks secrets, withholds<br/>what must not leave
    Note over C: writes one batch to its spool
    C->>M: the batch, with the connection's token
    M->>F: kept as received, named by its content
    M->>C: accepted
    Note over C: only now moves its position<br/>and clears the spool
    M->>R: asks for an import
    F->>R: the batch
    R->>W: rows merged on the source's own keys
    C->>M: a report: fixed words and counts, never data
```

The source is opened read-only, and the collector sends rows as they are: no
cleaning, no joining, no summary. Masking is the one change it makes, by fixed
rules and a pinned scanner, never by a model.

- What guarantees it: the position moves only after Miroir answers 200 or 201,
  so a batch that did not arrive is sent again by the next run. The same bytes
  are the same artifact and rows merge on keys, so a batch that arrives twice
  changes nothing. A token can only add to its own connection.
- What it costs: the warehouse is as far behind as the device's schedule. A
  masked value is gone from the warehouse for good; a better rule means sending
  that range again (flow G) while the source still holds it. Each delivery
  requests one import.

## B. A file that a row points at

A frame has a screenshot and a message has attachments. Those files are not
parsed, so they skip flow A and go straight to the bucket (decision 033).

```mermaid
sequenceDiagram
    box Device
        participant C as Collector
    end
    box Miroir
        participant M as Server
    end
    box Storage
        participant B as Bucket
    end
    Note over C: after the rows were accepted,<br/>never for a withheld frame
    C->>M: these files, with their sizes
    M->>C: per file: already there, refused,<br/>or a link signed for a few minutes
    C->>B: the file, to its link
    C->>M: stored
    Note over M: queues the file for thumbnails,<br/>if its kind has them
```

The file's name in the bucket comes from a column of its row, so row and file
are tied by construction and need no lookup table.

- What guarantees it: the device never holds a storage key. A link is good for
  one file and a few minutes. A file already there is not sent again, so an
  interrupted upload resumes where it stopped.
- What it costs: a screenshot cannot be masked. The bucket is private and
  viewing a file needs Miroir's login. A file that is not on the device (an
  attachment still in the cloud) is missing until it is.

## C. A service is fetched

For a service with an API nothing runs on a device. Prefect starts a load on the
connection's schedule, or when one is requested (decisions 012, 013, 021).

```mermaid
sequenceDiagram
    participant X as Service
    box Miroir
        participant P as Prefect
        participant R as Flow run
        participant M as Metadata store
        participant W as Warehouse
    end
    Note over P: the connection's schedule fires
    P->>R: starts one run, from the pinned image
    M->>R: the connection's credentials, decrypted in the run
    X->>R: what changed since the last load, with a small overlap
    R->>W: rows merged on the service's own keys
    R->>M: what ran, and how it ended
```

- What guarantees it: reading an overlap again is harmless, because rows merge.
  One load at a time per connection.
- What it costs: a service that expires data faster than the schedule runs
  loses rows.

## D. An app pushes an export

The phone's health export app posts each export to the connection's receiving
URL: flow A without a collector, through the same door, with the same token rule
and the same import (decision 017).

```mermaid
sequenceDiagram
    box Device
        participant A as Export app on the phone
    end
    box Miroir
        participant M as Server
        participant R as Flow run
        participant W as Warehouse
    end
    box Storage
        participant F as Artifacts
    end
    A->>M: the export, with the connection's token
    M->>F: kept as received
    M->>R: asks for an import
    F->>R: the export
    R->>W: rows merged
```

A file can also be uploaded in the interface or left in a watched folder. All
three ways register the file first and import it second, so any file can be
imported again later with a newer parser.

## E. From a load to tables you read

Raw tables mirror the source. Everything that interprets them (who is "me", local
days, sessions, counts) is a model in an analysis, built after each load
(decisions 022, 040, 042).

```mermaid
sequenceDiagram
    box Miroir
        participant R as Flow run (load)
        participant M as Metadata store
        participant B as Flow run (build)
        participant W as Warehouse
        participant D as Dashboards and timeline
    end
    R->>W: raw rows
    R->>M: load succeeded
    Note over M: one build per analysis<br/>that links this connection
    M->>B: build this analysis
    W->>B: the raw tables of its inputs
    B->>W: the analysis's own schemas
    B->>M: built, on which data
    W->>D: read
```

- What guarantees it: an analysis writes only into its own schemas, so two never
  collide. A failed build leaves the load succeeded and the analysis stale.
  Builds run one at a time.
- What it costs: a busy source would build constantly, so `messages` and
  `timeline` build at most every ten and five minutes, and loads inside that
  window share one build. A model that changes shape needs a rebuild from
  scratch.

## F. Thumbnails

Scrubbing through a day needs small images. A derive run makes them once, a few
minutes after files arrive (decision 040).

```mermaid
sequenceDiagram
    box Miroir
        participant M as Metadata store
        participant R as Flow run (derive)
        participant W as Warehouse
    end
    box Storage
        participant B as Bucket
    end
    Note over M: files confirmed in flow B wait in a queue
    M->>R: a batch of files, a few minutes later
    B->>R: each original
    Note over R: a separate process decodes it,<br/>with limits on memory and time
    R->>B: two WebP copies, 360 and 1280 pixels wide
    R->>W: one row per file in media_objects
    R->>M: done
```

`media_objects` is how a model knows whether a file exists and has thumbnails,
without asking the bucket. Files stored before thumbnails existed are walked by
`media-backfill`.

- What guarantees it: the decoder proves itself on built-in images before it
  touches a file, so a broken decoder stops the run instead of marking files
  failed. One bad file costs one row, not the batch.
- What it costs: only screenshots are derived. Attachments get neither
  thumbnails nor records, and no thumbnail is made of HEIC or video.

## G. Sending something again

A restore, a second Mac with the same messages, a new masking rule, a gap to
repair: all are flow A, started by hand for a range of dates (decisions 034,
038).

```mermaid
sequenceDiagram
    box Device
        participant S as Source on the device
        participant C as Collector
    end
    box Miroir
        participant M as Server
        participant W as Warehouse
    end
    Note over C: resend, from a date to a date
    S->>C: every row in the range, whatever was sent before
    Note over C: masks with today's rules
    C->>M: batches, as in flow A
    M->>W: rows merged on their keys:<br/>changed rows update, nothing doubles
```

- What guarantees it: ScreenMemory and Messages rows are keyed by the source's
  own identifiers (a frame's image path, Apple's guids), so the same thing from
  two devices or two backups is one row. Browser rows are keyed by the profile's
  own row IDs, so a second profile needs a connection of its own.
- What it costs: it works only while the source still holds the range. The
  saved position does not move.

## H. Reading

Two readers, one rule: read analyses, not raw tables.

```mermaid
sequenceDiagram
    participant Y as You
    box Miroir
        participant D as Metabase
        participant M as Server
        participant W as Warehouse
    end
    box Storage
        participant B as Bucket
    end
    Y->>D: opens a dashboard
    W->>D: analysis tables
    Y->>M: opens a day in the timeline
    W->>M: the day's frames and events, through a role that can only read
    B->>M: thumbnails
    M->>Y: the day, with its images
```

| Reader | Reads |
| --- | --- |
| Dashboards | Analysis schemas |
| Timeline | The `timeline` analysis (screen and browser) and thumbnails |

- What guarantees it: the timeline's role can read only the timeline's tables,
  and every query stops after five seconds. Images are served behind Miroir's
  login.
- What it costs: a dashboard is as fresh as the last build, not the last load.
