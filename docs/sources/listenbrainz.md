---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-24
summary: How the ListenBrainz connector mirrors every listen, where those listens come from, and the cleanups the history has had.
topics:
  - "[[Music listening]]"
  - "[[Warehouse]]"
created: 2026-09-24T22:01
---

# ListenBrainz

Every listen. ListenBrainz is the hub: players scrobble into it and Miroir
mirrors it.

- **Integration**: `listenbrainz`, fetched on a connection's schedule. The connections configured today are listed in the interface and by
  `miroir connections`.
- **Code**: [`src/integrations/listenbrainz/`](../../src/integrations/listenbrainz/)
- **Raw table**: `listens`
- **Packages**: `music`, `crossovers`

## Where the data comes from

| Period | Submitted by (`submission_client`) |
| --- | --- |
| 2019-06 to 2020-10 | ListenBrainz Archive Importer (older Last.fm history) |
| 2020-10 to 2025-09 | ListenBrainz's Last.fm importer |
| 2025-09 on | multi-scrobbler (Spotify and Plex), and a few BrainzPlayer listens |

multi-scrobbler runs in the cluster
([home-ops `media/multiscrobbler`](https://github.com/h-wb/home-ops/tree/main/kubernetes/apps/main/media/multiscrobbler))
and scrobbles to ListenBrainz and Last.fm; its alerts there say when a source
stops polling or a client needs to log in again.

## Extraction

One resource, `listens`, over `GET /1/user/{username}/listens`:

- merged on `(listened_at, recording_msid)`, since `listened_at` alone is not
  unique;
- incremental on `listened_at`, from the `start_date` setting;
- a custom paginator: `min_ts` is exclusive, so paging from a page's newest
  timestamp would skip other listens of the same second. It asks one second
  earlier and lets the merge key dedupe, and stops on a short page;
- pages 1.5 s apart: back-to-back paging gets ListenBrainz's HTML bot check
  instead of JSON.

## Settings and credentials

Settings `username` and `start_date` (an instant with a UTC offset). Credential
`access_token`, the user token, in Miroir's store or as the fnox reference
`LISTENBRAINZ__ACCESS_TOKEN`.

## History

- 2026-09-23: from 2025-12-13 to 2026-03-07 ListenBrainz's own Spotify connector
  ran beside multi-scrobbler and doubled most plays. The duplicates were deleted
  in ListenBrainz itself (`POST /1/delete-listen`), keeping multi-scrobbler's
  copy, and the history was loaded again. Listens only the connector had (during
  multi-scrobbler outages, or plays too short for multi-scrobbler) were kept.
- 2026-08-24 to 2026-09-22: multi-scrobbler lost its Spotify session and nothing
  was scrobbled. Those listens are not in the warehouse.

## Quirks

- ListenBrainz applies deletions shortly after the hour, and its listens endpoint
  caches by URL: vary the query to see fresh results.
- A sync never sees deletions, or listens backfilled below its cursor, and Miroir
  loads have no full refresh: reading the history again needs a connection with
  its own dataset and state.
