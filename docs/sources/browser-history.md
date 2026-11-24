---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-24
summary: How browser history reaches Miroir, as incremental batches from the collector or as a full Places snapshot, what is masked, and what it loads.
topics:
  - "[[Browser history]]"
  - "[[Device collector]]"
  - "[[Warehouse]]"
created: 2026-09-24T22:01
---

# Browser history

Browsing history from Zen, a Firefox-based browser, including how long each page
was in view and how much was typed on it.

- **Integration**: `browser_history`, sent by the collector's `zen_history`
  source (decision 032) or loaded from a snapshot. The connections configured today are listed in the interface and by
  `pipelines-miroir connections`.
- **Code**: connector [`src/integrations/browser_history/`](../../src/integrations/browser_history/)
  with two formats, a full snapshot and the collector's batches; collector source
  [`collector/miroir_collector/sources/zen_history.py`](../../collector/miroir_collector/sources/zen_history.py)
- **Raw tables**: `zen_*`, below, in the connection's dataset
- **Packages**: `browser` (`visits`, `domains_daily`), and the browser lane of
  `timeline`

## Where the data comes from

Zen keeps history in `places.sqlite` in its profile, locked while the browser
runs. Only the local database holds `moz_places_metadata` (time in view, typing);
Firefox Sync is end-to-end encrypted, keeps a recent window and has no such
data, and an extension would see none of it either.

### Through the collector

The collector's `zen_history` source sends only what is new, as
`firefox_places_batch` files, into the same `zen_*` tables a snapshot loads, so
both can feed one connection.

| Option | Default | Meaning |
| --- | --- | --- |
| `places_path` | none | The database to read |
| `profiles_dir` | `~/Library/Application Support/zen/Profiles` | Without `places_path`, the most recently changed `*/places.sqlite` under it |
| `since` | none | Where visits, pages and interactions start without a saved position |
| `max_rows` | 20000 | Rows per table per batch (1 to 200,000) |
| `mask_urls` | true | Mask secrets in addresses and titles |

- It never opens the live database. It copies `places.sqlite` and its WAL to a
  temporary folder, checks that neither changed during the copy (up to three
  tries), and requires `PRAGMA quick_check` to pass.
- Visits: those with an ID above the last one sent. Firefox reuses the IDs of
  deleted visits (after "clear recent history"), so it also takes visits later
  than any sent so far.
- Pages: every page a sent visit points at, and pages never sent before.
- Page interactions: rows updated since the last batch.
- Origins and bookmarks: the whole table, only when it changed. `since` does not
  filter them and a resend does not include them.

Unless `mask_urls` is false, secrets in addresses are masked on the device: a
password in `user:password@host`, the value of a secret-named query or fragment
parameter (`token`, `signature`, `code`, `key` and the like), and provider
credentials and token shapes in addresses and titles found by the scanner's
catalogue, your own rule files and the collector's own rules
([ScreenMemory](screenmemory.md#what-never-leaves-the-mac), decision 036). A long
ID in a path is left alone.

Limits: a page whose title or counters change without a new visit is not sent
again until it is visited. A reused ID overwrites the older row with that ID.

### As a snapshot

A gzip or plain copy of `places.sqlite` can be uploaded or left in a watched
folder. It is decompressed once per import, opened read-only, and every row is
loaded and merged on the IDs; a snapshot masks nothing. Make one with
`VACUUM INTO` from a copy of the database and its WAL: `sqlite3 .backup` cannot
read the live database while Zen holds its lock.

## Tables

| Resource | From | Key |
| --- | --- | --- |
| `zen_places` | `moz_places` (URL, title, visit count, frecency) | `id` |
| `zen_historyvisits` | `moz_historyvisits` (`place_id`, `from_visit`, `visit_type`, `source`) | `id` |
| `zen_places_metadata` | `moz_places_metadata` (`total_view_time`, `typing_time`, `key_presses`, scrolling) | `id` |
| `zen_origins` | `moz_origins` (scheme and host) | `id` |
| `zen_bookmarks` | `moz_bookmarks` | `id` |

IDs are local to the profile but stable, so they work as merge keys; a second
profile or browser needs a connection of its own. Text that is not valid UTF-8
(page descriptions as sites sent them) is decoded with replacement characters.

## Models

- Units: visit times are microseconds (PRTime); `moz_places_metadata` times and
  durations are milliseconds. Staging converts both.
- `visits`: one row per visit with URL, title, domain (the `moz_origins` host
  without `www.`), a readable `visit_type`, `source`, local `day` and `hour`, and
  `is_navigation` (false for redirects, reloads, embeds and framed links).
- `domains_daily`: per site and local day, navigations, minutes in view, minutes
  typing and key presses.

## Quirks

- Firefox trims old history as the database grows; the warehouse keeps it.
- Visits with `source = 3` come from an import into the browser's history
  ("restore"), such as another browser's history.
