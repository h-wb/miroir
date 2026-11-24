---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
summary: This reference explains how the browser history connector handles state and masking for snapshots and incremental batches.
topics:
  - "[[Browser history]]"
  - "[[Device collector]]"
  - "[[Warehouse]]"
created: 2026-09-24T22:01
---

# Browser history

Browsing history from Zen (Firefox-based) on the MacBook, from 2025-01-07 on,
including how long each page was in view and how much I typed on it.

- **Status**: the legacy load retired on 2026-11-11; its `browser_history_data` dataset and models were dropped on 2026-11-22 (decision 042). Miroir accepts snapshots and incremental collector batches.
- **Code**: connector [`src/integrations/browser_history/`](../../src/integrations/browser_history/); source [`collector/miroir_collector/sources/zen_history.py`](../../collector/miroir_collector/sources/zen_history.py)
- **Raw schema**: a Miroir connection uses its own dataset, not `browser_history_data`.
- **dbt**: the `browser` model group includes `visits` and `domains_daily`; the `timeline` analysis can also use browser history.
- **Historical dashboard reference**: Browser (31) showed an overview of visits and time in view, plus a weekday and hour breakdown.

## Where the data comes from

Zen keeps history in `places.sqlite` in its profile, locked while the browser
runs. The retired load used a mise task in the dotfiles to snapshot it:

- **[`conf.d/zen-history.personal.toml`](https://github.com/h-wb/dotfiles/blob/main/conf.d/zen-history.personal.toml)**
  (MacBook only, `personal` env): task `mise run zen-history-snapshot` copies the
  DB + its WAL, checks it (`PRAGMA quick_check`), `VACUUM INTO` one clean file and
  gzips it to `~/Nextcloud/Hugo/Documents/data/browser/zen-places.sqlite.gz`
  (`sqlite` comes from mise). A launchd agent in the same file runs it every 6h
  and at login; log in `~/Library/Logs/zen-history-snapshot.log`.
- `sqlite3 .backup` can't be used on the live DB: Zen holds an exclusive lock.
- Nextcloud's `Hugo/Documents/data` is the NAS share. The retired legacy flow
  read it at `/root/.prefect/data/browser`; Miroir flow runs see the same folder
  at `/miroir/data/browser`, ready for a watched folder.

The Arc history from before the switch (2025-01-07 → 2025-10-22) was imported
into Zen, so it's part of the same DB (visits with `source = 3` "restore" also
come from that import). History before 2025-01-07 doesn't exist anywhere.

Why a DB snapshot and not Firefox Sync or an extension: Sync is end-to-end
encrypted (needs account login + key derivation) and only keeps a recent window;
an extension would need signing and a webhook, and neither exposes
`moz_places_metadata` (time in view, typing), which only exists in the local DB.
Phone history would arrive in this DB through Sync (visits only) if enabled.

## Extraction

The snapshot is decompressed once per run and read read-only; tables load raw.

| Resource | Table | Key | Incremental |
|---|---|---|---|
| `zen_places` | `moz_places` (URL, title, visit count, frecency) | `id` | `last_visit_date` |
| `zen_historyvisits` | `moz_historyvisits` (`place_id`, `from_visit`, `visit_type`, `source`) | `id` | `visit_date` |
| `zen_places_metadata` | `moz_places_metadata` (`total_view_time`, `typing_time`, `key_presses`, scrolling) | `id` | `updated_at` |
| `zen_origins` | `moz_origins` (scheme + host) | `id` | full |
| `zen_bookmarks` | `moz_bookmarks` | `id` | full |

Ids are local to the profile but stable, so they work as merge keys. Text that
isn't valid UTF-8 (page descriptions as sites sent them) is decoded with
replacement characters rather than failing the run.

## dbt

- Units: visit times are **microseconds** (PRTime); `moz_places_metadata`
  timestamps and durations are **milliseconds**. Staging converts both.
- `browser.visits`: one row per visit with URL, title, domain (`moz_origins` host,
  `www.` stripped), readable `visit_type`, `source`, local `day`/`hour`, and
  `is_navigation` (false for redirect hops, reloads, embeds, framed links).
- `browser.domains_daily`: per site per local day, navigations plus minutes in
  view, minutes typing and key presses.

## Through the collector (decision 032)

The collector's `zen_history` source is the second way in. It sends only what is
new, as `firefox_places_batch` files, into the same `zen_*` tables the snapshots
load, so both can feed one connection.

- It never opens the live database. It copies `places.sqlite` and its WAL to a
  temporary folder, checks that neither file changed during the copy (retrying up
  to three times), and requires `PRAGMA quick_check` to pass.
- **Visits**: those with an id above the last one sent. Firefox reuses the ids of
  deleted visits (after "clear recent history"), so it also takes visits whose time
  is later than any sent so far.
- **Pages**: every page a sent visit points at, plus pages never sent before.
- **Page interactions**: rows updated since the last batch.
- **Origins and bookmarks**: the whole table, only when it changed.

Secrets in addresses are masked on the Mac unless `mask_urls` is false: a password
in `user:password@host`, the value of a secret-named query or fragment parameter
(`token`, `signature`, `code`, `key` and the like), and provider credentials and
known token shapes in addresses and titles, found by the scanner's catalogue,
your own rule files and the collector's own rules (decision 036). A long id in a
path is left alone. The snapshot path masks nothing.

Limits: a page whose title or counters change without a new visit is not sent
again until it is visited. A reused id overwrites the older row with that id, as
it does with snapshots.

## Quirks

- Firefox trims old history as the DB grows; the warehouse keeps it (merge).
- A new snapshot only reaches the NAS once the Nextcloud client uploads it; when
  it lags, upload the snapshot to the connection directly instead.
