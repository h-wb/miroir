# Browser history

Browsing history from Zen (Firefox-based) on the MacBook, from 2025-01-07 on,
including how long each page was in view and how much I typed on it.

- **Status**: legacy load retired 2026-11-11; the data stays in `browser_history_data`. To be redone as a Miroir connection with a watched folder at `/miroir/data/browser` (the same synced NAS folder the legacy flow read)
- **Code**: connector [`src/integrations/browser_history/`](../../src/integrations/browser_history/) (two formats: a full snapshot, and incremental batches from the agent); collector [`agent/miroir_agent/collectors/zen_history.py`](../../agent/miroir_agent/collectors/zen_history.py), built 2026-11-20, not deployed
- **Raw schema**: `browser_history_data.zen_*`
- **dbt**: `staging.stg_browser__{visits, page_interactions}` → `browser.{visits, domains_daily}`
- **Dashboard**: Browser (31): Overview (pages, hours in view, top sites, how I got there) / When (weekday × hour heatmap, hour and weekday charts, recent visits); Date, Group by and Site filters

## Where the data comes from

Zen keeps history in `places.sqlite` in its profile, locked while the browser
runs. A mise task in the dotfiles snapshots it:

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

## Through the agent (decision 032)

The agent's `zen_history` collector is the second way in. It sends only what is
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
(`token`, `signature`, `code`, `key` and the like), and known token shapes in
addresses and titles. A long id in a path is left alone. The snapshot path masks
nothing.

Limits: a page whose title or counters change without a new visit is not sent
again until it is visited. A reused id overwrites the older row with that id, as
it does with snapshots.

## Quirks

- Firefox trims old history as the DB grows; the warehouse keeps it (merge).
- A new snapshot only reaches the NAS once the Nextcloud client uploads it; when
  it lags, upload the snapshot to the connection directly instead.
