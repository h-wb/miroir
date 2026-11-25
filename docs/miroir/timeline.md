---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-25
summary: How the server reads the timeline's tables, the JSON API it answers (screen, browser and conversations), how it serves images, and how the Day view and search pages behave.
topics:
  - "[[Warehouse]]"
  - "[[Screen activity]]"
  - "[[Browser history]]"
  - "[[Apple Messages]]"
created: 2026-11-24T23:30
---

# Timeline

The timeline (decision 040) lets you move through a day of screenshots and
browser visits and search what was on screen. Its API also reads Apple Messages
conversations, when the analysis links a `messages` input. Its tables are built by the
[`timeline` package](analyses.md#the-timeline-package); the server only reads
them. It is the server's only access to the warehouse, in
`src/core/timeline.py`, which only `web` and `cli` import.

## Turning it on

It is off until the server's environment names a read-only connection and the
configuration has a `timeline` analysis:

- `MIROIR_TIMELINE_DATABASE_URL`, a `postgresql://` URL; or
- `MIROIR_TIMELINE_DATABASE__HOST`, `__USERNAME` and `__PASSWORD` (required) and
  `__PORT` and `__DATABASE` (default 5432 and the username). The URL wins.

The role should be able to select from the timeline's tables and nothing else,
with `statement_timeout = '5s'` and `default_transaction_read_only = on`. The
server sets both on every session it opens anyway, so one slow search cannot hold
a connection and a mistaken grant cannot lead to a write. Image lookups have a
1-second limit.

Each server process holds up to five connections as the reader: four for
queries, one of its own for image lookups, so images never wait behind searches.
During a rolling update two processes run, so the role's connection limit must
allow ten for both to run at full use.

The server starts without the warehouse; a database that is down only shows on
`GET /timeline` and `GET /api/timeline/health` (JSON: 200 when ready, 503 when not
configured, unreachable or writable).

Searches never put words in an address. They are sent with POST, on the page and
in the API; an address with `q` is refused (400), and the server's access log
drops every query string. Proxies in front log what they receive, which with POST
is no longer the words.

## The API

The server reads the first `timeline` analysis in the configuration, in the
schema its build writes. Others are named in a `warning` field of the health
check and of every answer. Every answer carries `analysis` and `timezone`. Dates
are local days in that time zone, `YYYY-MM-DD`; a day runs from its local
midnight to the next (where clocks go back at midnight, from the later of the
two). Times are ISO 8601 with their offset. Each route needs the login (cookie
or administrator bearer), reads through the read-only session, and bounds what
it returns. An unknown or repeated parameter is refused.

| Route | Parameters | Answer |
| --- | --- | --- |
| `GET /api/timeline/days` | `from`, `to`: days, inclusive, at most 400 apart | `{from, to, days: [{day, screen?: {events, frames}, browser?: {events}, messages?: {events}}]}`, only days with rows, in order; `messages` only when the analysis links a `messages` input |
| `GET /api/timeline/screen/{day}` | none | A day's frames as columns, below |
| `GET /api/timeline/frames/{key}` | none (`key` is the frame's media key, slashes included) | One frame, below; 404 if unknown |
| `GET /api/timeline/events` | `from`, `to`: a day or a time with offset (`to` as a day includes it); `source`: `screen`, `browser` and, when linked, `messages`, comma-separated (default `screen,browser`); `limit` 1–500 (200); `after`: the previous page's `next` | `{events: [...], next}`, events that started in `[from, to)`, oldest first |
| `GET /api/timeline/conversations` | `limit` 1–200 (50); `after`: the previous page's `next` | `{conversations: [...], next}`, newest activity first, below |
| `GET /api/timeline/conversations/{id}` | none (`id` is a `conversation_id`) | One conversation, below; 404 if unknown |
| `GET /api/timeline/conversations/{id}/messages` | At most one of `before` and `after` (a cursor of a previous answer), `around` (a message's guid) and `at` (a day or a time with offset); `limit` 1–200 (100) | `{conversation_id, messages: [...], before, after}`, below |
| `POST /api/timeline/search` | A JSON body (`Content-Type: application/json`, else 415; at most 8 KB; each field once). `q`: 1–200 characters, web search syntax (`"a phrase"`, `-word`, `or`), with at least one word to look for in every part (`-word` alone, or `a or -b`, is 400); `kind`: `screen` (default), `browser` or `messages`; `app`: a bundle identifier (screen only); `conversation`: a `conversation_id` (messages only); `from`, `to`: days, optional; `limit` 1–50 (20); `before`: the previous page's `next`. A `GET` answers 400 "Search with POST" | Moments, visits or messages, newest first, below |

The screen index of a day, small enough for about 3,000 frames (about 110 KB,
15 KB compressed):

```json
{"day": "2026-11-20", "start": "2026-11-20T00:00:00+01:00",
 "end": "2026-11-21T00:00:00+01:00", "connection": "screen",
 "media_kind": "screenshots", "count": 2, "truncated": false,
 "apps": [{"bundle_id": "com.apple.Safari", "name": "Safari", "colour": "#2196f3"}],
 "frames": {"t": [0, 60], "app": [0, null], "key": ["2026/11/20/a.jpeg", "2026/11/20/b.jpeg"],
            "media": [1, 0]}}
```

`t` is seconds from `start`; `app` an index into `apps`, or null; `key` the media
key. `media` is what is known of the screenshot: `0` nothing yet (it may still
be queued or not derived; not the same as absent), `1` both thumbnails exist,
`2` stored without thumbnails, `3` withheld (never uploaded). At most 20,000
frames; `truncated` says more exist. This answer is
`Cache-Control: private, no-cache` with a weak ETag, so a browser revalidates it
(late frames can still change a past day) and gets 304 when unchanged.

A frame: `{key, captured_at, day, t, app: {bundle_id, name, colour} | null, text,
text_truncated, withheld, masked, media: {connection, kind, key, state, status}}`.
`text` is the masked text (at most 100,000 characters), null when withheld;
`withheld` the reason; `masked` how many replacements the collector made;
`state` one of `unknown`, `thumbnails`, `stored`, `withheld`; `status` the
worker's thumbnail status or null.

An event: `{source, event_id, started_at, ended_at, kind, title, body, group,
payload, media}`. For a screen session `kind` is `app_session`, `title` the app,
`group` its bundle identifier, `payload` `{bundle_id, frames, last_frame}`, and
`media` its first frame's (`state` `withheld` when that frame is). For a visit
`kind` is `visit`, `body` the address, `group` the site, `media` null. For a
message `kind` is `message`, `title` its text, `group` its `conversation_id`,
`payload` `{guid, conversation, display_name, sender, from_me, service,
attachments, reactions, edited, unsent, audio, reply_to}` (`display_name` the
conversation's, `sender` the sender's name or null for mine, `attachments` and
`reactions` counts), and `media` its first attachment's. Title and body are cut
at 2,000 characters.

A conversation in the list: `{conversation_id, display_name, is_group,
participant_count, last_message_at, message_count, preview, preview_from_me}`.
Only conversations with a message are listed; those whose messages are all
undated come last, with a null `last_message_at`. `message_count` includes
undated messages. `preview` is the first 200 characters of the last dated
message, null when it has no text; `preview_from_me` whether I sent it. One
conversation adds `title` (a group's own), `first_message_at`, `participants:
[{handle, display_name}]` (never me) and `undated`, how many of its messages have
no time. An id that is not 32 lower-case hex characters is 404 without a query.

A conversation's messages are its dated ones, oldest first, paged by `(sent_at,
guid)`: without a parameter the latest page; `before` or `after` the page before
or after a cursor; `around` a page with that message in the middle (nearer an
end, the page still fills; 404 if it is not a dated message of this
conversation); `at` the page starting at that time (a day: its local midnight),
empty when nothing is that late. The answer's `before` and `after` are the
neighbouring pages' cursors, null at either end. Undated messages are not paged:
they have no place on the axis, and the conversation counts them.

A message: `{guid, sent_at, day, from_me, sender: {handle, display_name} | null,
service, text, text_truncated, text_unreadable, edited, edited_at, unsent, audio,
reply_to, attachments: [{guid, kind, mime_type, bytes, transfer_name, media}],
reactions: [{kind, from_me, sender, part, at}]}`. `sender` is null for mine; its
`display_name` the address book's name, else the handle as the message writes it.
`text` is whole up to 100,000 characters (`text_truncated` beyond), as the
collector delivered it. A reaction's `sender` is the person's name, or their handle when the timeline has no one by that handle (someone no longer in the chat), null for mine. An
attachment's `media` is `{connection, kind: "attachments", key, state, status}`
as a frame's, `state` `unknown`, `thumbnails` or `stored` (an attachment is never
withheld), and null for a file with no media key. Names are read from `people`
and `conversations` at each answer, so a renamed contact shows at the next build.

A screen search answers `{kind: "screen", moment_seconds: 300, apps, moments,
next}`. A moment is the matching frames of one app in one five-minute window
(windows start on multiples of five minutes): `{key, connection, at, until, day,
app, matches, snippet, media}`, `key` and `at` its first match, `until` its last.
A browser search answers `{kind: "browser", visits: [{event_id, at, title, url,
site, snippet}], next}`, matching titles and addresses. A message search answers `{kind: "messages",
messages: [{guid, conversation_id, display_name, sent_at, day, from_me, sender,
snippet}], next}`, dated messages only, matched through the text index of
`messages` whatever the filters. A `snippet` is a list of
strings alternating plain text and matches, plain text first: show it as text,
never as markup. `next` is null on the last page.

Errors are `{"error": "..."}`: 400 for a parameter, 404 for an unknown frame,
conversation or message, 503 when the timeline is not configured, not built yet
or unreachable (for the conversation routes and a message search, also when the
analysis has no `messages` input), 504 when a query reached the time limit. Every answer but the screen index is
`Cache-Control: private, no-store`. Answers over 1 KB are gzip-compressed for a
client that accepts it.

## Images through the server

`GET /media/{connection}/{kind}/{key}` without parameters redirects to a short
signed link. With `size`:

- `size=360` or `size=1280` copies that thumbnail from the bucket; 404 if the
  worker made none (show a placeholder);
- `size=original` copies an original image (by its extension) of at most 25 MB
  and the kind's limit, and redirects to a signed link for any other or larger
  file, so the storage serves range requests.

Copied bytes carry `Cache-Control: private, max-age=31536000, immutable` and the
bucket's ETag, and answer `If-None-Match` with 304. The connection, kind and key
are checked as for an upload. The server only copies bytes; it never decodes an
image.

For a connection a timeline analysis reads as its `screen` input, the route first
asks the timeline about a screenshot's key, at every size and for the plain
redirect: a withheld frame's file answers 404 (it may have been stored before a
newer rule withheld the frame); a key with no frame yet is served. Only the
analyses that can answer are asked (a `reader_role` set and tables built); a
frame withheld in any of them stays hidden. If none can answer, the route
refuses with the timeline's error (503 or 504) rather than serve the file. A key
with a control character is 404 before any lookup. Other connections are not
affected. The file stays in the bucket: it is hidden, not deleted.

## The pages

With a timeline configured, the header links **Timeline**. Every page needs the
login and is `Cache-Control: private, no-store`. JavaScript fills the Day view's
lanes and scrubber from the API (`static/timeline-core.js`, tested with
`node --test`, and `static/timeline.js`); the rest of each page works without it.

- `/timeline` opens the latest day with frames or visits in the last 400 days, or
  today. Without a reader or a timeline analysis it says why (503), as every
  timeline page does; tables not built or the warehouse down say so too (503),
  a query past the time limit 504.
- `/timeline/{day}` is the Day view: a month calendar beside it, each day shaded
  by its frames plus visits, with links to the days before and after; the frame
  shown, with its time, app, state and masked text, selectable, and "Open the
  original" when its file is stored; two lanes on the day's time axis (23 to 25
  hours), Screen coloured by app and Browser with one mark per visit; the visits
  within fifteen minutes of the frame shown. A day with nothing recorded says so.
- `?frame=<key>` opens the day at that frame (a frame of another day opens on its
  own day). The address follows the scrubber.
- Scrubbing: drag the slider, or drag or tap a lane. Keys on the slider or a
  lane: arrows a frame, Shift and an arrow a minute, Page Up and Page Down an
  hour, Home and End the first and last frame. While moving it shows the
  360-pixel thumbnail of the frame under the cursor, at most ten a second; at
  rest (a quarter second) the 1280-pixel one, the frame's text, and the two
  thumbnails on each side preloaded.
- Instead of an image: "Image state not known yet" (no record of the file: it
  may well be stored), "No thumbnail could be made" (stored; the original may
  still open), "Withheld" (never uploaded, no text either), "The image could not
  be loaded" (a record whose file is gone).
- `/timeline/search` searches screen text (moments, newest window first, apps in
  name order within one) or, with **Browser visits**, titles and addresses, with
  an app filter (screen only) and a date range. The form is posted, so the words
  are never in the address bar, the history or a log; `?q=` is refused (400) with
  an empty form. The form takes no files, at most 10 fields and 8 KB a part.
  Each moment opens the Day view at its first match. "Older results" posts the
  words, filters and next-page token again, in place with htmx or as a page.
- An htmx request made after the login expired gets `HX-Redirect` to the login.
- Up to 760 pixels wide everything is one column: the image, the text, the
  lanes, the slider, the calendar; lanes and slider are taller for a finger.

Data is always shown as text: titles, snippets and screen text are never read as
markup, and only `http` and `https` addresses become links (`rel="noreferrer"`).
The pages keep nothing in the browser beyond its HTTP cache: no service worker,
no IndexedDB, htmx's history off. Logging out sends
`Clear-Site-Data: "cache", "storage"` and empties the page's storages, and a page
restored from the back-forward cache after logging out reloads and asks for the
login.
