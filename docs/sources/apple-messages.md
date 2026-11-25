---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-24
summary: What the collector reads from Apple Messages on the Mac, what it sends, masks or never sends, and how Miroir merges messages, contacts and attachments.
topics:
  - "[[Messages]]"
  - "[[Device collector]]"
  - "[[Warehouse]]"
created: 2026-11-21T11:39
---

# Apple Messages

Message history (iMessage, SMS and RCS), collected on the Mac by the Miroir
collector (decision 038).

- **Integration**: `apple_messages`, sent by the collector, with attachments
  as media. The connections configured today are listed in the interface and by
  `pipelines-miroir connections`.
- **Code**: connector [`src/integrations/apple_messages/`](../../src/integrations/apple_messages/),
  source [`collector/miroir_collector/sources/apple_messages.py`](../../collector/miroir_collector/sources/apple_messages.py)
- **Packages**: `messages` (decision 041,
  [its tables](../miroir/analyses.md#the-messages-package)), without text; the
  `timeline` package's `messages` input (decision 040,
  [its tables](../miroir/analyses.md#the-timeline-package)) holds the text, for
  the timeline's conversation view and message search

## Where the data comes from

The `apple_messages` source reads `~/Library/Messages/chat.db` in place, read-only,
under one SQLite snapshot. Set `database` to use another path. On macOS, the
process running the collector needs permission to read the Messages folder.
Missing optional columns are sent as null. A missing table or required column
stops collection with a fixed error.

| Option | Default | Meaning |
|---|---|---|
| `database` | `~/Library/Messages/chat.db` | The Messages database |
| `since` | none | First message date to send |
| `settle_seconds` | 0 | Send a message only once it is this old (normal passes only) |
| `max_rows` | 20000 | Messages per batch (1 to 200,000) |
| `media` | false | Upload attachment files that are on disk |
| `home` | user's home | Home directory for `~` in attachment paths and for the address book; set this for a restored copy |
| `media_max_mb` | 500 | Maximum attachment size in MiB (1 to 5000); larger files count as `too_large` |
| `names` | false | Send address book names for the handles in Messages |

The collector keeps a fingerprint of the database's layout (the table and column
names it reads) with its cursor. When a system update changes that layout, the
next batch reports `layout_changed: 1` in its counts, even if collection still
works. A layout change alone does not send a batch: it is reported the next time
there is something to send.

## What the collector sends

The collector sends gzip JSON Lines (`apple_messages_batch`, version 1). A trailer
counts the eight record kinds, unreadable
text archives and masking replacements. The connector rejects incomplete batches
and record counts that do not match.

Normal passes send new messages from `since` (if set), at most `max_rows`
(default 20,000) per batch, as soon as a pass sees them. With `settle_seconds`
above 0, a message dated within that many seconds of now waits, and holds back
the rows after it until it is old enough. A message dated in the future (a
scheduled message, a wrong clock) is sent at once. Dates are compared row by row,
whether the row stores seconds or nanoseconds; the values sent are the
database's own integers.

Edits and unsends are found by comparing state, not by time. The cursor keeps a
digest of the guid, `date_edited`, `date_retracted` and the lengths of `text` and
`attributedBody` of every message already sent that was edited or unsent (and is
not before `since`); the lengths tell apart two edits made in the same second. When a pass finds
that digest changed, it sends **all** of those messages again in its batch, with
their links and attachments, whatever `max_rows` is: they are few, and sending
the whole set is what catches an edit synced late from another device with an
older time. The merge replaces the rows. The pass's counts report `resent`, how
many messages in the batch came from this step. Databases without those two
columns skip it. A message dated before `since` is never sent, edited or not.
The check reads the two times of every sent message on each pass, which takes
tens of milliseconds on 200,000 messages.

An edit marker that is later cleared (the time set back to 0) is not detected.
`resend --from ... --to ...` over the message's dates brings the row back in line.

Chats with their participants, and handles, are sent when those snapshots change.
`resend --from ... --to ...` sends every message dated in the chosen range, with
its links and attachment metadata, ignoring `since` and `settle_seconds`, without
advancing the saved cursor.

### Contact names

When `names` is enabled, the collector reads the Mac's AddressBook-v22 databases
under `<home>/Library/Application Support/AddressBook`, including account
databases in `Sources`. Only handles in Messages are sent, not the whole address
book. Email addresses match without regard to case, using the address book's
normalized address when present. Phone numbers match by digits (at least seven),
with a leading `1` on an 11-digit number optional. Matching cards that agree
are treated as one. If they disagree, the handle is counted in
`names_ambiguous` and no name is sent for it.

Names are not masked. The collector sends the whole matched set when it changes,
but not during a resend. If it can name nobody, it sends no contact records and
does not erase names already loaded. Missing, unreadable or incompatible address
books count as `names_unavailable` without stopping message or file collection.

### Attachment files

Set `media` to true and use a receive URL ending in `/receive`. Only nonempty
regular files already on disk under `<home>/Library/Messages/Attachments` are
offered. Paths are resolved before the containment check, so a path or symlink
pointing outside that folder is rejected. Files are stored by `media_key`; Miroir
skips an upload when it already holds the same key at the same size.
Miroir's worker then makes 360 and 1280 pixel thumbnails of HEIC, HEIF, JPEG,
PNG, GIF, WebP, TIFF and BMP attachments up to 50 MB and records every
attachment in `media_objects`; video, audio and documents get a row and no
thumbnail ([Media](../miroir/media.md#thumbnails-and-media_objects)).

The collector only offers files linked to messages in the normal delivery
scope (`since` and the acknowledged message cursor). It waits for Miroir to
acknowledge the attachment key and links first. When a late download makes
the key available or changes it, the collector sends the updated attachment
metadata and links without resending the message text. Files wait 60 seconds
after their last inode change. The normal walk also accounts for when the
metadata was acknowledged, so a file already on disk is not missed when its
message is delivered on a later page.

A `resend --media --from ... --to ...` range selects already acknowledged,
in-scope media by file inode change time; the row resend still uses message
dates. A pre-attachment collector cursor or a changed `since` starts media
tracking at the current row cursor rather than guessing which old rows were
sent. To include those older attachments, deliberately replay the wanted
scope using fresh collector state for the same connection. The collector does
not download or recover missing files.

A media pass that offers files also counts, once, the in-scope attachments
whose file is not on disk (`missing`) and those it cannot use (`invalid`: no
usable key, a path outside the folder, an empty file or a folder). A pass with
nothing new to offer reports neither.

## What never leaves the Mac

The collector takes plain text from `text` or extracts it from `attributedBody`.
It masks the free text (message text, subjects, group titles, chat display names
and attachment transfer names) using the same rules as
[ScreenMemory](screenmemory.md#what-never-leaves-the-mac). Identifiers are not
masked, on purpose: guids, `chat_identifier`, `room_name`, handles and their
services, service names, `mime_type`, `uti`, `balloon_bundle_id` and
`expressive_send_style_id`. The batch reports
where message text came from; a text archive it cannot read is counted and its
text is null. The raw `attributedBody`, `message_summary_info` and `payload_data`
are never sent. Neither are local row numbers, attachment paths or transfer state.
An attachment's `media_key` is its `guid` plus a short extension taken from its
file name (letters and digits only), taken before masking, never the path itself.
Text that is not valid UTF-8 is decoded with U+FFFD in place of the bad bytes,
and a U+0000 in any string sent becomes U+FFFD (a masked field holding one is
masked whole). Keys are never altered: a row whose key holds U+0000 or U+FFFD is
skipped and counted in `skipped`, since replacing the character could merge it
with another row. The connector refuses a batch holding U+0000 or a lone surrogate.
Masking is not anonymisation: addresses and ordinary message text can still be
present. Treat these tables as sensitive.

## Tables

| Table | Key | Rows |
|---|---|---|
| `messages` | `guid` | Message text and metadata, raw Apple dates, and UTC `sent_at`, `read_at`, `delivered_at`, `edited_at`, `retracted_at` |
| `chats` | `guid` | Chat identity and metadata |
| `handles` | `id`, `service` | Addresses and services |
| `attachments` | `guid` | Attachment metadata, with raw `created_date`, UTC `created_at`, and `media_key`, the name the file will be stored under (null when no extension is found) |
| `chat_messages` | `chat_guid`, `message_guid` | Messages in chats |
| `message_attachments` | `message_guid`, `attachment_guid` | Attachments on messages |
| `chat_handles` | `chat_guid`, `handle`, `handle_service` | Chat participants |
| `contacts` | `handle` | Matched address book names: `first_name`, `last_name`, `nickname`, `organization` |

One connection stands for the account. All eight tables merge on Apple's keys,
so messages, attachments, handles and the links between messages and
attachments do not appear twice, whether they come from several devices or after
a rebuild. Chats are the exception: a chat's `guid` depends on the system's
naming scheme, so two Macs on different macOS versions send the same
conversation as two chat rows, and its links in `chat_messages` and
`chat_handles`, whose keys hold the chat guid, appear once per scheme too. Match
conversations on `chat_identifier`. Absence from a delivery does not delete a row.

For the same key, the later load wins. With several machines sending, a machine
that still holds an older version of an edited message can put that version back
when it sends its own edited messages again; it is corrected when that machine
receives the newer state and sends again. Running the source from one always-on
machine avoids this.

## In the timeline

The `timeline` package reads these tables when its `messages` input is linked.
It reads only rows of loads `_dlt_loads` lists as complete and builds
`conversations`, `messages` (with text, reactions and attachments) and `people`.
What it relies on here:

- `chats`, `chat_handles` and `contacts` are read whole at each build: they are
  snapshots that the collector sends again when they change.
- The `handles` table is not read: a sender's address is on its message and a
  chat's people are in `chat_handles`.
- A message's attachments have no order in what is sent; the timeline orders them
  by `created_at`, then `transfer_name`, then guid.
- `media_objects`, written by the derive worker, joins on `media_key` with kind
  `attachments`: each attachment carries its file's thumbnail status and sizes,
  or nulls (unknown) until the worker has recorded it.
- Builds add indexes to these raw tables when the build's role owns them: load
  ID, `guid`, `chat_guid`, `message_guid`, `attachment_guid`, `media_key` and the
  reaction's target (`associated_message_guid` without its `p:N/` or `bp:`
  prefix).

## What it does not cover

Rich formatting, edit history, live events, reading backups directly and old
exports are not covered. Edits and unsends are sent again, but earlier
versions of an edited message are not kept. Read and delivery dates changed after
the first delivery are refreshed only by `resend`.
