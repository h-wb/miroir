---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
summary: Delivery, import, and collector signals help distinguish a quiet source from a failed or absent one.
topics:
  - "[[Warehouse]]"
  - "[[Collector]]"
created: 2026-11-21T11:51
---

# Observability: noticing a source that stopped

Miroir sees a connection from three places. Together they tell "nothing new"
apart from "broken" and from "switched off" (decision 039).

| What | Where it comes from | Shown on |
|---|---|---|
| **Device report** | After each `run`, the collector reports how it went: rows and media outcome with counts, the step and reason of a failure, whether a batch is waiting in its spool, and how many earlier runs it could not report. Fixed words and counts only, never data. | Connection page ("Collector"), overview, `GET /api/connections/{id}` → `delivery.collector`, metrics |
| **Last delivery** | When a file last arrived on the connection's receiving URL (or by upload). | Connection page ("Last export received"), `delivery.last_received_at`, metrics |
| **Last import** | When a load of the connection last succeeded. | Connection page and overview ("Data loaded"), `status.last_ingestion_success` |

Reading them together:

- Reports arrive, rows say `nothing_new`: the source is quiet; nothing is wrong.
- Reports arrive with `failed`: the device is running but a step fails; the
  page names the step and reason (for example "Stopped while delivering rows:
  network").
- No report for longer than the collector's schedule: the collector is not
  running (machine off, scheduler removed, collector uninstalled) or cannot
  reach Miroir at all. When it reaches Miroir again, its report says how many
  runs it could not report in between.
- Deliveries arrive but imports do not succeed: the problem is on the server;
  see the connection's attempts.

Miroir keeps the newest 500 reports per connection in the metadata store
(`collector_reports`). The time of the last report, of the last that did not
fail, and the failure streak are kept apart (`collector_summaries`), so
retention never forgets a success or shortens a streak. Both go with the
connection, and a new connection never inherits reports of an old one with the
same ID. Counts are integers from 0 to 2^53 with names of letters, digits, `_`
and `-`; times must fall between 2000 and 2100. A collector that
predates reports simply sends none; a Miroir that predates them answers 401
(its login guard) or 404, for example. The collector records 401, 404, and 405 as
`unsupported` and does not count them. A bad token shows in row delivery, not in reports.

## Metrics

`GET /metrics` answers in the Prometheus text format. It is off unless the
server has `MIROIR_METRICS_TOKEN` set (404 otherwise); then a scrape must send
`Authorization: Bearer <that token>`. The admin token and the browser session
are refused (401), so a scraper never holds an admin credential, and the server
refuses to start when the two variables hold the same value.

```yaml
scrape_configs:
  - job_name: miroir
    scheme: https
    authorization: {credentials_file: /etc/prometheus/miroir-metrics-token}
    static_configs: [{targets: ["miroir.example"]}]
```

All metrics are gauges, one sample per connection (label `connection`).
Connections without reports appear only in the first.

| Metric | Meaning |
|---|---|
| `miroir_connection_last_received_timestamp_seconds` | Unix time a file last arrived on the connection; absent if none ever did |
| `miroir_collector_last_report_timestamp_seconds` | Unix time Miroir received the collector's last report |
| `miroir_collector_last_success_timestamp_seconds` | Unix time of the last report whose rows and media did not fail; absent if none |
| `miroir_collector_consecutive_failures` | Failed runs reported in a row, up to the latest (0 when the latest did not fail) |
| `miroir_collector_pending` | 1 when a batch was waiting in the collector's spool after its last run |
| `miroir_collector_unreported_runs` | Runs before the last report that could not be reported |
| `miroir_collector_last_count` | Each count of the last report; labels `part` (`rows` or `media`) and `name` (`frames`, `missing`, or one level down such as `masked.token`) |

## Example alert rules

```yaml
groups:
  - name: miroir
    rules:
      # The collector runs about hourly: three hours without a report means it
      # stopped running or cannot reach Miroir.
      - alert: MiroirCollectorSilent
        expr: time() - miroir_collector_last_report_timestamp_seconds > 3 * 3600
        for: 15m
        annotations:
          summary: "No collector report for {{ $labels.connection }} in 3 hours"

      # One failure can be a network blip; three in a row is a broken step.
      - alert: MiroirCollectorFailing
        expr: miroir_collector_consecutive_failures > 2
        annotations:
          summary: "{{ $labels.connection }}: {{ $value }} failed collector runs in a row"

      # A count that rises: screenshots a run could not read (missing on disk,
      # or not a valid file). Use the name of whichever count matters.
      - alert: MiroirCollectorUnreadableMedia
        expr: max_over_time(miroir_collector_last_count{part="media", name=~"missing|invalid"}[1h]) > 0
        annotations:
          summary: "{{ $labels.connection }}: {{ $value }} {{ $labels.name }} media files"

      # A source that stops understanding what it reads still runs fine. For
      # Apple Messages that shows as text it could not read, or as a database
      # layout that changed after a system update.
      - alert: MiroirCollectorSourceChanged
        expr: max_over_time(miroir_collector_last_count{part="rows", name=~"unreadable|layout_changed"}[1h]) > 0
        annotations:
          summary: "{{ $labels.connection }}: {{ $labels.name }} is {{ $value }}"
```

Adjust the silence threshold to the collector's schedule. `last_count` holds
the counts of the last run only, so the last two rules look over a window
rather than at a single sample.
