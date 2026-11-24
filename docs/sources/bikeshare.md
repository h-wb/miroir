---
area: "[[Homelab]]"
categories:
  - "[[Resources]]"
reviewed: 2026-11-24
summary: How the Bike Share connector fetches every rental from the mobile app's API, and what it loads.
topics:
  - "[[Bike rides]]"
  - "[[Warehouse]]"
created: 2026-09-24T22:01
---

# Bike Share

Every bike share rental: stations, times, bike type.

- **Connection**: integration `bikeshare`, fetched on the connection's schedule;
  the installation's is `bikeshare`, bound to `bikeshare_data`
- **Code**: [`src/integrations/bikeshare/`](../../src/integrations/bikeshare/)
- **Raw tables**: `trips`, with dlt's child tables `trips__start_location` and
  `trips__end_location`
- **Packages**: `biking` (`rides`, `stations`, `routes`), `crossovers` (`rides`)
- **Dashboards**: Biking, Rides (not checked here)

## Where the data comes from

The operator's mobile app API (`layer.bicyclesharing.net/mobile/v1/…`,
`rental/closed`), called with the member ID and authorization token the app uses
and the app's request headers. There is no public API for rental history.

## Extraction

One resource, `trips`: `rental/closed` for the member, incremental on `sd` (the
rental's start), key `rentalId`. The first run asks for 500 rentals per page,
later runs 50, since only the newest matter.

## Settings and credentials

Credentials `member_id` and `authorization_token`, both taken from the mobile
app's traffic, in Miroir's store or as fnox references
(`BIKESHARE__MEMBER_ID`, `BIKESHARE__AUTHORIZATION_TOKEN` in `fnox.toml`).
Setting `verify_tls`, on by default; the app's endpoint has needed it off.

## Quirks

- The request mimics the app's `User-Agent`.
- When the app's token rotates or expires, runs fail with 401 or 403 and the
  connection shows `rejected`: capture a new one.
- The service only estimates distance; `rides` computes straight-line distance
  and speed from station coordinates, a lower bound on the real speed.
