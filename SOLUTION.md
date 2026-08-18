# SOLUTION.md

## What was broken, and why

Four defects, each matching one line of the ops report:

1. **Duplicate records / drifting counts.** `Ingest` checked `EventExists`
   then called `InsertEvent` as two separate round trips, and
   `events.event_id` only had a non-unique index behind it. Two
   redeliveries of the same `event_id` arriving close together could both
   pass the exists check before either had inserted — a check-then-act
   race with no database guardrail behind it.
2. **Same symptom, second cause.** `stats.Cache.Record` mutated the shared
   map without taking the write lock (`Get` locked, `Record` didn't).
   Concurrent deliveries for the same account raced on the same map entry.
3. **Recordings never marked processed, nothing in the logs.**
   `processRecording` ran in a detached goroutine handed the *HTTP
   request's* context. net/http cancels that context the instant the
   handler returns — before the simulated 50ms of work finishes. The
   resulting error was discarded (`// TODO: handle`), so it never surfaced.
4. **In-flight work disappearing on deploy.** Those same goroutines were
   never tracked. Shutdown only waited on `srv.Shutdown()` (in-flight HTTP
   requests) — nothing waited for recording work already running in the
   background.

## Why this deduplication strategy

Postgres is the single source of truth: a `UNIQUE` constraint on
`event_id`, with `INSERT ... ON CONFLICT (event_id) DO NOTHING` inside one
transaction that also does the call upsert and the stats increment. A
redelivery either writes nothing, or it's the first delivery and all three
writes commit together — atomic, no race window.

Alternatives considered and rejected as the *primary* mechanism:
- **Redis `SETNX` as the dedupe gate.** Faster, and worth adding later as
  a pre-check to cut load on Postgres under high duplicate rates — but
  Redis isn't durable here, so it can't be what guarantees correctness.
  It should sit in front of the constraint, never replace it.
- **Keep check-then-act, but add a Postgres advisory lock on `event_id`.**
  Works, but is more moving parts than the constraint already provides.

## At 10,000 webhooks/second

Two things wouldn't survive: `processRecording` as an in-process goroutine
per request (no backpressure, no retry, and a hard crash still loses
whatever's in flight even with the graceful-shutdown fix), and a single
Postgres instance taking every write synchronously on the request path.

I'd move recording processing off the request path: ack the webhook once
the event/call/stats transaction commits, then publish to a durable queue
for a separate worker pool — survives restarts, gets retries and
backpressure for free. On the write path I'd batch `account_stats`
increments (flush the in-memory cache to Postgres on an interval) instead
of one `UPDATE` per webhook.