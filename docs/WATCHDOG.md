# Controlled DXLink recovery (Phase 2)

The watchdog is **disabled by default**. It uses only ES Quote receipt inactivity;
SPY and XSP never initiate recovery. Phase 1 observation-age thresholds, UTC
timestamps, API fields, authentication and subscriptions are unchanged.

## Configuration

To enable, supply this environment to the Python service at its next intentional
start. This task does not edit `.env`, access Oracle or deploy anything.

```text
DXLINK_WATCHDOG_ENABLED=true
DXLINK_WATCHDOG_SECONDS=300
DXLINK_RECOVERY_COOLDOWN_SECONDS=600
DXLINK_RETRY_INITIAL_SECONDS=5
DXLINK_RETRY_MAX_SECONDS=300
```

Only `DXLINK_WATCHDOG_ENABLED=true` is required; the other lines show defaults.
The flag accepts `true` or `false` (case insensitive). Numeric settings must be
finite and positive, and maximum retry delay must be at least the initial delay.
All configuration is read when the worker is constructed. When the flag is
false or absent, the watchdog timeout, cooldown, retry and exclusion-date settings
are ignored, including invalid values. Disabled mode retains fixed five-second
connection-error retries. The flag itself and general Phase 1 freshness settings
remain validated.

`DXLINK_WATCHDOG_EXCLUDED_DATES` optionally supplies comma-separated ISO civil
dates in Chicago time, for example `2026-11-20,2026-11-23`. Either a session's
start date or its next-day trade date matching an exclusion suppresses recovery
for that entire session. This can conservatively cover newly announced early
closes, holidays or other known uncertainty; it cannot force recovery on a date
the built-in calendar excludes. No credentials belong in these settings.

## Calendar and timing

Regular hours use `zoneinfo.ZoneInfo("America/Chicago")`, including US DST:
Sunday 17:00 through Friday 16:00 CT, excluding 16:00–17:00 CT each day and the
Friday close through Sunday open. The additional `tzdata` requirement supplies
IANA timezone data on platforms such as Windows.

Sources reviewed for this implementation:
[CME equity futures regular hours](https://www.cmegroup.com/markets/equities/sp-500-and-nasdaq-100-futures.html)
and [CME 2026 holiday windows](https://www.cmegroup.com/trading-hours.html).
The implementation suppresses the **whole published holiday window plus one
civil day on each side**, and suppresses any session touching those dates.
This deliberately gives up recovery during some genuinely active hours rather
than guessing holiday opening/closing times. Thanksgiving and Christmas early
closes are covered this way. These windows are not exact ES holiday sessions.

**Only 2026 has been reviewed. All other years suppress watchdog recovery.**
From January 1, 2027 at 00:00 Chicago time, calendar coverage is expired. An
enabled watchdog emits a sanitized warning when it first observes expiry, then
at most once per monotonic hour per worker, including across reconnections.
No 2027 holiday schedule is inferred. HTTP service, Phase 1 freshness reporting
and ordinary connection-error retries continue; the warning does not enable
stall recovery during unreviewed periods.
Before extending the year guard in `es_schedule.py`, review CME's then-current
holiday windows, add conservative exclusions and deterministic tests, and check
the regular schedule. The calendar is not a fully authoritative holiday feed;
CME schedules can change and this code does not download updates at runtime.
Naive clocks or unavailable timezone data also suppress watchdog recovery.

The watchdog checks every 10 seconds. Each new connection or reopened eligible
session gets a full 300-second grace interval by default. Recent ES receipts,
including unchanged prices, reset inactivity; no initial ES observation also
times out after this grace. Time spent closed or uncertain is not counted.
Wall-clock uncertainty (UTC/monotonic disagreement over 30 seconds), backwards
elapsed time or a changed/unknown session rearm the grace interval. A delayed
poll does not reset inactivity when both clocks agree and both endpoints belong
to the same eligible session. A gap crossing a closure, holiday or ambiguous
session transition still gets conservative grace. Elapsed inactivity and cooldown use the
injected monotonic clock; wall time is used only for schedule eligibility and
discontinuity detection.

Under normal scheduling, a stall is confirmed at roughly 300–310 seconds after
the last ES receipt (or grace start). The worker reports `RECOVERING`, cancels
and awaits both listeners and the watchdog, then exits the old streamer context
before any new Session or streamer is created. It waits the current retry delay
(initially 5 seconds), recreates the session, reselects the active ES contract and
restores the same Quote/Trade subscriptions. Network setup and SDK cleanup add
their own duration; this is not a recovery-time guarantee. No OS process or
systemd restart is performed.

Cooldown prevents another **stall-triggered decision** for 600 seconds after
the previous decision. It does not inhibit ordinary connection-error retries.
Retry delays double (5, 10, 20, 40, 80, 160, 300, 300… seconds by default), including
repeated unproductive subscriptions. Backoff resets only when a connection has
lasted at least the watchdog interval and has a recent ES observation from that
connection. A subscription alone is insufficient. Attempts are unlimited but
their delay is bounded. Exponential retry settings apply only with the watchdog
enabled. Disabled mode always uses the previous fixed five-second retry delay.

One stream loop owns recovery, and `start()` is idempotent while it runs. All
listener completion, error, recovery and shutdown paths cancel and await sibling
tasks; cleanup is shielded from the SDK's AnyIO cancellation scope. Intentional
stall exceptions are raised only after normal context exit, avoiding SDK task
group wrapping. SDK exception groups are also retried with a sanitized category.
Intentional shutdown is latched before cancellation. Once requested, the worker
cannot create another session or begin another retry sleep, even if SDK teardown
replaces cancellation with a service, network or unexpected exception. Such
shutdown errors log only a sanitized category. Repeated `stop()` calls await the
same cleanup without cancelling it again. Shutdown is permanent for that worker
instance; construct a new worker for a deliberate subsequent lifecycle.

Listener cleanup and SDK streamer exit each schedule a diagnostic warning if
still pending after 30 seconds; their timer handles are cancelled on completion.
These warnings never cancel cleanup or force a replacement connection. There
are no extra diagnostic tasks or forced teardown timers. A hung cleanup remains
blocked and may require operator intervention; a blocked event loop also delays
the warning. This preserves the no-overlap policy instead of claiming teardown
has completed merely because a deadline elapsed.

If SDK `__aexit__` raises (including cancellation or an exception group), closure
is unconfirmed. The worker latches a terminal failure and reports `CLEANUP_FAILED`
for all snapshots during ordinary operation, retaining the last prices and receipt
timestamps. It logs only a fixed sanitized error. There are no retry sleeps or
replacement sessions, and neither elapsed cooldown nor another `start()` clears
the latch. The SDK's enter and exit still execute on the owning worker task.
An exception from streaming is distinguishable from an exception raised by exit:
ordinary recovery proceeds only after the exit method returns successfully.

Intentional shutdown still finishes with `STOPPED`, including when exit fails;
the terminal closure-failure latch remains set for that worker lifetime. `stop()`
after an operational `CLEANUP_FAILED` likewise reports `STOPPED` without clearing
the latch or claiming the underlying connection closed. Operator intervention
must establish that the old connection is closed before arranging a replacement
worker/process. A new worker object alone is not a cleanup mechanism. No automatic
forced close or service restart is provided.

`RECOVERING` lasts through stall cleanup, retry waits and reconnection setup.
Once subscriptions are installed, Phase 1 status evaluation resumes: retained
prices may still be `STALE` or `WAITING_FOR_DATA` until new data arrives.
Connection failures unrelated to a stall report `RECONNECTING`. Error logs
contain a local phase, category and retry delay, never raw exception text or
OAuth/network payloads. Third-party debug logging remains outside this guarantee.

## Limitations and rollback

An ES stall is a recovery heuristic, not proof of a broken connection. Unexpected
exchange halts, entitlement problems, an unsuitable selected contract or sparse
overnight quotes may still cause unnecessary attempts. A frozen upstream feed
that continues delivering cached or replayed ES events will defeat receipt-age
detection: every accepted receipt refreshes the local monotonic age and UTC
`updated_at`, regardless of how old its exchange timestamp is. Recent receipt
does not establish exchange-time progress. Exchange-timestamp progression is
not implemented; the actual DXLink event format, timestamp units and cached,
missing or out-of-order timestamp semantics must be verified before adding it.
SPY-only stalls and XSP inactivity deliberately cannot trigger this watchdog.
Synchronous SDK work can block the event loop; this change does not move SDK
authentication into another process/thread or add a global connection deadline.
There is no jitter, central coordination across service replicas, circuit breaker
or live Oracle verification. Unlisted closures must be handled by exclusion
dates or disabling the flag. Tests use simulated streams, not live SDK sockets.

For configuration rollback, set `DXLINK_WATCHDOG_ENABLED=false` (or remove the
variable) and intentionally restart the Python service through the normal
operator procedure. This leaves Phase 1 freshness reporting active, plus the
safer listener cleanup and fixed five-second retries. Disabled watchdog-only
configuration is ignored. For an exact code rollback,
restore the recorded Phase 1 revision using the existing deployment/rollback
procedure; do not overwrite credentials or shared environment files. None of
these operational actions is executed by this implementation task.

## Offline validation

Run `python -m pytest -q` with the requirements installed. Tests inject wall and
monotonic clocks and asynchronous sleeps, simulate Quote/Trade events and use
an AnyIO-backed streamer double. They cover calendar boundaries, both DST
transitions, holiday uncertainty, operator exclusions, clock changes, normal
activity, no initial ES event, stalls, cooldown, bounded retry failures, listener
completion/failure, cleanup ordering, idempotent start and shutdown without
orphan tasks. Regressions also cover teardown exceptions during shutdown,
recurring scheduling delays, disabled-mode configuration/retries, January 2027
warning rate limits and hanging cleanup diagnostics without forced replacement.
Close-failure regressions leave a fake streamer's open marker set, trigger the
real ES watchdog, and verify terminal suppression across retry/cooldown intervals
and repeated starts. A successful-close case verifies normal replacement and
fresh ES data still work.
Existing API/freshness and secret-sanitization tests remain active.
