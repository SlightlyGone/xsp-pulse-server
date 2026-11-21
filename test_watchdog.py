import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from anyio import create_task_group

import tasty_worker
from es_schedule import es_session_start
from tasty_worker import PriceSnapshot, StreamStalled, XspPriceWorker


class Clock:
    def __init__(self, utc="2026-11-20T15:00:00+00:00"):
        self.utc = datetime.fromisoformat(utc)
        self.elapsed = 0.0
        self.ticks = 0
        self.limit = 1000
        self.on_tick = lambda: None

    def monotonic(self):
        return self.elapsed

    def utc_now(self):
        return self.utc

    async def sleep(self, seconds):
        self.elapsed += seconds
        self.utc += timedelta(seconds=seconds)
        self.ticks += 1
        self.on_tick()
        if self.ticks >= self.limit:
            raise asyncio.CancelledError
        await asyncio.sleep(0)


@pytest.fixture
def make_worker(monkeypatch):
    for name in ("DXLINK_WATCHDOG_ENABLED", "DXLINK_WATCHDOG_SECONDS", "DXLINK_RECOVERY_COOLDOWN_SECONDS",
                 "DXLINK_RETRY_INITIAL_SECONDS", "DXLINK_RETRY_MAX_SECONDS", "DXLINK_WATCHDOG_EXCLUDED_DATES"):
        monkeypatch.delenv(name, raising=False)

    def make(utc="2026-11-20T15:00:00+00:00", enabled=True):
        monkeypatch.setenv("DXLINK_WATCHDOG_ENABLED", str(enabled).lower())
        clock = Clock(utc)
        worker = XspPriceWorker(monotonic=clock.monotonic, utc_now=clock.utc_now, sleep=clock.sleep)
        worker._set_all_status("STREAMING")
        return worker, clock

    return make


@pytest.mark.parametrize("utc,active", [
    ("2026-11-20T21:59:59+00:00", True),  # Friday before 16 CT
    ("2026-11-20T22:00:00+00:00", False),
    ("2026-11-21T15:00:00+00:00", False),
    ("2026-11-22T22:59:59+00:00", False),
    ("2026-11-22T23:00:00+00:00", True),
    ("2026-11-23T22:00:00+00:00", False),  # Daily break
    ("2026-11-23T22:59:59+00:00", False),
    ("2026-11-23T23:00:00+00:00", True),
    ("2026-03-06T22:00:00+00:00", False),  # Before DST weekend
    ("2026-03-08T21:59:59+00:00", False),
    ("2026-03-08T22:00:00+00:00", True),  # CDT open at 22 UTC
    ("2026-11-01T22:59:59+00:00", False),
    ("2026-11-01T23:00:00+00:00", True),  # CST open at 23 UTC
    ("2026-11-26T15:00:00+00:00", False),  # Uncertain Thanksgiving
    ("2026-11-27T15:00:00+00:00", False),  # Early-close uncertainty
    ("2026-11-25T15:00:00+00:00", False),  # Buffer day
    ("2026-04-03T15:00:00+00:00", False),  # Good Friday
    ("2027-02-02T15:00:00+00:00", False),  # Unreviewed year
])
def test_schedule_and_dst(utc, active):
    assert (es_session_start(datetime.fromisoformat(utc)) is not None) is active


def test_uncertain_clock_or_missing_timezone_suppresses_recovery(monkeypatch):
    assert es_session_start(datetime(2026, 11, 20, 15)) is None
    import es_schedule
    from zoneinfo import ZoneInfoNotFoundError

    def missing_zone(name):
        raise ZoneInfoNotFoundError(name)

    monkeypatch.setattr(es_schedule, "ZoneInfo", missing_zone)
    assert es_session_start(datetime(2026, 11, 20, 15, tzinfo=UTC)) is None


def test_operator_excluded_dates_suppress_whole_overnight_session(make_worker, monkeypatch):
    monkeypatch.setenv("DXLINK_WATCHDOG_ENABLED", "true")
    monkeypatch.setenv("DXLINK_WATCHDOG_EXCLUDED_DATES", "2026-11-20, 2026-11-23")
    worker = XspPriceWorker()
    for utc in ("2026-11-20T15:00:00+00:00", "2026-11-22T23:30:00+00:00"):
        assert es_session_start(datetime.fromisoformat(utc), worker._excluded_dates) is None


def test_watchdog_defaults_disabled_and_independent(make_worker, monkeypatch):
    monkeypatch.delenv("DXLINK_WATCHDOG_ENABLED", raising=False)
    monkeypatch.setenv("ES_FRESHNESS_SECONDS", "12")
    worker = XspPriceWorker()
    assert not worker.watchdog_enabled
    assert worker.watchdog_seconds == 300
    assert worker.recovery_cooldown == 600
    assert worker.retry_initial == 5
    assert worker.retry_max == 5


@pytest.mark.parametrize("name,value", [
    ("DXLINK_WATCHDOG_ENABLED", "yes-secret-marker"),
    ("DXLINK_WATCHDOG_SECONDS", "0"),
    ("DXLINK_RECOVERY_COOLDOWN_SECONDS", "nan"),
    ("DXLINK_RETRY_INITIAL_SECONDS", "-1"),
    ("DXLINK_RETRY_MAX_SECONDS", "1"),
    ("DXLINK_WATCHDOG_EXCLUDED_DATES", "secret-marker"),
])
def test_configuration_validation_is_sanitized(make_worker, monkeypatch, name, value):
    monkeypatch.setenv("DXLINK_WATCHDOG_ENABLED", "true")
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError) as error:
        XspPriceWorker()
    assert "secret-marker" not in str(error.value)


def test_es_activity_prevents_recovery_without_xsp(make_worker):
    worker, clock = make_worker()
    clock.limit = 100
    clock.on_tick = lambda: worker._record_snapshot("ES", PriceSnapshot(symbol="/ESZ6"))
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(worker._watchdog())
    assert worker.connection_status == "STREAMING"
    assert worker.observation_age("XSP") is None


@pytest.mark.parametrize("supporting", [None, "SPY", "XSP"])
def test_stall_requires_es_even_if_other_events_continue(make_worker, supporting):
    worker, clock = make_worker()
    if supporting:
        clock.on_tick = lambda: worker._record_snapshot(supporting, PriceSnapshot())
    with pytest.raises(StreamStalled):
        asyncio.run(worker._watchdog())
    assert clock.elapsed == 300
    assert worker.connection_status == "RECOVERING"
    assert worker.get_snapshot("ES").status == "RECOVERING"


@pytest.mark.parametrize("utc", ["2026-11-21T15:00:00+00:00", "2026-11-23T22:00:00+00:00",
                                 "2026-11-26T15:00:00+00:00", "2027-02-02T15:00:00+00:00"])
def test_no_recovery_during_closed_or_uncertain_periods(make_worker, utc):
    worker, clock = make_worker(utc)
    clock.limit = 40
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(worker._watchdog())
    assert worker.connection_status == "STREAMING"


def test_cooldown_limits_repeated_stalls(make_worker):
    worker, clock = make_worker()
    worker._last_recovery_at = 0
    with pytest.raises(StreamStalled):
        asyncio.run(worker._watchdog())
    assert clock.elapsed == 600


def test_break_reopen_gets_full_grace(make_worker):
    worker, clock = make_worker("2026-11-23T21:59:00+00:00")
    with pytest.raises(StreamStalled):
        asyncio.run(worker._watchdog())
    assert clock.utc == datetime.fromisoformat("2026-11-23T23:05:00+00:00")


@pytest.mark.parametrize("jump", [3600, -3600])
def test_wall_clock_discontinuity_rearms_grace(make_worker, jump):
    worker, clock = make_worker()

    def tick():
        if clock.ticks == 20:
            clock.utc += timedelta(seconds=jump)

    clock.on_tick = tick
    with pytest.raises(StreamStalled):
        asyncio.run(worker._watchdog())
    assert clock.elapsed == 500


def test_new_connection_ignores_old_es_age(make_worker):
    worker, clock = make_worker()
    worker._record_snapshot("ES", PriceSnapshot())
    clock.elapsed = 1000
    with pytest.raises(StreamStalled):
        asyncio.run(worker._watchdog())
    assert clock.elapsed == 1300


@pytest.mark.parametrize("failures", [0, 2])
def test_serial_recovery_cleans_tasks_recreates_session_and_shuts_down(make_worker, monkeypatch, caplog, failures):
    worker, clock = make_worker()
    monkeypatch.setenv("TASTYTRADE_CLIENT_SECRET", "fake-secret")
    monkeypatch.setenv("TASTYTRADE_REFRESH_TOKEN", "fake-refresh")
    attempts, retry_sleeps = [], []
    instances = []
    ready = None

    def session(**kwargs):
        assert all(not stream.open and stream.listeners == 0 for stream in instances)
        attempts.append(clock.elapsed)
        if 1 < len(attempts) <= 1 + failures:
            raise OSError("credential-secret-marker")
        return object()

    async def future_get(session, product_codes):
        return [SimpleNamespace(active_month=True, streamer_symbol="/ESZ6:XCME", symbol="/ESZ6")]

    monkeypatch.setattr(tasty_worker, "Session", session)
    monkeypatch.setattr(tasty_worker.Future, "get", future_get)

    class Streamer:
        def __init__(self, session):
            self.open = False
            self.listeners = 0
            self.subscriptions = []
            instances.append(self)

        @asynccontextmanager
        async def context(self):
            assert not any(s.open for s in instances if s is not self)
            self.open = True
            try:
                async with create_task_group() as group:
                    group.start_soon(asyncio.Event().wait)
                    yield self
                    group.cancel_scope.cancel()
            finally:
                assert self.listeners == 0
                self.open = False

        async def __aenter__(self):
            self.cm = self.context()
            return await self.cm.__aenter__()

        async def __aexit__(self, *args):
            return await self.cm.__aexit__(*args)

        async def subscribe(self, kind, symbols):
            self.subscriptions.append((kind, symbols))

        async def listen(self, kind):
            self.listeners += 1
            try:
                if len(instances) == 2 and kind is tasty_worker.Quote:
                    yield SimpleNamespace(event_symbol="/ESZ6:XCME", bid_price=100, ask_price=102)
                    ready.set()
                await asyncio.Event().wait()
            finally:
                self.listeners -= 1

    async def sleep(seconds):
        if not any(stream.open for stream in instances):
            assert worker.get_snapshot("ES").status == "RECOVERING"
            retry_sleeps.append(seconds)
        await clock.sleep(seconds)

    worker._sleep_fn = sleep
    monkeypatch.setattr(tasty_worker, "DXLinkStreamer", Streamer)

    async def run():
        nonlocal ready
        ready = asyncio.Event()
        await worker.start()
        task = worker._task
        await worker.start()  # Idempotent; no concurrent recovery owner.
        assert worker._task is task
        await ready.wait()
        assert worker.get_snapshot("ES").mid == "101.00"
        await worker.stop()
        assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]

    asyncio.run(run())
    assert len(instances) == 2
    assert all(not s.open and s.listeners == 0 for s in instances)
    assert all(s.subscriptions == [(tasty_worker.Quote, ["SPY", "/ESZ6:XCME"]), (tasty_worker.Trade, ["XSP"])] for s in instances)
    assert retry_sleeps == ([5] if failures == 0 else [5, 10, 20])
    assert worker.connection_status == "STOPPED"


@pytest.mark.parametrize("exit_error", [None, OSError, ValueError, ExceptionGroup, asyncio.CancelledError])
def test_ordinary_recovery_requires_confirmed_streamer_exit(make_worker, monkeypatch, caplog, exit_error):
    worker, clock = make_worker()
    monkeypatch.setenv("TASTYTRADE_CLIENT_SECRET", "fake-secret")
    monkeypatch.setenv("TASTYTRADE_REFRESH_TOKEN", "fake-refresh")
    instances, attempts, retries = [], [], []
    ready = None

    def session(**kwargs):
        assert not any(s.open for s in instances)
        attempts.append(True)
        return object()

    async def futures(session, product_codes):
        return [SimpleNamespace(active_month=True, streamer_symbol="es", symbol="/ESZ6")]

    class Streamer:
        def __init__(self, session):
            self.open = False
            self.listeners = 0
            instances.append(self)

        async def __aenter__(self):
            assert not any(s.open for s in instances)
            self.open = True
            return self

        async def subscribe(self, *args):
            pass

        async def __aexit__(self, *args):
            assert self.listeners == 0
            if exit_error is not None:
                # Failure deliberately retains the original open connection.
                if exit_error is ExceptionGroup:
                    raise ExceptionGroup("token-secret-marker", [OSError("token-secret-marker")])
                raise exit_error("token-secret-marker")
            self.open = False

        async def listen(self, kind):
            self.listeners += 1
            try:
                if len(instances) == 2 and kind is tasty_worker.Quote:
                    yield SimpleNamespace(event_symbol="es", bid_price=100, ask_price=102)
                    ready.set()
                await asyncio.Event().wait()
            finally:
                self.listeners -= 1

    async def pause(seconds):
        if not any(s.open for s in instances):
            retries.append(seconds)
        await clock.sleep(seconds)

    monkeypatch.setattr(tasty_worker, "Session", session)
    monkeypatch.setattr(tasty_worker.Future, "get", futures)
    monkeypatch.setattr(tasty_worker, "DXLinkStreamer", Streamer)
    worker._sleep_fn = pause

    async def run():
        nonlocal ready
        ready = asyncio.Event()
        await worker.start()
        if exit_error is None:
            await ready.wait()
            assert not instances[0].open and instances[1].open
            assert worker.get_snapshot("ES").mid == "101.00"
        else:
            await worker._task  # Real watchdog reaches its 300-second deadline.
            assert instances[0].open
            assert worker.connection_status == "CLEANUP_FAILED"
            assert retries == []
            for _ in range(3):
                clock.elapsed += worker.recovery_cooldown + worker.retry_max + 1
                clock.utc += timedelta(seconds=worker.recovery_cooldown + worker.retry_max + 1)
                await worker.start()
                await worker._stream_loop()  # Direct entry is also blocked.
                await asyncio.sleep(0)
                assert worker.get_snapshot("ES").status == "CLEANUP_FAILED"
            assert len(attempts) == len(instances) == 1
        await worker.stop()
        await worker.start()
        assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]

    asyncio.run(run())
    assert all(s.listeners == 0 for s in instances)
    assert len(instances) == (2 if exit_error is None else 1)
    if exit_error is None:
        assert retries == [5]
        assert not any(s.open for s in instances)
    else:
        assert worker._closure_failed
        assert "closure unconfirmed; recovery suppressed" in caplog.text
    assert "token-secret-marker" not in caplog.text
    assert "credential-secret-marker" not in caplog.text


def test_retry_backoff_is_bounded_and_cancellation_stops(make_worker, monkeypatch):
    worker, clock = make_worker()
    monkeypatch.setenv("TASTYTRADE_CLIENT_SECRET", "fake-secret")
    monkeypatch.setenv("TASTYTRADE_REFRESH_TOKEN", "fake-refresh")
    delays = []

    def fail(**kwargs):
        raise tasty_worker.TastytradeError("secret-marker")

    async def sleep(seconds):
        delays.append(seconds)
        if len(delays) == 9:
            raise asyncio.CancelledError

    monkeypatch.setattr(tasty_worker, "Session", fail)
    worker._sleep_fn = sleep
    asyncio.run(worker._stream_loop())
    assert delays == [5, 10, 20, 40, 80, 160, 300, 300, 300]
    assert worker.connection_status == "STOPPED"


def test_disabled_watchdog_and_listener_failure_cleanup(make_worker):
    worker, _ = make_worker(enabled=False)
    closed = []

    async def fail():
        await asyncio.sleep(0)
        raise OSError("secret-marker")

    async def waiting():
        try:
            await asyncio.Event().wait()
        finally:
            closed.append(True)

    async def must_not_run():
        pytest.fail("Disabled watchdog started")

    worker._watchdog = must_not_run
    with pytest.raises(OSError):
        asyncio.run(worker._run_listeners(fail(), waiting()))
    assert closed == [True]


def test_shutdown_interrupts_recovery_backoff(make_worker, monkeypatch):
    worker, _ = make_worker()
    monkeypatch.setenv("TASTYTRADE_CLIENT_SECRET", "fake-secret")
    monkeypatch.setenv("TASTYTRADE_REFRESH_TOKEN", "fake-refresh")
    sleeping = None

    def fail(**kwargs):
        # Match SDK background failures wrapped by an AnyIO task group.
        raise ExceptionGroup("secret-marker", [OSError("secret-marker")])

    async def pause(seconds):
        sleeping.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(tasty_worker, "Session", fail)
    worker._sleep_fn = pause

    async def run():
        nonlocal sleeping
        sleeping = asyncio.Event()
        await worker.start()
        await sleeping.wait()
        assert worker.connection_status == "RECONNECTING"
        await worker.stop()
        assert worker._task.done()
        assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]

    asyncio.run(run())
    assert worker.connection_status == "STOPPED"


def test_listener_normal_exit_cancels_sibling(make_worker):
    worker, _ = make_worker(enabled=False)
    closed = []

    async def ended():
        return

    async def waiting():
        try:
            await asyncio.Event().wait()
        finally:
            closed.append(True)

    with pytest.raises(OSError, match="Market-data listener ended"):
        asyncio.run(worker._run_listeners(ended(), waiting()))
    assert closed == [True]


def test_sdk_background_failure_cancels_and_awaits_listeners(make_worker):
    worker, _ = make_worker()
    closed = []

    async def waiting():
        try:
            await asyncio.Event().wait()
        finally:
            closed.append(True)

    async def sdk_failure():
        await asyncio.sleep(0)
        raise OSError("secret-marker")

    async def run():
        with pytest.raises(ExceptionGroup):
            async with create_task_group() as group:
                group.start_soon(sdk_failure)
                await worker._run_listeners(waiting(), waiting())
        assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]

    asyncio.run(run())
    assert closed == [True, True]


@pytest.mark.parametrize("enabled", [False, True])
def test_healthy_connection_resets_retry_backoff(make_worker, monkeypatch, enabled):
    worker, clock = make_worker(enabled=enabled)
    monkeypatch.setenv("TASTYTRADE_CLIENT_SECRET", "fake-secret")
    monkeypatch.setenv("TASTYTRADE_REFRESH_TOKEN", "fake-refresh")
    attempts, delays = [], []

    def session(**kwargs):
        attempts.append(True)
        if len(attempts) != 2:
            raise OSError("secret-marker")
        return object()

    async def futures(session, product_codes):
        return [SimpleNamespace(active_month=True, streamer_symbol="/ESZ6:XCME", symbol="/ESZ6")]

    class Streamer:
        def __init__(self, session):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def subscribe(self, kind, symbols):
            pass

        async def listen(self, kind):
            if kind is tasty_worker.Quote:
                clock.elapsed += 300
                yield SimpleNamespace(event_symbol="/ESZ6:XCME", bid_price=100, ask_price=102)
                clock.elapsed += 1
                raise OSError("secret-marker")
            await asyncio.Event().wait()

    async def sleep(seconds):
        delays.append(seconds)
        if len(delays) == 3:
            raise asyncio.CancelledError
        clock.elapsed += seconds

    monkeypatch.setattr(tasty_worker, "Session", session)
    monkeypatch.setattr(tasty_worker.Future, "get", futures)
    monkeypatch.setattr(tasty_worker, "DXLinkStreamer", Streamer)
    async def waiting_watchdog():
        await asyncio.Event().wait()

    monkeypatch.setattr(worker, "_watchdog", waiting_watchdog)
    worker._sleep_fn = sleep
    asyncio.run(worker._stream_loop())
    assert delays == ([5, 5, 10] if enabled else [5, 5, 5])
    assert worker.connection_status == "STOPPED"


def test_recurring_polling_delay_retains_inactivity(make_worker):
    worker, clock = make_worker()

    async def delayed(seconds):
        clock.ticks += 1
        gap = 31 if clock.ticks % 20 == 0 else 10
        clock.elapsed += gap
        clock.utc += timedelta(seconds=gap)
        if clock.ticks > 100:
            pytest.fail("ES silence never recovered under recurring scheduling delays")

    worker._sleep_fn = delayed
    with pytest.raises(StreamStalled):
        asyncio.run(worker._watchdog())
    assert clock.elapsed == 301


def test_long_poll_crossing_break_rearms_grace(make_worker):
    worker, clock = make_worker("2026-11-23T21:59:00+00:00")

    async def delayed(seconds):
        gap = 3660 if clock.ticks == 0 else seconds
        clock.ticks += 1
        clock.elapsed += gap
        clock.utc += timedelta(seconds=gap)

    worker._sleep_fn = delayed
    with pytest.raises(StreamStalled):
        asyncio.run(worker._watchdog())
    assert clock.utc == datetime.fromisoformat("2026-11-23T23:05:00+00:00")


@pytest.mark.parametrize("enabled", [None, "false"])
def test_disabled_ignores_invalid_watchdog_settings_and_retries_every_five_seconds(make_worker, monkeypatch, enabled):
    if enabled is not None:
        monkeypatch.setenv("DXLINK_WATCHDOG_ENABLED", enabled)
    for name in ("DXLINK_WATCHDOG_SECONDS", "DXLINK_RECOVERY_COOLDOWN_SECONDS", "DXLINK_RETRY_INITIAL_SECONDS",
                 "DXLINK_RETRY_MAX_SECONDS", "DXLINK_WATCHDOG_EXCLUDED_DATES"):
        monkeypatch.setenv(name, "invalid-secret-marker")
    worker = XspPriceWorker()
    assert worker._excluded_dates == frozenset()
    assert worker.retry_initial == worker.retry_max == 5
    monkeypatch.setenv("TASTYTRADE_CLIENT_SECRET", "fake-secret")
    monkeypatch.setenv("TASTYTRADE_REFRESH_TOKEN", "fake-refresh")
    delays = []

    def fail(**kwargs):
        raise OSError("secret-marker")

    async def pause(seconds):
        delays.append(seconds)
        if len(delays) == 6:
            raise asyncio.CancelledError

    monkeypatch.setattr(tasty_worker, "Session", fail)
    worker._sleep_fn = pause
    asyncio.run(worker._stream_loop())
    assert delays == [5] * 6
    monkeypatch.setenv("ES_FRESHNESS_SECONDS", "invalid")
    with pytest.raises(ValueError, match="ES_FRESHNESS_SECONDS"):
        XspPriceWorker()  # General freshness configuration still matters.


@pytest.mark.parametrize("enabled", [False, True])
def test_january_2027_expiry_warning_is_sanitized_rate_limited_and_survives_reconnect(make_worker, caplog, enabled):
    worker, clock = make_worker("2027-01-01T05:59:50+00:00", enabled=enabled)
    assert worker._session_start(clock.utc) is None  # Still 2026 in Chicago; holiday.
    assert "calendar coverage expired" not in caplog.text
    clock.utc += timedelta(seconds=10)
    clock.elapsed += 10
    for _ in range(100):
        assert worker._session_start(clock.utc) is None
    warnings = lambda: [r for r in caplog.records if "calendar coverage expired" in r.message]
    assert len(warnings()) == int(enabled)
    clock.utc += timedelta(hours=1)
    clock.elapsed += 3599
    worker._session_start(clock.utc)
    assert len(warnings()) == int(enabled)
    clock.elapsed += 1
    worker._session_start(clock.utc)
    assert len(warnings()) == 2 * int(enabled)
    if enabled:
        # New watchdog instances in the same worker must share the warning limit.
        clock.limit = 40
        for _ in range(2):
            clock.ticks = 0
            with pytest.raises(asyncio.CancelledError):
                asyncio.run(worker._watchdog())
        assert len(warnings()) == 2
        assert worker._last_recovery_at is None
    assert "secret-marker" not in caplog.text


@pytest.mark.parametrize("error", [OSError, tasty_worker.TastytradeError, ValueError, ExceptionGroup])
def test_shutdown_teardown_error_cannot_reconnect_or_sleep(make_worker, monkeypatch, caplog, error):
    worker, _ = make_worker()
    monkeypatch.setenv("TASTYTRADE_CLIENT_SECRET", "fake-secret")
    monkeypatch.setenv("TASTYTRADE_REFRESH_TOKEN", "fake-refresh")
    attempts = []
    entered = None
    closed = []

    def session(**kwargs):
        attempts.append(True)
        return object()

    async def futures(session, product_codes):
        return [SimpleNamespace(active_month=True, streamer_symbol="es", symbol="/ESZ6")]

    class Streamer:
        def __init__(self, session):
            pass

        async def __aenter__(self):
            return self

        async def subscribe(self, *args):
            pass

        async def __aexit__(self, kind, value, tb):
            assert worker._shutdown_requested
            assert kind is asyncio.CancelledError
            if error is ExceptionGroup:
                raise ExceptionGroup("secret-marker", [OSError("secret-marker")])
            raise error("secret-marker")

        async def listen(self, kind):
            try:
                if kind is tasty_worker.Quote:
                    entered.set()
                await asyncio.Event().wait()
                yield  # Async generator, never sends a real event.
            finally:
                closed.append(kind)

    async def must_not_sleep(seconds):
        pytest.fail("Shutdown initiated a new retry/watchdog sleep")

    monkeypatch.setattr(tasty_worker, "Session", session)
    monkeypatch.setattr(tasty_worker.Future, "get", futures)
    monkeypatch.setattr(tasty_worker, "DXLinkStreamer", Streamer)

    async def waiting_watchdog():
        await asyncio.Event().wait()

    monkeypatch.setattr(worker, "_watchdog", waiting_watchdog)

    async def run():
        nonlocal entered
        entered = asyncio.Event()
        await worker.start()
        await entered.wait()
        worker._sleep_fn = must_not_sleep
        await worker.stop()
        await worker.start()  # Intentional shutdown stays latched for this worker.
        await worker._stream_loop()
        await worker.stop()
        assert worker._task.done()
        assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]

    asyncio.run(run())
    assert len(attempts) == 1
    assert set(closed) == {tasty_worker.Quote, tasty_worker.Trade}
    assert worker.connection_status == "STOPPED"
    assert "recovery suppressed" in caplog.text
    assert "secret-marker" not in caplog.text


def test_shutdown_before_start_is_persistent(make_worker, monkeypatch):
    worker, _ = make_worker()

    def must_not_connect(**kwargs):
        pytest.fail("Stopped worker connected")

    monkeypatch.setattr(tasty_worker, "Session", must_not_connect)

    async def run():
        await worker.stop()
        await worker.start()
        await worker._stream_loop()

    asyncio.run(run())
    assert worker._task is None
    assert worker.connection_status == "STOPPED"


@pytest.mark.parametrize("phase", ["listeners", "DXLink streamer"])
@pytest.mark.parametrize("shutdown", [False, True])
def test_hanging_cleanup_warns_without_overlap_or_forced_close(make_worker, monkeypatch, caplog, phase, shutdown):
    worker, _ = make_worker()
    monkeypatch.setenv("TASTYTRADE_CLIENT_SECRET", "fake-secret")
    monkeypatch.setenv("TASTYTRADE_REFRESH_TOKEN", "fake-refresh")
    entered = pending = release = stall = retrying = None
    attempts, alarms = [], []

    class Alarm:
        cancelled = False

        def __init__(self, callback, args):
            self.callback, self.args = callback, args

        def cancel(self):
            self.cancelled = True

        def fire(self):
            if not self.cancelled:
                self.callback(*self.args)

    def session(**kwargs):
        attempts.append(True)
        return object()

    async def futures(session, product_codes):
        return [SimpleNamespace(active_month=True, streamer_symbol="es", symbol="/ESZ6")]

    class Streamer:
        def __init__(self, session):
            pass

        async def __aenter__(self):
            return self

        async def subscribe(self, *args):
            pass

        async def __aexit__(self, *args):
            if phase == "DXLink streamer":
                pending.set()
                await release.wait()

        async def listen(self, kind):
            try:
                if kind is tasty_worker.Quote:
                    entered.set()
                await asyncio.Event().wait()
                yield
            finally:
                if phase == "listeners" and kind is tasty_worker.Quote:
                    pending.set()
                    await release.wait()

    async def waiting_watchdog():
        await stall.wait()
        worker._set_all_status("RECOVERING")
        raise StreamStalled

    async def pause(seconds):
        retrying.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(tasty_worker, "Session", session)
    monkeypatch.setattr(tasty_worker.Future, "get", futures)
    monkeypatch.setattr(tasty_worker, "DXLinkStreamer", Streamer)
    monkeypatch.setattr(worker, "_watchdog", waiting_watchdog)
    worker._sleep_fn = pause

    async def run():
        nonlocal entered, pending, release, stall, retrying
        entered, pending, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        stall, retrying = asyncio.Event(), asyncio.Event()
        loop = asyncio.get_running_loop()
        original = loop.call_later

        def call_later(delay, callback, *args, **kwargs):
            if delay == 30:
                alarm = Alarm(callback, args)
                alarms.append(alarm)
                return alarm
            return original(delay, callback, *args, **kwargs)

        monkeypatch.setattr(loop, "call_later", call_later)
        await worker.start()
        await entered.wait()
        stopping = second_stop = None
        if shutdown:
            stopping = asyncio.create_task(worker.stop())
        else:
            stall.set()
        await pending.wait()
        await worker.start()
        if shutdown:
            second_stop = asyncio.create_task(worker.stop())
        await asyncio.sleep(0)
        alarm = next(a for a in alarms if not a.cancelled)
        assert "cleanup still pending" not in caplog.text
        alarm.fire()  # Deterministic diagnostic deadline, no wall-time sleep.
        assert f"{phase} cleanup still pending after 30 seconds; replacement blocked" in caplog.text
        assert not worker._task.done()
        assert not retrying.is_set()
        assert worker._shutdown_requested is shutdown
        assert len(attempts) == 1
        release.set()
        if shutdown:
            assert not stopping.done() and not second_stop.done()
            await asyncio.gather(stopping, second_stop)
        else:
            await retrying.wait()  # Retry only becomes possible after confirmed exit.
            await worker.stop()
        assert all(a.cancelled for a in alarms)
        assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]

    asyncio.run(run())
    assert worker.connection_status == "STOPPED"
