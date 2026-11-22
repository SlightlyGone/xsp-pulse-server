import asyncio
import logging

import pytest

import tasty_worker
from tasty_worker import StreamerClosureFailed, StreamStalled, XspPriceWorker, safe_exception_types


def test_exception_summary_is_bounded_and_never_formats_upstream_data():
    class HostileError(Exception):
        def __str__(self):
            raise AssertionError("Exception messages must not be read")

        def __repr__(self):
            raise AssertionError("Exception repr must not be read")

    HostileError.__name__ = "secret-type-marker"
    error = BaseExceptionGroup("secret-group-marker", [
        asyncio.CancelledError("secret-cancel-marker"),
        ExceptionGroup("secret-nested-marker", [
            tasty_worker.TastytradeError("secret-token-marker"), HostileError(),
        ]),
    ])
    assert safe_exception_types(error) == (
        "BaseExceptionGroup[CancelledError,ExceptionGroup[TastytradeError,other]]"
    )
    wide = ExceptionGroup("secret-marker", [error.exceptions[1]] * 1000)
    assert "truncated" in safe_exception_types(wide)
    assert len(safe_exception_types(wide)) < 500
    deep = ValueError("secret-marker")
    for _ in range(20):
        deep = ExceptionGroup("secret-marker", [deep])
    assert safe_exception_types(deep).count("ExceptionGroup") == 5
    assert "truncated" in safe_exception_types(deep)


@pytest.mark.parametrize("resource", ["streamer", "session"])
@pytest.mark.parametrize("trigger,expected", [
    (None, "normal_exit"),
    (StreamStalled("secret-marker"), "watchdog_stall"),
    (OSError("secret-marker"), "listener_error"),
    (asyncio.CancelledError("secret-marker"), "cancellation"),
])
def test_cleanup_failure_context_and_sanitization(monkeypatch, caplog, resource, trigger, expected):
    monkeypatch.setenv("DXLINK_WATCHDOG_ENABLED", "false")
    now = [100.0]
    worker = XspPriceWorker(monotonic=lambda: now[0])
    worker._connected_at = 10.0
    worker._received_at["ES"] = 95.0

    class Manager:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            raise BaseExceptionGroup("secret-group-marker", [
                asyncio.CancelledError("secret-marker"),
                ExceptionGroup("secret-marker", [tasty_worker.TastytradeError("secret-token-marker")]),
            ])

    monkeypatch.setattr(tasty_worker, "DXLinkStreamer", lambda session: Manager())
    monkeypatch.setattr(tasty_worker, "Session", lambda **kwargs: Manager())

    async def run():
        context = worker._streamer_context(None) if resource == "streamer" else worker._session_context("secret", "refresh")
        with pytest.raises(StreamerClosureFailed):
            async with context:
                if trigger is not None:
                    raise trigger

    asyncio.run(run())
    assert worker._closure_failed
    assert f"trigger={expected} " in caplog.text
    assert "exception_types=BaseExceptionGroup[CancelledError,ExceptionGroup[TastytradeError]]" in caplog.text
    assert "connected_seconds=90.0 es_age_seconds=5.0 shutdown=False" in caplog.text
    assert "secret" not in caplog.text
    for record in caplog.records:
        assert record.exc_info is None and record.stack_info is None
        assert not any(isinstance(arg, BaseException) for arg in record.args)


def test_shutdown_and_missing_observation_context(monkeypatch, caplog):
    monkeypatch.setenv("DXLINK_WATCHDOG_ENABLED", "false")
    worker = XspPriceWorker()
    worker._shutdown_requested = True
    worker._capture_cleanup_trigger(asyncio.CancelledError("secret-marker"))
    worker._log_cleanup_failure("test cleanup", OSError("secret-marker"))
    assert "trigger=shutdown trigger_exception_types=CancelledError" in caplog.text
    assert "connected_seconds=None es_age_seconds=None shutdown=True" in caplog.text
    assert "secret-marker" not in caplog.text


def test_successful_exit_has_no_failure_diagnostics(monkeypatch, caplog):
    monkeypatch.setenv("DXLINK_WATCHDOG_ENABLED", "false")
    worker = XspPriceWorker()

    class Manager:
        async def __aexit__(self, *args):
            return False

    async def run():
        assert not await worker._close_streamer(Manager(), None, None, None)
        assert not await worker._close_session(Manager(), None, None, None)

    with caplog.at_level(logging.ERROR, logger="tasty_worker"):
        asyncio.run(run())
    assert not worker._closure_failed
    assert not caplog.records
