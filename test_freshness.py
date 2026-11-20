import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

import server
import tasty_worker
from tasty_worker import PriceSnapshot, XspPriceWorker


class Clock:
    elapsed = 0.0
    utc = datetime(2026, 11, 20, 15, tzinfo=UTC)

    def monotonic(self):
        return self.elapsed

    def utc_now(self):
        return self.utc


@pytest.fixture
def observed(monkeypatch):
    for key in ("SPY", "XSP", "ES"):
        monkeypatch.delenv(f"{key}_FRESHNESS_SECONDS", raising=False)
    clock = Clock()
    worker = XspPriceWorker(monotonic=clock.monotonic, utc_now=clock.utc_now)
    worker._set_all_status("STREAMING")
    return worker, clock


@pytest.mark.parametrize("key,threshold", [("SPY", 120), ("ES", 120), ("XSP", 900)])
def test_fresh_boundary_stale_and_recovery(observed, key, threshold):
    worker, clock = observed
    worker._record_snapshot(key, PriceSnapshot(symbol=key, mid="100.00"))
    assert worker.observation_age(key) == 0  # Zero is a valid monotonic timestamp.
    original = worker.get_snapshot(key)
    assert original.status == "STREAMING"
    clock.elapsed = threshold
    assert worker.get_snapshot(key).status == "STREAMING"
    clock.elapsed += 0.01
    stale = worker.get_snapshot(key)
    assert stale.status == "STALE"
    assert stale.mid == original.mid
    assert stale.updated_at == original.updated_at
    assert worker.connection_status == "STREAMING"
    worker._record_snapshot(key, PriceSnapshot(symbol=key, mid="100.00"))
    assert worker.get_snapshot(key).status == "STREAMING"  # Same price still counts.


def test_no_initial_observation_is_not_connection_failure(observed):
    worker, clock = observed
    clock.elapsed = 1_000_000
    for key in ("SPY", "XSP", "ES"):
        snapshot = worker.get_snapshot(key)
        assert snapshot.status == "WAITING_FOR_DATA"
        assert snapshot.updated_at == ""
        assert snapshot.mid is None
        assert worker.observation_age(key) is None
    assert worker.connection_status == "STREAMING"


@pytest.mark.parametrize("state", ["DISCONNECTED", "CONNECTING", "RECONNECTING", "STOPPED", "MISSING_CREDENTIALS"])
def test_connection_state_takes_precedence_without_erasing_age(observed, state):
    worker, clock = observed
    worker._record_snapshot("SPY", PriceSnapshot(mid="100.00"))
    clock.elapsed = 1000
    worker._set_all_status(state)
    assert worker.get_snapshot("SPY").status == state
    assert worker.get_snapshot("XSP").status == state
    assert worker.observation_age("SPY") == 1000
    worker._set_all_status("STREAMING")
    assert worker.get_snapshot("SPY").status == "STALE"
    assert worker.get_snapshot("XSP").status == "WAITING_FOR_DATA"


def test_wall_clock_jumps_do_not_control_freshness(observed):
    worker, clock = observed
    worker._record_snapshot("SPY", PriceSnapshot())
    updated = worker.latest.updated_at
    clock.utc += timedelta(days=100)
    assert worker.latest.status == "STREAMING"
    clock.utc -= timedelta(days=200)
    clock.elapsed = 121
    assert worker.latest.status == "STALE"
    assert worker.latest.updated_at == updated
    worker._record_snapshot("SPY", PriceSnapshot())
    assert worker.latest.updated_at == clock.utc.isoformat()
    assert worker.latest.status == "STREAMING"


def test_instruments_age_independently(observed):
    worker, clock = observed
    for key in ("SPY", "ES", "XSP"):
        worker._record_snapshot(key, PriceSnapshot(symbol=key))
    clock.elapsed = 901
    worker._record_snapshot("ES", PriceSnapshot(symbol="/ESZ6"))
    assert worker.get_snapshot("spy").status == "STALE"
    assert worker.get_snapshot("XSP").status == "STALE"
    assert worker.get_snapshot("/es").status == "STREAMING"
    assert worker.get_snapshot("/es").symbol == "/ESZ6"
    assert worker.observation_age("XSP") == 901
    assert worker.observation_age("ES") == 0


def test_api_retrieval_ages_without_events_and_preserves_schema(observed, monkeypatch):
    worker, clock = observed
    monkeypatch.setattr(server, "worker", worker)
    monkeypatch.setenv("APP_API_KEY", "test-key")
    worker._record_snapshot("ES", PriceSnapshot(symbol="/ESZ6", bid="6000.00", ask="6000.50", mid="6000.25"))
    client = TestClient(server.app)  # No lifespan, so no real stream starts.
    headers = {"Authorization": "Bearer test-key"}
    fresh = client.get("/price/xsp?symbol=/es", headers=headers).json()
    assert fresh["status"] == "STREAMING"
    assert set(fresh) == {"symbol", "bid", "ask", "mid", "status", "updated_at"}
    clock.elapsed = 121
    stale = client.get("/price/xsp?symbol=ES", headers=headers).json()
    assert stale == {**fresh, "status": "STALE"}
    assert fresh["status"] == "STREAMING"
    assert client.get("/price/xsp?symbol=ES").status_code == 401
    assert client.get("/price/xsp?symbol=unknown", headers=headers).status_code == 400
    assert client.get("/health").json() == {"ok": True}


def test_threshold_configuration(observed, monkeypatch):
    monkeypatch.setenv("SPY_FRESHNESS_SECONDS", "42.5")
    monkeypatch.setenv("XSP_FRESHNESS_SECONDS", "1800")
    worker = XspPriceWorker()
    assert worker.freshness_seconds == {"SPY": 42.5, "XSP": 1800, "ES": 120}


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "secret-marker"])
def test_invalid_threshold_is_rejected_without_echoing_value(observed, monkeypatch, value):
    monkeypatch.setenv("ES_FRESHNESS_SECONDS", value)
    with pytest.raises(ValueError, match="ES_FRESHNESS_SECONDS must be a finite positive number") as error:
        XspPriceWorker()
    assert "secret-marker" not in str(error.value)


def test_stop_before_background_task_runs(observed, monkeypatch):
    worker, _ = observed

    async def must_not_run():
        pytest.fail("Cancelled task should not start")

    monkeypatch.setattr(worker, "_stream_loop", must_not_run)

    async def run():
        await worker.start()
        await worker.stop()

    asyncio.run(run())
    assert worker.get_snapshot("SPY").status == "STOPPED"


def test_listeners_record_receipt_and_preserve_subscriptions(observed, monkeypatch):
    worker, clock = observed
    monkeypatch.setenv("TASTYTRADE_CLIENT_SECRET", "fake-secret")
    monkeypatch.setenv("TASTYTRADE_REFRESH_TOKEN", "fake-refresh")
    monkeypatch.setattr(tasty_worker, "Session", lambda **kwargs: object())

    async def futures(session, product_codes):
        assert product_codes == ["ES"]
        return [SimpleNamespace(active_month=True, streamer_symbol="/ESZ6:XCME", symbol="/ESZ6")]

    monkeypatch.setattr(tasty_worker.Future, "get", futures)

    class FakeStreamer:
        subscriptions = []

        def __init__(self, session):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def subscribe(self, event_type, symbols):
            self.subscriptions.append((event_type, symbols))

        async def listen(self, event_type):
            if event_type is tasty_worker.Quote:
                for symbol in ("unknown", "SPY", "/ESZ6:XCME"):
                    yield SimpleNamespace(event_symbol=symbol, bid_price=100, ask_price=102)
                await trades_done.wait()
                for key in ("SPY", "ES", "XSP"):
                    assert worker.observation_age(key) == 0
                raise asyncio.CancelledError
            else:
                yield SimpleNamespace(event_symbol="unknown", price=999)
                yield SimpleNamespace(event_symbol="XSP", price=None)
                yield SimpleNamespace(event_symbol="XSP", price=50)
                trades_done.set()

    monkeypatch.setattr(tasty_worker, "DXLinkStreamer", FakeStreamer)

    async def run():
        nonlocal trades_done
        trades_done = asyncio.Event()
        await worker._stream_loop()

    trades_done = None
    asyncio.run(run())
    assert FakeStreamer.subscriptions == [(tasty_worker.Quote, ["SPY", "/ESZ6:XCME"]), (tasty_worker.Trade, ["XSP"])]
    assert worker.get_snapshot("SPY").mid == "101.00"
    assert worker.get_snapshot("ES").symbol == "/ESZ6"
    assert worker.get_snapshot("XSP").mid == "50.00"
    assert worker.get_snapshot("XSP").updated_at == clock.utc.isoformat()
    assert worker.connection_status == "STOPPED"
