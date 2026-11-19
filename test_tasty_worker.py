import asyncio

import pytest
from fastapi.testclient import TestClient

import server
import tasty_worker
from tasty_worker import XspPriceWorker


def statuses(worker):
    return {snapshot.status for snapshot in worker.snapshots.values()}


def test_missing_credentials_updates_all_instruments(monkeypatch):
    monkeypatch.delenv("TASTYTRADE_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("TASTYTRADE_REFRESH_TOKEN", raising=False)
    worker = XspPriceWorker()

    asyncio.run(worker._stream_loop())

    assert statuses(worker) == {"MISSING_CREDENTIALS"}


@pytest.mark.parametrize("error_type", [OSError, tasty_worker.TastytradeError])
def test_retry_and_stop_sanitize_errors(monkeypatch, caplog, error_type):
    monkeypatch.setenv("TASTYTRADE_CLIENT_SECRET", "client-secret-marker")
    monkeypatch.setenv("TASTYTRADE_REFRESH_TOKEN", "refresh-token-marker")
    monkeypatch.setenv("APP_API_KEY", "api-key-marker")
    worker = XspPriceWorker()
    monkeypatch.setattr(server, "worker", worker)
    client = TestClient(server.app)

    def fail_session(**kwargs):
        assert statuses(worker) == {"CONNECTING"}
        raise error_type("oauth response client-secret-marker refresh-token-marker api-key-marker")

    async def retry_sleep(seconds):
        assert seconds == 5
        assert statuses(worker) == {"RECONNECTING"}
        for symbol in ("SPY", "XSP", "ES"):
            response = client.get("/price/xsp", params={"symbol": symbol},
                                  headers={"Authorization": "Bearer api-key-marker"})
            assert response.status_code == 200
            assert response.json()["status"] == "RECONNECTING"
            assert "marker" not in response.text
        raise asyncio.CancelledError

    monkeypatch.setattr(tasty_worker, "Session", fail_session)
    monkeypatch.setattr(tasty_worker.asyncio, "sleep", retry_sleep)

    async def run():
        try:
            await worker._stream_loop()
        except asyncio.CancelledError:
            pass

    asyncio.run(run())
    assert statuses(worker) == {"STOPPED"}
    assert "session setup" in caplog.text
    assert "marker" not in caplog.text
    assert "oauth response" not in caplog.text
