from fastapi.testclient import TestClient

import server
from server import app, worker
from tasty_worker import PriceSnapshot, XspPriceWorker

client = TestClient(app)


def test_mid_calculation():
    assert worker.compute_mid(580.10, 580.30) == "580.20"
    assert worker.compute_mid(None, 580.30) is None


def test_unauthorized_request_rejected():
    res = client.get("/price/xsp")
    assert res.status_code == 401


def test_authorized_price_snapshot(monkeypatch) -> None:
    monkeypatch.setenv("APP_API_KEY", "dev-secret")
    isolated = XspPriceWorker()
    isolated._set_all_status("STREAMING")
    isolated._record_snapshot("SPY", PriceSnapshot(mid="581.45"))
    monkeypatch.setattr(server, "worker", isolated)
    res = client.get("/price/xsp", headers={"Authorization": "Bearer dev-secret"})
    assert res.status_code == 200
    assert res.json()["mid"] == "581.45"
    assert res.json()["status"] == "STREAMING"
