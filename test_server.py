from fastapi.testclient import TestClient

from server import app, worker

client = TestClient(app)


def test_mid_calculation():
    assert worker.compute_mid(580.10, 580.30) == "580.20"
    assert worker.compute_mid(None, 580.30) is None


def test_unauthorized_request_rejected():
    res = client.get("/price/xsp")
    assert res.status_code == 401


def test_authorized_price_snapshot(monkeypatch) -> None:
    monkeypatch.setenv("APP_API_KEY", "dev-secret")
    worker.latest.mid = "581.45"
    worker.latest.status = "STREAMING"
    res = client.get("/price/xsp", headers={"Authorization": "Bearer dev-secret"})
    assert res.status_code == 200
    assert res.json()["mid"] == "581.45"
