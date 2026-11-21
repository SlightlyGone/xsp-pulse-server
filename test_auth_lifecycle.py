import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from httpx2 import MockTransport, Response
from tastytrade import Session as RealSession
import tastytrade.session as session_module
import tastytrade.streamer as sdk_streamer

import tasty_worker
from tasty_worker import XspPriceWorker
from test_support import SessionDouble


@pytest.fixture
def credentials(monkeypatch):
    monkeypatch.setenv("TASTYTRADE_CLIENT_SECRET", "fake-secret")
    monkeypatch.setenv("TASTYTRADE_REFRESH_TOKEN", "fake-refresh")
    monkeypatch.setenv("DXLINK_WATCHDOG_ENABLED", "true")


def test_real_sdk_refresh_boundary_and_http_lifetime(monkeypatch):
    now = [1000.0]
    refreshes = []
    monkeypatch.setattr(session_module.time, "time", lambda: now[0])

    def respond(request):
        if request.url.path == "/oauth/token":
            refreshes.append(True)
            return Response(200, json={"access_token": "fake-access", "expires_in": 900})
        return Response(200, json={"data": {"ok": True}})

    session = RealSession("fake-secret", "fake-refresh", transport=MockTransport(respond))

    async def run():
        async with session:
            await session._get("/probe")
            now[0] = 1839
            await session._get("/probe")
            assert len(refreshes) == 1
            now[0] = 1840
            await asyncio.gather(session._get("/probe"), session._get("/probe"))
            assert len(refreshes) == 2
        assert session._client.is_closed

    asyncio.run(run())


@pytest.mark.parametrize("code", ["invalid_grant", "invalid_client"])
def test_structured_oauth_errors_close_sessions_and_back_off(credentials, monkeypatch, caplog, code):
    sessions, delays = [], []

    def respond(request):
        assert request.url.path == "/oauth/token"
        return Response(400, json={"error": {"code": code, "message": "secret-marker"}})

    def factory(**kwargs):
        assert all(s._client.is_closed for s in sessions)
        session = RealSession(**kwargs, transport=MockTransport(respond))
        sessions.append(session)
        return session

    async def future_get(session, **kwargs):
        await session._get("/probe")
        pytest.fail("Permanent OAuth rejection unexpectedly authenticated")

    async def pause(seconds):
        assert all(s._client.is_closed for s in sessions)
        delays.append(seconds)
        if len(delays) == 3:
            raise asyncio.CancelledError

    monkeypatch.setattr(tasty_worker, "Session", factory)
    monkeypatch.setattr(tasty_worker.Future, "get", future_get)
    worker = XspPriceWorker(sleep=pause)
    asyncio.run(worker._stream_loop())
    # 13.2.3 flattens structured OAuth errors to plain TastytradeError strings.
    # We cannot reliably classify them as permanent without a structured boundary.
    assert delays == [5, 10, 20]
    assert len(sessions) == 3 and all(s._client.is_closed for s in sessions)
    assert "secret-marker" not in caplog.text


@pytest.mark.parametrize("mode", ["background", "unclosed", "shutdown", "entry_retry"])
def test_real_sdk_background_auth_exit_and_entry_recovery(credentials, monkeypatch, caplog, mode):
    caplog.set_level("WARNING", logger="tastytrade")
    sessions, sockets, requests = [], [], []
    subscribed = error_seen = None

    def respond(request):
        path = request.url.path
        requests.append(path)
        if path == "/oauth/token":
            return Response(200, json={"access_token": "fake-access", "expires_in": 900})
        if path == "/api-quote-tokens":
            if mode == "entry_retry" and requests.count(path) == 1:
                return Response(401, json={"error": {"code": "unauthorized", "message": "secret-marker"}})
            return Response(200, json={"data": {"dxlink-url": "wss://offline.invalid", "token": "fake-quote"}})
        return Response(200, json={"data": {"ok": True}})

    def factory(**kwargs):
        assert all(s._client.is_closed for s in sessions)
        assert not any(s.open for s in sockets)
        session = RealSession(**kwargs, transport=MockTransport(respond))
        sessions.append(session)
        return session

    async def future_get(session, **kwargs):
        await session._get("/probe")
        return [SimpleNamespace(active_month=True, streamer_symbol="es", symbol="/ESZ6")]

    class Socket:
        def __init__(self):
            self.open = True
            self.queue = asyncio.Queue()
            self.queue.put_nowait({"type": "AUTH_STATE", "state": "AUTHORIZED"})
            self.subscriptions = 0

        async def send_json(self, message):
            if message["type"] == "CHANNEL_REQUEST":
                self.queue.put_nowait({"type": "CHANNEL_OPENED", "channel": message["channel"]})
            if message["type"] == "FEED_SUBSCRIPTION":
                self.subscriptions += 1
                if self.subscriptions == 2:
                    subscribed.set()

        async def receive_json(self):
            message = await self.queue.get()
            if message["type"] == "ERROR":
                error_seen.set()
            return message

    class Client:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        @asynccontextmanager
        async def websocket(self, *args, **kwargs):
            assert not any(s.open for s in sockets)
            socket = Socket()
            sockets.append(socket)
            try:
                yield socket
            finally:
                if mode == "unclosed":
                    raise OSError("secret-marker")
                socket.open = False

    async def pause(seconds):
        if seconds == 10:  # Watchdog polls should not advance virtual time here.
            await asyncio.Event().wait()
        else:
            assert mode == "entry_retry" and seconds == 5
            assert all(s._client.is_closed for s in sessions)
            await asyncio.sleep(0)

    monkeypatch.setattr(tasty_worker, "Session", factory)
    monkeypatch.setattr(tasty_worker.Future, "get", future_get)
    monkeypatch.setattr(sdk_streamer, "AsyncClient", Client)
    worker = XspPriceWorker(sleep=pause)

    async def run():
        nonlocal subscribed, error_seen
        subscribed = asyncio.Event()
        error_seen = asyncio.Event()
        await worker.start()
        await subscribed.wait()
        if mode == "entry_retry":
            assert len(sessions) == 2 and len(sockets) == 1
            assert requests.count("/oauth/token") == requests.count("/api-quote-tokens") == 2
            await worker.stop()
        else:
            # Actual 13.2.3 _reader -> AnyIO task group -> SDK context exit.
            sockets[0].queue.put_nowait({"type": "ERROR", "message": "Your authentication token has expired, reauthentication is required secret-marker"})
            if mode == "shutdown":
                await error_seen.wait()
                await worker.stop()
            else:
                await worker._task
                assert worker.connection_status == "CLEANUP_FAILED"
                assert worker._closure_failed
                await worker.start()
                assert len(sessions) == len(sockets) == 1
            assert sockets[0].open is (mode == "unclosed")
            await worker.stop()
        assert all(s._client.is_closed for s in sessions)
        assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]

    asyncio.run(asyncio.wait_for(run(), 3))
    assert "secret-marker" not in caplog.text


def test_session_cleanup_error_is_terminal(credentials, monkeypatch, caplog):
    class BrokenSession(SessionDouble):
        async def __aexit__(self, *args):
            raise OSError("secret-marker")

    attempts = []

    def factory(**kwargs):
        attempts.append(True)
        return BrokenSession()

    async def fail(session, **kwargs):
        raise OSError("secret-marker")

    monkeypatch.setattr(tasty_worker, "Session", factory)
    monkeypatch.setattr(tasty_worker.Future, "get", fail)
    worker = XspPriceWorker()

    async def run():
        await worker._stream_loop()
        await worker.start()

    asyncio.run(run())
    assert attempts == [True]
    assert worker.connection_status == "CLEANUP_FAILED"
    assert "secret-marker" not in caplog.text
