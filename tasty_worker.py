import asyncio
import logging
import math
import os
import time
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime
from decimal import Decimal
from threading import Lock

from anyio import CancelScope
from pydantic import BaseModel
from tastytrade import DXLinkStreamer, Session
from tastytrade.dxfeed import Quote, Trade
from tastytrade.instruments import Future
from tastytrade.utils import TastytradeError

from es_schedule import calendar_expired, es_session_start


logger = logging.getLogger(__name__)


class StreamStalled(Exception):
    """Local ES inactivity; never includes an upstream exception or payload."""


class StreamerClosureFailed(Exception):
    """Streamer exit did not confirm closure; this worker must never reconnect."""


def positive_seconds(name: str, default: float) -> float:
    try:
        value = float(os.environ.get(name, str(default)))
    except ValueError:
        raise ValueError(f"{name} must be a finite positive number") from None
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a finite positive number")
    return value


class PriceSnapshot(BaseModel):
    symbol: str = "SPY"
    bid: str | None = None
    ask: str | None = None
    mid: str | None = None
    status: str = "DISCONNECTED"
    updated_at: str = ""


class XspPriceWorker:
    def __init__(
        self,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        utc_now: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._monotonic = monotonic
        self._utc_now = utc_now
        self._sleep_fn = sleep
        enabled = os.environ.get("DXLINK_WATCHDOG_ENABLED", "false").lower()
        if enabled not in {"true", "false"}:
            raise ValueError("DXLINK_WATCHDOG_ENABLED must be true or false")
        self.watchdog_enabled = enabled == "true"
        self.watchdog_seconds = 300.0
        self.recovery_cooldown = 600.0
        self.retry_initial = self.retry_max = 5.0
        self._excluded_dates: frozenset[date] = frozenset()
        if self.watchdog_enabled:
            self.watchdog_seconds = positive_seconds("DXLINK_WATCHDOG_SECONDS", 300)
            self.recovery_cooldown = positive_seconds("DXLINK_RECOVERY_COOLDOWN_SECONDS", 600)
            self.retry_initial = positive_seconds("DXLINK_RETRY_INITIAL_SECONDS", 5)
            self.retry_max = positive_seconds("DXLINK_RETRY_MAX_SECONDS", 300)
            if self.retry_max < self.retry_initial:
                raise ValueError("DXLINK_RETRY_MAX_SECONDS must be >= DXLINK_RETRY_INITIAL_SECONDS")
            try:
                self._excluded_dates = frozenset(
                    date.fromisoformat(value.strip())
                    for value in os.environ.get("DXLINK_WATCHDOG_EXCLUDED_DATES", "").split(",")
                    if value.strip()
                )
            except ValueError:
                raise ValueError("DXLINK_WATCHDOG_EXCLUDED_DATES must contain ISO dates") from None
        self._last_recovery_at: float | None = None
        self._last_calendar_warning_at: float | None = None
        self._shutdown_requested = False
        self._closure_failed = False
        self._lock = Lock()  # API reads run in FastAPI's thread pool.
        self.connection_status = "DISCONNECTED"
        self._received_at: dict[str, float] = {}
        self.freshness_seconds: dict[str, float] = {}
        for key, default in (("SPY", 120), ("XSP", 900), ("ES", 120)):
            self.freshness_seconds[key] = positive_seconds(f"{key}_FRESHNESS_SECONDS", default)
        self.snapshots: dict[str, PriceSnapshot] = {
            "SPY": PriceSnapshot(symbol="SPY"),
            "XSP": PriceSnapshot(symbol="XSP"),
            "ES": PriceSnapshot(symbol="/ES"),
        }
        self._task: asyncio.Task | None = None

    @property
    def latest(self) -> PriceSnapshot:
        return self.get_snapshot("SPY")

    def observation_age(self, symbol: str) -> float | None:
        """Local receipt age, not exchange latency or connection health."""
        key = symbol.upper().lstrip("/")
        with self._lock:
            if key not in self.snapshots:
                raise KeyError(f"Unknown symbol: {symbol}")
            received = self._received_at.get(key)
            return None if received is None else max(0.0, self._monotonic() - received)

    def get_snapshot(self, symbol: str) -> PriceSnapshot:
        key = symbol.upper().lstrip("/")
        with self._lock:
            if key not in self.snapshots:
                raise KeyError(f"Unknown symbol: {symbol}")
            status = self.connection_status
            if status == "STREAMING":
                received = self._received_at.get(key)
                if received is None:
                    status = "WAITING_FOR_DATA"
                elif self._monotonic() - received > self.freshness_seconds[key]:
                    status = "STALE"
            return self.snapshots[key].model_copy(update={"status": status})

    def _record_snapshot(self, key: str, snapshot: PriceSnapshot) -> None:
        with self._lock:
            snapshot.updated_at = self._utc_now().isoformat()
            self._received_at[key] = self._monotonic()
            self.snapshots[key] = snapshot

    def compute_mid(self, bid: float | None, ask: float | None) -> str | None:
        if bid is None or ask is None:
            return None
        mid_val = (Decimal(str(bid)) + Decimal(str(ask))) / Decimal(2)
        return f"{mid_val:.2f}"

    async def start(self) -> None:
        if not self._shutdown_requested and not self._closure_failed and (self._task is None or self._task.done()):
            self._task = asyncio.create_task(self._stream_loop())

    async def stop(self) -> None:
        first_request = not self._shutdown_requested
        self._shutdown_requested = True
        if self._task:
            if first_request:
                self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._set_all_status("STOPPED")

    def _set_all_status(self, status: str) -> None:
        with self._lock:
            self.connection_status = status
            for snap in self.snapshots.values():
                snap.status = status

    async def _pause(self, seconds: float) -> None:
        if self._shutdown_requested:
            raise asyncio.CancelledError
        await (self._sleep_fn(seconds) if self._sleep_fn else asyncio.sleep(seconds))

    def _session_start(self, utc: datetime) -> datetime | None:
        if self.watchdog_enabled and calendar_expired(utc):
            now = self._monotonic()
            last = self._last_calendar_warning_at
            if last is None or now - last >= 3600:
                logger.warning("ES watchdog inactive: calendar coverage expired; stall recovery suppressed")
                self._last_calendar_warning_at = now
        return es_session_start(utc, self._excluded_dates)

    def _schedule_cleanup_warning(self, phase: str) -> asyncio.TimerHandle:
        # A diagnostic only: never time out teardown and open another streamer.
        return asyncio.get_running_loop().call_later(
            30, logger.warning, "Market data %s cleanup still pending after 30 seconds; replacement blocked", phase
        )

    @asynccontextmanager
    async def _streamer_context(self, session):
        # Keep SDK enter/exit on the owning task (including AnyIO cancel scopes).
        manager = DXLinkStreamer(session)
        streamer = await manager.__aenter__()
        try:
            yield streamer
        except BaseException as exc:
            if not await self._close_streamer(manager, type(exc), exc, exc.__traceback__):
                raise
        else:
            await self._close_streamer(manager, None, None, None)

    async def _close_streamer(self, manager, exc_type, exc, traceback):
        warning = self._schedule_cleanup_warning("DXLink streamer")
        try:
            return await manager.__aexit__(exc_type, exc, traceback)
        except BaseException:
            self._closure_failed = True
            logger.error("Market data streamer closure unconfirmed; recovery suppressed for worker lifetime")
            raise StreamerClosureFailed from None
        finally:
            warning.cancel()

    async def _watchdog(self) -> None:
        # Each connection/reopen gets a full active-period grace interval,
        # regardless of any retained observation from an earlier connection.
        baseline = previous_mono = self._monotonic()
        previous_utc = self._utc_now()
        session = self._session_start(previous_utc)
        while not self._shutdown_requested:
            await self._pause(10)
            if self._shutdown_requested:
                return
            now, utc = self._monotonic(), self._utc_now()
            current = self._session_start(utc)
            elapsed = now - previous_mono
            wall_elapsed = (utc - previous_utc).total_seconds()
            # A consistent long poll within one session is still ES inactivity.
            if current is None or current != session or elapsed < 0 or abs(wall_elapsed - elapsed) > 30:
                baseline = now
            session, previous_mono, previous_utc = current, now, utc
            if current is None:
                continue
            with self._lock:
                received = self._received_at.get("ES")
            last = max(baseline, received) if received is not None else baseline
            cooled = self._last_recovery_at is None or now - self._last_recovery_at >= self.recovery_cooldown
            if now - last >= self.watchdog_seconds and cooled:
                self._last_recovery_at = now
                self._set_all_status("RECOVERING")
                raise StreamStalled

    async def _run_listeners(self, quotes: Awaitable[None], trades: Awaitable[None]) -> None:
        tasks = [asyncio.create_task(quotes), asyncio.create_task(trades)]
        if self.watchdog_enabled:
            tasks.append(asyncio.create_task(self._watchdog()))
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
            raise OSError("Market-data listener ended")
        finally:
            warning = self._schedule_cleanup_warning("listeners")
            for task in tasks:
                task.cancel()
            # SDK task-group cancellation must not interrupt listener cleanup.
            try:
                with CancelScope(shield=True):
                    await asyncio.gather(*tasks, return_exceptions=True)
            finally:
                warning.cancel()

    async def _stream_loop(self) -> None:
        if self._closure_failed:
            self._set_all_status("STOPPED" if self._shutdown_requested else "CLEANUP_FAILED")
            return
        if self._shutdown_requested:
            self._set_all_status("STOPPED")
            return
        secret = os.environ.get("TASTYTRADE_CLIENT_SECRET", "")
        refresh = os.environ.get("TASTYTRADE_REFRESH_TOKEN", "")
        if not secret or not refresh:
            self._set_all_status("MISSING_CREDENTIALS")
            logger.warning("Market data credentials are missing")
            return

        retry_delay = self.retry_initial
        recovering = False
        while not self._shutdown_requested:
            connected_at = None
            try:
                self._set_all_status("RECOVERING" if recovering else "CONNECTING")
                phase = "session setup"
                session = Session(provider_secret=secret, refresh_token=refresh, is_test=False)

                phase = "future selection"
                futures = await Future.get(session, product_codes=["ES"])
                if self._shutdown_requested:
                    break
                active_future = next(f for f in futures if f.active_month)
                es_stream_symbol = active_future.streamer_symbol

                quote_map: dict[str, tuple[str, str]] = {
                    "SPY": ("SPY", "SPY"),
                    es_stream_symbol: ("ES", active_future.symbol),
                }

                phase = "streaming"
                listener_error = None
                async with self._streamer_context(session) as streamer:
                    await streamer.subscribe(Quote, list(quote_map.keys()))
                    if self._shutdown_requested:
                        raise asyncio.CancelledError
                    await streamer.subscribe(Trade, ["XSP"])
                    if self._shutdown_requested:
                        raise asyncio.CancelledError
                    with self._lock:
                        self.snapshots["SPY"].symbol = "SPY"
                        self.snapshots["XSP"].symbol = "XSP"
                        self.snapshots["ES"].symbol = active_future.symbol
                    self._set_all_status("STREAMING")
                    recovering = False
                    connected_at = self._monotonic()

                    async def listen_quotes(quote_map=quote_map) -> None:
                        async for quote in streamer.listen(Quote):
                            mapping = quote_map.get(quote.event_symbol)
                            if not mapping:
                                continue    
                            cache_key, display_symbol = mapping
                            mid = self.compute_mid(quote.bid_price, quote.ask_price)
                            self._record_snapshot(cache_key, PriceSnapshot(
                                symbol=display_symbol,
                                bid=f"{quote.bid_price:.2f}" if quote.bid_price else None,
                                ask=f"{quote.ask_price:.2f}" if quote.ask_price else None,
                                mid=mid,
                                status="STREAMING",
                            ))

                    async def listen_trades() -> None:
                        async for trade in streamer.listen(Trade):
                            if trade.event_symbol != "XSP" or trade.price is None:
                                continue
                            price_str = f"{Decimal(str(trade.price)):.2f}"
                            self._record_snapshot("XSP", PriceSnapshot(
                                symbol="XSP",
                                bid=price_str,
                                ask=price_str,
                                mid=price_str,
                                status="STREAMING",
                            ))

                    try:
                        await self._run_listeners(listen_quotes(), listen_trades())
                    except (StreamStalled, TastytradeError, OSError, TimeoutError) as exc:
                        # Exit the SDK's task group normally: otherwise some SDK
                        # versions wrap our deliberate stall in ExceptionGroup.
                        listener_error = exc
                if listener_error is not None:
                    raise listener_error
            except StreamerClosureFailed:
                self._set_all_status("STOPPED" if self._shutdown_requested else "CLEANUP_FAILED")
                return
            except asyncio.CancelledError:
                self._set_all_status("STOPPED")
                break
            except (StreamStalled, TastytradeError, OSError, TimeoutError, ExceptionGroup) as exc:
                if self._shutdown_requested:
                    logger.warning("Market data shutdown cleanup failed; recovery suppressed")
                    break
                recovering = recovering or isinstance(exc, StreamStalled) or self.connection_status == "RECOVERING"
                category = ("ES inactivity" if isinstance(exc, StreamStalled) else
                            "authentication or service error" if isinstance(exc, (TastytradeError, ExceptionGroup)) else
                            "network or timeout error")
                # A subscription alone doesn't reset backoff. Require a sustained
                # connection and a recent ES observation from that connection.
                with self._lock:
                    received = self._received_at.get("ES")
                now = self._monotonic()
                if (connected_at is not None and received is not None and received >= connected_at
                        and now - connected_at >= self.watchdog_seconds and now - received < self.watchdog_seconds):
                    retry_delay = self.retry_initial
                logger.warning("Market data %s failed (%s); retrying in %s seconds", phase, category, retry_delay)
                self._set_all_status("RECOVERING" if recovering else "RECONNECTING")
                try:
                    await self._pause(retry_delay)
                    retry_delay = min(self.retry_max, retry_delay * 2)
                except asyncio.CancelledError:
                    self._set_all_status("STOPPED")
                    break
            except Exception:
                if not self._shutdown_requested:
                    raise
                logger.warning("Market data shutdown cleanup failed; recovery suppressed")
                break
        self._set_all_status("STOPPED")
