import asyncio
import logging
import math
import os
import time
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from threading import Lock

from pydantic import BaseModel
from tastytrade import DXLinkStreamer, Session
from tastytrade.dxfeed import Quote, Trade
from tastytrade.instruments import Future
from tastytrade.utils import TastytradeError


logger = logging.getLogger(__name__)


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
    ) -> None:
        self._monotonic = monotonic
        self._utc_now = utc_now
        self._lock = Lock()  # API reads run in FastAPI's thread pool.
        self.connection_status = "DISCONNECTED"
        self._received_at: dict[str, float] = {}
        self.freshness_seconds: dict[str, float] = {}
        for key, default in (("SPY", 120), ("XSP", 900), ("ES", 120)):
            name = f"{key}_FRESHNESS_SECONDS"
            try:
                threshold = float(os.environ.get(name, str(default)))
            except ValueError:
                raise ValueError(f"{name} must be a finite positive number") from None
            if not math.isfinite(threshold) or threshold <= 0:
                raise ValueError(f"{name} must be a finite positive number")
            self.freshness_seconds[key] = threshold
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
        self._task = asyncio.create_task(self._stream_loop())

    async def stop(self) -> None:
        if self._task:
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

    async def _stream_loop(self) -> None:
        secret = os.environ.get("TASTYTRADE_CLIENT_SECRET", "")
        refresh = os.environ.get("TASTYTRADE_REFRESH_TOKEN", "")
        if not secret or not refresh:
            self._set_all_status("MISSING_CREDENTIALS")
            logger.warning("Market data credentials are missing")
            return

        while True:
            try:
                self._set_all_status("CONNECTING")
                phase = "session setup"
                session = Session(provider_secret=secret, refresh_token=refresh, is_test=False)

                phase = "future selection"
                futures = await Future.get(session, product_codes=["ES"])
                active_future = next(f for f in futures if f.active_month)
                es_stream_symbol = active_future.streamer_symbol

                quote_map: dict[str, tuple[str, str]] = {
                    "SPY": ("SPY", "SPY"),
                    es_stream_symbol: ("ES", active_future.symbol),
                }

                phase = "streaming"
                async with DXLinkStreamer(session) as streamer:
                    await streamer.subscribe(Quote, list(quote_map.keys()))
                    await streamer.subscribe(Trade, ["XSP"])
                    with self._lock:
                        self.snapshots["SPY"].symbol = "SPY"
                        self.snapshots["XSP"].symbol = "XSP"
                        self.snapshots["ES"].symbol = active_future.symbol
                    self._set_all_status("STREAMING")

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

                    await asyncio.gather(listen_quotes(), listen_trades())
            except asyncio.CancelledError:
                self._set_all_status("STOPPED")
                break
            except (TastytradeError, OSError, TimeoutError) as exc:
                category = "authentication or service error" if isinstance(exc, TastytradeError) else "network or timeout error"
                logger.warning("Market data %s failed (%s); retrying in 5 seconds", phase, category)
                self._set_all_status("RECONNECTING")
                try:
                    await asyncio.sleep(5)
                except asyncio.CancelledError:
                    self._set_all_status("STOPPED")
                    break
