import asyncio
import os
from datetime import UTC, datetime
from decimal import Decimal

from pydantic import BaseModel
from tastytrade import DXLinkStreamer, Session
from tastytrade.dxfeed import Quote, Trade
from tastytrade.instruments import Future
from tastytrade.utils import TastytradeError


class PriceSnapshot(BaseModel):
    symbol: str = "SPY"
    bid: str | None = None
    ask: str | None = None
    mid: str | None = None
    status: str = "DISCONNECTED"
    updated_at: str = ""

class XspPriceWorker:
    def __init__(self) -> None:
        self.snapshots: dict[str, PriceSnapshot] = {
            "SPY": PriceSnapshot(symbol="SPY"),
            "XSP": PriceSnapshot(symbol="XSP"),
            "ES": PriceSnapshot(symbol="/ES"),
        }
        self._task: asyncio.Task | None = None

    @property
    def latest(self) -> PriceSnapshot:
        return self.snapshots["SPY"]

    def get_snapshot(self, symbol: str) -> PriceSnapshot:
        key = symbol.upper().lstrip("/")
        if key not in self.snapshots:
            raise KeyError(f"Unknown symbol: {symbol}")
        return self.snapshots[key]    

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

    def _set_all_status(self, status: str) -> None:
        for snap in self.snapshots.values():
            snap.status = status

    async def _stream_loop(self) -> None:
        secret = os.environ.get("TASTYTRADE_CLIENT_SECRET", "")
        refresh = os.environ.get("TASTYTRADE_REFRESH_TOKEN", "")
        if not secret or not refresh:
            self.latest.status = "MISSING_CREDENTIALS"
            return

        while True:
            try:
                self.latest.status = "CONNECTING"
                session = Session(provider_secret=secret, refresh_token=refresh, is_test=False)

                futures = await Future.get(session, product_codes=["ES"])
                active_future = next(f for f in futures if f.active_month)
                es_stream_symbol = active_future.streamer_symbol

                quote_map: dict[str, tuple[str, str]] = {
                    "SPY": ("SPY", "SPY"),
                    es_stream_symbol: ("ES", active_future.symbol),
                }

                async with DXLinkStreamer(session) as streamer:
                    await streamer.subscribe(Quote, list(quote_map.keys()))
                    await streamer.subscribe(Trade, ["XSP"])
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
                            self.snapshots[cache_key] = PriceSnapshot(
                                symbol=display_symbol,
                                bid=f"{quote.bid_price:.2f}" if quote.bid_price else None,
                                ask=f"{quote.ask_price:.2f}" if quote.ask_price else None,
                                mid=mid,
                                status="STREAMING",
                                updated_at=datetime.now(UTC).isoformat(),
                            )

                    async def listen_trades() -> None:
                        async for trade in streamer.listen(Trade):
                            if trade.event_symbol != "XSP" or trade.price is None:
                                continue
                            price_str = f"{Decimal(str(trade.price)):.2f}"
                            self.snapshots["XSP"] = PriceSnapshot(
                                symbol="XSP",
                                bid=price_str,
                                ask=price_str,
                                mid=price_str,
                                status="STREAMING",
                                updated_at=datetime.now(UTC).isoformat(),
                            )

                    await asyncio.gather(listen_quotes(), listen_trades())
            except asyncio.CancelledError:
                self.latest.status = "STOPPED"
                break
            except (TastytradeError, OSError, TimeoutError) as exc:
                self.latest.status = f"RECONNECTING ({exc})"
                await asyncio.sleep(5)
