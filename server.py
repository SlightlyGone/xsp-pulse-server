import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException

from tasty_worker import PriceSnapshot, XspPriceWorker

worker = XspPriceWorker()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await worker.start()
    yield
    await worker.stop()


app = FastAPI(title="XSP Pulse Server", lifespan=lifespan)


def verify_key(authorization: str = Header(default="")) -> None:
    expected = f"Bearer {os.environ.get('APP_API_KEY', 'redacted')}"
    if authorization != expected:
        raise HTTPException(status_code=401, detail="Invalid API Key")


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.get("/price/xsp", response_model=PriceSnapshot, dependencies=[Depends(verify_key)])
def get_price(symbol: str = "SPY") -> PriceSnapshot:
    try:
        return worker.get_snapshot(symbol)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
