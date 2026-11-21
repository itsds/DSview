"""
main.py

Backend service entrypoint (FastAPI). Wires up the WebSocket
live-candle push (routers/ws.py, from Redis), the REST historical-candles
endpoint (routers/candles.py, reading Postgres's candles_1m with each
candle's indicators from candle_indicators), the WebSocket live-indicator
push (routers/indicators.py, from Redis), the volume footprint
(routers/footprint.py: history and profiles from Postgres's footprint_1m,
the live minute from Redis) and the WebSocket whale-alert feed
(routers/whales.py, from Kafka's alerts.whales).

Run from backend/ (matching ingestion/streaming's flat-import
convention), with the repo root on PYTHONPATH (see config/settings.py):

    cd backend && uvicorn main:app --reload --port 8000

Author: @DS
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config.settings import CORS_ALLOWED_ORIGINS

from routers.candles import router as candles_router
from routers.footprint import router as footprint_router
from routers.indicators import router as indicators_router
from routers.whales import router as whales_router
from routers.ws import router as ws_router

app = FastAPI(title="DSView Backend")

# REST (unlike the WebSocket router) is subject to browser CORS: the
# frontend's Vite dev server and this API run on different ports, which
# counts as a different origin. Once the frontend has a real deployed URL,
# set DSVIEW_CORS_ALLOWED_ORIGINS rather than editing this.
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOWED_ORIGINS,
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(ws_router)
app.include_router(candles_router)
app.include_router(indicators_router)
app.include_router(footprint_router)
app.include_router(whales_router)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
