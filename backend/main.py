"""
main.py

Backend service entrypoint (FastAPI). Wires up both the WebSocket
live-candle push (routers/ws.py) and the REST historical-candles endpoint
(routers/candles.py, reading Postgres's candles_1m).

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


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
