"""
main.py

Backend service entrypoint (FastAPI).

Currently only wires up the WebSocket live-candle router — REST
historical endpoints (routers/candles.py) wait on the Postgres/storage
layer, which doesn't exist yet (see CLAUDE.md's Deployment approach:
verify each layer before adding the next).

Run from backend/ (matching ingestion/streaming's flat-import
convention):

    cd backend && uvicorn main:app --reload --port 8000

Author: @DS
"""

from __future__ import annotations

from fastapi import FastAPI

from routers.ws import router as ws_router

app = FastAPI(title="DSView Backend")
app.include_router(ws_router)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
