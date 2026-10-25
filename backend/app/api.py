from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from .market import ForexFeed

logging.basicConfig(level=logging.INFO)
feed = ForexFeed()


@asynccontextmanager
async def lifespan(_: FastAPI):
    tasks = [
        asyncio.create_task(feed.run(), name="forex-provider"),
        asyncio.create_task(feed.consume_candles(), name="spark-candles"),
    ]
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task
        await feed.close()


app = FastAPI(title="DomainFree Forex Signal", lifespan=lifespan)


@app.get("/api/health")
async def health() -> dict[str, Any]:
    return {
        "status": feed.status,
        "provider": "OANDA practice",
        "pairs": feed.snapshot()["pairs"],
        "last_update": feed.last_update,
        "last_candle": feed.last_candle,
        "kafka": feed.producer is not None,
        "spark_model": {
            pair: {
                "trained_samples": model.training_samples,
                "ready": model.training_samples >= 30,
            }
            for pair, model in feed.models.items()
        },
    }


@app.websocket("/ws/market")
async def market_socket(client: WebSocket) -> None:
    await feed.add_client(client)
    sender = asyncio.create_task(feed.send_to_client(client))
    try:
        while True:
            await client.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        sender.cancel()
        with suppress(asyncio.CancelledError):
            await sender
        feed.remove_client(client)
