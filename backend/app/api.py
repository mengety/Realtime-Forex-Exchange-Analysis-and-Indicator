from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from .market import MarketFeed

logging.basicConfig(level=logging.INFO)
feed = MarketFeed()


@asynccontextmanager
async def lifespan(_: FastAPI):
    worker = asyncio.create_task(feed.run())
    try:
        yield
    finally:
        worker.cancel()
        with suppress(asyncio.CancelledError):
            await worker


app = FastAPI(title="DomainFree Market Anomaly API", lifespan=lifespan)


@app.get("/api/health")
async def health() -> dict[str, str | None]:
    return {
        "status": feed.status,
        "feed": feed.feed_label,
        "last_update": feed.last_update,
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
