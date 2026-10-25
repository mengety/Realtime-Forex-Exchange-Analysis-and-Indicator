from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
from datetime import datetime, timezone
from typing import Any

import websockets
from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect
from websockets.exceptions import ConnectionClosed, WebSocketException

from .detector import ASSET_INFO, AssetDetector, AssetState, utc_now

logger = logging.getLogger(__name__)
COIN_IDS = tuple(ASSET_INFO)
CG_CHANNEL = json.dumps({"channel": "CGSimplePrice"}, separators=(",", ":"))


class MarketFeed:
    def __init__(self) -> None:
        self.api_key = os.getenv("COINGECKO_API_KEY", "").strip()
        self.assets: dict[str, AssetState] = {}
        self.clients: dict[WebSocket, asyncio.Queue[dict[str, Any]]] = {}
        self.detectors = {coin_id: AssetDetector(coin_id) for coin_id in COIN_IDS}
        self.status = "demo" if not self.api_key else "connecting"
        self.last_update: str | None = None
        self._demo_prices = {
            "bitcoin": 67420.0,
            "ethereum": 3520.0,
            "solana": 178.4,
            "ripple": 0.62,
            "binancecoin": 591.2,
            "dogecoin": 0.164,
        }
        self._demo_volumes = {
            coin_id: random.uniform(1e9, 15e9) for coin_id in COIN_IDS
        }

    @property
    def feed_label(self) -> str:
        return "coingecko" if self.api_key else "demo"

    def snapshot(self) -> dict[str, Any]:
        return {
            "type": "snapshot",
            "feed": self.feed_label,
            "status": self.status,
            "last_update": self.last_update,
            "assets": [self._serialize(asset) for asset in self.assets.values()],
        }

    async def add_client(self, client: WebSocket) -> None:
        await client.accept()
        self.clients[client] = asyncio.Queue(maxsize=16)
        await client.send_json(self.snapshot())

    def remove_client(self, client: WebSocket) -> None:
        self.clients.pop(client, None)

    async def send_to_client(self, client: WebSocket) -> None:
        queue = self.clients.get(client)
        if queue is None:
            return
        try:
            while True:
                await client.send_json(await queue.get())
        except (OSError, WebSocketDisconnect, RuntimeError):
            self.remove_client(client)

    async def run(self) -> None:
        if not self.api_key:
            await self._run_demo()
            return
        await self._run_coingecko()

    async def _run_coingecko(self) -> None:
        delay = 1
        while True:
            try:
                self.status = "connecting"
                await self._broadcast({"type": "status", "status": self.status})
                async with websockets.connect(
                    "wss://stream.coingecko.com/v1",
                    extra_headers={"x-cg-pro-api-key": self.api_key},
                    ping_interval=10,
                    ping_timeout=20,
                    open_timeout=12,
                ) as upstream:
                    await upstream.send(json.dumps({
                        "command": "subscribe", "identifier": CG_CHANNEL,
                    }))
                    await upstream.send(json.dumps({
                        "command": "message",
                        "identifier": CG_CHANNEL,
                        "data": json.dumps({
                            "coin_id": list(COIN_IDS), "vs_currencies": ["usd"],
                            "action": "set_tokens",
                        }),
                    }))
                    self.status = "live"
                    delay = 1
                    await self._broadcast({"type": "status", "status": self.status})
                    async for raw_message in upstream:
                        try:
                            payload = json.loads(raw_message)
                            if isinstance(payload, dict) and "i" in payload:
                                await self._handle_coin(payload)
                        except (json.JSONDecodeError, TypeError, ValueError):
                            logger.warning("Ignoring an invalid CoinGecko message")
            except asyncio.CancelledError:
                raise
            except (OSError, ConnectionClosed, TimeoutError, WebSocketException) as error:
                logger.warning(
                    "CoinGecko WebSocket disconnected (%s); reconnecting",
                    type(error).__name__,
                )
                self.status = "reconnecting"
                await self._broadcast({"type": "status", "status": self.status})
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def _run_demo(self) -> None:
        self.status = "demo"
        await self._broadcast({"type": "status", "status": self.status})
        tick = 0
        while True:
            tick += 1
            for coin_id in COIN_IDS:
                current = self._demo_prices[coin_id]
                move = random.gauss(0, 0.0008)
                if tick % 83 == 0 and coin_id == "solana":
                    move = 0.025
                self._demo_prices[coin_id] = current * math.exp(move)
                self._demo_volumes[coin_id] *= math.exp(random.gauss(0, 0.012))
                await self._handle_state_from_price(
                    coin_id,
                    self._demo_prices[coin_id],
                    random.gauss(0, 0.35),
                    self._demo_volumes[coin_id],
                    utc_now(),
                )
            await asyncio.sleep(1)

    async def _handle_coin(self, payload: dict[str, Any]) -> None:
        coin_id = payload.get("i")
        if not isinstance(coin_id, str) or coin_id not in self.detectors:
            return
        price = payload.get("p")
        if price is None:
            return
        try:
            price_value = float(price)
            volume_value = float(payload.get("v") or 0.0)
            change_value = float(payload.get("pp") or 0.0)
        except (TypeError, ValueError):
            return
        if not all(math.isfinite(value) for value in (price_value, volume_value, change_value)):
            return
        timestamp_value = payload.get("t")
        try:
            timestamp = (
                datetime.fromtimestamp(float(timestamp_value), timezone.utc).isoformat()
                if timestamp_value is not None
                else utc_now()
            )
        except (TypeError, ValueError, OverflowError, OSError):
            timestamp = utc_now()
        await self._handle_state_from_price(
            coin_id, price_value, change_value, volume_value, timestamp
        )

    async def _handle_state_from_price(
        self, coin_id: str, price: float, change: float, volume: float, timestamp: str
    ) -> None:
        state = await self.detectors[coin_id].observe(
            price, volume, change, timestamp
        )
        self.assets[coin_id] = state
        self.last_update = timestamp
        await self._broadcast({"type": "tick", "asset": self._serialize(state)})

    async def _broadcast(self, message: dict[str, Any]) -> None:
        for queue in self.clients.values():
            if queue.full():
                queue.get_nowait()
            queue.put_nowait(message)

    @staticmethod
    def _serialize(asset: AssetState) -> dict[str, Any]:
        return {
            "id": asset.coin_id,
            "symbol": asset.symbol,
            "name": asset.name,
            "price": asset.price,
            "change_24h": asset.change_24h,
            "volume_24h": asset.volume_24h,
            "timestamp": asset.timestamp,
            "anomaly": asset.anomaly,
            "anomaly_score": asset.anomaly_score,
        }
