from __future__ import annotations

import asyncio
import json
import logging
import math
import os
from datetime import datetime, timezone
from typing import Any

import aiohttp
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect

from .model import OnlineFxModel

logger = logging.getLogger(__name__)
PAIRS = ("EUR/USD", "USD/JPY", "EUR/JPY")
OANDA_INSTRUMENTS = {
    "EUR_USD": "EUR/USD",
    "USD_JPY": "USD/JPY",
    "EUR_JPY": "EUR/JPY",
}
RAW_TICKS_TOPIC = "fx.ticks.raw"
CANDLES_TOPIC = "fx.candles"


class ForexFeed:
    def __init__(self) -> None:
        self.access_token = os.getenv("OANDA_ACCESS_TOKEN", "").strip()
        self.account_id = os.getenv("OANDA_ACCOUNT_ID", "").strip()
        self.bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
        )
        self.status = (
            "missing_credentials"
            if not self.access_token or not self.account_id
            else "connecting"
        )
        self.last_update: str | None = None
        self.last_candle: str | None = None
        self.ticks: dict[str, dict[str, Any]] = {}
        self.candles: dict[str, list[dict[str, Any]]] = {
            pair: [] for pair in PAIRS
        }
        self.predictions: dict[str, dict[str, Any]] = {}
        self.models = {pair: OnlineFxModel(pair) for pair in PAIRS}
        self.clients: dict[WebSocket, asyncio.Queue[dict[str, Any]]] = {}
        self.producer: AIOKafkaProducer | None = None

    def snapshot(self) -> dict[str, Any]:
        return {
            "type": "snapshot",
            "feed": "oanda_practice",
            "status": self.status,
            "kafka": self.producer is not None,
            "pairs": PAIRS,
            "last_update": self.last_update,
            "last_candle": self.last_candle,
            "ticks": list(self.ticks.values()),
            "candles": self.candles,
            "predictions": self.predictions,
        }

    async def add_client(self, client: WebSocket) -> None:
        await client.accept()
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=128)
        self.clients[client] = queue
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
        while True:
            if not self.access_token or not self.account_id:
                self.status = "missing_credentials"
                await self._broadcast({"type": "status", "status": self.status})
                await asyncio.sleep(10)
                continue
            try:
                await self._connect_kafka()
                await self._run_oanda_stream()
            except asyncio.CancelledError:
                raise
            except (OSError, KafkaError, TimeoutError, aiohttp.ClientError) as error:
                logger.warning(
                    "OANDA practice feed unavailable (%s); reconnecting",
                    type(error).__name__,
                )
                self.status = "reconnecting"
                await self._broadcast({"type": "status", "status": self.status})
                await asyncio.sleep(3)

    async def _connect_kafka(self) -> None:
        if self.producer is not None:
            return
        producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
        )
        await producer.start()
        self.producer = producer

    async def _run_oanda_stream(self) -> None:
        self.status = "connecting"
        await self._broadcast({"type": "status", "status": self.status})
        url = (
            "https://stream-fxpractice.oanda.com/v3/accounts/"
            f"{self.account_id}/pricing/stream"
        )
        headers = {"Authorization": f"Bearer {self.access_token}"}
        params = {"instruments": "EUR_USD,USD_JPY,EUR_JPY"}
        timeout = aiohttp.ClientTimeout(total=None, connect=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers, params=params) as upstream:
                upstream.raise_for_status()
                self.status = "live"
                await self._broadcast({"type": "status", "status": self.status})
                async for raw_line in upstream.content:
                    try:
                        payload = json.loads(raw_line)
                        if isinstance(payload, dict) and "instrument" in payload:
                            await self._handle_quote(payload)
                        elif isinstance(payload, dict) and payload.get("errorMessage"):
                            raise aiohttp.ClientResponseError(
                                upstream.request_info,
                                upstream.history,
                                status=401,
                                message="OANDA rejected the price stream",
                            )
                    except (json.JSONDecodeError, TypeError, ValueError):
                        logger.warning("Ignoring an invalid OANDA stream message")

    async def _handle_quote(self, payload: dict[str, Any]) -> None:
        instrument = str(payload.get("instrument", "")).upper()
        pair = OANDA_INSTRUMENTS.get(instrument)
        try:
            bid = float(payload["bids"][0]["price"])
            ask = float(payload["asks"][0]["price"])
            price = (bid + ask) / 2
            timestamp = datetime.fromisoformat(
                str(payload["time"]).replace("Z", "+00:00")
            ).astimezone(timezone.utc).isoformat()
        except (KeyError, IndexError, TypeError, ValueError, OverflowError, OSError):
            return
        if pair is None or not math.isfinite(price) or price <= 0:
            return
        tick = {
            "pair": pair,
            "price": price,
            "bid": bid,
            "ask": ask,
            "timestamp": timestamp,
        }
        if self.producer is None:
            raise OSError("Kafka producer is not connected")
        try:
            await self.producer.send_and_wait(RAW_TICKS_TOPIC, tick, key=pair.encode())
        except (KafkaError, OSError):
            await self.producer.stop()
            self.producer = None
            raise
        self.ticks[pair] = tick
        self.last_update = timestamp
        await self._broadcast({"type": "tick", "tick": tick})

    async def consume_candles(self) -> None:
        while True:
            consumer = AIOKafkaConsumer(
                CANDLES_TOPIC,
                bootstrap_servers=self.bootstrap_servers,
                group_id="forex-model",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            )
            try:
                await consumer.start()
                async for message in consumer:
                    await self._handle_candle(message.value)
            except asyncio.CancelledError:
                raise
            except (OSError, KafkaError, TimeoutError, ValueError) as error:
                logger.warning(
                    "Candle pipeline unavailable (%s); retrying",
                    type(error).__name__,
                )
                self.status = "pipeline_unavailable"
                await self._broadcast({"type": "status", "status": self.status})
                await asyncio.sleep(3)
            finally:
                await consumer.stop()

    async def _handle_candle(self, candle: dict[str, Any]) -> None:
        pair = candle.get("pair")
        if not isinstance(pair, str) or pair not in self.models:
            return
        try:
            prediction = await asyncio.to_thread(self.models[pair].observe, candle)
        except (KeyError, TypeError, ValueError, ArithmeticError):
            logger.warning("Ignoring invalid candle for %s", pair)
            return
        candles = self.candles[pair]
        candles.append(candle)
        if len(candles) > 300:
            del candles[:-300]
        self.predictions[pair] = prediction
        self.last_candle = str(candle.get("timestamp"))
        await self._broadcast({"type": "candle", "candle": candle})
        await self._broadcast({"type": "prediction", "prediction": prediction})

    async def _broadcast(self, message: dict[str, Any]) -> None:
        for queue in self.clients.values():
            if queue.full():
                queue.get_nowait()
            queue.put_nowait(message)

    async def close(self) -> None:
        if self.producer is not None:
            await self.producer.stop()
            self.producer = None
