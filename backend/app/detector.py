from __future__ import annotations

import asyncio
import math
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np
from sklearn.ensemble import IsolationForest


@dataclass
class AssetState:
    coin_id: str
    symbol: str
    name: str
    price: float
    change_24h: float
    volume_24h: float
    timestamp: str
    anomaly: bool = False
    anomaly_score: float = 0.0


ASSET_INFO = {
    "bitcoin": ("BTC", "Bitcoin"),
    "ethereum": ("ETH", "Ethereum"),
    "solana": ("SOL", "Solana"),
    "ripple": ("XRP", "XRP"),
    "binancecoin": ("BNB", "BNB"),
    "dogecoin": ("DOGE", "Dogecoin"),
}


class AssetDetector:
    """Bounded, per-asset Isolation Forest over returns and volume changes."""

    def __init__(self, coin_id: str) -> None:
        self.coin_id = coin_id
        self._observations: deque[tuple[float, float]] = deque(maxlen=240)
        self._model: IsolationForest | None = None
        self._last_fit = 0
        self._previous_price: float | None = None
        self._previous_volume: float | None = None
        self._lock = asyncio.Lock()

    async def observe(
        self, price: float, volume: float, change_24h: float, timestamp: str
    ) -> AssetState:
        async with self._lock:
            price = max(price, 1e-12)
            volume = max(volume, 0.0)
            price_return = (
                math.log(price / self._previous_price)
                if self._previous_price is not None
                else 0.0
            )
            volume_change = (
                math.log((volume + 1.0) / (self._previous_volume + 1.0))
                if self._previous_volume is not None
                else 0.0
            )
            features = np.asarray([[price_return, volume_change]], dtype=float)

            anomaly = False
            score = 0.0
            if self._model is not None:
                anomaly = bool(self._model.predict(features)[0] == -1)
                score = max(0.0, float(-self._model.decision_function(features)[0]))

            self._observations.append((price_return, volume_change))
            self._previous_price = price
            self._previous_volume = volume

            if len(self._observations) >= 32 and len(self._observations) - self._last_fit >= 16:
                training_data = np.asarray(self._observations, dtype=float)
                await asyncio.to_thread(self._fit, training_data)

            symbol, name = ASSET_INFO[self.coin_id]
            return AssetState(
                coin_id=self.coin_id,
                symbol=symbol,
                name=name,
                price=price,
                change_24h=change_24h,
                volume_24h=volume,
                timestamp=timestamp,
                anomaly=anomaly,
                anomaly_score=round(score, 5),
            )

    def _fit(self, training_data: np.ndarray) -> None:
        model = IsolationForest(
            n_estimators=80,
            contamination=0.025,
            max_samples=min(128, len(training_data)),
            random_state=42,
            n_jobs=1,
        )
        model.fit(training_data)
        self._model = model
        self._last_fit = len(self._observations)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
