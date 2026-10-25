from __future__ import annotations

from collections import deque
import math
from typing import Any

import numpy as np
from sklearn.linear_model import SGDClassifier


class OnlineFxModel:
    """Learns one-bar direction from closed Spark candles, then predicts the next bar."""

    def __init__(self, pair: str) -> None:
        self.pair = pair
        self.model = SGDClassifier(loss="log_loss", random_state=42)
        self.candles: deque[dict[str, Any]] = deque(maxlen=300)
        self.previous_features: np.ndarray | None = None
        self.previous_close: float | None = None
        self.training_samples = 0
        self.initialized = False

    def observe(self, candle: dict[str, Any]) -> dict[str, Any]:
        close = float(candle["close"])
        high = float(candle["high"])
        low = float(candle["low"])
        open_price = float(candle["open"])
        spread = float(candle.get("average_spread", 0.0))
        if (
            not all(math.isfinite(value) for value in (close, high, low, open_price, spread))
            or min(close, high, low, open_price) <= 0
            or spread < 0
            or low > min(open_price, close)
            or high < max(open_price, close)
        ):
            raise ValueError("Invalid OHLC candle")

        self.candles.append(candle)
        features = self._features(open_price, high, low, close)
        if self.previous_features is not None and self.previous_close is not None:
            label = int(close > self.previous_close)
            self.model.partial_fit(
                self.previous_features.reshape(1, -1),
                np.asarray([label]),
                classes=np.asarray([0, 1]) if not self.initialized else None,
            )
            self.initialized = True
            self.training_samples += 1

        self.previous_features = features
        self.previous_close = close
        if self.training_samples < 30:
            return {
                "pair": self.pair,
                "timestamp": candle["timestamp"],
                "status": "warming_up",
                "direction": "NEUTRAL",
                "confidence": 0.0,
                "predicted_close": close,
                "training_samples": self.training_samples,
                "horizon": "next 1-minute candle",
            }

        probabilities = self.model.predict_proba(features.reshape(1, -1))[0]
        probability_up = float(probabilities[1])
        direction = "UP" if probability_up >= 0.5 else "DOWN"
        confidence = max(probability_up, 1.0 - probability_up)
        recent_returns = [
            abs(float(item["close"]) / float(item["open"]) - 1.0)
            for item in list(self.candles)[-30:]
        ]
        expected_move = max(float(np.mean(recent_returns)), 0.00001)
        signed_move = expected_move if direction == "UP" else -expected_move
        return {
            "pair": self.pair,
            "timestamp": candle["timestamp"],
            "status": "active",
            "direction": direction,
            "confidence": round(confidence, 4),
            "probability_up": round(probability_up, 4),
            "predicted_close": round(close * (1 + signed_move), 6),
            "training_samples": self.training_samples,
            "horizon": "next 1-minute candle",
        }

    def _features(
        self, open_price: float, high: float, low: float, close: float
    ) -> np.ndarray:
        candles = list(self.candles)
        previous_close = (
            float(candles[-2]["close"]) if len(candles) > 1 else close
        )
        closes = np.asarray([float(item["close"]) for item in candles], dtype=float)
        recent_closes = closes[-15:]
        differences = np.diff(recent_closes)
        gains = np.maximum(differences, 0)
        losses = np.maximum(-differences, 0)
        average_gain = float(np.mean(gains)) if gains.size else 0.0
        average_loss = float(np.mean(losses)) if losses.size else 0.0
        rsi = (
            100.0
            if average_loss == 0 and average_gain > 0
            else 50.0
            if average_gain == average_loss == 0
            else 100.0 - 100.0 / (1.0 + average_gain / average_loss)
        )
        sma = float(np.mean(closes[-20:]))
        return np.asarray(
            [
                (close / previous_close - 1.0) * 10000,
                (close / open_price - 1.0) * 10000,
                (high / low - 1.0) * 10000,
                (close / sma - 1.0) * 10000,
                rsi,
                (spread / close) * 10000,
            ],
            dtype=float,
        )
