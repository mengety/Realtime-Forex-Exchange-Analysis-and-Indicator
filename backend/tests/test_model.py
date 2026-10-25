import unittest

from app.model import OnlineFxModel


class OnlineFxModelTests(unittest.TestCase):
    def test_model_trains_on_completed_bars_and_predicts_next_bar(self):
        model = OnlineFxModel("EUR/USD")
        output = None

        for index in range(32):
            open_price = 1.08 + index * 0.0001
            close = open_price + (0.00008 if index % 2 else -0.00004)
            output = model.observe({
                "pair": "EUR/USD",
                "timestamp": f"2026-01-01T00:{index:02d}:00Z",
                "open": open_price,
                "high": max(open_price, close) + 0.00003,
                "low": min(open_price, close) - 0.00003,
                "close": close,
                "average_spread": 0.00002,
            })

        self.assertEqual(model.training_samples, 31)
        self.assertEqual(output["status"], "active")
        self.assertIn(output["direction"], {"UP", "DOWN"})
        self.assertGreaterEqual(output["confidence"], 0.5)
        self.assertGreater(output["predicted_close"], 0)

    def test_invalid_candle_is_rejected(self):
        model = OnlineFxModel("USD/JPY")
        with self.assertRaises(ValueError):
            model.observe({
                "timestamp": "2026-01-01T00:00:00Z",
                "open": 150.0,
                "high": 149.0,
                "low": 151.0,
                "close": 150.0,
            })
        self.assertEqual(model.training_samples, 0)


if __name__ == "__main__":
    unittest.main()
