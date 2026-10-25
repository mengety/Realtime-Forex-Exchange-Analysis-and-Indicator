# DomainFree Forex Signal

Real-time EUR/USD, USD/JPY, and EUR/JPY quotes flow from OANDA's free practice-account price stream through Kafka and Spark Structured Streaming. Spark validates and cleans quote ticks, creates one-minute OHLC candles, writes candles to HDFS as Parquet, and publishes them back to Kafka. A per-pair online SGD classifier learns from those closed candles and estimates the direction and approximate close of the next one-minute candle. The dashboard renders live candlesticks, technical context, and the forecast marker.

## Free live data setup

OANDA's developer API requires a v20 practice account and a personal access token. Practice accounts are free; the practice API uses the stream host and does not place trades. Create a practice account and token using OANDA's [Getting Started guide](https://developer.oanda.com/rest-live-v20/introduction/) and [price-stream documentation](https://developer.oanda.com/rest-live-v20/pricing-ep/).

Copy `.env.example` to `.env` and set the two values from the practice account. Do not use a live-account token:

```dotenv
OANDA_ACCESS_TOKEN=your_practice_access_token
OANDA_ACCOUNT_ID=your_practice_account_id
```

## Run

Start the full stack (dashboard, API, Kafka/Zookeeper, Spark Streaming, and Hadoop HDFS):

```bash
docker compose up --build
```

Open [http://localhost](http://localhost). The single-node HDFS NameNode/DataNode and Spark Structured Streaming job are included in the default stack. Visit [http://localhost:9870](http://localhost:9870) for the HDFS NameNode UI. Processed candle parquet files are written to `/forex/candles` in HDFS. From a Spark shell, inspect them with:

```python
spark.read.parquet("hdfs://namenode:9000/forex/candles").show()
```

## Stream and model

1. OANDA's practice price stream emits bid/ask quotes for only `EUR_USD`, `USD_JPY`, and `EUR_JPY`. The app uses their mid-price and publishes validated ticks to Kafka topic `fx.ticks.raw`. OANDA documents that its price stream emits at most four prices per second per instrument.
2. Spark filters invalid/unwanted symbols, publishes clean events to `fx.ticks.cleaned`, and aggregates one-minute OHLC candles to `fx.candles` and HDFS. Forex quotes do not have consolidated trade volume; the candles record quote tick counts instead.
3. The backend's online SGD classifier learns one-candle direction labels from subsequent cleaned candles and predicts the next one-minute direction after 30 labeled examples per pair. The features include short returns, candle body/range, moving-average distance, RSI, and bid/ask spread.
4. Forecasts are experimental statistical estimates, not financial advice or a promise of future price movement. The model starts fresh on container restart; persisted cleaned candles remain available in HDFS for later offline retraining.

The OANDA practice feed may be closed on weekends and during market maintenance. The app does not fabricate market prices; it clearly reports missing credentials, provider reconnects, and pipeline readiness.

Run the online-model unit tests after installing the backend requirements with:

```bash
PYTHONPATH=backend python3 -m unittest discover -s backend/tests
```
