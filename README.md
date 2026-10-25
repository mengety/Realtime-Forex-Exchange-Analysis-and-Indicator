# DomainFree Market Signal

A live crypto-market anomaly dashboard for high-frequency ticker updates. The backend consumes CoinGecko's CGSimplePrice WebSocket feed, applies a rolling Isolation Forest to per-asset returns and volume changes, and fans updates out to browser clients over a local WebSocket.

## Run locally

```bash
docker compose up --build
```

Open [http://localhost](http://localhost). Without a CoinGecko API key, the app labels its generated sample stream as **Demo feed**. To connect live data, create a `.env` file in the project root:

```dotenv
COINGECKO_API_KEY=your_paid_plan_api_key
```

Then run:

```bash
docker compose --env-file .env up --build
```

CoinGecko WebSockets require a paid plan (Basic or above). The API key stays in the backend and is never sent to the browser. Disconnections automatically retry with exponential backoff.

## Components

- `backend/app/market.py` maintains one CoinGecko connection, reconnects with backoff, and broadcasts ticker updates.
- `backend/app/detector.py` keeps a bounded 240-observation window per asset and periodically retrains an Isolation Forest off the event loop.
- `frontend/` is a dependency-free dashboard served by Nginx, which proxies HTTP API requests and browser WebSockets to the backend.
- `docker-compose.yml` runs the API and frontend. The legacy Kafka/Postgres development stack remains under `docker/`.

The demo feed is for UI and model evaluation only. Anomaly signals are statistical indicators, not financial advice.
