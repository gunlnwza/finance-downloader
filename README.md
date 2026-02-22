# fin-loader

> Because downloading data incrementally should be clean and convenient.

Small market data downloader.

Pulls OHLCV data from multiple providers, and stores it locally for research and backtesting.

## What it does
-	Fetches forex (and other) data from supported providers
-	Handles incremental updates
-	Retries on temporary failures
-	Logs errors cleanly
-	Saves to Parquet

Designed to be run by cron.

## Providers (current)
- Alpha Vantage
- Massive
- Twelve Data

## CLI
Example:
```bash
python3 main.py alpha_vantage major 1 day
python3 main.py massive major 1 day
python3 main.py twelve_data major 1 hour
```
