import os
from dotenv import load_dotenv
from provider import AlphaVantage, Massive, TwelveData, ForexSymbol, Timeframe
from downloader import Downloader


if __name__ == "__main__":
    load_dotenv()

    symbol = ForexSymbol("EUR", "USD")
    tf = Timeframe(1, 'day')

    # provider = AlphaVantage(os.getenv("ALPHA_VANTAGE_API_KEY"))
    # provider = Massive(os.getenv("MASSIVE_API_KEY"))
    provider = TwelveData(os.getenv("TWELVE_DATA_API_KEY"))

    downloader = Downloader(provider)
    downloader.download(symbol, tf)
