import argparse
import os
import logging

from dotenv import load_dotenv

from provider import AlphaVantage, Massive, TwelveData, ForexSymbol, Timeframe
from downloader import Downloader

if __name__ == "__main__":
    load_dotenv()
    logging.basicConfig(
        filename='app.log',
        filemode='w',
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)-8s: %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S'
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("provider")
    parser.add_argument("base")
    parser.add_argument("quote")
    parser.add_argument("tf_length", type=int)
    parser.add_argument("tf_unit")
    args = parser.parse_args()

    if args.provider == "alpha_vantage":
        provider = AlphaVantage(os.getenv("ALPHA_VANTAGE_API_KEY"))
    elif args.provider == "massive":
        provider = Massive(os.getenv("MASSIVE_API_KEY"))
    elif args.provider == "twelve_data":
        provider = TwelveData(os.getenv("TWELVE_DATA_API_KEY"))
    else:
        raise ValueError(f"Unsupported provider: {args.provider}")

    downloader = Downloader(provider)
    symbol = ForexSymbol(args.base, args.quote)
    tf = Timeframe(args.tf_length, args.tf_unit)
    downloader.download(symbol, tf)
