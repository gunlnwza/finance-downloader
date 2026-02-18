import argparse
import os
import logging
import time

from dotenv import load_dotenv
import requests

from provider import AlphaVantage, Massive, TwelveData, ForexSymbol, Timeframe
from downloader import Downloader


def multiple_retries_download(downloader: Downloader, s: ForexSymbol, tf: Timeframe):
    MAX_RETRIES = 5
    BASE_SLEEP = 5
    MAX_SLEEP = 60
    
    retries = 0
    sleep_time = BASE_SLEEP

    while retries < MAX_RETRIES:
        try:
            downloader.download(s, tf)
            break  # success → exit retry loop

        except requests.exceptions.ConnectionError as e:
            retries += 1
            logging.warning(
                f"{base}/{quote} failed (attempt {retries}/{MAX_RETRIES}): {e}"
            )

            if retries >= MAX_RETRIES:
                logging.error(f"{base}/{quote} permanently failed.")
                break

            time.sleep(min(sleep_time, MAX_SLEEP))
            sleep_time *= 2  # exponential backoff


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
    parser.add_argument("base", help='Base currency or "major"')
    parser.add_argument("quote", nargs="?", help="Quote currency (omit if using major)")
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
    tf = Timeframe(args.tf_length, args.tf_unit)

    if args.base.lower() == "major":
        major_pairs = [
            ("EUR", "USD"),
            ("GBP", "USD"),
            ("USD", "JPY"),
            ("USD", "CHF"),
            ("AUD", "USD"),
            ("NZD", "USD"),
            ("USD", "CAD"),
        ]
        for base, quote in major_pairs:
            s = ForexSymbol(base, quote)
            multiple_retries_download(downloader, s, tf)
    else:
        if args.quote is None:
            raise ValueError("Quote currency must be provided unless using 'major'")
        s = ForexSymbol(args.base, args.quote)
        multiple_retries_download(downloader, s, tf)
