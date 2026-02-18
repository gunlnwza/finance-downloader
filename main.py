import sys
import argparse
import logging

from dotenv import load_dotenv

from finloader.core import ForexSymbol, Timeframe
from finloader.provider import DataProvider
from finloader.downloader import RetriesDownloader


def main():
    load_dotenv()

    logging.basicConfig(
        filename='app.log',
        filemode='a',
        level=logging.DEBUG,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S'
    )
    logging.info("")
    logging.info(f"python3 {' '.join(sys.argv)}")

    parser = argparse.ArgumentParser()
    parser.add_argument("provider")
    parser.add_argument("--major", action="store_true")
    parser.add_argument("--base")
    parser.add_argument("--quote")
    parser.add_argument("--tf_length", type=int, required=True)
    parser.add_argument("--tf_unit", required=True)
    args = parser.parse_args()

    provider = DataProvider.from_name(args.provider)
    downloader = RetriesDownloader(provider)
    tf = Timeframe(args.tf_length, args.tf_unit)

    try:
        if args.major:
            for base, quote in ForexSymbol.MAJOR_PAIRS:
                s = ForexSymbol(base, quote)
                downloader.download(s, tf)
        else:
            if args.quote is None:
                raise ValueError("Quote currency must be provided unless using 'major'")
            s = ForexSymbol(args.base, args.quote)
            downloader.download(s, tf)
    except ConnectionError as e:
        logging.error("Not connected to the internet")
        sys.exit(e)


if __name__ == "__main__":
    main()
