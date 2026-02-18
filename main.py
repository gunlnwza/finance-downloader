import sys
import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv

from finloader.core import ForexSymbol, Timeframe
from finloader.provider import DataProvider
from finloader.downloader import RetriesDownloader

logger = logging.getLogger("finloader.cli")
logger.setLevel(logging.DEBUG)


def setup_logging():
    LOG_DIR = Path("logs")
    LOG_DIR.mkdir(exist_ok=True)
    
    log_filepath = LOG_DIR / "finloader.log"

    logging.basicConfig(
        filename=log_filepath,
        filemode='a',
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
        datefmt='%m-%d-%Y %H:%M:%S'
    )

    # Silence noisy third-party libraries
    noisy_libs = [
        # "urllib3",
        # "urllib3.connectionpool",
        # "requests",
        # "chardet",
        # "charset_normalizer",
    ]
    for lib in noisy_libs:
        logging.getLogger(lib).setLevel(logging.ERROR)


def parse_inputs():
    parser = argparse.ArgumentParser()
    parser.add_argument("provider")
    parser.add_argument("base")
    parser.add_argument("quote")
    parser.add_argument("tf_length", type=int)
    parser.add_argument("tf_unit")
    return parser.parse_args()


def main():
    load_dotenv()
    setup_logging()

    args = parse_inputs()
    logger.info("")
    logger.info(f"python3 {' '.join(sys.argv)}")

    provider = DataProvider.from_name(args.provider)
    s = ForexSymbol(args.base, args.quote)
    tf = Timeframe(args.tf_length, args.tf_unit)
    try:
        downloader = RetriesDownloader(provider)
        downloader.download(s, tf)
    except ConnectionError as e:
        logger.error(e)
        sys.exit(e)
    except KeyboardInterrupt as e:
        sys.exit(e)


if __name__ == "__main__":
    main()
