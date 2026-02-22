import sys
import argparse
import logging
from logging.handlers import RotatingFileHandler
from rich.logging import RichHandler
from pathlib import Path

from dotenv import load_dotenv

from finloader.core import ForexSymbol, Timeframe
from finloader.provider import DataProvider
from finloader.downloader import Downloader

logger = logging.getLogger("finloader.cli")


def setup_logging(log_path: str = "logs/finloader.log", level=logging.INFO):
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()

    # Prevent duplicate logs if setup_logging() is called more than once
    if root.handlers:
        for h in list(root.handlers):
            root.removeHandler(h)

    root.setLevel(level)

    # Terminal (Rich)
    rich_handler = RichHandler(
        show_time=True,
        show_level=True,
        show_path=True,
        rich_tracebacks=True,
        markup=False,
    )
    rich_handler.setLevel(level)
    root.addHandler(rich_handler)

    # Rotating file
    file_fmt = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = RotatingFileHandler(
        log_path,
        maxBytes=1000_000,
        backupCount=1,
        encoding="utf-8",
    )
    fh.setLevel(level)
    fh.setFormatter(file_fmt)
    root.addHandler(fh)

    # Silence third-party libraries
    noisy_libs = [
        "urllib3",
        "urllib3.connectionpool",
        "requests",
        "chardet",
        "charset_normalizer",
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
    parser.add_argument("-d", "--debug", action="store_true")
    return parser.parse_args()


def main():
    load_dotenv()

    args = parse_inputs()
    setup_logging(level=logging.DEBUG if args.debug else logging.INFO)
    logger.info(f"$ python3 {' '.join(sys.argv)}")

    try:
        provider = DataProvider.from_name(args.provider)
        s = ForexSymbol(args.base, args.quote)
        tf = Timeframe(args.tf_length, args.tf_unit)

        downloader = Downloader(provider)
        downloader.download(s, tf)
    except KeyboardInterrupt:
        pass
    except ValueError as e:
        logger.error(e)
    except Exception:
        logger.exception("Unhandled error")
        sys.exit(1)


if __name__ == "__main__":
    main()
