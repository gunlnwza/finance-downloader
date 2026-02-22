from abc import ABC, abstractmethod
import logging
import os
import time

import pandas as pd

from ..core import ForexSymbol, Timeframe
from ..schema import validate_data
from ..exceptions import TemporaryRateLimit, DailyRateLimit

logger = logging.getLogger(__name__)


class DataProvider(ABC):
    def __init__(self, name: str, api_key: str):
        self.name = name
        self.api_key = api_key

    @classmethod
    def from_name(cls, name: str):
        if name == "alpha_vantage":
            return AlphaVantage(os.getenv("ALPHA_VANTAGE_API_KEY"))
        elif name == "massive":
            return Massive(os.getenv("MASSIVE_API_KEY"))
        elif name == "twelve_data":
            return TwelveData(os.getenv("TWELVE_DATA_API_KEY"))
        else:
            raise ValueError(f"Unsupported provider: {name}")

    def __str__(self):
        return self.name
    
    def __repr__(self):
        return self.__class__.__name__

    def get(self, s: ForexSymbol, tf: Timeframe, utc_start: pd.Timestamp) -> pd.DataFrame:
        """
        `utc_start` must be in UTC, (every pd.Timestamp used must be in UTC).
        """
        if utc_start.tzinfo is None:
            raise ValueError("Must be timezone-aware UTC")
        if str(utc_start.tz) != "UTC":
            raise ValueError("Must pass UTC timestamp")

        logger.info(f"Calling {self.name} API for: {s} ({tf})")
        raw = self._call_api_with_retries(s, tf, utc_start)  # handle rate-limits
        if raw is None:
            logger.warning(f"'{s}' ({tf}) was not downloaded")
            return None

        df = self._normalize(raw)
        validate_data(df)
        return df
    
    def _call_api_with_retries(
            self, s: ForexSymbol, tf: Timeframe, utc_start: pd.Timestamp, *,
            max_retries=5, base_sleep=20, max_sleep=60
        ) -> pd.DataFrame:

        retries = 0
        sleep_time = base_sleep

        while retries < max_retries:
            try:
                data = self._call_api(s, tf, utc_start)
                return data  # success
            except TemporaryRateLimit as e:
                retries += 1
                logger.warning(f"{s} failed (attempt {retries}/{max_retries}): {e}")
                if retries >= max_retries:
                    logger.error(f"{s} permanently failed")
                    break

                logger.warning(f"trying again in {sleep_time}s")
                time.sleep(sleep_time)

                sleep_time = min(sleep_time * 2, max_sleep)  # exponential backoff
            except DailyRateLimit as e:
                logger.warning(f"{self.name}: daily rate-limited")
                break

        return None  # failure

    @abstractmethod
    def _call_api(self, s: ForexSymbol, tf: Timeframe, time_start_utc: pd.Timestamp):
        pass

    @abstractmethod
    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        pass


from .alpha_vantage import AlphaVantage
from .massive import Massive
from .twelve_data import TwelveData
