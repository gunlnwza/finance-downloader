from pathlib import Path
import logging
import time

import socket
import pandas as pd

from .core import ForexSymbol, Timeframe
from .provider import DataProvider
from .exceptions import TemporaryRateLimit, DailyRateLimit
from .schema import validate_data

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Downloader:
    _data_dir = Path(__file__).parent.parent / "data"

    def __init__(self, provider: DataProvider):
        self.provider = provider

        Downloader._data_dir.mkdir(exist_ok=True)

        self._provider_dir = Downloader._data_dir / self.provider.name
        self._provider_dir.mkdir(exist_ok=True)

    ###########################################################################
    # Files helpers
    ###########################################################################

    def _symbol_dir(self, s: ForexSymbol):
        dir = self._provider_dir / str(s)
        dir.mkdir(exist_ok=True)
        return dir

    def _get_filename(self, s: ForexSymbol, tf: Timeframe):
        return f"{self.provider.name}_{s.base}{s.quote}_{tf.length}{tf.unit}.csv"

    def _get_filepath(self, s: ForexSymbol, tf: Timeframe):
        return self._symbol_dir(s) / self._get_filename(s, tf)

    ###########################################################################
    # time-related funcs
    ###########################################################################

    def _last_time_in_file(self, filepath: Path):
        df = pd.read_csv(filepath, index_col="time")
        df.index = pd.to_datetime(df.index, utc=True)
        return df.index[-1]

    def _get_data_latest_utc(self, s: ForexSymbol, tf: Timeframe):
        DEFAULT_TIME_START = pd.Timestamp("2000-01-01", tz="UTC")

        filepath = self._get_filepath(s, tf)
        if not filepath.exists():
            return DEFAULT_TIME_START

        time_start_utc = self._last_time_in_file(filepath)
        logger.debug(f"requested time_start_utc = {time_start_utc}")
        return time_start_utc
    
    def _is_data_stale(self, data_latest_utc: pd.Timestamp, tf: Timeframe):
        now = pd.Timestamp.now(tz="UTC")
        if not tf.is_intraday:
            now = now.normalize()  # zero out the time if (day, week, month)

        time_diff = now - data_latest_utc
        logger.debug(f"lhs (time diff): {time_diff}, rhs (tf.timedelta): {tf.timedelta}")

        return time_diff > tf.timedelta
    
    ###########################################################################
    # Main funcs
    ###########################################################################

    def download(self, s: ForexSymbol, tf: Timeframe, **kwargs):
        """
        Orchestrate downloading process:
        - Download all the (`s`, `tf`)'s data its `DataProvider` can get.
        - Download everything if file does not exist.
        - Download only from latest data if file exists.
        """
        data_latest_utc = self._get_data_latest_utc(s, tf)
        if not self._is_data_stale(data_latest_utc, tf):
            logger.info(f"'{self._get_filename(s, tf)}' is up to date")
            return

        data = self._get_data(s, tf, data_latest_utc, **kwargs)
        self._save(data, s, tf)

    def _get_data(self, s: ForexSymbol, tf: Timeframe, time_start_utc: pd.Timestamp) -> pd.DataFrame:
        """Sub-class must implement _get_data()"""
        data = self.provider.get(s, tf, time_start_utc)
        return data

    def _save(self, data: pd.DataFrame, s: ForexSymbol, tf: Timeframe):
        validate_data(data)

        if len(data) == 0:
            logger.info(f"'{self._get_filename(s, tf)}' is up to date")
            return

        filepath = self._get_filepath(s, tf)
        if filepath.exists():
            existing = pd.read_csv(filepath, index_col="time")
            existing.index = pd.to_datetime(existing.index, utc=True)
            old_len = len(existing)

            logger.debug(f"\n\n{data.tail()}\n")

            # Concatenate and remove duplicate timestamps (keep latest)
            combined = pd.concat([existing, data])
            combined = combined[~combined.index.duplicated(keep="last")]
            combined = combined.sort_index()
        else:
            old_len = 0
            combined = data.sort_index()

        validate_data(combined)
        combined.to_csv(filepath)
        logger.info(f"Save '{self._get_filename(s, tf)}' ({len(combined) - old_len} bars added)")


class RetriesDownloader(Downloader):
    def __init__(self, provider):
        super().__init__(provider)

    def _get_data(self,
                  s: ForexSymbol,
                  tf: Timeframe,
                  time_start: pd.Timestamp,
                  *,
                  max_retries=5,
                  base_sleep=20,
                  max_sleep=60
                ) -> pd.DataFrame:
        retries = 0
        sleep_time = base_sleep

        while retries < max_retries:
            try:
                data = self.provider.get(s, tf, time_start)
                return data  # success

            except TemporaryRateLimit as e:
                retries += 1
                logger.warning(f"{s} failed (attempt {retries}/{max_retries}): {e}")
                if retries >= max_retries:
                    logger.error(f"{s} permanently failed")
                    return None  # failure
                logger.warning(f"trying again in {sleep_time}s")
                time.sleep(sleep_time)
                sleep_time = min(sleep_time * 2, max_sleep)  # exponential backoff

            except DailyRateLimit as e:
                logger.error(f"{self.provider}, daily rate limited")
                return None  # failure

        return None
