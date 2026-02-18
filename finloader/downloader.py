from pathlib import Path
import logging
import time

import pandas as pd

from .core import ForexSymbol, Timeframe
from .provider import DataProvider
from .exceptions import TemporaryRateLimit, DailyRateLimit
from .schema import validate_data


class Downloader:
    _data_dir = Path(__file__).parent / "data"

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
    # time_start calculation funcs
    ###########################################################################

    def _last_time_in_file(self, filepath: Path):
        df = pd.read_csv(filepath, index_col="time")
        df.index = pd.to_datetime(df.index, utc=True)
        return df.index[-1]

    def _get_time_start(self, s: ForexSymbol, tf: Timeframe):
        DEFAULT_TIME_START = pd.Timestamp("2000-01-01", tz="UTC")

        filepath = self._get_filepath(s, tf)
        if not filepath.exists():
            return DEFAULT_TIME_START

        time_start = self._last_time_in_file(filepath)
        logging.debug(f"time_start: {time_start}")
        return time_start
    
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
        time_start = self._get_time_start(s, tf)
        data = self._get_data(s, tf, time_start, **kwargs)
        self._save(data, s, tf)

    def _get_data(self, s: ForexSymbol, tf: Timeframe, time_start: pd.Timestamp) -> pd.DataFrame:
        """Sub-class must implement _get_data()"""
        data = self.provider.get(s, tf, time_start)
        return data

    def _save(self, data: pd.DataFrame, s: ForexSymbol, tf: Timeframe):
        validate_data(data)

        filepath = self._get_filepath(s, tf)
        if filepath.exists():
            existing = pd.read_csv(filepath, index_col="time")
            existing.index = pd.to_datetime(existing.index, utc=True)

            # Concatenate and remove duplicate timestamps (keep latest)
            combined = pd.concat([existing, data])
            combined = combined[~combined.index.duplicated(keep="last")]
            combined = combined.sort_index()
        else:
            combined = data.sort_index()

        validate_data(combined)
        combined.to_csv(filepath)
        logging.info(f"Save '{self._get_filename(s, tf)}'")


class RetriesDownloader(Downloader):
    def __init__(self, provider):
        super().__init__(provider)

    def _get_data(self,
                  s: ForexSymbol,
                  tf: Timeframe,
                  time_start: pd.Timestamp,
                  *,
                  max_retries=5,
                  base_sleep=5,
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
                logging.warning(f"{s} failed (attempt {retries}/{max_retries}): {e}")
                if retries >= max_retries:
                    logging.error(f"{s} permanently failed")
                    return None  # failure
                time.sleep(sleep_time)
                sleep_time = min(sleep_time * 2, max_sleep)  # exponential backoff

            except DailyRateLimit as e:
                logging.error(f"{self.provider}, daily rate limited")
                return None  # failure

        return None
