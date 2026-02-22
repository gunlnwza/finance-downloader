from pathlib import Path
import logging

import pandas as pd

from .core import ForexSymbol, Timeframe
from .provider import DataProvider
from .schema import validate_data

logger = logging.getLogger(__name__)


class Downloader:
    _data_dir = Path(__file__).parent.parent / "data"
    DEFAULT_TIME_START = pd.Timestamp("2000-01-01", tz="UTC")

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
        filepath = self._get_filepath(s, tf)
        if not filepath.exists():
            return Downloader.DEFAULT_TIME_START

        utc_start = self._last_time_in_file(filepath)
        logger.debug(f"requested utc_start = {utc_start}")
        return utc_start
    
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

    def download(self, s: ForexSymbol, tf: Timeframe):
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

        data = self.provider.get(s, tf, data_latest_utc)
        self._save(data, s, tf)

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
