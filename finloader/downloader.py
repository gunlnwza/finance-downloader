from pathlib import Path
import logging

import pandas as pd

from .core import ForexSymbol, Timeframe
from .provider import DataProvider
from .schema import validate_data, FILE_EXTENSION

logger = logging.getLogger(__name__)


class SymbolFile:
    DEFAULT_TIME_START = pd.Timestamp("2000-01-01", tz="UTC")  # in the case of empty file

    def __init__(self, provider_dir: Path, s: ForexSymbol, tf: Timeframe):
        self.provider_dir = provider_dir
        self.symbol = s
        self.tf = tf

        self.dir = self.provider_dir / str(s)
        self.dir.mkdir(parents=True, exist_ok=True)

        self.name = f"{self.provider_dir.name}_{self.symbol}_{self.tf}.{FILE_EXTENSION}"

        self.path = self.dir / self.name

    def __str__(self):
        return self.name

    def __repr__(self):
        return f"SymbolFile({self.provider_dir.name}, {self.symbol}, {self.tf})"

    def exists(self):
        return self.path.exists()

    def latest_utc(self):
        if not self.path.exists():
            return SymbolFile.DEFAULT_TIME_START

        df = pd.read_parquet(self.path)
        if df.empty:
            return SymbolFile.DEFAULT_TIME_START

        # index is already persisted in parquet; assume it is sorted
        return df.index.max()

    def need_update(self):
        now = pd.Timestamp.now(tz="UTC")
        if self.tf in (Timeframe.DAY, Timeframe.WEEK, Timeframe.MONTH):
            now = now.normalize()  # zero out the time part

        time_diff = now - self.latest_utc()
        logger.debug(
            "%s need_update=%s (time_diff=%s, tf=%s)",
            self,
            time_diff >= self.tf.timedelta,
            time_diff,
            self.tf.timedelta,
        )
        return time_diff >= self.tf.timedelta


class Downloader:
    def __init__(self, provider: DataProvider, data_dir: Path | None = None):
        self.provider = provider

        if data_dir is None:
            project_root = Path(__file__).resolve().parents[1]
            data_dir = project_root / "data"

        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.provider_dir = self.data_dir / self.provider.name
        self.provider_dir.mkdir(parents=True, exist_ok=True)

    def download(self, symbol: ForexSymbol, tf: Timeframe):
        """
        Orchestrate downloading process:
        - Download all the (`s`, `tf`)'s data its `DataProvider` can get.
        - Download everything if file does not exist.
        - Download only from latest data if file exists.
        """
        symbol_file = SymbolFile(self.provider_dir, symbol, tf)
        if not symbol_file.need_update():
            logger.info(f"'{symbol_file}' is up to date")
            return
        
        data = self.provider.get(symbol, tf, symbol_file.latest_utc())
        if data is None:
            logger.warning(f"'{symbol_file}' is not updated")
            return
        self._save(data, symbol_file)

    def _save(self, data: pd.DataFrame, symbol_file: SymbolFile):
        validate_data(data)
        if symbol_file.exists():
            logger.info(f"Appending to '{symbol_file}'")
            old_len, appended = self._append_data(data, symbol_file)
        else:
            logger.info(f"Create new file '{symbol_file}'")
            old_len, appended = 0, data
        validate_data(appended)

        appended.to_parquet(symbol_file.path)
        logger.info(f"Save '{symbol_file}' ({len(appended) - old_len} bars added)")

    def _append_data(self, data: pd.DataFrame, symbol_file: SymbolFile):
        # TODO: optimize later with stream-based method

        # Load the old file
        existing = pd.read_parquet(symbol_file.path)
        old_len = len(existing)

        # Concatenate and remove duplicate timestamps (keep latest)
        new = pd.concat([existing, data])
        new = new[~new.index.duplicated(keep="last")]
        new = new.sort_index()

        return old_len, new
