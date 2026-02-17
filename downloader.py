from pathlib import Path
import pandas as pd
import logging

from core import ForexSymbol, Timeframe
from provider import DataProvider


class Downloader:
    def __init__(self, provider: DataProvider):
        self.provider = provider
    
    def _save(self, data: pd.DataFrame, s: ForexSymbol, tf: Timeframe):
        provider_dir = Path(__file__).parent / "data" / self.provider.name
        provider_dir.mkdir(exist_ok=True)

        symbol_dir = provider_dir / str(s)
        symbol_dir.mkdir(exist_ok=True)

        filename = f"{self.provider.name}_{s.base}{s.quote}_{tf.length}{tf.unit}.csv"
        data.to_csv(symbol_dir / filename)
        logging.info(f"Save '{filename}'")

    def download(self, symbol: ForexSymbol, time_frame: Timeframe):
        """
        Orchestrate downloading process:
        - Download all the (`symbol`, `timeframe`)'s data its `DataProvider` can get.
        - Download everything if file does not exist.
        - Download only from latest data if file exists.
        """
        data_exist = False
        if data_exist:
            time_start = 42  # TODO: get latest time in the data
        else:
            time_start = None

        data = self.provider.get(symbol, time_frame, time_start)
        self._save(data, symbol, time_frame)
