from pathlib import Path
import pandas as pd

from core import ForexSymbol, Timeframe
from provider import DataProvider


class Downloader:
    def __init__(self, provider: DataProvider):
        self.provider = provider
    
    def _save(self, data: pd.DataFrame, s: ForexSymbol, tf: Timeframe):
        name = f"{self.provider.__class__.__name__}_{s.base}{s.quote}_{tf.length}{tf.unit}.csv"
        data.to_csv(Path(__file__) / "data" / self.provider.name / name)

    def download(self, symbol: ForexSymbol, time_frame: Timeframe):
        """
        Orchestrate downloading process:
        - Download all the (`symbol`, `timeframe`)'s data its `DataProvider` can get.
        - Download everything if file does not exist.
        - Download only from latest data if file exists.
        """
        time_start = 0
        data = self.provider.get(symbol, time_frame, time_start)
        self._save(data, symbol, time_frame)
