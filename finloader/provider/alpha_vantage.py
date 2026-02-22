from io import StringIO
import logging

import pandas as pd
import requests

from .base import DataProvider
from ..core import ForexSymbol, Timeframe
from ..exceptions import TemporaryRateLimit, DailyRateLimit

logger = logging.getLogger(__name__)


class AlphaVantage(DataProvider):
    def __init__(self, api_key):
        super().__init__("alpha_vantage", api_key)

    def _get_api_function(self, tf: Timeframe) -> str:
        functions = {
            Timeframe.DAY: 'FX_DAILY',
            Timeframe.WEEK: 'FX_WEEKLY',
            Timeframe.MONTH: 'FX_MONTHLY'
        }
        if tf.unit not in functions:
            raise ValueError("AlphaVantage: unsupported Timeframe")
        return functions[tf.unit]

    def _get_api_outputsize(self, time_start_utc: pd.Timestamp) -> str:
        DIFF_DAYS_TO_DOWNLOAD_FULL = 90

        time_end_utc = pd.Timestamp.now(tz="UTC")
        if time_end_utc - time_start_utc >= pd.Timedelta(days=DIFF_DAYS_TO_DOWNLOAD_FULL):
            outputsize = "full"
        else:
            outputsize = "compact"
        logger.debug(f"using outputsize={outputsize}")
        return outputsize

    def _call_api(self, s: ForexSymbol, tf: Timeframe, time_start_utc: pd.Timestamp):
        params = {
            "from_symbol": s.base,
            "to_symbol": s.quote,
            "function": self._get_api_function(tf),
            "outputsize": self._get_api_outputsize(time_start_utc),
            "datatype": "csv",
            "apikey": self.api_key
        }
        try:
            res = requests.get("https://www.alphavantage.co/query", params, timeout=10)
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError("Not connected to the internet")

        if not res.ok:
            raise ValueError("AlphaVantage: data not downloaded")

        content_type = str(res.headers.get("Content-Type", ""))
        if "json" in content_type.lower():
            data = res.json()
            if "Information" in data:
                raise TemporaryRateLimit("AlphaVantage: temporary rate limited")
            raise ValueError(f"AlphaVantage: {data}")

        return res
    
    def _normalize(self, res) -> pd.DataFrame:
        df = pd.read_csv(StringIO(res.text), index_col="timestamp")
        df.index = pd.to_datetime(df.index, utc=True)
        df.index.name = "time"
        if "volume" not in df.columns:
            df["volume"] = 0
        df = df.sort_index()
        return df
