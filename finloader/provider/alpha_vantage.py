from io import StringIO
import logging

import pandas as pd
import requests

from .base import DataProvider
from ..core import ForexSymbol, Timeframe
from ..exceptions import TemporaryRateLimit, DailyRateLimit

logger = logging.getLogger(__name__)


class AlphaVantage(DataProvider):
    DIFF_DAYS_TO_DOWNLOAD_FULL = 90

    def __init__(self, api_key):
        super().__init__("alpha_vantage", api_key)

    def _get_api_function(self, tf: Timeframe) -> str:
        functions = {
            Timeframe.DAY: 'FX_DAILY',
            Timeframe.WEEK: 'FX_WEEKLY',
            Timeframe.MONTH: 'FX_MONTHLY'
        }
        if tf.unit not in functions:
            raise ValueError(f"AlphaVantage: timeframe '{tf}' is not supported by free API")
        return functions[tf.unit]

    def _get_api_outputsize(self, utc_start: pd.Timestamp) -> str:
        time_diff = pd.Timestamp.now(tz="UTC") - utc_start
        timedelta = pd.Timedelta(days=AlphaVantage.DIFF_DAYS_TO_DOWNLOAD_FULL)
        return "full" if time_diff >= timedelta else "compact"

    def _call_api(self, s: ForexSymbol, tf: Timeframe, time_start_utc: pd.Timestamp):
        params = {
            "from_symbol": s.base,
            "to_symbol": s.quote,
            "function": self._get_api_function(tf),
            "outputsize": self._get_api_outputsize(time_start_utc),
            "datatype": "csv",  # API data sending format, DO NOT EDIT
            "apikey": self.api_key
        }
        logger.debug(f"using outputsize={params['outputsize']}")
        try:
            res = requests.get("https://www.alphavantage.co/query", params, timeout=10)
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError("Not connected to the internet")

        if not res.ok:
            raise ValueError("AlphaVantage: data not downloaded")

        content_type = str(res.headers.get("Content-Type", ""))
        if "json" in content_type.lower():
            data = res.json()
            if "Error Message" in data:
                raise ValueError(f"AlphaVantage: {data['Error Message']}")
            elif "Information" in data:
                if "our standard API rate limit is 25 requests per day" in data["Information"]:
                    raise DailyRateLimit("AlphaVantage: daily rate-limited")
                else:  # Please consider spreading out your free API requests more sparingly
                    raise TemporaryRateLimit("AlphaVantage: temporary rate-limited")
            else:
                raise ValueError(f"AlphaVantage: {data}")

        return res
    
    def _normalize(self, res) -> pd.DataFrame:
        df = pd.read_csv(StringIO(res.text), index_col="timestamp")  # converting AlphaVantage CSV into df, DO NOT EDIT
        df.index = pd.to_datetime(df.index, utc=True)
        df.index.name = "time"
        if "volume" not in df.columns:
            df["volume"] = 0
        df = df.sort_index()
        return df
