import logging
from io import StringIO

import pandas as pd
import requests

from .base import DataProvider
from ..core import ForexSymbol, Timeframe
from ..exceptions import TemporaryRateLimit, DailyRateLimit

logger = logging.getLogger(__name__)


class TwelveData(DataProvider):
    def __init__(self, api_key):
        super().__init__("twelve_data", api_key, base_sleep=60)

    def _get_api_symbol(self, s: ForexSymbol):
        return f"{s.base}/{s.quote}"  # must have '/' in-between

    def _get_api_interval(self, tf: Timeframe):
        unit = {
            Timeframe.MINUTE: "min",
            Timeframe.HOUR: "h",
            Timeframe.DAY: "day",
            Timeframe.WEEK: "week",
            Timeframe.MONTH: "month", 
        }[tf.unit]
        return f"{tf.length}{unit}"

    def _get_api_start_date(self, time_start_utc: pd.Timestamp):
        return time_start_utc.strftime("%Y-%m-%dT%H:%M:%S")

    def _call_api(self, s: ForexSymbol, tf: Timeframe, time_start_utc: pd.Timestamp):
        params = {
            "symbol": self._get_api_symbol(s),
            "interval": self._get_api_interval(tf),
            "start_date": self._get_api_start_date(time_start_utc),
            "timezone": "UTC",
            "format": "CSV",  # API data sending format, DO NOT EDIT
            "apikey": self.api_key
        }
        try:
            res = requests.get("https://api.twelvedata.com/time_series", params, timeout=10)
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError("Not connected to the internet")

        if not res.ok:
            raise ValueError("TwelveData: data not downloaded")

        content_type = res.headers.get("Content-Type", "")
        if "json" in content_type.lower():
            data = res.json()
            match data["code"]:
                case 400:  # No data is available on the specified dates
                    return None
                case 429:  # rate limited
                    raise TemporaryRateLimit("TwelveData: temporary rate limited")
                case _:
                    raise ValueError("Twelve data: unhandled error code")

        return res

    def _normalize(self, res):
        if res is None:
            df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        else:
            df = pd.read_csv(  # converting Twelve Data CSV, DO NOT EDIT
                StringIO(res.text),
                sep=";",
                index_col="datetime",
                parse_dates=True
            )
            if "volume" not in df.columns:
                df["volume"] = 0

        df.index.name = "time"
        df.index = pd.to_datetime(df.index, utc=True)
        df = df.sort_index(ascending=True)
        return df
