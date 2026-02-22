import logging

import pandas as pd
from polygon import RESTClient
import urllib3

from .base import DataProvider
from ..core import ForexSymbol, Timeframe
from ..exceptions import TemporaryRateLimit, DailyRateLimit

logger = logging.getLogger(__name__)


class Massive(DataProvider):
    def __init__(self, api_key):
        super().__init__("massive", api_key)

    def _get_api_timespan(self, tf: Timeframe):
        return {
            Timeframe.MINUTE: "minute",
            Timeframe.HOUR: "hour",
            Timeframe.DAY: "day",
            Timeframe.WEEK: "week",
            Timeframe.MONTH: "month", 
        }[tf.unit]
    
    def _convert_timestamp(self, ts: pd.Timestamp):
        res = ts.strftime("%Y-%m-%d")
        return res

    def _call_api(self, s: ForexSymbol, tf: Timeframe, time_start_utc: pd.Timestamp):
        client = RESTClient(self.api_key)
        time_end_utc = pd.Timestamp.now(tz="UTC")

        try:
            aggs = list(client.list_aggs(
                ticker=f"C:{s.base}{s.quote}",
                multiplier=tf.length,
                timespan=self._get_api_timespan(tf),
                from_=self._convert_timestamp(time_start_utc),
                to=self._convert_timestamp(time_end_utc),
                adjusted="true",
                sort="asc"
            ))
        except urllib3.exceptions.MaxRetryError as e:
            root = e.__cause__ or e
            if isinstance(root, urllib3.exceptions.NameResolutionError):
                raise ConnectionError("Not connected to the internet")
            else:
                raise TemporaryRateLimit("Massive: temporary rate limited")

        if not aggs:
            raise ValueError("Massive: data not downloaded")
        return aggs

    def _normalize(self, aggs):
        df = pd.DataFrame(aggs)
        if "volume" not in df.columns:
            df["volume"] = 0
        df.index = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df.index.name = "time"
        df.drop(["vwap", "timestamp", "transactions", "otc"], axis=1, inplace=True)
        return df

