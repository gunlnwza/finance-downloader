import logging

import pandas as pd
from polygon import RESTClient
import urllib3

from .base import DataProvider
from ..core import ForexSymbol, Timeframe
from ..exceptions import TemporaryRateLimit

logger = logging.getLogger(__name__)


class Massive(DataProvider):
    ALLOWED_TIMEFRAME_UNITS = (Timeframe.DAY, Timeframe.WEEK, Timeframe.MONTH)

    def __init__(self, api_key):
        super().__init__("massive", api_key, base_sleep=60)

    @classmethod
    def _convert_timestamp(cls, ts: pd.Timestamp):
        return ts.strftime("%Y-%m-%d")

    def _get_api_timespan(self, tf: Timeframe):
        return {
            Timeframe.MINUTE: "minute",
            Timeframe.HOUR: "hour",
            Timeframe.DAY: "day",
            Timeframe.WEEK: "week",
            Timeframe.MONTH: "month", 
        }[tf.unit]
    
    def _call_api(self, s: ForexSymbol, tf: Timeframe, utc_start: pd.Timestamp):
        if tf.unit not in Massive.ALLOWED_TIMEFRAME_UNITS:
            raise ValueError(f"Massive: timeframe '{tf}' is not supported by free API")

        client = RESTClient(self.api_key)
        utc_end = pd.Timestamp.now(tz="UTC")

        try:
            aggs = list(client.list_aggs(
                ticker=f"C:{s.base}{s.quote}",
                multiplier=tf.length,
                timespan=self._get_api_timespan(tf),
                from_=self._convert_timestamp(utc_start),
                to=self._convert_timestamp(utc_end),
                adjusted="true",
                sort="asc"
            ))
        except urllib3.exceptions.MaxRetryError as e:
            root = e.__cause__ or e
            if isinstance(root, urllib3.exceptions.NameResolutionError):  # DNS / no internet
                raise ConnectionError("Massive: not connected to the internet") from e
            else:
                raise TemporaryRateLimit("Massive: temporary rate-limited") from e

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
