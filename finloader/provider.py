from io import StringIO
from abc import ABC, abstractmethod
import logging
import os

import pandas as pd
import requests
from polygon import RESTClient
import urllib3

from .core import ForexSymbol, Timeframe
from .exceptions import TemporaryRateLimit, DailyRateLimit
from .schema import validate_data

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class DataProvider(ABC):
    def __init__(self, name: str, api_key: str):
        self.name = name
        self.api_key = api_key

    @classmethod
    def from_name(cls, name: str):
        if name == "alpha_vantage":
            return AlphaVantage(os.getenv("ALPHA_VANTAGE_API_KEY"))
        elif name == "massive":
            return Massive(os.getenv("MASSIVE_API_KEY"))
        elif name == "twelve_data":
            return TwelveData(os.getenv("TWELVE_DATA_API_KEY"))
        else:
            raise ValueError(f"Unsupported provider: {name}")

    def __str__(self):
        return self.name
    
    def __repr__(self):
        return self.__class__.__name__

    def get(self, s: ForexSymbol, tf: Timeframe, time_start_utc: pd.Timestamp) -> pd.DataFrame:
        """
        time_start_utc must be in UTC, every pd.Timestamp must be in UTC!
        """
        self._validate_input(time_start_utc)

        logger.info(f"Calling {self.name} API for: {s} ({tf})")
        raw = self._call_api(s, tf, time_start_utc)

        df = self._normalize(raw)
        validate_data(df)
        return df

    def _validate_input(self, time_start_utc: pd.Timestamp):
        # symbol, and tf are already validated on construction

        if time_start_utc.tzinfo is None:
            raise ValueError("Must be timezone-aware UTC")
        if str(time_start_utc.tz) != "UTC":
            raise ValueError("Must pass UTC timestamp")

    @abstractmethod
    def _call_api(self, s: ForexSymbol, tf: Timeframe, time_start_utc: pd.Timestamp):
        pass

    @abstractmethod
    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        pass


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


class TwelveData(DataProvider):
    def __init__(self, api_key):
        super().__init__("twelve_data", api_key)

    def _get_api_symbol(self, s: ForexSymbol):
        return f"{s.base}/{s.quote}"  # must have / in-between

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
            "format": "CSV",
            "apikey": self.api_key
        }
        try:
            res = requests.get("https://api.twelvedata.com/time_series", params, timeout=10)
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError("Not connected to the internet")

        if not res.ok:
            raise ValueError("TwelveData: data not downloaded")

        # logger.debug(f"\nres.text[:300]\n{res.text[:300]}")

        content_type = res.headers.get("Content-Type", "")
        if "json" in content_type.lower():
            data = res.json()
            match data["code"]:
                case 400:  # No data is available on the specified dates
                    return None
                case 429:  # rate limited
                    raise TemporaryRateLimit("TwelveData: temporary rate limited")

        return res

    def _normalize(self, res):
        if res is None:
            df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        else:
            df = pd.read_csv(
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
