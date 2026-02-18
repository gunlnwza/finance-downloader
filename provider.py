from io import StringIO
from abc import ABC, abstractmethod
import logging

import pandas as pd
import requests
from polygon import RESTClient

from core import ForexSymbol, Timeframe


class DataProvider(ABC):
    REQUIRED_INDEX_NAME = "time"
    REQUIRED_COLUMNS = ["open", "high", "low", "close", "volume"]

    def __init__(self, name: str, api_key: str):
        self.name = name
        self.api_key = api_key

    def get(self, symbol: ForexSymbol, tf: Timeframe, time_start_utc: pd.Timestamp) -> pd.DataFrame:
        """
        time_start_utc must be in UTC, every pd.Timestamp must be in UTC!
        """
        self._validate_input(time_start_utc)
        logging.info(f"Calling {self.name} API")
        raw = self._call_api(symbol, tf, time_start_utc)
        df = self._normalize(raw)
        return self._validate_schema(df)

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

    def _validate_schema(self, df):
        if df.index.name != self.REQUIRED_INDEX_NAME \
            or not all(col in df.columns for col in self.REQUIRED_COLUMNS):
            raise ValueError("Schema mismatch")
        
        if df.index.tz is None:
            raise ValueError("Schema's index must be UTC")
        if not df.index.is_monotonic_increasing:
            raise ValueError("Schema's index must be sorted")
        if df.index.has_duplicates:
            raise ValueError("Duplicate timestamps detected in schema")

        return df


class AlphaVantage(DataProvider):
    def __init__(self, api_key):
        super().__init__("alpha_vantage", api_key)

    def _get_api_outputsize(self, time_start_utc: pd.Timestamp) -> str:
        DIFF_DAYS_TO_DOWNLOAD_FULL = 90
        time_end_utc = pd.Timestamp.now(tz="UTC")
        if time_end_utc - time_start_utc >= pd.Timedelta(days=DIFF_DAYS_TO_DOWNLOAD_FULL):
            return "full"
        else:
            return "compact"

    def _get_api_function(self, tf: Timeframe) -> str:
        functions = {
            Timeframe.DAY: 'FX_DAILY',
            Timeframe.WEEK: 'FX_WEEKLY',
            Timeframe.MONTH: 'FX_MONTHLY'
        }
        if tf.unit not in functions:
            raise ValueError("AlphaVantage: unsupported Timeframe")
        return functions[tf.unit]

    def _call_api(self, s: ForexSymbol, tf: Timeframe, time_start_utc: pd.Timestamp):
        params = {
            "from_symbol": s.base,
            "to_symbol": s.quote,
            "outputsize": self._get_api_outputsize(time_start_utc),
            "function": self._get_api_function(tf),
            "datatype": "csv",
            "apikey": self.api_key
        }
        res = requests.get("https://www.alphavantage.co/query", params, timeout=10)
        if not res.ok:
            raise ValueError("AlphaVantage: data not downloaded")

        content_type = res.headers.get("Content-Type", "")
        if content_type and "json" in content_type.lower():
            raise ValueError(f"AlphaVantage: {res.json()}")

        return res
    
    def _normalize(self, res) -> pd.DataFrame:
        df = pd.read_csv(StringIO(res.text), index_col="timestamp", parse_dates=True)
        if "volume" not in df.columns:
            df["volume"] = 0
        df.index.name = "time"
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
        logging.debug(f"Massive, res: {res}")
        return res

    def _call_api(self, s: ForexSymbol, tf: Timeframe, time_start_utc: pd.Timestamp):
        client = RESTClient(self.api_key)

        time_end_utc = pd.Timestamp.now(tz="UTC")
        aggs = list(client.list_aggs(
            ticker=f"C:{s.base}{s.quote}",
            multiplier=tf.length,
            timespan=self._get_api_timespan(tf),
            from_=self._convert_timestamp(time_start_utc),
            to=self._convert_timestamp(time_end_utc),
            adjusted="true",
            sort="asc"
        ))
        if not aggs:
            raise ValueError("Massive: data not downloaded")

        return aggs

    def _normalize(self, aggs):
        df = pd.DataFrame(aggs)
        logging.debug(f"df\n{df}")
        if "volume" not in df.columns:
            df["volume"] = 0
        df.index = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df.index.name = "time"
        df.drop(["vwap", "timestamp", "transactions", "otc"], axis=1, inplace=True)
        logging.debug(f"df\n{df}")
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
        res = requests.get("https://api.twelvedata.com/time_series", params, timeout=10)
        if not res.ok:
            raise ValueError("TwelveData: data not downloaded")

        return res

    def _normalize(self, res):
        logging.debug("TwelveData._normalize() | res.text")
        logging.debug(f"\n{res.text}")
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
        logging.debug("TwelveData._normalize() | df")
        logging.debug(f"\n{df}")
        return df
