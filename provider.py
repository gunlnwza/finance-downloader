from io import StringIO
from abc import ABC, abstractmethod

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

    def get(self, symbol: ForexSymbol, tf: Timeframe, time_start: str | None = None) -> pd.DataFrame:
        """
        if `time_start` is `None`:
            get the oldest data `DataProvider` can find
        else:
            get data from `time_start` onward
        """
        if time_start is not None:
            time_start = pd.Timestamp(time_start)
        raw = self._get_data_by_api(symbol, tf, time_start)
        df = self._normalize(raw)
        return self._validate(df)

    @abstractmethod
    def _get_data_by_api(self, s: ForexSymbol, tf: Timeframe, time_start: pd.Timestamp | None):
        pass

    @abstractmethod
    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    def _validate(self, df):
        if df.index.name != self.REQUIRED_INDEX_NAME \
            or not all(col in df.columns for col in self.REQUIRED_COLUMNS):
            raise ValueError("Schema mismatch")
        return df


class AlphaVantage(DataProvider):
    def __init__(self, api_key):
        super().__init__("alpha_vantage", api_key)

    def _get_data_by_api(self, s: ForexSymbol, tf: Timeframe, time_start: pd.Timestamp | None):
        time_end = pd.Timestamp.now()
        DIFF_DAYS_TO_DOWNLOAD_FULL = 90
        if time_end - time_start >= pd.Timedelta(days=DIFF_DAYS_TO_DOWNLOAD_FULL):
            outputsize = "full"
        else:
            outputsize = "compact"
        
        functions = {Timeframe.DAY: 'FX_DAILY', Timeframe.WEEK: 'FX_WEEKLY', Timeframe.MONTH: 'FX_MONTHLY'}
        if tf.unit not in functions:
            raise ValueError("AlphaVantage: unsupported Timeframe")
        function_ = functions[tf.unit]

        params = {
            "from_symbol": s.base,
            "to_symbol": s.quote,
            "outputsize": outputsize,
            "function": function_,
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
    
    def _get_data_by_api(self, s: ForexSymbol, tf: Timeframe, time_start: pd.Timestamp | None):
        client = RESTClient(self.api_key)

        time_end = pd.Timestamp.now()

        aggs = list(client.list_aggs(
            f"C:{s.base}{s.quote}",
            tf.length,
            tf.unit,
            time_start,
            time_end,
            adjusted="true",
            sort="asc"
        ))
        if not aggs:
            raise ValueError("Massive: data not downloaded")

        return aggs

    def _normalize(self, aggs):
        df = pd.DataFrame(aggs)
        if "volume" not in df.columns:
            df["volume"] = 0
        df.index = pd.to_datetime(df["timestamp"], unit="ms")
        df.index.name = "time"
        df.drop(["vwap", "timestamp", "transactions", "otc"], axis=1, inplace=True)
        return df


class TwelveData(DataProvider):
    def __init__(self, api_key):
        super().__init__("twelve_data", api_key)

    def _get_data_by_api(self, s: ForexSymbol, tf: Timeframe, time_start: pd.Timestamp | None):
        time_end = pd.Timestamp.now()

        # 1 <= outputsize <= 5000, default is 30
        outputsize = time_end - time_start

        params = {
            "symbol":f"{s.base}/{s.quote}",  # must have / in-between
            "interval":f"{tf.length}{tf.unit}",
            "outputsize": outputsize,  
            "format": "CSV",
            "apikey": self.api_key
        }
        res = requests.get("https://api.twelvedata.com/time_series", params, timeout=10)
        if not res.ok:
            raise ValueError("TwelveData: data not downloaded")

        return res

    def _normalize(self, res):
        df = pd.read_csv(StringIO(res.text), sep=";", index_col="time", parse_dates=True)
        if "volume" not in df.columns:
            df["volume"] = 0
        return df


###############################################################################

if __name__ == "__main__":
    # tf = Timeframe(5, 'minute')
    # print(tf)
    # symbol = ForexSymbol("EUR", "USD")
    # print(symbol)
    provider = AlphaVantage(None)
    # provider