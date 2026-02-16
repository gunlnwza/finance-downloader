import pandas as pd
import requests
from io import StringIO
from polygon import RESTClient


"""
symbol: tuple of two strings
(EUR, USD)
(XAU, USD)

time_frame: str (TradingView convention)
- 1, 5, 15, 30, 60 (minutes)
- 1h, 4h (hours)
- 1d (day)
- 1w (week)
- 1m (month)

time_range: tuple of two datetimes
(2026-02-15, 2026-02-16)
"""

class DataProvider:
    def __init__(self, api_key):
        self.api_key = api_key

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError

    def get(self, symbol: tuple, time_frame: str, time_range: tuple) -> pd.DataFrame:
        raise NotImplementedError


class AlphaVantage(DataProvider):
    def __init__(self, api_key):
        super().__init__(api_key)

    def _normalize(self, res) -> pd.DataFrame:
        df = pd.read_csv(StringIO(res.text), index_col="timestamp", parse_dates=True)
        df = df.sort_index()
        if "volume" not in df.columns:
            df["volume"] = 0
        return df

    def get(self, symbol, time_frame, time_range):
        base, quote = symbol
        function_ = "FX_DAILY"  # TODO: convert from time_frame
        outputsize = "compact"  # TODO: convert from time_range

        params = {
            "apikey": self.api_key,
            "from_symbol": base,
            "to_symbol": quote,
            "function": function_,
            "datatype": "csv",
            "outputsize": outputsize
        }
        res = requests.get("https://www.alphavantage.co/query", params, timeout=10)
        if not res.ok:
            raise ValueError("AlphaVantage: data not downloaded")

        content_type = res.headers.get("Content-Type", "")
        if content_type and "json" in content_type.lower():
            raise ValueError(f"AlphaVantage: {res.json()}")

        return self._normalize(res)


class Massive(DataProvider):
    def __init__(self, api_key):
        super().__init__(api_key)

    def _normalize(self, aggs):
        df = pd.DataFrame(aggs)
        df.index = pd.to_datetime(df["timestamp"], unit="ms")
        df.drop(["vwap", "timestamp", "transactions", "otc"], axis=1, inplace=True)
        return df

    def get(self, symbol, time_frame, time_range):
        tf_amount, tf_unit = 1, "day"  # TODO: convert from time_frame
        base, quote = symbol
        start, end = time_range
        client = RESTClient(self.api_key)

        aggs = list(client.list_aggs(
            f"C:{base}{quote}", tf_amount, tf_unit, start, end, adjusted="true", sort="asc"
        ))
        if not aggs:
            raise ValueError("Massive: data not downloaded")
        return self._normalize(aggs)


class TwelveData(DataProvider):
    def __init__(self, api_key):
        super().__init__(api_key)

    def _normalize(self, res):
        df = pd.read_csv(StringIO(res.text), sep=";", index_col="datetime", parse_dates=True)
        if "volume" not in df.columns:
            df["volume"] = 0
        return df

    def get(self, symbol, time_frame, time_range):
        base, quote = symbol

        params = {
            "symbol":f"{base}/{quote}",  # must have / !
            "interval":"1min",
            "outputsize": 30,  # default is 30, support [1, 5000], TODO: convert time_range & time_frame
            "format": "CSV",
            "apikey":self.api_key
        }
        res = requests.get("https://api.twelvedata.com/time_series", params, timeout=10)
        if not res.ok:
            raise ValueError("TwelveData: data not downloaded")

        return self._normalize(res)
