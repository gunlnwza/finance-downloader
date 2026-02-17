
class ForexSymbol:
    def __init__(self, base: str, quote: str):
        self.base = base.upper()
        self.quote = quote.upper()
    
        self._validate()  # assert that base and quote are correct

    def __repr__(self):
        return f"ForexSymbol({self.base}, {self.quote})"

    def _validate(self):
        if not (len(self.base) == 3 and len(self.quote) == 3):
            raise ValueError(f"Invalid ForexSymbol: {self}")

        # TODO: match against dict of currencies


class Timeframe:
    """
    Supported timeframes:
    - 1m, 5m, 15m, 30m
    - 1h, 4h
    - 1d
    - 1w
    - 1M
    """
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"

    def __init__(self, length: int, unit: str | None = None):
        self.length = length
        self.unit = unit

        self._validate()  # assert that length and unit are correct
    
    def __repr__(self):
        return f"Timeframe({self.length}, {self.unit})"

    def _validate(self):
        if not (
            (self.unit == Timeframe.MINUTE and self.length in (1, 5, 15, 30))
            or (self.unit == Timeframe.HOUR and self.length in (1, 4))
            or (self.unit == Timeframe.DAY and self.length in (1,))
            or (self.unit == Timeframe.WEEK and self.length in (1,))
            or (self.unit == Timeframe.MONTH and self.length in (1,))
        ):
            raise ValueError(f"Timeframe not supported: '{self}'")
        
        if not isinstance(self.length, int):
            raise ValueError(f"Invalid Timeframe length: '{self}'")
