import pandas as pd

FILE_EXTENSION = "parquet"

REQUIRED_INDEX_NAME = "time"
REQUIRED_COLUMNS = ["open", "high", "low", "close", "volume"]


def validate_data(df):
    """Check integrity and schema correctness"""

    if not isinstance(df, pd.DataFrame):
        raise ValueError("Invalid data type, not a pd.DataFrame")        

    if df.index.name != REQUIRED_INDEX_NAME \
        or not all(col in df.columns for col in REQUIRED_COLUMNS):
        raise ValueError("Schema mismatch")
    
    if df.index.tz is None:
        raise ValueError("df's index must be UTC")
    if not df.index.is_monotonic_increasing:
        raise ValueError("df's index must be sorted")
    if df.index.has_duplicates:
        raise ValueError("Duplicate timestamps detected in df")
