"""
Pandas operations.
"""

import pandas as pd
from config.logging import get_logger

logger = get_logger(module=__name__)


def table_to_dataframe(data: list[dict]) -> pd.DataFrame:
    """
    Convert a list of row dicts to a pandas DataFrame.

    Ensures 'county_fips' is always a zero-padded string of length 3.
    """
    logger.debug("Converting list of row dicts to pandas DataFrame")

    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    # Ensure 'county_fips' exists and is properly formatted
    if "county_fips" in df.columns:
        df["county_fips"] = df["county_fips"].astype(str).str.zfill(3)
    else:
        logger.warning("'county_fips' column not found in data")

    return df


def clean_currency_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    '''
    Clean multiple currency columns in a DataFrame at once.

    Stips $ and commas, converts to float.
    Args:
        df: DataFrame to clean
        columns: List of columns to clean
    Returns:
        DataFrame with cleaned currency columns
    '''
    df = df.copy()
    for col in columns:
        # Strip common formatting characters
        cleaned = (
            df[col]
            .astype(str)
            .str.replace(r"[\$,]", "", regex=True)
            .str.strip()
        )
        # pandas built-in numeric conversion
        df[col] = pd.to_numeric(cleaned, errors="coerce").fillna(0)
    return df
