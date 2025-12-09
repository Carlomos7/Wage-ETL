"""
Pandas operations.
"""

import pandas as pd
from config.logging import get_logger
from src.transform.constants import (
    FAMILY_CONFIG_MAP,
    normalize_header_for_lookup,
    get_family_config_metadata,
)

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

def add_family_config_columns(df: pd.DataFrame, source_col: str) -> pd.DataFrame:
    '''
    Parse family configurations from a source column and create new columns for adults, working adults, and children.
    '''
    df = df.copy()
    
    # Normalize the raw columns
    normalized = df[source_col].fillna("").astype(str).apply(normalize_header_for_lookup)

    # Convert each normalized header into metadata dicts
    metadata = normalized.apply(get_family_config_metadata)

    # Create output columns (fallback to none)
    df["adults"] = metadata.apply(lambda m: m["adults"] if m else None)
    df["working_adults"] = metadata.apply(lambda m: m["working_adults"] if m else None)
    df["children"] = metadata.apply(lambda m: m["children"] if m else None)

    return df