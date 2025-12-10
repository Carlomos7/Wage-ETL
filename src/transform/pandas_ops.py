"""
Pandas operations.
"""

from datetime import date
import pandas as pd
from config.logging import get_logger
from src.transform.normalizers import (
    normalize_header_for_lookup,
    get_family_config_metadata,
    lookup_category_value,
    normalize_category_key,
)
from pydantic import BaseModel, ValidationError
from typing import Type
from src.transform.models import WageRecord, ExpenseRecord

logger = get_logger(module=__name__)


def table_to_dataframe(data: list[dict]) -> pd.DataFrame:
    """
    Convert a list of row dicts to a pandas DataFrame.

    Ensures 'county_fips' is always a zero-padded string of length 3.
    Note: This function works with raw extracted data. The full FIPS (5 digits)
    is created later in normalize_wages/normalize_expenses.
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

    Strips $ and commas, converts to float.
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
    normalized = df[source_col].fillna("").astype(
        str).apply(normalize_header_for_lookup)

    # Convert each normalized header into metadata dicts
    metadata = normalized.apply(get_family_config_metadata)

    # Create output columns (fallback to none)
    df["adults"] = metadata.apply(lambda m: m["adults"] if m else None)
    df["working_adults"] = metadata.apply(
        lambda m: m["working_adults"] if m else None)
    df["children"] = metadata.apply(lambda m: m["children"] if m else None)

    return df


def normalize_category_column(df: pd.DataFrame, source_col: str, target_col: str) -> pd.DataFrame:
    """
    Map category names to normalized values using CATEGORY_MAP.
    """
    df = df.copy()
    source = df[source_col]

    df[target_col] = (
        source
        .apply(normalize_category_key)  # normalize key
        .apply(lambda key: lookup_category_value(key) or key)  # map or fallback
    )

    return df


def dataframe_to_models(df: pd.DataFrame, model_class: Type[BaseModel]) -> tuple[list[BaseModel], list[dict]]:
    '''
    Iterate over dataframe rows and convert to Pydantic models.
    '''
    models = []
    errors = []
    for index, row in df.iterrows():
        try:
            models.append(model_class(**row.to_dict()))
        except ValidationError as e:
            errors.append({"row_index": index, "errors": e.errors()})
        except Exception as e:
            errors.append({"row_index": index, "errors": [{"msg": str(e)}]})
    return models, errors


def _melt_family_configs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Melt wide family configuration columns into long format.
    """
    value_vars = [c for c in df.columns if c.lower() not in [
        "category", "county_fips"]]
    return df.melt(
        id_vars=["category"], value_vars=value_vars, var_name="family", value_name="value"
    )


def normalize_wages(df: pd.DataFrame, state_fips: str, county_fips: str, page_updated_at: date, validate: bool = True) -> pd.DataFrame:
    """
    Normalize wages data. Melts wide to long format, then orchestrates cleaning functions.
    
    Args:
        df: Input DataFrame with wide format data
        state_fips: State FIPS code (2 digits)
        county_fips: County FIPS code (3 digits)
        page_updated_at: Date when the page was last updated
        validate: Whether to validate using Pydantic models
    """
    if df.empty:
        return df

    state_fips = str(state_fips).zfill(2)
    county_fips = str(county_fips).zfill(3)
    full_fips = state_fips + county_fips
    
    df.columns = [c.lower() if c in ['Category', 'county_fips']
                  else c for c in df.columns]

    long_df = _melt_family_configs(df)
    long_df = add_family_config_columns(long_df, "family")
    long_df = normalize_category_column(long_df, "category", "wage_type")
    long_df = clean_currency_columns(long_df, ["value"])
    long_df = long_df.rename(columns={"value": "hourly_wage"})
    long_df["county_fips"] = full_fips
    long_df["page_updated_at"] = page_updated_at

    if validate:
        models, errors = dataframe_to_models(long_df, WageRecord)
        if errors:
            logger.warning(f"Wage normalization validation errors: {errors}")
        else:
            long_df = pd.DataFrame([m.model_dump() for m in models])

    return long_df.reset_index(drop=True)


def normalize_expenses(df: pd.DataFrame, state_fips: str, county_fips: str, page_updated_at: date, validate: bool = True) -> pd.DataFrame:
    """
    Normalize expenses data. Melts wide to long format, then orchestrates cleaning functions.
    
    Args:
        df: Input DataFrame with wide format data
        state_fips: State FIPS code (2 digits)
        county_fips: County FIPS code (3 digits)
        page_updated_at: Date when the page was last updated
        validate: Whether to validate using Pydantic models
    """
    if df.empty:
        return df

    state_fips = str(state_fips).zfill(2)
    county_fips = str(county_fips).zfill(3)
    full_fips = state_fips + county_fips
    
    df.columns = [c.lower() if c in ['Category', 'county_fips']
                  else c for c in df.columns]

    long_df = _melt_family_configs(df)
    long_df = add_family_config_columns(long_df, "family")
    long_df = normalize_category_column(
        long_df, "category", "expense_category")
    long_df = clean_currency_columns(long_df, ["value"])
    long_df = long_df.rename(columns={"value": "annual_amount"})
    long_df["county_fips"] = full_fips
    long_df["page_updated_at"] = page_updated_at

    if validate:
        models, errors = dataframe_to_models(long_df, ExpenseRecord)
        if errors:
            logger.warning(
                f"Expense normalization validation errors: {errors}")
        else:
            long_df = pd.DataFrame([m.model_dump() for m in models])

    return long_df.reset_index(drop=True)
