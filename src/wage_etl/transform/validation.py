import pandas as pd
from src.transform.models import WageRecord, ExpenseRecord
from src.transform.pandas_ops import dataframe_to_models

NON_FAMILY_COLS = {"category", "county_fips"}


def validate_wide_format_input(df: pd.DataFrame) -> tuple[bool, list[str]]:
    '''
    Check scraped data before transformation.
    '''
    errors = []

    # Empty check
    if df.empty:
        errors.append("DataFrame is empty")
        return False, errors

    # Normalize column names
    col_map = {c.lower(): c for c in df.columns}
    cols_lower = set(col_map.keys())

    # Required column checks
    if "category" not in cols_lower:
        errors.append("'category' column not found")

    # Family configuration columns
    family_cols = [
        col_map[c] for c in cols_lower
        if c not in NON_FAMILY_COLS
    ]

    # Missing Value Validation
    total_cells = df.shape[0] * df.shape[1]
    null_ratio = df.isna().sum().sum() / total_cells

    if null_ratio > 0.10:
        errors.append(
            f"DataFrame has more than 10% null values ({null_ratio:.2%})")

    return (len(errors) == 0, errors)


def _validate_common(df: pd.DataFrame, county_fips: str) -> list[dict]:
    '''
    Validate common fields across all data types.
    
    Args:
        df: DataFrame to validate
        county_fips: Full FIPS code (5 digits: state + county)
    '''

    # County FIPS validation
    errors = []
    if "county_fips" not in df.columns:
        return [{"field": "county_fips", "msg": "county_fips column missing"}]

    # Normalize county FIPS (expect 5 digits: state + county)
    expected_fips = str(county_fips).zfill(5)
    df_fips = df["county_fips"].astype(str).str.zfill(5)

    # Validate county FIPS values
    if not df_fips.eq(expected_fips).all():
        errors.append(
            {"field": "county_fips", "msg": "county_fips values are inconsistent"})

    return errors


def validate_wages(df: pd.DataFrame, county_fips: str) -> tuple[bool, list[dict]]:
    '''
    Validate wages data.
    Args:
        df: Pandas DataFrame containing wages data
        county_fips: Full FIPS code (5 digits: state + county)

    Returns:
        Tuple containing:
        - Boolean indicating if validation passed
        - List of validation errors
    '''
    errors = _validate_common(df, county_fips)
    if errors:
        return False, errors

    _, model_errors = dataframe_to_models(df, WageRecord)
    errors.extend(model_errors)
    return (len(errors) == 0, errors)


def validate_expenses(df: pd.DataFrame, county_fips: str) -> tuple[bool, list[dict]]:
    '''
    Validate expenses data.
    Args:
        df: Pandas DataFrame containing expenses data
        county_fips: Full FIPS code (5 digits: state + county)

    Returns:
        Tuple containing:
        - Boolean indicating if validation passed
        - List of validation errors
    '''
    errors = _validate_common(df, county_fips)
    if errors:
        return False, errors
    _, model_errors = dataframe_to_models(df, ExpenseRecord)
    errors.extend(model_errors)
    return (len(errors) == 0, errors)
