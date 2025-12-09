import pandas as pd

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
        errors.append(f"DataFrame has more than 10% null values ({null_ratio:.2%})")
    
    return (len(errors) == 0, errors)
