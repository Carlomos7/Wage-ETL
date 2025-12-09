'''
Transform package for data transformation operations.
'''
from src.transform.models import (
    WageRecord,
    ExpenseRecord,
)

from src.transform.constants import (
    FAMILY_CONFIG_MAP,
    normalize_header_for_lookup,
    get_family_config_metadata,
)

from src.transform.pandas_ops import (
    clean_currency_columns,
    add_family_config_columns,
    normalize_category_column,
    table_to_dataframe,
    dataframe_to_models,
    normalize_wages,
    normalize_expenses,
)

from src.transform.validation import (
    validate_wide_format_input,
    validate_wages,
    validate_expenses,
)

__all__ = [
    # Models
    'WageRecord',
    'ExpenseRecord',
    # Constants
    'FAMILY_CONFIG_MAP',
    'normalize_header_for_lookup',
    'get_family_config_metadata',
    # DataFrame utilities
    'clean_currency_columns',
    'add_family_config_columns',
    'normalize_category_column',
    'table_to_dataframe',
    # DataFrame to Pydantic models
    'dataframe_to_models',
    # Transformations
    'normalize_wages',
    'normalize_expenses',
    # Validation
    'validate_wide_format_input',
    'validate_wages',
    'validate_expenses',
]
