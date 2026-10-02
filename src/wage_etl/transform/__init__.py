'''
Transform package for data transformation operations.
'''
from wage_etl.transform.models import (
    WageRecord,
    ExpenseRecord,
)

from wage_etl.transform.constants import (
    FAMILY_CONFIG_MAP,
    CATEGORY_MAP,
)

from wage_etl.transform.normalizers import (
    normalize_header_for_lookup,
    get_family_config_metadata,
)

from wage_etl.transform.pandas_ops import (
    clean_currency_columns,
    add_family_config_columns,
    normalize_category_column,
    table_to_dataframe,
    dataframe_to_models,
    normalize_wages,
    normalize_expenses,
)

from wage_etl.transform.validation import (
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
    'CATEGORY_MAP',
    # Normalizers
    'normalize_header_for_lookup',
    'get_family_config_metadata',
    # DataFrame utilities
    'clean_currency_columns',
    'add_family_config_columns',
    'normalize_category_column',
    'table_to_dataframe',
    'dataframe_to_models',
    # Transformations
    'normalize_wages',
    'normalize_expenses',
    # Validation
    'validate_wide_format_input',
    'validate_wages',
    'validate_expenses',
]
