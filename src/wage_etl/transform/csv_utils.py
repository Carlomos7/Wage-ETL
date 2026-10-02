'''
CSV storage utilities for saving transformed data.
'''
from pathlib import Path

import pandas as pd

from wage_etl.config.logging import get_logger

logger = get_logger(module=__name__)


def save_dataframe_to_csv(
    df: pd.DataFrame,
    filepath: Path,
    create_parents: bool = True
) -> None:
    '''
    Save a DataFrame to a CSV file.

    Args:
        df: DataFrame to save
        filepath: Path to CSV file
        create_parents: Whether to create parent directories if they don't exist
    '''
    if create_parents:
        filepath.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(filepath, index=False)
    logger.debug(f'Saved {len(df)} records to {filepath}')


def get_output_paths(output_dir: Path, state_fips: str, year: int) -> tuple[Path, Path]:
    '''
    Get the output file paths for wages and expenses data.

    Args:
        output_dir: Directory that holds the year folders
        state_fips: State FIPS code
        year: Year for the data

    Returns:
        Tuple of (wages_path, expenses_path)
    '''
    year_dir = output_dir / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)

    wages_path = year_dir / f'wages_{state_fips}.csv'
    expenses_path = year_dir / f'expenses_{state_fips}.csv'

    return wages_path, expenses_path
