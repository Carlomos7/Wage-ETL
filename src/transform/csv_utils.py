'''
CSV storage utilities.
'''
from pathlib import Path
from typing import Dict, Set, Optional
import pandas as pd
from config.logging import get_logger, format_log_with_metadata
from config.settings import get_settings
from src.transform.pandas_ops import table_to_dataframe

logger = get_logger(module=__name__)

class CSVIndexCache:
    '''
    Lightweight cache that tracks which counties exist in CSV files
    to avoid repeated full file reads.
    '''
    
    def __init__(self):
        self._indices: Dict[Path, Set[str]] = {}
    
    def has_county(self, filename: Path, county_fips: str) -> bool:
        '''
        Check if a county exists in the CSV file using cached index.
        '''
        county_fips_str = str(county_fips).zfill(3)
        
        # Load index if not cached
        if filename not in self._indices:
            self._load_index(filename)
        
        return county_fips_str in self._indices.get(filename, set())
    
    def _load_index(self, filename: Path) -> None:
        '''
        Load the set of county FIPS codes from a CSV file.
        Only reads the county_fips column for efficiency.
        '''
        if not filename.exists():
            self._indices[filename] = set()
            return
        
        try:
            # Try to read only the county_fips column for efficiency
            try:
                df = pd.read_csv(filename, dtype={'county_fips': str}, usecols=['county_fips'])
            except (KeyError, ValueError):
                # Column doesn't exist, read full file to check
                df = pd.read_csv(filename, dtype={'county_fips': str})
            
            if 'county_fips' in df.columns:
                df['county_fips'] = df['county_fips'].str.zfill(3)
                self._indices[filename] = set(df['county_fips'].unique())
            else:
                self._indices[filename] = set()
        except Exception as e:
            logger.warning(f"Failed to load index for {filename}: {e}")
            self._indices[filename] = set()
    
    def update_index(self, filename: Path, county_fips: str, added: bool = True) -> None:
        '''
        Update the index after writing to a file.
        '''
        county_fips_str = str(county_fips).zfill(3)
        
        # Ensure index exists
        if filename not in self._indices:
            self._load_index(filename)
        
        if added:
            self._indices[filename].add(county_fips_str)
        else:
            self._indices[filename].discard(county_fips_str)

def upsert_to_csv(df: pd.DataFrame, filename: Path, county_fips: str, index_cache: Optional[CSVIndexCache] = None, year: Optional[int] = None, state_fips: Optional[str] = None) -> None:
    '''
    Upsert the dataframe to the csv file.

    Args:
        df: DataFrame to upsert
        filename: Path to CSV file
        county_fips: County FIPS code
        index_cache: Optional cache to update after writing
        year: Optional year for logging metadata
        state_fips: Optional state FIPS for logging metadata
    '''
    county_fips = str(county_fips).zfill(3)

    if year is not None and state_fips is not None:
        logger.debug(format_log_with_metadata(
            f'Upserting dataframe to {filename.name}',
            year, state_fips, county_fips))
    else:
        logger.debug(f'Upserting dataframe to {filename}')

    if filename.exists():
        df_master = pd.read_csv(filename, dtype={'county_fips': str})
        df_master['county_fips'] = df_master['county_fips'].str.zfill(3)
        # Remove old rows for the county
        if 'county_fips' in df_master.columns:
            df_master = df_master[df_master['county_fips'] != county_fips]
    else:
        df_master = pd.DataFrame()

    # Append new data
    df_master = pd.concat([df_master, df], ignore_index=True, sort=False)
    df_master = df_master[df.columns]
    df_master.to_csv(filename, index=False)

    # Update cache if provided
    if index_cache:
        index_cache.update_index(filename, county_fips, added=True)


def get_output_paths(state_fips: str, year: int) -> tuple[Path, Path]:
    '''
    Get the output file paths for wages and expenses data.
    
    Args:
        state_fips: State FIPS code
        year: Year for the data
        
    Returns:
        Tuple of (wages_filename, expenses_filename)
    '''
    settings = get_settings()
    output_path = settings.raw_dir / str(year)
    output_path.mkdir(parents=True, exist_ok=True)
    
    wages_filename = output_path / Path(f'wage_rates_{state_fips}.csv')
    expenses_filename = output_path / Path(f'expense_breakdown_{state_fips}.csv')
    
    return wages_filename, expenses_filename


def transform_and_save(
    wages_data: list[dict],
    expenses_data: list[dict],
    state_fips: str,
    county_fips: str,
    index_cache: Optional[CSVIndexCache] = None,
    year: Optional[int] = None
) -> bool:
    '''
    Transform and save scraped data to CSV files.
    
    This function handles the full transformation pipeline:
    1. Get output file paths
    2. Check if data already exists (skip if it does)
    3. Convert lists of dicts to DataFrames
    4. Upsert to CSV files
    
    Args:
        wages_data: List of row dicts for wages data
        expenses_data: List of row dicts for expenses data
        state_fips: State FIPS code
        county_fips: County FIPS code
        index_cache: Optional cache to check existence and update after writing
        year: Optional year (defaults to current year)
        
    Returns:
        True if data was saved, False if it already existed
    '''
    if year is None:
        from datetime import datetime
        year = datetime.now().year
    
    # Get file paths
    wages_filename, expenses_filename = get_output_paths(state_fips, year)
    
    # Check if county data already exists in both files using cache
    if index_cache:
        wages_has_county = index_cache.has_county(wages_filename, county_fips)
        expenses_has_county = index_cache.has_county(expenses_filename, county_fips)
        
        if wages_has_county and expenses_has_county:
            logger.debug(format_log_with_metadata(
                f"County {state_fips + county_fips} already exists in both files. Skipping.",
                year, state_fips, county_fips))
            return False
    
    # Convert lists of dicts to DataFrames
    wages_df = table_to_dataframe(wages_data)
    expenses_df = table_to_dataframe(expenses_data)
    
    # Upsert to CSV files
    upsert_to_csv(wages_df, wages_filename, county_fips, index_cache, year, state_fips)
    upsert_to_csv(expenses_df, expenses_filename, county_fips, index_cache, year, state_fips)
    
    return True
