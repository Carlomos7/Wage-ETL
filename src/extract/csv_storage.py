'''
CSV storage utilities.
'''
from pathlib import Path
from typing import Dict, Set, Optional
import pandas as pd
from config.logging import get_logger
from src.extract.logging_utils import format_log_with_metadata

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
