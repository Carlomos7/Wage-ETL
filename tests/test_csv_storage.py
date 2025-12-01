import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, Mock

from src.extract.csv_storage import CSVIndexCache, upsert_to_csv


class TestCSVIndexCache:
    """Tests for the CSVIndexCache class."""

    @pytest.fixture
    def temp_csv(self, tmp_path):
        """Create a temporary CSV file with test data."""
        csv_path = tmp_path / "test.csv"
        df = pd.DataFrame({
            'county_fips': ['001', '002', '003'],
            'data': ['a', 'b', 'c']
        })
        df.to_csv(csv_path, index=False)
        return csv_path

    def test_has_county_returns_true_when_exists(self, temp_csv):
        """Should return True when county exists in CSV."""
        cache = CSVIndexCache()
        assert cache.has_county(temp_csv, '001') is True
        assert cache.has_county(temp_csv, '002') is True
        assert cache.has_county(temp_csv, '003') is True

    def test_has_county_returns_false_when_not_exists(self, temp_csv):
        """Should return False when county doesn't exist in CSV."""
        cache = CSVIndexCache()
        assert cache.has_county(temp_csv, '999') is False

    def test_has_county_with_nonexistent_file(self, tmp_path):
        """Should return False for non-existent file."""
        cache = CSVIndexCache()
        nonexistent = tmp_path / "nonexistent.csv"
        assert cache.has_county(nonexistent, '001') is False

    def test_has_county_zero_pads_county_fips(self, temp_csv):
        """Should zero-pad county FIPS codes."""
        cache = CSVIndexCache()
        # CSV has '001', should match '1' or '01'
        assert cache.has_county(temp_csv, '1') is True
        assert cache.has_county(temp_csv, '01') is True
        assert cache.has_county(temp_csv, '001') is True

    def test_update_index_adds_county(self, temp_csv):
        """Should add county to index after update."""
        cache = CSVIndexCache()
        # Initially doesn't exist
        assert cache.has_county(temp_csv, '999') is False
        
        # Update index
        cache.update_index(temp_csv, '999', added=True)
        
        # Now should exist
        assert cache.has_county(temp_csv, '999') is True

    def test_update_index_removes_county(self, temp_csv):
        """Should remove county from index after update."""
        cache = CSVIndexCache()
        # Initially exists
        assert cache.has_county(temp_csv, '001') is True
        
        # Update index to remove
        cache.update_index(temp_csv, '001', added=False)
        
        # Now should not exist in cache (but still in file)
        # Note: This tests cache update, not file modification
        assert cache.has_county(temp_csv, '001') is False

    def test_load_index_handles_missing_column(self, tmp_path):
        """Should handle CSV files without county_fips column."""
        csv_path = tmp_path / "no_county.csv"
        df = pd.DataFrame({'other_col': ['a', 'b']})
        df.to_csv(csv_path, index=False)
        
        cache = CSVIndexCache()
        # Should not raise error
        assert cache.has_county(csv_path, '001') is False


class TestUpsertToCSV:
    """Tests for the upsert_to_csv function."""

    @pytest.fixture
    def temp_csv(self, tmp_path):
        """Create a temporary CSV file."""
        return tmp_path / "test.csv"

    def test_upsert_creates_new_file(self, temp_csv):
        """Should create new file if it doesn't exist."""
        df = pd.DataFrame({
            'county_fips': ['001'],
            'value': [100]
        })
        
        upsert_to_csv(df, temp_csv, '001')
        
        assert temp_csv.exists()
        result_df = pd.read_csv(temp_csv, dtype={'county_fips': str})
        assert len(result_df) == 1
        assert result_df.iloc[0]['county_fips'] == '001'

    def test_upsert_replaces_existing_county(self, temp_csv):
        """Should replace existing county data."""
        # Create initial file
        initial_df = pd.DataFrame({
            'county_fips': ['001', '002'],
            'value': [100, 200]
        })
        initial_df.to_csv(temp_csv, index=False)
        
        # Upsert with new data for county 001
        new_df = pd.DataFrame({
            'county_fips': ['001'],
            'value': [150]
        })
        
        upsert_to_csv(new_df, temp_csv, '001')
        
        result_df = pd.read_csv(temp_csv, dtype={'county_fips': str})
        assert len(result_df) == 2
        # County 001 should be updated
        county_001 = result_df[result_df['county_fips'] == '001']
        assert len(county_001) == 1
        assert county_001.iloc[0]['value'] == 150
        # County 002 should remain
        county_002 = result_df[result_df['county_fips'] == '002']
        assert len(county_002) == 1
        assert county_002.iloc[0]['value'] == 200

    def test_upsert_updates_cache(self, temp_csv):
        """Should update index cache when provided."""
        cache = CSVIndexCache()
        df = pd.DataFrame({
            'county_fips': ['001'],
            'value': [100]
        })
        
        upsert_to_csv(df, temp_csv, '001', index_cache=cache)
        
        # Cache should now know about county 001
        assert cache.has_county(temp_csv, '001') is True

    def test_upsert_with_logging_metadata(self, temp_csv):
        """Should use logging metadata when provided."""
        df = pd.DataFrame({
            'county_fips': ['001'],
            'value': [100]
        })
        
        # Should not raise error with metadata
        upsert_to_csv(df, temp_csv, '001', year=2025, state_fips='34')

