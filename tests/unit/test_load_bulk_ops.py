"""
Tests for load bulk operations.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
import pandas as pd
from io import StringIO

from src.load.bulk_ops import (
    copy_to_temp,
    WAGES_COLUMNS,
    WAGES_COLUMN_DEFS,
    EXPENSES_COLUMNS,
    EXPENSES_COLUMN_DEFS,
)


class TestCopyToTemp:
    """Tests for copy_to_temp function."""

    def test_copy_to_temp_success(self):
        """Test successful copy to temp table."""
        # Setup
        df = pd.DataFrame({
            "run_id": [1, 2],
            "county_fips": ["001", "002"],
            "adults": [1, 2],
            "working_adults": [1, 2],
            "children": [0, 1],
            "wage_type": ["living", "poverty"],
            "hourly_wage": [20.0, 15.0]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 2
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)

        # Test
        result = copy_to_temp(
            mock_conn,
            df,
            "tmp_wages",
            WAGES_COLUMNS,
            WAGES_COLUMN_DEFS
        )

        # Verify
        assert result == 2
        mock_cursor.execute.assert_called_once()
        create_table_call = mock_cursor.execute.call_args_list[0]
        assert "CREATE TEMP TABLE tmp_wages" in create_table_call[0][0]
        assert "ON COMMIT DROP" in create_table_call[0][0]

        mock_cursor.copy_expert.assert_called_once()
        copy_call = mock_cursor.copy_expert.call_args
        assert "COPY tmp_wages" in copy_call[0][0]
        assert isinstance(copy_call[0][1], StringIO)

    def test_copy_to_temp_empty_dataframe(self):
        """Test copy_to_temp with empty DataFrame."""
        df = pd.DataFrame()

        mock_conn = MagicMock()

        result = copy_to_temp(
            mock_conn,
            df,
            "tmp_wages",
            WAGES_COLUMNS,
            WAGES_COLUMN_DEFS
        )

        assert result == 0
        mock_conn.cursor.assert_not_called()

    def test_copy_to_temp_column_subset(self):
        """Test that only specified columns are copied."""
        df = pd.DataFrame({
            "run_id": [1],
            "county_fips": ["001"],
            "adults": [1],
            "working_adults": [1],
            "children": [0],
            "wage_type": ["living"],
            "hourly_wage": [20.0],
            "extra_col": ["should_not_be_copied"]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)

        copy_to_temp(
            mock_conn,
            df,
            "tmp_wages",
            WAGES_COLUMNS,
            WAGES_COLUMN_DEFS
        )

        # Verify copy_expert was called with correct columns
        copy_call = mock_cursor.copy_expert.call_args
        assert "COPY tmp_wages" in copy_call[0][0]
        # Check that the buffer doesn't contain extra_col
        buffer = copy_call[0][1]
        buffer_content = buffer.getvalue()
        assert "extra_col" not in buffer_content
        assert "001" in buffer_content  # Verify data is there

    def test_copy_to_temp_expenses(self):
        """Test copy_to_temp with expenses columns."""
        df = pd.DataFrame({
            "run_id": [1],
            "county_fips": ["001"],
            "adults": [1],
            "working_adults": [1],
            "children": [0],
            "expense_category": ["food"],
            "annual_amount": [5000.0]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)

        result = copy_to_temp(
            mock_conn,
            df,
            "tmp_expenses",
            EXPENSES_COLUMNS,
            EXPENSES_COLUMN_DEFS
        )

        assert result == 1
        create_table_call = mock_cursor.execute.call_args_list[0]
        assert "CREATE TEMP TABLE tmp_expenses" in create_table_call[0][0]


class TestColumnDefinitions:
    """Tests for column definition constants."""

    def test_wages_columns(self):
        """Test WAGES_COLUMNS constant."""
        expected = ["run_id", "county_fips", "adults", "working_adults", 
                   "children", "wage_type", "hourly_wage"]
        assert WAGES_COLUMNS == expected

    def test_wages_column_defs(self):
        """Test WAGES_COLUMN_DEFS constant."""
        assert "run_id INTEGER" in WAGES_COLUMN_DEFS
        assert "county_fips VARCHAR(3)" in WAGES_COLUMN_DEFS
        assert "hourly_wage NUMERIC(10,2)" in WAGES_COLUMN_DEFS

    def test_expenses_columns(self):
        """Test EXPENSES_COLUMNS constant."""
        expected = ["run_id", "county_fips", "adults", "working_adults",
                   "children", "expense_category", "annual_amount"]
        assert EXPENSES_COLUMNS == expected

    def test_expenses_column_defs(self):
        """Test EXPENSES_COLUMN_DEFS constant."""
        assert "run_id INTEGER" in EXPENSES_COLUMN_DEFS
        assert "county_fips VARCHAR(3)" in EXPENSES_COLUMN_DEFS
        assert "expense_category VARCHAR(50)" in EXPENSES_COLUMN_DEFS
        assert "annual_amount NUMERIC(10,2)" in EXPENSES_COLUMN_DEFS

