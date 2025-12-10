"""
Tests for load staging operations.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
import pandas as pd
from io import StringIO

from src.load.staging import (
    bulk_upsert_wages,
    bulk_upsert_expenses,
    load_rejects,
    get_staging_counts,
    truncate_staging,
    ALLOWED_REJECT_TABLES,
)


class TestBulkUpsertWages:
    """Tests for bulk_upsert_wages function."""

    @patch('src.load.staging.get_connection')
    @patch('src.load.staging.copy_to_temp')
    def test_bulk_upsert_wages_success(self, mock_copy_to_temp, mock_get_connection):
        """Test successful bulk upsert of wages."""
        # Setup
        df = pd.DataFrame({
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
        mock_get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=False)
        mock_copy_to_temp.return_value = 2

        # Test
        result = bulk_upsert_wages(df, run_id=123)

        # Verify
        assert result == 2
        mock_copy_to_temp.assert_called_once()
        # Verify run_id was added
        call_df = mock_copy_to_temp.call_args[0][1]
        assert "run_id" in call_df.columns
        assert (call_df["run_id"] == 123).all()

        # Verify INSERT statement was executed
        mock_cursor.execute.assert_called_once()
        insert_call = mock_cursor.execute.call_args[0][0]
        assert "INSERT INTO stg_wages" in insert_call
        assert "ON CONFLICT" in insert_call

    def test_bulk_upsert_wages_empty_dataframe(self):
        """Test bulk_upsert_wages with empty DataFrame."""
        df = pd.DataFrame()
        result = bulk_upsert_wages(df, run_id=123)
        assert result == 0

    @patch('src.load.staging.get_connection')
    def test_bulk_upsert_wages_missing_columns(self, mock_get_connection):
        """Test bulk_upsert_wages with missing required columns."""
        df = pd.DataFrame({
            "county_fips": ["001"],
            "adults": [1],
            # Missing other required columns
        })

        with pytest.raises(ValueError, match="Missing required columns"):
            bulk_upsert_wages(df, run_id=123)

    @patch('src.load.staging.get_connection')
    @patch('src.load.staging.copy_to_temp')
    def test_bulk_upsert_wages_column_order(self, mock_copy_to_temp, mock_get_connection):
        """Test that columns are reordered correctly."""
        df = pd.DataFrame({
            "hourly_wage": [20.0],
            "wage_type": ["living"],
            "county_fips": ["001"],
            "adults": [1],
            "working_adults": [1],
            "children": [0],
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=False)

        bulk_upsert_wages(df, run_id=123)

        # Verify columns are in correct order
        call_df = mock_copy_to_temp.call_args[0][1]
        expected_order = ["run_id", "county_fips", "adults", "working_adults",
                         "children", "wage_type", "hourly_wage"]
        assert list(call_df.columns) == expected_order


class TestBulkUpsertExpenses:
    """Tests for bulk_upsert_expenses function."""

    @patch('src.load.staging.get_connection')
    @patch('src.load.staging.copy_to_temp')
    def test_bulk_upsert_expenses_success(self, mock_copy_to_temp, mock_get_connection):
        """Test successful bulk upsert of expenses."""
        df = pd.DataFrame({
            "county_fips": ["001", "002"],
            "adults": [1, 2],
            "working_adults": [1, 2],
            "children": [0, 1],
            "expense_category": ["food", "housing"],
            "annual_amount": [5000.0, 12000.0]
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 2
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=False)
        mock_copy_to_temp.return_value = 2

        result = bulk_upsert_expenses(df, run_id=123)

        assert result == 2
        mock_copy_to_temp.assert_called_once()
        call_df = mock_copy_to_temp.call_args[0][1]
        assert "run_id" in call_df.columns

        mock_cursor.execute.assert_called_once()
        insert_call = mock_cursor.execute.call_args[0][0]
        assert "INSERT INTO stg_expenses" in insert_call

    def test_bulk_upsert_expenses_empty_dataframe(self):
        """Test bulk_upsert_expenses with empty DataFrame."""
        df = pd.DataFrame()
        result = bulk_upsert_expenses(df, run_id=123)
        assert result == 0

    @patch('src.load.staging.get_connection')
    def test_bulk_upsert_expenses_missing_columns(self, mock_get_connection):
        """Test bulk_upsert_expenses with missing required columns."""
        df = pd.DataFrame({
            "county_fips": ["001"],
            # Missing other required columns
        })

        with pytest.raises(ValueError, match="Missing required columns"):
            bulk_upsert_expenses(df, run_id=123)


class TestLoadRejects:
    """Tests for load_rejects function."""

    @patch('src.load.staging.get_connection')
    def test_load_rejects_success(self, mock_get_connection):
        """Test successful loading of rejects."""
        records = [
            {"raw_data": {"county": "001"}, "rejection_reason": "Invalid format"},
            {"raw_data": {"county": "002"}, "rejection_reason": "Missing field"}
        ]

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 2
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=False)

        result = load_rejects(records, run_id=123, table="stg_wages_rejects")

        assert result == 2
        mock_cursor.copy_expert.assert_called_once()
        copy_call = mock_cursor.copy_expert.call_args
        assert "COPY stg_wages_rejects" in copy_call[0][0]
        assert isinstance(copy_call[0][1], StringIO)

    def test_load_rejects_empty_list(self):
        """Test load_rejects with empty list."""
        result = load_rejects([], run_id=123, table="stg_wages_rejects")
        assert result == 0

    def test_load_rejects_invalid_table(self):
        """Test load_rejects with invalid table name."""
        records = [{"raw_data": {}, "rejection_reason": "Test"}]

        with pytest.raises(ValueError, match="Invalid reject table"):
            load_rejects(records, run_id=123, table="invalid_table")

    @patch('src.load.staging.get_connection')
    def test_load_rejects_with_raw_data_key(self, mock_get_connection):
        """Test load_rejects when records have 'raw_data' key."""
        records = [
            {"raw_data": {"county": "001"}, "rejection_reason": "Error"}
        ]

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=False)

        result = load_rejects(records, run_id=123, table="stg_wages_rejects")
        assert result == 1

    @patch('src.load.staging.get_connection')
    def test_load_rejects_without_raw_data_key(self, mock_get_connection):
        """Test load_rejects when record itself is the raw_data."""
        records = [
            {"county": "001", "rejection_reason": "Error"}
        ]

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=False)

        result = load_rejects(records, run_id=123, table="stg_wages_rejects")
        assert result == 1

    @patch('src.load.staging.get_connection')
    def test_load_rejects_truncates_long_reasons(self, mock_get_connection):
        """Test that long rejection reasons are truncated."""
        long_reason = "x" * 2000
        records = [
            {"raw_data": {}, "rejection_reason": long_reason}
        ]

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=False)

        load_rejects(records, run_id=123, table="stg_wages_rejects")

        # Verify the reason was truncated to 1000 chars
        copy_call = mock_cursor.copy_expert.call_args
        buffer = copy_call[0][1]
        content = buffer.getvalue()
        # The reason in the CSV should be truncated
        assert len(long_reason) == 2000
        # The actual content in buffer will have the truncated version


class TestGetStagingCounts:
    """Tests for get_staging_counts function."""

    @patch('src.load.staging.get_cursor')
    def test_get_staging_counts(self, mock_get_cursor):
        """Test getting counts for all staging tables."""
        mock_cursor = MagicMock()
        # Mock fetchone to return different counts for each table
        mock_cursor.fetchone.side_effect = [
            [10],  # stg_wages
            [20],  # stg_expenses
            [5],   # stg_wages_rejects
            [3],   # stg_expenses_rejects
        ]
        mock_get_cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_get_cursor.return_value.__exit__ = Mock(return_value=False)

        result = get_staging_counts()

        assert result == {
            "stg_wages": 10,
            "stg_expenses": 20,
            "stg_wages_rejects": 5,
            "stg_expenses_rejects": 3,
        }

        # Verify execute was called 4 times (once per table)
        assert mock_cursor.execute.call_count == 4


class TestTruncateStaging:
    """Tests for truncate_staging function."""

    @patch('src.load.staging.get_cursor')
    def test_truncate_staging(self, mock_get_cursor):
        """Test truncating all staging tables."""
        mock_cursor = MagicMock()
        mock_get_cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_get_cursor.return_value.__exit__ = Mock(return_value=False)

        truncate_staging()

        # Verify execute was called 4 times (once per table)
        assert mock_cursor.execute.call_count == 4

        # Verify all tables were truncated
        calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        assert any("TRUNCATE TABLE stg_wages" in call for call in calls)
        assert any("TRUNCATE TABLE stg_expenses" in call for call in calls)
        assert any("TRUNCATE TABLE stg_wages_rejects" in call for call in calls)
        assert any("TRUNCATE TABLE stg_expenses_rejects" in call for call in calls)
        assert all("CASCADE" in call for call in calls)


class TestAllowedRejectTables:
    """Tests for ALLOWED_REJECT_TABLES constant."""

    def test_allowed_reject_tables(self):
        """Test that ALLOWED_REJECT_TABLES contains expected tables."""
        assert "stg_wages_rejects" in ALLOWED_REJECT_TABLES
        assert "stg_expenses_rejects" in ALLOWED_REJECT_TABLES
        assert len(ALLOWED_REJECT_TABLES) == 2
        assert isinstance(ALLOWED_REJECT_TABLES, frozenset)

