"""
Tests for load run tracker.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from wage_etl.load.run_tracker import start_run, end_run, get_latest_run


def _cursor_db(cursor=None, side_effect=None):
    """Database double whose cursor() yields cursor or raises."""
    db = Mock()
    if side_effect is not None:
        db.cursor.side_effect = side_effect
        return db
    db.cursor.return_value.__enter__ = Mock(return_value=cursor)
    db.cursor.return_value.__exit__ = Mock(return_value=False)
    return db


class TestStartRun:
    """Tests for start_run function."""

    @patch('wage_etl.load.run_tracker.datetime')
    def test_start_run_success(self, mock_datetime):
        """Test successful run start."""
        # Setup mocks
        now = datetime(2024, 1, 1, 12, 0, 0)
        now_date = now.date()
        # Create a mock datetime object that returns the date when .date() is called
        mock_now = MagicMock()
        mock_now.date.return_value = now_date
        mock_datetime.now.return_value = mock_now

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [123]
        db = _cursor_db(mock_cursor)

        # Test
        run_id = start_run(db, "01")

        # Verify
        assert run_id == 123
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        assert "INSERT INTO etl_runs" in call_args[0][0]
        # Verify the parameters match (datetime, state_fips, date)
        params = call_args[0][1]
        assert params[0] == mock_now
        assert params[1] == "01"
        assert params[2] == now_date

    def test_start_run_database_error(self):
        """Test that database errors are propagated."""
        db = _cursor_db(side_effect=Exception("Database error"))

        with pytest.raises(Exception, match="Database error"):
            start_run(db, "01")


class TestEndRun:
    """Tests for end_run function."""

    @patch('wage_etl.load.run_tracker.datetime')
    def test_end_run_success(self, mock_datetime):
        """Test successful run end."""
        now = datetime(2024, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = now

        mock_cursor = MagicMock()
        db = _cursor_db(mock_cursor)

        # Test
        end_run(
            db,
            run_id=123,
            status="SUCCESS",
            counties=10,
            wages_loaded=100,
            wages_rejected=5,
            expenses_loaded=200,
            expenses_rejected=10
        )

        # Verify
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        assert "UPDATE etl_runs" in call_args[0][0]
        assert call_args[0][1] == (
            now, "SUCCESS", 10, 100, 5, 200, 10, None, 123
        )

    @patch('wage_etl.load.run_tracker.datetime')
    def test_end_run_with_error(self, mock_datetime):
        """Test end_run with error message."""
        now = datetime(2024, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = now

        mock_cursor = MagicMock()
        db = _cursor_db(mock_cursor)

        # Test
        end_run(
            db,
            run_id=123,
            status="FAILED",
            error="Test error message"
        )

        # Verify error message is included
        call_args = mock_cursor.execute.call_args
        assert call_args[0][1][7] == "Test error message"  # error_message position

    def test_end_run_default_values(self):
        """Test end_run with default values."""
        mock_cursor = MagicMock()
        db = _cursor_db(mock_cursor)

        end_run(db, run_id=123, status="SUCCESS")

        # Verify defaults are used
        call_args = mock_cursor.execute.call_args
        params = call_args[0][1]
        assert params[2] == 0  # counties
        assert params[3] == 0  # wages_loaded
        assert params[4] == 0  # wages_rejected
        assert params[5] == 0  # expenses_loaded
        assert params[6] == 0  # expenses_rejected


class TestGetLatestRun:
    """Tests for get_latest_run function."""

    def test_get_latest_run_with_state(self):
        """Test get_latest_run with state_fips filter."""
        mock_cursor = MagicMock()
        mock_row = {
            "run_id": 123,
            "state_fips": "01",
            "run_status": "SUCCESS"
        }
        mock_cursor.fetchone.return_value = mock_row
        db = _cursor_db(mock_cursor)

        result = get_latest_run(db, "01")

        assert result == mock_row
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        assert "WHERE state_fips = %s" in call_args[0][0]
        assert call_args[0][1] == ("01",)

    def test_get_latest_run_without_state(self):
        """Test get_latest_run without state_fips filter."""
        mock_cursor = MagicMock()
        mock_row = {
            "run_id": 123,
            "state_fips": "02",
            "run_status": "SUCCESS"
        }
        mock_cursor.fetchone.return_value = mock_row
        db = _cursor_db(mock_cursor)

        result = get_latest_run(db)

        assert result == mock_row
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args
        assert "WHERE" not in call_args[0][0]  # No WHERE clause

    def test_get_latest_run_no_results(self):
        """Test get_latest_run when no runs exist."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        db = _cursor_db(mock_cursor)

        result = get_latest_run(db)

        assert result is None

