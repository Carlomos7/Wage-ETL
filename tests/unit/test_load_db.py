"""
Tests for load database connection management.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import psycopg2
from psycopg2.extras import RealDictCursor

from src.load.db import get_connection, get_cursor, test_connection as db_test_connection


class TestGetConnection:
    """Tests for get_connection context manager."""

    @patch('src.load.db.psycopg2.connect')
    @patch('src.load.db.get_settings')
    def test_connection_success(self, mock_settings, mock_connect):
        """Test successful connection with auto-commit."""
        # Setup mocks
        mock_settings.return_value = Mock(
            db_host="localhost",
            db_port=5432,
            db_name="test_db",
            db_user="test_user",
            db_password="test_pass"
        )
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Test
        with get_connection() as conn:
            assert conn == mock_conn

        # Verify connection was made with correct parameters
        mock_connect.assert_called_once_with(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass"
        )
        # Verify commit and close were called
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch('src.load.db.psycopg2.connect')
    @patch('src.load.db.get_settings')
    def test_connection_rollback_on_exception(self, mock_settings, mock_connect):
        """Test that exceptions trigger rollback."""
        mock_settings.return_value = Mock(
            db_host="localhost",
            db_port=5432,
            db_name="test_db",
            db_user="test_user",
            db_password="test_pass"
        )
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Test exception handling
        with pytest.raises(ValueError):
            with get_connection() as conn:
                raise ValueError("Test error")

        # Verify rollback was called, commit was not
        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_not_called()
        mock_conn.close.assert_called_once()

    @patch('src.load.db.psycopg2.connect')
    @patch('src.load.db.get_settings')
    def test_connection_close_on_connect_error(self, mock_settings, mock_connect):
        """Test that connection errors are handled."""
        mock_settings.return_value = Mock(
            db_host="localhost",
            db_port=5432,
            db_name="test_db",
            db_user="test_user",
            db_password="test_pass"
        )
        mock_connect.side_effect = psycopg2.OperationalError("Connection failed")

        # Test that connection error is raised
        with pytest.raises(psycopg2.OperationalError):
            with get_connection() as conn:
                pass


class TestGetCursor:
    """Tests for get_cursor context manager."""

    @patch('src.load.db.get_connection')
    def test_cursor_default(self, mock_get_connection):
        """Test cursor creation with default (non-dict) cursor."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=False)

        with get_cursor() as cur:
            assert cur == mock_cursor

        mock_conn.cursor.assert_called_once_with(cursor_factory=None)

    @patch('src.load.db.get_connection')
    def test_cursor_dict_cursor(self, mock_get_connection):
        """Test cursor creation with dict_cursor=True."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_get_connection.return_value.__enter__ = Mock(return_value=mock_conn)
        mock_get_connection.return_value.__exit__ = Mock(return_value=False)

        with get_cursor(dict_cursor=True) as cur:
            assert cur == mock_cursor

        mock_conn.cursor.assert_called_once_with(cursor_factory=RealDictCursor)


class TestTestConnection:
    """Tests for test_connection function."""

    @patch('src.load.db.get_cursor')
    def test_connection_success(self, mock_get_cursor):
        """Test successful connection test."""
        mock_cursor = MagicMock()
        mock_get_cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_get_cursor.return_value.__exit__ = Mock(return_value=False)

        result = db_test_connection()
        assert result is True
        mock_cursor.execute.assert_called_once_with("SELECT 1")

    @patch('src.load.db.get_cursor')
    def test_connection_failure(self, mock_get_cursor):
        """Test connection failure."""
        mock_get_cursor.side_effect = psycopg2.OperationalError("Connection failed")

        result = db_test_connection()
        assert result is False

    @patch('src.load.db.get_cursor')
    def test_connection_other_exception(self, mock_get_cursor):
        """Test that other exceptions are caught."""
        mock_get_cursor.side_effect = ValueError("Unexpected error")

        result = db_test_connection()
        assert result is False

