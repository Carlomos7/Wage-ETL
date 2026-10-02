"""
Tests for load database connection management.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import psycopg2
from psycopg2.extras import RealDictCursor
from pydantic import SecretStr

from wage_etl.config.models import DatabaseSettings
from wage_etl.load.db import Database


def database_settings() -> DatabaseSettings:
    """Settings used by connection tests."""
    return DatabaseSettings(
        host="localhost",
        port=5432,
        name="test_db",
        user="test_user",
        password=SecretStr("test_pass"),
    )


class TestConnect:
    """Tests for Database.connect."""

    @patch("wage_etl.load.db.psycopg2.connect")
    def test_connection_success(self, mock_connect):
        """Test successful connection with auto-commit."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        db = Database(database_settings())

        with db.connect() as conn:
            assert conn == mock_conn

        mock_connect.assert_called_once_with(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass",
        )
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("wage_etl.load.db.psycopg2.connect")
    def test_connection_rollback_on_exception(self, mock_connect):
        """Test that exceptions trigger rollback."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        db = Database(database_settings())

        with pytest.raises(ValueError):
            with db.connect():
                raise ValueError("Test error")

        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_not_called()
        mock_conn.close.assert_called_once()

    @patch("wage_etl.load.db.psycopg2.connect")
    def test_connection_close_on_connect_error(self, mock_connect):
        """Test that connection errors are raised."""
        mock_connect.side_effect = psycopg2.OperationalError("Connection failed")
        db = Database(database_settings())

        with pytest.raises(psycopg2.OperationalError):
            with db.connect():
                pass


class TestCursor:
    """Tests for Database.cursor."""

    def test_cursor_default(self):
        """Test cursor creation with default (non-dict) cursor."""
        db = Database(database_settings())
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        db.connect = Mock()
        db.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        db.connect.return_value.__exit__ = Mock(return_value=False)

        with db.cursor() as cur:
            assert cur == mock_cursor

        mock_conn.cursor.assert_called_once_with(cursor_factory=None)

    def test_cursor_dict_cursor(self):
        """Test cursor creation with dict_cursor=True."""
        db = Database(database_settings())
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        db.connect = Mock()
        db.connect.return_value.__enter__ = Mock(return_value=mock_conn)
        db.connect.return_value.__exit__ = Mock(return_value=False)

        with db.cursor(dict_cursor=True) as cur:
            assert cur == mock_cursor

        mock_conn.cursor.assert_called_once_with(cursor_factory=RealDictCursor)


class TestTestConnection:
    """Tests for Database.test."""

    def test_connection_success(self):
        """Test successful connection test."""
        db = Database(database_settings())
        mock_cursor = MagicMock()
        db.cursor = Mock()
        db.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        db.cursor.return_value.__exit__ = Mock(return_value=False)

        assert db.test() is True
        mock_cursor.execute.assert_called_once_with("SELECT 1")

    def test_connection_failure(self):
        """Test connection failure."""
        db = Database(database_settings())
        db.cursor = Mock(side_effect=psycopg2.OperationalError("Connection failed"))

        assert db.test() is False

    def test_connection_other_exception(self):
        """Test that other exceptions are caught."""
        db = Database(database_settings())
        db.cursor = Mock(side_effect=ValueError("Unexpected error"))

        assert db.test() is False
