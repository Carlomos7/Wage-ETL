"""
Database connection management.
"""
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor

from wage_etl.config.logging import get_logger
from wage_etl.config.models import DatabaseSettings

logger = get_logger(module=__name__)


class Database:
    """Owns connection settings and opens a connection per operation."""

    def __init__(self, settings: DatabaseSettings) -> None:
        self._settings = settings

    @contextmanager
    def connect(self):
        """Get a database connection with auto-commit/rollback."""
        conn = psycopg2.connect(
            host=self._settings.host,
            port=self._settings.port,
            database=self._settings.name,
            user=self._settings.user,
            password=self._settings.password.get_secret_value(),
        )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @contextmanager
    def cursor(self, dict_cursor: bool = False):
        """Get a cursor with automatic connection handling."""
        cursor_factory = RealDictCursor if dict_cursor else None
        with self.connect() as conn:
            with conn.cursor(cursor_factory=cursor_factory) as cur:
                yield cur

    def test(self) -> bool:
        """Test database connectivity."""
        try:
            with self.cursor() as cur:
                cur.execute("SELECT 1")
            logger.info("Database connection OK")
            return True
        except Exception as exc:
            logger.error(f"Database connection failed: {exc}")
            return False
