"""
Database connection management.
"""
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor

from config.settings import get_settings
from config.logging import get_logger

logger = get_logger(module=__name__)


@contextmanager
def get_connection():
    """Get a database connection with auto-commit/rollback."""
    settings = get_settings()
    conn = psycopg2.connect(
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
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
def get_cursor(dict_cursor: bool = False):
    """Get a cursor with automatic connection handling."""
    cursor_factory = RealDictCursor if dict_cursor else None
    with get_connection() as conn:
        with conn.cursor(cursor_factory=cursor_factory) as cur:
            yield cur


def test_connection() -> bool:
    """Test database connectivity."""
    try:
        with get_cursor() as cur:
            cur.execute("SELECT 1")
        logger.info("Database connection OK")
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False
