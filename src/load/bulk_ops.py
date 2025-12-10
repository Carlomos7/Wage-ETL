"""
Bulk operations using PostgreSQL COPY.
"""
from io import StringIO

import pandas as pd

from config.logging import get_logger
from src.load.db import get_connection

logger = get_logger(module=__name__)


def copy_to_temp(
    conn,
    df: pd.DataFrame,
    temp_table: str,
    columns: list[str],
    column_defs: str,
) -> int:
    """
    Create temp table and COPY data into it.

    Args:
        conn: Database connection
        df: DataFrame to load
        temp_table: Temp table name
        columns: Column names to copy
        column_defs: SQL column definitions for CREATE TABLE

    Returns:
        Number of rows copied
    """
    if df.empty:
        return 0

    buffer = StringIO()
    df[columns].to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TEMP TABLE {temp_table} ({column_defs}) ON COMMIT DROP")
        cur.copy_expert(
            f"COPY {temp_table} ({','.join(columns)}) FROM STDIN WITH CSV",
            buffer
        )
        return cur.rowcount


# Column definitions for staging tables
WAGES_COLUMNS = ["run_id", "county_fips", "adults",
                 "working_adults", "children", "wage_type", "hourly_wage"]
WAGES_COLUMN_DEFS = """
    run_id INTEGER,
    county_fips VARCHAR(3),
    adults INTEGER,
    working_adults INTEGER,
    children INTEGER,
    wage_type VARCHAR(20),
    hourly_wage NUMERIC(10,2)
"""

EXPENSES_COLUMNS = ["run_id", "county_fips", "adults",
                    "working_adults", "children", "expense_category", "annual_amount"]
EXPENSES_COLUMN_DEFS = """
    run_id INTEGER,
    county_fips VARCHAR(3),
    adults INTEGER,
    working_adults INTEGER,
    children INTEGER,
    expense_category VARCHAR(50),
    annual_amount NUMERIC(10,2)
"""
