CREATE TABLE IF NOT EXISTS stg_expenses (
    expense_id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES etl_runs(run_id),
    county_fips VARCHAR(3) NOT NULL,
    adults INTEGER NOT NULL,
    working_adults INTEGER NOT NULL,
    children INTEGER NOT NULL,
    expense_category VARCHAR(50) NOT NULL,
    annual_amount NUMERIC(10, 2) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_expense_record UNIQUE (
        county_fips,
        adults,
        working_adults,
        children,
        expense_category
    )
);