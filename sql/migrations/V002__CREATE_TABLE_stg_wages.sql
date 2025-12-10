CREATE TABLE IF NOT EXISTS stg_wages (
    wage_id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES etl_runs(run_id),
    county_fips VARCHAR(3) NOT NULL,
    adults INTEGER NOT NULL,
    working_adults INTEGER NOT NULL,
    children INTEGER NOT NULL,
    wage_type VARCHAR(20) NOT NULL,
    hourly_wage NUMERIC(10, 2) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_wage_record UNIQUE (
        county_fips,
        adults,
        working_adults,
        children,
        wage_type
    )
);