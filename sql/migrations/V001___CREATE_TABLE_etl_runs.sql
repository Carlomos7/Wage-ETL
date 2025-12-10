CREATE TABLE IF NOT EXISTS etl_runs (
    run_id SERIAL PRIMARY KEY,
    run_start_timestamp TIMESTAMP NOT NULL,
    run_end_timestamp TIMESTAMP,
    run_status VARCHAR(20) NOT NULL,
    state_fips VARCHAR(2),
    counties_processed INTEGER,
    wages_loaded INTEGER,
    wages_rejected INTEGER,
    expenses_loaded INTEGER,
    expenses_rejected INTEGER,
    error_message TEXT,
    scrape_date DATE,
    CONSTRAINT check_run_status CHECK (
        run_status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL')
    )
);