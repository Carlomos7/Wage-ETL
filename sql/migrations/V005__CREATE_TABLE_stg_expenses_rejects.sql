CREATE TABLE IF NOT EXISTS stg_expenses_rejects (
    reject_id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES etl_runs(run_id),
    raw_data JSONB,
    rejection_reason TEXT,
    reject_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);