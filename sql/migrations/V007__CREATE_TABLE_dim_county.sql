CREATE TABLE IF NOT EXISTS dim_county (
    county_id SERIAL PRIMARY KEY,
    state_fips VARCHAR(2) NOT NULL,
    county_fips VARCHAR(3) NOT NULL,
    full_fips VARCHAR(5) UNIQUE NOT NULL,
    county_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(state_fips, county_fips)
);