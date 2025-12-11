# Wage ETL Pipeline

>Automated ETL System for Economic Analysis

<h1 align="center">
  <br>
  <div>
    <a href="https://github.com/Carlomos7/Wage-ETL/issues">
        <img src="https://img.shields.io/github/issues-raw/Carlomos7/Wage-ETL?labelColor=303446&style=for-the-badge">
    </a>
    <a href="https://github.com/Carlomos7/Wage-ETL/issues?q=is%3Aissue+is%3Aclosed">
        <img src="https://img.shields.io/github/issues-closed-raw/Carlomos7/Wage-ETL?labelColor=303446&style=for-the-badge">
    </a>
    <a href="https://github.com/Carlomos7/Wage-ETL">
        <img src="https://img.shields.io/github/repo-size/Carlomos7/Wage-ETL?labelColor=303446&style=for-the-badge">
    </a>
    <a href="https://github.com/Carlomos7/Wage-ETL">
        <img src="https://img.shields.io/github/milestones/all/Carlomos7/Wage-ETL?labelColor=303446&style=for-the-badge"/>
    </a>
    <br>
  </div>
</h1>
<br>

A Python-based ETL system that extracts living wage and expense data from MIT's Living Wage Calculator and US Census Bureau API, transforms it from wide-format HTML tables to normalized long-format records, validates it using Pydantic models, and loads it into PostgreSQL staging tables with comprehensive error handling.

## Architecture Overview

```mermaid
graph TD
    A["<b>DATA SOURCES</b><br/>━━━━━━━━━━━━━━━━<br/>• MIT Living Wage Calculator (HTML)<br/>• US Census Bureau API (JSON)"]
    B["<b>EXTRACT LAYER</b><br/>━━━━━━━━━━━━━━━━<br/>• HttpClient (unified HTTP operations)<br/>• ResponseCache (file-based, TTL)<br/>• WageExtractor (BeautifulSoup)<br/>• CensusExtractor (API client)"]
    C["<b>TRANSFORM LAYER</b><br/>━━━━━━━━━━━━━━━━<br/>• Wide → Long format conversion<br/>• Currency cleaning (DataFrame-level)<br/>• Pydantic validation models<br/>• Family config parsing"]
    D["<b>LOAD LAYER</b><br/>━━━━━━━━━━━━━━━━<br/>• Bulk upsert operations<br/>• Staging tables (stg_*)<br/>• Reject tables (data quality)<br/>• PostgreSQL with psycopg2"]
    
    A -->|Data Flow| B
    B -->|Extracted Data| C
    C -->|Transformed Data| D
    
    classDef sourceStyle fill:#4A90E2,stroke:#2E5C8A,stroke-width:3px,color:#fff
    classDef extractStyle fill:#7B68EE,stroke:#5A4BB5,stroke-width:3px,color:#fff
    classDef transformStyle fill:#50C878,stroke:#3A9B5C,stroke-width:3px,color:#fff
    classDef loadStyle fill:#FF6B6B,stroke:#CC5555,stroke-width:3px,color:#fff
    
    class A sourceStyle
    class B extractStyle
    class C transformStyle
    class D loadStyle
```

## Key Features

- Web scraping with BeautifulSoup4 and Census API integration
- HTTP response caching with TTL-based expiration
- DataFrame-level data cleaning and currency normalization
- Strict schema validation with Pydantic v2
- Bulk PostgreSQL operations using COPY for performance
- Reject table handling for data quality tracking
- Idempotent pipeline design for safe re-runs
- Full test coverage with pytest

## Technologies Used

**Core Language & Runtime:**

- Python 3.13 (with comprehensive type hints)
- uv (modern, fast dependency resolver and package manager)

**Data Processing:**

- pandas 2.3.3 - Data manipulation and transformation
- Pydantic 2.12.4 - Data validation and modeling
- BeautifulSoup4 4.14.2 - HTML parsing for web scraping

**Database & Connectivity:**

- PostgreSQL 16 - Relational database for staging and analytics
- psycopg2-binary 2.9.11 - PostgreSQL adapter for Python

**HTTP & APIs:**

- requests 2.32.5 - HTTP library for API calls and web scraping

**Configuration & Settings:**

- PyYAML 6.0.3 - YAML configuration file parsing
- python-dotenv 1.2.1 - Environment variable management
- pydantic-settings 2.12.0 - Settings management with validation

**Local Development Setup:**

- Docker Compose - Container orchestration
- Flyway - Database migration management
- PostgreSQL 16 (containerized)

**Testing & Quality:**

- pytest 9.0.2 - Testing framework
- coverage.py 7.13.0 - Code coverage analysis

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

Before you begin, ensure you have the following installed:

- **Python 3.13+**
- **uv** *Optional*- [Modern Python package manager](https://github.com/Carlomos7/Wage-ETL)
- **Docker** - For local environment setup
- **Git** - For cloning the repository

### Installing

1. **Clone the repository**

   ```bash
   git clone https://github.com/Carlomos7/Wage-ETL.git
   cd Wage-ETL
   ```

2. **Install dependencies using uv**

   ```bash
   uv sync
   ```

   or pip

3. **Set up environment variables**
   Create a `.env` file in the project root:

   ```bash
   # Database Configuration
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=wage_etl
   DB_USER=postgres
   DB_PASSWORD=your_password_here
   
   # Optional: Logging Configuration
   LOG_LEVEL=INFO
   LOG_TO_FILE=true
   ```

4. **Start the database infrastructure**

   ```bash
   docker-compose up -d
   ```

   This will start:
   - PostgreSQL 16 database
   - Flyway migrations (automatically runs SQL migrations)
   - PgAdmin (optional, accessible at <http://localhost:5050>)

5. **Run the ETL pipeline**

   ```bash
   uv run -m main
   ```

**Example: Querying the data**

After running the pipeline, you can query the staging tables:

```sql
-- Check run status
SELECT run_id, run_status, counties_processed, wages_loaded, expenses_loaded
FROM etl_runs
ORDER BY run_start_timestamp DESC
LIMIT 5;

-- View sample wage data
SELECT county_fips, adults, working_adults, children, wage_type, hourly_wage
FROM stg_wages
LIMIT 10;

-- Check for rejected records
SELECT COUNT(*) as reject_count, rejection_reason
FROM stg_wages_rejects
GROUP BY rejection_reason;
```

### Initial Configuration

The pipeline is configured through YAML files and environment variables. By default, the configuration files are set as follows:

**[`config/config.yaml`](config/config.yaml)** - Main pipeline configuration:

- **API Configuration**:
  - Census API base URL: `https://api.census.gov/data`
  - Dataset: `2023/acs/acs5`
  - Cache TTL: 90 days
  - Max retries: 3
  - Timeout: 30 seconds

- **Scraping Configuration**:
  - Base URL: `https://livingwage.mit.edu`
  - Cache TTL: 30 days
  - Delay between requests: 1-3 seconds (randomized)
  - Max retries: 3
  - Timeout: 30 seconds

- **Pipeline Configuration**:
  - Target states: `["NJ"]` (default - modify to process other states)
  - Minimum success rate: 0.8 (80%)

**Environment Variables** (`.env` file):

- Database connection parameters (required)
- Logging configuration (optional, defaults to INFO level)

**State FIPS Mapping** (`config/state_fips.json`):

- Maps state abbreviations to FIPS codes
- Used for filtering counties by state

To process different states, edit `config/config.yaml`:

```yaml
pipeline:
  target_states:
    - "NY"
    - "CA"
    - "TX"
```

## Running the tests with coverage

The project includes comprehensive unit and integration tests. Run them using pytest:

**Run all tests:**

```bash
uv run coverage run -m pytest # or just uv run pytest
```

**Run coverage report:**

```bash
uv run coverage report
```

**Generate HTML Coverage Report:**

```bash
uv run coverage html
```

**Generate HTML Coverage Report:**

```bash
uv run coverage erase
```

**View coverage report:**
After running with coverage, open `htmlcov/index.html` in your browser to see the detailed coverage report.

## Usage

### Basic Pipeline Execution

1. **Start the infrastructure** (if not already running):

   ```bash
   docker-compose up -d
   ```

2. **Run the ETL pipeline**:

   ```bash
   uv run -m main
   ```

The pipeline will:

- Extract county FIPS codes from the Census API for configured states
- Scrape wage and expense data from MIT Living Wage Calculator for each county
- Transform wide-format data to long format
- Validate data using Pydantic models
- Load valid records into staging tables (`stg_wages`, `stg_expenses`)
- Store invalid records in reject tables (`stg_wages_rejects`, `stg_expenses_rejects`)
- Track the run in `etl_runs` table with status and statistics

### Pipeline Flow

```mint
1. Extract
   ├── Census API → County FIPS codes
   └── Web Scraper → HTML tables per county

2. Transform
   ├── Parse HTML → Wide-format DataFrames
   ├── Validate → Pre-transform checks
   ├── Normalize → Long-format DataFrames
   └── Validate → Pydantic model conversion

3. Load
   ├── Accumulate → Combine all counties
   ├── Bulk COPY → Temporary tables
   ├── UPSERT → Staging tables (ON CONFLICT)
   ├── Load Rejects → Invalid records
   └── Update Run → etl_runs table
```

### Monitoring Pipeline Execution

**View logs:**

- Console output: Real-time structured logs
- File logs: `logs/etl.log` and `logs/error.log`

**Check run status in database:**

```sql
SELECT 
    run_id,
    run_status,
    run_start_timestamp,
    run_end_timestamp,
    state_fips,
    counties_processed,
    wages_loaded,
    wages_rejected,
    expenses_loaded,
    expenses_rejected
FROM etl_runs
ORDER BY run_start_timestamp DESC;
```

**Query staging data:**

```sql
-- Sample wage data
SELECT * FROM stg_wages LIMIT 10;

-- Sample expense data
SELECT * FROM stg_expenses LIMIT 10;

-- Rejected records
SELECT * FROM stg_wages_rejects;
SELECT * FROM stg_expenses_rejects;
```

## Configuration

### YAML Configuration ([`config/config.yaml`](config/config.yaml))

**API Configuration:**

- `api.base_url`: Census API base URL (default: `https://api.census.gov/data`)
- `api.dataset`: Census dataset identifier (default: `2023/acs/acs5`)
- `api.variables`: List of variables to fetch (default: `["NAME"]`)
- `api.county`: County filter (default: `["*"]` for all counties)
- `api.max_retries`: Maximum retry attempts (default: `3`)
- `api.timeout_seconds`: Request timeout (default: `30`)
- `api.cache_ttl_days`: Cache expiration in days (default: `90`)
- `api.ssl_verify`: Enable SSL verification (default: `true`)

**Scraping Configuration:**

- `scraping.base_url`: MIT Living Wage Calculator base URL (default: `https://livingwage.mit.edu`)
- `scraping.max_retries`: Maximum retry attempts (default: `3`)
- `scraping.timeout_seconds`: Request timeout (default: `30`)
- `scraping.cache_ttl_days`: Cache expiration in days (default: `30`)
- `scraping.ssl_verify`: Enable SSL verification (default: `true`)
- `scraping.min_delay_seconds`: Minimum delay between requests (default: `1.0`)
- `scraping.max_delay_seconds`: Maximum delay between requests (default: `3.0`)

**Pipeline Configuration:**

- `pipeline.min_success_rate`: Minimum success rate threshold (default: `0.8`)
- `pipeline.target_states`: List of state abbreviations to process (default: `["NJ"]`)

### Environment Variables (`.env`)

**Required:**

- `DB_HOST`: PostgreSQL host (default: `localhost`)
- `DB_PORT`: PostgreSQL port (default: `5432`)
- `DB_NAME`: Database name (default: `wage_etl`)
- `DB_USER`: Database user (default: `postgres`)
- `DB_PASSWORD`: Database password (required)

**Optional:**

- `LOG_LEVEL`: Logging level - DEBUG, INFO, WARNING, ERROR, CRITICAL (default: `INFO`)
- `LOG_TO_FILE`: Enable file logging (default: `true`)
- `PGADMIN_EMAIL`: PgAdmin login email (default: `admin@example.com`)
- `PGADMIN_PASSWORD`: PgAdmin login password (default: `admin`)

### State FIPS Mapping ([`config/state_fips.json`](config/state_fips.json))

Maps US state abbreviations to FIPS codes. Used for:

- Filtering counties by state
- Validating state inputs
- Generating state-specific queries

## Links

**Data Sources:**

- [MIT Living Wage Calculator](https://livingwage.mit.edu/) - Living wage data sourced from the Living Wage Institute via https://livingwage.mit.edu
- [US Census Bureau API](https://www.census.gov/data/developers/data-sets.html) - Source of county and state metadata
- [Census API Documentation](https://www.census.gov/data/developers/guidance/api-user-guide.html) - API usage guide

**Project Resources:**

- [Documentation Tasks](https://github.com/users/Carlomos7/projects/9) - Kanban Board of Issues

## Licensing

The code in this project is licensed under the MIT License.

See the [LICENSE](LICENSE) file for the full text of the license.

**Copyright (c) 2025 Carlos Ray Segarra**

## References

- [DOs and DON'Ts of Web Scraping in 2025](https://medium.com/@datajournal/dos-and-donts-of-web-scraping-e4f9b2a49431)
- [Pydantic Setting Management Documentation](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)
