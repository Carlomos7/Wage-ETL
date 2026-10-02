# config/

Application configuration. Secrets in `.env`, app settings in YAML, static data in JSON. The Python that loads them lives in `src/wage_etl/config/`.

```mint
config/
├── config.yaml        # App settings (URLs, timeouts, target states)
├── state_fips.json    # State abbreviation to FIPS code lookup
├── logging_conf.json  # Logging handlers and formatters
└── README.md
```

## Config Sources

Highest priority first: constructor arguments, environment variables, `.env`, `config.yaml`, then secrets. `state_fips.json` is reference data, loaded on its own, not a setting.

| What                  | Where               | Example                                      |
| --------------------- | ------------------- | -------------------------------------------- |
| Database credentials  | `.env`              | `WAGE_ETL_DB__HOST=localhost`                |
| Log level             | `.env`              | `WAGE_ETL_LOGGING__LEVEL=DEBUG`              |
| API/scraping settings | `config.yaml`       | `timeout_seconds: 30`                        |
| Target states         | `config.yaml`       | `target_states: ["NJ", "NY"]`                |
| One-run state override | environment        | `WAGE_ETL_PIPELINE__TARGET_STATES='["NY"]'`  |
| State FIPS codes      | `state_fips.json`   | `"NJ": "34"`                                 |
| Log format/handlers   | `logging_conf.json` | rotating file handler                        |

## Environment Variables

Create a `.env` file in the project root. The app and Docker Compose both read these names.

```bash
# Database
WAGE_ETL_DB__HOST=localhost
WAGE_ETL_DB__PORT=5432
WAGE_ETL_DB__NAME=wage_db
WAGE_ETL_DB__USER=postgres
WAGE_ETL_DB__PASSWORD=secret

# Logging (optional)
WAGE_ETL_LOGGING__LEVEL=INFO
WAGE_ETL_LOGGING__TO_FILE=true
```

`cache_dir` is always `data_dir / cache`. Override the data directory with `WAGE_ETL_PATHS__DATA_DIR`.

## Common Changes

**Change target states** - edit `config.yaml`:

```yaml
pipeline:
  target_states:
    - "NJ"
    - "NY"
    - "CA"
```

**Run one state without editing the file:**

```bash
WAGE_ETL_PIPELINE__TARGET_STATES='["NY"]' uv run wage-etl
```

**Run all states** - use wildcard:

```yaml
pipeline:
  target_states:
    - "*"
```

**Adjust scraping delay** - be polite to MIT's servers:

```yaml
scraping:
  min_delay_seconds: 2.0
  max_delay_seconds: 5.0
```

## Usage

```python
from wage_etl.config import get_settings

settings = get_settings()
settings.paths.ensure_dirs()

settings.db.host
settings.db.password.get_secret_value()
settings.api.base_url
settings.scraping.timeout_seconds
settings.pipeline.target_states
settings.paths.cache_dir
```

State FIPS codes are loaded with `StateCodes.from_json`, not from `settings`.

## YAML Configuration Details

The pipeline is configured through [`config.yaml`](config.yaml). By default, the configuration files are set as follows:

### API Configuration

- `api.base_url`: Census API base URL (default: `https://api.census.gov/data`)
- `api.dataset`: Census dataset identifier (default: `2023/acs/acs5`)
- `api.variables`: List of variables to fetch (default: `["NAME"]`)
- `api.county`: County filter (default: `["*"]` for all counties)
- `api.max_retries`: Maximum retry attempts (default: `3`)
- `api.timeout_seconds`: Request timeout (default: `30`)
- `api.cache_ttl_days`: Cache expiration in days (default: `90`)
- `api.ssl_verify`: Enable SSL verification (default: `true`)

### Scraping Configuration

- `scraping.base_url`: MIT Living Wage Calculator base URL (default: `https://livingwage.mit.edu`)
- `scraping.max_retries`: Maximum retry attempts (default: `3`)
- `scraping.timeout_seconds`: Request timeout (default: `30`)
- `scraping.cache_ttl_days`: Cache expiration in days (default: `30`)
- `scraping.ssl_verify`: Enable SSL verification (default: `true`)
- `scraping.min_delay_seconds`: Minimum delay between requests (default: `1.0`)
- `scraping.max_delay_seconds`: Maximum delay between requests (default: `3.0`)

### Pipeline Configuration

- `pipeline.min_success_rate`: Minimum success rate threshold (default: `0.8`)
- `pipeline.target_states`: List of state abbreviations to process (default: `["NJ"]`)

### State FIPS Mapping

The [`state_fips.json`](state_fips.json) file maps US state abbreviations to FIPS codes. Used for:

- Filtering counties by state
- Validating state inputs
- Generating state-specific queries

To process different states, edit `config.yaml`:

```yaml
pipeline:
  target_states:
    - "NY"
    - "CA"
    - "TX"
```

## Validation

All config is validated with Pydantic on startup:

- `base_url` must be an HTTP URL
- `max_delay_seconds` must be ≥ `min_delay_seconds`
- `min_success_rate` must be between 0 and 1
- log level must be DEBUG/INFO/WARNING/ERROR/CRITICAL
- unknown keys inside a YAML section are errors

Bad config fails fast with a clear error message. The database password is a secret and is omitted from `repr(settings)`.
