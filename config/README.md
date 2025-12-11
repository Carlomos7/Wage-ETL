# config/

Application configuration. Secrets in `.env`, app settings in YAML, static data in JSON.

```mint
config/
├── __init__.py
├── settings.py        # Main Settings class (pydantic-settings)
├── models.py          # Pydantic models for config validation
├── config.yaml        # App settings (URLs, timeouts, target states)
├── state_fips.json    # State abbreviation to FIPS code lookup
├── logging.py         # Logging setup
└── logging_conf.json  # Logging handlers and formatters
```

## Config Sources

| What                  | Where               | Example                       |
| --------------------- | ------------------- | ----------------------------- |
| Database credentials  | `.env`              | `DB_HOST=localhost`           |
| Log level             | `.env`              | `LOG_LEVEL=DEBUG`             |
| API/scraping settings | `config.yaml`       | `timeout_seconds: 30`         |
| Target states         | `config.yaml`       | `target_states: ["NJ", "NY"]` |
| State FIPS codes      | `state_fips.json`   | `"NJ": "34"`                  |
| Log format/handlers   | `logging_conf.json` | rotating file handler         |

## Environment Variables

Create a `.env` file in the project root:

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=wage_db
DB_USER=postgres
DB_PASSWORD=secret

# Logging (optional)
LOG_LEVEL=INFO
LOG_TO_FILE=true
```

## Common Changes

**Change target states** - edit `config.yaml`:

```yaml
pipeline:
  target_states:
    - "NJ"
    - "NY"
    - "CA"
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
from config import get_settings

settings = get_settings()

# Database
settings.db_host
settings.db_password

# App config (from YAML)
settings.api.base_url
settings.scraping.timeout_seconds
settings.pipeline.target_states

# State lookup (from JSON)
settings.state_config.fips_map["NJ"]  # "34"
```

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

- URLs can't be empty
- `max_delay_seconds` must be ≥ `min_delay_seconds`
- `min_success_rate` must be between 0 and 1
- `log_level` must be DEBUG/INFO/WARNING/ERROR/CRITICAL

Bad config fails fast with a clear error message.
