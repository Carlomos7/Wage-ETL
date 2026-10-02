"""
Logging configuration for ETL pipeline using dictConfig.
"""
import json
import logging
import logging.config
from pathlib import Path

from wage_etl.config.models import LoggingSettings, PathSettings

DEFAULT_LOGGER_NAME = "wage_etl"


def setup_logging(logging_settings: LoggingSettings, paths: PathSettings) -> logging.Logger:
    """
    Configure logging using JSON config file.
    Environment differences handled by settings (level, to_file).
    """
    config_file = logging_settings.config_file

    if not config_file.exists():
        raise FileNotFoundError(f"Logging configuration not found: {config_file}")

    with open(config_file, "r", encoding="utf-8") as handle:
        config = json.load(handle)

    for logger_config in config.get("loggers", {}).values():
        logger_config["level"] = logging_settings.level

    if logging_settings.to_file:
        paths.log_dir.mkdir(parents=True, exist_ok=True)

        for handler in config.get("handlers", {}).values():
            if "filename" in handler:
                filename = Path(handler["filename"]).name
                handler["filename"] = str(paths.log_dir / filename)
    else:
        config["handlers"] = {
            key: value
            for key, value in config.get("handlers", {}).items()
            if value.get("class") != "logging.handlers.RotatingFileHandler"
        }

        for logger_config in config.get("loggers", {}).values():
            logger_config["handlers"] = ["console"]

    logging.config.dictConfig(config)

    logger = logging.getLogger(logging_settings.name)
    logger.info(f"Logging initialized - Level: {logging_settings.level}")

    return logger


def get_logger(name: str = DEFAULT_LOGGER_NAME, module: str | None = None) -> logging.Logger:
    """
    Get a logger for a specific module or the default logger.

    Args:
        name: The name of the logger.
        module: The name of the module to get a logger for.

    Returns:
        A logger for the specific module or the default logger.
    """
    if module:
        return logging.getLogger(name).getChild(module)
    return logging.getLogger(name)


def format_log_with_metadata(message: str, year: int, state_fips: str, county_fips: str) -> str:
    """
    Format log message with structured metadata for provenance tracking.

    Args:
        message: The log message
        year: Year of the scrape
        state_fips: State FIPS code
        county_fips: County FIPS code (will be zero-padded)

    Returns:
        Formatted message with metadata prefix
    """
    county_fips_str = str(county_fips).zfill(3)
    metadata = f"[year={year}][state={state_fips}][county={county_fips_str}]"
    return f"{metadata} {message}"
