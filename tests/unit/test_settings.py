"""Tests for Settings loading and source priority."""

from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from wage_etl.config.models import DatabaseSettings, PathSettings
from wage_etl.config.settings import Settings, get_settings


def _db() -> DatabaseSettings:
    return DatabaseSettings(
        host="localhost",
        port=5432,
        name="wage_db",
        user="postgres",
        password=SecretStr("super-secret"),
    )


def test_yaml_loads_pipeline_targets():
    """config.yaml supplies api, scraping, and pipeline."""
    settings = Settings(_env_file=None, db=_db())
    assert settings.pipeline.target_states == ["NJ"]
    assert str(settings.api.base_url).rstrip("/") == "https://api.census.gov/data"
    assert settings.scraping.min_delay_seconds == 1.0


def test_prefixed_env_overrides_yaml(monkeypatch):
    """WAGE_ETL_PIPELINE__TARGET_STATES replaces the YAML value for one run."""
    monkeypatch.setenv("WAGE_ETL_PIPELINE__TARGET_STATES", '["NY"]')
    settings = Settings(_env_file=None, db=_db())
    assert settings.pipeline.target_states == ["NY"]


def test_prefixed_env_fills_database(monkeypatch):
    """WAGE_ETL_DB__* populates nested database settings."""
    monkeypatch.setenv("WAGE_ETL_DB__HOST", "db-host")
    monkeypatch.setenv("WAGE_ETL_DB__PORT", "5433")
    monkeypatch.setenv("WAGE_ETL_DB__NAME", "wage_db")
    monkeypatch.setenv("WAGE_ETL_DB__USER", "postgres")
    monkeypatch.setenv("WAGE_ETL_DB__PASSWORD", "secret")

    settings = Settings(_env_file=None)
    assert settings.db.host == "db-host"
    assert settings.db.port == 5433
    assert settings.db.name == "wage_db"
    assert settings.db.user == "postgres"
    assert settings.db.password.get_secret_value() == "secret"


def test_password_is_hidden_from_repr():
    """SecretStr does not appear in repr(settings)."""
    settings = Settings(_env_file=None, db=_db())
    assert "super-secret" not in repr(settings)
    assert "super-secret" not in repr(settings.db)


def test_cache_dir_follows_data_dir(tmp_path):
    """cache_dir is derived from data_dir, not captured at class definition."""
    paths = PathSettings(data_dir=tmp_path / "custom", log_dir=tmp_path / "logs")
    assert paths.cache_dir == tmp_path / "custom" / "cache"


def test_yaml_typo_is_rejected(tmp_path, monkeypatch):
    """A typo inside a YAML section is an error."""
    config_file = tmp_path / "bad.yaml"
    config_file.write_text(
        """
api:
  base_url: https://example.com
  dataset: "2023/acs/acs5"
  variables: ["NAME"]
  county: ["*"]
  timout_seconds: 30
scraping:
  base_url: https://example.com
pipeline:
  target_states: ["NJ"]
""",
        encoding="utf-8",
    )

    class TypoSettings(Settings):
        model_config = Settings.model_config.copy()
        model_config["yaml_file"] = config_file
        model_config["env_file"] = None

    monkeypatch.delenv("WAGE_ETL_API__TIMEOUT_SECONDS", raising=False)
    with pytest.raises(ValidationError):
        TypoSettings(_env_file=None, db=_db())


def test_get_settings_does_not_create_directories(tmp_path, monkeypatch):
    """Reading settings does not create data or log directories."""
    data_dir = tmp_path / "data"
    log_dir = tmp_path / "logs"
    monkeypatch.setenv("WAGE_ETL_PATHS__DATA_DIR", str(data_dir))
    monkeypatch.setenv("WAGE_ETL_PATHS__LOG_DIR", str(log_dir))
    monkeypatch.setenv("WAGE_ETL_DB__HOST", "localhost")
    monkeypatch.setenv("WAGE_ETL_DB__PORT", "5432")
    monkeypatch.setenv("WAGE_ETL_DB__NAME", "wage_db")
    monkeypatch.setenv("WAGE_ETL_DB__USER", "postgres")
    monkeypatch.setenv("WAGE_ETL_DB__PASSWORD", "secret")

    get_settings.cache_clear()
    try:
        settings = get_settings()
        assert settings.paths.data_dir == Path(str(data_dir))
        assert not data_dir.exists()
        assert not log_dir.exists()
    finally:
        get_settings.cache_clear()
