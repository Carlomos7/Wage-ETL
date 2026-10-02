"""
Tests for configuration models.
"""
import pytest
from pydantic import ValidationError
from wage_etl.config.models import (
    HttpClientConfig,
    ApiConfig,
    ScrapingConfig,
    PipelineConfig,
    StateCodes,
)


class TestHttpClientConfig:
    """Tests for HttpClientConfig."""

    def test_valid_data(self):
        """Test HttpClientConfig with valid data."""
        config = HttpClientConfig(base_url="https://example.com")
        assert str(config.base_url).rstrip("/") == "https://example.com"
        assert config.max_retries == 3
        assert config.timeout_seconds == 30

    def test_invalid_empty_url(self):
        """Test HttpClientConfig with empty URL."""
        with pytest.raises(ValidationError):
            HttpClientConfig(base_url="")


class TestApiConfig:
    """Tests for ApiConfig."""

    def test_valid_data(self):
        """Test ApiConfig with valid data."""
        api_config = ApiConfig(
            base_url="https://api.census.gov/data",
            dataset="2023/acs/acs5",
            variables=["NAME"],
            county=["*"]
        )
        assert str(api_config.base_url).rstrip("/") == "https://api.census.gov/data"
        assert api_config.dataset == "2023/acs/acs5"
        assert api_config.variables == ["NAME"]
        assert api_config.county == ["*"]
        assert api_config.cache_ttl_days == 30  # Default from HttpClientConfig

    def test_invalid_empty_url(self):
        """Test ApiConfig with empty URL."""
        with pytest.raises(ValidationError):
            ApiConfig(
                base_url="",
                dataset="2023/acs/acs5",
                variables=["NAME"],
                county=["*"]
            )


class TestScrapingConfig:
    """Tests for ScrapingConfig."""

    def test_valid_data(self):
        """Test ScrapingConfig with valid data."""
        scraping_config = ScrapingConfig(base_url="https://livingwage.mit.edu")
        assert str(scraping_config.base_url).rstrip("/") == "https://livingwage.mit.edu"
        assert scraping_config.min_delay_seconds == 1.0
        assert scraping_config.max_delay_seconds == 3.0

    def test_invalid_delay_range(self):
        """Test ScrapingConfig with invalid delay range."""
        with pytest.raises(ValidationError):
            ScrapingConfig(
                base_url="https://example.com",
                min_delay_seconds=5.0,
                max_delay_seconds=2.0,
            )

    def test_zero_min_delay_still_checked(self):
        """A min delay of 0 still rejects a smaller max delay."""
        with pytest.raises(ValidationError):
            ScrapingConfig(
                base_url="https://example.com",
                min_delay_seconds=0,
                max_delay_seconds=-1,
            )

    def test_zero_delays_are_valid(self):
        """Equal delays of 0 are allowed."""
        config = ScrapingConfig(
            base_url="https://example.com",
            min_delay_seconds=0,
            max_delay_seconds=0,
        )
        assert config.min_delay_seconds == 0
        assert config.max_delay_seconds == 0


class TestPipelineConfig:
    """Tests for PipelineConfig."""

    def test_valid_data(self):
        """Test PipelineConfig with valid data."""
        config = PipelineConfig()
        assert config.min_success_rate == 0.8
        assert config.target_states == ["*"]  # Default is "*" which normalizes to ["*"]

    def test_invalid_success_rate(self):
        """Test PipelineConfig with invalid success rate."""
        with pytest.raises(ValidationError):
            PipelineConfig(min_success_rate=1.5)



class TestStateCodes:
    """Tests for StateCodes."""

    def test_valid_data(self):
        """Test StateCodes with valid FIPS map."""
        fips_map = {"AL": "01", "AK": "02"}
        state_config = StateCodes(fips_map=fips_map)
        assert state_config.fips_map == fips_map

    def test_invalid_empty_fips_map(self):
        """Test StateCodes with empty FIPS map."""
        with pytest.raises(ValidationError):
            StateCodes(fips_map={})
