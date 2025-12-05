"""
Shared pytest fixtures for extraction tests.
"""
import pytest
from pathlib import Path
from unittest.mock import Mock

from src.extract.cache import ResponseCache
from src.extract.http import HttpClient


@pytest.fixture
def temp_cache_dir(tmp_path):
    """Create a temporary cache directory for testing."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


@pytest.fixture
def mock_http_client():
    """Create a mock HttpClient for testing."""
    return Mock(spec=HttpClient)


@pytest.fixture
def mock_response_cache():
    """Create a mock ResponseCache for testing."""
    return Mock(spec=ResponseCache)

