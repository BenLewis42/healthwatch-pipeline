"""Tests for CDC API extraction module"""

import pytest
from unittest.mock import patch, MagicMock
from src.extract.cdc_api_client import CDCAPIClient
from src.extract.config import DataConfig
from datetime import datetime


@pytest.fixture
def api_client():
    """Create API client instance for testing."""
    return CDCAPIClient()


@pytest.fixture
def sample_covid_response():
    """Sample COVID API response."""
    return {
        "data": [
            {
                "state": "CA",
                "date": "2024-02-01",
                "cases": 100,
                "deaths": 5,
            },
            {
                "state": "NY",
                "date": "2024-02-01",
                "cases": 150,
                "deaths": 8,
            },
        ]
    }


def test_api_client_initialization(api_client):
    """Test API client initializes correctly."""
    assert api_client.config is not None
    assert api_client.session is not None


@patch("src.extract.cdc_api_client.requests.Session.get")
def test_fetch_covid_data(mock_get, api_client, sample_covid_response):
    """Test COVID data fetching."""
    mock_response = MagicMock()
    mock_response.json.return_value = sample_covid_response
    mock_get.return_value = mock_response

    result = api_client.fetch_covid_data("2024-01-01", "2024-02-01")

    assert result == sample_covid_response
    mock_get.assert_called_once()


def test_save_raw_data(api_client, tmp_path):
    """Test saving raw data to file."""
    import os

    # Change to temp directory
    original_cwd = os.getcwd()
    os.chdir(tmp_path)

    try:
        test_data = {"test": "data"}
        filepath = api_client.save_raw_data(test_data, "test_source")

        assert filepath.exists()
        assert "test_source" in filepath.name

        # Verify file contents
        import json

        with open(filepath) as f:
            saved_data = json.load(f)
        assert saved_data == test_data

    finally:
        os.chdir(original_cwd)


def test_data_config():
    """Test DataConfig initialization."""
    config = DataConfig()

    assert config.start_date.year == 2023
    assert config.data_sources == ["covid", "flu"]
